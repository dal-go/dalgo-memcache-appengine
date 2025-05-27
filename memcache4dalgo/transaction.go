package memcache4dalgo

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/dal-go/dalgo/dal"
	"github.com/dal-go/dalgo/update"
	"google.golang.org/appengine/v2/memcache"
	"slices"
	"strings"
)

type transaction struct {
	ro dal.ReadTransaction
	rw dal.ReadwriteTransaction
	// isCacheable returns true if the key is cacheable
	isCacheable     func(key *dal.Key) bool
	itemsForCaching []*memcache.Item
}

func (t *transaction) addRecordsForCaching(ctx context.Context, record ...dal.Record) {
	for _, r := range record {
		if !t.isCacheable(r.Key()) {
			continue
		}
		key := r.Key()
		if value, err := json.Marshal(r.Data()); err != nil {
			Warningf(ctx, "failed to marshal record data for key=%s: %v", key, err)
			t.itemsForCaching = append(t.itemsForCaching, &memcache.Item{Key: key.String()})
		} else {
			t.itemsForCaching = append(t.itemsForCaching, &memcache.Item{Value: value, Key: key.String()})
		}
	}
}

func (t *transaction) ID() string {
	return t.rw.ID()
}

func (t *transaction) Options() dal.TransactionOptions {
	return t.ro.Options()
}

func (t *transaction) Get(ctx context.Context, record dal.Record) error {
	return getRecord(ctx, record, "tx", t.isCacheable, t.ro.Get)
}

func (t *transaction) Exists(ctx context.Context, key *dal.Key) (exists bool, err error) {
	return existsByKey(ctx, key, "tx", t.isCacheable, t.ro.Exists)
}

func (t *transaction) GetMulti(ctx context.Context, records []dal.Record) error {
	return getMultiRecords(ctx, records, t.isCacheable, t.ro.GetMulti)
}

func (t *transaction) QueryReader(ctx context.Context, query dal.Query) (dal.Reader, error) {
	return t.ro.QueryReader(ctx, query)
}

func (t *transaction) QueryAllRecords(ctx context.Context, query dal.Query) (records []dal.Record, err error) {
	return t.ro.QueryAllRecords(ctx, query)
}

func (t *transaction) Set(ctx context.Context, record dal.Record) (err error) {
	if err = t.rw.Set(ctx, record); err == nil {
		t.addRecordsForCaching(ctx, record)
	}
	return err
}

func (t *transaction) SetMulti(ctx context.Context, records []dal.Record) (err error) {
	if err = t.rw.SetMulti(ctx, records); err == nil {
		t.addRecordsForCaching(ctx, records...)
	}
	return err
}

func (t *transaction) Delete(ctx context.Context, key *dal.Key) (err error) {
	if err = t.rw.Delete(ctx, key); err == nil && t.isCacheable(key) {
		t.itemsForCaching = append(t.itemsForCaching, &memcache.Item{Key: key.String()})
	}
	return err
}

func (t *transaction) DeleteMulti(ctx context.Context, keys []*dal.Key) (err error) {
	if err = t.rw.DeleteMulti(ctx, keys); err == nil {
		for _, key := range keys {
			if t.isCacheable(key) {
				t.itemsForCaching = append(t.itemsForCaching, &memcache.Item{Key: key.String()})
			}
		}
	}
	return err
}

func (t *transaction) Update(ctx context.Context, key *dal.Key, updates []update.Update, preconditions ...dal.Precondition) (err error) {
	if err = t.rw.Update(ctx, key, updates, preconditions...); err == nil {
		t.itemsForCaching = append(t.itemsForCaching, &memcache.Item{Key: key.String()})
	}
	return err
}

func (t *transaction) UpdateRecord(ctx context.Context, record dal.Record, updates []update.Update, preconditions ...dal.Precondition) (err error) {
	if err = t.rw.UpdateRecord(ctx, record, updates, preconditions...); err == nil {
		t.addRecordsForCaching(ctx, record)
	}
	return err
}

func (t *transaction) UpdateMulti(ctx context.Context, keys []*dal.Key, updates []update.Update, preconditions ...dal.Precondition) (err error) {
	if err = t.rw.UpdateMulti(ctx, keys, updates, preconditions...); err == nil {
		for _, key := range keys {
			if t.isCacheable(key) {
				t.itemsForCaching = append(t.itemsForCaching, &memcache.Item{Key: key.String()})
			}
		}
	}
	return err
}

func (t *transaction) Insert(ctx context.Context, record dal.Record, opts ...dal.InsertOption) (err error) {
	if err = t.rw.Insert(ctx, record, opts...); err == nil {
		t.addRecordsForCaching(ctx, record)
	}
	return
}

func (t *transaction) InsertMulti(ctx context.Context, records []dal.Record, opts ...dal.InsertOption) (err error) {
	if err = t.rw.InsertMulti(ctx, records, opts...); err == nil {
		t.addRecordsForCaching(ctx, records...)
	}
	return err
}

func (t *transaction) flushItemsToCache(ctx context.Context) error {
	if len(t.itemsForCaching) == 0 {
		return nil
	}
	var appliedKeys []string

	itemsToSet := make([]*memcache.Item, 0, len(t.itemsForCaching))
	var keysToDelete []string

	// We store items to cache in reverse to make sure we apply the latest changes
	for i := len(t.itemsForCaching) - 1; i >= 0; i-- {
		item := t.itemsForCaching[i]
		if slices.Contains(appliedKeys, item.Key) {
			continue
		}
		appliedKeys = append(appliedKeys, item.Key)
		if item.Value != nil {
			itemsToSet = append(itemsToSet, item)
		} else {
			keysToDelete = append(keysToDelete, item.Key)
		}
	}
	if len(itemsToSet) > 0 {
		if err := memcache.SetMulti(ctx, itemsToSet); err != nil {
			notSetKeys := make([]string, len(itemsToSet))
			for _, item := range itemsToSet {
				notSetKeys = append(keysToDelete, item.Key)
			}
			Warningf(ctx, "failed to set to memcache by %d keys=[%s]: %w", len(notSetKeys), strings.Join(notSetKeys, ","), err)
			keysToDelete = append(keysToDelete, notSetKeys...)
		}
	}
	if len(keysToDelete) > 0 {
		if len(keysToDelete) == 1 {
			if err := memcache.Delete(ctx, keysToDelete[0]); err != nil {
				return fmt.Errorf("failed to delete from memcache by key=%s: %w", keysToDelete[0], err)
			}
		} else if err := memcache.DeleteMulti(ctx, keysToDelete); err != nil {
			return fmt.Errorf("failed to delete from memcache by %d keys=[%s]: %w", len(keysToDelete), strings.Join(keysToDelete, ","), err)

		}
	}
	return nil
}
