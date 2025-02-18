package memcache4dalgo

import (
	"context"
	"encoding/json"
	"github.com/dal-go/dalgo/dal"
	"google.golang.org/appengine/memcache"
	"strings"
)

type transaction struct {
	ro dal.ReadTransaction
	rw dal.ReadwriteTransaction
	// isCacheable returns true if the key is cacheable
	isCacheable func(key *dal.Key) bool
}

func (t transaction) ID() string {
	return t.rw.ID()
}

func (t transaction) Options() dal.TransactionOptions {
	return t.ro.Options()
}

func (t transaction) Get(ctx context.Context, record dal.Record) error {
	return getRecord(ctx, record, "tx", t.isCacheable, t.ro.Get)
}

func (t transaction) GetMulti(ctx context.Context, records []dal.Record) error {
	return getMultiRecords(ctx, records, t.isCacheable, t.ro.GetMulti)
}

func (t transaction) QueryReader(ctx context.Context, query dal.Query) (dal.Reader, error) {
	return t.ro.QueryReader(ctx, query)
}

func (t transaction) QueryAllRecords(ctx context.Context, query dal.Query) (records []dal.Record, err error) {
	return t.ro.QueryAllRecords(ctx, query)
}

func (t transaction) Set(ctx context.Context, record dal.Record) (err error) {
	if err = deleteFromCacheByKey(ctx, record.Key(), t.isCacheable); err != nil {
		return
	}
	return t.rw.Set(ctx, record)
}

func (t transaction) SetMulti(ctx context.Context, records []dal.Record) (err error) {
	if err = deleteRecordsFromCache(ctx, records, t.isCacheable); err != nil {
		return err
	}
	return t.rw.SetMulti(ctx, records)
}

func (t transaction) Delete(ctx context.Context, key *dal.Key) (err error) {
	if err = deleteFromCacheByKey(ctx, key, t.isCacheable); err != nil {
		return
	}
	return t.rw.Delete(ctx, key)
}

func (t transaction) DeleteMulti(ctx context.Context, keys []*dal.Key) (err error) {
	if err = deleteCachedByKeys(ctx, keys, t.isCacheable); err != nil {
		return
	}
	return t.rw.DeleteMulti(ctx, keys)
}

func (t transaction) Update(ctx context.Context, key *dal.Key, updates []dal.Update, preconditions ...dal.Precondition) (err error) {
	if err = deleteFromCacheByKey(ctx, key, t.isCacheable); err != nil {
		return
	}
	return t.rw.Update(ctx, key, updates, preconditions...)
}

func (t transaction) UpdateRecord(ctx context.Context, record dal.Record, updates []dal.Update, preconditions ...dal.Precondition) (err error) {
	if err = deleteFromCacheByKey(ctx, record.Key(), t.isCacheable); err != nil {
		return
	}
	if err = t.rw.UpdateRecord(ctx, record, updates, preconditions...); err == nil {
		if err = setRecordToCache(ctx, record, "UpdateRecord()"); err != nil {
			return
		}
	}
	return
}

func (t transaction) UpdateMulti(ctx context.Context, keys []*dal.Key, updates []dal.Update, preconditions ...dal.Precondition) (err error) {
	if err = deleteCachedByKeys(ctx, keys, t.isCacheable); err != nil {
		return
	}
	return t.rw.UpdateMulti(ctx, keys, updates, preconditions...)
}

func deleteFromCacheByKey(ctx context.Context, key *dal.Key, isCacheable func(key *dal.Key) bool) (err error) {
	if isCacheable != nil && !isCacheable(key) {
		return nil
	}
	mk := key.String()
	err = memcache.Delete(ctx, mk)
	if Debugf != nil {
		Debugf(ctx, "memcache4dalgo.deleteFromCacheByKey: %s", mk)
	}
	return
}

func deleteCachedByKeys(ctx context.Context, keys []*dal.Key, isCacheable func(key *dal.Key) bool) (err error) {
	mks := make([]string, 0, len(keys))
	for _, k := range keys {
		if isCacheable == nil || isCacheable(k) {
			mks = append(mks, k.String())
		}
	}
	if len(mks) == 1 {
		err = memcache.Delete(ctx, mks[0])
	} else if len(mks) > 1 {
		err = memcache.DeleteMulti(ctx, mks)
	}
	if len(mks) > 0 && Debugf != nil {
		Debugf(ctx, "memcache4dalgo.deleteCachedByKeys: %v", strings.Join(mks, ", "))
	}
	return
}

func deleteRecordsFromCache(ctx context.Context, records []dal.Record, isCacheable func(key *dal.Key) bool) (err error) {
	keys := make([]*dal.Key, len(records))
	mks := make([]string, 0, len(keys))
	for _, r := range records {
		key := r.Key()
		if isCacheable != nil && !isCacheable(key) {
			continue
		}
		keys = append(keys, key)
		mks = append(mks, key.String())
	}
	if len(mks) == 1 {
		err = memcache.Delete(ctx, mks[0])
	} else if len(mks) > 1 {
		err = memcache.DeleteMulti(ctx, mks)
	}
	if len(mks) > 0 && Debugf != nil {
		Debugf(ctx, "memcache4dalgo.deleteRecordsFromCache: %v", strings.Join(mks, ", "))
	}
	return err
}

func (t transaction) Insert(ctx context.Context, record dal.Record, opts ...dal.InsertOption) (err error) {
	if err = t.rw.Insert(ctx, record, opts...); err == nil {
		key := record.Key().String()
		var value []byte
		if value, err = json.Marshal(record.Data()); err == nil {
			if err = memcache.Add(ctx, &memcache.Item{Value: value, Key: key}); err != nil {
				return
			}
		}
	}
	return
}

func (t transaction) InsertMulti(ctx context.Context, records []dal.Record, opts ...dal.InsertOption) error {
	return t.rw.InsertMulti(ctx, records, opts...)
}
