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
	return getRecord(ctx, record, t.isCacheable, t.ro.Get)
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

func (t transaction) Set(ctx context.Context, record dal.Record) error {
	deleteCached(ctx, record.Key(), t.isCacheable)
	return t.rw.Set(ctx, record)
}

func (t transaction) SetMulti(ctx context.Context, records []dal.Record) error {
	deleteCached4records(ctx, records)
	return t.rw.SetMulti(ctx, records)
}

func (t transaction) Delete(ctx context.Context, key *dal.Key) error {
	deleteCached(ctx, key, t.isCacheable)
	return t.rw.Delete(ctx, key)
}

func (t transaction) DeleteMulti(ctx context.Context, keys []*dal.Key) error {
	deleteCachedByKeys(ctx, keys, t.isCacheable)
	return t.rw.DeleteMulti(ctx, keys)
}

func (t transaction) Update(ctx context.Context, key *dal.Key, updates []dal.Update, preconditions ...dal.Precondition) error {
	deleteCached(ctx, key, t.isCacheable)
	return t.rw.Update(ctx, key, updates, preconditions...)
}

func (t transaction) UpdateMulti(ctx context.Context, keys []*dal.Key, updates []dal.Update, preconditions ...dal.Precondition) error {
	deleteCachedByKeys(ctx, keys, t.isCacheable)
	return t.rw.UpdateMulti(ctx, keys, updates, preconditions...)
}

func deleteCached(ctx context.Context, key *dal.Key, isCacheable func(key *dal.Key) bool) {
	if !isCacheable(key) {
		return
	}
	mk := key.String()
	_ = memcache.Delete(ctx, mk)
	if Debugf != nil {
		Debugf(ctx, "memcache4dalgo.deleteCached: %s", mk)
	}
}

func deleteCachedByKeys(ctx context.Context, keys []*dal.Key, isCacheable func(key *dal.Key) bool) {
	mks := make([]string, len(keys))
	for i, k := range keys {
		mks[i] = k.String()
	}
	_ = memcache.DeleteMulti(ctx, mks)
	if Debugf != nil {
		Debugf(ctx, "memcache4dalgo.deleteCachedByKeys: %v", strings.Join(mks, ", "))
	}
}

func deleteCached4records(ctx context.Context, records []dal.Record) {
	keys := make([]string, len(records))
	for i, r := range records {
		keys[i] = r.Key().String()
	}
	_ = memcache.DeleteMulti(ctx, keys)
}

func (t transaction) Insert(ctx context.Context, record dal.Record, opts ...dal.InsertOption) error {
	err := t.rw.Insert(ctx, record, opts...)
	if err == nil {
		key := record.Key().String()
		var value []byte
		if value, err = json.Marshal(record.Data()); err == nil {
			_ = memcache.Add(ctx, &memcache.Item{Value: value, Key: key})
		}
	}
	return err
}

func (t transaction) InsertMulti(ctx context.Context, records []dal.Record, opts ...dal.InsertOption) error {
	return t.rw.InsertMulti(ctx, records, opts...)
}
