package memcache4dalgo

import (
	"context"
	"encoding/json"
	"github.com/dal-go/dalgo/dal"
	"google.golang.org/appengine/memcache"
)

func getRecord(
	ctx context.Context,
	record dal.Record,
	isCacheable func(key *dal.Key) bool,
	get func(ctx context.Context, record dal.Record) error,
) (err error) {
	key := record.Key()
	if !isCacheable(key) {
		return get(ctx, record)
	}
	mk := key.String()
	var item *memcache.Item
	if item, err = memcache.Get(ctx, mk); err == nil {
		record.SetError(nil)
		if err = json.Unmarshal(item.Value, record.Data()); err == nil {
			if Debugf != nil {
				Debugf(ctx, "memcache4dalgo.getRecord: hit %s", mk)
			}
			return
		}
	}
	if err = get(ctx, record); err == nil {
		if err = setRecordToCache(ctx, record, "getRecord"); err != nil {
			return
		}
	}
	return
}

func setRecordToCache(ctx context.Context, record dal.Record, caller string) (err error) {
	var value []byte
	if value, err = json.Marshal(record.Data()); err == nil {
		mk := record.Key().String()
		_ = memcache.Set(ctx, &memcache.Item{Value: value, Key: mk})
		if Debugf != nil {
			Debugf(ctx, "memcache4dalgo.%s: miss & set %s", caller, mk)
		}
	}
	return
}
