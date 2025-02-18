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
	if !isCacheable(key) { // If the record is not cacheable, we just get it from the database
		return get(ctx, record)
	}
	mk := key.String()
	var item *memcache.Item
	if item, err = memcache.Get(ctx, mk); err == nil {
		record.SetError(nil) // We must indicate we are going to access data for unmarshalling
		if err = json.Unmarshal(item.Value, record.Data()); err == nil {
			if Debugf != nil {
				Debugf(ctx, "memcache4dalgo.getRecord: hit %s", mk)
			}
			return
		} else if Debugf != nil { // Ignore the error and try to get the record from the database
			Debugf(ctx, "memcache4dalgo.getRecord: failed to unmarshal value from received from memcache ny key=%s: %v", mk, err)
		}
	} else if Debugf != nil { // Ignore the error and try to get the record from the database
		Debugf(ctx, "memcache4dalgo.getRecord: memcache.Get(key=%s) returned error: %v", mk, err)
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
			Debugf(ctx, "memcache4dalgo.%s(): set record to cache with key=%s", caller, mk)
		}
	}
	return
}
