package memcache4dalgo

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/dal-go/dalgo/dal"
	"google.golang.org/appengine/memcache"
	"time"
)

func getRecord(
	ctx context.Context,
	record dal.Record,
	caller string,
	isCacheable func(key *dal.Key) bool,
	get func(ctx context.Context, record dal.Record) error,
) (err error) {
	key := record.Key()
	if !isCacheable(key) { // If the record is not cacheable, we just get it from the database
		return get(ctx, record)
	}
	mk := key.String()
	started := time.Now()
	debugf := func(ctx context.Context, format string, args ...any) {
		if Debugf != nil {
			Debugf(ctx, "memcache4dalgo.getRecord("+caller+"): "+format, args...)
		}
	}
	var item *memcache.Item
	if item, err = memcache.Get(ctx, mk); err == nil {
		record.SetError(nil) // We must indicate we are going to access data for unmarshalling
		if err = json.Unmarshal(item.Value, record.Data()); err == nil {
			debugf(ctx, "cache hit on key=%s returned in %v", mk, time.Since(started))
			return // No need t get the record from the database
		} else { // Ignore the error and try to get the record from the database
			debugf(ctx, "failed to unmarshal value from memcache, key=%s: %v", mk, err)
		}
	} else if errors.Is(err, memcache.ErrCacheMiss) {
		debugf(ctx, "cache miss on key=%s returned in %v", mk, time.Since(started))
	} else {
		// Ignore the error and try to get the record from the database
		if Warningf != nil {
			Warningf(ctx, "memcache.Get(key=%s) returned error in %v: %v", mk, time.Since(started), err)
		} else {
			debugf(ctx, "WARNING: memcache.Get(key=%s) returned error in %v: %v", mk, time.Since(started), err)
		}
	}
	if err = get(ctx, record); err == nil {
		if err = setRecordToCache(ctx, record, fmt.Sprintf("getRecord(%s)", caller)); err != nil {
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
			Debugf(ctx, "memcache4dalgo.%s: set record to cache with key=%s", caller, mk)
		}
	}
	return
}
