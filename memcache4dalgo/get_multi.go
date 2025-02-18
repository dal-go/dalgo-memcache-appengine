package memcache4dalgo

import (
	"context"
	"encoding/json"
	"github.com/dal-go/dalgo/dal"
	"google.golang.org/appengine/memcache"
	"strings"
)

func getMultiRecords(
	ctx context.Context,
	records []dal.Record,
	isCacheable func(key *dal.Key) bool,
	getMulti func(context.Context, []dal.Record) error,
) (err error) {
	keys := make([]*dal.Key, len(records))
	mks := make([]string, 0, len(records))
	recordsByKey := make(map[string]dal.Record, len(records))
	for i, r := range records {
		keys[i] = r.Key()
		mk := keys[i].String()
		recordsByKey[mk] = r
		if isCacheable == nil || isCacheable(keys[i]) {
			mks = append(mks, mk)
		}
	}
	var itemsByKey map[string]*memcache.Item
	if itemsByKey, err = memcache.GetMulti(ctx, mks); err == nil {
		for key, item := range itemsByKey {
			r := recordsByKey[key]
			r.SetError(nil)
			if err = json.Unmarshal(item.Value, r.Data()); err == nil {
				delete(recordsByKey, key)
				if Debugf != nil {
					Debugf(ctx, "memcache4dalgo.getMultiRecords: hit %s", key)
				}
			}
		}
	}
	if len(recordsByKey) > 0 {
		var i int
		for _, r := range recordsByKey {
			records[i] = r
			i++
		}
		records = records[:len(recordsByKey)]
		if err = getMulti(ctx, records); err == nil {
			if err = setRecordsToCache(ctx, records, "getMultiRecords", isCacheable); err != nil {
				return err
			}
		}
	}
	return err
}

func setRecordsToCache(ctx context.Context, records []dal.Record, caller string, isCacheable func(key *dal.Key) bool) (err error) {
	mks := make([]string, 0, len(records))
	items := make([]*memcache.Item, 0, len(records))
	for _, r := range records {
		key := r.Key()
		if !isCacheable(key) {
			continue
		}
		mk := key.String()
		var value []byte
		if value, err = json.Marshal(r.Data()); err == nil {
			items = append(items, &memcache.Item{Value: value, Key: mk})
			if Debugf != nil {
				mks = append(mks, mk)
			}
		}
	}
	if len(items) == 1 {
		err = memcache.Set(ctx, items[0])
	} else if len(items) > 1 {
		err = memcache.SetMulti(ctx, items)
	}
	if Debugf != nil && len(mks) > 0 {
		Debugf(ctx, "memcache4dalgo.%s: %v", caller, strings.Join(mks, ", "))
	}
	return

}
