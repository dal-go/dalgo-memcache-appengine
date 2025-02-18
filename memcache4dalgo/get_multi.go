package memcache4dalgo

import (
	"context"
	"encoding/json"
	"github.com/dal-go/dalgo/dal"
	"google.golang.org/appengine/memcache"
	"strings"
)

func getMultiRecords(ctx context.Context, records []dal.Record, getMulti func(context.Context, []dal.Record) error) error {
	keys := make([]string, len(records))
	recordsByKey := make(map[string]dal.Record, len(records))
	for i, r := range records {
		keys[i] = r.Key().String()
		recordsByKey[keys[i]] = r
	}
	items, err := memcache.GetMulti(ctx, keys)
	if err == nil {
		for key, item := range items {
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
	if len(recordsByKey) < len(keys) {
		var i int
		for _, r := range recordsByKey {
			records[i] = r
			i++
		}
		records = records[:len(recordsByKey)]
	}
	if err = getMulti(ctx, records); err == nil {
		var mks []string
		for _, r := range records {
			if r.Error() != nil {
				continue
			}
			key := r.Key().String()
			var value []byte
			if value, err = json.Marshal(r.Data()); err == nil {
				_ = memcache.Set(ctx, &memcache.Item{Value: value, Key: key})
				if Debugf != nil {
					mks = append(mks, key)
				}
			}
		}
		if Debugf != nil && len(mks) > 0 {
			Debugf(ctx, "memcache4dalgo.getMultiRecords: miss & set %v", strings.Join(mks, ", "))
		}
	}
	return err
}
