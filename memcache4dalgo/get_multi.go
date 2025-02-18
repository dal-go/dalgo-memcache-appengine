package memcache4dalgo

import (
	"context"
	"encoding/json"
	"github.com/dal-go/dalgo/dal"
	"google.golang.org/appengine/memcache"
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
			if err = json.Unmarshal(item.Value, recordsByKey[key].Data()); err == nil {
				delete(recordsByKey, key)
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
	return getMulti(ctx, records)
}
