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
) error {
	keys := make([]*dal.Key, len(records))
	mks := make([]string, 0, len(records))
	recordsByKey := make(map[string]dal.Record, len(records))
	for i, r := range records {
		keys[i] = r.Key()
		mk := keys[i].String()
		recordsByKey[mk] = r
		if isCacheable(keys[i]) {
			mks = append(mks, mk)
		}
	}
	itemsByKey, err := memcache.GetMulti(ctx, mks)
	if err == nil {
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
			mks = mks[:0]
			items := make([]*memcache.Item, 0, len(records))

			for _, r := range records {
				if r.Error() != nil {
					continue
				}
				key := r.Key().String()
				var value []byte
				if value, err = json.Marshal(r.Data()); err == nil {
					items = append(items, &memcache.Item{Value: value, Key: key})
					if Debugf != nil {
						mks = append(mks, key)
					}
				}
			}
			if len(items) == 1 {
				_ = memcache.Set(ctx, items[0])
			} else if len(items) > 1 {
				_ = memcache.SetMulti(ctx, items)
			}

			if Debugf != nil && len(mks) > 0 {
				Debugf(ctx, "memcache4dalgo.getMultiRecords: miss & set %v", strings.Join(mks, ", "))
			}
		}
	}
	return err
}
