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
	mc := key.String()
	var item *memcache.Item
	if item, err = memcache.Get(ctx, mc); err == nil {
		record.SetError(nil)
		if err = json.Unmarshal(item.Value, record.Data()); err == nil {
			if Debugf != nil {
				Debugf(ctx, "memcache4dalgo.getRecord: hit %s", mc)
			}
			return
		}
	}
	if err = get(ctx, record); err == nil {
		var value []byte
		if value, err = json.Marshal(record.Data()); err == nil {
			_ = memcache.Set(ctx, &memcache.Item{Value: value, Key: mc})
			if Debugf != nil {
				Debugf(ctx, "memcache4dalgo.getRecord: miss & set %s", mc)
			}
		}
	}
	return
}
