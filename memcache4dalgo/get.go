package memcache4dalgo

import (
	"context"
	"encoding/json"
	"github.com/dal-go/dalgo/dal"
	"google.golang.org/appengine/memcache"
)

func getRecord(ctx context.Context, record dal.Record, get func(ctx context.Context, record dal.Record) error) (err error) {
	key := record.Key().String()
	var item *memcache.Item
	if item, err = memcache.Get(ctx, key); err == nil {
		if err = json.Unmarshal(item.Value, record.Data()); err == nil {
			return
		}
	}
	return get(ctx, record)
}
