package dalgo2memcachegae

import (
	"context"
	"testing"
)

func TestTransaction_FlushItemsToCache_Empty(t *testing.T) {
	tx := &transaction{}
	if err := tx.flushItemsToCache(context.Background()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}
