package memcache4dalgo

import (
	"context"
	"github.com/dal-go/dalgo/dal"
	"testing"
)

func TestExistsByKey_NotCacheable(t *testing.T) {
	ctx := context.Background()
	key := &dal.Key{}
	called := false
	exists, err := existsByKey(ctx, key, "test", func(_ *dal.Key) bool { return false }, func(ctx context.Context, k *dal.Key) (bool, error) {
		called = true
		if k == nil {
			t.Fatal("key is nil in existsFunc")
		}
		return true, nil
	})
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if !called {
		t.Fatal("expected existsFunc to be called when not cacheable")
	}
	if !exists {
		t.Error("expected exists to be true")
	}
}

func TestGetMultiRecords_Empty(t *testing.T) {
	ctx := context.Background()
	if err := getMultiRecords(ctx, false, nil, nil, nil); err != nil {
		t.Fatalf("unexpected error for empty records: %v", err)
	}
}
