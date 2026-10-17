package dalgo2memcachegae

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	dalrecord "github.com/dal-go/record"
	"google.golang.org/appengine/v2/memcache"
)

type sampleData struct {
	Name string `json:"name"`
}

func TestExistsByKey(t *testing.T) {
	ctx := context.Background()
	key := dalrecord.NewKeyWithID("kind", "id1")

	t.Run("not_cacheable", func(t *testing.T) {
		called := false
		exists, err := existsByKey(ctx, key, "test", func(_ *dalrecord.Key) bool { return false }, func(ctx context.Context, k *dalrecord.Key) (bool, error) {
			called = true
			return true, nil
		})
		if err != nil || !called || !exists {
			t.Fatalf("expected true, nil, called, got exists=%v, err=%v, called=%v", exists, err, called)
		}
	})

	t.Run("cache_hit", func(t *testing.T) {
		origGet := memcacheGet
		defer func() { memcacheGet = origGet }()
		memcacheGet = func(ctx context.Context, k string) (*memcache.Item, error) {
			return &memcache.Item{Key: k, Value: []byte("val")}, nil
		}

		exists, err := existsByKey(ctx, key, "test", func(_ *dalrecord.Key) bool { return true }, func(ctx context.Context, k *dalrecord.Key) (bool, error) {
			t.Fatal("existsFunc should not be called on cache hit")
			return false, nil
		})
		if err != nil || !exists {
			t.Fatalf("expected true, nil, got exists=%v, err=%v", exists, err)
		}
	})

	t.Run("cache_miss", func(t *testing.T) {
		origGet := memcacheGet
		defer func() { memcacheGet = origGet }()
		memcacheGet = func(ctx context.Context, k string) (*memcache.Item, error) {
			return nil, memcache.ErrCacheMiss
		}

		called := false
		exists, err := existsByKey(ctx, key, "test", func(_ *dalrecord.Key) bool { return true }, func(ctx context.Context, k *dalrecord.Key) (bool, error) {
			called = true
			return false, nil
		})
		if err != nil || !called || exists {
			t.Fatalf("expected false, nil, called, got exists=%v, err=%v, called=%v", exists, err, called)
		}
	})

	t.Run("cache_error_other", func(t *testing.T) {
		origGet := memcacheGet
		defer func() { memcacheGet = origGet }()
		memcacheGet = func(ctx context.Context, k string) (*memcache.Item, error) {
			return nil, errors.New("memcache_server_error")
		}

		called := false
		exists, err := existsByKey(ctx, key, "test", func(_ *dalrecord.Key) bool { return true }, func(ctx context.Context, k *dalrecord.Key) (bool, error) {
			called = true
			return true, nil
		})
		if err != nil || !called || !exists {
			t.Fatalf("expected true, nil, called, got exists=%v, err=%v, called=%v", exists, err, called)
		}
	})
}

func TestGetRecord(t *testing.T) {
	ctx := context.Background()
	key := dalrecord.NewKeyWithID("kind", "id1")

	t.Run("not_cacheable", func(t *testing.T) {
		called := false
		data := sampleData{}
		rec := dalrecord.NewRecordWithData(key, &data)
		err := getRecord(ctx, false, rec, "test", func(_ *dalrecord.Key) bool { return false }, func(ctx context.Context, record dalrecord.Record) error {
			called = true
			return nil
		})
		if err != nil || !called {
			t.Fatalf("expected nil err, called=true, got %v, %v", err, called)
		}
	})

	t.Run("cache_hit", func(t *testing.T) {
		origGet := memcacheGet
		defer func() { memcacheGet = origGet }()
		raw, _ := json.Marshal(sampleData{Name: "Alice"})
		memcacheGet = func(ctx context.Context, k string) (*memcache.Item, error) {
			return &memcache.Item{Key: k, Value: raw}, nil
		}

		data := sampleData{}
		rec := dalrecord.NewRecordWithData(key, &data)
		err := getRecord(ctx, false, rec, "test", func(_ *dalrecord.Key) bool { return true }, func(ctx context.Context, record dalrecord.Record) error {
			t.Fatal("underlying get should not be called on cache hit")
			return nil
		})
		if err != nil {
			t.Fatalf("unexpected err: %v", err)
		}
		if data.Name != "Alice" {
			t.Fatalf("expected Alice, got %s", data.Name)
		}
	})

	t.Run("cache_hit_unmarshal_error", func(t *testing.T) {
		origGet := memcacheGet
		defer func() { memcacheGet = origGet }()
		memcacheGet = func(ctx context.Context, k string) (*memcache.Item, error) {
			return &memcache.Item{Key: k, Value: []byte("not_json")}, nil
		}

		called := false
		data := sampleData{}
		rec := dalrecord.NewRecordWithData(key, &data)
		err := getRecord(ctx, false, rec, "test", func(_ *dalrecord.Key) bool { return true }, func(ctx context.Context, record dalrecord.Record) error {
			called = true
			record.SetError(nil)
			data.Name = "FromDB"
			return nil
		})
		if err != nil || !called || data.Name != "FromDB" {
			t.Fatalf("expected FromDB, nil err, called=true, got %v, %v, %s", err, called, data.Name)
		}
	})

	t.Run("cache_miss", func(t *testing.T) {
		origGet := memcacheGet
		defer func() { memcacheGet = origGet }()
		memcacheGet = func(ctx context.Context, k string) (*memcache.Item, error) {
			return nil, memcache.ErrCacheMiss
		}

		called := false
		data := sampleData{}
		rec := dalrecord.NewRecordWithData(key, &data)
		err := getRecord(ctx, false, rec, "test", func(_ *dalrecord.Key) bool { return true }, func(ctx context.Context, record dalrecord.Record) error {
			called = true
			record.SetError(nil)
			data.Name = "FromDB"
			return nil
		})
		if err != nil || !called || data.Name != "FromDB" {
			t.Fatalf("expected FromDB, nil err, called=true, got %v, %v, %s", err, called, data.Name)
		}
	})

	t.Run("cache_error_other", func(t *testing.T) {
		origGet := memcacheGet
		defer func() { memcacheGet = origGet }()
		memcacheGet = func(ctx context.Context, k string) (*memcache.Item, error) {
			return nil, errors.New("memcache_server_error")
		}

		called := false
		data := sampleData{}
		rec := dalrecord.NewRecordWithData(key, &data)
		err := getRecord(ctx, false, rec, "test", func(_ *dalrecord.Key) bool { return true }, func(ctx context.Context, record dalrecord.Record) error {
			called = true
			record.SetError(nil)
			return nil
		})
		if err != nil || !called {
			t.Fatalf("expected nil err, called=true, got %v, %v", err, called)
		}
	})

	t.Run("in_transaction", func(t *testing.T) {
		origGet := memcacheGet
		defer func() { memcacheGet = origGet }()
		memcacheGet = func(ctx context.Context, k string) (*memcache.Item, error) {
			t.Fatal("memcache.Get must not be called when in transaction")
			return nil, nil
		}

		called := false
		data := sampleData{}
		rec := dalrecord.NewRecordWithData(key, &data)
		err := getRecord(ctx, true, rec, "test", func(_ *dalrecord.Key) bool { return true }, func(ctx context.Context, record dalrecord.Record) error {
			called = true
			record.SetError(nil)
			return nil
		})
		if err != nil || !called {
			t.Fatalf("expected nil err, called=true, got %v, %v", err, called)
		}
	})

	t.Run("set_to_cache_marshal_error", func(t *testing.T) {
		called := false
		rec := dalrecord.NewRecordWithData(key, make(chan int))
		err := getRecord(ctx, false, rec, "test", func(_ *dalrecord.Key) bool { return true }, func(ctx context.Context, record dalrecord.Record) error {
			called = true
			record.SetError(nil)
			return nil
		})
		if err != nil || !called {
			t.Fatalf("expected nil err, called=true, got %v, %v", err, called)
		}
	})

	t.Run("set_to_cache_memcache_error", func(t *testing.T) {
		origSet := memcacheSet
		defer func() { memcacheSet = origSet }()
		memcacheSet = func(ctx context.Context, item *memcache.Item) error {
			return errors.New("memcache_set_error")
		}

		called := false
		rec := dalrecord.NewRecordWithData(key, &sampleData{Name: "test"})
		err := getRecord(ctx, false, rec, "test", func(_ *dalrecord.Key) bool { return true }, func(ctx context.Context, record dalrecord.Record) error {
			called = true
			record.SetError(nil)
			return nil
		})
		if err != nil || !called {
			t.Fatalf("expected nil err, called=true, got %v, %v", err, called)
		}
	})
}
