package dalgo2memcachegae

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	dalrecord "github.com/dal-go/record"
	"google.golang.org/appengine/v2/memcache"
)

func TestGetMultiRecords(t *testing.T) {
	ctx := context.Background()

	t.Run("empty_records", func(t *testing.T) {
		err := getMultiRecords(ctx, false, nil, nil, nil)
		if err != nil {
			t.Fatalf("unexpected error for empty records: %v", err)
		}
	})

	t.Run("cache_hits_and_misses", func(t *testing.T) {
		origGetMulti := memcacheGetMulti
		origSetMulti := memcacheSetMulti
		defer func() {
			memcacheGetMulti = origGetMulti
			memcacheSetMulti = origSetMulti
		}()

		val1, _ := json.Marshal(sampleData{Name: "Item1"})
		memcacheGetMulti = func(ctx context.Context, keys []string) (map[string]*memcache.Item, error) {
			return map[string]*memcache.Item{
				"kind/id1": {Key: "kind/id1", Value: val1},
				"kind/id2": {Key: "kind/id2", Value: []byte("invalid_json")},
			}, nil
		}
		memcacheSetMulti = func(ctx context.Context, items []*memcache.Item) error {
			return nil
		}

		k1 := dalrecord.NewKeyWithID("kind", "id1")
		k2 := dalrecord.NewKeyWithID("kind", "id2")
		k3 := dalrecord.NewKeyWithID("kind", "id3")

		d1 := sampleData{}
		d2 := sampleData{}
		d3 := sampleData{}

		r1 := dalrecord.NewRecordWithData(k1, &d1)
		r2 := dalrecord.NewRecordWithData(k2, &d2)
		r3 := dalrecord.NewRecordWithData(k3, &d3)

		getMultiCalled := false
		err := getMultiRecords(ctx, false, []dalrecord.Record{r1, r2, r3}, func(key *dalrecord.Key) bool { return true }, func(ctx context.Context, records []dalrecord.Record) error {
			getMultiCalled = true
			for _, r := range records {
				r.SetError(nil)
			}
			return nil
		})

		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !getMultiCalled {
			t.Fatal("expected getMulti to be called")
		}
		if d1.Name != "Item1" {
			t.Fatalf("expected Item1, got %s", d1.Name)
		}
	})

	t.Run("get_multi_underlying_error", func(t *testing.T) {
		origGetMulti := memcacheGetMulti
		defer func() { memcacheGetMulti = origGetMulti }()
		memcacheGetMulti = func(ctx context.Context, keys []string) (map[string]*memcache.Item, error) {
			return nil, errors.New("memcache_err")
		}

		k1 := dalrecord.NewKeyWithID("kind", "id1")
		d1 := sampleData{}
		r1 := dalrecord.NewRecordWithData(k1, &d1)

		err := getMultiRecords(ctx, false, []dalrecord.Record{r1}, func(key *dalrecord.Key) bool { return true }, func(ctx context.Context, records []dalrecord.Record) error {
			return errors.New("db_get_multi_err")
		})
		if err == nil || !strings.Contains(err.Error(), "db_get_multi_err") {
			t.Fatalf("expected db_get_multi_err, got %v", err)
		}
	})

	t.Run("in_transaction_no_cache_get", func(t *testing.T) {
		origGetMulti := memcacheGetMulti
		defer func() { memcacheGetMulti = origGetMulti }()
		memcacheGetMulti = func(ctx context.Context, keys []string) (map[string]*memcache.Item, error) {
			t.Fatal("memcache.GetMulti must not be called when in transaction")
			return nil, nil
		}

		k1 := dalrecord.NewKeyWithID("kind", "id1")
		d1 := sampleData{}
		r1 := dalrecord.NewRecordWithData(k1, &d1)

		called := false
		err := getMultiRecords(ctx, true, []dalrecord.Record{r1}, func(key *dalrecord.Key) bool { return true }, func(ctx context.Context, records []dalrecord.Record) error {
			called = true
			records[0].SetError(nil)
			return nil
		})
		if err != nil || !called {
			t.Fatalf("expected nil err, called=true, got %v, %v", err, called)
		}
	})

	t.Run("setRecordsToCache_error", func(t *testing.T) {
		origSet := memcacheSet
		defer func() { memcacheSet = origSet }()
		memcacheSet = func(ctx context.Context, item *memcache.Item) error {
			return errors.New("memcache_set_failed")
		}

		k1 := dalrecord.NewKeyWithID("kind", "id1")
		d1 := sampleData{}
		r1 := dalrecord.NewRecordWithData(k1, &d1)

		err := getMultiRecords(ctx, true, []dalrecord.Record{r1}, func(key *dalrecord.Key) bool { return true }, func(ctx context.Context, records []dalrecord.Record) error {
			records[0].SetError(nil)
			return nil
		})
		if err == nil || !strings.Contains(err.Error(), "memcache_set_failed") {
			t.Fatalf("expected memcache_set_failed, got %v", err)
		}
	})
}

func TestSetRecordsToCache(t *testing.T) {
	ctx := context.Background()

	t.Run("single_item", func(t *testing.T) {
		origSet := memcacheSet
		defer func() { memcacheSet = origSet }()
		setCalled := false
		memcacheSet = func(ctx context.Context, item *memcache.Item) error {
			setCalled = true
			return nil
		}

		k1 := dalrecord.NewKeyWithID("kind", "id1")
		d1 := sampleData{Name: "Item1"}
		r1 := dalrecord.NewRecordWithData(k1, &d1)
		r1.SetError(nil)

		err := setRecordsToCache(ctx, []dalrecord.Record{r1}, "test", func(key *dalrecord.Key) bool { return true })
		if err != nil || !setCalled {
			t.Fatalf("expected nil err, setCalled=true, got %v, %v", err, setCalled)
		}
	})

	t.Run("multiple_items", func(t *testing.T) {
		origSetMulti := memcacheSetMulti
		defer func() { memcacheSetMulti = origSetMulti }()
		setMultiCalled := false
		memcacheSetMulti = func(ctx context.Context, items []*memcache.Item) error {
			setMultiCalled = true
			return nil
		}

		k1 := dalrecord.NewKeyWithID("kind", "id1")
		k2 := dalrecord.NewKeyWithID("kind", "id2")
		d1 := sampleData{Name: "Item1"}
		d2 := sampleData{Name: "Item2"}
		r1 := dalrecord.NewRecordWithData(k1, &d1)
		r2 := dalrecord.NewRecordWithData(k2, &d2)
		r1.SetError(nil)
		r2.SetError(nil)

		err := setRecordsToCache(ctx, []dalrecord.Record{r1, r2}, "test", func(key *dalrecord.Key) bool { return true })
		if err != nil || !setMultiCalled {
			t.Fatalf("expected nil err, setMultiCalled=true, got %v, %v", err, setMultiCalled)
		}
	})

	t.Run("not_cacheable_and_marshal_err", func(t *testing.T) {
		k1 := dalrecord.NewKeyWithID("kind", "id1")
		k2 := dalrecord.NewKeyWithID("kind", "id2")
		r1 := dalrecord.NewRecordWithData(k1, &sampleData{Name: "ok"})
		r2 := dalrecord.NewRecordWithData(k2, make(chan int))
		r1.SetError(nil)
		r2.SetError(nil)

		err := setRecordsToCache(ctx, []dalrecord.Record{r1, r2}, "test", func(key *dalrecord.Key) bool {
			return key.String() != k1.String() // k1 is not cacheable, k2 has marshal err
		})
		if err == nil {
			t.Fatal("expected error from json.Marshal, got nil")
		}
	})
}
