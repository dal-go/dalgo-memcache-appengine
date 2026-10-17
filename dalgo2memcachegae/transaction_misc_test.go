package dalgo2memcachegae

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/dal-go/dalgo/dal"
	"github.com/dal-go/dalgo/recordset"
	dalrecord "github.com/dal-go/record"
	"github.com/dal-go/record/update"
	"google.golang.org/appengine/v2/memcache"
)

func TestTransaction_Methods(t *testing.T) {
	ctx := context.Background()

	ro := mockROTx{
		options: dal.NewTransactionOptions(),
		get: func(ctx context.Context, record dalrecord.Record) error {
			record.SetError(nil)
			return nil
		},
		exists: func(ctx context.Context, key *dalrecord.Key) (bool, error) {
			return true, nil
		},
		getMulti: func(ctx context.Context, records []dalrecord.Record) error {
			for _, r := range records {
				r.SetError(nil)
			}
			return nil
		},
		queryRecordsReader: func(ctx context.Context, query dal.Query) (dal.RecordsReader, error) {
			return nil, nil
		},
		queryRecordsetReader: func(ctx context.Context, query dal.Query, options ...recordset.Option) (dal.RecordsetReader, error) {
			return nil, nil
		},
	}
	rw := mockRWTx{
		mockROTx: ro,
		id:       "tx123",
	}

	tx := &transaction{
		ro:          ro,
		rw:          rw,
		isCacheable: func(key *dalrecord.Key) bool { return true },
	}

	if tx.ID() != "tx123" {
		t.Fatalf("expected tx123, got %s", tx.ID())
	}
	if tx.Options() == nil {
		t.Fatal("expected non-nil Options()")
	}

	k1 := dalrecord.NewKeyWithID("kind", "id1")
	r1 := dalrecord.NewRecordWithData(k1, &sampleData{Name: "data1"})

	if err := tx.Get(ctx, r1); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	exists, err := tx.Exists(ctx, k1)
	if err != nil || !exists {
		t.Fatalf("expected true, nil, got %v, %v", exists, err)
	}
	if err := tx.GetMulti(ctx, []dalrecord.Record{r1}); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}

	rr, err := tx.ExecuteQueryToRecordsReader(ctx, nil)
	if err != nil || rr != nil {
		t.Fatalf("expected nil, nil, got %v, %v", rr, err)
	}

	rsr, err := tx.ExecuteQueryToRecordsetReader(ctx, nil)
	if err != nil || rsr != nil {
		t.Fatalf("expected nil, nil, got %v, %v", rsr, err)
	}

	t.Run("Set_and_SetMulti", func(t *testing.T) {
		rw.set = func(ctx context.Context, record dalrecord.Record) error { return errors.New("set_err") }
		tx.rw = rw
		if err := tx.Set(ctx, r1); err == nil {
			t.Fatal("expected error")
		}

		rw.set = func(ctx context.Context, record dalrecord.Record) error { return nil }
		tx.rw = rw
		if err := tx.Set(ctx, r1); err != nil {
			t.Fatalf("unexpected err: %v", err)
		}

		rw.setMulti = func(ctx context.Context, records []dalrecord.Record) error { return errors.New("set_multi_err") }
		tx.rw = rw
		if err := tx.SetMulti(ctx, []dalrecord.Record{r1}); err == nil {
			t.Fatal("expected error")
		}

		rw.setMulti = func(ctx context.Context, records []dalrecord.Record) error { return nil }
		tx.rw = rw
		if err := tx.SetMulti(ctx, []dalrecord.Record{r1}); err != nil {
			t.Fatalf("unexpected err: %v", err)
		}
	})

	t.Run("Delete_and_DeleteMulti", func(t *testing.T) {
		rw.delete = func(ctx context.Context, key *dalrecord.Key) error { return errors.New("del_err") }
		tx.rw = rw
		if err := tx.Delete(ctx, k1); err == nil {
			t.Fatal("expected error")
		}

		rw.delete = func(ctx context.Context, key *dalrecord.Key) error { return nil }
		tx.rw = rw
		if err := tx.Delete(ctx, k1); err != nil {
			t.Fatalf("unexpected err: %v", err)
		}

		// Delete when not cacheable
		txUncached := &transaction{ro: ro, rw: rw, isCacheable: func(key *dalrecord.Key) bool { return false }}
		if err := txUncached.Delete(ctx, k1); err != nil {
			t.Fatalf("unexpected err: %v", err)
		}

		rw.deleteMulti = func(ctx context.Context, keys []*dalrecord.Key) error { return errors.New("del_multi_err") }
		tx.rw = rw
		if err := tx.DeleteMulti(ctx, []*dalrecord.Key{k1}); err == nil {
			t.Fatal("expected error")
		}

		rw.deleteMulti = func(ctx context.Context, keys []*dalrecord.Key) error { return nil }
		tx.rw = rw
		if err := tx.DeleteMulti(ctx, []*dalrecord.Key{k1}); err != nil {
			t.Fatalf("unexpected err: %v", err)
		}
		if err := txUncached.DeleteMulti(ctx, []*dalrecord.Key{k1}); err != nil {
			t.Fatalf("unexpected err: %v", err)
		}
	})

	t.Run("Update_and_UpdateRecord_and_UpdateMulti", func(t *testing.T) {
		rw.update = func(ctx context.Context, key *dalrecord.Key, updates []update.Update, preconditions ...dal.Precondition) error {
			return errors.New("upd_err")
		}
		tx.rw = rw
		if err := tx.Update(ctx, k1, nil); err == nil {
			t.Fatal("expected error")
		}

		rw.update = func(ctx context.Context, key *dalrecord.Key, updates []update.Update, preconditions ...dal.Precondition) error {
			return nil
		}
		tx.rw = rw
		if err := tx.Update(ctx, k1, nil); err != nil {
			t.Fatalf("unexpected err: %v", err)
		}

		rw.updateRecord = func(ctx context.Context, record dalrecord.Record, updates []update.Update, preconditions ...dal.Precondition) error {
			return errors.New("upd_rec_err")
		}
		tx.rw = rw
		if err := tx.UpdateRecord(ctx, r1, nil); err == nil {
			t.Fatal("expected error")
		}

		rw.updateRecord = func(ctx context.Context, record dalrecord.Record, updates []update.Update, preconditions ...dal.Precondition) error {
			return nil
		}
		tx.rw = rw
		if err := tx.UpdateRecord(ctx, r1, nil); err != nil {
			t.Fatalf("unexpected err: %v", err)
		}

		rw.updateMulti = func(ctx context.Context, keys []*dalrecord.Key, updates []update.Update, preconditions ...dal.Precondition) error {
			return errors.New("upd_multi_err")
		}
		tx.rw = rw
		if err := tx.UpdateMulti(ctx, []*dalrecord.Key{k1}, nil); err == nil {
			t.Fatal("expected error")
		}

		rw.updateMulti = func(ctx context.Context, keys []*dalrecord.Key, updates []update.Update, preconditions ...dal.Precondition) error {
			return nil
		}
		tx.rw = rw
		if err := tx.UpdateMulti(ctx, []*dalrecord.Key{k1}, nil); err != nil {
			t.Fatalf("unexpected err: %v", err)
		}

		txUncached := &transaction{ro: ro, rw: rw, isCacheable: func(key *dalrecord.Key) bool { return false }}
		if err := txUncached.UpdateMulti(ctx, []*dalrecord.Key{k1}, nil); err != nil {
			t.Fatalf("unexpected err: %v", err)
		}
	})

	t.Run("Insert_and_InsertMulti", func(t *testing.T) {
		rw.insert = func(ctx context.Context, record dalrecord.Record, opts ...dal.InsertOption) error {
			return errors.New("ins_err")
		}
		tx.rw = rw
		if err := tx.Insert(ctx, r1); err == nil {
			t.Fatal("expected error")
		}

		rw.insert = func(ctx context.Context, record dalrecord.Record, opts ...dal.InsertOption) error {
			return nil
		}
		tx.rw = rw
		if err := tx.Insert(ctx, r1); err != nil {
			t.Fatalf("unexpected err: %v", err)
		}

		rw.insertMulti = func(ctx context.Context, records []dalrecord.Record, opts ...dal.InsertOption) error {
			return errors.New("ins_multi_err")
		}
		tx.rw = rw
		if err := tx.InsertMulti(ctx, []dalrecord.Record{r1}); err == nil {
			t.Fatal("expected error")
		}

		rw.insertMulti = func(ctx context.Context, records []dalrecord.Record, opts ...dal.InsertOption) error {
			return nil
		}
		tx.rw = rw
		if err := tx.InsertMulti(ctx, []dalrecord.Record{r1}); err != nil {
			t.Fatalf("unexpected err: %v", err)
		}
	})

	t.Run("addRecordsForCaching_marshal_error_and_not_cacheable", func(t *testing.T) {
		badRec := dalrecord.NewRecordWithData(k1, make(chan int))
		tx.addRecordsForCaching(ctx, badRec)

		uncachedRec := dalrecord.NewRecordWithData(k1, &sampleData{Name: "uncached"})
		txUncached := &transaction{isCacheable: func(key *dalrecord.Key) bool { return false }}
		txUncached.addRecordsForCaching(ctx, uncachedRec)
		if len(txUncached.itemsForCaching) != 0 {
			t.Fatalf("expected 0 items, got %d", len(txUncached.itemsForCaching))
		}
	})
}

func TestTransaction_FlushItemsToCache(t *testing.T) {
	ctx := context.Background()

	t.Run("empty", func(t *testing.T) {
		tx := &transaction{}
		if err := tx.flushItemsToCache(ctx); err != nil {
			t.Fatalf("unexpected err: %v", err)
		}
	})

	t.Run("set_and_delete_success", func(t *testing.T) {
		origSetMulti := memcacheSetMulti
		origDelete := memcacheDelete
		origDeleteMulti := memcacheDeleteMulti
		defer func() {
			memcacheSetMulti = origSetMulti
			memcacheDelete = origDelete
			memcacheDeleteMulti = origDeleteMulti
		}()

		setMultiCalled := false
		deleteCalled := false
		memcacheSetMulti = func(ctx context.Context, items []*memcache.Item) error {
			setMultiCalled = true
			return nil
		}
		memcacheDelete = func(ctx context.Context, key string) error {
			deleteCalled = true
			return nil
		}

		tx := &transaction{
			itemsForCaching: []*memcache.Item{
				{Key: "k1", Value: []byte("v1")},
				{Key: "k1", Value: []byte("v1_latest")}, // duplicate key test
				{Key: "k2", Value: nil},                 // single key to delete
			},
		}

		err := tx.flushItemsToCache(ctx)
		if err != nil {
			t.Fatalf("unexpected err: %v", err)
		}
		if !setMultiCalled || !deleteCalled {
			t.Fatalf("expected setMultiCalled=true, deleteCalled=true, got %v, %v", setMultiCalled, deleteCalled)
		}
	})

	t.Run("set_fails_fallback_to_delete", func(t *testing.T) {
		origSetMulti := memcacheSetMulti
		origDeleteMulti := memcacheDeleteMulti
		defer func() {
			memcacheSetMulti = origSetMulti
			memcacheDeleteMulti = origDeleteMulti
		}()

		memcacheSetMulti = func(ctx context.Context, items []*memcache.Item) error {
			return errors.New("set_multi_fail")
		}
		deletedKeys := []string{}
		memcacheDeleteMulti = func(ctx context.Context, keys []string) error {
			deletedKeys = keys
			return nil
		}

		tx := &transaction{
			itemsForCaching: []*memcache.Item{
				{Key: "k1", Value: []byte("v1")},
				{Key: "k2", Value: []byte("v2")},
			},
		}

		err := tx.flushItemsToCache(ctx)
		if err != nil {
			t.Fatalf("unexpected err: %v", err)
		}
		if len(deletedKeys) != 2 {
			t.Fatalf("expected 2 keys deleted, got %d", len(deletedKeys))
		}
	})

	t.Run("delete_multi_error", func(t *testing.T) {
		origDeleteMulti := memcacheDeleteMulti
		defer func() { memcacheDeleteMulti = origDeleteMulti }()
		memcacheDeleteMulti = func(ctx context.Context, keys []string) error {
			return errors.New("del_multi_err")
		}

		tx := &transaction{
			itemsForCaching: []*memcache.Item{
				{Key: "k1", Value: nil},
				{Key: "k2", Value: nil},
			},
		}

		err := tx.flushItemsToCache(ctx)
		if err == nil || !strings.Contains(err.Error(), "failed to delete from memcache by 2 keys") {
			t.Fatalf("expected failed to delete error, got %v", err)
		}
	})
}
