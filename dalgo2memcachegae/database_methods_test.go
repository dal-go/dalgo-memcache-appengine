package dalgo2memcachegae

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/dal-go/dalgo/dal"
	dalrecord "github.com/dal-go/record"
)

func TestDatabase_Methods(t *testing.T) {
	ctx := context.Background()
	underlying := mockUnderlyingDB{
		adapter:    dal.NewAdapter("test_adapter", "v1"),
		concurrent: true,
	}
	db := &database{
		DB:          underlying,
		isCacheable: func(key *dalrecord.Key) bool { return true },
	}

	if db.ID() != "cloud.google.com/go/memcache/apiv1" {
		t.Fatalf("expected cloud.google.com/go/memcache/apiv1, got %s", db.ID())
	}
	if db.Adapter().Name() != "test_adapter" {
		t.Fatalf("expected test_adapter, got %s", db.Adapter().Name())
	}
	if !db.SupportsConcurrentConnections() {
		t.Fatal("expected SupportsConcurrentConnections to be true")
	}
	if db.Schema() != nil {
		t.Fatal("expected Schema() to be nil")
	}

	t.Run("RunReadonlyTransaction", func(t *testing.T) {
		called := false
		underlying.runROTx = func(ctx context.Context, f dal.ROTxWorker, options ...dal.TransactionOption) error {
			return f(ctx, mockROTx{})
		}
		db.DB = underlying
		err := db.RunReadonlyTransaction(ctx, func(ctx context.Context, tx dal.ReadTransaction) error {
			called = true
			return nil
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !called {
			t.Fatal("expected tx worker to be called")
		}
	})

	t.Run("RunReadwriteTransaction_error", func(t *testing.T) {
		underlying.runRWTx = func(ctx context.Context, f dal.RWTxWorker, options ...dal.TransactionOption) error {
			return errors.New("rw_err")
		}
		db.DB = underlying
		err := db.RunReadwriteTransaction(ctx, func(ctx context.Context, tx dal.ReadwriteTransaction) error {
			return nil
		})
		if err == nil {
			t.Fatal("expected error")
		}
	})

	t.Run("RunReadwriteTransaction_flush_error", func(t *testing.T) {
		origDelete := memcacheDelete
		defer func() { memcacheDelete = origDelete }()
		memcacheDelete = func(ctx context.Context, key string) error {
			return errors.New("delete_failed")
		}

		underlying.runRWTx = func(ctx context.Context, f dal.RWTxWorker, options ...dal.TransactionOption) error {
			t := &transaction{ro: mockROTx{}, rw: mockRWTx{}, isCacheable: func(key *dalrecord.Key) bool { return true }}
			if err := f(ctx, t); err != nil {
				return err
			}
			return nil
		}
		db.DB = underlying
		err := db.RunReadwriteTransaction(ctx, func(ctx context.Context, tx dal.ReadwriteTransaction) error {
			return tx.Delete(ctx, dalrecord.NewKeyWithID("kind", "id1"))
		})
		if err == nil || !strings.Contains(err.Error(), "failed to delete from memcache") {
			t.Fatalf("expected failed to delete from memcache, got %v", err)
		}
	})

	t.Run("RunReadwriteTransaction_success", func(t *testing.T) {
		underlying.runRWTx = func(ctx context.Context, f dal.RWTxWorker, options ...dal.TransactionOption) error {
			t := &transaction{ro: mockROTx{}, rw: mockRWTx{}, isCacheable: func(key *dalrecord.Key) bool { return true }}
			return f(ctx, t)
		}
		db.DB = underlying
		err := db.RunReadwriteTransaction(ctx, func(ctx context.Context, tx dal.ReadwriteTransaction) error {
			return nil
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("ExecuteQueryToRecordsReader", func(t *testing.T) {
		assertPanics(t, func() {
			_, _ = db.ExecuteQueryToRecordsReader(nil, nil)
		})
		assertPanics(t, func() {
			_, _ = db.ExecuteQueryToRecordsReader(ctx, nil)
		})
		query := mockQuery{}
		res, err := db.ExecuteQueryToRecordsReader(ctx, query)
		if res != nil || err != dal.ErrNotSupported {
			t.Fatalf("expected nil, ErrNotSupported, got %v, %v", res, err)
		}
	})

	t.Run("ExecuteQueryToRecordsetReader", func(t *testing.T) {
		assertPanics(t, func() {
			_, _ = db.ExecuteQueryToRecordsetReader(nil, nil)
		})
		assertPanics(t, func() {
			_, _ = db.ExecuteQueryToRecordsetReader(ctx, nil)
		})
		query := mockQuery{}
		res, err := db.ExecuteQueryToRecordsetReader(ctx, query)
		if res != nil || err != dal.ErrNotSupported {
			t.Fatalf("expected nil, ErrNotSupported, got %v, %v", res, err)
		}
	})

	t.Run("Get_and_Exists_and_GetMulti", func(t *testing.T) {
		underlying.get = func(ctx context.Context, record dalrecord.Record) error {
			return nil
		}
		underlying.exists = func(ctx context.Context, key *dalrecord.Key) (bool, error) {
			return true, nil
		}
		underlying.getMulti = func(ctx context.Context, records []dalrecord.Record) error {
			return nil
		}
		db.DB = underlying

		key := dalrecord.NewKeyWithID("kind", "id1")
		var val string
		rec := dalrecord.NewRecordWithData(key, &val)

		err := db.Get(ctx, rec)
		if err != nil {
			t.Fatalf("unexpected err: %v", err)
		}

		exists, err := db.Exists(ctx, key)
		if err != nil || !exists {
			t.Fatalf("expected true, nil, got %v, %v", exists, err)
		}

		err = db.GetMulti(ctx, []dalrecord.Record{rec})
		if err != nil {
			t.Fatalf("unexpected err: %v", err)
		}
	})
}

func assertPanics(t *testing.T, f func()) {
	t.Helper()
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic")
		}
	}()
	f()
}
