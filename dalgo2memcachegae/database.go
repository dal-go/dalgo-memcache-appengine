package dalgo2memcachegae

import (
	"context"

	"github.com/dal-go/dalgo/dal"
	"github.com/dal-go/dalgo/recordset"

	dalrecord "github.com/dal-go/record"
	"reflect"
)

func isNil(i interface{}) bool {
	if i == nil {
		return true
	}
	return reflect.ValueOf(i).IsNil()
}

func NewDB(db dal.DB, isCacheable func(key *dalrecord.Key) bool) dal.DB {
	if isNil(db) {
		panic("db is nil")
	}
	return &database{DB: db, isCacheable: isCacheable}
}

// database decorates a dal.DB with a memcache read-through layer.
//
// dal.DB is embedded rather than held in a named field so that the
// unexported marker method that seals dal.DB (see dal.NewDB) is promoted
// onto *database — since dalgo v0.64 that promotion is the only way a
// decorator outside package dal can satisfy dal.DB. Every method this type
// declares below still overrides the embedded one.
type database struct {
	dal.DB
	// isCacheable returns true if the key is cacheable
	isCacheable func(key *dalrecord.Key) bool
}

func (v database) ID() string {
	return "cloud.google.com/go/memcache/apiv1"
}

func (v database) Adapter() dal.Adapter {
	return v.DB.Adapter()
}

func (v database) SupportsConcurrentConnections() bool {
	return v.DB.SupportsConcurrentConnections()
}

func (v database) Schema() dal.Schema {
	return nil
}

func (v database) RunReadonlyTransaction(ctx context.Context, f dal.ROTxWorker, options ...dal.TransactionOption) error {
	return v.DB.RunReadonlyTransaction(ctx, func(ctx context.Context, tx dal.ReadTransaction) error {
		return f(ctx, &transaction{ro: tx, isCacheable: v.isCacheable})
	}, options...)
}

func (v database) RunReadwriteTransaction(ctx context.Context, f dal.RWTxWorker, options ...dal.TransactionOption) (err error) {
	var t *transaction
	err = v.DB.RunReadwriteTransaction(ctx, func(ctx context.Context, tx dal.ReadwriteTransaction) error {
		t = &transaction{ro: tx, rw: tx, isCacheable: v.isCacheable}
		return f(ctx, t)
	}, options...)
	if err == nil {
		if err = t.flushItemsToCache(ctx); err != nil {
			return err
		}
	}
	return err
}

func (v database) GetMulti(ctx context.Context, records []dalrecord.Record) error {
	return getMultiRecords(ctx, false, records, v.isCacheable, v.DB.GetMulti)
}

func (v database) ExecuteQueryToRecordsReader(ctx context.Context, query dal.Query) (dal.RecordsReader, error) {
	if ctx == nil {
		panic("ctx is nil")
	}
	if query == nil {
		panic("query is nil")
	}
	return nil, dal.ErrNotSupported
}

func (v database) ExecuteQueryToRecordsetReader(ctx context.Context, query dal.Query, options ...recordset.Option) (dal.RecordsetReader, error) {
	if ctx == nil {
		panic("ctx is nil")
	}
	if query == nil {
		panic("query is nil")
	}
	_ = recordset.NewOptions(options...)
	return nil, dal.ErrNotSupported
}

func (v database) Get(ctx context.Context, record dalrecord.Record) (err error) {
	return getRecord(ctx, false, record, "db", v.isCacheable, v.DB.Get)
}

func (v database) Exists(ctx context.Context, key *dalrecord.Key) (exists bool, err error) {
	return existsByKey(ctx, key, "db", v.isCacheable, v.DB.Exists)
}
