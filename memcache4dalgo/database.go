package memcache4dalgo

import (
	"context"

	"github.com/dal-go/dalgo/dal"
	"github.com/dal-go/dalgo/recordset"

	"reflect"
)

func isNil(i interface{}) bool {
	if i == nil {
		return true
	}
	return reflect.ValueOf(i).IsNil()
}

func NewDB(db dal.DB, isCacheable func(key *dal.Key) bool) dal.DB {
	if isNil(db) {
		panic("db is nil")
	}
	return &database{db: db, isCacheable: isCacheable}
}

type database struct {
	db dal.DB
	// isCacheable returns true if the key is cacheable
	isCacheable func(key *dal.Key) bool
}

func (v database) GetRecordsetReader(ctx context.Context, query dal.Query, rs *recordset.Recordset) (dal.RecordsetReader, error) {
	//TODO implement me
	panic("implement me")
}

func (v database) ID() string {
	return "cloud.google.com/go/memcache/apiv1"
}

func (v database) Adapter() dal.Adapter {
	return v.db.Adapter()
}

func (v database) Schema() dal.Schema {
	return nil
}

func (v database) RunReadonlyTransaction(ctx context.Context, f dal.ROTxWorker, options ...dal.TransactionOption) error {
	return v.db.RunReadonlyTransaction(ctx, func(ctx context.Context, tx dal.ReadTransaction) error {
		return f(ctx, &transaction{ro: tx, isCacheable: v.isCacheable})
	}, options...)
}

func (v database) RunReadwriteTransaction(ctx context.Context, f dal.RWTxWorker, options ...dal.TransactionOption) (err error) {
	var t *transaction
	err = v.db.RunReadwriteTransaction(ctx, func(ctx context.Context, tx dal.ReadwriteTransaction) error {
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

func (v database) GetMulti(ctx context.Context, records []dal.Record) error {
	return getMultiRecords(ctx, false, records, v.isCacheable, v.db.GetMulti)
}

func (v database) GetRecordsReader(ctx context.Context, query dal.Query) (dal.RecordsReader, error) {
	return v.db.GetRecordsReader(ctx, query)
}

func (v database) Get(ctx context.Context, record dal.Record) (err error) {
	return getRecord(ctx, false, record, "db", v.isCacheable, v.db.Get)
}

func (v database) Exists(ctx context.Context, key *dal.Key) (exists bool, err error) {
	return existsByKey(ctx, key, "db", v.isCacheable, v.db.Exists)
}
