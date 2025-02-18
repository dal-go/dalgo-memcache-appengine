package memcache4dalgo

import (
	"context"
	"github.com/dal-go/dalgo/dal"
)

func NewDB(db dal.DB) dal.DB {
	return database{db: db}
}

type database struct {
	db dal.DB
}

func (v database) ID() string {
	return "cloud.google.com/go/memcache/apiv1"
}

func (v database) Adapter() dal.Adapter {
	return v.db.Adapter()
}

func (v database) RunReadonlyTransaction(ctx context.Context, f dal.ROTxWorker, options ...dal.TransactionOption) error {
	return v.db.RunReadonlyTransaction(ctx, func(ctx context.Context, tx dal.ReadTransaction) error {
		return f(ctx, transaction{ro: tx})
	}, options...)
}

func (v database) RunReadwriteTransaction(ctx context.Context, f dal.RWTxWorker, options ...dal.TransactionOption) error {
	return v.db.RunReadwriteTransaction(ctx, func(ctx context.Context, tx dal.ReadwriteTransaction) error {
		return f(ctx, transaction{ro: tx, rw: tx})
	}, options...)
}

func (v database) GetMulti(ctx context.Context, records []dal.Record) error {
	return getMultiRecords(ctx, records, v.db.GetMulti)
}

func (v database) QueryReader(ctx context.Context, query dal.Query) (dal.Reader, error) {
	return v.db.QueryReader(ctx, query)
}

func (v database) QueryAllRecords(ctx context.Context, query dal.Query) (records []dal.Record, err error) {
	return v.db.QueryAllRecords(ctx, query)
}

func (v database) Get(ctx context.Context, record dal.Record) (err error) {
	return getRecord(ctx, record, v.db.Get)
}
