package dalgo2memcachegae

import (
	"context"

	"github.com/dal-go/dalgo/dal"
	"github.com/dal-go/dalgo/recordset"
	dalrecord "github.com/dal-go/record"
	"github.com/dal-go/record/update"
	"google.golang.org/appengine/v2/memcache"
)

func init() {
	resetMemcacheSeams()
}

func resetMemcacheSeams() {
	memcacheGet = func(ctx context.Context, key string) (*memcache.Item, error) {
		return nil, memcache.ErrCacheMiss
	}
	memcacheSet = func(ctx context.Context, item *memcache.Item) error {
		return nil
	}
	memcacheGetMulti = func(ctx context.Context, keys []string) (map[string]*memcache.Item, error) {
		return map[string]*memcache.Item{}, nil
	}
	memcacheSetMulti = func(ctx context.Context, items []*memcache.Item) error {
		return nil
	}
	memcacheDelete = func(ctx context.Context, key string) error {
		return nil
	}
	memcacheDeleteMulti = func(ctx context.Context, keys []string) error {
		return nil
	}
}

type mockUnderlyingDB struct {
	dal.DB
	adapter    dal.Adapter
	concurrent bool
	runROTx    func(ctx context.Context, f dal.ROTxWorker, options ...dal.TransactionOption) error
	runRWTx    func(ctx context.Context, f dal.RWTxWorker, options ...dal.TransactionOption) error
	getMulti   func(ctx context.Context, records []dalrecord.Record) error
	get        func(ctx context.Context, record dalrecord.Record) error
	exists     func(ctx context.Context, key *dalrecord.Key) (bool, error)
}

func (m mockUnderlyingDB) Adapter() dal.Adapter {
	if m.adapter != nil {
		return m.adapter
	}
	return dal.NewAdapter("mock", "v1")
}

func (m mockUnderlyingDB) SupportsConcurrentConnections() bool {
	return m.concurrent
}

func (m mockUnderlyingDB) RunReadonlyTransaction(ctx context.Context, f dal.ROTxWorker, options ...dal.TransactionOption) error {
	if m.runROTx != nil {
		return m.runROTx(ctx, f, options...)
	}
	return nil
}

func (m mockUnderlyingDB) RunReadwriteTransaction(ctx context.Context, f dal.RWTxWorker, options ...dal.TransactionOption) error {
	if m.runRWTx != nil {
		return m.runRWTx(ctx, f, options...)
	}
	return nil
}

func (m mockUnderlyingDB) GetMulti(ctx context.Context, records []dalrecord.Record) error {
	if m.getMulti != nil {
		return m.getMulti(ctx, records)
	}
	return nil
}

func (m mockUnderlyingDB) Get(ctx context.Context, record dalrecord.Record) error {
	if m.get != nil {
		return m.get(ctx, record)
	}
	return nil
}

func (m mockUnderlyingDB) Exists(ctx context.Context, key *dalrecord.Key) (bool, error) {
	if m.exists != nil {
		return m.exists(ctx, key)
	}
	return false, nil
}

type mockROTx struct {
	dal.ReadTransaction
	options              dal.TransactionOptions
	get                  func(ctx context.Context, record dalrecord.Record) error
	exists               func(ctx context.Context, key *dalrecord.Key) (bool, error)
	getMulti             func(ctx context.Context, records []dalrecord.Record) error
	queryRecordsReader   func(ctx context.Context, query dal.Query) (dal.RecordsReader, error)
	queryRecordsetReader func(ctx context.Context, query dal.Query, options ...recordset.Option) (dal.RecordsetReader, error)
}

func (m mockROTx) Options() dal.TransactionOptions {
	return m.options
}

func (m mockROTx) Get(ctx context.Context, record dalrecord.Record) error {
	if m.get != nil {
		return m.get(ctx, record)
	}
	return nil
}

func (m mockROTx) Exists(ctx context.Context, key *dalrecord.Key) (bool, error) {
	if m.exists != nil {
		return m.exists(ctx, key)
	}
	return false, nil
}

func (m mockROTx) GetMulti(ctx context.Context, records []dalrecord.Record) error {
	if m.getMulti != nil {
		return m.getMulti(ctx, records)
	}
	return nil
}

func (m mockROTx) ExecuteQueryToRecordsReader(ctx context.Context, query dal.Query) (dal.RecordsReader, error) {
	if m.queryRecordsReader != nil {
		return m.queryRecordsReader(ctx, query)
	}
	return nil, nil
}

func (m mockROTx) ExecuteQueryToRecordsetReader(ctx context.Context, query dal.Query, options ...recordset.Option) (dal.RecordsetReader, error) {
	if m.queryRecordsetReader != nil {
		return m.queryRecordsetReader(ctx, query, options...)
	}
	return nil, nil
}

type mockRWTx struct {
	mockROTx
	id           string
	set          func(ctx context.Context, record dalrecord.Record) error
	setMulti     func(ctx context.Context, records []dalrecord.Record) error
	delete       func(ctx context.Context, key *dalrecord.Key) error
	deleteMulti  func(ctx context.Context, keys []*dalrecord.Key) error
	update       func(ctx context.Context, key *dalrecord.Key, updates []update.Update, preconditions ...dal.Precondition) error
	updateRecord func(ctx context.Context, record dalrecord.Record, updates []update.Update, preconditions ...dal.Precondition) error
	updateMulti  func(ctx context.Context, keys []*dalrecord.Key, updates []update.Update, preconditions ...dal.Precondition) error
	insert       func(ctx context.Context, record dalrecord.Record, opts ...dal.InsertOption) error
	insertMulti  func(ctx context.Context, records []dalrecord.Record, opts ...dal.InsertOption) error
}

func (m mockRWTx) ID() string {
	return m.id
}

func (m mockRWTx) Set(ctx context.Context, record dalrecord.Record) error {
	if m.set != nil {
		return m.set(ctx, record)
	}
	return nil
}

func (m mockRWTx) SetMulti(ctx context.Context, records []dalrecord.Record) error {
	if m.setMulti != nil {
		return m.setMulti(ctx, records)
	}
	return nil
}

func (m mockRWTx) Delete(ctx context.Context, key *dalrecord.Key) error {
	if m.delete != nil {
		return m.delete(ctx, key)
	}
	return nil
}

func (m mockRWTx) DeleteMulti(ctx context.Context, keys []*dalrecord.Key) error {
	if m.deleteMulti != nil {
		return m.deleteMulti(ctx, keys)
	}
	return nil
}

func (m mockRWTx) Update(ctx context.Context, key *dalrecord.Key, updates []update.Update, preconditions ...dal.Precondition) error {
	if m.update != nil {
		return m.update(ctx, key, updates, preconditions...)
	}
	return nil
}

func (m mockRWTx) UpdateRecord(ctx context.Context, record dalrecord.Record, updates []update.Update, preconditions ...dal.Precondition) error {
	if m.updateRecord != nil {
		return m.updateRecord(ctx, record, updates, preconditions...)
	}
	return nil
}

func (m mockRWTx) UpdateMulti(ctx context.Context, keys []*dalrecord.Key, updates []update.Update, preconditions ...dal.Precondition) error {
	if m.updateMulti != nil {
		return m.updateMulti(ctx, keys, updates, preconditions...)
	}
	return nil
}

func (m mockRWTx) Insert(ctx context.Context, record dalrecord.Record, opts ...dal.InsertOption) error {
	if m.insert != nil {
		return m.insert(ctx, record, opts...)
	}
	return nil
}

func (m mockRWTx) InsertMulti(ctx context.Context, records []dalrecord.Record, opts ...dal.InsertOption) error {
	if m.insertMulti != nil {
		return m.insertMulti(ctx, records, opts...)
	}
	return nil
}

type mockQuery struct{}

func (m mockQuery) String() string { return "mockQuery" }
func (m mockQuery) Offset() int    { return 0 }
func (m mockQuery) Limit() int     { return 0 }
func (m mockQuery) GetRecordsReader(ctx context.Context, qe dal.QueryExecutor) (dal.RecordsReader, error) {
	return nil, nil
}
func (m mockQuery) GetRecordsetReader(ctx context.Context, qe dal.QueryExecutor) (dal.RecordsetReader, error) {
	return nil, nil
}
