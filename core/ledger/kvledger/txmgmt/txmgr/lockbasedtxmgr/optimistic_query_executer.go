package lockbasedtxmgr

import (
	commonledger "github.com/hyperledger/fabric/common/ledger"
	"github.com/hyperledger/fabric/core/ledger"
)

type optimisticQueryExecutor struct {
	lockBasedQueryExecutor
	txmgr *OptimisticTxMgr
}

func newOptimisticQueryExecutor(txmgr *OptimisticTxMgr, txid string, performCollCheck bool) *optimisticQueryExecutor {
	logger.Debugf("constructing new optimistic query executor txid = [%s]", txid)
	helper := newQueryHelper(&txmgr.LockBasedTxMgr, nil, performCollCheck)
	return &optimisticQueryExecutor{
		lockBasedQueryExecutor: lockBasedQueryExecutor{helper, txid},
		txmgr:                  txmgr,
	}
}

func (q *optimisticQueryExecutor) GetState(ns string, key string) (val []byte, err error) {
	q.txmgr.commitRWLock.RLock()
	defer q.txmgr.commitRWLock.RUnlock()
	return q.lockBasedQueryExecutor.GetState(ns, key)
}

func (q *optimisticQueryExecutor) GetStateMetadata(namespace, key string) (map[string][]byte, error) {
	q.txmgr.commitRWLock.RLock()
	defer q.txmgr.commitRWLock.RUnlock()
	return q.lockBasedQueryExecutor.GetStateMetadata(namespace, key)
}

func (q *optimisticQueryExecutor) GetStateMultipleKeys(namespace string, keys []string) ([][]byte, error) {
	q.txmgr.commitRWLock.RLock()
	defer q.txmgr.commitRWLock.RUnlock()
	return q.lockBasedQueryExecutor.GetStateMultipleKeys(namespace, keys)
}

func (q *optimisticQueryExecutor) GetStateRangeScanIterator(namespace string, startKey string, endKey string) (commonledger.ResultsIterator, error) {
	q.txmgr.commitRWLock.RLock()
	defer q.txmgr.commitRWLock.RUnlock()
	return q.lockBasedQueryExecutor.GetStateRangeScanIterator(namespace, startKey, endKey)
}

func (q *optimisticQueryExecutor) GetStateRangeScanIteratorWithMetadata(namespace string, startKey string, endKey string, metadata map[string]interface{}) (ledger.QueryResultsIterator, error) {
	q.txmgr.commitRWLock.RLock()
	defer q.txmgr.commitRWLock.RUnlock()
	return q.lockBasedQueryExecutor.GetStateRangeScanIteratorWithMetadata(namespace, startKey, endKey, metadata)
}

func (q *optimisticQueryExecutor) ExecuteQuery(namespace, query string) (commonledger.ResultsIterator, error) {
	q.txmgr.commitRWLock.RLock()
	defer q.txmgr.commitRWLock.RUnlock()
	return q.lockBasedQueryExecutor.ExecuteQuery(namespace, query)
}

func (q *optimisticQueryExecutor) ExecuteQueryWithMetadata(namespace, query string, metadata map[string]interface{}) (ledger.QueryResultsIterator, error) {
	q.txmgr.commitRWLock.RLock()
	defer q.txmgr.commitRWLock.RUnlock()
	return q.lockBasedQueryExecutor.ExecuteQueryWithMetadata(namespace, query, metadata)
}

func (q *optimisticQueryExecutor) GetPrivateData(namespace, collection, key string) ([]byte, error) {
	q.txmgr.commitRWLock.RLock()
	defer q.txmgr.commitRWLock.RUnlock()
	return q.lockBasedQueryExecutor.GetPrivateData(namespace, collection, key)
}

func (q *optimisticQueryExecutor) GetPrivateDataHash(namespace, collection, key string) ([]byte, error) {
	q.txmgr.commitRWLock.RLock()
	defer q.txmgr.commitRWLock.RUnlock()
	return q.lockBasedQueryExecutor.GetPrivateDataHash(namespace, collection, key)
}

func (q *optimisticQueryExecutor) GetPrivateDataMetadata(namespace, collection, key string) (map[string][]byte, error) {
	q.txmgr.commitRWLock.RLock()
	defer q.txmgr.commitRWLock.RUnlock()
	return q.lockBasedQueryExecutor.GetPrivateDataMetadata(namespace, collection, key)
}

func (q *optimisticQueryExecutor) GetPrivateDataMetadataByHash(namespace, collection string, keyhash []byte) (map[string][]byte, error) {
	q.txmgr.commitRWLock.RLock()
	defer q.txmgr.commitRWLock.RUnlock()
	return q.lockBasedQueryExecutor.GetPrivateDataMetadataByHash(namespace, collection, keyhash)
}

func (q *optimisticQueryExecutor) GetPrivateDataMultipleKeys(namespace, collection string, keys []string) ([][]byte, error) {
	q.txmgr.commitRWLock.RLock()
	defer q.txmgr.commitRWLock.RUnlock()
	return q.lockBasedQueryExecutor.GetPrivateDataMultipleKeys(namespace, collection, keys)
}

func (q *optimisticQueryExecutor) GetPrivateDataRangeScanIterator(namespace, collection, startKey, endKey string) (commonledger.ResultsIterator, error) {
	q.txmgr.commitRWLock.RLock()
	defer q.txmgr.commitRWLock.RUnlock()
	return q.lockBasedQueryExecutor.GetPrivateDataRangeScanIterator(namespace, collection, startKey, endKey)
}

func (q *optimisticQueryExecutor) ExecuteQueryOnPrivateData(namespace, collection, query string) (commonledger.ResultsIterator, error) {
	q.txmgr.commitRWLock.RLock()
	defer q.txmgr.commitRWLock.RUnlock()
	return q.lockBasedQueryExecutor.ExecuteQueryOnPrivateData(namespace, collection, query)
}

func (q *optimisticQueryExecutor) Done() {
	logger.Debugf("Done with optimistic transaction simulation / query execution [%s]", q.txid)

	h := q.helper
	if h.doneInvoked {
		return
	}

	defer func() {
		h.doneInvoked = true
		for _, itr := range h.itrs {
			itr.Close()
		}
	}()
}