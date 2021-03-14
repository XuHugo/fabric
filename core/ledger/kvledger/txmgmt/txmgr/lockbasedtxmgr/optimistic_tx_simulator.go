package lockbasedtxmgr

import (
	commonledger "github.com/hyperledger/fabric/common/ledger"
	"github.com/hyperledger/fabric/core/ledger"
)

type optimisticTxSimulator struct {
	lockBasedTxSimulator
	optimisticQueryExecutor
}

func newOptimisticTxSimulator(txmgr *OptimisticTxMgr, txid string) (*optimisticTxSimulator, error) {
	logger.Debugf("constructing new optimistic tx simulator txid = [%s]", txid)

	lockBasedTxSimulator, err := newLockBasedTxSimulator(&txmgr.LockBasedTxMgr, txid)
	if err != nil {
		return nil, err
	}
	height, err := txmgr.GetLastSavepoint()
	if err != nil {
		return nil, err
	}
	lockBasedTxSimulator.helper.height = height
	txsim := &optimisticTxSimulator{
		lockBasedTxSimulator:    *lockBasedTxSimulator,
		optimisticQueryExecutor: optimisticQueryExecutor{lockBasedTxSimulator.lockBasedQueryExecutor, txmgr},
	}
	return txsim, nil
}

func (s *optimisticTxSimulator) GetPrivateDataRangeScanIterator(namespace, collection, startKey, endKey string) (commonledger.ResultsIterator, error) {
	if err := s.checkBeforePvtdataQueries(); err != nil {
		return nil, err
	}
	return s.optimisticQueryExecutor.GetPrivateDataRangeScanIterator(namespace, collection, startKey, endKey)
}

func (s *optimisticTxSimulator) ExecuteQueryOnPrivateData(namespace, collection, query string) (commonledger.ResultsIterator, error) {
	if err := s.checkBeforePvtdataQueries(); err != nil {
		return nil, err
	}
	return s.optimisticQueryExecutor.ExecuteQueryOnPrivateData(namespace, collection, query)
}

func (s *optimisticTxSimulator) GetStateRangeScanIteratorWithMetadata(namespace string, startKey string, endKey string, metadata map[string]interface{}) (ledger.QueryResultsIterator, error) {
	if err := s.checkBeforePaginatedQueries(); err != nil {
		return nil, err
	}
	return s.optimisticQueryExecutor.GetStateRangeScanIteratorWithMetadata(namespace, startKey, endKey, metadata)
}

func (s *optimisticTxSimulator) ExecuteQueryWithMetadata(namespace, query string, metadata map[string]interface{}) (ledger.QueryResultsIterator, error) {
	if err := s.checkBeforePaginatedQueries(); err != nil {
		return nil, err
	}
	return s.optimisticQueryExecutor.ExecuteQueryWithMetadata(namespace, query, metadata)
}
