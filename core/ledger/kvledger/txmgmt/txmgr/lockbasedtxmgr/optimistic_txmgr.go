package lockbasedtxmgr

import (
	"github.com/hyperledger/fabric/core/ledger"
	"github.com/hyperledger/fabric/core/ledger/kvledger/bookkeeping"
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/privacyenabledstate"
	"github.com/hyperledger/fabric/core/ledger/pvtdatapolicy"
)

type OptimisticTxMgr struct {
	LockBasedTxMgr
}

func NewOptimisticTxMgr(ledgerid string, db privacyenabledstate.DB, stateListeners []ledger.StateListener,
	btlPolicy pvtdatapolicy.BTLPolicy, bookkeepingProvider bookkeeping.Provider, ccInfoProvider ledger.DeployedChaincodeInfoProvider) (*OptimisticTxMgr, error) {

	txmgr, err := NewLockBasedTxMgr(ledgerid, db, stateListeners, btlPolicy, bookkeepingProvider, ccInfoProvider)
	if err != nil {
		return nil, err
	}
	return &OptimisticTxMgr{*txmgr}, nil
}

func (txmgr *OptimisticTxMgr) NewQueryExecutor(txid string) (ledger.QueryExecutor, error) {
	qe := newOptimisticQueryExecutor(txmgr, txid, true)
	return qe, nil
}

func (txmgr *OptimisticTxMgr) NewQueryExecutorNoCollChecks() (ledger.QueryExecutor, error) {
	qe := newOptimisticQueryExecutor(txmgr, "", false)
	return qe, nil
}

func (txmgr *OptimisticTxMgr) NewTxSimulator(txid string) (ledger.TxSimulator, error) {
	logger.Debugf("constructing new optimistic tx simulator")
	s, err := newOptimisticTxSimulator(txmgr, txid)
	if err != nil {
		return nil, err
	}
	return s, nil
}
