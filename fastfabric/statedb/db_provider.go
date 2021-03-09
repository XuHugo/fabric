package statedb

import (
	"github.com/pkg/errors"
	"github.com/syndtr/goleveldb/leveldb/iterator"
	"sync"
)

var dbNameKeySep = []byte{0x00}
var lastKeyIndicator = byte(0x01)

type Provider struct {
	db *DB
	dbHandles map[string]*DBHandle
	mux sync.Mutex
}

func NewProvider(dbPath string) *Provider{
	var p = &Provider{dbHandles: make(map[string]*DBHandle),mux: sync.Mutex{}}
	p.db = createDB()
	return p
}

func (p *Provider) GetDBHandle(dbName string) *DBHandle{
	p.mux.Lock()
	defer p.mux.Unlock()

	if p.dbHandles[dbName] == nil{
		p.dbHandles[dbName] = &DBHandle{dbName: dbName, db: p.db}
	}
	return p.dbHandles[dbName]
}

func (p *Provider) Close(){
	p.db.Close()
}

type DBHandle struct {
	dbName string
	db *DB
}

func (h *DBHandle) Get(key []byte) ([]byte, error){
	val, err := h.db.Get(constructLevelKey(h.dbName, key))
	if err == KeyNotFound{
		return nil, nil
	}
	if err != nil{
		return nil, errors.Wrapf(err, "error retrieving key [%#v]", key)
	}
	return val, nil
}

func (h *DBHandle) Put(key []byte, value []byte, sync bool) error{
	return h.db.Put(constructLevelKey(h.dbName, key), value, sync)
}

func (h *DBHandle) Delete(key []byte, sync bool) error{
	return h.db.Delete(constructLevelKey(h.dbName, key), sync)
}

func (h *DBHandle) WriteBatch(batch *UpdateBatch, sync bool) error{
	for k, v := range batch.KVs{
		key := constructLevelKey(h.dbName, []byte(k))
		if v == nil{
			h.db.Delete(key, true)
		}else{
			h.db.Put(key, v,  true)
		}
	}
	return nil
}

func (h *DBHandle) GetIterator(startKey []byte, endKey []byte) *Iterator{
	sKey := constructLevelKey(h.dbName, startKey)
	eKey := constructLevelKey(h.dbName, endKey)
	if endKey == nil{
		eKey[len(eKey)-1] = lastKeyIndicator
	}
	return &Iterator{Iterator:h.db.GetIterator(sKey, eKey)}
}

type UpdateBatch struct {
	KVs map[string][]byte
}

func NewUpdateBatch() *UpdateBatch{
	return &UpdateBatch{KVs: make(map[string][]byte)}
}

func (batch *UpdateBatch) Put(key []byte, value []byte){
	batch.KVs[string(key)]=value
}

func (batch *UpdateBatch) Delete(key []byte){
	batch.KVs[string(key)] = nil
}

func (batch *UpdateBatch)Len() int{
	return len(batch.KVs)
}

type Iterator struct {
	iterator.Iterator
}

func constructLevelKey(dbName string, key []byte) []byte{
	return append(append([]byte(dbName), dbNameKeySep...), key...)
}