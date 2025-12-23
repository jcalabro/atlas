package storage

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
)

type Store struct {
	db fdb.Database
}

func New(db fdb.Database) *Store {
	return &Store{db: db}
}

type StoredRecord struct {
	CID       string          `json:"cid"`
	Record    json.RawMessage `json:"record"`
	IndexedAt int64           `json:"indexed_at"`
}

type StoredIdentity struct {
	Handle   string `json:"handle"`
	IsActive bool   `json:"is_active"`
	Status   string `json:"status"`
}

func (s *Store) PutRecord(did, collection, rkey, cid string, record json.RawMessage) error {
	key := fdb.Key(tuple.Tuple{"r", did, collection, rkey}.Pack())
	val := StoredRecord{
		CID:       cid,
		Record:    record,
		IndexedAt: time.Now().Unix(),
	}
	data, err := json.Marshal(val)
	if err != nil {
		return fmt.Errorf("marshal record: %w", err)
	}
	_, err = s.db.Transact(func(tx fdb.Transaction) (any, error) {
		tx.Set(key, data)
		return nil, nil
	})
	return err
}

func (s *Store) DeleteRecord(did, collection, rkey string) error {
	key := fdb.Key(tuple.Tuple{"r", did, collection, rkey}.Pack())
	_, err := s.db.Transact(func(tx fdb.Transaction) (any, error) {
		tx.Clear(key)
		return nil, nil
	})
	return err
}

func (s *Store) GetRecord(did, collection, rkey string) (*StoredRecord, error) {
	key := fdb.Key(tuple.Tuple{"r", did, collection, rkey}.Pack())
	val, err := s.db.Transact(func(tx fdb.Transaction) (any, error) {
		return tx.Get(key).Get()
	})
	if err != nil {
		return nil, fmt.Errorf("get record: %w", err)
	}
	data, ok := val.([]byte)
	if !ok || len(data) == 0 {
		return nil, nil
	}
	var rec StoredRecord
	if err := json.Unmarshal(data, &rec); err != nil {
		return nil, fmt.Errorf("unmarshal record: %w", err)
	}
	return &rec, nil
}

func (s *Store) PutIdentity(did, handle, status string, isActive bool) error {
	key := fdb.Key(tuple.Tuple{"i", did}.Pack())
	val := StoredIdentity{
		Handle:   handle,
		IsActive: isActive,
		Status:   status,
	}
	data, err := json.Marshal(val)
	if err != nil {
		return fmt.Errorf("marshal identity: %w", err)
	}
	_, err = s.db.Transact(func(tx fdb.Transaction) (any, error) {
		tx.Set(key, data)
		return nil, nil
	})
	return err
}

func (s *Store) GetIdentity(did string) (*StoredIdentity, error) {
	key := fdb.Key(tuple.Tuple{"i", did}.Pack())
	val, err := s.db.Transact(func(tx fdb.Transaction) (any, error) {
		return tx.Get(key).Get()
	})
	if err != nil {
		return nil, fmt.Errorf("get identity: %w", err)
	}
	data, ok := val.([]byte)
	if !ok || len(data) == 0 {
		return nil, nil
	}
	var ident StoredIdentity
	if err := json.Unmarshal(data, &ident); err != nil {
		return nil, fmt.Errorf("unmarshal identity: %w", err)
	}
	return &ident, nil
}
