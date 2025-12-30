package storage

import (
	"fmt"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
	"github.com/jcalabro/atlas/internal/types"
	"google.golang.org/protobuf/proto"
)

type Store struct {
	db fdb.Database
}

func New(db fdb.Database) *Store {
	return &Store{db: db}
}

func transaction[T any](s *Store, fn func(tx fdb.Transaction) (T, error)) (T, error) {
	var t T

	resI, err := s.db.Transact(func(tx fdb.Transaction) (any, error) {
		return fn(tx)
	})
	if err != nil {
		return t, err
	}

	res, ok := resI.(T)
	if !ok {
		return t, fmt.Errorf("failed to cast transaction result %T to %T", resI, t)
	}

	return res, nil
}

func readTransaction[T any](s *Store, fn func(tx fdb.ReadTransaction) (T, error)) (T, error) {
	var t T

	resI, err := s.db.ReadTransact(func(tx fdb.ReadTransaction) (any, error) {
		return fn(tx)
	})
	if err != nil {
		return t, err
	}

	res, ok := resI.(T)
	if !ok {
		return t, fmt.Errorf("failed to cast read transaction result %T to %T", resI, t)
	}

	return res, nil
}

func (s *Store) PutRecord(rec *types.Record) error {
	key := fdb.Key(tuple.Tuple{"r", rec.Did, rec.Collection, rec.Rkey}.Pack())

	data, err := proto.Marshal(rec)
	if err != nil {
		return fmt.Errorf("marshal record: %w", err)
	}

	_, err = transaction(s, func(tx fdb.Transaction) (any, error) {
		tx.Set(key, data)
		return nil, nil
	})
	return err
}

func (s *Store) DeleteRecord(did, collection, rkey string) error {
	key := fdb.Key(tuple.Tuple{"r", did, collection, rkey}.Pack())

	_, err := transaction(s, func(tx fdb.Transaction) (any, error) {
		tx.Clear(key)
		return nil, nil
	})
	return err
}
