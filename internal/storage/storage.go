package storage

import (
	"fmt"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
	"github.com/jcalabro/atlas/internal/at"
	"github.com/jcalabro/atlas/pkg/atlas"
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

func (s *Store) PutRecord(rec *atlas.Record) error {
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

func (s *Store) GetRecords(uris []at.URI) ([]*atlas.Record, error) {
	bufs, err := readTransaction(s, func(tx fdb.ReadTransaction) ([][]byte, error) {
		futures := make([]fdb.FutureByteSlice, 0, len(uris))
		for _, uri := range uris {
			key := tuple.Tuple{"r", uri.DID, uri.Collection, uri.Rkey}.Pack()
			futures = append(futures, tx.Get(fdb.Key(key)))
		}

		results := make([][]byte, 0, len(futures))
		for _, future := range futures {
			val, err := future.Get()
			if err != nil {
				return nil, err
			}
			results = append(results, val)
		}

		return results, nil
	})
	if err != nil {
		return nil, fmt.Errorf("get records: %w", err)
	}

	records := make([]*atlas.Record, 0, len(uris))
	for _, buf := range bufs {
		if len(buf) == 0 {
			continue
		}
		var rec atlas.Record
		if err := proto.Unmarshal(buf, &rec); err != nil {
			return nil, fmt.Errorf("unmarshal record: %w", err)
		}
		records = append(records, &rec)
	}

	return records, nil
}

func (s *Store) PutActor(actor *atlas.Actor) error {
	key := fdb.Key(tuple.Tuple{"a", actor.Did}.Pack())

	data, err := proto.Marshal(actor)
	if err != nil {
		return fmt.Errorf("marshal actor: %w", err)
	}

	_, err = transaction(s, func(tx fdb.Transaction) (any, error) {
		tx.Set(key, data)
		return nil, nil
	})
	return err
}

func (s *Store) GetActors(dids []string) ([]*atlas.Actor, error) {
	bufs, err := readTransaction(s, func(tx fdb.ReadTransaction) ([][]byte, error) {
		futures := make([]fdb.FutureByteSlice, 0, len(dids))
		for _, did := range dids {
			key := tuple.Tuple{"a", did}.Pack()
			futures = append(futures, tx.Get(fdb.Key(key)))
		}

		results := make([][]byte, 0, len(futures))
		for _, future := range futures {
			val, err := future.Get()
			if err != nil {
				return nil, err
			}
			results = append(results, val)
		}

		return results, nil
	})
	if err != nil {
		return nil, fmt.Errorf("get actors: %w", err)
	}

	actors := make([]*atlas.Actor, 0, len(dids))
	for _, buf := range bufs {
		if len(buf) == 0 {
			continue
		}
		var actor atlas.Actor
		if err := proto.Unmarshal(buf, &actor); err != nil {
			return nil, fmt.Errorf("unmarshal actor: %w", err)
		}
		actors = append(actors, &actor)
	}

	return actors, nil
}
