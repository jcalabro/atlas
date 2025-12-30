package storage

import (
	"fmt"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
	"github.com/jcalabro/atlas/internal/at"
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

func (s *Store) GetRecords(uris []at.URI) ([]*types.Record, error) {
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

	records := make([]*types.Record, 0, len(uris))
	for _, buf := range bufs {
		if len(buf) == 0 {
			continue
		}
		var rec types.Record
		if err := proto.Unmarshal(buf, &rec); err != nil {
			return nil, fmt.Errorf("unmarshal record: %w", err)
		}
		records = append(records, &rec)
	}

	return records, nil
}

func (s *Store) ListRecords(did, collection string, limit int, cursor string) ([]*types.Record, string, error) {
	if limit <= 0 {
		limit = 100
	}

	prefix := tuple.Tuple{"r", did, collection}

	// Transaction only does FDB I/O, returns raw bytes
	bufs, err := readTransaction(s, func(tx fdb.ReadTransaction) ([][]byte, error) {
		var keyRange fdb.KeyRange
		if cursor == "" {
			var err error
			keyRange, err = fdb.PrefixRange(prefix.Pack())
			if err != nil {
				return nil, fmt.Errorf("prefix range: %w", err)
			}
		} else {
			// Cursor is the rkey - build key to scan before it
			prefixRange, err := fdb.PrefixRange(prefix.Pack())
			if err != nil {
				return nil, fmt.Errorf("prefix range: %w", err)
			}
			cursorKey := tuple.Tuple{"r", did, collection, cursor}.Pack()
			keyRange = fdb.KeyRange{
				Begin: prefixRange.Begin,
				End:   fdb.Key(cursorKey),
			}
		}

		rangeResult := tx.GetRange(keyRange, fdb.RangeOptions{
			Limit:   limit,
			Reverse: true,
		})

		iter := rangeResult.Iterator()
		var results [][]byte
		for iter.Advance() {
			kv, err := iter.Get()
			if err != nil {
				return nil, err
			}
			results = append(results, kv.Value)
		}

		return results, nil
	})
	if err != nil {
		return nil, "", fmt.Errorf("list records: %w", err)
	}

	// Unmarshal outside transaction
	records := make([]*types.Record, 0, len(bufs))
	for _, buf := range bufs {
		if len(buf) == 0 {
			continue
		}
		var rec types.Record
		if err := proto.Unmarshal(buf, &rec); err != nil {
			return nil, "", fmt.Errorf("unmarshal record: %w", err)
		}
		records = append(records, &rec)
	}

	var nextCursor string
	if len(records) == limit {
		nextCursor = records[len(records)-1].Rkey
	}

	return records, nextCursor, nil
}

func (s *Store) PutActor(actor *types.Actor) error {
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

func (s *Store) GetActors(dids []string) ([]*types.Actor, error) {
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

	actors := make([]*types.Actor, 0, len(dids))
	for _, buf := range bufs {
		if len(buf) == 0 {
			continue
		}
		var actor types.Actor
		if err := proto.Unmarshal(buf, &actor); err != nil {
			return nil, fmt.Errorf("unmarshal actor: %w", err)
		}
		actors = append(actors, &actor)
	}

	return actors, nil
}
