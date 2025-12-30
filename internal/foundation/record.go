package foundation

import (
	"context"
	"fmt"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
	"github.com/jcalabro/atlas/internal/at"
	"github.com/jcalabro/atlas/internal/types"
	"google.golang.org/protobuf/proto"
)

// Writes a record to the database
func (db *DB) PutRecord(ctx context.Context, rec *types.Record) error {
	key := fdb.Key(tuple.Tuple{"r", rec.Did, rec.Collection, rec.Rkey}.Pack())

	data, err := proto.Marshal(rec)
	if err != nil {
		return fmt.Errorf("marshal record: %w", err)
	}

	_, err = transaction(db, func(tx fdb.Transaction) (any, error) {
		tx.Set(key, data)
		return nil, nil
	})
	return err
}

// Gets a record from the database by its URI
func (db *DB) GetRecord(uri at.URI) (*types.Record, error) {
	key := fdb.Key(tuple.Tuple{"r", uri.DID, uri.Collection, uri.Rkey}.Pack())

	buf, err := readTransaction(db, func(tx fdb.ReadTransaction) ([]byte, error) {
		return tx.Get(key).Get()
	})
	if err != nil {
		return nil, err
	}

	var record types.Record
	if err := proto.Unmarshal(buf, &record); err != nil {
		return nil, err
	}

	return &record, nil
}
