package foundation

import (
	"context"
	"encoding/binary"
	"fmt"
	"time"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/directory"
	"github.com/jcalabro/atlas/internal/at"
	"github.com/jcalabro/atlas/internal/metrics"
	"github.com/jcalabro/atlas/internal/types"
	"go.opentelemetry.io/otel/attribute"
	"google.golang.org/protobuf/proto"
)

func packURI(dir directory.DirectorySubspace, u *at.URI) fdb.Key {
	return pack(dir, u.Repo, u.Collection, u.Rkey)
}

// ValidateRecord checks that a record has all required fields
func ValidateRecord(r *types.Record) error {
	switch {
	case r == nil:
		return fmt.Errorf("record is nil")
	case r.Did == "":
		return fmt.Errorf("did is required")
	case r.Collection == "":
		return fmt.Errorf("collection is required")
	case r.Rkey == "":
		return fmt.Errorf("rkey is required")
	case r.Cid == "":
		return fmt.Errorf("cid is required")
	case len(r.Value) == 0:
		return fmt.Errorf("value is required")
	case r.CreatedAt == nil:
		return fmt.Errorf("created_at is required")
	}

	return nil
}

// SaveRecord stores a record in the database
func (db *DB) SaveRecord(ctx context.Context, record *types.Record) (err error) {
	start := time.Now()
	defer func() { observeOperation("SaveRecord", start, err) }()

	_, span := db.tracer.Start(ctx, "SaveRecord")
	defer span.End()

	span.SetAttributes(
		attribute.String("uri", record.URI().String()),
		attribute.String("cid", record.Cid),
		attribute.Int("size", len(record.Value)),
		attribute.String("created_at", metrics.FormatPBTime(record.CreatedAt)),
	)

	if err := ValidateRecord(record); err != nil {
		return fmt.Errorf("invalid record: %w", err)
	}

	_, err = transaction(db.db, func(tx fdb.Transaction) ([]byte, error) {
		return nil, db.saveRecordTx(tx, record)
	})

	return err
}

// saveRecordTx stores a record within an existing transaction
func (db *DB) saveRecordTx(tx fdb.Transaction, record *types.Record) error {
	buf, err := proto.Marshal(record)
	if err != nil {
		return fmt.Errorf("failed to marshal record: %w", err)
	}

	recordKey := packURI(
		db.records.records,
		&at.URI{Repo: record.Did, Collection: record.Collection, Rkey: record.Rkey},
	)

	tx.Set(recordKey, buf)
	return nil
}

// GetRecord retrieves a record by its AT URI
func (db *DB) GetRecord(ctx context.Context, uri string) (_ *types.Record, err error) {
	start := time.Now()
	defer func() { observeOperation("GetRecord", start, err) }()

	_, span := db.tracer.Start(ctx, "GetRecord")
	defer span.End()

	span.SetAttributes(attribute.String("uri", uri))

	aturi, err := at.ParseURI(uri)
	if err != nil {
		return nil, fmt.Errorf("invalid AT URI: %w", err)
	}

	key := packURI(db.records.records, aturi)

	var record types.Record
	ok, err := readProto(db.db, &record, func(tx fdb.ReadTransaction) ([]byte, error) {
		return tx.Get(key).Get()
	})
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, nil
	}

	return &record, nil
}

// DeleteRecordTx clears a record within an existing transaction.
func (db *DB) DeleteRecordTx(tx fdb.Transaction, uri *at.URI) {
	key := packURI(db.records.records, uri)
	tx.Clear(key)
}

// incrementCollectionCountTx atomically increments the collection count for a (did, collection) pair.
func (db *DB) incrementCollectionCountTx(tx fdb.Transaction, did, collection string) {
	key := pack(db.records.collectionCounts, did, collection)
	one := make([]byte, 8)
	binary.BigEndian.PutUint64(one, 1)
	tx.Add(key, one)
}

// decrementCollectionCountTx atomically decrements the collection count for a (did, collection) pair.
func (db *DB) decrementCollectionCountTx(tx fdb.Transaction, did, collection string) {
	key := pack(db.records.collectionCounts, did, collection)
	// -1 as uint64 in big-endian (two's complement)
	minusOne := make([]byte, 8)
	binary.BigEndian.PutUint64(minusOne, ^uint64(0))
	tx.Add(key, minusOne)
}

// GetCollections returns the list of distinct collection NSIDs for a DID.
// It reads from the collection counts secondary index for efficiency.
func (db *DB) GetCollections(ctx context.Context, did string) (_ []string, err error) {
	start := time.Now()
	defer func() { observeOperation("GetCollections", start, err) }()

	_, span := db.tracer.Start(ctx, "GetCollections")
	defer span.End()

	span.SetAttributes(attribute.String("did", did))

	return readTransaction(db.db, func(tx fdb.ReadTransaction) ([]string, error) {
		// range scan over the collection counts index for this DID
		rangeBegin := pack(db.records.collectionCounts, did)
		rangeEnd := pack(db.records.collectionCounts, did+"\xff")
		kr := fdb.KeyRange{Begin: rangeBegin, End: rangeEnd}

		var collections []string
		iter := tx.GetRange(kr, fdb.RangeOptions{}).Iterator()
		for iter.Advance() {
			kv, err := iter.Get()
			if err != nil {
				return nil, fmt.Errorf("failed to iterate collection counts: %w", err)
			}

			// extract collection from the key tuple (did, collection)
			tup, err := db.records.collectionCounts.Unpack(kv.Key)
			if err != nil {
				return nil, fmt.Errorf("failed to unpack collection count key: %w", err)
			}
			if len(tup) < 2 {
				continue
			}

			collection, ok := tup[1].(string)
			if !ok {
				continue
			}

			// only include collections with count > 0
			if len(kv.Value) == 8 {
				count := int64(binary.BigEndian.Uint64(kv.Value))
				if count > 0 {
					collections = append(collections, collection)
				}
			}
		}

		return collections, nil
	})
}
