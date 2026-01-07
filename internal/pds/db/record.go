package db

import (
	"context"
	"encoding/binary"
	"fmt"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/directory"
	"github.com/jcalabro/atlas/internal/at"
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
	_, span, done := db.observe(ctx, "SaveRecord")
	defer func() { done(err) }()

	span.SetAttributes(
		attribute.String("uri", record.URI().String()),
		attribute.String("cid", record.Cid),
		attribute.Int("size", len(record.Value)),
	)

	if err = ValidateRecord(record); err != nil {
		err = fmt.Errorf("invalid record: %w", err)
		return
	}

	_, err = transaction(db.db, func(tx fdb.Transaction) ([]byte, error) {
		return nil, db.saveRecordTx(tx, record)
	})

	return
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
func (db *DB) GetRecord(ctx context.Context, uri string) (record *types.Record, err error) {
	_, span, done := db.observe(ctx, "GetRecord")
	defer func() { done(err) }()

	span.SetAttributes(attribute.String("uri", uri))

	aturi, err := at.ParseURI(uri)
	if err != nil {
		err = fmt.Errorf("invalid AT URI: %w", err)
		return
	}

	key := packURI(db.records.records, aturi)

	var r types.Record
	err = readProto(db.db, &r, func(tx fdb.ReadTransaction) ([]byte, error) {
		return tx.Get(key).Get()
	})
	if err != nil {
		return
	}

	record = &r
	return
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

// ListRecordsResult contains the result of listing records in a collection.
type ListRecordsResult struct {
	Records []*types.Record
	Cursor  string
}

// ListRecords returns records in a collection for a given DID.
// Supports pagination via cursor and can be reversed.
func (db *DB) ListRecords(
	ctx context.Context,
	did string,
	collection string,
	limit int,
	cursor string,
	reverse bool,
) (result *ListRecordsResult, err error) {
	_, span, done := db.observe(ctx, "ListRecords")
	defer func() { done(err) }()

	span.SetAttributes(
		attribute.String("did", did),
		attribute.String("collection", collection),
		attribute.Int("limit", limit),
		attribute.String("cursor", cursor),
		attribute.Bool("reverse", reverse),
	)

	result, err = readTransaction(db.db, func(tx fdb.ReadTransaction) (*ListRecordsResult, error) {
		var rangeBegin, rangeEnd fdb.Key

		if cursor == "" {
			// no cursor - start from beginning or end of collection
			rangeBegin = pack(db.records.records, did, collection)
			rangeEnd = pack(db.records.records, did, collection+"\xff")
		} else {
			// cursor is the rkey of the last record seen
			if reverse {
				// for reverse, get records with rkey < cursor
				rangeBegin = pack(db.records.records, did, collection)
				rangeEnd = pack(db.records.records, did, collection, cursor)
			} else {
				// for forward, get records with rkey > cursor
				rangeBegin = pack(db.records.records, did, collection, cursor+"\x00")
				rangeEnd = pack(db.records.records, did, collection+"\xff")
			}
		}

		kr := fdb.KeyRange{Begin: rangeBegin, End: rangeEnd}
		opts := fdb.RangeOptions{
			Limit:   limit + 1, // fetch one extra to detect if there are more results
			Reverse: reverse,
		}

		var records []*types.Record
		iter := tx.GetRange(kr, opts).Iterator()
		for iter.Advance() {
			kv, err := iter.Get()
			if err != nil {
				return nil, fmt.Errorf("failed to iterate records: %w", err)
			}

			var record types.Record
			if err := proto.Unmarshal(kv.Value, &record); err != nil {
				return nil, fmt.Errorf("failed to unmarshal record: %w", err)
			}

			records = append(records, &record)
		}

		// determine cursor for next page
		var nextCursor string
		if len(records) > limit {
			// there are more results - set cursor to last returned record's rkey
			records = records[:limit]
			nextCursor = records[len(records)-1].Rkey
		}

		return &ListRecordsResult{
			Records: records,
			Cursor:  nextCursor,
		}, nil
	})

	return
}

// GetCollections returns the list of distinct collection NSIDs for a DID.
// It reads from the collection counts secondary index for efficiency.
func (db *DB) GetCollections(ctx context.Context, did string) (collections []string, err error) {
	_, span, done := db.observe(ctx, "GetCollections")
	defer func() { done(err) }()

	span.SetAttributes(attribute.String("did", did))

	collections, err = readTransaction(db.db, func(tx fdb.ReadTransaction) ([]string, error) {
		rangeBegin := pack(db.records.collectionCounts, did)
		rangeEnd := pack(db.records.collectionCounts, did+"\xff")
		kr := fdb.KeyRange{Begin: rangeBegin, End: rangeEnd}

		var result []string
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
					result = append(result, collection)
				}
			}
		}

		return result, nil
	})

	return
}
