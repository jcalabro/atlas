package foundation

import (
	"context"
	"fmt"

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
func (db *DB) SaveRecord(ctx context.Context, record *types.Record) error {
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

	_, err := transaction(db.db, func(tx fdb.Transaction) ([]byte, error) {
		if err := db.SaveRecordTx(tx, record); err != nil {
			return nil, err
		}
		return nil, nil
	})

	return err
}

// SaveRecordTx stores a record within an existing transaction.
func (db *DB) SaveRecordTx(tx fdb.Transaction, record *types.Record) error {
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
func (db *DB) GetRecord(ctx context.Context, uri string) (*types.Record, error) {
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

// DeleteRecord clears a record by its AT URI
func (db *DB) DeleteRecord(ctx context.Context, uri string) error {
	_, span := db.tracer.Start(ctx, "DeleteRecord")
	defer span.End()

	span.SetAttributes(attribute.String("uri", uri))

	aturi, err := at.ParseURI(uri)
	if err != nil {
		return fmt.Errorf("invalid AT URI: %w", err)
	}

	_, err = transaction(db.db, func(tx fdb.Transaction) ([]byte, error) {
		db.DeleteRecordTx(tx, aturi)
		return nil, nil
	})

	return err
}

// DeleteRecordTx clears a record within an existing transaction.
func (db *DB) DeleteRecordTx(tx fdb.Transaction, uri *at.URI) {
	key := packURI(db.records.records, uri)
	tx.Clear(key)
}
