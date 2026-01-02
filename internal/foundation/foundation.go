package foundation

import (
	"context"
	"fmt"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/directory"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/protobuf/proto"
)

// Options for configuring the FDB client
type Config struct {
	ClusterFile string
	APIVersion  int
}

// DB allows the caller to query FDB for saving and retrieving data
type DB struct {
	tracer trace.Tracer
	db     *fdb.Database

	actors       directory.DirectorySubspace
	didsByEmail  directory.DirectorySubspace
	didsByHandle directory.DirectorySubspace
}

func New(tracer trace.Tracer, cfg Config) (*DB, error) {
	if err := fdb.APIVersion(cfg.APIVersion); err != nil {
		return nil, fmt.Errorf("failed to set fdb client api version: %w", err)
	}

	d, err := fdb.OpenDatabase(cfg.ClusterFile)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize fdb client from cluster file %q: %w", cfg.ClusterFile, err)
	}

	db := &DB{tracer: tracer, db: &d}

	if err := db.db.Options().SetTransactionTimeout(5000); err != nil { // milliseconds
		return nil, fmt.Errorf("failed to set fdb transaction timeout: %w", err)
	}

	if err := db.db.Options().SetTransactionRetryLimit(100); err != nil {
		return nil, fmt.Errorf("failed to set fdb transaction retry limit: %w", err)
	}

	_, err = db.db.ReadTransact(func(tx fdb.ReadTransaction) (any, error) {
		return tx.Get(fdb.Key("PING")).Get()
	})
	if err != nil {
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	db.actors, err = directory.CreateOrOpen(db.db, []string{"actors"}, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create actors directory: %w", err)
	}

	db.didsByEmail, err = directory.CreateOrOpen(db.db, []string{"dids_by_email"}, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create dids_by_email directory: %w", err)
	}

	db.didsByHandle, err = directory.CreateOrOpen(db.db, []string{"dids_by_handle"}, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create dids_by_handle directory: %w", err)
	}

	return db, nil
}

// Pings the database to ensure we have connectivity
func (db *DB) Ping(ctx context.Context) error {
	_, span := db.tracer.Start(ctx, "Ping")
	defer span.End()

	_, err := readTransaction(db.db, func(tx fdb.ReadTransaction) ([]byte, error) {
		return tx.Get(fdb.Key("PING")).Get()
	})

	return err
}

// Executes the anonymous function as a write transaction, then attempts to cast the return type
func transaction[T any](db *fdb.Database, fn func(tx fdb.Transaction) (T, error)) (T, error) {
	var t T

	resI, err := db.Transact(func(tx fdb.Transaction) (any, error) {
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

// Executes the anonymous function as a read transaction, then attempts to cast the return type
func readTransaction[T any](db *fdb.Database, fn func(tx fdb.ReadTransaction) (T, error)) (T, error) {
	var t T

	resI, err := db.ReadTransact(func(tx fdb.ReadTransaction) (any, error) {
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

// Executes the given anonymous function as a read transaction, then attempts to protobuf unmarshal
// the resulting `[]byte` in to the given `item`. Returns `false` if the item does not exist in the db.
func readProto(db *fdb.Database, item proto.Message, fn func(tx fdb.ReadTransaction) ([]byte, error)) (bool, error) {
	buf, err := readTransaction(db, fn)
	if err != nil {
		return false, err
	}
	if len(buf) == 0 {
		return false, nil // not found
	}

	if err := proto.Unmarshal(buf, item); err != nil {
		return false, err
	}

	return true, nil
}

func pack(dir directory.DirectorySubspace, keys ...tuple.TupleElement) fdb.Key {
	tup := tuple.Tuple(keys)
	if dir == nil {
		return fdb.Key(tup.Pack())
	}
	return dir.Pack(tup)
}
