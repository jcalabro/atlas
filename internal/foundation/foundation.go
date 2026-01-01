package foundation

import (
	"context"
	"fmt"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/directory"
	"go.opentelemetry.io/otel/trace"
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

	actors directory.DirectorySubspace
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
		return nil, fmt.Errorf("failed to create redis directory: %w", err)
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
