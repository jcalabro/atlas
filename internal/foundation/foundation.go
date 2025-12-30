package foundation

import (
	"fmt"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
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
	db     fdb.Database
}

func New(tracer trace.Tracer, cfg Config) (*DB, error) {
	if err := fdb.APIVersion(cfg.APIVersion); err != nil {
		return nil, fmt.Errorf("failed to set fdb client api version: %w", err)
	}

	db, err := fdb.OpenDatabase(cfg.ClusterFile)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize fdb client from cluster file %q: %w", cfg.ClusterFile, err)
	}

	if err := db.Options().SetTransactionTimeout(5000); err != nil { // milliseconds
		return nil, fmt.Errorf("failed to set fdb transaction timeout: %w", err)
	}

	if err := db.Options().SetTransactionRetryLimit(100); err != nil {
		return nil, fmt.Errorf("failed to set fdb transaction retry limit: %w", err)
	}

	_, err = db.ReadTransact(func(tx fdb.ReadTransaction) (any, error) {
		return tx.Get(fdb.Key("PING")).Get()
	})
	if err != nil {
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	return &DB{tracer: tracer, db: db}, nil
}

// Executes the anonymous function as a write transaction, then attempts to cast the return type
func transaction[T any](s *DB, fn func(tx fdb.Transaction) (T, error)) (T, error) {
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

// Executes the anonymous function as a read transaction, then attempts to cast the return type
func readTransaction[T any](s *DB, fn func(tx fdb.ReadTransaction) (T, error)) (T, error) {
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
