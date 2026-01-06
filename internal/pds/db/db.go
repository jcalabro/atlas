package db

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/directory"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
	"github.com/jcalabro/atlas/internal/metrics"
	pdsmetrics "github.com/jcalabro/atlas/internal/pds/metrics"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/protobuf/proto"
)

// ErrNotFound is returned when a requested item does not exist in the database
var ErrNotFound = errors.New("not found")

// Options for configuring the FDB client
type Config struct {
	ClusterFile string
	APIVersion  int
}

// DB allows the caller to query FDB for saving and retrieving data
type DB struct {
	tracer trace.Tracer
	db     *fdb.Database

	// The collection of users on each logical PDS and its secondary indicies
	actors actors

	// Records stored in user repos
	records records

	// IPLD blocks for MST and record storage
	blockDir blockDir

	// Firehose events for subscribeRepos
	eventDir eventDir
}

type actors struct {
	// Primary index. Actors are keyed by DID (globally unique)
	actors directory.DirectorySubspace

	// Secondary index. Handles are globally unique since they resolve to a single DID
	didsByHandle directory.DirectorySubspace

	// Secondary index. Emails are keyed by (pds_host, email) for per-PDS uniqueness
	didsByEmail directory.DirectorySubspace

	// Secondary index. Allows listing actors by PDS host
	didsByHost directory.DirectorySubspace

	// Stores the last TID integer value per repo (did) for monotonic generation
	tidsByDID directory.DirectorySubspace
}

type records struct {
	// Primary index. Records are keyed by (did, collection, rkey)
	records directory.DirectorySubspace

	// Secondary index. Tracks count of records per collection per DID.
	// Key: (did, collection), Value: int64 count (little-endian)
	collectionCounts directory.DirectorySubspace
}

type blockDir struct {
	// Primary index. Blocks are keyed by (did, cid)
	blocks directory.DirectorySubspace

	// Secondary index. Blocks keyed by (did, rev, cid) for incremental sync.
	// Value is empty - this is just for querying which CIDs were added in each rev.
	blocksByRev directory.DirectorySubspace
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

	db.actors.actors, err = directory.CreateOrOpen(db.db, []string{"actors"}, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create actors directory: %w", err)
	}

	db.actors.didsByHandle, err = directory.CreateOrOpen(db.db, []string{"dids_by_handle"}, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create dids_by_handle directory: %w", err)
	}

	db.actors.didsByEmail, err = directory.CreateOrOpen(db.db, []string{"dids_by_email"}, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create dids_by_email directory: %w", err)
	}

	db.actors.didsByHost, err = directory.CreateOrOpen(db.db, []string{"dids_by_host"}, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create dids_by_host directory: %w", err)
	}

	db.actors.tidsByDID, err = directory.CreateOrOpen(db.db, []string{"tids_by_did"}, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create tids_last directory: %w", err)
	}

	db.records.records, err = directory.CreateOrOpen(db.db, []string{"records"}, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create records directory: %w", err)
	}

	db.records.collectionCounts, err = directory.CreateOrOpen(db.db, []string{"collection_counts"}, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create collection_counts directory: %w", err)
	}

	db.blockDir.blocks, err = directory.CreateOrOpen(db.db, []string{"blocks"}, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create blocks directory: %w", err)
	}

	db.blockDir.blocksByRev, err = directory.CreateOrOpen(db.db, []string{"blocks_by_rev"}, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create blocks_by_rev directory: %w", err)
	}

	if err := db.initEventDirs(); err != nil {
		return nil, err
	}

	return db, nil
}

// Pings the database to ensure we have connectivity
func (db *DB) Ping(ctx context.Context) (err error) {
	_, _, done := db.observe(ctx, "Ping")
	defer func() { done(err) }()

	_, err = readTransaction(db.db, func(tx fdb.ReadTransaction) ([]byte, error) {
		return tx.Get(fdb.Key("PING")).Get()
	})

	return
}

// Transact runs the given function within a FDB transaction.
// Use this for operations that need to atomically update multiple items.
func (db *DB) Transact(fn func(tx fdb.Transaction) error) error {
	_, err := transaction(db.db, func(tx fdb.Transaction) (any, error) {
		if err := fn(tx); err != nil {
			return nil, err
		}
		return nil, nil
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

	// handle nil result (common when function only has side effects)
	if resI == nil {
		return t, nil
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

	if resI == nil {
		return t, ErrNotFound
	}

	res, ok := resI.(T)
	if !ok {
		return t, fmt.Errorf("failed to cast read transaction result %T to %T", resI, t)
	}

	return res, nil
}

// Executes the given anonymous function as a read transaction, then attempts to protobuf unmarshal
// the resulting `[]byte` in to the given `item`. Returns `ErrNotFound` if the item does not exist in the db.
func readProto(db *fdb.Database, item proto.Message, fn func(fdb.ReadTransaction) ([]byte, error)) error {
	buf, err := readTransaction(db, fn)
	if err != nil {
		return err
	}
	if len(buf) == 0 {
		return ErrNotFound
	}

	return proto.Unmarshal(buf, item)
}

// Records prometheus metrics and OTEL span status for foundation queries.
// The returned `done` function must be called to end the span and record metrics.
func (db *DB) observe(ctx context.Context, name string) (context.Context, trace.Span, func(error)) {
	ctx, span := db.tracer.Start(ctx, name)
	start := time.Now()

	return ctx, span, func(err error) {
		defer span.End()

		var status string
		switch {
		case err == nil:
			status = metrics.StatusOK
			span.SetStatus(codes.Ok, "")
		case errors.Is(err, ErrNotFound):
			status = metrics.StatusNotFound
			span.SetStatus(codes.Ok, "not found")
		default:
			status = metrics.StatusError
			span.RecordError(err)
		}

		pdsmetrics.Queries.WithLabelValues(name, status).Inc()
		pdsmetrics.QueryDuration.WithLabelValues(name, status).Observe(time.Since(start).Seconds())
	}
}

func pack(dir directory.DirectorySubspace, keys ...tuple.TupleElement) fdb.Key {
	tup := tuple.Tuple(keys)
	if dir == nil {
		return fdb.Key(tup.Pack())
	}
	return dir.Pack(tup)
}

func atLeastOneByteSlice(bufs [][]byte) bool {
	for _, buf := range bufs {
		if len(buf) > 0 {
			return true
		}
	}

	return false // all byte slices are empty (or len zero)
}
