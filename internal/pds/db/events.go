package db

import (
	"context"
	"encoding/binary"
	"fmt"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/directory"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
	"github.com/jcalabro/atlas/internal/types"
	"go.opentelemetry.io/otel/attribute"
	"google.golang.org/protobuf/proto"
)

// eventDir holds the FDB directory subspaces for event storage
type eventDir struct {
	// Primary index: events keyed by versionstamp for global ordering
	// Key: (versionstamp), Value: serialized RepoEvent
	events directory.DirectorySubspace

	// Secondary index: events by PDS host for filtering
	// Key: (pds_host, versionstamp), Value: empty
	eventsByHost directory.DirectorySubspace

	// Stores the latest versionstamp for watch notifications
	// Key: "latest", Value: latest versionstamp (8 bytes)
	latestSeq directory.DirectorySubspace
}

const (
	// versionstampLength is the length of an FDB versionstamp (10 bytes)
	// 8 bytes for commit version + 2 bytes for batch order
	versionstampLength = 10

	// latestSeqKey is the key used to store the latest sequence number
	latestSeqKey = "latest"
)

// initEventDirs initializes the event directory subspaces
func (db *DB) initEventDirs() error {
	var err error

	db.eventDir.events, err = directory.CreateOrOpen(db.db, []string{"events"}, nil)
	if err != nil {
		return fmt.Errorf("failed to create events directory: %w", err)
	}

	db.eventDir.eventsByHost, err = directory.CreateOrOpen(db.db, []string{"events_by_host"}, nil)
	if err != nil {
		return fmt.Errorf("failed to create events_by_host directory: %w", err)
	}

	db.eventDir.latestSeq, err = directory.CreateOrOpen(db.db, []string{"latest_seq"}, nil)
	if err != nil {
		return fmt.Errorf("failed to create latest_seq directory: %w", err)
	}

	return nil
}

// WriteEventTx writes a repo event to the events subspace within an existing transaction.
// The event's sequence number will be assigned by FDB's versionstamp at commit time.
// This should be called as part of the same transaction that performs the repo mutation.
func (db *DB) WriteEventTx(tx fdb.Transaction, event *types.RepoEvent) error {
	// serialize the event (seq will be 0, filled in by reader from key)
	eventBytes, err := proto.Marshal(event)
	if err != nil {
		return fmt.Errorf("failed to marshal event: %w", err)
	}

	// create a key with versionstamp placeholder
	// the format for SetVersionstampedKey is: prefix + placeholder + suffix
	// where placeholder is 14 bytes (10 byte versionstamp + 4 byte offset)
	prefix := db.eventDir.events.Bytes()

	// versionstamp placeholder: 10 zero bytes followed by 4-byte little-endian offset
	// the offset tells FDB where in the key to place the versionstamp (relative to start)
	placeholder := make([]byte, 14)
	binary.LittleEndian.PutUint32(placeholder[10:], uint32(len(prefix)))

	key := append(prefix, placeholder...)
	tx.SetVersionstampedKey(fdb.Key(key), eventBytes)

	// write secondary index by host
	hostPrefix := db.eventDir.eventsByHost.Pack(tuple.Tuple{event.PdsHost})
	hostPlaceholder := make([]byte, 14)
	binary.LittleEndian.PutUint32(hostPlaceholder[10:], uint32(len(hostPrefix)))
	hostKey := append(hostPrefix, hostPlaceholder...)
	tx.SetVersionstampedKey(fdb.Key(hostKey), nil)

	// update the latest sequence marker (for watch notifications)
	// use SetVersionstampedValue so watchers can detect new events
	latestKey := db.eventDir.latestSeq.Pack(tuple.Tuple{latestSeqKey})
	latestPlaceholder := make([]byte, 14)
	// offset 0 since we want the versionstamp at the start of the value
	tx.SetVersionstampedValue(latestKey, latestPlaceholder)

	return nil
}

// WriteIdentityEvent writes an identity event to the events subspace.
// This is used for identity events that happen outside of repo mutations.
func (db *DB) WriteIdentityEvent(ctx context.Context, event *types.RepoEvent) (err error) {
	_, _, done := db.observe(ctx, "WriteIdentityEvent")
	defer func() { done(err) }()

	_, err = db.db.Transact(func(tx fdb.Transaction) (any, error) {
		return nil, db.WriteEventTx(tx, event)
	})
	return err
}

// GetEventsSince retrieves events starting from (but not including) the given cursor.
// If cursor is nil, retrieves from the beginning.
// Returns events and the cursor for the last event returned.
func (db *DB) GetEventsSince(ctx context.Context, cursor []byte, limit int) (events []*types.RepoEvent, nextCursor []byte, err error) {
	_, span, done := db.observe(ctx, "GetEventsSince")
	defer func() { done(err) }()

	span.SetAttributes(
		attribute.Int("limit", limit),
		attribute.Int("cursor_len", len(cursor)),
	)

	type result struct {
		events     []*types.RepoEvent
		nextCursor []byte
	}

	res, err := readTransaction(db.db, func(tx fdb.ReadTransaction) (*result, error) {
		// determine start key
		var startKey fdb.Key
		if len(cursor) == 0 {
			// start from beginning of events subspace
			startKey = db.eventDir.events.FDBKey()
		} else {
			// start after the cursor (exclusive)
			// cursor is the raw versionstamp bytes
			startKey = fdb.Key(append(db.eventDir.events.Bytes(), cursor...))
			startKey = append(startKey, 0x00) // make it exclusive by adding a byte
		}

		// end key is the end of the events subspace
		endKey := fdb.Key(append(db.eventDir.events.Bytes(), 0xFF))

		rng := fdb.KeyRange{Begin: startKey, End: endKey}
		iter := tx.GetRange(rng, fdb.RangeOptions{Limit: limit}).Iterator()

		var events []*types.RepoEvent
		var lastKey []byte

		for iter.Advance() {
			kv, err := iter.Get()
			if err != nil {
				return nil, fmt.Errorf("failed to get event: %w", err)
			}

			// extract versionstamp from key (after prefix)
			prefixLen := len(db.eventDir.events.Bytes())
			if len(kv.Key) < prefixLen+versionstampLength {
				continue // malformed key
			}
			versionstamp := kv.Key[prefixLen : prefixLen+versionstampLength]

			// parse event
			var event types.RepoEvent
			if err := proto.Unmarshal(kv.Value, &event); err != nil {
				return nil, fmt.Errorf("failed to unmarshal event: %w", err)
			}

			// set sequence from versionstamp (first 8 bytes as big-endian int64)
			event.Seq = int64(binary.BigEndian.Uint64(versionstamp[:8]))

			events = append(events, &event)
			lastKey = versionstamp
		}

		return &result{events: events, nextCursor: lastKey}, nil
	})

	if err != nil {
		return nil, nil, err
	}

	return res.events, res.nextCursor, nil
}

// GetEventsSinceSeq retrieves events starting from (but not including) the given sequence number.
// This is a convenience wrapper that converts an int64 seq to a cursor.
func (db *DB) GetEventsSinceSeq(ctx context.Context, seq int64, limit int) ([]*types.RepoEvent, []byte, error) {
	// convert seq to versionstamp cursor (8 bytes big-endian + 2 zero bytes for batch order)
	cursor := make([]byte, versionstampLength)
	binary.BigEndian.PutUint64(cursor[:8], uint64(seq))
	return db.GetEventsSince(ctx, cursor, limit)
}

// WatchLatestSeq returns a future that will be ready when the latest sequence changes.
// Use this to efficiently wait for new events without polling.
func (db *DB) WatchLatestSeq(ctx context.Context) (fdb.FutureNil, error) {
	// we need to do this in a transaction to get the watch
	var watch fdb.FutureNil

	_, err := db.db.Transact(func(tx fdb.Transaction) (any, error) {
		latestKey := db.eventDir.latestSeq.Pack(tuple.Tuple{latestSeqKey})
		watch = tx.Watch(latestKey)
		return nil, nil
	})

	if err != nil {
		return nil, err
	}

	return watch, nil
}

// GetLatestSeq returns the latest sequence number (versionstamp) in the events subspace.
// Returns nil if no events exist.
func (db *DB) GetLatestSeq(ctx context.Context) (cursor []byte, err error) {
	_, span, done := db.observe(ctx, "GetLatestSeq")
	defer func() { done(err) }()

	cursor, err = readTransaction(db.db, func(tx fdb.ReadTransaction) ([]byte, error) {
		latestKey := db.eventDir.latestSeq.Pack(tuple.Tuple{latestSeqKey})
		val, err := tx.Get(latestKey).Get()
		if err != nil {
			return nil, err
		}
		if len(val) < versionstampLength {
			return nil, nil // no events yet
		}
		return val[:versionstampLength], nil
	})

	if err == ErrNotFound {
		return nil, nil
	}

	span.SetAttributes(attribute.Int("cursor_len", len(cursor)))
	return
}

// SeqToInt64 converts a versionstamp cursor to an int64 sequence number.
// This extracts the 8-byte commit version portion.
func SeqToInt64(cursor []byte) int64 {
	if len(cursor) < 8 {
		return 0
	}
	return int64(binary.BigEndian.Uint64(cursor[:8]))
}

// Int64ToSeq converts an int64 sequence number to a versionstamp cursor.
func Int64ToSeq(seq int64) []byte {
	cursor := make([]byte, versionstampLength)
	binary.BigEndian.PutUint64(cursor[:8], uint64(seq))
	return cursor
}
