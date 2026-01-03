package foundation

import (
	"context"
	"fmt"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// Blockstore implements a per-DID blockstore backed by FoundationDB.
// It implements the minimal interface required by indigo's repo package.
//
// The blockstore uses a write buffer (pending) to provide read-your-writes
// semantics within a request. This is necessary because indigo's MST code
// creates blocks and then immediately reads them back during tree operations.
// The pending map allows Get to return blocks that haven't been flushed to FDB yet.
//
// Typical flow:
//  1. MST operations call Put() → blocks go to pending map
//  2. MST operations call Get() → checks pending first, then FDB
//  3. At commit time, FlushTx() writes all pending blocks to FDB atomically
//  4. ClearPending() is called after successful transaction commit
type Blockstore struct {
	db     *DB
	tracer trace.Tracer
	did    string

	// pending holds blocks that have been Put but not yet flushed to FDB.
	// This write buffer is essential for MST operations which need to read
	// blocks they just wrote before the final atomic commit.
	pending map[string]blocks.Block
}

// NewBlockstore creates a new blockstore for the given DID.
func (db *DB) NewBlockstore(did string) *Blockstore {
	return &Blockstore{
		db:      db,
		tracer:  db.tracer,
		did:     did,
		pending: make(map[string]blocks.Block),
	}
}

// Get retrieves a block by its CID.
func (bs *Blockstore) Get(ctx context.Context, c cid.Cid) (blocks.Block, error) {
	_, span := bs.tracer.Start(ctx, "Blockstore.Get")
	defer span.End()

	span.SetAttributes(
		attribute.String("did", bs.did),
		attribute.String("cid", c.String()),
	)

	// check pending first
	if blk, ok := bs.pending[c.String()]; ok {
		return blk, nil
	}

	key := pack(bs.db.blockDir.blocks, bs.did, c.Bytes())

	val, err := readTransaction(bs.db.db, func(tx fdb.ReadTransaction) ([]byte, error) {
		return tx.Get(key).Get()
	})
	if err != nil {
		return nil, fmt.Errorf("failed to get block: %w", err)
	}
	if val == nil {
		return nil, fmt.Errorf("block not found: %s", c.String())
	}

	return blocks.NewBlockWithCid(val, c)
}

// Has returns whether the blockstore contains a block with the given CID.
func (bs *Blockstore) Has(ctx context.Context, c cid.Cid) (bool, error) {
	_, span := bs.tracer.Start(ctx, "Blockstore.Has")
	defer span.End()

	span.SetAttributes(
		attribute.String("did", bs.did),
		attribute.String("cid", c.String()),
	)

	// check pending first
	if _, ok := bs.pending[c.String()]; ok {
		return true, nil
	}

	key := pack(bs.db.blockDir.blocks, bs.did, c.Bytes())

	val, err := readTransaction(bs.db.db, func(tx fdb.ReadTransaction) ([]byte, error) {
		return tx.Get(key).Get()
	})
	if err != nil {
		return false, fmt.Errorf("failed to check block: %w", err)
	}

	return val != nil, nil
}

// GetSize returns the size of a block.
func (bs *Blockstore) GetSize(ctx context.Context, c cid.Cid) (int, error) {
	blk, err := bs.Get(ctx, c)
	if err != nil {
		return 0, err
	}
	return len(blk.RawData()), nil
}

// Put stores a block. The block is held in memory until Flush is called.
func (bs *Blockstore) Put(ctx context.Context, blk blocks.Block) error {
	_, span := bs.tracer.Start(ctx, "Blockstore.Put")
	defer span.End()

	span.SetAttributes(
		attribute.String("did", bs.did),
		attribute.String("cid", blk.Cid().String()),
		attribute.Int("size", len(blk.RawData())),
	)

	bs.pending[blk.Cid().String()] = blk
	return nil
}

// PutMany stores multiple blocks.
func (bs *Blockstore) PutMany(ctx context.Context, blks []blocks.Block) error {
	_, span := bs.tracer.Start(ctx, "Blockstore.PutMany")
	defer span.End()

	span.SetAttributes(
		attribute.String("did", bs.did),
		attribute.Int("count", len(blks)),
	)

	for _, blk := range blks {
		bs.pending[blk.Cid().String()] = blk
	}
	return nil
}

// Flush writes all pending blocks to FoundationDB.
func (bs *Blockstore) Flush(ctx context.Context) error {
	_, span := bs.tracer.Start(ctx, "Blockstore.Flush")
	defer span.End()

	span.SetAttributes(
		attribute.String("did", bs.did),
		attribute.Int("pending_count", len(bs.pending)),
	)

	if len(bs.pending) == 0 {
		return nil
	}

	_, err := transaction(bs.db.db, func(tx fdb.Transaction) (any, error) {
		bs.FlushTx(tx)
		return nil, nil
	})
	if err != nil {
		return fmt.Errorf("failed to flush blocks: %w", err)
	}

	// clear pending after successful flush
	bs.pending = make(map[string]blocks.Block)
	return nil
}

// FlushTx writes all pending blocks within an existing transaction.
// Call ClearPending after the transaction commits successfully.
func (bs *Blockstore) FlushTx(tx fdb.Transaction) {
	for _, blk := range bs.pending {
		key := pack(bs.db.blockDir.blocks, bs.did, blk.Cid().Bytes())
		tx.Set(key, blk.RawData())
	}
}

// ClearPending clears the pending blocks map after a successful transaction.
func (bs *Blockstore) ClearPending() {
	bs.pending = make(map[string]blocks.Block)
}

// DeleteBlock removes a block from the store.
func (bs *Blockstore) DeleteBlock(ctx context.Context, c cid.Cid) error {
	_, span := bs.tracer.Start(ctx, "Blockstore.DeleteBlock")
	defer span.End()

	span.SetAttributes(
		attribute.String("did", bs.did),
		attribute.String("cid", c.String()),
	)

	// remove from pending if present
	delete(bs.pending, c.String())

	key := pack(bs.db.blockDir.blocks, bs.did, c.Bytes())

	_, err := transaction(bs.db.db, func(tx fdb.Transaction) (any, error) {
		tx.Clear(key)
		return nil, nil
	})
	return err
}

// AllKeysChan returns a channel of all CIDs in the blockstore.
// This is required by the blockstore interface but we don't need it for MST operations.
func (bs *Blockstore) AllKeysChan(ctx context.Context) (<-chan cid.Cid, error) {
	return nil, fmt.Errorf("AllKeysChan not implemented")
}

// HashOnRead is a no-op - we trust the stored data.
func (bs *Blockstore) HashOnRead(enabled bool) {}
