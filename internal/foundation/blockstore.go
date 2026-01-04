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

// blockstore implements a per-DID blockstore backed by FoundationDB.
// It implements the minimal interface required by indigo's repo package.
type blockstore struct {
	db     *DB
	tracer trace.Tracer
	did    string

	// readTx is the FDB read transaction for read-only mode.
	readTx fdb.ReadTransaction

	// writeTx is the FDB transaction for write mode.
	// When non-nil, all reads and writes happen within this transaction.
	writeTx *fdb.Transaction
}

// newReadBlockstore creates a read-only blockstore bound to an FDB read transaction.
func (db *DB) newReadBlockstore(did string, tx fdb.ReadTransaction) *blockstore {
	return &blockstore{
		db:     db,
		tracer: db.tracer,
		did:    did,
		readTx: tx,
	}
}

// newWriteBlockstore creates a blockstore bound to an FDB write transaction.
// All reads and writes will happen within this transaction.
func (db *DB) newWriteBlockstore(did string, tx fdb.Transaction) *blockstore {
	return &blockstore{
		db:      db,
		tracer:  db.tracer,
		did:     did,
		writeTx: &tx,
	}
}

// Get retrieves a block by its CID.
func (bs *blockstore) Get(ctx context.Context, c cid.Cid) (blocks.Block, error) {
	_, span := bs.tracer.Start(ctx, "Blockstore.Get")
	defer span.End()

	span.SetAttributes(
		attribute.String("did", bs.did),
		attribute.String("cid", c.String()),
	)

	key := pack(bs.db.blockDir.blocks, bs.did, c.Bytes())

	var val []byte
	var err error

	if bs.writeTx != nil {
		val, err = (*bs.writeTx).Get(key).Get()
	} else if bs.readTx != nil {
		val, err = bs.readTx.Get(key).Get()
	} else {
		return nil, fmt.Errorf("blockstore get requires a transaction")
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get block: %w", err)
	}
	if val == nil {
		return nil, fmt.Errorf("block not found: %s", c.String())
	}

	return blocks.NewBlockWithCid(val, c)
}

// Has returns whether the blockstore contains a block with the given CID.
func (bs *blockstore) Has(ctx context.Context, c cid.Cid) (bool, error) {
	_, span := bs.tracer.Start(ctx, "Blockstore.Has")
	defer span.End()

	span.SetAttributes(
		attribute.String("did", bs.did),
		attribute.String("cid", c.String()),
	)

	key := pack(bs.db.blockDir.blocks, bs.did, c.Bytes())

	var val []byte
	var err error

	if bs.writeTx != nil {
		val, err = (*bs.writeTx).Get(key).Get()
	} else if bs.readTx != nil {
		val, err = bs.readTx.Get(key).Get()
	} else {
		return false, fmt.Errorf("blockstore has requires a transaction")
	}
	if err != nil {
		return false, fmt.Errorf("failed to check block: %w", err)
	}

	return val != nil, nil
}

// GetSize returns the size of a block.
func (bs *blockstore) GetSize(ctx context.Context, c cid.Cid) (int, error) {
	blk, err := bs.Get(ctx, c)
	if err != nil {
		return 0, err
	}
	return len(blk.RawData()), nil
}

// Put stores a block. In transactional mode, writes directly to FDB.
// In read-only mode, this method will panic as writes require a transaction.
func (bs *blockstore) Put(ctx context.Context, blk blocks.Block) error {
	_, span := bs.tracer.Start(ctx, "Blockstore.Put")
	defer span.End()

	span.SetAttributes(
		attribute.String("did", bs.did),
		attribute.String("cid", blk.Cid().String()),
		attribute.Int("size", len(blk.RawData())),
	)

	if bs.writeTx == nil {
		return fmt.Errorf("blockstore put requires a transaction")
	}

	key := pack(bs.db.blockDir.blocks, bs.did, blk.Cid().Bytes())
	(*bs.writeTx).Set(key, blk.RawData())
	return nil
}

// PutMany stores multiple blocks. Requires transactional mode.
func (bs *blockstore) PutMany(ctx context.Context, blks []blocks.Block) error {
	_, span := bs.tracer.Start(ctx, "Blockstore.PutMany")
	defer span.End()

	span.SetAttributes(
		attribute.String("did", bs.did),
		attribute.Int("count", len(blks)),
	)

	if bs.writeTx == nil {
		return fmt.Errorf("blockstore put_many requires a transaction")
	}

	for _, blk := range blks {
		key := pack(bs.db.blockDir.blocks, bs.did, blk.Cid().Bytes())
		(*bs.writeTx).Set(key, blk.RawData())
	}

	return nil
}

// DeleteBlock removes a block from the store. Requires transactional mode.
func (bs *blockstore) DeleteBlock(ctx context.Context, c cid.Cid) error {
	_, span := bs.tracer.Start(ctx, "Blockstore.DeleteBlock")
	defer span.End()

	span.SetAttributes(
		attribute.String("did", bs.did),
		attribute.String("cid", c.String()),
	)

	if bs.writeTx == nil {
		return fmt.Errorf("blockstore delete_block requires a transaction")
	}

	key := pack(bs.db.blockDir.blocks, bs.did, c.Bytes())
	(*bs.writeTx).Clear(key)
	return nil
}

func (bs *blockstore) AllKeysChan(ctx context.Context) (<-chan cid.Cid, error) {
	return nil, fmt.Errorf("AllKeysChan not implemented")
}

// HashOnRead is a no-op
func (bs *blockstore) HashOnRead(enabled bool) {}
