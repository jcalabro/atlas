package db

import (
	"context"
	"fmt"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"go.opentelemetry.io/otel/attribute"
)

// blockstore implements a per-DID blockstore backed by FoundationDB.
// It implements the minimal interface required by indigo's repo package.
type blockstore struct {
	db  *DB
	did string

	// readTx is the FDB read transaction for read-only mode.
	readTx fdb.ReadTransaction

	// writeTx is the FDB transaction for write mode.
	// When non-nil, all reads and writes happen within this transaction.
	writeTx *fdb.Transaction

	// rev is the revision being written. Used to populate the blocks_by_rev index.
	// Only set for write blockstores.
	rev string
}

// newReadBlockstore creates a read-only blockstore bound to an FDB read transaction.
func (db *DB) newReadBlockstore(did string, tx fdb.ReadTransaction) *blockstore {
	return &blockstore{
		db:     db,
		did:    did,
		readTx: tx,
	}
}

// newWriteBlockstore creates a blockstore bound to an FDB write transaction.
// All reads and writes will happen within this transaction.
// Use SetRev to set the revision for secondary index writes.
func (db *DB) newWriteBlockstore(did string, tx fdb.Transaction) *blockstore {
	return &blockstore{
		db:      db,
		did:     did,
		writeTx: &tx,
	}
}

// SetRev sets the revision for the blockstore. When set, Put and PutMany will
// populate the blocks_by_rev secondary index for incremental sync.
func (bs *blockstore) SetRev(rev string) {
	bs.rev = rev
}

// Get retrieves a block by its CID.
func (bs *blockstore) Get(ctx context.Context, c cid.Cid) (blocks.Block, error) {
	var val []byte
	var err error

	key := pack(bs.db.blockDir.blocks, bs.did, c.Bytes())
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
	var val []byte
	var err error

	key := pack(bs.db.blockDir.blocks, bs.did, c.Bytes())
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
	if bs.writeTx == nil {
		return fmt.Errorf("blockstore put requires a transaction")
	}

	// write to primary index
	key := pack(bs.db.blockDir.blocks, bs.did, blk.Cid().Bytes())
	(*bs.writeTx).Set(key, blk.RawData())

	// write to secondary index for incremental sync
	if bs.rev != "" {
		revKey := pack(bs.db.blockDir.blocksByRev, bs.did, bs.rev, blk.Cid().Bytes())
		(*bs.writeTx).Set(revKey, nil)
	}

	return nil
}

// PutMany stores multiple blocks. Requires transactional mode.
func (bs *blockstore) PutMany(ctx context.Context, blks []blocks.Block) error {
	if bs.writeTx == nil {
		return fmt.Errorf("blockstore put_many requires a transaction")
	}

	for _, blk := range blks {
		// write to primary index
		key := pack(bs.db.blockDir.blocks, bs.did, blk.Cid().Bytes())
		(*bs.writeTx).Set(key, blk.RawData())

		// write to secondary index for incremental sync
		if bs.rev != "" {
			revKey := pack(bs.db.blockDir.blocksByRev, bs.did, bs.rev, blk.Cid().Bytes())
			(*bs.writeTx).Set(revKey, nil)
		}
	}

	return nil
}

// DeleteBlock removes a block from the store. Requires transactional mode.
func (bs *blockstore) DeleteBlock(ctx context.Context, c cid.Cid) error {
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

// GetBlocks retrieves multiple blocks by their CIDs for a given DID.
// Returns the blocks that were found. Missing blocks are silently skipped.
func (db *DB) GetBlocks(ctx context.Context, did string, cids []cid.Cid) (result []blocks.Block, err error) {
	_, span, done := db.observe(ctx, "GetBlocks")
	defer func() { done(err) }()

	span.SetAttributes(
		attribute.String("did", did),
		attribute.Int("num_cids", len(cids)),
	)

	result, err = readTransaction(db.db, func(tx fdb.ReadTransaction) ([]blocks.Block, error) {
		bs := db.newReadBlockstore(did, tx)
		blks := make([]blocks.Block, 0, len(cids))

		for _, c := range cids {
			blk, err := bs.Get(ctx, c)
			if err != nil {
				// skip blocks that are not found
				continue
			}
			blks = append(blks, blk)
		}

		return blks, nil
	})

	return
}

// GetAllBlocks retrieves all blocks for a given DID.
func (db *DB) GetAllBlocks(ctx context.Context, did string) (result []blocks.Block, err error) {
	_, span, done := db.observe(ctx, "GetAllBlocks")
	defer func() { done(err) }()

	span.SetAttributes(attribute.String("did", did))

	result, err = readTransaction(db.db, func(tx fdb.ReadTransaction) ([]blocks.Block, error) {
		rangeBegin := pack(db.blockDir.blocks, did)
		rangeEnd := pack(db.blockDir.blocks, did+"\xff")

		kr := fdb.KeyRange{Begin: rangeBegin, End: rangeEnd}

		var blks []blocks.Block
		iter := tx.GetRange(kr, fdb.RangeOptions{}).Iterator()
		for iter.Advance() {
			kv, err := iter.Get()
			if err != nil {
				return nil, fmt.Errorf("failed to iterate blocks: %w", err)
			}

			// extract CID bytes from the key tuple (did, cid_bytes)
			tup, err := db.blockDir.blocks.Unpack(kv.Key)
			if err != nil {
				return nil, fmt.Errorf("failed to unpack block key: %w", err)
			}
			if len(tup) < 2 {
				continue
			}

			cidBytes, ok := tup[1].([]byte)
			if !ok {
				continue
			}

			_, c, err := cid.CidFromBytes(cidBytes)
			if err != nil {
				return nil, fmt.Errorf("failed to parse cid from key: %w", err)
			}

			blk, err := blocks.NewBlockWithCid(kv.Value, c)
			if err != nil {
				return nil, fmt.Errorf("failed to create block: %w", err)
			}

			blks = append(blks, blk)
		}

		return blks, nil
	})

	return
}

// GetBlocksSince retrieves all blocks added after the given revision.
// Used for incremental sync via the `since` parameter.
func (db *DB) GetBlocksSince(ctx context.Context, did string, sinceRev string) (result []blocks.Block, err error) {
	_, span, done := db.observe(ctx, "GetBlocksSince")
	defer func() { done(err) }()

	span.SetAttributes(
		attribute.String("did", did),
		attribute.String("since", sinceRev),
	)

	result, err = readTransaction(db.db, func(tx fdb.ReadTransaction) ([]blocks.Block, error) {
		// query the secondary index for all revisions after sinceRev
		// use sinceRev + "\x00" to exclude the exact sinceRev
		rangeBegin := pack(db.blockDir.blocksByRev, did, sinceRev+"\x00")
		rangeEnd := pack(db.blockDir.blocksByRev, did+"\xff")

		kr := fdb.KeyRange{Begin: rangeBegin, End: rangeEnd}

		// collect all CIDs from the secondary index
		var cids []cid.Cid
		iter := tx.GetRange(kr, fdb.RangeOptions{}).Iterator()
		for iter.Advance() {
			kv, err := iter.Get()
			if err != nil {
				return nil, fmt.Errorf("failed to iterate blocks_by_rev: %w", err)
			}

			// extract CID bytes from the key tuple (did, rev, cid_bytes)
			tup, err := db.blockDir.blocksByRev.Unpack(kv.Key)
			if err != nil {
				return nil, fmt.Errorf("failed to unpack blocks_by_rev key: %w", err)
			}
			if len(tup) < 3 {
				continue
			}

			cidBytes, ok := tup[2].([]byte)
			if !ok {
				continue
			}

			_, c, err := cid.CidFromBytes(cidBytes)
			if err != nil {
				return nil, fmt.Errorf("failed to parse cid from key: %w", err)
			}

			cids = append(cids, c)
		}

		// fetch the actual block data from the primary index
		blks := make([]blocks.Block, 0, len(cids))
		for _, c := range cids {
			key := pack(db.blockDir.blocks, did, c.Bytes())
			val, err := tx.Get(key).Get()
			if err != nil {
				return nil, fmt.Errorf("failed to get block: %w", err)
			}
			if val == nil {
				// block was deleted, skip
				continue
			}

			blk, err := blocks.NewBlockWithCid(val, c)
			if err != nil {
				return nil, fmt.Errorf("failed to create block: %w", err)
			}
			blks = append(blks, blk)
		}

		return blks, nil
	})

	return
}
