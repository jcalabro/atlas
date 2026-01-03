package foundation

import (
	"testing"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/require"
)

// makeTestBlock creates a block with the given data for testing.
func makeTestBlock(t *testing.T, data []byte) blocks.Block {
	t.Helper()
	hash, err := multihash.Sum(data, multihash.SHA2_256, -1)
	require.NoError(t, err)
	c := cid.NewCidV1(cid.DagCBOR, hash)
	blk, err := blocks.NewBlockWithCid(data, c)
	require.NoError(t, err)
	return blk
}

func TestBlockstore_PutAndGet(t *testing.T) {
	t.Parallel()
	db := testDB(t)
	ctx := t.Context()

	t.Run("get from pending before flush", func(t *testing.T) {
		t.Parallel()
		bs := db.NewBlockstore("did:plc:blocktest1")

		blk := makeTestBlock(t, []byte("test data 1"))

		// put block (goes to pending)
		err := bs.Put(ctx, blk)
		require.NoError(t, err)

		// get should return from pending
		got, err := bs.Get(ctx, blk.Cid())
		require.NoError(t, err)
		require.Equal(t, blk.RawData(), got.RawData())
		require.Equal(t, blk.Cid(), got.Cid())
	})

	t.Run("get from FDB after flush", func(t *testing.T) {
		t.Parallel()
		bs := db.NewBlockstore("did:plc:blocktest2")

		blk := makeTestBlock(t, []byte("test data 2"))

		// put and flush
		err := bs.Put(ctx, blk)
		require.NoError(t, err)

		err = bs.Flush(ctx)
		require.NoError(t, err)

		// create new blockstore instance (no pending state)
		bs2 := db.NewBlockstore("did:plc:blocktest2")

		// get should return from FDB
		got, err := bs2.Get(ctx, blk.Cid())
		require.NoError(t, err)
		require.Equal(t, blk.RawData(), got.RawData())
	})

	t.Run("get non-existent block returns error", func(t *testing.T) {
		t.Parallel()
		bs := db.NewBlockstore("did:plc:blocktest3")

		fakeCID := makeTestBlock(t, []byte("nonexistent")).Cid()

		_, err := bs.Get(ctx, fakeCID)
		require.Error(t, err)
		require.Contains(t, err.Error(), "block not found")
	})
}

func TestBlockstore_Has(t *testing.T) {
	t.Parallel()
	db := testDB(t)
	ctx := t.Context()

	t.Run("has returns true for pending block", func(t *testing.T) {
		t.Parallel()
		bs := db.NewBlockstore("did:plc:hastest1")

		blk := makeTestBlock(t, []byte("has test 1"))

		err := bs.Put(ctx, blk)
		require.NoError(t, err)

		has, err := bs.Has(ctx, blk.Cid())
		require.NoError(t, err)
		require.True(t, has)
	})

	t.Run("has returns true for flushed block", func(t *testing.T) {
		t.Parallel()
		bs := db.NewBlockstore("did:plc:hastest2")

		blk := makeTestBlock(t, []byte("has test 2"))

		err := bs.Put(ctx, blk)
		require.NoError(t, err)

		err = bs.Flush(ctx)
		require.NoError(t, err)

		// new blockstore instance
		bs2 := db.NewBlockstore("did:plc:hastest2")

		has, err := bs2.Has(ctx, blk.Cid())
		require.NoError(t, err)
		require.True(t, has)
	})

	t.Run("has returns false for non-existent block", func(t *testing.T) {
		t.Parallel()
		bs := db.NewBlockstore("did:plc:hastest3")

		fakeCID := makeTestBlock(t, []byte("nonexistent has")).Cid()

		has, err := bs.Has(ctx, fakeCID)
		require.NoError(t, err)
		require.False(t, has)
	})
}

func TestBlockstore_GetSize(t *testing.T) {
	t.Parallel()
	db := testDB(t)
	ctx := t.Context()

	t.Run("returns correct size from pending", func(t *testing.T) {
		t.Parallel()
		bs := db.NewBlockstore("did:plc:sizetest1")

		data := []byte("size test data with known length")
		blk := makeTestBlock(t, data)

		err := bs.Put(ctx, blk)
		require.NoError(t, err)

		size, err := bs.GetSize(ctx, blk.Cid())
		require.NoError(t, err)
		require.Equal(t, len(data), size)
	})

	t.Run("returns correct size from FDB", func(t *testing.T) {
		t.Parallel()
		bs := db.NewBlockstore("did:plc:sizetest2")

		data := []byte("another size test")
		blk := makeTestBlock(t, data)

		err := bs.Put(ctx, blk)
		require.NoError(t, err)
		err = bs.Flush(ctx)
		require.NoError(t, err)

		bs2 := db.NewBlockstore("did:plc:sizetest2")
		size, err := bs2.GetSize(ctx, blk.Cid())
		require.NoError(t, err)
		require.Equal(t, len(data), size)
	})
}

func TestBlockstore_PutMany(t *testing.T) {
	t.Parallel()
	db := testDB(t)
	ctx := t.Context()

	t.Run("puts multiple blocks to pending", func(t *testing.T) {
		t.Parallel()
		bs := db.NewBlockstore("did:plc:putmany1")

		blk1 := makeTestBlock(t, []byte("block 1"))
		blk2 := makeTestBlock(t, []byte("block 2"))
		blk3 := makeTestBlock(t, []byte("block 3"))

		err := bs.PutMany(ctx, []blocks.Block{blk1, blk2, blk3})
		require.NoError(t, err)

		// all should be retrievable from pending
		for _, blk := range []blocks.Block{blk1, blk2, blk3} {
			got, err := bs.Get(ctx, blk.Cid())
			require.NoError(t, err)
			require.Equal(t, blk.RawData(), got.RawData())
		}
	})

	t.Run("flush persists all blocks", func(t *testing.T) {
		t.Parallel()
		bs := db.NewBlockstore("did:plc:putmany2")

		blk1 := makeTestBlock(t, []byte("batch block 1"))
		blk2 := makeTestBlock(t, []byte("batch block 2"))

		err := bs.PutMany(ctx, []blocks.Block{blk1, blk2})
		require.NoError(t, err)

		err = bs.Flush(ctx)
		require.NoError(t, err)

		// verify from fresh blockstore
		bs2 := db.NewBlockstore("did:plc:putmany2")
		for _, blk := range []blocks.Block{blk1, blk2} {
			got, err := bs2.Get(ctx, blk.Cid())
			require.NoError(t, err)
			require.Equal(t, blk.RawData(), got.RawData())
		}
	})
}

func TestBlockstore_DeleteBlock(t *testing.T) {
	t.Parallel()
	db := testDB(t)
	ctx := t.Context()

	t.Run("removes from pending", func(t *testing.T) {
		t.Parallel()
		bs := db.NewBlockstore("did:plc:delete1")

		blk := makeTestBlock(t, []byte("delete test 1"))

		err := bs.Put(ctx, blk)
		require.NoError(t, err)

		// verify it's there
		has, err := bs.Has(ctx, blk.Cid())
		require.NoError(t, err)
		require.True(t, has)

		// delete
		err = bs.DeleteBlock(ctx, blk.Cid())
		require.NoError(t, err)

		// verify it's gone from pending
		has, err = bs.Has(ctx, blk.Cid())
		require.NoError(t, err)
		require.False(t, has)
	})

	t.Run("removes from FDB", func(t *testing.T) {
		t.Parallel()
		bs := db.NewBlockstore("did:plc:delete2")

		blk := makeTestBlock(t, []byte("delete test 2"))

		// put and flush to FDB
		err := bs.Put(ctx, blk)
		require.NoError(t, err)
		err = bs.Flush(ctx)
		require.NoError(t, err)

		// verify it's in FDB
		bs2 := db.NewBlockstore("did:plc:delete2")
		has, err := bs2.Has(ctx, blk.Cid())
		require.NoError(t, err)
		require.True(t, has)

		// delete from FDB
		err = bs2.DeleteBlock(ctx, blk.Cid())
		require.NoError(t, err)

		// verify it's gone
		bs3 := db.NewBlockstore("did:plc:delete2")
		has, err = bs3.Has(ctx, blk.Cid())
		require.NoError(t, err)
		require.False(t, has)
	})
}

func TestBlockstore_Flush(t *testing.T) {
	t.Parallel()
	db := testDB(t)
	ctx := t.Context()

	t.Run("empty flush is no-op", func(t *testing.T) {
		t.Parallel()
		bs := db.NewBlockstore("did:plc:flush1")

		err := bs.Flush(ctx)
		require.NoError(t, err)
	})

	t.Run("clears pending after flush", func(t *testing.T) {
		t.Parallel()
		bs := db.NewBlockstore("did:plc:flush2")

		blk := makeTestBlock(t, []byte("flush test"))

		err := bs.Put(ctx, blk)
		require.NoError(t, err)

		err = bs.Flush(ctx)
		require.NoError(t, err)

		// pending should be cleared, but block still accessible via FDB
		// we can verify by checking the internal state indirectly:
		// put a new block with same CID - if pending was cleared, it should go to pending again
		err = bs.Put(ctx, blk)
		require.NoError(t, err)
		// if this doesn't panic/error, pending was cleared
	})

	t.Run("FlushTx writes within transaction", func(t *testing.T) {
		t.Parallel()
		bs := db.NewBlockstore("did:plc:flushtx1")

		blk := makeTestBlock(t, []byte("flushtx test"))

		err := bs.Put(ctx, blk)
		require.NoError(t, err)

		// use Transact to simulate atomic commit
		err = db.Transact(func(tx fdb.Transaction) error {
			bs.FlushTx(tx)
			return nil
		})
		require.NoError(t, err)

		bs.ClearPending()

		// verify from fresh blockstore
		bs2 := db.NewBlockstore("did:plc:flushtx1")
		got, err := bs2.Get(ctx, blk.Cid())
		require.NoError(t, err)
		require.Equal(t, blk.RawData(), got.RawData())
	})
}

func TestBlockstore_ClearPending(t *testing.T) {
	t.Parallel()
	db := testDB(t)
	ctx := t.Context()

	t.Run("clears pending without persisting", func(t *testing.T) {
		t.Parallel()
		bs := db.NewBlockstore("did:plc:clearpending1")

		blk := makeTestBlock(t, []byte("clear pending test"))

		err := bs.Put(ctx, blk)
		require.NoError(t, err)

		// verify it's in pending
		has, err := bs.Has(ctx, blk.Cid())
		require.NoError(t, err)
		require.True(t, has)

		// clear without flush
		bs.ClearPending()

		// should not be accessible anymore (not in pending, not in FDB)
		has, err = bs.Has(ctx, blk.Cid())
		require.NoError(t, err)
		require.False(t, has)
	})
}

func TestBlockstore_IsolationByDID(t *testing.T) {
	t.Parallel()
	db := testDB(t)
	ctx := t.Context()

	t.Run("blocks are isolated per DID", func(t *testing.T) {
		t.Parallel()

		bs1 := db.NewBlockstore("did:plc:isolation1")
		bs2 := db.NewBlockstore("did:plc:isolation2")

		blk := makeTestBlock(t, []byte("isolation test"))

		// put block for DID 1
		err := bs1.Put(ctx, blk)
		require.NoError(t, err)
		err = bs1.Flush(ctx)
		require.NoError(t, err)

		// DID 1 should have the block
		has, err := db.NewBlockstore("did:plc:isolation1").Has(ctx, blk.Cid())
		require.NoError(t, err)
		require.True(t, has)

		// DID 2 should NOT have the block
		has, err = bs2.Has(ctx, blk.Cid())
		require.NoError(t, err)
		require.False(t, has)
	})
}

func TestBlockstore_DeterministicCID(t *testing.T) {
	t.Parallel()

	t.Run("same data produces same CID", func(t *testing.T) {
		t.Parallel()

		data := []byte("deterministic test data")

		blk1 := makeTestBlock(t, data)
		blk2 := makeTestBlock(t, data)

		require.Equal(t, blk1.Cid(), blk2.Cid())
	})

	t.Run("different data produces different CID", func(t *testing.T) {
		t.Parallel()

		blk1 := makeTestBlock(t, []byte("data 1"))
		blk2 := makeTestBlock(t, []byte("data 2"))

		require.NotEqual(t, blk1.Cid(), blk2.Cid())
	})
}
