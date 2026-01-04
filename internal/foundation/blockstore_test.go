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

	t.Run("put and get within transaction", func(t *testing.T) {
		t.Parallel()

		blk := makeTestBlock(t, []byte("test data 1"))

		err := db.Transact(func(tx fdb.Transaction) error {
			bs := db.newWriteBlockstore("did:plc:blocktest1", tx)

			// put block
			err := bs.Put(ctx, blk)
			require.NoError(t, err)

			// get should return from transaction
			got, err := bs.Get(ctx, blk.Cid())
			require.NoError(t, err)
			require.Equal(t, blk.RawData(), got.RawData())
			require.Equal(t, blk.Cid(), got.Cid())

			return nil
		})
		require.NoError(t, err)
	})

	t.Run("read after transaction commits", func(t *testing.T) {
		t.Parallel()

		blk := makeTestBlock(t, []byte("test data 2"))

		// put within a transaction
		err := db.Transact(func(tx fdb.Transaction) error {
			bs := db.newWriteBlockstore("did:plc:blocktest2", tx)
			return bs.Put(ctx, blk)
		})
		require.NoError(t, err)

		// read with a new read-only blockstore
		_, err = db.db.ReadTransact(func(tx fdb.ReadTransaction) (any, error) {
			bs := db.newReadBlockstore("did:plc:blocktest2", tx)
			got, err := bs.Get(ctx, blk.Cid())
			require.NoError(t, err)
			require.Equal(t, blk.RawData(), got.RawData())
			return nil, nil
		})
		require.NoError(t, err)
	})

	t.Run("get non-existent block returns error", func(t *testing.T) {
		t.Parallel()

		fakeCID := makeTestBlock(t, []byte("nonexistent")).Cid()

		_, err := db.db.ReadTransact(func(tx fdb.ReadTransaction) (any, error) {
			bs := db.newReadBlockstore("did:plc:blocktest3", tx)
			_, err := bs.Get(ctx, fakeCID)
			require.Error(t, err)
			require.Contains(t, err.Error(), "block not found")
			return nil, nil
		})
		require.NoError(t, err)
	})

	t.Run("put without write transaction returns error", func(t *testing.T) {
		t.Parallel()

		blk := makeTestBlock(t, []byte("no tx test"))

		_, err := db.db.ReadTransact(func(tx fdb.ReadTransaction) (any, error) {
			bs := db.newReadBlockstore("did:plc:blocktest4", tx)
			err := bs.Put(ctx, blk)
			require.Error(t, err)
			require.Contains(t, err.Error(), "requires a transaction")
			return nil, nil
		})
		require.NoError(t, err)
	})
}

func TestBlockstore_Has(t *testing.T) {
	t.Parallel()
	db := testDB(t)
	ctx := t.Context()

	t.Run("has returns true within transaction", func(t *testing.T) {
		t.Parallel()

		blk := makeTestBlock(t, []byte("has test 1"))

		err := db.Transact(func(tx fdb.Transaction) error {
			bs := db.newWriteBlockstore("did:plc:hastest1", tx)

			err := bs.Put(ctx, blk)
			require.NoError(t, err)

			has, err := bs.Has(ctx, blk.Cid())
			require.NoError(t, err)
			require.True(t, has)

			return nil
		})
		require.NoError(t, err)
	})

	t.Run("has returns true after commit", func(t *testing.T) {
		t.Parallel()

		blk := makeTestBlock(t, []byte("has test 2"))

		err := db.Transact(func(tx fdb.Transaction) error {
			bs := db.newWriteBlockstore("did:plc:hastest2", tx)
			return bs.Put(ctx, blk)
		})
		require.NoError(t, err)

		// read-only blockstore
		_, err = db.db.ReadTransact(func(tx fdb.ReadTransaction) (any, error) {
			bs := db.newReadBlockstore("did:plc:hastest2", tx)
			has, err := bs.Has(ctx, blk.Cid())
			require.NoError(t, err)
			require.True(t, has)
			return nil, nil
		})
		require.NoError(t, err)
	})

	t.Run("has returns false for non-existent block", func(t *testing.T) {
		t.Parallel()

		fakeCID := makeTestBlock(t, []byte("nonexistent has")).Cid()

		_, err := db.db.ReadTransact(func(tx fdb.ReadTransaction) (any, error) {
			bs := db.newReadBlockstore("did:plc:hastest3", tx)
			has, err := bs.Has(ctx, fakeCID)
			require.NoError(t, err)
			require.False(t, has)
			return nil, nil
		})
		require.NoError(t, err)
	})
}

func TestBlockstore_GetSize(t *testing.T) {
	t.Parallel()
	db := testDB(t)
	ctx := t.Context()

	t.Run("returns correct size within transaction", func(t *testing.T) {
		t.Parallel()

		data := []byte("size test data with known length")
		blk := makeTestBlock(t, data)

		err := db.Transact(func(tx fdb.Transaction) error {
			bs := db.newWriteBlockstore("did:plc:sizetest1", tx)

			err := bs.Put(ctx, blk)
			require.NoError(t, err)

			size, err := bs.GetSize(ctx, blk.Cid())
			require.NoError(t, err)
			require.Equal(t, len(data), size)

			return nil
		})
		require.NoError(t, err)
	})

	t.Run("returns correct size after commit", func(t *testing.T) {
		t.Parallel()

		data := []byte("another size test")
		blk := makeTestBlock(t, data)

		err := db.Transact(func(tx fdb.Transaction) error {
			bs := db.newWriteBlockstore("did:plc:sizetest2", tx)
			return bs.Put(ctx, blk)
		})
		require.NoError(t, err)

		_, err = db.db.ReadTransact(func(tx fdb.ReadTransaction) (any, error) {
			bs := db.newReadBlockstore("did:plc:sizetest2", tx)
			size, err := bs.GetSize(ctx, blk.Cid())
			require.NoError(t, err)
			require.Equal(t, len(data), size)
			return nil, nil
		})
		require.NoError(t, err)
	})
}

func TestBlockstore_PutMany(t *testing.T) {
	t.Parallel()
	db := testDB(t)
	ctx := t.Context()

	t.Run("puts multiple blocks in transaction", func(t *testing.T) {
		t.Parallel()

		blk1 := makeTestBlock(t, []byte("block 1"))
		blk2 := makeTestBlock(t, []byte("block 2"))
		blk3 := makeTestBlock(t, []byte("block 3"))

		err := db.Transact(func(tx fdb.Transaction) error {
			bs := db.newWriteBlockstore("did:plc:putmany1", tx)

			err := bs.PutMany(ctx, []blocks.Block{blk1, blk2, blk3})
			require.NoError(t, err)

			// all should be retrievable within transaction
			for _, blk := range []blocks.Block{blk1, blk2, blk3} {
				got, err := bs.Get(ctx, blk.Cid())
				require.NoError(t, err)
				require.Equal(t, blk.RawData(), got.RawData())
			}

			return nil
		})
		require.NoError(t, err)
	})

	t.Run("blocks persisted after commit", func(t *testing.T) {
		t.Parallel()

		blk1 := makeTestBlock(t, []byte("batch block 1"))
		blk2 := makeTestBlock(t, []byte("batch block 2"))

		err := db.Transact(func(tx fdb.Transaction) error {
			bs := db.newWriteBlockstore("did:plc:putmany2", tx)
			return bs.PutMany(ctx, []blocks.Block{blk1, blk2})
		})
		require.NoError(t, err)

		// verify from read-only blockstore
		_, err = db.db.ReadTransact(func(tx fdb.ReadTransaction) (any, error) {
			bs := db.newReadBlockstore("did:plc:putmany2", tx)
			for _, blk := range []blocks.Block{blk1, blk2} {
				got, err := bs.Get(ctx, blk.Cid())
				require.NoError(t, err)
				require.Equal(t, blk.RawData(), got.RawData())
			}
			return nil, nil
		})
		require.NoError(t, err)
	})

	t.Run("PutMany without write transaction returns error", func(t *testing.T) {
		t.Parallel()

		blk := makeTestBlock(t, []byte("no tx putmany"))

		_, err := db.db.ReadTransact(func(tx fdb.ReadTransaction) (any, error) {
			bs := db.newReadBlockstore("did:plc:putmany3", tx)
			err := bs.PutMany(ctx, []blocks.Block{blk})
			require.Error(t, err)
			require.Contains(t, err.Error(), "requires a transaction")
			return nil, nil
		})
		require.NoError(t, err)
	})
}

func TestBlockstore_DeleteBlock(t *testing.T) {
	t.Parallel()
	db := testDB(t)
	ctx := t.Context()

	t.Run("delete within transaction", func(t *testing.T) {
		t.Parallel()

		blk := makeTestBlock(t, []byte("delete test 1"))

		// first put the block
		err := db.Transact(func(tx fdb.Transaction) error {
			bs := db.newWriteBlockstore("did:plc:delete1", tx)
			return bs.Put(ctx, blk)
		})
		require.NoError(t, err)

		// verify it exists
		_, err = db.db.ReadTransact(func(tx fdb.ReadTransaction) (any, error) {
			bs := db.newReadBlockstore("did:plc:delete1", tx)
			has, err := bs.Has(ctx, blk.Cid())
			require.NoError(t, err)
			require.True(t, has)
			return nil, nil
		})
		require.NoError(t, err)

		// delete in a new transaction
		err = db.Transact(func(tx fdb.Transaction) error {
			bs := db.newWriteBlockstore("did:plc:delete1", tx)
			return bs.DeleteBlock(ctx, blk.Cid())
		})
		require.NoError(t, err)

		// verify it's gone
		_, err = db.db.ReadTransact(func(tx fdb.ReadTransaction) (any, error) {
			bs := db.newReadBlockstore("did:plc:delete1", tx)
			has, err := bs.Has(ctx, blk.Cid())
			require.NoError(t, err)
			require.False(t, has)
			return nil, nil
		})
		require.NoError(t, err)
	})

	t.Run("delete without write transaction returns error", func(t *testing.T) {
		t.Parallel()

		fakeCID := makeTestBlock(t, []byte("no tx delete")).Cid()

		_, err := db.db.ReadTransact(func(tx fdb.ReadTransaction) (any, error) {
			bs := db.newReadBlockstore("did:plc:delete2", tx)
			err := bs.DeleteBlock(ctx, fakeCID)
			require.Error(t, err)
			require.Contains(t, err.Error(), "requires a transaction")
			return nil, nil
		})
		require.NoError(t, err)
	})
}

func TestBlockstore_IsolationByDID(t *testing.T) {
	t.Parallel()
	db := testDB(t)
	ctx := t.Context()

	t.Run("blocks are isolated per DID", func(t *testing.T) {
		t.Parallel()

		blk := makeTestBlock(t, []byte("isolation test"))

		// put block for DID 1
		err := db.Transact(func(tx fdb.Transaction) error {
			bs := db.newWriteBlockstore("did:plc:isolation1", tx)
			return bs.Put(ctx, blk)
		})
		require.NoError(t, err)

		_, err = db.db.ReadTransact(func(tx fdb.ReadTransaction) (any, error) {
			// DID 1 should have the block
			bs1 := db.newReadBlockstore("did:plc:isolation1", tx)
			has, err := bs1.Has(ctx, blk.Cid())
			require.NoError(t, err)
			require.True(t, has)

			// DID 2 should NOT have the block
			bs2 := db.newReadBlockstore("did:plc:isolation2", tx)
			has, err = bs2.Has(ctx, blk.Cid())
			require.NoError(t, err)
			require.False(t, has)
			return nil, nil
		})
		require.NoError(t, err)
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

func TestBlockstore_TransactionRollback(t *testing.T) {
	t.Parallel()
	db := testDB(t)
	ctx := t.Context()

	t.Run("blocks not persisted on transaction error", func(t *testing.T) {
		t.Parallel()

		blk := makeTestBlock(t, []byte("rollback test"))

		// put block but return error to roll back
		err := db.Transact(func(tx fdb.Transaction) error {
			bs := db.newWriteBlockstore("did:plc:rollback1", tx)
			err := bs.Put(ctx, blk)
			require.NoError(t, err)

			// return error to trigger rollback
			return fdb.Error{Code: 1234}
		})
		require.Error(t, err)

		// block should not exist
		_, err = db.db.ReadTransact(func(tx fdb.ReadTransaction) (any, error) {
			bs := db.newReadBlockstore("did:plc:rollback1", tx)
			has, err := bs.Has(ctx, blk.Cid())
			require.NoError(t, err)
			require.False(t, has)
			return nil, nil
		})
		require.NoError(t, err)
	})
}
