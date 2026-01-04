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

func TestBlockstore_SetRev(t *testing.T) {
	t.Parallel()
	db := testDB(t)
	ctx := t.Context()

	t.Run("SetRev populates secondary index on Put", func(t *testing.T) {
		t.Parallel()

		did := "did:plc:setrev1"
		rev := "3jqfcqzm3fo2i"
		blk := makeTestBlock(t, []byte("setrev test data"))

		err := db.Transact(func(tx fdb.Transaction) error {
			bs := db.newWriteBlockstore(did, tx)
			bs.SetRev(rev)
			return bs.Put(ctx, blk)
		})
		require.NoError(t, err)

		// verify block exists in primary index
		_, err = db.db.ReadTransact(func(tx fdb.ReadTransaction) (any, error) {
			bs := db.newReadBlockstore(did, tx)
			has, err := bs.Has(ctx, blk.Cid())
			require.NoError(t, err)
			require.True(t, has)
			return nil, nil
		})
		require.NoError(t, err)

		// verify secondary index was populated via GetBlocksSince
		blocksSince, err := db.GetBlocksSince(ctx, did, "")
		require.NoError(t, err)
		require.Len(t, blocksSince, 1)
		require.Equal(t, blk.Cid(), blocksSince[0].Cid())
	})

	t.Run("SetRev populates secondary index on PutMany", func(t *testing.T) {
		t.Parallel()

		did := "did:plc:setrev2"
		rev := "3jqfcqzm3fo2j"
		blk1 := makeTestBlock(t, []byte("setrev putmany 1"))
		blk2 := makeTestBlock(t, []byte("setrev putmany 2"))

		err := db.Transact(func(tx fdb.Transaction) error {
			bs := db.newWriteBlockstore(did, tx)
			bs.SetRev(rev)
			return bs.PutMany(ctx, []blocks.Block{blk1, blk2})
		})
		require.NoError(t, err)

		// verify secondary index was populated
		blocksSince, err := db.GetBlocksSince(ctx, did, "")
		require.NoError(t, err)
		require.Len(t, blocksSince, 2)

		cidSet := make(map[string]bool)
		for _, blk := range blocksSince {
			cidSet[blk.Cid().String()] = true
		}
		require.True(t, cidSet[blk1.Cid().String()])
		require.True(t, cidSet[blk2.Cid().String()])
	})

	t.Run("no rev means no secondary index writes", func(t *testing.T) {
		t.Parallel()

		did := "did:plc:setrev3"
		blk := makeTestBlock(t, []byte("no rev test"))

		// put without setting rev
		err := db.Transact(func(tx fdb.Transaction) error {
			bs := db.newWriteBlockstore(did, tx)
			// don't call SetRev
			return bs.Put(ctx, blk)
		})
		require.NoError(t, err)

		// block exists in primary index
		_, err = db.db.ReadTransact(func(tx fdb.ReadTransaction) (any, error) {
			bs := db.newReadBlockstore(did, tx)
			has, err := bs.Has(ctx, blk.Cid())
			require.NoError(t, err)
			require.True(t, has)
			return nil, nil
		})
		require.NoError(t, err)

		// but GetBlocksSince returns nothing (no secondary index entry)
		blocksSince, err := db.GetBlocksSince(ctx, did, "")
		require.NoError(t, err)
		require.Len(t, blocksSince, 0)
	})
}

func TestGetAllBlocks(t *testing.T) {
	t.Parallel()
	db := testDB(t)
	ctx := t.Context()

	t.Run("returns all blocks for a DID", func(t *testing.T) {
		t.Parallel()

		did := "did:plc:getall1"
		blk1 := makeTestBlock(t, []byte("getall block 1"))
		blk2 := makeTestBlock(t, []byte("getall block 2"))
		blk3 := makeTestBlock(t, []byte("getall block 3"))

		err := db.Transact(func(tx fdb.Transaction) error {
			bs := db.newWriteBlockstore(did, tx)
			return bs.PutMany(ctx, []blocks.Block{blk1, blk2, blk3})
		})
		require.NoError(t, err)

		allBlocks, err := db.GetAllBlocks(ctx, did)
		require.NoError(t, err)
		require.Len(t, allBlocks, 3)

		cidSet := make(map[string]bool)
		for _, blk := range allBlocks {
			cidSet[blk.Cid().String()] = true
		}
		require.True(t, cidSet[blk1.Cid().String()])
		require.True(t, cidSet[blk2.Cid().String()])
		require.True(t, cidSet[blk3.Cid().String()])
	})

	t.Run("returns empty for DID with no blocks", func(t *testing.T) {
		t.Parallel()

		did := "did:plc:getall2"

		allBlocks, err := db.GetAllBlocks(ctx, did)
		require.NoError(t, err)
		require.Len(t, allBlocks, 0)
	})

	t.Run("blocks are isolated per DID", func(t *testing.T) {
		t.Parallel()

		did1 := "did:plc:getall3a"
		did2 := "did:plc:getall3b"
		blk1 := makeTestBlock(t, []byte("did1 block"))
		blk2 := makeTestBlock(t, []byte("did2 block"))

		err := db.Transact(func(tx fdb.Transaction) error {
			bs1 := db.newWriteBlockstore(did1, tx)
			if err := bs1.Put(ctx, blk1); err != nil {
				return err
			}
			bs2 := db.newWriteBlockstore(did2, tx)
			return bs2.Put(ctx, blk2)
		})
		require.NoError(t, err)

		blocks1, err := db.GetAllBlocks(ctx, did1)
		require.NoError(t, err)
		require.Len(t, blocks1, 1)
		require.Equal(t, blk1.Cid(), blocks1[0].Cid())

		blocks2, err := db.GetAllBlocks(ctx, did2)
		require.NoError(t, err)
		require.Len(t, blocks2, 1)
		require.Equal(t, blk2.Cid(), blocks2[0].Cid())
	})
}

func TestGetBlocksSince(t *testing.T) {
	t.Parallel()
	db := testDB(t)
	ctx := t.Context()

	t.Run("returns blocks after specified revision", func(t *testing.T) {
		t.Parallel()

		did := "did:plc:since1"
		rev1 := "3jqfcqzm3fo21"
		rev2 := "3jqfcqzm3fo22"
		rev3 := "3jqfcqzm3fo23"

		blk1 := makeTestBlock(t, []byte("rev1 block"))
		blk2 := makeTestBlock(t, []byte("rev2 block"))
		blk3 := makeTestBlock(t, []byte("rev3 block"))

		// write blocks with different revisions
		err := db.Transact(func(tx fdb.Transaction) error {
			bs := db.newWriteBlockstore(did, tx)
			bs.SetRev(rev1)
			if err := bs.Put(ctx, blk1); err != nil {
				return err
			}

			bs.SetRev(rev2)
			if err := bs.Put(ctx, blk2); err != nil {
				return err
			}

			bs.SetRev(rev3)
			return bs.Put(ctx, blk3)
		})
		require.NoError(t, err)

		// get blocks since rev1 (should return rev2 and rev3 blocks)
		blocksSince, err := db.GetBlocksSince(ctx, did, rev1)
		require.NoError(t, err)
		require.Len(t, blocksSince, 2)

		cidSet := make(map[string]bool)
		for _, blk := range blocksSince {
			cidSet[blk.Cid().String()] = true
		}
		require.False(t, cidSet[blk1.Cid().String()], "rev1 block should not be included")
		require.True(t, cidSet[blk2.Cid().String()], "rev2 block should be included")
		require.True(t, cidSet[blk3.Cid().String()], "rev3 block should be included")
	})

	t.Run("returns empty when since equals latest revision", func(t *testing.T) {
		t.Parallel()

		did := "did:plc:since2"
		rev := "3jqfcqzm3fo2x"
		blk := makeTestBlock(t, []byte("latest rev block"))

		err := db.Transact(func(tx fdb.Transaction) error {
			bs := db.newWriteBlockstore(did, tx)
			bs.SetRev(rev)
			return bs.Put(ctx, blk)
		})
		require.NoError(t, err)

		// get blocks since the current rev (should return nothing)
		blocksSince, err := db.GetBlocksSince(ctx, did, rev)
		require.NoError(t, err)
		require.Len(t, blocksSince, 0)
	})

	t.Run("returns all blocks when since is empty", func(t *testing.T) {
		t.Parallel()

		did := "did:plc:since3"
		rev := "3jqfcqzm3fo2y"
		blk1 := makeTestBlock(t, []byte("block A"))
		blk2 := makeTestBlock(t, []byte("block B"))

		err := db.Transact(func(tx fdb.Transaction) error {
			bs := db.newWriteBlockstore(did, tx)
			bs.SetRev(rev)
			return bs.PutMany(ctx, []blocks.Block{blk1, blk2})
		})
		require.NoError(t, err)

		// empty since should return all blocks in secondary index
		blocksSince, err := db.GetBlocksSince(ctx, did, "")
		require.NoError(t, err)
		require.Len(t, blocksSince, 2)
	})

	t.Run("handles deleted blocks gracefully", func(t *testing.T) {
		t.Parallel()

		did := "did:plc:since4"
		rev := "3jqfcqzm3fo2z"
		blk := makeTestBlock(t, []byte("to be deleted"))

		// put block with rev
		err := db.Transact(func(tx fdb.Transaction) error {
			bs := db.newWriteBlockstore(did, tx)
			bs.SetRev(rev)
			return bs.Put(ctx, blk)
		})
		require.NoError(t, err)

		// delete the block from primary index (but secondary index entry remains)
		err = db.Transact(func(tx fdb.Transaction) error {
			bs := db.newWriteBlockstore(did, tx)
			return bs.DeleteBlock(ctx, blk.Cid())
		})
		require.NoError(t, err)

		// GetBlocksSince should skip deleted blocks
		blocksSince, err := db.GetBlocksSince(ctx, did, "")
		require.NoError(t, err)
		require.Len(t, blocksSince, 0, "deleted block should be skipped")
	})

	t.Run("blocks are isolated per DID", func(t *testing.T) {
		t.Parallel()

		did1 := "did:plc:since5a"
		did2 := "did:plc:since5b"
		rev := "3jqfcqzm3fo30"
		blk1 := makeTestBlock(t, []byte("did1 since block"))
		blk2 := makeTestBlock(t, []byte("did2 since block"))

		err := db.Transact(func(tx fdb.Transaction) error {
			bs1 := db.newWriteBlockstore(did1, tx)
			bs1.SetRev(rev)
			if err := bs1.Put(ctx, blk1); err != nil {
				return err
			}

			bs2 := db.newWriteBlockstore(did2, tx)
			bs2.SetRev(rev)
			return bs2.Put(ctx, blk2)
		})
		require.NoError(t, err)

		blocks1, err := db.GetBlocksSince(ctx, did1, "")
		require.NoError(t, err)
		require.Len(t, blocks1, 1)
		require.Equal(t, blk1.Cid(), blocks1[0].Cid())

		blocks2, err := db.GetBlocksSince(ctx, did2, "")
		require.NoError(t, err)
		require.Len(t, blocks2, 1)
		require.Equal(t, blk2.Cid(), blocks2[0].Cid())
	})
}
