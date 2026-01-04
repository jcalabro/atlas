package foundation

import (
	"bytes"
	"context"
	"errors"
	"fmt"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/bluesky-social/indigo/atproto/atcrypto"
	"github.com/bluesky-social/indigo/atproto/repo"
	"github.com/bluesky-social/indigo/atproto/repo/mst"
	"github.com/bluesky-social/indigo/atproto/syntax"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"github.com/jcalabro/atlas/internal/at"
	"github.com/jcalabro/atlas/internal/types"
	"github.com/multiformats/go-multihash"
)

// ErrConcurrentModification is returned when a swapCommit check fails,
// indicating another server modified the repo concurrently.
var ErrConcurrentModification = errors.New("concurrent modification detected")

// cidBuilder is used to compute CIDs for DAG-CBOR encoded data
var cidBuilder = cid.NewPrefixV1(cid.DagCBOR, multihash.SHA2_256)

// InitRepo creates an empty repository for a new account.
// Returns the initial root CID and revision.
func (db *DB) InitRepo(ctx context.Context, actor *types.Actor) (cid.Cid, string, error) {
	var commitCID cid.Cid
	var rev string

	err := db.Transact(func(tx fdb.Transaction) error {
		bs := db.NewWriteBlockstore(actor.Did, tx)

		// create an empty MST tree
		tree := mst.NewEmptyTree()

		// write tree blocks to get root CID (empty tree still has a root)
		rootCID, err := tree.WriteDiffBlocks(ctx, bs)
		if err != nil {
			return fmt.Errorf("failed to write tree blocks: %w", err)
		}

		// create the commit
		clk := syntax.NewTIDClock(0)
		commit := repo.Commit{
			DID:     actor.Did,
			Version: repo.ATPROTO_REPO_VERSION,
			Prev:    nil,
			Data:    *rootCID,
			Rev:     clk.Next().String(),
		}

		// sign the commit
		privkey, err := atcrypto.ParsePrivateBytesK256(actor.SigningKey)
		if err != nil {
			return fmt.Errorf("failed to parse signing key: %w", err)
		}
		if err := commit.Sign(privkey); err != nil {
			return fmt.Errorf("failed to sign commit: %w", err)
		}

		// store the commit block
		commitCID, err = storeCommit(ctx, bs, &commit)
		if err != nil {
			return fmt.Errorf("failed to store commit: %w", err)
		}

		rev = commit.Rev
		return nil
	})
	if err != nil {
		return cid.Undef, "", err
	}

	return commitCID, rev, nil
}

// CreateRecordResult contains the result of an atomic record creation.
type CreateRecordResult struct {
	RecordCID cid.Cid
	CommitCID cid.Cid
	Rev       string
}

// CreateRecord atomically creates a record in the repo.
// All MST operations, block writes, secondary index updates, and actor updates
// happen within a single FDB transaction.
func (db *DB) CreateRecord(
	ctx context.Context,
	actor *types.Actor,
	record *types.Record,
	cborBytes []byte,
	swapCommit *string,
) (*CreateRecordResult, error) {
	var result CreateRecordResult

	err := db.Transact(func(tx fdb.Transaction) error {
		// check swapCommit - verify the current head hasn't changed
		currentHead, err := db.GetActorHeadTx(tx, actor.Did)
		if err != nil {
			return fmt.Errorf("failed to get current head: %w", err)
		}

		if swapCommit != nil && currentHead != *swapCommit {
			return ErrConcurrentModification
		}

		// verify head hasn't changed since we loaded the actor
		if currentHead != actor.Head {
			return ErrConcurrentModification
		}

		// create transactional blockstore
		bs := db.NewWriteBlockstore(actor.Did, tx)

		// load the existing commit to get the data CID and clock
		headCID, err := cid.Decode(actor.Head)
		if err != nil {
			return fmt.Errorf("failed to parse repo head CID: %w", err)
		}

		commit, clk, err := loadCommit(ctx, bs, headCID)
		if err != nil {
			return fmt.Errorf("failed to load commit: %w", err)
		}

		// load the MST from the commit's data CID
		tree, err := mst.LoadTreeFromStore(ctx, bs, commit.Data)
		if err != nil {
			return fmt.Errorf("failed to load MST: %w", err)
		}

		// store the record block and get its CID
		recordCID, err := cidBuilder.Sum(cborBytes)
		if err != nil {
			return fmt.Errorf("failed to compute record CID: %w", err)
		}

		recordBlock, err := blocks.NewBlockWithCid(cborBytes, recordCID)
		if err != nil {
			return fmt.Errorf("failed to create record block: %w", err)
		}

		if err := bs.Put(ctx, recordBlock); err != nil {
			return fmt.Errorf("failed to store record block: %w", err)
		}

		// insert record into MST
		rpath := record.Collection + "/" + record.Rkey
		if _, err := tree.Insert([]byte(rpath), recordCID); err != nil {
			return fmt.Errorf("failed to insert record into MST: %w", err)
		}

		// write dirty MST blocks and get new root CID
		rootCID, err := tree.WriteDiffBlocks(ctx, bs)
		if err != nil {
			return fmt.Errorf("failed to write MST blocks: %w", err)
		}

		// create and sign new commit
		newCommit := repo.Commit{
			DID:     actor.Did,
			Version: repo.ATPROTO_REPO_VERSION,
			Prev:    &headCID,
			Data:    *rootCID,
			Rev:     clk.Next().String(),
		}

		privkey, err := atcrypto.ParsePrivateBytesK256(actor.SigningKey)
		if err != nil {
			return fmt.Errorf("failed to parse signing key: %w", err)
		}
		if err := newCommit.Sign(privkey); err != nil {
			return fmt.Errorf("failed to sign commit: %w", err)
		}

		// store the commit block
		commitCID, err := storeCommit(ctx, bs, &newCommit)
		if err != nil {
			return fmt.Errorf("failed to store commit: %w", err)
		}

		// save record to secondary index
		record.Cid = recordCID.String()
		if err := db.SaveRecordTx(tx, record); err != nil {
			return fmt.Errorf("failed to save record: %w", err)
		}

		// update actor with new head and rev
		actor.Head = commitCID.String()
		actor.Rev = newCommit.Rev
		if err := db.SaveActorTx(tx, actor); err != nil {
			return fmt.Errorf("failed to save actor: %w", err)
		}

		result = CreateRecordResult{
			RecordCID: recordCID,
			CommitCID: commitCID,
			Rev:       newCommit.Rev,
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	return &result, nil
}

// DeleteRecordResult contains the result of an atomic record deletion.
type DeleteRecordResult struct {
	CommitCID cid.Cid
	Rev       string
}

// DeleteRecord atomically deletes a record from the repo.
// All MST operations, block writes, secondary index updates, and actor updates
// happen within a single FDB transaction.
func (db *DB) DeleteRecord(
	ctx context.Context,
	actor *types.Actor,
	uri *at.URI,
	swapCommit *string,
) (*DeleteRecordResult, error) {
	var result DeleteRecordResult

	err := db.Transact(func(tx fdb.Transaction) error {
		// check swapCommit - verify the current head hasn't changed
		currentHead, err := db.GetActorHeadTx(tx, actor.Did)
		if err != nil {
			return fmt.Errorf("failed to get current head: %w", err)
		}

		if swapCommit != nil && currentHead != *swapCommit {
			return ErrConcurrentModification
		}

		// verify head hasn't changed since we loaded the actor
		if currentHead != actor.Head {
			return ErrConcurrentModification
		}

		// create transactional blockstore
		bs := db.NewWriteBlockstore(actor.Did, tx)

		// load the existing commit to get the data CID and clock
		headCID, err := cid.Decode(actor.Head)
		if err != nil {
			return fmt.Errorf("failed to parse repo head CID: %w", err)
		}

		commit, clk, err := loadCommit(ctx, bs, headCID)
		if err != nil {
			return fmt.Errorf("failed to load commit: %w", err)
		}

		// load the MST from the commit's data CID
		tree, err := mst.LoadTreeFromStore(ctx, bs, commit.Data)
		if err != nil {
			return fmt.Errorf("failed to load MST: %w", err)
		}

		// remove record from MST
		rpath := uri.Collection + "/" + uri.Rkey
		if _, err := tree.Remove([]byte(rpath)); err != nil {
			return fmt.Errorf("failed to remove record from MST: %w", err)
		}

		// write dirty MST blocks and get new root CID
		rootCID, err := tree.WriteDiffBlocks(ctx, bs)
		if err != nil {
			return fmt.Errorf("failed to write MST blocks: %w", err)
		}

		// create and sign new commit
		newCommit := repo.Commit{
			DID:     actor.Did,
			Version: repo.ATPROTO_REPO_VERSION,
			Prev:    &headCID,
			Data:    *rootCID,
			Rev:     clk.Next().String(),
		}

		privkey, err := atcrypto.ParsePrivateBytesK256(actor.SigningKey)
		if err != nil {
			return fmt.Errorf("failed to parse signing key: %w", err)
		}
		if err := newCommit.Sign(privkey); err != nil {
			return fmt.Errorf("failed to sign commit: %w", err)
		}

		// store the commit block
		commitCID, err := storeCommit(ctx, bs, &newCommit)
		if err != nil {
			return fmt.Errorf("failed to store commit: %w", err)
		}

		// delete record from secondary index
		db.DeleteRecordTx(tx, uri)

		// update actor with new head and rev
		actor.Head = commitCID.String()
		actor.Rev = newCommit.Rev
		if err := db.SaveActorTx(tx, actor); err != nil {
			return fmt.Errorf("failed to save actor: %w", err)
		}

		result = DeleteRecordResult{
			CommitCID: commitCID,
			Rev:       newCommit.Rev,
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	return &result, nil
}

// loadCommit loads a commit from the blockstore and returns it along with a TID clock
// initialized from the commit's rev.
func loadCommit(ctx context.Context, bs *Blockstore, commitCID cid.Cid) (*repo.Commit, *syntax.TIDClock, error) {
	blk, err := bs.Get(ctx, commitCID)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get commit block: %w", err)
	}

	var commit repo.Commit
	if err := commit.UnmarshalCBOR(bytes.NewReader(blk.RawData())); err != nil {
		return nil, nil, fmt.Errorf("failed to unmarshal commit: %w", err)
	}

	// initialize clock from the commit's rev
	clk := syntax.ClockFromTID(syntax.TID(commit.Rev))
	return &commit, &clk, nil
}

// storeCommit serializes and stores a commit block, returning its CID.
func storeCommit(ctx context.Context, bs *Blockstore, commit *repo.Commit) (cid.Cid, error) {
	buf := new(bytes.Buffer)
	if err := commit.MarshalCBOR(buf); err != nil {
		return cid.Undef, fmt.Errorf("failed to marshal commit: %w", err)
	}

	commitBytes := buf.Bytes()
	commitCID, err := cidBuilder.Sum(commitBytes)
	if err != nil {
		return cid.Undef, fmt.Errorf("failed to compute commit CID: %w", err)
	}

	commitBlock, err := blocks.NewBlockWithCid(commitBytes, commitCID)
	if err != nil {
		return cid.Undef, fmt.Errorf("failed to create commit block: %w", err)
	}

	if err := bs.Put(ctx, commitBlock); err != nil {
		return cid.Undef, fmt.Errorf("failed to store commit block: %w", err)
	}

	return commitCID, nil
}
