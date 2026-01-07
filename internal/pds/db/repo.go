package db

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"time"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/bluesky-social/indigo/atproto/atcrypto"
	"github.com/bluesky-social/indigo/atproto/repo"
	"github.com/bluesky-social/indigo/atproto/repo/mst"
	"github.com/bluesky-social/indigo/atproto/syntax"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	cbor "github.com/ipfs/go-ipld-cbor"
	"github.com/jcalabro/atlas/internal/at"
	"github.com/jcalabro/atlas/internal/metrics"
	"github.com/jcalabro/atlas/internal/types"
	"github.com/multiformats/go-multihash"
	"go.opentelemetry.io/otel/attribute"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// ErrConcurrentModification is returned when a swapCommit check fails,
// indicating another server modified the repo concurrently.
var ErrConcurrentModification = errors.New("concurrent modification detected")

// cidBuilder is used to compute CIDs for DAG-CBOR encoded data
var cidBuilder = cid.NewPrefixV1(cid.DagCBOR, multihash.SHA2_256)

// buildCarFile creates a CAR file from the given blocks with the specified root CID.
// This is used to build the blocks field of firehose events.
func buildCarFile(root cid.Cid, blks []blocks.Block) ([]byte, error) {
	var buf bytes.Buffer

	// write CAR header
	header := map[string]any{
		"version": uint64(1),
		"roots":   []cid.Cid{root},
	}
	headerBytes, err := cbor.DumpObject(header)
	if err != nil {
		return nil, fmt.Errorf("failed to encode car header: %w", err)
	}

	// write length-prefixed header
	lenBuf := make([]byte, binary.MaxVarintLen64)
	n := binary.PutUvarint(lenBuf, uint64(len(headerBytes)))
	buf.Write(lenBuf[:n])
	buf.Write(headerBytes)

	// write each block as length-prefixed (cid + data)
	for _, blk := range blks {
		cidBytes := blk.Cid().Bytes()
		dataBytes := blk.RawData()
		totalLen := len(cidBytes) + len(dataBytes)

		n := binary.PutUvarint(lenBuf, uint64(totalLen))
		buf.Write(lenBuf[:n])
		buf.Write(cidBytes)
		buf.Write(dataBytes)
	}

	return buf.Bytes(), nil
}

// InitRepo creates an empty repository for a new account.
// Returns the initial root CID and revision.
func (db *DB) InitRepo(ctx context.Context, actor *types.Actor) (commitCID cid.Cid, rev string, err error) {
	_, span, done := db.observe(ctx, "InitRepo")
	defer func() { done(err) }()

	span.SetAttributes(
		attribute.String("did", actor.Did),
		attribute.String("handle", actor.Handle),
	)

	type result struct {
		commitCID cid.Cid
		rev       string
	}

	res, err := transaction(db.db, func(tx fdb.Transaction) (*result, error) {
		// compute rev for the initial commit
		clk := syntax.NewTIDClock(0)
		newRev := clk.Next().String()

		bs := db.newWriteBlockstore(actor.Did, tx)
		bs.SetRev(newRev)

		// create an empty MST tree
		tree := mst.NewEmptyTree()

		// write tree blocks to get root CID (empty tree still has a root)
		rootCID, err := tree.WriteDiffBlocks(ctx, bs)
		if err != nil {
			return nil, fmt.Errorf("failed to write tree blocks: %w", err)
		}

		// create the commit
		commit := repo.Commit{
			DID:     actor.Did,
			Version: repo.ATPROTO_REPO_VERSION,
			Prev:    nil,
			Data:    *rootCID,
			Rev:     newRev,
		}

		// sign the commit
		privkey, err := atcrypto.ParsePrivateBytesK256(actor.SigningKey)
		if err != nil {
			return nil, fmt.Errorf("failed to parse signing key: %w", err)
		}
		if err := commit.Sign(privkey); err != nil {
			return nil, fmt.Errorf("failed to sign commit: %w", err)
		}

		// store the commit block
		commitCID, err := storeCommit(ctx, bs, &commit)
		if err != nil {
			return nil, fmt.Errorf("failed to store commit: %w", err)
		}

		return &result{commitCID: commitCID, rev: commit.Rev}, nil
	})
	if err != nil {
		return
	}

	commitCID = res.commitCID
	rev = res.rev
	return
}

// CreateRecordResult contains the result of an atomic record creation
type CreateRecordResult struct {
	RecordCID cid.Cid
	CommitCID cid.Cid
	Rev       string
}

// CreateRecord atomically creates a record in the repo. All MST operations,
// block writes, secondary index updates, and actor updates happen within a
// single FDB write transaction.
func (db *DB) CreateRecord(
	ctx context.Context,
	actor *types.Actor,
	record *types.Record,
	cborBytes []byte,
	swapCommit *string,
) (result *CreateRecordResult, err error) {
	_, span, done := db.observe(ctx, "CreateRecord")
	defer func() { done(err) }()

	span.SetAttributes(
		attribute.String("did", actor.Did),
		attribute.String("handle", actor.Handle),
		attribute.String("uri", record.URI().String()),
		attribute.Int("record_size", len(record.Value)),
		attribute.Int("cbor_size", len(cborBytes)),
		metrics.NilString("swap_commit", swapCommit),
	)

	result, err = transaction(db.db, func(tx fdb.Transaction) (*CreateRecordResult, error) {
		// check swapCommit - verify the current head hasn't been changed by
		// another process/thread attempting to write concurrently
		existing, err := db.getActorByDIDTx(tx, actor.Did)
		if err != nil {
			return nil, fmt.Errorf("failed to get current head: %w", err)
		}

		if swapCommit != nil && existing.Head != *swapCommit {
			return nil, ErrConcurrentModification
		}

		// verify head hasn't changed since we loaded the actor
		if existing.Head != actor.Head {
			return nil, ErrConcurrentModification
		}

		// load the existing commit to get the data CID and clock
		headCID, err := cid.Decode(actor.Head)
		if err != nil {
			return nil, fmt.Errorf("failed to parse repo head CID: %w", err)
		}

		bs := db.newWriteBlockstore(actor.Did, tx)
		commit, clk, err := loadCommit(ctx, bs, headCID)
		if err != nil {
			return nil, fmt.Errorf("failed to load commit: %w", err)
		}

		// compute new rev and set it on the blockstore for secondary index writes
		newRev := clk.Next().String()
		bs.SetRev(newRev)

		// enable write tracking for firehose event generation
		bs.EnableWriteTracking()

		// load the MST from the commit's data CID
		tree, err := mst.LoadTreeFromStore(ctx, bs, commit.Data)
		if err != nil {
			return nil, fmt.Errorf("failed to load MST: %w", err)
		}

		// store the record block and get its CID
		recordCID, err := cidBuilder.Sum(cborBytes)
		if err != nil {
			return nil, fmt.Errorf("failed to compute record CID: %w", err)
		}

		recordBlock, err := blocks.NewBlockWithCid(cborBytes, recordCID)
		if err != nil {
			return nil, fmt.Errorf("failed to create record block: %w", err)
		}

		if err := bs.Put(ctx, recordBlock); err != nil {
			return nil, fmt.Errorf("failed to store record block: %w", err)
		}

		// insert record into MST
		rpath := record.Collection + "/" + record.Rkey
		if _, err := tree.Insert([]byte(rpath), recordCID); err != nil {
			return nil, fmt.Errorf("failed to insert record into MST: %w", err)
		}

		// write dirty MST blocks and get new root CID
		rootCID, err := tree.WriteDiffBlocks(ctx, bs)
		if err != nil {
			return nil, fmt.Errorf("failed to write MST blocks: %w", err)
		}

		// create and sign new commit
		newCommit := repo.Commit{
			DID:     actor.Did,
			Version: repo.ATPROTO_REPO_VERSION,
			Prev:    &headCID,
			Data:    *rootCID,
			Rev:     newRev,
		}

		privkey, err := atcrypto.ParsePrivateBytesK256(actor.SigningKey)
		if err != nil {
			return nil, fmt.Errorf("failed to parse signing key: %w", err)
		}
		if err := newCommit.Sign(privkey); err != nil {
			return nil, fmt.Errorf("failed to sign commit: %w", err)
		}

		// store the commit block
		commitCID, err := storeCommit(ctx, bs, &newCommit)
		if err != nil {
			return nil, fmt.Errorf("failed to store commit: %w", err)
		}

		// save record to the records secondary index
		record.Cid = recordCID.String()
		if err := db.saveRecordTx(tx, record); err != nil {
			return nil, fmt.Errorf("failed to save record: %w", err)
		}

		// update collection count index
		db.incrementCollectionCountTx(tx, actor.Did, record.Collection)

		// update actor with new head and rev
		actor.Head = commitCID.String()
		actor.Rev = newCommit.Rev
		if err := db.saveActorTx(tx, actor); err != nil {
			return nil, fmt.Errorf("failed to save actor: %w", err)
		}

		// build and write firehose event
		carBytes, err := buildCarFile(commitCID, bs.GetWriteLog())
		if err != nil {
			return nil, fmt.Errorf("failed to build CAR file: %w", err)
		}

		event := &types.RepoEvent{
			PdsHost: actor.PdsHost,
			Repo:    actor.Did,
			Rev:     newRev,
			Since:   commit.Rev,
			Commit:  commitCID.Bytes(),
			Blocks:  carBytes,
			Ops: []*types.RepoOp{{
				Action: "create",
				Path:   rpath,
				Cid:    recordCID.Bytes(),
			}},
			Time: timestamppb.New(time.Now()),
		}
		if err := db.WriteEventTx(tx, event); err != nil {
			return nil, fmt.Errorf("failed to write firehose event: %w", err)
		}

		return &CreateRecordResult{
			RecordCID: recordCID,
			CommitCID: commitCID,
			Rev:       newCommit.Rev,
		}, nil
	})

	return
}

// PutRecordResult contains the result of an atomic record put (create or update)
type PutRecordResult struct {
	RecordCID cid.Cid
	CommitCID cid.Cid
	Rev       string
}

// PutRecord atomically creates or updates a record in the repo. All MST operations,
// block writes, secondary index updates, and actor updates happen within a
// single FDB write transaction.
func (db *DB) PutRecord(
	ctx context.Context,
	actor *types.Actor,
	record *types.Record,
	cborBytes []byte,
	swapRecord *string,
	swapCommit *string,
) (result *PutRecordResult, err error) {
	_, span, done := db.observe(ctx, "PutRecord")
	defer func() { done(err) }()

	span.SetAttributes(
		attribute.String("did", actor.Did),
		attribute.String("handle", actor.Handle),
		attribute.String("uri", record.URI().String()),
		attribute.Int("record_size", len(record.Value)),
		attribute.Int("cbor_size", len(cborBytes)),
		metrics.NilString("swap_record", swapRecord),
		metrics.NilString("swap_commit", swapCommit),
	)

	result, err = transaction(db.db, func(tx fdb.Transaction) (*PutRecordResult, error) {
		// check swapCommit - verify the current head hasn't been changed by
		// another process/thread attempting to write concurrently
		existing, err := db.getActorByDIDTx(tx, actor.Did)
		if err != nil {
			return nil, fmt.Errorf("failed to get current head: %w", err)
		}

		if swapCommit != nil && existing.Head != *swapCommit {
			return nil, ErrConcurrentModification
		}

		// verify head hasn't changed since we loaded the actor
		if existing.Head != actor.Head {
			return nil, ErrConcurrentModification
		}

		// load the existing commit to get the data CID and clock
		headCID, err := cid.Decode(actor.Head)
		if err != nil {
			return nil, fmt.Errorf("failed to parse repo head CID: %w", err)
		}

		bs := db.newWriteBlockstore(actor.Did, tx)
		commit, clk, err := loadCommit(ctx, bs, headCID)
		if err != nil {
			return nil, fmt.Errorf("failed to load commit: %w", err)
		}

		// compute new rev and set it on the blockstore for secondary index writes
		newRev := clk.Next().String()
		bs.SetRev(newRev)

		// enable write tracking for firehose event generation
		bs.EnableWriteTracking()

		// load the MST from the commit's data CID
		tree, err := mst.LoadTreeFromStore(ctx, bs, commit.Data)
		if err != nil {
			return nil, fmt.Errorf("failed to load MST: %w", err)
		}

		// check if record already exists in the MST
		rpath := []byte(record.Collection + "/" + record.Rkey)
		existingCID, err := tree.Get(rpath)
		isNewRecord := err != nil || existingCID == nil

		// if swapRecord is provided, verify the existing record's CID matches
		if swapRecord != nil {
			if isNewRecord {
				return nil, fmt.Errorf("swapRecord provided but record does not exist")
			}
			if existingCID.String() != *swapRecord {
				return nil, ErrConcurrentModification
			}
		}

		// store the record block and get its CID
		recordCID, err := cidBuilder.Sum(cborBytes)
		if err != nil {
			return nil, fmt.Errorf("failed to compute record CID: %w", err)
		}

		recordBlock, err := blocks.NewBlockWithCid(cborBytes, recordCID)
		if err != nil {
			return nil, fmt.Errorf("failed to create record block: %w", err)
		}

		if err := bs.Put(ctx, recordBlock); err != nil {
			return nil, fmt.Errorf("failed to store record block: %w", err)
		}

		// for updates, remove the old record first, then insert
		// MST doesn't have an Update method, so we Remove then Insert
		if !isNewRecord {
			if _, err := tree.Remove(rpath); err != nil {
				return nil, fmt.Errorf("failed to remove old record from MST: %w", err)
			}
		}

		if _, err := tree.Insert(rpath, recordCID); err != nil {
			return nil, fmt.Errorf("failed to insert record into MST: %w", err)
		}

		// write dirty MST blocks and get new root CID
		rootCID, err := tree.WriteDiffBlocks(ctx, bs)
		if err != nil {
			return nil, fmt.Errorf("failed to write MST blocks: %w", err)
		}

		// create and sign new commit
		newCommit := repo.Commit{
			DID:     actor.Did,
			Version: repo.ATPROTO_REPO_VERSION,
			Prev:    &headCID,
			Data:    *rootCID,
			Rev:     newRev,
		}

		privkey, err := atcrypto.ParsePrivateBytesK256(actor.SigningKey)
		if err != nil {
			return nil, fmt.Errorf("failed to parse signing key: %w", err)
		}
		if err := newCommit.Sign(privkey); err != nil {
			return nil, fmt.Errorf("failed to sign commit: %w", err)
		}

		// store the commit block
		commitCID, err := storeCommit(ctx, bs, &newCommit)
		if err != nil {
			return nil, fmt.Errorf("failed to store commit: %w", err)
		}

		// save record to the records secondary index
		record.Cid = recordCID.String()
		if err := db.saveRecordTx(tx, record); err != nil {
			return nil, fmt.Errorf("failed to save record: %w", err)
		}

		// only update collection count for new records
		if isNewRecord {
			db.incrementCollectionCountTx(tx, actor.Did, record.Collection)
		}

		// update actor with new head and rev
		actor.Head = commitCID.String()
		actor.Rev = newCommit.Rev
		if err := db.saveActorTx(tx, actor); err != nil {
			return nil, fmt.Errorf("failed to save actor: %w", err)
		}

		// build and write firehose event
		carBytes, err := buildCarFile(commitCID, bs.GetWriteLog())
		if err != nil {
			return nil, fmt.Errorf("failed to build CAR file: %w", err)
		}

		action := "update"
		if isNewRecord {
			action = "create"
		}
		event := &types.RepoEvent{
			PdsHost: actor.PdsHost,
			Repo:    actor.Did,
			Rev:     newRev,
			Since:   commit.Rev,
			Commit:  commitCID.Bytes(),
			Blocks:  carBytes,
			Ops: []*types.RepoOp{{
				Action: action,
				Path:   string(rpath),
				Cid:    recordCID.Bytes(),
			}},
			Time: timestamppb.New(time.Now()),
		}
		if err := db.WriteEventTx(tx, event); err != nil {
			return nil, fmt.Errorf("failed to write firehose event: %w", err)
		}

		return &PutRecordResult{
			RecordCID: recordCID,
			CommitCID: commitCID,
			Rev:       newCommit.Rev,
		}, nil
	})

	return
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
) (result *DeleteRecordResult, err error) {
	_, span, done := db.observe(ctx, "DeleteRecord")
	defer func() { done(err) }()

	span.SetAttributes(
		attribute.String("did", actor.Did),
		attribute.String("handle", actor.Handle),
		attribute.String("uri", uri.String()),
		metrics.NilString("swap_commit", swapCommit),
	)

	result, err = transaction(db.db, func(tx fdb.Transaction) (*DeleteRecordResult, error) {
		// check swapCommit - verify the current head hasn't changed
		existing, err := db.getActorByDIDTx(tx, actor.Did)
		if err != nil {
			return nil, fmt.Errorf("failed to get current head: %w", err)
		}

		if swapCommit != nil && existing.Head != *swapCommit {
			return nil, ErrConcurrentModification
		}

		// verify head hasn't changed since we loaded the actor
		if existing.Head != actor.Head {
			return nil, ErrConcurrentModification
		}

		// load the existing commit to get the data CID and clock
		headCID, err := cid.Decode(actor.Head)
		if err != nil {
			return nil, fmt.Errorf("failed to parse repo head CID: %w", err)
		}

		bs := db.newWriteBlockstore(actor.Did, tx)
		commit, clk, err := loadCommit(ctx, bs, headCID)
		if err != nil {
			return nil, fmt.Errorf("failed to load commit: %w", err)
		}

		// compute new rev and set it on the blockstore for secondary index writes
		newRev := clk.Next().String()
		bs.SetRev(newRev)

		// enable write tracking for firehose event generation
		bs.EnableWriteTracking()

		// load the MST from the commit's data CID
		tree, err := mst.LoadTreeFromStore(ctx, bs, commit.Data)
		if err != nil {
			return nil, fmt.Errorf("failed to load MST: %w", err)
		}

		// remove record from MST
		rpath := uri.Collection + "/" + uri.Rkey
		if _, err := tree.Remove([]byte(rpath)); err != nil {
			return nil, fmt.Errorf("failed to remove record from MST: %w", err)
		}

		// write dirty MST blocks and get new root CID
		rootCID, err := tree.WriteDiffBlocks(ctx, bs)
		if err != nil {
			return nil, fmt.Errorf("failed to write MST blocks: %w", err)
		}

		// create and sign new commit
		newCommit := repo.Commit{
			DID:     actor.Did,
			Version: repo.ATPROTO_REPO_VERSION,
			Prev:    &headCID,
			Data:    *rootCID,
			Rev:     newRev,
		}

		privkey, err := atcrypto.ParsePrivateBytesK256(actor.SigningKey)
		if err != nil {
			return nil, fmt.Errorf("failed to parse signing key: %w", err)
		}
		if err := newCommit.Sign(privkey); err != nil {
			return nil, fmt.Errorf("failed to sign commit: %w", err)
		}

		// store the commit block
		commitCID, err := storeCommit(ctx, bs, &newCommit)
		if err != nil {
			return nil, fmt.Errorf("failed to store commit: %w", err)
		}

		// delete record from secondary index
		db.DeleteRecordTx(tx, uri)

		// update collection count index
		db.decrementCollectionCountTx(tx, actor.Did, uri.Collection)

		// update actor with new head and rev
		actor.Head = commitCID.String()
		actor.Rev = newCommit.Rev
		if err := db.saveActorTx(tx, actor); err != nil {
			return nil, fmt.Errorf("failed to save actor: %w", err)
		}

		// build and write firehose event
		carBytes, err := buildCarFile(commitCID, bs.GetWriteLog())
		if err != nil {
			return nil, fmt.Errorf("failed to build CAR file: %w", err)
		}

		event := &types.RepoEvent{
			PdsHost: actor.PdsHost,
			Repo:    actor.Did,
			Rev:     newRev,
			Since:   commit.Rev,
			Commit:  commitCID.Bytes(),
			Blocks:  carBytes,
			Ops: []*types.RepoOp{{
				Action: "delete",
				Path:   rpath,
				// CID is nil for deletes
			}},
			Time: timestamppb.New(time.Now()),
		}
		if err := db.WriteEventTx(tx, event); err != nil {
			return nil, fmt.Errorf("failed to write firehose event: %w", err)
		}

		return &DeleteRecordResult{
			CommitCID: commitCID,
			Rev:       newCommit.Rev,
		}, nil
	})

	return
}

// WriteOp represents a single operation in an applyWrites batch
type WriteOp struct {
	Action     string // "create", "update", or "delete"
	Collection string
	Rkey       string
	Value      []byte // CBOR-encoded record data (nil for delete)
}

// WriteOpResult contains the result of a single write operation
type WriteOpResult struct {
	Action      string
	URI         string
	CID         string // empty for delete
	IsNewRecord bool   // true if this was a create (vs update)
}

// ApplyWritesResult contains the result of an atomic batch write
type ApplyWritesResult struct {
	CommitCID cid.Cid
	Rev       string
	Results   []WriteOpResult
}

// ApplyWrites atomically applies multiple write operations to a repo.
// All MST operations, block writes, secondary index updates, and actor updates
// happen within a single FDB write transaction.
func (db *DB) ApplyWrites(
	ctx context.Context,
	actor *types.Actor,
	ops []WriteOp,
	swapCommit *string,
) (result *ApplyWritesResult, err error) {
	_, span, done := db.observe(ctx, "ApplyWrites")
	defer func() { done(err) }()

	span.SetAttributes(
		attribute.String("did", actor.Did),
		attribute.String("handle", actor.Handle),
		attribute.Int("num_ops", len(ops)),
		metrics.NilString("swap_commit", swapCommit),
	)

	result, err = transaction(db.db, func(tx fdb.Transaction) (*ApplyWritesResult, error) {
		// check swapCommit - verify the current head hasn't been changed
		existing, err := db.getActorByDIDTx(tx, actor.Did)
		if err != nil {
			return nil, fmt.Errorf("failed to get current head: %w", err)
		}

		if swapCommit != nil && existing.Head != *swapCommit {
			return nil, ErrConcurrentModification
		}

		// verify head hasn't changed since we loaded the actor
		if existing.Head != actor.Head {
			return nil, ErrConcurrentModification
		}

		// load the existing commit to get the data CID and clock
		headCID, err := cid.Decode(actor.Head)
		if err != nil {
			return nil, fmt.Errorf("failed to parse repo head CID: %w", err)
		}

		bs := db.newWriteBlockstore(actor.Did, tx)
		commit, clk, err := loadCommit(ctx, bs, headCID)
		if err != nil {
			return nil, fmt.Errorf("failed to load commit: %w", err)
		}

		// compute new rev and set it on the blockstore
		newRev := clk.Next().String()
		bs.SetRev(newRev)

		// enable write tracking for firehose event generation
		bs.EnableWriteTracking()

		// load the MST from the commit's data CID
		tree, err := mst.LoadTreeFromStore(ctx, bs, commit.Data)
		if err != nil {
			return nil, fmt.Errorf("failed to load MST: %w", err)
		}

		results := make([]WriteOpResult, 0, len(ops))
		repoOps := make([]*types.RepoOp, 0, len(ops))

		for _, op := range ops {
			rpath := []byte(op.Collection + "/" + op.Rkey)
			uri := "at://" + actor.Did + "/" + op.Collection + "/" + op.Rkey

			switch op.Action {
			case "create":
				// store the record block and get its CID
				recordCID, err := cidBuilder.Sum(op.Value)
				if err != nil {
					return nil, fmt.Errorf("failed to compute record CID: %w", err)
				}

				recordBlock, err := blocks.NewBlockWithCid(op.Value, recordCID)
				if err != nil {
					return nil, fmt.Errorf("failed to create record block: %w", err)
				}

				if err := bs.Put(ctx, recordBlock); err != nil {
					return nil, fmt.Errorf("failed to store record block: %w", err)
				}

				// insert into MST
				if _, err := tree.Insert(rpath, recordCID); err != nil {
					return nil, fmt.Errorf("failed to insert record into MST: %w", err)
				}

				// save to secondary index
				record := &types.Record{
					Did:        actor.Did,
					Collection: op.Collection,
					Rkey:       op.Rkey,
					Cid:        recordCID.String(),
					Value:      op.Value,
					CreatedAt:  timestamppb.Now(),
				}
				if err := db.saveRecordTx(tx, record); err != nil {
					return nil, fmt.Errorf("failed to save record: %w", err)
				}

				db.incrementCollectionCountTx(tx, actor.Did, op.Collection)

				results = append(results, WriteOpResult{
					Action:      "create",
					URI:         uri,
					CID:         recordCID.String(),
					IsNewRecord: true,
				})
				repoOps = append(repoOps, &types.RepoOp{
					Action: "create",
					Path:   string(rpath),
					Cid:    recordCID.Bytes(),
				})

			case "update":
				// store the record block and get its CID
				recordCID, err := cidBuilder.Sum(op.Value)
				if err != nil {
					return nil, fmt.Errorf("failed to compute record CID: %w", err)
				}

				recordBlock, err := blocks.NewBlockWithCid(op.Value, recordCID)
				if err != nil {
					return nil, fmt.Errorf("failed to create record block: %w", err)
				}

				if err := bs.Put(ctx, recordBlock); err != nil {
					return nil, fmt.Errorf("failed to store record block: %w", err)
				}

				// check if record exists
				existingCID, err := tree.Get(rpath)
				isNewRecord := err != nil || existingCID == nil

				// remove old record if it exists
				if !isNewRecord {
					if _, err := tree.Remove(rpath); err != nil {
						return nil, fmt.Errorf("failed to remove old record from MST: %w", err)
					}
				}

				// insert new record
				if _, err := tree.Insert(rpath, recordCID); err != nil {
					return nil, fmt.Errorf("failed to insert record into MST: %w", err)
				}

				// save to secondary index
				record := &types.Record{
					Did:        actor.Did,
					Collection: op.Collection,
					Rkey:       op.Rkey,
					Cid:        recordCID.String(),
					Value:      op.Value,
					CreatedAt:  timestamppb.Now(),
				}
				if err := db.saveRecordTx(tx, record); err != nil {
					return nil, fmt.Errorf("failed to save record: %w", err)
				}

				if isNewRecord {
					db.incrementCollectionCountTx(tx, actor.Did, op.Collection)
				}

				action := "update"
				if isNewRecord {
					action = "create"
				}
				results = append(results, WriteOpResult{
					Action:      action,
					URI:         uri,
					CID:         recordCID.String(),
					IsNewRecord: isNewRecord,
				})
				repoOps = append(repoOps, &types.RepoOp{
					Action: action,
					Path:   string(rpath),
					Cid:    recordCID.Bytes(),
				})

			case "delete":
				// remove from MST
				if _, err := tree.Remove(rpath); err != nil {
					return nil, fmt.Errorf("failed to remove record from MST: %w", err)
				}

				// delete from secondary index
				aturi := &at.URI{Repo: actor.Did, Collection: op.Collection, Rkey: op.Rkey}
				db.DeleteRecordTx(tx, aturi)

				db.decrementCollectionCountTx(tx, actor.Did, op.Collection)

				results = append(results, WriteOpResult{
					Action: "delete",
					URI:    uri,
				})
				repoOps = append(repoOps, &types.RepoOp{
					Action: "delete",
					Path:   string(rpath),
				})

			default:
				return nil, fmt.Errorf("unknown action: %s", op.Action)
			}
		}

		// write dirty MST blocks and get new root CID
		rootCID, err := tree.WriteDiffBlocks(ctx, bs)
		if err != nil {
			return nil, fmt.Errorf("failed to write MST blocks: %w", err)
		}

		// create and sign new commit
		newCommit := repo.Commit{
			DID:     actor.Did,
			Version: repo.ATPROTO_REPO_VERSION,
			Prev:    &headCID,
			Data:    *rootCID,
			Rev:     newRev,
		}

		privkey, err := atcrypto.ParsePrivateBytesK256(actor.SigningKey)
		if err != nil {
			return nil, fmt.Errorf("failed to parse signing key: %w", err)
		}
		if err := newCommit.Sign(privkey); err != nil {
			return nil, fmt.Errorf("failed to sign commit: %w", err)
		}

		// store the commit block
		commitCID, err := storeCommit(ctx, bs, &newCommit)
		if err != nil {
			return nil, fmt.Errorf("failed to store commit: %w", err)
		}

		// update actor with new head and rev
		actor.Head = commitCID.String()
		actor.Rev = newCommit.Rev
		if err := db.saveActorTx(tx, actor); err != nil {
			return nil, fmt.Errorf("failed to save actor: %w", err)
		}

		// build and write firehose event
		carBytes, err := buildCarFile(commitCID, bs.GetWriteLog())
		if err != nil {
			return nil, fmt.Errorf("failed to build CAR file: %w", err)
		}

		event := &types.RepoEvent{
			PdsHost: actor.PdsHost,
			Repo:    actor.Did,
			Rev:     newRev,
			Since:   commit.Rev,
			Commit:  commitCID.Bytes(),
			Blocks:  carBytes,
			Ops:     repoOps,
			Time:    timestamppb.New(time.Now()),
		}
		if err := db.WriteEventTx(tx, event); err != nil {
			return nil, fmt.Errorf("failed to write firehose event: %w", err)
		}

		return &ApplyWritesResult{
			CommitCID: commitCID,
			Rev:       newCommit.Rev,
			Results:   results,
		}, nil
	})

	return
}

// loadCommit loads a commit from the blockstore and returns it along with a TID clock
// initialized from the commit's rev.
func loadCommit(ctx context.Context, bs *blockstore, commitCID cid.Cid) (*repo.Commit, *syntax.TIDClock, error) {
	blk, err := bs.Get(ctx, commitCID)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get commit block: %w", err)
	}

	var commit repo.Commit
	if err := commit.UnmarshalCBOR(bytes.NewReader(blk.RawData())); err != nil {
		return nil, nil, fmt.Errorf("failed to unmarshal commit: %w", err)
	}

	clk := syntax.ClockFromTID(syntax.TID(commit.Rev))
	return &commit, &clk, nil
}

// storeCommit serializes and stores a commit block, returning its CID.
func storeCommit(ctx context.Context, bs *blockstore, commit *repo.Commit) (cid.Cid, error) {
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
