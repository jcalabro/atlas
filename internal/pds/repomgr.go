package pds

import (
	"context"
	"errors"
	"fmt"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/bluesky-social/indigo/atproto/atcrypto"
	"github.com/bluesky-social/indigo/repo"
	"github.com/ipfs/go-cid"
	"github.com/jcalabro/atlas/internal/at"
	"github.com/jcalabro/atlas/internal/foundation"
	"github.com/jcalabro/atlas/internal/types"
)

// ErrConcurrentModification is returned when a swapCommit check fails,
// indicating another server modified the repo concurrently.
var ErrConcurrentModification = errors.New("concurrent modification detected")

// RepoManager handles ATProto repository operations using the MST.
type RepoManager struct {
	db *foundation.DB
}

// NewRepoManager creates a new repo manager.
func NewRepoManager(db *foundation.DB) *RepoManager {
	return &RepoManager{db: db}
}

// InitRepo creates an empty repository for a new account.
// Returns the initial root CID and revision.
func (rm *RepoManager) InitRepo(ctx context.Context, actor *types.Actor) (cid.Cid, string, error) {
	bs := rm.db.NewBlockstore(actor.Did)

	// create a new empty repo
	r := repo.NewRepo(ctx, actor.Did, bs)

	// create signing function using the actor's signing key
	signFn := makeSigningFunc(actor.SigningKey)

	// commit the empty repo to get initial root and rev
	root, rev, err := r.Commit(ctx, signFn)
	if err != nil {
		return cid.Undef, "", fmt.Errorf("failed to commit initial repo: %w", err)
	}

	// flush blocks to FDB
	if err := bs.Flush(ctx); err != nil {
		return cid.Undef, "", fmt.Errorf("failed to flush blocks: %w", err)
	}

	return root, rev, nil
}

// OpenRepo opens an existing repository for the given actor.
func (rm *RepoManager) OpenRepo(ctx context.Context, actor *types.Actor) (*repo.Repo, *foundation.Blockstore, error) {
	if actor.Head == "" {
		return nil, nil, fmt.Errorf("actor has no repo head")
	}

	rootCID, err := cid.Decode(actor.Head)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to parse repo head CID: %w", err)
	}

	bs := rm.db.NewBlockstore(actor.Did)

	r, err := repo.OpenRepo(ctx, bs, rootCID)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to open repo: %w", err)
	}

	return r, bs, nil
}

// CommitRepo commits changes to a repo and flushes blocks.
// Returns the new root CID and revision.
// Deprecated: Use CommitCreateRecord or CommitDeleteRecord for atomic operations.
func (rm *RepoManager) CommitRepo(ctx context.Context, r *repo.Repo, bs *foundation.Blockstore, signingKey []byte) (cid.Cid, string, error) {
	signFn := makeSigningFunc(signingKey)

	root, rev, err := r.Commit(ctx, signFn)
	if err != nil {
		return cid.Undef, "", fmt.Errorf("failed to commit repo: %w", err)
	}

	if err := bs.Flush(ctx); err != nil {
		return cid.Undef, "", fmt.Errorf("failed to flush blocks: %w", err)
	}

	return root, rev, nil
}

// CreateRecordResult contains the result of an atomic record creation.
type CreateRecordResult struct {
	RecordCID cid.Cid
	CommitCID cid.Cid
	Rev       string
}

// CommitCreateRecord atomically creates a record in the repo.
// It commits the MST changes, saves the record, and updates the actor in a single transaction.
// If swapCommit is provided, the operation fails if the current head doesn't match.
func (rm *RepoManager) CommitCreateRecord(
	ctx context.Context,
	r *repo.Repo,
	bs *foundation.Blockstore,
	actor *types.Actor,
	record *types.Record,
	swapCommit *string,
) (*CreateRecordResult, error) {
	signFn := makeSigningFunc(actor.SigningKey)

	// commit the repo to get the new root and rev
	rootCID, rev, err := r.Commit(ctx, signFn)
	if err != nil {
		return nil, fmt.Errorf("failed to commit repo: %w", err)
	}

	// parse record CID
	recordCID, err := cid.Decode(record.Cid)
	if err != nil {
		return nil, fmt.Errorf("failed to parse record CID: %w", err)
	}

	// perform all writes atomically
	err = rm.db.Transact(func(tx fdb.Transaction) error {
		// check swapCommit - verify the current head hasn't changed
		currentHead, err := rm.db.GetActorHeadTx(tx, actor.Did)
		if err != nil {
			return fmt.Errorf("failed to get current head: %w", err)
		}

		// if swapCommit provided, verify it matches
		if swapCommit != nil && currentHead != *swapCommit {
			return ErrConcurrentModification
		}

		// even without swapCommit, verify head hasn't changed since we opened the repo
		if currentHead != actor.Head {
			return ErrConcurrentModification
		}

		// flush blocks
		bs.FlushTx(tx)

		// save record
		if err := rm.db.SaveRecordTx(tx, record); err != nil {
			return fmt.Errorf("failed to save record: %w", err)
		}

		// update actor with new head and rev
		actor.Head = rootCID.String()
		actor.Rev = rev
		if err := rm.db.SaveActorTx(tx, actor); err != nil {
			return fmt.Errorf("failed to save actor: %w", err)
		}

		return nil
	})
	if err != nil {
		return nil, err
	}

	// clear pending blocks after successful commit
	bs.ClearPending()

	return &CreateRecordResult{
		RecordCID: recordCID,
		CommitCID: rootCID,
		Rev:       rev,
	}, nil
}

// DeleteRecordResult contains the result of an atomic record deletion.
type DeleteRecordResult struct {
	CommitCID cid.Cid
	Rev       string
}

// CommitDeleteRecord atomically deletes a record from the repo.
// It commits the MST changes, deletes the record, and updates the actor in a single transaction.
// If swapCommit is provided, the operation fails if the current head doesn't match.
func (rm *RepoManager) CommitDeleteRecord(
	ctx context.Context,
	r *repo.Repo,
	bs *foundation.Blockstore,
	actor *types.Actor,
	uri *at.URI,
	swapCommit *string,
) (*DeleteRecordResult, error) {
	signFn := makeSigningFunc(actor.SigningKey)

	// commit the repo to get the new root and rev
	rootCID, rev, err := r.Commit(ctx, signFn)
	if err != nil {
		return nil, fmt.Errorf("failed to commit repo: %w", err)
	}

	// perform all writes atomically
	err = rm.db.Transact(func(tx fdb.Transaction) error {
		// check swapCommit - verify the current head hasn't changed
		currentHead, err := rm.db.GetActorHeadTx(tx, actor.Did)
		if err != nil {
			return fmt.Errorf("failed to get current head: %w", err)
		}

		// if swapCommit provided, verify it matches
		if swapCommit != nil && currentHead != *swapCommit {
			return ErrConcurrentModification
		}

		// even without swapCommit, verify head hasn't changed since we opened the repo
		if currentHead != actor.Head {
			return ErrConcurrentModification
		}

		// flush blocks
		bs.FlushTx(tx)

		// delete record
		rm.db.DeleteRecordTx(tx, uri)

		// update actor with new head and rev
		actor.Head = rootCID.String()
		actor.Rev = rev
		if err := rm.db.SaveActorTx(tx, actor); err != nil {
			return fmt.Errorf("failed to save actor: %w", err)
		}

		return nil
	})
	if err != nil {
		return nil, err
	}

	// clear pending blocks after successful commit
	bs.ClearPending()

	return &DeleteRecordResult{
		CommitCID: rootCID,
		Rev:       rev,
	}, nil
}

// makeSigningFunc creates a signing function for repo commits.
func makeSigningFunc(signingKey []byte) func(ctx context.Context, did string, msg []byte) ([]byte, error) {
	return func(ctx context.Context, did string, msg []byte) ([]byte, error) {
		k, err := atcrypto.ParsePrivateBytesK256(signingKey)
		if err != nil {
			return nil, fmt.Errorf("failed to parse signing key: %w", err)
		}

		sig, err := k.HashAndSign(msg)
		if err != nil {
			return nil, fmt.Errorf("failed to sign: %w", err)
		}

		return sig, nil
	}
}
