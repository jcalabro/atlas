package foundation

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/bluesky-social/indigo/atproto/atcrypto"
	"github.com/bluesky-social/indigo/repo"
	"github.com/ipfs/go-cid"
	"github.com/jcalabro/atlas/internal/at"
	"github.com/jcalabro/atlas/internal/types"
)

// ErrConcurrentModification is returned when a swapCommit check fails,
// indicating another server modified the repo concurrently.
var ErrConcurrentModification = errors.New("concurrent modification detected")

// rawCBOR wraps CBOR bytes and implements the CborMarshaler interface
// required by indigo's repo package for storing records in the MST.
type rawCBOR []byte

func (r rawCBOR) MarshalCBOR(w io.Writer) error {
	_, err := w.Write(r)
	return err
}

// InitRepo creates an empty repository for a new account.
// Returns the initial root CID and revision.
func (db *DB) InitRepo(ctx context.Context, actor *types.Actor) (cid.Cid, string, error) {
	var rootCID cid.Cid
	var rev string

	err := db.Transact(func(tx fdb.Transaction) error {
		bs := db.NewWriteBlockstore(actor.Did, tx)

		// create a new empty repo
		r := repo.NewRepo(ctx, actor.Did, bs)

		signFn := makeSigningFunc(actor.SigningKey)

		// commit the empty repo to get initial root and rev
		var err error
		rootCID, rev, err = r.Commit(ctx, signFn)
		if err != nil {
			return fmt.Errorf("failed to commit initial repo: %w", err)
		}

		return nil
	})
	if err != nil {
		return cid.Undef, "", err
	}

	return rootCID, rev, nil
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

		// parse the current head CID and open the repo
		rootCID, err := cid.Decode(actor.Head)
		if err != nil {
			return fmt.Errorf("failed to parse repo head CID: %w", err)
		}

		r, err := repo.OpenRepo(ctx, bs, rootCID)
		if err != nil {
			return fmt.Errorf("failed to open repo: %w", err)
		}

		// add record to MST
		rpath := record.Collection + "/" + record.Rkey
		recordCID, err := r.PutRecord(ctx, rpath, rawCBOR(cborBytes))
		if err != nil {
			return fmt.Errorf("failed to put record in MST: %w", err)
		}

		// commit the repo
		signFn := makeSigningFunc(actor.SigningKey)
		newRootCID, rev, err := r.Commit(ctx, signFn)
		if err != nil {
			return fmt.Errorf("failed to commit repo: %w", err)
		}

		// save record to secondary index
		record.Cid = recordCID.String()
		if err := db.SaveRecordTx(tx, record); err != nil {
			return fmt.Errorf("failed to save record: %w", err)
		}

		// update actor with new head and rev
		actor.Head = newRootCID.String()
		actor.Rev = rev
		if err := db.SaveActorTx(tx, actor); err != nil {
			return fmt.Errorf("failed to save actor: %w", err)
		}

		result = CreateRecordResult{
			RecordCID: recordCID,
			CommitCID: newRootCID,
			Rev:       rev,
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

		// parse the current head CID and open the repo
		rootCID, err := cid.Decode(actor.Head)
		if err != nil {
			return fmt.Errorf("failed to parse repo head CID: %w", err)
		}

		r, err := repo.OpenRepo(ctx, bs, rootCID)
		if err != nil {
			return fmt.Errorf("failed to open repo: %w", err)
		}

		// delete record from MST
		rpath := uri.Collection + "/" + uri.Rkey
		if err := r.DeleteRecord(ctx, rpath); err != nil {
			return fmt.Errorf("failed to delete record from MST: %w", err)
		}

		// commit the repo
		signFn := makeSigningFunc(actor.SigningKey)
		newRootCID, rev, err := r.Commit(ctx, signFn)
		if err != nil {
			return fmt.Errorf("failed to commit repo: %w", err)
		}

		// delete record from secondary index
		db.DeleteRecordTx(tx, uri)

		// update actor with new head and rev
		actor.Head = newRootCID.String()
		actor.Rev = rev
		if err := db.SaveActorTx(tx, actor); err != nil {
			return fmt.Errorf("failed to save actor: %w", err)
		}

		result = DeleteRecordResult{
			CommitCID: newRootCID,
			Rev:       rev,
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	return &result, nil
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
