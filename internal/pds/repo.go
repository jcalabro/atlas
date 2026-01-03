package pds

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"

	"github.com/bluesky-social/indigo/api/atproto"
	"github.com/bluesky-social/indigo/atproto/atdata"
	"github.com/bluesky-social/indigo/atproto/syntax"
	"github.com/jcalabro/atlas/internal/at"
	"github.com/jcalabro/atlas/internal/types"
	"github.com/jcalabro/atlas/internal/util"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// rawCBOR wraps CBOR bytes and implements the CborMarshaler interface
// required by indigo's repo package for storing records in the MST.
type rawCBOR []byte

func (r rawCBOR) MarshalCBOR(w io.Writer) error {
	_, err := w.Write(r)
	return err
}

func (s *server) handleGetRecord(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	span := spanFromContext(ctx)
	defer span.End()

	repo := r.URL.Query().Get("repo")
	collection := r.URL.Query().Get("collection")
	rkey := r.URL.Query().Get("rkey")
	cidParam := r.URL.Query().Get("cid")

	switch {
	case repo == "":
		s.badRequest(w, fmt.Errorf("repo is required"))
		return
	case collection == "":
		s.badRequest(w, fmt.Errorf("collection is required"))
		return
	case rkey == "":
		s.badRequest(w, fmt.Errorf("rkey is required"))
		return
	}

	if _, err := syntax.ParseNSID(collection); err != nil {
		s.badRequest(w, fmt.Errorf("invalid collection NSID: %w", err))
		return
	}

	if _, err := syntax.ParseRecordKey(rkey); err != nil {
		s.badRequest(w, fmt.Errorf("invalid rkey: %w", err))
		return
	}

	// resolve repo to DID if it's a handle
	did := repo
	if _, err := syntax.ParseDID(repo); err != nil {
		// not a DID, try to resolve as handle
		ident, err := s.directory.LookupHandle(ctx, syntax.Handle(repo))
		if err != nil {
			s.notFound(w, fmt.Errorf("could not resolve handle: %w", err))
			return
		}
		did = ident.DID.String()
	}

	uri := at.FormatURI(did, collection, rkey)

	record, err := s.db.GetRecord(ctx, uri)
	if err != nil {
		s.internalErr(w, fmt.Errorf("failed to get record: %w", err))
		return
	}
	if record == nil {
		s.notFound(w, fmt.Errorf("record not found"))
		return
	}

	// if cid param provided, verify it matches
	if cidParam != "" {
		if _, err := syntax.ParseCID(cidParam); err != nil {
			s.badRequest(w, fmt.Errorf("invalid cid: %w", err))
			return
		}
		if record.Cid != cidParam {
			s.notFound(w, fmt.Errorf("record not found with specified cid"))
			return
		}
	}

	// unmarshal CBOR to JSON-friendly value
	val, err := atdata.UnmarshalCBOR(record.Value)
	if err != nil {
		s.internalErr(w, fmt.Errorf("failed to decode record value: %w", err))
		return
	}

	type response struct {
		Uri   string         `json:"uri"`
		Cid   string         `json:"cid"`
		Value map[string]any `json:"value"`
	}

	s.jsonOK(w, response{
		Uri:   uri,
		Cid:   record.Cid,
		Value: val,
	})
}

func (s *server) handleListRepos(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	span := spanFromContext(ctx)
	defer span.End()

	host := hostFromContext(ctx)

	cursor := r.URL.Query().Get("cursor")
	if cursor != "" {
		if _, err := syntax.ParseDID(cursor); err != nil {
			s.badRequest(w, fmt.Errorf("invalid cursor (must be a did)"))
			return
		}
	}

	limit, err := parseIntParam(r, "limit", 500)
	if err != nil || limit < 0 {
		s.badRequest(w, fmt.Errorf("invalid limit"))
		return
	}
	if limit > 500 {
		limit = 500 // set the max scan size
	}

	actors, next, err := s.db.ListActors(ctx, host.hostname, cursor, limit)
	if err != nil {
		s.internalErr(w, fmt.Errorf("failed to list repos: %w", err))
		return
	}

	repos := make([]*atproto.SyncListRepos_Repo, len(actors))
	for ndx, actor := range actors {
		repos[ndx] = &atproto.SyncListRepos_Repo{
			Active: util.Ptr(actor.Active),
			Did:    actor.Did,
			Head:   actor.Head,
			Rev:    actor.Rev,
		}
	}

	s.jsonOK(w, atproto.SyncListRepos_Output{
		Cursor: nextCursorOrNil(next),
		Repos:  repos,
	})
}

// createRecordInput mirrors atproto.RepoCreateRecord_Input but with
// a raw json.RawMessage for the record field so we can handle arbitrary records.
type createRecordInput struct {
	Repo       string          `json:"repo"`
	Collection string          `json:"collection"`
	Rkey       *string         `json:"rkey,omitempty"`
	Validate   *bool           `json:"validate,omitempty"`
	Record     json.RawMessage `json:"record"`
	SwapCommit *string         `json:"swapCommit,omitempty"`
}

func (s *server) handleCreateRecord(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	span := spanFromContext(ctx)
	defer span.End()

	actor := actorFromContext(ctx)
	if actor == nil {
		s.internalErr(w, fmt.Errorf("actor not found in context"))
		return
	}

	var in createRecordInput
	if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
		s.badRequest(w, fmt.Errorf("invalid request body: %w", err))
		return
	}

	// verify the repo matches the authenticated user
	if in.Repo != actor.Did && in.Repo != actor.Handle {
		s.forbidden(w, fmt.Errorf("repo must match authenticated user"))
		return
	}

	// verify the collection is a valid NSID
	if _, err := syntax.ParseNSID(in.Collection); err != nil {
		s.badRequest(w, fmt.Errorf("invalid collection NSID: %w", err))
		return
	}

	// parse or generate rkey
	var rkey string
	if in.Rkey != nil && *in.Rkey != "" {
		// validate provided rkey
		if _, err := syntax.ParseRecordKey(*in.Rkey); err != nil {
			s.badRequest(w, fmt.Errorf("invalid rkey: %w", err))
			return
		}
		rkey = *in.Rkey
	} else {
		// generate a TID-based rkey using distributed counter
		tid, err := s.db.NextTID(ctx, actor.Did)
		if err != nil {
			s.internalErr(w, fmt.Errorf("failed to generate tid: %w", err))
			return
		}
		rkey = tid.String()
	}

	// check if record already exists
	uri := at.FormatURI(actor.Did, in.Collection, rkey)
	existing, err := s.db.GetRecord(ctx, uri)
	if err != nil {
		s.internalErr(w, fmt.Errorf("failed to check existing record: %w", err))
		return
	}
	if existing != nil {
		s.conflict(w, fmt.Errorf("record already exists"))
		return
	}

	// parse the record JSON and convert to CBOR
	recordData, err := atdata.UnmarshalJSON(in.Record)
	if err != nil {
		s.badRequest(w, fmt.Errorf("invalid record data: %w", err))
		return
	}

	// ensure record has $type field matching collection
	if recordData["$type"] == nil || recordData["$type"] == "" {
		recordData["$type"] = in.Collection
	}

	// marshal to CBOR
	cborBytes, err := atdata.MarshalCBOR(recordData)
	if err != nil {
		s.internalErr(w, fmt.Errorf("failed to marshal record to CBOR: %w", err))
		return
	}

	// open the repo and add record to MST
	repo, bs, err := s.repoMgr.OpenRepo(ctx, actor)
	if err != nil {
		s.internalErr(w, fmt.Errorf("failed to open repo: %w", err))
		return
	}

	// add record to MST using collection/rkey path
	rpath := in.Collection + "/" + rkey
	recordCID, err := repo.PutRecord(ctx, rpath, rawCBOR(cborBytes))
	if err != nil {
		s.internalErr(w, fmt.Errorf("failed to put record in MST: %w", err))
		return
	}

	// create record entry for secondary index
	record := &types.Record{
		Did:        actor.Did,
		Collection: in.Collection,
		Rkey:       rkey,
		Cid:        recordCID.String(),
		Value:      cborBytes,
		CreatedAt:  timestamppb.Now(),
	}

	// atomically commit: flush blocks, save record, update actor
	result, err := s.repoMgr.CommitCreateRecord(ctx, repo, bs, actor, record, in.SwapCommit)
	if err != nil {
		if errors.Is(err, ErrConcurrentModification) {
			s.conflict(w, fmt.Errorf("repo was modified concurrently, please retry"))
			return
		}
		s.internalErr(w, fmt.Errorf("failed to commit record: %w", err))
		return
	}

	resp := atproto.RepoCreateRecord_Output{
		Uri:              uri,
		Cid:              result.RecordCID.String(),
		Commit:           &atproto.RepoDefs_CommitMeta{Cid: result.CommitCID.String(), Rev: result.Rev},
		ValidationStatus: util.Ptr("valid"),
	}

	s.jsonOK(w, resp)
}

func (s *server) handleDeleteRecord(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	span := spanFromContext(ctx)
	defer span.End()

	actor := actorFromContext(ctx)
	if actor == nil {
		s.internalErr(w, fmt.Errorf("actor not found in context"))
		return
	}

	var in atproto.RepoDeleteRecord_Input
	if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
		s.badRequest(w, fmt.Errorf("invalid request body: %w", err))
		return
	}

	switch {
	case in.Repo == "":
		s.badRequest(w, fmt.Errorf("repo is required"))
		return
	case in.Collection == "":
		s.badRequest(w, fmt.Errorf("collection is required"))
		return
	case in.Rkey == "":
		s.badRequest(w, fmt.Errorf("rkey is required"))
		return
	}

	// verify the repo matches the authenticated user
	if in.Repo != actor.Did && in.Repo != actor.Handle {
		s.forbidden(w, fmt.Errorf("repo must match authenticated user"))
		return
	}

	// verify the collection is a valid NSID
	if _, err := syntax.ParseNSID(in.Collection); err != nil {
		s.badRequest(w, fmt.Errorf("invalid collection NSID: %w", err))
		return
	}

	// verify the rkey is valid
	if _, err := syntax.ParseRecordKey(in.Rkey); err != nil {
		s.badRequest(w, fmt.Errorf("invalid rkey: %w", err))
		return
	}

	uri := at.FormatURI(actor.Did, in.Collection, in.Rkey)

	// check if record exists
	existing, err := s.db.GetRecord(ctx, uri)
	if err != nil {
		s.internalErr(w, fmt.Errorf("failed to check existing record: %w", err))
		return
	}
	if existing == nil {
		s.notFound(w, fmt.Errorf("record not found"))
		return
	}

	// if swapRecord is provided, verify the CID matches
	if in.SwapRecord != nil {
		if _, err := syntax.ParseCID(*in.SwapRecord); err != nil {
			s.badRequest(w, fmt.Errorf("invalid swapRecord cid: %w", err))
			return
		}
		if existing.Cid != *in.SwapRecord {
			s.conflict(w, fmt.Errorf("record cid does not match swapRecord"))
			return
		}
	}

	// open the repo and delete record from MST
	repo, bs, err := s.repoMgr.OpenRepo(ctx, actor)
	if err != nil {
		s.internalErr(w, fmt.Errorf("failed to open repo: %w", err))
		return
	}

	// delete record from MST using collection/rkey path
	rpath := in.Collection + "/" + in.Rkey
	if err := repo.DeleteRecord(ctx, rpath); err != nil {
		s.internalErr(w, fmt.Errorf("failed to delete record from MST: %w", err))
		return
	}

	// parse the URI for the atomic commit
	aturi := &at.URI{Repo: actor.Did, Collection: in.Collection, Rkey: in.Rkey}

	// atomically commit: flush blocks, delete record, update actor
	result, err := s.repoMgr.CommitDeleteRecord(ctx, repo, bs, actor, aturi, in.SwapCommit)
	if err != nil {
		if errors.Is(err, ErrConcurrentModification) {
			s.conflict(w, fmt.Errorf("repo was modified concurrently, please retry"))
			return
		}
		s.internalErr(w, fmt.Errorf("failed to commit deletion: %w", err))
		return
	}

	s.jsonOK(w, &atproto.RepoDeleteRecord_Output{
		Commit: &atproto.RepoDefs_CommitMeta{Cid: result.CommitCID.String(), Rev: result.Rev},
	})
}
