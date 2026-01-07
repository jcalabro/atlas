package pds

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"

	"github.com/bluesky-social/indigo/api/atproto"
	"github.com/bluesky-social/indigo/atproto/atdata"
	"github.com/bluesky-social/indigo/atproto/identity"
	"github.com/bluesky-social/indigo/atproto/syntax"
	"github.com/jcalabro/atlas/internal/at"
	"github.com/jcalabro/atlas/internal/pds/db"
	"github.com/jcalabro/atlas/internal/types"
	"github.com/jcalabro/atlas/internal/util"
	"go.opentelemetry.io/otel/attribute"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func (s *server) handleDescribeServer(w http.ResponseWriter, r *http.Request) {
	host := hostFromContext(r.Context())
	if host == nil {
		s.internalErr(w, fmt.Errorf("host config not found in context"))
		return
	}

	nullStr := func(str string) *string {
		if str == "" {
			return nil
		}
		return &str
	}

	// @NOTE (jrc): we haven't implemented invite codes or phone # verification yet
	s.jsonOK(w, &atproto.ServerDescribeServer_Output{
		AvailableUserDomains: host.userDomains,
		Contact: &atproto.ServerDescribeServer_Contact{
			Email: nullStr(host.contactEmail),
		},
		Did: host.serviceDID,
		Links: &atproto.ServerDescribeServer_Links{
			PrivacyPolicy:  nullStr(host.privacyPolicy),
			TermsOfService: nullStr(host.termsOfService),
		},
	})
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
		s.badRequest(w, fmt.Errorf("invalid collection nsid: %w", err))
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
	if errors.Is(err, db.ErrNotFound) {
		s.notFound(w, fmt.Errorf("record not found"))
		return
	}
	if err != nil {
		s.internalErr(w, fmt.Errorf("failed to get record: %w", err))
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

	// verify user is attempting to write to the repo they own
	if in.Repo != actor.Did && in.Repo != actor.Handle {
		s.forbidden(w, fmt.Errorf("forbidden"))
		return
	}

	// verify the collection is a valid NSID
	if _, err := syntax.ParseNSID(in.Collection); err != nil {
		s.badRequest(w, fmt.Errorf("invalid collection nsid: %w", err))
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
	if err != nil && !errors.Is(err, db.ErrNotFound) {
		s.internalErr(w, fmt.Errorf("failed to check existing record: %w", err))
		return
	}
	if existing != nil {
		s.conflict(w, fmt.Errorf("record %q already exists", uri))
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

	cborBytes, err := atdata.MarshalCBOR(recordData)
	if err != nil {
		s.internalErr(w, fmt.Errorf("failed to marshal record to CBOR: %w", err))
		return
	}

	// create record entry for secondary index
	record := &types.Record{
		Did:        actor.Did,
		Collection: in.Collection,
		Rkey:       rkey,
		Value:      cborBytes,
		CreatedAt:  timestamppb.Now(),
	}

	// atomically create record: MST operations, blocks, secondary index, actor update
	result, err := s.db.CreateRecord(ctx, actor, record, cborBytes, in.SwapCommit)
	if err != nil {
		if errors.Is(err, db.ErrConcurrentModification) {
			s.conflict(w, fmt.Errorf("repo was modified concurrently, please retry"))
			return
		}
		s.internalErr(w, fmt.Errorf("failed to create record: %w", err))
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

	// verify user is attempting to write to the repo they own
	if in.Repo != actor.Did && in.Repo != actor.Handle {
		s.forbidden(w, fmt.Errorf("forbidden"))
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

	// verify the collection is a valid NSID
	if _, err := syntax.ParseNSID(in.Collection); err != nil {
		s.badRequest(w, fmt.Errorf("invalid collection nsid: %w", err))
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
	if errors.Is(err, db.ErrNotFound) {
		s.notFound(w, fmt.Errorf("record not found"))
		return
	}
	if err != nil {
		s.internalErr(w, fmt.Errorf("failed to check existing record: %w", err))
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

	// parse the URI for the delete operation
	aturi := &at.URI{Repo: actor.Did, Collection: in.Collection, Rkey: in.Rkey}

	// atomically delete record: MST operations, blocks, secondary index, actor update
	result, err := s.db.DeleteRecord(ctx, actor, aturi, in.SwapCommit)
	if err != nil {
		if errors.Is(err, db.ErrConcurrentModification) {
			s.conflict(w, fmt.Errorf("repo was modified concurrently, please retry"))
			return
		}
		s.internalErr(w, fmt.Errorf("failed to delete record: %w", err))
		return
	}

	s.jsonOK(w, &atproto.RepoDeleteRecord_Output{
		Commit: &atproto.RepoDefs_CommitMeta{Cid: result.CommitCID.String(), Rev: result.Rev},
	})
}

// putRecordInput mirrors atproto.RepoPutRecord_Input but with
// a raw json.RawMessage for the record field so we can handle arbitrary records.
type putRecordInput struct {
	Repo       string          `json:"repo"`
	Collection string          `json:"collection"`
	Rkey       string          `json:"rkey"`
	Validate   *bool           `json:"validate,omitempty"`
	Record     json.RawMessage `json:"record"`
	SwapRecord *string         `json:"swapRecord,omitempty"`
	SwapCommit *string         `json:"swapCommit,omitempty"`
}

func (s *server) handlePutRecord(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	actor := actorFromContext(ctx)
	if actor == nil {
		s.internalErr(w, fmt.Errorf("actor not found in context"))
		return
	}

	var in putRecordInput
	if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
		s.badRequest(w, fmt.Errorf("invalid request body: %w", err))
		return
	}

	// verify user is attempting to write to the repo they own
	if in.Repo != actor.Did && in.Repo != actor.Handle {
		s.forbidden(w, fmt.Errorf("forbidden"))
		return
	}

	// validate the input
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
	case len(in.Record) == 0:
		s.badRequest(w, fmt.Errorf("record is required"))
		return
	}

	if _, err := syntax.ParseNSID(in.Collection); err != nil {
		s.badRequest(w, fmt.Errorf("invalid collection nsid: %w", err))
		return
	}

	if _, err := syntax.ParseRecordKey(in.Rkey); err != nil {
		s.badRequest(w, fmt.Errorf("invalid rkey: %w", err))
		return
	}

	// validate swapRecord CID if provided
	if in.SwapRecord != nil {
		if _, err := syntax.ParseCID(*in.SwapRecord); err != nil {
			s.badRequest(w, fmt.Errorf("invalid swapRecord cid: %w", err))
			return
		}
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

	cborBytes, err := atdata.MarshalCBOR(recordData)
	if err != nil {
		s.internalErr(w, fmt.Errorf("failed to marshal record to CBOR: %w", err))
		return
	}

	// create record entry for secondary index
	record := &types.Record{
		Did:        actor.Did,
		Collection: in.Collection,
		Rkey:       in.Rkey,
		Value:      cborBytes,
		CreatedAt:  timestamppb.Now(),
	}

	uri := at.FormatURI(actor.Did, in.Collection, in.Rkey)

	// atomically put record: MST operations, blocks, secondary index, actor update
	result, err := s.db.PutRecord(ctx, actor, record, cborBytes, in.SwapRecord, in.SwapCommit)
	if err != nil {
		if errors.Is(err, db.ErrConcurrentModification) {
			s.conflict(w, fmt.Errorf("repo was modified concurrently, please retry"))
			return
		}
		s.internalErr(w, fmt.Errorf("failed to put record: %w", err))
		return
	}

	resp := atproto.RepoPutRecord_Output{
		Uri:              uri,
		Cid:              result.RecordCID.String(),
		Commit:           &atproto.RepoDefs_CommitMeta{Cid: result.CommitCID.String(), Rev: result.Rev},
		ValidationStatus: util.Ptr("valid"),
	}

	s.jsonOK(w, resp)
}

// applyWritesInput mirrors atproto.RepoApplyWrites_Input but with
// a simpler structure for handling arbitrary records.
type applyWritesInput struct {
	Repo       string            `json:"repo"`
	Validate   *bool             `json:"validate,omitempty"`
	Writes     []applyWritesItem `json:"writes"`
	SwapCommit *string           `json:"swapCommit,omitempty"`
}

type applyWritesItem struct {
	Type       string          `json:"$type"`
	Collection string          `json:"collection"`
	Rkey       string          `json:"rkey"`
	Value      json.RawMessage `json:"value,omitempty"`
}

func (s *server) handleApplyWrites(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	actor := actorFromContext(ctx)
	if actor == nil {
		s.internalErr(w, fmt.Errorf("actor not found in context"))
		return
	}

	var in applyWritesInput
	if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
		s.badRequest(w, fmt.Errorf("invalid request body: %w", err))
		return
	}

	// verify user is attempting to write to the repo they own
	if in.Repo != actor.Did && in.Repo != actor.Handle {
		s.forbidden(w, fmt.Errorf("forbidden"))
		return
	}

	if len(in.Writes) == 0 {
		s.badRequest(w, fmt.Errorf("writes is required"))
		return
	}

	// convert input items to db.WriteOp
	ops := make([]db.WriteOp, 0, len(in.Writes))
	for i, item := range in.Writes {
		// validate collection NSID
		if _, err := syntax.ParseNSID(item.Collection); err != nil {
			s.badRequest(w, fmt.Errorf("invalid collection nsid in write %d: %w", i, err))
			return
		}

		// determine action from $type
		var action string
		switch item.Type {
		case "com.atproto.repo.applyWrites#create":
			action = "create"
		case "com.atproto.repo.applyWrites#update":
			action = "update"
		case "com.atproto.repo.applyWrites#delete":
			action = "delete"
		default:
			s.badRequest(w, fmt.Errorf("invalid write type in write %d: %s", i, item.Type))
			return
		}

		var rkey string
		if action == "create" && item.Rkey == "" {
			// generate a TID-based rkey
			tid, err := s.db.NextTID(ctx, actor.Did)
			if err != nil {
				s.internalErr(w, fmt.Errorf("failed to generate tid: %w", err))
				return
			}
			rkey = tid.String()
		} else {
			if item.Rkey == "" {
				s.badRequest(w, fmt.Errorf("rkey is required for %s in write %d", action, i))
				return
			}
			// validate rkey
			if _, err := syntax.ParseRecordKey(item.Rkey); err != nil {
				s.badRequest(w, fmt.Errorf("invalid rkey in write %d: %w", i, err))
				return
			}
			rkey = item.Rkey
		}

		op := db.WriteOp{
			Action:     action,
			Collection: item.Collection,
			Rkey:       rkey,
		}

		// for create/update, parse and convert record to CBOR
		if action != "delete" {
			if len(item.Value) == 0 {
				s.badRequest(w, fmt.Errorf("value is required for %s in write %d", action, i))
				return
			}

			recordData, err := atdata.UnmarshalJSON(item.Value)
			if err != nil {
				s.badRequest(w, fmt.Errorf("invalid record data in write %d: %w", i, err))
				return
			}

			// ensure record has $type field matching collection
			if recordData["$type"] == nil || recordData["$type"] == "" {
				recordData["$type"] = item.Collection
			}

			cborBytes, err := atdata.MarshalCBOR(recordData)
			if err != nil {
				s.internalErr(w, fmt.Errorf("failed to marshal record to CBOR in write %d: %w", i, err))
				return
			}

			op.Value = cborBytes
		}

		ops = append(ops, op)
	}

	// check if any creates would conflict with existing records
	for i, op := range ops {
		if op.Action == "create" {
			uri := at.FormatURI(actor.Did, op.Collection, op.Rkey)
			existing, err := s.db.GetRecord(ctx, uri)
			if err != nil && !errors.Is(err, db.ErrNotFound) {
				s.internalErr(w, fmt.Errorf("failed to check existing record: %w", err))
				return
			}
			if existing != nil {
				s.conflict(w, fmt.Errorf("record %q already exists (write %d)", uri, i))
				return
			}
		}
	}

	// apply all writes atomically
	result, err := s.db.ApplyWrites(ctx, actor, ops, in.SwapCommit)
	if err != nil {
		if errors.Is(err, db.ErrConcurrentModification) {
			s.conflict(w, fmt.Errorf("repo was modified concurrently, please retry"))
			return
		}
		s.internalErr(w, fmt.Errorf("failed to apply writes: %w", err))
		return
	}

	// build response using indigo types
	outputResults := make([]*atproto.RepoApplyWrites_Output_Results_Elem, 0, len(result.Results))
	for _, res := range result.Results {
		elem := &atproto.RepoApplyWrites_Output_Results_Elem{}
		switch res.Action {
		case "create":
			elem.RepoApplyWrites_CreateResult = &atproto.RepoApplyWrites_CreateResult{
				Uri:              res.URI,
				Cid:              res.CID,
				ValidationStatus: util.Ptr("valid"),
			}
		case "update":
			elem.RepoApplyWrites_UpdateResult = &atproto.RepoApplyWrites_UpdateResult{
				Uri:              res.URI,
				Cid:              res.CID,
				ValidationStatus: util.Ptr("valid"),
			}
		case "delete":
			elem.RepoApplyWrites_DeleteResult = &atproto.RepoApplyWrites_DeleteResult{}
		}
		outputResults = append(outputResults, elem)
	}

	resp := atproto.RepoApplyWrites_Output{
		Commit: &atproto.RepoDefs_CommitMeta{
			Cid: result.CommitCID.String(),
			Rev: result.Rev,
		},
		Results: outputResults,
	}

	s.jsonOK(w, resp)
}

func (s *server) handleDescribeRepo(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	span := spanFromContext(ctx)
	defer span.End()

	repo := r.URL.Query().Get("repo")
	span.SetAttributes(attribute.String("repo", repo))

	if repo == "" {
		s.badRequest(w, fmt.Errorf("repo is required"))
		return
	}

	// parse as either DID or handle
	atid, err := syntax.ParseAtIdentifier(repo)
	if err != nil {
		s.badRequest(w, fmt.Errorf("invalid repo identifier: %w", err))
		return
	}

	// look up the identity (does bi-directional handle verification)
	ident, err := s.directory.Lookup(ctx, *atid)
	if errors.Is(err, identity.ErrDIDNotFound) || errors.Is(err, identity.ErrHandleNotFound) {
		s.notFound(w, fmt.Errorf("repo not found"))
		return
	}
	if err != nil {
		s.internalErr(w, fmt.Errorf("failed to lookup identity: %w", err))
		return
	}

	// check if handle resolves correctly
	// if the handle is "handle.invalid", it means bi-directional verification failed
	handleIsCorrect := ident.Handle != syntax.HandleInvalid

	// get collections from the database
	collections, err := s.db.GetCollections(ctx, ident.DID.String())
	if err != nil {
		s.internalErr(w, fmt.Errorf("failed to get collections: %w", err))
		return
	}

	// ensure collections is never nil (spec says it's a list)
	if collections == nil {
		collections = []string{}
	}

	// build the DID document from the identity
	didDoc := ident.DIDDocument()

	s.jsonOK(w, &atproto.RepoDescribeRepo_Output{
		Did:             ident.DID.String(),
		Handle:          ident.Handle.String(),
		DidDoc:          didDoc,
		Collections:     collections,
		HandleIsCorrect: handleIsCorrect,
	})
}
