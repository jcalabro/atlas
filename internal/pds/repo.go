package pds

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sync"

	"github.com/bluesky-social/indigo/api/atproto"
	"github.com/bluesky-social/indigo/atproto/atdata"
	"github.com/bluesky-social/indigo/atproto/syntax"
	"github.com/ipfs/go-cid"
	"github.com/jcalabro/atlas/internal/types"
	"github.com/jcalabro/atlas/internal/util"
	mh "github.com/multiformats/go-multihash"
	"google.golang.org/protobuf/types/known/timestamppb"
)

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

// tidClock is a process-global TID clock for generating record keys.
// Using clock ID 0 is fine for a single process; in a distributed system
// you'd want different clock IDs per process.
var (
	tidClock     = syntax.NewTIDClock(0)
	tidClockOnce sync.Once
)

func nextTID() syntax.TID {
	tidClockOnce.Do(func() {
		tidClock = syntax.NewTIDClock(0)
	})
	return tidClock.Next()
}

// computeCID computes a CID from CBOR bytes using SHA-256 and dag-cbor codec.
func computeCID(cborBytes []byte) (cid.Cid, error) {
	// 0x71 = dag-cbor codec, 0x12 = sha2-256
	hash, err := mh.Sum(cborBytes, mh.SHA2_256, -1)
	if err != nil {
		return cid.Undef, fmt.Errorf("failed to hash cbor: %w", err)
	}
	return cid.NewCidV1(0x71, hash), nil
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
	span := spanFromContext(r.Context())
	defer span.End()

	actor := actorFromContext(ctx)
	if actor == nil {
		s.err(w, http.StatusUnauthorized, fmt.Errorf("authentication required"))
		return
	}

	var in createRecordInput
	if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
		s.badRequest(w, fmt.Errorf("invalid request body: %w", err))
		return
	}

	// verify the repo matches the authenticated user
	if in.Repo != actor.Did && in.Repo != actor.Handle {
		s.badRequest(w, fmt.Errorf("repo must match authenticated user"))
		return
	}

	// validate collection is a valid NSID
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
		// generate a TID-based rkey
		rkey = nextTID().String()
	}

	// check if record already exists
	uri := fmt.Sprintf("at://%s/%s/%s", actor.Did, in.Collection, rkey)
	existing, err := s.db.GetRecord(ctx, uri)
	if err != nil {
		s.internalErr(w, fmt.Errorf("failed to check existing record: %w", err))
		return
	}
	if existing != nil {
		s.badRequest(w, fmt.Errorf("record already exists at %s/%s", in.Collection, rkey))
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

	// compute CID from CBOR bytes
	recordCID, err := computeCID(cborBytes)
	if err != nil {
		s.internalErr(w, fmt.Errorf("failed to compute CID: %w", err))
		return
	}

	// create record entry
	record := &types.Record{
		Did:        actor.Did,
		Collection: in.Collection,
		Rkey:       rkey,
		Cid:        recordCID.String(),
		Value:      cborBytes,
		CreatedAt:  timestamppb.Now(),
	}

	// save record to FDB
	if err := s.db.SaveRecord(ctx, record); err != nil {
		s.internalErr(w, fmt.Errorf("failed to save record: %w", err))
		return
	}

	// return response
	// NOTE: we're not updating the repo commit yet - that will come with full MST support
	resp := atproto.RepoCreateRecord_Output{
		Uri:              uri,
		Cid:              recordCID.String(),
		ValidationStatus: util.Ptr("valid"),
	}

	s.jsonOK(w, resp)
}
