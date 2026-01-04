package pds

import (
	"bytes"
	"fmt"
	"net/http"

	"github.com/bluesky-social/indigo/api/atproto"
	"github.com/bluesky-social/indigo/atproto/syntax"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	cbor "github.com/ipfs/go-ipld-cbor"
	"github.com/ipld/go-car"
	carutil "github.com/ipld/go-car/util"
)

func (s *server) handleGetBlocks(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	span := spanFromContext(ctx)
	defer span.End()

	didParam := r.URL.Query().Get("did")
	cidParams := r.URL.Query()["cids"]

	if didParam == "" {
		s.badRequest(w, fmt.Errorf("did is required"))
		return
	}

	if len(cidParams) == 0 {
		s.badRequest(w, fmt.Errorf("cids is required"))
		return
	}

	// validate DID
	if _, err := syntax.ParseDID(didParam); err != nil {
		s.badRequest(w, fmt.Errorf("invalid did: %w", err))
		return
	}

	// get the actor to verify they exist and get the root CID
	actor, err := s.db.GetActorByDID(ctx, didParam)
	if err != nil {
		s.internalErr(w, fmt.Errorf("failed to get actor: %w", err))
		return
	}
	if actor == nil {
		s.notFound(w, fmt.Errorf("repo not found"))
		return
	}

	// parse the requested CIDs
	cids := make([]cid.Cid, 0, len(cidParams))
	for _, cs := range cidParams {
		c, err := cid.Decode(cs)
		if err != nil {
			s.badRequest(w, fmt.Errorf("invalid cid %q: %w", cs, err))
			return
		}
		cids = append(cids, c)
	}

	// parse the root CID for the CAR header
	rootCID, err := cid.Decode(actor.Head)
	if err != nil {
		s.internalErr(w, fmt.Errorf("failed to parse actor head cid: %w", err))
		return
	}

	// build the CAR response
	buf := new(bytes.Buffer)

	// write CAR header
	hb, err := cbor.DumpObject(&car.CarHeader{
		Roots:   []cid.Cid{rootCID},
		Version: 1,
	})
	if err != nil {
		s.internalErr(w, fmt.Errorf("failed to encode car header: %w", err))
		return
	}

	if err := carutil.LdWrite(buf, hb); err != nil {
		s.internalErr(w, fmt.Errorf("failed to write car header: %w", err))
		return
	}

	// get blocks from the blockstore and write them to the CAR
	blks, err := s.db.GetBlocks(ctx, actor.Did, cids)
	if err != nil {
		s.internalErr(w, fmt.Errorf("failed to get blocks: %w", err))
		return
	}

	for _, blk := range blks {
		if err := carutil.LdWrite(buf, blk.Cid().Bytes(), blk.RawData()); err != nil {
			s.internalErr(w, fmt.Errorf("failed to write block to car: %w", err))
			return
		}
	}

	w.Header().Set("Content-Type", "application/vnd.ipld.car")
	w.WriteHeader(http.StatusOK)
	if _, err := buf.WriteTo(w); err != nil {
		s.log.Error("failed to write car response", "err", err)
	}
}

func (s *server) handleGetLatestCommit(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	span := spanFromContext(ctx)
	defer span.End()

	did := r.URL.Query().Get("did")
	if did == "" {
		s.badRequest(w, fmt.Errorf("did is required"))
		return
	}

	// validate DID
	if _, err := syntax.ParseDID(did); err != nil {
		s.badRequest(w, fmt.Errorf("invalid did: %w", err))
		return
	}

	actor, err := s.db.GetActorByDID(ctx, did)
	if err != nil {
		s.internalErr(w, fmt.Errorf("failed to get actor: %w", err))
		return
	}
	if actor == nil {
		s.notFound(w, fmt.Errorf("repo not found"))
		return
	}

	s.jsonOK(w, &atproto.SyncGetLatestCommit_Output{
		Cid: actor.Head,
		Rev: actor.Rev,
	})
}

func (s *server) handleGetRepoStatus(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	span := spanFromContext(ctx)
	defer span.End()

	did := r.URL.Query().Get("did")
	if did == "" {
		s.badRequest(w, fmt.Errorf("did is required"))
		return
	}

	if _, err := syntax.ParseDID(did); err != nil {
		s.badRequest(w, fmt.Errorf("invalid did: %w", err))
		return
	}

	actor, err := s.db.GetActorByDID(ctx, did)
	if err != nil {
		s.internalErr(w, fmt.Errorf("failed to get actor: %w", err))
		return
	}
	if actor == nil {
		s.notFound(w, fmt.Errorf("repo not found"))
		return
	}

	out := &atproto.SyncGetRepoStatus_Output{
		Did:    actor.Did,
		Active: actor.Active,
	}

	// only include rev if active
	if actor.Active {
		out.Rev = &actor.Rev
	}

	s.jsonOK(w, out)
}

func (s *server) handleGetRepo(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	span := spanFromContext(ctx)
	defer span.End()

	did := r.URL.Query().Get("did")
	if did == "" {
		s.badRequest(w, fmt.Errorf("did is required"))
		return
	}

	if _, err := syntax.ParseDID(did); err != nil {
		s.badRequest(w, fmt.Errorf("invalid did: %w", err))
		return
	}

	// the `since` parameter is used to get blocks since a specific revision
	since := r.URL.Query().Get("since")

	actor, err := s.db.GetActorByDID(ctx, did)
	if err != nil {
		s.internalErr(w, fmt.Errorf("failed to get actor: %w", err))
		return
	}
	if actor == nil {
		s.notFound(w, fmt.Errorf("repo not found"))
		return
	}

	rootCID, err := cid.Decode(actor.Head)
	if err != nil {
		s.internalErr(w, fmt.Errorf("failed to parse actor head cid: %w", err))
		return
	}

	buf := new(bytes.Buffer)

	hb, err := cbor.DumpObject(&car.CarHeader{
		Roots:   []cid.Cid{rootCID},
		Version: 1,
	})
	if err != nil {
		s.internalErr(w, fmt.Errorf("failed to encode car header: %w", err))
		return
	}

	if err := carutil.LdWrite(buf, hb); err != nil {
		s.internalErr(w, fmt.Errorf("failed to write car header: %w", err))
		return
	}

	var blks []blocks.Block
	if since != "" {
		blks, err = s.db.GetBlocksSince(ctx, actor.Did, since)
	} else {
		blks, err = s.db.GetAllBlocks(ctx, actor.Did)
	}
	if err != nil {
		s.internalErr(w, fmt.Errorf("failed to get blocks: %w", err))
		return
	}

	for _, blk := range blks {
		if err := carutil.LdWrite(buf, blk.Cid().Bytes(), blk.RawData()); err != nil {
			s.internalErr(w, fmt.Errorf("failed to write block to car: %w", err))
			return
		}
	}

	w.Header().Set("Content-Type", "application/vnd.ipld.car")
	w.WriteHeader(http.StatusOK)
	if _, err := buf.WriteTo(w); err != nil {
		s.log.Error("failed to write car response", "err", err)
	}
}
