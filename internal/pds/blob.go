package pds

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net/http"

	"github.com/bluesky-social/indigo/api/atproto"
	"github.com/bluesky-social/indigo/lex/util"
	"github.com/ipfs/go-cid"
	"github.com/jcalabro/atlas/internal/pds/db"
	"github.com/jcalabro/atlas/internal/types"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/multiformats/go-multihash"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// blobstore wraps an S3-compatible client for blob storage
type blobstore struct {
	client *minio.Client
	bucket string
}

func newBlobstore(cfg *BlobstoreConfig) (*blobstore, error) {
	client, err := minio.New(cfg.Endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(cfg.AccessKey, cfg.SecretKey, ""),
		Secure: false, // use HTTP for local dev
		Region: cfg.Region,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create minio client: %w", err)
	}

	return &blobstore{
		client: client,
		bucket: cfg.Bucket,
	}, nil
}

// blobKey returns the S3 object key for a blob
func blobKey(did string, c cid.Cid) string {
	return fmt.Sprintf("blobs/%s/%s", did, c.String())
}

func (s *server) handleUploadBlob(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	actor := actorFromContext(ctx)
	if actor == nil {
		s.internalErr(w, fmt.Errorf("actor not found in context"))
		return
	}

	if s.blobstore == nil {
		s.internalErr(w, fmt.Errorf("blobstore not configured"))
		return
	}

	// get content type from request
	mimeType := r.Header.Get("Content-Type")
	if mimeType == "" {
		mimeType = "application/octet-stream"
	}

	// read the entire body to compute CID
	data, err := io.ReadAll(r.Body)
	if err != nil {
		s.badRequest(w, fmt.Errorf("failed to read request body: %w", err))
		return
	}

	if len(data) == 0 {
		s.badRequest(w, fmt.Errorf("empty blob"))
		return
	}

	// compute CID (raw codec with SHA2-256)
	cidBuilder := cid.NewPrefixV1(cid.Raw, multihash.SHA2_256)
	blobCID, err := cidBuilder.Sum(data)
	if err != nil {
		s.internalErr(w, fmt.Errorf("failed to compute CID: %w", err))
		return
	}

	// upload to S3
	key := blobKey(actor.Did, blobCID)
	_, err = s.blobstore.client.PutObject(ctx, s.blobstore.bucket, key, bytes.NewReader(data), int64(len(data)), minio.PutObjectOptions{
		ContentType: mimeType,
	})
	if err != nil {
		s.internalErr(w, fmt.Errorf("failed to upload blob to S3: %w", err))
		return
	}

	// save blob metadata to database
	blob := &types.Blob{
		Did:       actor.Did,
		Cid:       blobCID.Bytes(),
		MimeType:  mimeType,
		Size:      int64(len(data)),
		CreatedAt: timestamppb.Now(),
	}
	if err := s.db.SaveBlob(ctx, blob); err != nil {
		s.internalErr(w, fmt.Errorf("failed to save blob metadata: %w", err))
		return
	}

	// return the blob reference
	resp := atproto.RepoUploadBlob_Output{
		Blob: &util.LexBlob{
			Ref:      util.LexLink(blobCID),
			MimeType: mimeType,
			Size:     int64(len(data)),
		},
	}

	s.jsonOK(w, resp)
}

func (s *server) handleListBlobs(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	did := r.URL.Query().Get("did")
	if did == "" {
		s.badRequest(w, fmt.Errorf("did is required"))
		return
	}

	cursor := r.URL.Query().Get("cursor")
	limit, err := parseIntParam(r, "limit", 500)
	if err != nil || limit < 0 {
		s.badRequest(w, fmt.Errorf("invalid limit"))
		return
	}
	if limit > 1000 {
		limit = 1000
	}

	blobs, nextCursor, err := s.db.ListBlobs(ctx, did, cursor, int(limit))
	if err != nil {
		s.internalErr(w, fmt.Errorf("failed to list blobs: %w", err))
		return
	}

	// convert blob CIDs to strings
	cids := make([]string, len(blobs))
	for i, blob := range blobs {
		c, err := cid.Cast(blob.Cid)
		if err != nil {
			s.internalErr(w, fmt.Errorf("failed to parse blob CID: %w", err))
			return
		}
		cids[i] = c.String()
	}

	s.jsonOK(w, atproto.SyncListBlobs_Output{
		Cids:   cids,
		Cursor: nextCursorOrNil(nextCursor),
	})
}

func (s *server) handleGetBlob(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	did := r.URL.Query().Get("did")
	if did == "" {
		s.badRequest(w, fmt.Errorf("did is required"))
		return
	}

	cidStr := r.URL.Query().Get("cid")
	if cidStr == "" {
		s.badRequest(w, fmt.Errorf("cid is required"))
		return
	}

	blobCID, err := cid.Parse(cidStr)
	if err != nil {
		s.badRequest(w, fmt.Errorf("invalid cid: %w", err))
		return
	}

	if s.blobstore == nil {
		s.internalErr(w, fmt.Errorf("blobstore not configured"))
		return
	}

	// verify blob exists in our database
	blob, err := s.db.GetBlob(ctx, did, blobCID.Bytes())
	if errors.Is(err, db.ErrNotFound) {
		s.notFound(w, fmt.Errorf("blob not found"))
		return
	}
	if err != nil {
		s.internalErr(w, fmt.Errorf("failed to get blob metadata: %w", err))
		return
	}

	// fetch from S3
	key := blobKey(did, blobCID)
	obj, err := s.blobstore.client.GetObject(ctx, s.blobstore.bucket, key, minio.GetObjectOptions{})
	if err != nil {
		s.internalErr(w, fmt.Errorf("failed to get blob from S3: %w", err))
		return
	}
	defer func() {
		if err := obj.Close(); err != nil {
			s.log.Error("failed to close blob object", "err", err)
		}
	}()

	// set headers
	w.Header().Set("Content-Type", blob.MimeType)
	w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=%s", blobCID.String()))
	w.WriteHeader(http.StatusOK)

	// stream the blob
	if _, err := io.Copy(w, obj); err != nil {
		s.log.Error("failed to stream blob", "err", err)
	}
}
