package pds

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/bluesky-social/indigo/api/atproto"
	"github.com/bluesky-social/indigo/lex/util"
	"github.com/ipfs/go-cid"
	"github.com/jcalabro/atlas/internal/pds/db"
	"github.com/jcalabro/atlas/internal/pds/metrics"
	"github.com/jcalabro/atlas/internal/types"
	"github.com/multiformats/go-multihash"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// blobstore wraps an S3-compatible client for blob storage
type blobstore struct {
	client *s3.Client
	bucket string
}

func newBlobstore(cfg *BlobstoreConfig) (*blobstore, error) {
	client := s3.New(s3.Options{
		BaseEndpoint: aws.String(fmt.Sprintf("http://%s", cfg.Endpoint)),
		Region:       cfg.Region,
		Credentials:  credentials.NewStaticCredentialsProvider(cfg.AccessKey, cfg.SecretKey, ""),
		UsePathStyle: true, // required for S3-compatible services like Garage
	})

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
	_, err = s.blobstore.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(s.blobstore.bucket),
		Key:         aws.String(key),
		Body:        bytes.NewReader(data),
		ContentType: aws.String(mimeType),
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
		metrics.BlobUploads.WithLabelValues("error").Inc()
		s.internalErr(w, fmt.Errorf("failed to save blob metadata: %w", err))
		return
	}

	// record successful upload metrics
	metrics.BlobUploads.WithLabelValues("success").Inc()
	metrics.BlobUploadBytes.Add(float64(len(data)))

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
		metrics.BlobDownloads.WithLabelValues("not_found").Inc()
		s.notFound(w, fmt.Errorf("blob not found"))
		return
	}
	if err != nil {
		metrics.BlobDownloads.WithLabelValues("error").Inc()
		s.internalErr(w, fmt.Errorf("failed to get blob metadata: %w", err))
		return
	}

	// fetch from S3
	key := blobKey(did, blobCID)
	result, err := s.blobstore.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.blobstore.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		metrics.BlobDownloads.WithLabelValues("error").Inc()
		s.internalErr(w, fmt.Errorf("failed to get blob from S3: %w", err))
		return
	}
	defer func() {
		if err := result.Body.Close(); err != nil {
			s.log.Error("failed to close blob object", "err", err)
		}
	}()

	// record successful download metrics
	metrics.BlobDownloads.WithLabelValues("success").Inc()
	metrics.BlobDownloadBytes.Add(float64(blob.Size))

	// set headers
	w.Header().Set("Content-Type", blob.MimeType)
	w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=%s", blobCID.String()))
	w.WriteHeader(http.StatusOK)

	// stream the blob
	if _, err := io.Copy(w, result.Body); err != nil {
		s.log.Error("failed to stream blob", "err", err)
	}
}

// bucketExists checks if the configured bucket exists (used for health checks and tests)
func (bs *blobstore) bucketExists(ctx context.Context) (bool, error) {
	_, err := bs.client.HeadBucket(ctx, &s3.HeadBucketInput{
		Bucket: aws.String(bs.bucket),
	})
	if err != nil {
		return false, err
	}
	return true, nil
}
