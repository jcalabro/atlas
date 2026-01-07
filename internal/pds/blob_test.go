package pds

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/bluesky-social/indigo/api/atproto"
	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/require"
)

const (
	testBlobstoreEndpoint = "localhost:3900"
	testBlobstoreBucket   = "blobs"
	testBlobstoreRegion   = "garage"
	testBlobstoreKeyID    = "GK000000000000000000000000"
	testBlobstoreSecret   = "0000000000000000000000000000000000000000000000000000000000000000"
)

func testBlobstore(t *testing.T) *blobstore {
	t.Helper()

	client := s3.New(s3.Options{
		BaseEndpoint: aws.String(fmt.Sprintf("http://%s", testBlobstoreEndpoint)),
		Region:       testBlobstoreRegion,
		Credentials:  credentials.NewStaticCredentialsProvider(testBlobstoreKeyID, testBlobstoreSecret, ""),
		UsePathStyle: true,
	})

	bs := &blobstore{
		client: client,
		bucket: testBlobstoreBucket,
	}

	// check if bucket exists
	ctx := t.Context()
	exists, err := bs.bucketExists(ctx)
	if err != nil || !exists {
		t.Skipf("skipping blob test: blobstore not available (bucket %q does not exist or error: %v)", testBlobstoreBucket, err)
	}

	return bs
}

func testServerWithBlobstore(t *testing.T) *server {
	t.Helper()

	srv := testServer(t)
	srv.blobstore = testBlobstore(t)
	return srv
}

func TestHandleUploadBlob(t *testing.T) {
	t.Parallel()

	srv := testServerWithBlobstore(t)
	router := srv.router()
	ctx := t.Context()

	actor, session := setupTestActor(t, srv, "did:plc:uploadblobtest1", "uploadblob@example.com", "uploadblob.dev.atlaspds.dev")

	t.Run("success - uploads blob and returns CID", func(t *testing.T) {
		t.Parallel()

		blobContent := []byte("hello world this is test blob content")
		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/xrpc/com.atproto.repo.uploadBlob", bytes.NewReader(blobContent))
		req.Header.Set("Content-Type", "text/plain")
		req = addAuthContext(t, ctx, srv, req, actor, session.AccessToken)
		router.ServeHTTP(w, req)

		require.Equal(t, http.StatusOK, w.Code)
		require.Equal(t, "application/json", w.Header().Get("Content-Type"))

		var resp atproto.RepoUploadBlob_Output
		err := json.Unmarshal(w.Body.Bytes(), &resp)
		require.NoError(t, err)

		require.NotNil(t, resp.Blob)
		require.Equal(t, "text/plain", resp.Blob.MimeType)
		require.Equal(t, int64(len(blobContent)), resp.Blob.Size)

		// verify CID is valid
		require.NotEmpty(t, resp.Blob.Ref.String())

		// verify the CID matches our expected computation
		cidBuilder := cid.NewPrefixV1(cid.Raw, multihash.SHA2_256)
		expectedCID, err := cidBuilder.Sum(blobContent)
		require.NoError(t, err)
		require.Equal(t, expectedCID.String(), resp.Blob.Ref.String())
	})

	t.Run("success - uploads blob with default content type", func(t *testing.T) {
		t.Parallel()

		blobContent := []byte("binary data without content type")
		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/xrpc/com.atproto.repo.uploadBlob", bytes.NewReader(blobContent))
		// intentionally not setting Content-Type
		req = addAuthContext(t, ctx, srv, req, actor, session.AccessToken)
		router.ServeHTTP(w, req)

		require.Equal(t, http.StatusOK, w.Code)

		var resp atproto.RepoUploadBlob_Output
		err := json.Unmarshal(w.Body.Bytes(), &resp)
		require.NoError(t, err)

		require.NotNil(t, resp.Blob)
		require.Equal(t, "application/octet-stream", resp.Blob.MimeType)
	})

	t.Run("error - empty blob", func(t *testing.T) {
		t.Parallel()

		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/xrpc/com.atproto.repo.uploadBlob", bytes.NewReader([]byte{}))
		req = addAuthContext(t, ctx, srv, req, actor, session.AccessToken)
		router.ServeHTTP(w, req)

		require.Equal(t, http.StatusBadRequest, w.Code)
	})

	t.Run("error - unauthorized without token", func(t *testing.T) {
		t.Parallel()

		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/xrpc/com.atproto.repo.uploadBlob", bytes.NewReader([]byte("test")))
		req = addTestHostContext(srv, req)
		router.ServeHTTP(w, req)

		require.Equal(t, http.StatusUnauthorized, w.Code)
	})
}

func TestHandleListBlobs(t *testing.T) {
	t.Parallel()

	srv := testServerWithBlobstore(t)
	router := srv.router()
	ctx := t.Context()

	actor, session := setupTestActor(t, srv, "did:plc:listblobstest1", "listblobs@example.com", "listblobs.dev.atlaspds.dev")

	// upload some test blobs
	uploadedCIDs := make([]string, 0, 5)
	for i := range 5 {
		blobContent := fmt.Appendf(nil, "list blobs test content %d", i)
		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/xrpc/com.atproto.repo.uploadBlob", bytes.NewReader(blobContent))
		req.Header.Set("Content-Type", "text/plain")
		req = addAuthContext(t, ctx, srv, req, actor, session.AccessToken)
		router.ServeHTTP(w, req)
		require.Equal(t, http.StatusOK, w.Code)

		var resp atproto.RepoUploadBlob_Output
		err := json.Unmarshal(w.Body.Bytes(), &resp)
		require.NoError(t, err)
		uploadedCIDs = append(uploadedCIDs, resp.Blob.Ref.String())
	}

	t.Run("success - lists blobs for user", func(t *testing.T) {
		t.Parallel()

		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, fmt.Sprintf("/xrpc/com.atproto.sync.listBlobs?did=%s", actor.Did), nil)
		req = addTestHostContext(srv, req)
		router.ServeHTTP(w, req)

		require.Equal(t, http.StatusOK, w.Code)

		var resp atproto.SyncListBlobs_Output
		err := json.Unmarshal(w.Body.Bytes(), &resp)
		require.NoError(t, err)

		require.NotNil(t, resp.Cids)
		require.GreaterOrEqual(t, len(resp.Cids), 5)

		// verify our uploaded CIDs are in the response
		cidSet := make(map[string]bool)
		for _, c := range resp.Cids {
			cidSet[c] = true
		}
		for _, uploadedCID := range uploadedCIDs {
			require.True(t, cidSet[uploadedCID], "uploaded CID %s should be in list", uploadedCID)
		}
	})

	t.Run("success - respects limit parameter", func(t *testing.T) {
		t.Parallel()

		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, fmt.Sprintf("/xrpc/com.atproto.sync.listBlobs?did=%s&limit=2", actor.Did), nil)
		req = addTestHostContext(srv, req)
		router.ServeHTTP(w, req)

		require.Equal(t, http.StatusOK, w.Code)

		var resp atproto.SyncListBlobs_Output
		err := json.Unmarshal(w.Body.Bytes(), &resp)
		require.NoError(t, err)

		require.LessOrEqual(t, len(resp.Cids), 2)
	})

	t.Run("success - caps limit at 1000", func(t *testing.T) {
		t.Parallel()

		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, fmt.Sprintf("/xrpc/com.atproto.sync.listBlobs?did=%s&limit=9999", actor.Did), nil)
		req = addTestHostContext(srv, req)
		router.ServeHTTP(w, req)

		require.Equal(t, http.StatusOK, w.Code)
		// just verifying the request succeeded with oversized limit
	})

	t.Run("error - missing did parameter", func(t *testing.T) {
		t.Parallel()

		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/xrpc/com.atproto.sync.listBlobs", nil)
		req = addTestHostContext(srv, req)
		router.ServeHTTP(w, req)

		require.Equal(t, http.StatusBadRequest, w.Code)
	})

	t.Run("error - invalid limit", func(t *testing.T) {
		t.Parallel()

		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, fmt.Sprintf("/xrpc/com.atproto.sync.listBlobs?did=%s&limit=-1", actor.Did), nil)
		req = addTestHostContext(srv, req)
		router.ServeHTTP(w, req)

		require.Equal(t, http.StatusBadRequest, w.Code)
	})
}

func TestHandleGetBlob(t *testing.T) {
	t.Parallel()

	srv := testServerWithBlobstore(t)
	router := srv.router()
	ctx := t.Context()

	actor, session := setupTestActor(t, srv, "did:plc:getblobtest1", "getblob@example.com", "getblob.dev.atlaspds.dev")

	// upload a test blob
	blobContent := []byte("get blob test content - hello world!")
	var uploadedCID string
	{
		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/xrpc/com.atproto.repo.uploadBlob", bytes.NewReader(blobContent))
		req.Header.Set("Content-Type", "text/plain")
		req = addAuthContext(t, ctx, srv, req, actor, session.AccessToken)
		router.ServeHTTP(w, req)
		require.Equal(t, http.StatusOK, w.Code)

		var resp atproto.RepoUploadBlob_Output
		err := json.Unmarshal(w.Body.Bytes(), &resp)
		require.NoError(t, err)
		uploadedCID = resp.Blob.Ref.String()
	}

	t.Run("success - gets blob content", func(t *testing.T) {
		t.Parallel()

		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, fmt.Sprintf("/xrpc/com.atproto.sync.getBlob?did=%s&cid=%s", actor.Did, uploadedCID), nil)
		req = addTestHostContext(srv, req)
		router.ServeHTTP(w, req)

		require.Equal(t, http.StatusOK, w.Code)
		require.Equal(t, "text/plain", w.Header().Get("Content-Type"))
		require.Contains(t, w.Header().Get("Content-Disposition"), uploadedCID)

		body, err := io.ReadAll(w.Body)
		require.NoError(t, err)
		require.Equal(t, blobContent, body)
	})

	t.Run("error - missing did parameter", func(t *testing.T) {
		t.Parallel()

		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, fmt.Sprintf("/xrpc/com.atproto.sync.getBlob?cid=%s", uploadedCID), nil)
		req = addTestHostContext(srv, req)
		router.ServeHTTP(w, req)

		require.Equal(t, http.StatusBadRequest, w.Code)
	})

	t.Run("error - missing cid parameter", func(t *testing.T) {
		t.Parallel()

		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, fmt.Sprintf("/xrpc/com.atproto.sync.getBlob?did=%s", actor.Did), nil)
		req = addTestHostContext(srv, req)
		router.ServeHTTP(w, req)

		require.Equal(t, http.StatusBadRequest, w.Code)
	})

	t.Run("error - invalid cid", func(t *testing.T) {
		t.Parallel()

		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, fmt.Sprintf("/xrpc/com.atproto.sync.getBlob?did=%s&cid=invalid", actor.Did), nil)
		req = addTestHostContext(srv, req)
		router.ServeHTTP(w, req)

		require.Equal(t, http.StatusBadRequest, w.Code)
	})

	t.Run("error - blob not found", func(t *testing.T) {
		t.Parallel()

		// generate a valid CID that doesn't exist
		cidBuilder := cid.NewPrefixV1(cid.Raw, multihash.SHA2_256)
		fakeCID, err := cidBuilder.Sum([]byte("this content does not exist"))
		require.NoError(t, err)

		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, fmt.Sprintf("/xrpc/com.atproto.sync.getBlob?did=%s&cid=%s", actor.Did, fakeCID.String()), nil)
		req = addTestHostContext(srv, req)
		router.ServeHTTP(w, req)

		require.Equal(t, http.StatusNotFound, w.Code)
	})
}
