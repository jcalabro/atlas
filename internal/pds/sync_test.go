package pds

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/bluesky-social/indigo/api/atproto"
	"github.com/ipld/go-car"
	carutil "github.com/ipld/go-car/util"
	"github.com/stretchr/testify/require"
)

func TestHandleGetLatestCommit(t *testing.T) {
	t.Parallel()
	srv := testServer(t)
	router := srv.router()
	ctx := context.WithValue(t.Context(), hostContextKey{}, srv.hosts[testPDSHost])

	t.Run("success - returns latest commit for repo", func(t *testing.T) {
		t.Parallel()

		actor, _ := setupTestActor(t, srv, "did:plc:latestcommit1", "latestcommit1@example.com", "latestcommit1.dev.atlaspds.dev")

		w := httptest.NewRecorder()
		url := fmt.Sprintf("/xrpc/com.atproto.sync.getLatestCommit?did=%s", actor.Did)
		req := httptest.NewRequest(http.MethodGet, url, nil)
		req = addTestHostContext(srv, req)
		router.ServeHTTP(w, req)

		require.Equal(t, http.StatusOK, w.Code)
		require.Equal(t, "application/json", w.Header().Get("Content-Type"))

		var out struct {
			Cid string `json:"cid"`
			Rev string `json:"rev"`
		}
		err := json.Unmarshal(w.Body.Bytes(), &out)
		require.NoError(t, err)

		require.Equal(t, actor.Head, out.Cid)
		require.Equal(t, actor.Rev, out.Rev)
		require.NotEmpty(t, out.Cid)
		require.NotEmpty(t, out.Rev)
	})

	t.Run("success - commit updates after creating record", func(t *testing.T) {
		t.Parallel()

		actor, session := setupTestActor(t, srv, "did:plc:latestcommit2", "latestcommit2@example.com", "latestcommit2.dev.atlaspds.dev")
		initialHead := actor.Head
		initialRev := actor.Rev

		// get latest commit before creating record
		w := httptest.NewRecorder()
		url := fmt.Sprintf("/xrpc/com.atproto.sync.getLatestCommit?did=%s", actor.Did)
		req := httptest.NewRequest(http.MethodGet, url, nil)
		req = addTestHostContext(srv, req)
		router.ServeHTTP(w, req)
		require.Equal(t, http.StatusOK, w.Code)

		var beforeOut struct {
			Cid string `json:"cid"`
			Rev string `json:"rev"`
		}
		err := json.Unmarshal(w.Body.Bytes(), &beforeOut)
		require.NoError(t, err)
		require.Equal(t, initialHead, beforeOut.Cid)
		require.Equal(t, initialRev, beforeOut.Rev)

		// create a record
		tid, err := srv.db.NextTID(ctx, actor.Did)
		require.NoError(t, err)

		input := map[string]any{
			"repo":       actor.Did,
			"collection": "app.bsky.feed.post",
			"rkey":       tid.String(),
			"record": map[string]any{
				"$type":     "app.bsky.feed.post",
				"text":      "Test post for getLatestCommit",
				"createdAt": time.Now().Format(time.RFC3339),
			},
		}

		body, err := json.Marshal(input)
		require.NoError(t, err)

		w = httptest.NewRecorder()
		req = httptest.NewRequest(http.MethodPost, "/xrpc/com.atproto.repo.createRecord", bytes.NewReader(body))
		req = addAuthContext(t, ctx, srv, req, actor, session.AccessToken)
		srv.handleCreateRecord(w, req)
		require.Equal(t, http.StatusOK, w.Code)

		// get latest commit after creating record
		w = httptest.NewRecorder()
		req = httptest.NewRequest(http.MethodGet, url, nil)
		req = addTestHostContext(srv, req)
		router.ServeHTTP(w, req)
		require.Equal(t, http.StatusOK, w.Code)

		var afterOut struct {
			Cid string `json:"cid"`
			Rev string `json:"rev"`
		}
		err = json.Unmarshal(w.Body.Bytes(), &afterOut)
		require.NoError(t, err)

		// head and rev should have changed
		require.NotEqual(t, initialHead, afterOut.Cid, "head should change after creating record")
		require.NotEqual(t, initialRev, afterOut.Rev, "rev should change after creating record")
	})

	t.Run("error - missing did parameter", func(t *testing.T) {
		t.Parallel()

		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/xrpc/com.atproto.sync.getLatestCommit", nil)
		req = addTestHostContext(srv, req)
		router.ServeHTTP(w, req)

		require.Equal(t, http.StatusBadRequest, w.Code)
	})

	t.Run("error - invalid did format", func(t *testing.T) {
		t.Parallel()

		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/xrpc/com.atproto.sync.getLatestCommit?did=not-a-did", nil)
		req = addTestHostContext(srv, req)
		router.ServeHTTP(w, req)

		require.Equal(t, http.StatusBadRequest, w.Code)
	})

	t.Run("error - repo not found", func(t *testing.T) {
		t.Parallel()

		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/xrpc/com.atproto.sync.getLatestCommit?did=did:plc:nonexistent", nil)
		req = addTestHostContext(srv, req)
		router.ServeHTTP(w, req)

		require.Equal(t, http.StatusNotFound, w.Code)
	})
}

func TestHandleGetRepoStatus(t *testing.T) {
	t.Parallel()
	srv := testServer(t)
	router := srv.router()
	ctx := context.WithValue(t.Context(), hostContextKey{}, srv.hosts[testPDSHost])

	t.Run("success - returns status for active repo", func(t *testing.T) {
		t.Parallel()

		actor, _ := setupTestActor(t, srv, "did:plc:repostatus1", "repostatus1@example.com", "repostatus1.dev.atlaspds.dev")

		w := httptest.NewRecorder()
		url := fmt.Sprintf("/xrpc/com.atproto.sync.getRepoStatus?did=%s", actor.Did)
		req := httptest.NewRequest(http.MethodGet, url, nil)
		req = addTestHostContext(srv, req)
		router.ServeHTTP(w, req)

		require.Equal(t, http.StatusOK, w.Code)
		require.Equal(t, "application/json", w.Header().Get("Content-Type"))

		var out atproto.SyncGetRepoStatus_Output
		err := json.Unmarshal(w.Body.Bytes(), &out)
		require.NoError(t, err)

		require.Equal(t, actor.Did, out.Did)
		require.True(t, out.Active)
		require.NotNil(t, out.Rev, "rev should be present for active repos")
		require.Equal(t, actor.Rev, *out.Rev)
		require.Nil(t, out.Status, "status should be nil for active repos")
	})

	t.Run("success - returns status for inactive repo", func(t *testing.T) {
		t.Parallel()

		actor, _ := setupTestActor(t, srv, "did:plc:repostatus2", "repostatus2@example.com", "repostatus2.dev.atlaspds.dev")

		// deactivate the actor
		actor.Active = false
		err := srv.db.SaveActor(ctx, actor)
		require.NoError(t, err)

		w := httptest.NewRecorder()
		url := fmt.Sprintf("/xrpc/com.atproto.sync.getRepoStatus?did=%s", actor.Did)
		req := httptest.NewRequest(http.MethodGet, url, nil)
		req = addTestHostContext(srv, req)
		router.ServeHTTP(w, req)

		require.Equal(t, http.StatusOK, w.Code)

		var out atproto.SyncGetRepoStatus_Output
		err = json.Unmarshal(w.Body.Bytes(), &out)
		require.NoError(t, err)

		require.Equal(t, actor.Did, out.Did)
		require.False(t, out.Active)
		require.Nil(t, out.Rev, "rev should be nil for inactive repos")
	})

	t.Run("error - missing did parameter", func(t *testing.T) {
		t.Parallel()

		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/xrpc/com.atproto.sync.getRepoStatus", nil)
		req = addTestHostContext(srv, req)
		router.ServeHTTP(w, req)

		require.Equal(t, http.StatusBadRequest, w.Code)
	})

	t.Run("error - invalid did format", func(t *testing.T) {
		t.Parallel()

		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/xrpc/com.atproto.sync.getRepoStatus?did=not-a-did", nil)
		req = addTestHostContext(srv, req)
		router.ServeHTTP(w, req)

		require.Equal(t, http.StatusBadRequest, w.Code)
	})

	t.Run("error - repo not found", func(t *testing.T) {
		t.Parallel()

		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/xrpc/com.atproto.sync.getRepoStatus?did=did:plc:nonexistent", nil)
		req = addTestHostContext(srv, req)
		router.ServeHTTP(w, req)

		require.Equal(t, http.StatusNotFound, w.Code)
	})
}

func TestHandleGetBlocks(t *testing.T) {
	t.Parallel()
	srv := testServer(t)
	router := srv.router()
	ctx := context.WithValue(t.Context(), hostContextKey{}, srv.hosts[testPDSHost])

	t.Run("success - retrieves blocks from repo", func(t *testing.T) {
		t.Parallel()

		actor, session := setupTestActor(t, srv, "did:plc:getblocks1", "getblocks1@example.com", "getblocks1.dev.atlaspds.dev")

		// create a record to have blocks in the repo
		tid, err := srv.db.NextTID(ctx, actor.Did)
		require.NoError(t, err)
		rkey := tid.String()

		input := map[string]any{
			"repo":       actor.Did,
			"collection": "app.bsky.feed.post",
			"rkey":       rkey,
			"record": map[string]any{
				"$type":     "app.bsky.feed.post",
				"text":      "Test post for getBlocks",
				"createdAt": time.Now().Format(time.RFC3339),
			},
		}

		body, err := json.Marshal(input)
		require.NoError(t, err)

		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/xrpc/com.atproto.repo.createRecord", bytes.NewReader(body))
		req = addAuthContext(t, ctx, srv, req, actor, session.AccessToken)
		srv.handleCreateRecord(w, req)
		require.Equal(t, http.StatusOK, w.Code)

		var createOut atproto.RepoCreateRecord_Output
		err = json.Unmarshal(w.Body.Bytes(), &createOut)
		require.NoError(t, err)

		// reload actor to get the current head
		actor, err = srv.db.GetActorByDID(ctx, actor.Did)
		require.NoError(t, err)

		// request the record block
		w = httptest.NewRecorder()
		url := fmt.Sprintf("/xrpc/com.atproto.sync.getBlocks?did=%s&cids=%s", actor.Did, createOut.Cid)
		req = httptest.NewRequest(http.MethodGet, url, nil)
		req = addTestHostContext(srv, req)
		router.ServeHTTP(w, req)

		require.Equal(t, http.StatusOK, w.Code)
		require.Equal(t, "application/vnd.ipld.car", w.Header().Get("Content-Type"))

		// verify it's a valid CAR file
		carReader, err := car.NewCarReader(bytes.NewReader(w.Body.Bytes()))
		require.NoError(t, err)

		// verify header has roots
		require.Len(t, carReader.Header.Roots, 1)
		require.Equal(t, actor.Head, carReader.Header.Roots[0].String())

		// read blocks from CAR
		blockCount := 0
		for {
			blk, err := carReader.Next()
			if err != nil {
				break
			}
			blockCount++
			// verify the block CID matches what we requested
			require.Equal(t, createOut.Cid, blk.Cid().String())
		}
		require.Equal(t, 1, blockCount, "should have exactly one block")
	})

	t.Run("success - retrieves multiple blocks", func(t *testing.T) {
		t.Parallel()

		actor, session := setupTestActor(t, srv, "did:plc:getblocks2", "getblocks2@example.com", "getblocks2.dev.atlaspds.dev")

		// create two records to have multiple blocks
		var cids []string
		for i := range 2 {
			tid, err := srv.db.NextTID(ctx, actor.Did)
			require.NoError(t, err)
			rkey := tid.String()

			input := map[string]any{
				"repo":       actor.Did,
				"collection": "app.bsky.feed.post",
				"rkey":       rkey,
				"record": map[string]any{
					"$type":     "app.bsky.feed.post",
					"text":      fmt.Sprintf("Test post %d for getBlocks", i),
					"createdAt": time.Now().Format(time.RFC3339),
				},
			}

			body, err := json.Marshal(input)
			require.NoError(t, err)

			// reload actor to get current head before each create
			actor, err = srv.db.GetActorByDID(ctx, actor.Did)
			require.NoError(t, err)

			w := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodPost, "/xrpc/com.atproto.repo.createRecord", bytes.NewReader(body))
			req = addAuthContext(t, ctx, srv, req, actor, session.AccessToken)
			srv.handleCreateRecord(w, req)
			require.Equal(t, http.StatusOK, w.Code)

			var createOut atproto.RepoCreateRecord_Output
			err = json.Unmarshal(w.Body.Bytes(), &createOut)
			require.NoError(t, err)
			cids = append(cids, createOut.Cid)
		}

		// reload actor to get current head
		actor, err := srv.db.GetActorByDID(ctx, actor.Did)
		require.NoError(t, err)

		// request both blocks
		w := httptest.NewRecorder()
		url := fmt.Sprintf("/xrpc/com.atproto.sync.getBlocks?did=%s&cids=%s&cids=%s", actor.Did, cids[0], cids[1])
		req := httptest.NewRequest(http.MethodGet, url, nil)
		req = addTestHostContext(srv, req)
		router.ServeHTTP(w, req)

		require.Equal(t, http.StatusOK, w.Code)
		require.Equal(t, "application/vnd.ipld.car", w.Header().Get("Content-Type"))

		// verify it's a valid CAR file with 2 blocks
		carReader, err := car.NewCarReader(bytes.NewReader(w.Body.Bytes()))
		require.NoError(t, err)

		blockCount := 0
		returnedCids := make(map[string]bool)
		for {
			blk, err := carReader.Next()
			if err != nil {
				break
			}
			blockCount++
			returnedCids[blk.Cid().String()] = true
		}
		require.Equal(t, 2, blockCount, "should have exactly two blocks")
		require.True(t, returnedCids[cids[0]], "should contain first CID")
		require.True(t, returnedCids[cids[1]], "should contain second CID")
	})

	t.Run("success - returns empty CAR for non-existent blocks", func(t *testing.T) {
		t.Parallel()

		actor, _ := setupTestActor(t, srv, "did:plc:getblocks3", "getblocks3@example.com", "getblocks3.dev.atlaspds.dev")

		// request a block that doesn't exist
		nonExistentCID := "bafyreihx6qqvghcmvpqq33kg4s7ztnh6mlt5cqpynjjxgcoynvndx5cuee"

		w := httptest.NewRecorder()
		url := fmt.Sprintf("/xrpc/com.atproto.sync.getBlocks?did=%s&cids=%s", actor.Did, nonExistentCID)
		req := httptest.NewRequest(http.MethodGet, url, nil)
		req = addTestHostContext(srv, req)
		router.ServeHTTP(w, req)

		require.Equal(t, http.StatusOK, w.Code)
		require.Equal(t, "application/vnd.ipld.car", w.Header().Get("Content-Type"))

		// should be a valid CAR with no blocks
		carReader, err := car.NewCarReader(bytes.NewReader(w.Body.Bytes()))
		require.NoError(t, err)
		require.Len(t, carReader.Header.Roots, 1)

		// verify no blocks
		_, err = carReader.Next()
		require.Error(t, err, "should have no blocks")
	})

	t.Run("error - missing did parameter", func(t *testing.T) {
		t.Parallel()

		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/xrpc/com.atproto.sync.getBlocks?cids=bafyreihx6qqvghcmvpqq33kg4s7ztnh6mlt5cqpynjjxgcoynvndx5cuee", nil)
		req = addTestHostContext(srv, req)
		router.ServeHTTP(w, req)

		require.Equal(t, http.StatusBadRequest, w.Code)
	})

	t.Run("error - missing cids parameter", func(t *testing.T) {
		t.Parallel()

		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/xrpc/com.atproto.sync.getBlocks?did=did:plc:test", nil)
		req = addTestHostContext(srv, req)
		router.ServeHTTP(w, req)

		require.Equal(t, http.StatusBadRequest, w.Code)
	})

	t.Run("error - invalid did format", func(t *testing.T) {
		t.Parallel()

		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/xrpc/com.atproto.sync.getBlocks?did=not-a-did&cids=bafyreihx6qqvghcmvpqq33kg4s7ztnh6mlt5cqpynjjxgcoynvndx5cuee", nil)
		req = addTestHostContext(srv, req)
		router.ServeHTTP(w, req)

		require.Equal(t, http.StatusBadRequest, w.Code)
	})

	t.Run("error - invalid cid format", func(t *testing.T) {
		t.Parallel()

		actor, _ := setupTestActor(t, srv, "did:plc:getblocks4", "getblocks4@example.com", "getblocks4.dev.atlaspds.dev")

		w := httptest.NewRecorder()
		url := fmt.Sprintf("/xrpc/com.atproto.sync.getBlocks?did=%s&cids=not-a-valid-cid", actor.Did)
		req := httptest.NewRequest(http.MethodGet, url, nil)
		req = addTestHostContext(srv, req)
		router.ServeHTTP(w, req)

		require.Equal(t, http.StatusBadRequest, w.Code)
	})

	t.Run("error - repo not found", func(t *testing.T) {
		t.Parallel()

		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/xrpc/com.atproto.sync.getBlocks?did=did:plc:nonexistent&cids=bafyreihx6qqvghcmvpqq33kg4s7ztnh6mlt5cqpynjjxgcoynvndx5cuee", nil)
		req = addTestHostContext(srv, req)
		router.ServeHTTP(w, req)

		require.Equal(t, http.StatusNotFound, w.Code)
	})
}

// TestLdWriteFormat verifies that the CAR format is correct
func TestLdWriteFormat(t *testing.T) {
	t.Parallel()

	t.Run("ldWrite produces valid length-delimited data", func(t *testing.T) {
		t.Parallel()

		buf := new(bytes.Buffer)
		data := []byte("test data block")

		err := carutil.LdWrite(buf, data)
		require.NoError(t, err)

		// verify the format: varint length prefix + data
		result := buf.Bytes()
		require.Greater(t, len(result), len(data), "should have length prefix")

		// read varint length
		length, n := readUvarint(result)
		require.Equal(t, uint64(len(data)), length)
		require.Equal(t, data, result[n:])
	})

	t.Run("ldWrite with multiple byte slices concatenates them", func(t *testing.T) {
		t.Parallel()

		buf := new(bytes.Buffer)
		cidBytes := []byte{0x01, 0x55, 0x12, 0x20} // fake CID prefix
		data := []byte("block content")

		err := carutil.LdWrite(buf, cidBytes, data)
		require.NoError(t, err)

		result := buf.Bytes()

		// read varint length - should be sum of both byte slices
		length, n := readUvarint(result)
		require.Equal(t, uint64(len(cidBytes)+len(data)), length)

		// verify concatenated data
		require.Equal(t, cidBytes, result[n:n+len(cidBytes)])
		require.Equal(t, data, result[n+len(cidBytes):])
	})
}

// helper to read unsigned varint
func readUvarint(buf []byte) (uint64, int) {
	var x uint64
	var s uint
	for i, b := range buf {
		if b < 0x80 {
			return x | uint64(b)<<s, i + 1
		}
		x |= uint64(b&0x7f) << s
		s += 7
	}
	return x, len(buf)
}

// TestGetBlocksIntegration tests the full flow of creating a record and retrieving its blocks
func TestGetBlocksIntegration(t *testing.T) {
	t.Parallel()
	srv := testServer(t)
	router := srv.router()
	ctx := context.WithValue(t.Context(), hostContextKey{}, srv.hosts[testPDSHost])

	t.Run("can retrieve commit block", func(t *testing.T) {
		t.Parallel()

		actor, session := setupTestActor(t, srv, "did:plc:getblocksint1", "getblocksint1@example.com", "getblocksint1.dev.atlaspds.dev")

		// create a record
		tid, err := srv.db.NextTID(ctx, actor.Did)
		require.NoError(t, err)

		input := map[string]any{
			"repo":       actor.Did,
			"collection": "app.bsky.feed.post",
			"rkey":       tid.String(),
			"record": map[string]any{
				"$type":     "app.bsky.feed.post",
				"text":      "Integration test post",
				"createdAt": time.Now().Format(time.RFC3339),
			},
		}

		body, err := json.Marshal(input)
		require.NoError(t, err)

		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/xrpc/com.atproto.repo.createRecord", bytes.NewReader(body))
		req = addAuthContext(t, ctx, srv, req, actor, session.AccessToken)
		srv.handleCreateRecord(w, req)
		require.Equal(t, http.StatusOK, w.Code)

		var createOut atproto.RepoCreateRecord_Output
		err = json.Unmarshal(w.Body.Bytes(), &createOut)
		require.NoError(t, err)

		// reload actor to get current head (commit CID)
		actor, err = srv.db.GetActorByDID(ctx, actor.Did)
		require.NoError(t, err)

		// request the commit block using the actor's head
		w = httptest.NewRecorder()
		url := fmt.Sprintf("/xrpc/com.atproto.sync.getBlocks?did=%s&cids=%s", actor.Did, actor.Head)
		req = httptest.NewRequest(http.MethodGet, url, nil)
		req = addTestHostContext(srv, req)
		router.ServeHTTP(w, req)

		require.Equal(t, http.StatusOK, w.Code)

		// parse CAR and verify we got the commit block
		carReader, err := car.NewCarReader(bytes.NewReader(w.Body.Bytes()))
		require.NoError(t, err)

		blk, err := carReader.Next()
		require.NoError(t, err)
		require.Equal(t, actor.Head, blk.Cid().String())
	})

	t.Run("can retrieve both record and commit blocks", func(t *testing.T) {
		t.Parallel()

		actor, session := setupTestActor(t, srv, "did:plc:getblocksint2", "getblocksint2@example.com", "getblocksint2.dev.atlaspds.dev")

		// create a record
		tid, err := srv.db.NextTID(ctx, actor.Did)
		require.NoError(t, err)

		input := map[string]any{
			"repo":       actor.Did,
			"collection": "app.bsky.feed.post",
			"rkey":       tid.String(),
			"record": map[string]any{
				"$type":     "app.bsky.feed.post",
				"text":      "Integration test post 2",
				"createdAt": time.Now().Format(time.RFC3339),
			},
		}

		body, err := json.Marshal(input)
		require.NoError(t, err)

		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/xrpc/com.atproto.repo.createRecord", bytes.NewReader(body))
		req = addAuthContext(t, ctx, srv, req, actor, session.AccessToken)
		srv.handleCreateRecord(w, req)
		require.Equal(t, http.StatusOK, w.Code)

		var createOut atproto.RepoCreateRecord_Output
		err = json.Unmarshal(w.Body.Bytes(), &createOut)
		require.NoError(t, err)
		recordCID := createOut.Cid

		// reload actor to get current head
		actor, err = srv.db.GetActorByDID(ctx, actor.Did)
		require.NoError(t, err)
		commitCID := actor.Head

		// request both the record and commit blocks
		w = httptest.NewRecorder()
		url := fmt.Sprintf("/xrpc/com.atproto.sync.getBlocks?did=%s&cids=%s&cids=%s", actor.Did, recordCID, commitCID)
		req = httptest.NewRequest(http.MethodGet, url, nil)
		req = addTestHostContext(srv, req)
		router.ServeHTTP(w, req)

		require.Equal(t, http.StatusOK, w.Code)

		// parse CAR and verify we got both blocks
		carReader, err := car.NewCarReader(bytes.NewReader(w.Body.Bytes()))
		require.NoError(t, err)

		returnedCids := make(map[string]bool)
		for {
			blk, err := carReader.Next()
			if err != nil {
				break
			}
			returnedCids[blk.Cid().String()] = true
		}

		require.Len(t, returnedCids, 2)
		require.True(t, returnedCids[recordCID], "should have record block")
		require.True(t, returnedCids[commitCID], "should have commit block")
	})
}

// TestGetBlocksCIDValidation tests CID parsing edge cases
func TestGetBlocksCIDValidation(t *testing.T) {
	t.Parallel()
	srv := testServer(t)
	router := srv.router()

	actor, _ := setupTestActor(t, srv, "did:plc:cidvalidation", "cidvalidation@example.com", "cidvalidation.dev.atlaspds.dev")

	testCases := []struct {
		name        string
		cid         string
		expectError bool
	}{
		{"valid CIDv1", "bafyreihx6qqvghcmvpqq33kg4s7ztnh6mlt5cqpynjjxgcoynvndx5cuee", false},
		{"empty cid", "", true},
		{"invalid base32", "bafyrei!!invalid!!", true},
		{"too short", "bafy", true},
		{"not base encoded", "helloworld", true},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			w := httptest.NewRecorder()
			url := fmt.Sprintf("/xrpc/com.atproto.sync.getBlocks?did=%s&cids=%s", actor.Did, tc.cid)
			req := httptest.NewRequest(http.MethodGet, url, nil)
			req = addTestHostContext(srv, req)
			router.ServeHTTP(w, req)

			if tc.expectError {
				require.Equal(t, http.StatusBadRequest, w.Code, "expected bad request for cid: %s", tc.cid)
			} else {
				// for valid CIDs that don't exist, we should get success (empty CAR)
				require.Equal(t, http.StatusOK, w.Code, "expected success for cid: %s", tc.cid)
			}
		})
	}
}
