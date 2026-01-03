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
	"github.com/jcalabro/atlas/internal/types"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestComputeCID(t *testing.T) {
	t.Parallel()

	t.Run("deterministic", func(t *testing.T) {
		t.Parallel()
		input := []byte(`{"$type":"app.bsky.feed.post","text":"hello"}`)

		cid1, err := computeCID(input)
		require.NoError(t, err)

		cid2, err := computeCID(input)
		require.NoError(t, err)

		require.Equal(t, cid1, cid2)
	})

	t.Run("different inputs produce different CIDs", func(t *testing.T) {
		t.Parallel()
		cid1, err := computeCID([]byte(`{"text":"hello"}`))
		require.NoError(t, err)

		cid2, err := computeCID([]byte(`{"text":"world"}`))
		require.NoError(t, err)

		require.NotEqual(t, cid1, cid2)
	})

	t.Run("produces CIDv1 with dag-cbor codec", func(t *testing.T) {
		t.Parallel()
		c, err := computeCID([]byte(`{"test":"data"}`))
		require.NoError(t, err)

		require.Equal(t, uint64(1), c.Version())
		require.Equal(t, uint64(0x71), c.Type()) // dag-cbor
		require.True(t, c.Defined())
		require.NotEmpty(t, c.String())
		require.True(t, len(c.String()) > 10) // CIDs are reasonably long
	})
}

func TestHandleListRepos(t *testing.T) {
	t.Parallel()
	srv := testServer(t)
	router := srv.router()
	ctx := t.Context()

	// create test actors with a unique prefix to avoid conflicts
	prefix := "did:plc:zzzzztestrepos"
	for i := 1; i <= 5; i++ {
		actor := &types.Actor{
			Did:           fmt.Sprintf("%s%03d", prefix, i),
			Email:         fmt.Sprintf("testrepos%d@example.com", i),
			Handle:        fmt.Sprintf("testrepos%d.dev.atlaspds.net", i),
			PdsHost:       testPDSHost,
			CreatedAt:     timestamppb.New(time.Now()),
			PasswordHash:  fmt.Appendf(nil, "hash%d", i),
			SigningKey:    fmt.Appendf(nil, "key%d", i),
			RotationKeys:  [][]byte{fmt.Appendf(nil, "rotation%d", i)},
			RefreshTokens: []*types.RefreshToken{},
			Active:        true,
		}
		err := srv.db.SaveActor(ctx, actor)
		require.NoError(t, err)
	}

	t.Run("success - returns repos with valid structure", func(t *testing.T) {
		t.Parallel()
		w := httptest.NewRecorder()
		// query starting from our test prefix
		req := httptest.NewRequest(http.MethodGet, fmt.Sprintf("/xrpc/com.atproto.sync.listRepos?cursor=%s000&limit=3", prefix), nil)
		req = addTestHostContext(srv, req)
		router.ServeHTTP(w, req)

		require.Equal(t, http.StatusOK, w.Code)
		require.Equal(t, "application/json", w.Header().Get("Content-Type"))

		var out atproto.SyncListRepos_Output
		err := json.Unmarshal(w.Body.Bytes(), &out)
		require.NoError(t, err)

		// verify we got repos
		require.NotNil(t, out.Repos)
		require.GreaterOrEqual(t, len(out.Repos), 3, "should have at least our 3 test repos")

		// verify our test repos are in the response
		foundOurRepos := 0
		for _, repo := range out.Repos {
			if len(repo.Did) >= len(prefix) && repo.Did[:len(prefix)] == prefix {
				foundOurRepos++
				// verify repo has DID
				require.NotEmpty(t, repo.Did)
				// verify active field is set
				require.NotNil(t, repo.Active)
				require.True(t, *repo.Active)
			}
		}
		require.GreaterOrEqual(t, foundOurRepos, 3, "should find our test repos")

		// verify cursor is set (since we have more than 3 total actors)
		require.NotNil(t, out.Cursor)
	})

	t.Run("success - respects limit parameter", func(t *testing.T) {
		t.Parallel()
		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, fmt.Sprintf("/xrpc/com.atproto.sync.listRepos?cursor=%s000&limit=2", prefix), nil)
		req = addTestHostContext(srv, req)
		router.ServeHTTP(w, req)

		require.Equal(t, http.StatusOK, w.Code)

		var out atproto.SyncListRepos_Output
		err := json.Unmarshal(w.Body.Bytes(), &out)
		require.NoError(t, err)

		require.NotNil(t, out.Repos)
		require.LessOrEqual(t, len(out.Repos), 2, "should respect limit of 2")
	})

	t.Run("success - cursor points to next page", func(t *testing.T) {
		t.Parallel()

		// first request with limit 2
		w1 := httptest.NewRecorder()
		req1 := httptest.NewRequest(http.MethodGet, fmt.Sprintf("/xrpc/com.atproto.sync.listRepos?cursor=%s000&limit=2", prefix), nil)
		req1 = addTestHostContext(srv, req1)
		router.ServeHTTP(w1, req1)

		require.Equal(t, http.StatusOK, w1.Code)

		var out1 atproto.SyncListRepos_Output
		err := json.Unmarshal(w1.Body.Bytes(), &out1)
		require.NoError(t, err)
		require.NotNil(t, out1.Cursor)
		require.NotEmpty(t, *out1.Cursor, "should have a cursor for next page")

		// second request using the cursor from first request
		w2 := httptest.NewRecorder()
		req2 := httptest.NewRequest(http.MethodGet, fmt.Sprintf("/xrpc/com.atproto.sync.listRepos?cursor=%s&limit=2", *out1.Cursor), nil)
		req2 = addTestHostContext(srv, req2)
		router.ServeHTTP(w2, req2)

		require.Equal(t, http.StatusOK, w2.Code)

		var out2 atproto.SyncListRepos_Output
		err = json.Unmarshal(w2.Body.Bytes(), &out2)
		require.NoError(t, err)

		// verify second page has different repos
		if len(out1.Repos) > 0 && len(out2.Repos) > 0 {
			require.NotEqual(t, out1.Repos[0].Did, out2.Repos[0].Did, "second page should have different repos")
		}
	})

	t.Run("success - caps limit at 500", func(t *testing.T) {
		t.Parallel()
		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/xrpc/com.atproto.sync.listRepos?limit=501", nil)
		req = addTestHostContext(srv, req)
		router.ServeHTTP(w, req)

		require.Equal(t, http.StatusOK, w.Code)

		var out atproto.SyncListRepos_Output
		err := json.Unmarshal(w.Body.Bytes(), &out)
		require.NoError(t, err)
		require.NotNil(t, out.Repos)
		// limit is capped at 500, so we should get at most 500 repos
		require.LessOrEqual(t, len(out.Repos), 500)
	})

	t.Run("error - invalid limit (negative)", func(t *testing.T) {
		t.Parallel()
		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/xrpc/com.atproto.sync.listRepos?limit=-1", nil)
		req = addTestHostContext(srv, req)
		router.ServeHTTP(w, req)

		require.Equal(t, http.StatusBadRequest, w.Code)
		require.Equal(t, "application/json", w.Header().Get("Content-Type"))
	})

	t.Run("error - invalid limit (non-numeric)", func(t *testing.T) {
		t.Parallel()
		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/xrpc/com.atproto.sync.listRepos?limit=abc", nil)
		req = addTestHostContext(srv, req)
		router.ServeHTTP(w, req)

		require.Equal(t, http.StatusBadRequest, w.Code)
		require.Equal(t, "application/json", w.Header().Get("Content-Type"))
	})

	t.Run("error - invalid cursor (not a DID)", func(t *testing.T) {
		t.Parallel()
		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/xrpc/com.atproto.sync.listRepos?cursor=not-a-did", nil)
		req = addTestHostContext(srv, req)
		router.ServeHTTP(w, req)

		require.Equal(t, http.StatusBadRequest, w.Code)
		require.Equal(t, "application/json", w.Header().Get("Content-Type"))
	})
}

func TestHandleCreateRecord(t *testing.T) {
	t.Parallel()
	srv := testServer(t)

	ctx := context.WithValue(t.Context(), hostContextKey{}, srv.hosts[testPDSHost])

	t.Run("success - creates record with generated rkey", func(t *testing.T) {
		t.Parallel()

		actor, session := setupTestActor(t, srv, "did:plc:createrecord1", "create1@example.com", "create1.dev.atlaspds.dev")

		input := map[string]any{
			"repo":       actor.Did,
			"collection": "app.bsky.feed.post",
			"record": map[string]any{
				"$type":     "app.bsky.feed.post",
				"text":      "Hello, world!",
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

		var out atproto.RepoCreateRecord_Output
		err = json.Unmarshal(w.Body.Bytes(), &out)
		require.NoError(t, err)

		require.NotEmpty(t, out.Uri)
		require.NotEmpty(t, out.Cid)
		require.Contains(t, out.Uri, actor.Did)
		require.Contains(t, out.Uri, "app.bsky.feed.post")
	})

	t.Run("success - creates record with specified rkey", func(t *testing.T) {
		t.Parallel()

		actor, session := setupTestActor(t, srv, "did:plc:createrecord2", "create2@example.com", "create2.dev.atlaspds.dev")

		customTID, err := srv.db.NextTID(ctx, actor.Did)
		require.NoError(t, err)
		customRkey := customTID.String() // use unique rkey to avoid collisions with previous test runs
		input := map[string]any{
			"repo":       actor.Did,
			"collection": "app.bsky.feed.post",
			"rkey":       customRkey,
			"record": map[string]any{
				"$type":     "app.bsky.feed.post",
				"text":      "Custom rkey post",
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

		var out atproto.RepoCreateRecord_Output
		err = json.Unmarshal(w.Body.Bytes(), &out)
		require.NoError(t, err)

		require.Contains(t, out.Uri, customRkey)
	})

	t.Run("success - record can be retrieved after creation", func(t *testing.T) {
		t.Parallel()

		actor, session := setupTestActor(t, srv, "did:plc:createrecord3", "create3@example.com", "create3.dev.atlaspds.dev")

		tid, err := srv.db.NextTID(ctx, actor.Did)
		require.NoError(t, err)
		rkey := tid.String() // use unique rkey to avoid collisions with previous test runs
		input := map[string]any{
			"repo":       actor.Did,
			"collection": "app.bsky.feed.like",
			"rkey":       rkey,
			"record": map[string]any{
				"$type":   "app.bsky.feed.like",
				"subject": map[string]any{"uri": "at://did:plc:other/app.bsky.feed.post/abc", "cid": "bafyrei..."},
			},
		}

		body, err := json.Marshal(input)
		require.NoError(t, err)

		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/xrpc/com.atproto.repo.createRecord", bytes.NewReader(body))
		req = addAuthContext(t, ctx, srv, req, actor, session.AccessToken)
		srv.handleCreateRecord(w, req)

		require.Equal(t, http.StatusOK, w.Code)

		// verify record was saved to DB
		uri := fmt.Sprintf("at://%s/%s/%s", actor.Did, "app.bsky.feed.like", rkey)
		record, err := srv.db.GetRecord(ctx, uri)
		require.NoError(t, err)
		require.NotNil(t, record)
		require.Equal(t, actor.Did, record.Did)
		require.Equal(t, "app.bsky.feed.like", record.Collection)
		require.Equal(t, rkey, record.Rkey)
	})

	t.Run("error - repo mismatch", func(t *testing.T) {
		t.Parallel()

		actor, session := setupTestActor(t, srv, "did:plc:createrecord4", "create4@example.com", "create4.dev.atlaspds.dev")

		input := map[string]any{
			"repo":       "did:plc:someoneelse",
			"collection": "app.bsky.feed.post",
			"record": map[string]any{
				"$type": "app.bsky.feed.post",
				"text":  "Trying to post as someone else",
			},
		}

		body, err := json.Marshal(input)
		require.NoError(t, err)

		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/xrpc/com.atproto.repo.createRecord", bytes.NewReader(body))
		req = addAuthContext(t, ctx, srv, req, actor, session.AccessToken)
		srv.handleCreateRecord(w, req)

		require.Equal(t, http.StatusForbidden, w.Code)
	})

	t.Run("error - invalid collection NSID", func(t *testing.T) {
		t.Parallel()

		actor, session := setupTestActor(t, srv, "did:plc:createrecord5", "create5@example.com", "create5.dev.atlaspds.dev")

		input := map[string]any{
			"repo":       actor.Did,
			"collection": "not-a-valid-nsid",
			"record": map[string]any{
				"$type": "not-a-valid-nsid",
				"text":  "Invalid collection",
			},
		}

		body, err := json.Marshal(input)
		require.NoError(t, err)

		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/xrpc/com.atproto.repo.createRecord", bytes.NewReader(body))
		req = addAuthContext(t, ctx, srv, req, actor, session.AccessToken)
		srv.handleCreateRecord(w, req)

		require.Equal(t, http.StatusBadRequest, w.Code)
	})

	t.Run("error - invalid rkey", func(t *testing.T) {
		t.Parallel()

		actor, session := setupTestActor(t, srv, "did:plc:createrecord6", "create6@example.com", "create6.dev.atlaspds.dev")

		input := map[string]any{
			"repo":       actor.Did,
			"collection": "app.bsky.feed.post",
			"rkey":       "invalid/rkey/with/slashes",
			"record": map[string]any{
				"$type": "app.bsky.feed.post",
				"text":  "Invalid rkey",
			},
		}

		body, err := json.Marshal(input)
		require.NoError(t, err)

		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/xrpc/com.atproto.repo.createRecord", bytes.NewReader(body))
		req = addAuthContext(t, ctx, srv, req, actor, session.AccessToken)
		srv.handleCreateRecord(w, req)

		require.Equal(t, http.StatusBadRequest, w.Code)
	})

	t.Run("error - duplicate record", func(t *testing.T) {
		t.Parallel()

		actor, session := setupTestActor(t, srv, "did:plc:createrecord7", "create7@example.com", "create7.dev.atlaspds.dev")

		tid, err := srv.db.NextTID(ctx, actor.Did)
		require.NoError(t, err)
		rkey := tid.String() // use unique rkey to avoid collisions with previous test runs
		input := map[string]any{
			"repo":       actor.Did,
			"collection": "app.bsky.feed.post",
			"rkey":       rkey,
			"record": map[string]any{
				"$type":     "app.bsky.feed.post",
				"text":      "First post",
				"createdAt": time.Now().Format(time.RFC3339),
			},
		}

		body, err := json.Marshal(input)
		require.NoError(t, err)

		// first request should succeed
		w1 := httptest.NewRecorder()
		req1 := httptest.NewRequest(http.MethodPost, "/xrpc/com.atproto.repo.createRecord", bytes.NewReader(body))
		req1 = addAuthContext(t, ctx, srv, req1, actor, session.AccessToken)
		srv.handleCreateRecord(w1, req1)
		require.Equal(t, http.StatusOK, w1.Code)

		// second request with same rkey should fail
		w2 := httptest.NewRecorder()
		req2 := httptest.NewRequest(http.MethodPost, "/xrpc/com.atproto.repo.createRecord", bytes.NewReader(body))
		req2 = addAuthContext(t, ctx, srv, req2, actor, session.AccessToken)
		srv.handleCreateRecord(w2, req2)
		require.Equal(t, http.StatusConflict, w2.Code)
	})

	t.Run("error - no auth", func(t *testing.T) {
		t.Parallel()

		input := map[string]any{
			"repo":       "did:plc:noauth",
			"collection": "app.bsky.feed.post",
			"record": map[string]any{
				"$type": "app.bsky.feed.post",
				"text":  "No auth",
			},
		}

		body, err := json.Marshal(input)
		require.NoError(t, err)

		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/xrpc/com.atproto.repo.createRecord", bytes.NewReader(body))
		req = addTestHostContext(srv, req)
		srv.handleCreateRecord(w, req)

		require.Equal(t, http.StatusUnauthorized, w.Code)
	})
}

func TestHandleGetRecord(t *testing.T) {
	t.Parallel()
	srv := testServer(t)
	router := srv.router()
	ctx := context.WithValue(t.Context(), hostContextKey{}, srv.hosts[testPDSHost])

	// create a test actor and record
	actor, session := setupTestActor(t, srv, "did:plc:getrecord1", "getrecord1@example.com", "getrecord1.dev.atlaspds.dev")

	// create a record via createRecord endpoint
	tid, err := srv.db.NextTID(ctx, actor.Did)
	require.NoError(t, err)
	rkey := tid.String()

	recordText := "Test record for getRecord"
	input := map[string]any{
		"repo":       actor.Did,
		"collection": "app.bsky.feed.post",
		"rkey":       rkey,
		"record": map[string]any{
			"$type":     "app.bsky.feed.post",
			"text":      recordText,
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

	t.Run("success - retrieves record by DID", func(t *testing.T) {
		t.Parallel()
		w := httptest.NewRecorder()
		url := fmt.Sprintf("/xrpc/com.atproto.repo.getRecord?repo=%s&collection=app.bsky.feed.post&rkey=%s", actor.Did, rkey)
		req := httptest.NewRequest(http.MethodGet, url, nil)
		req = addTestHostContext(srv, req)
		router.ServeHTTP(w, req)

		require.Equal(t, http.StatusOK, w.Code)
		require.Equal(t, "application/json", w.Header().Get("Content-Type"))

		var out struct {
			Uri   string         `json:"uri"`
			Cid   string         `json:"cid"`
			Value map[string]any `json:"value"`
		}
		err := json.Unmarshal(w.Body.Bytes(), &out)
		require.NoError(t, err)

		require.Equal(t, fmt.Sprintf("at://%s/app.bsky.feed.post/%s", actor.Did, rkey), out.Uri)
		require.Equal(t, recordCID, out.Cid)
		require.Equal(t, "app.bsky.feed.post", out.Value["$type"])
		require.Equal(t, recordText, out.Value["text"])
	})

	t.Run("success - retrieves record with cid parameter", func(t *testing.T) {
		t.Parallel()
		w := httptest.NewRecorder()
		url := fmt.Sprintf("/xrpc/com.atproto.repo.getRecord?repo=%s&collection=app.bsky.feed.post&rkey=%s&cid=%s", actor.Did, rkey, recordCID)
		req := httptest.NewRequest(http.MethodGet, url, nil)
		req = addTestHostContext(srv, req)
		router.ServeHTTP(w, req)

		require.Equal(t, http.StatusOK, w.Code)

		var out struct {
			Uri   string         `json:"uri"`
			Cid   string         `json:"cid"`
			Value map[string]any `json:"value"`
		}
		err := json.Unmarshal(w.Body.Bytes(), &out)
		require.NoError(t, err)

		require.Equal(t, recordCID, out.Cid)
	})

	t.Run("error - missing repo parameter", func(t *testing.T) {
		t.Parallel()
		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/xrpc/com.atproto.repo.getRecord?collection=app.bsky.feed.post&rkey=abc", nil)
		req = addTestHostContext(srv, req)
		router.ServeHTTP(w, req)

		require.Equal(t, http.StatusBadRequest, w.Code)
	})

	t.Run("error - missing collection parameter", func(t *testing.T) {
		t.Parallel()
		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/xrpc/com.atproto.repo.getRecord?repo=did:plc:test&rkey=abc", nil)
		req = addTestHostContext(srv, req)
		router.ServeHTTP(w, req)

		require.Equal(t, http.StatusBadRequest, w.Code)
	})

	t.Run("error - missing rkey parameter", func(t *testing.T) {
		t.Parallel()
		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/xrpc/com.atproto.repo.getRecord?repo=did:plc:test&collection=app.bsky.feed.post", nil)
		req = addTestHostContext(srv, req)
		router.ServeHTTP(w, req)

		require.Equal(t, http.StatusBadRequest, w.Code)
	})

	t.Run("error - invalid collection NSID", func(t *testing.T) {
		t.Parallel()
		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/xrpc/com.atproto.repo.getRecord?repo=did:plc:test&collection=not-valid&rkey=abc", nil)
		req = addTestHostContext(srv, req)
		router.ServeHTTP(w, req)

		require.Equal(t, http.StatusBadRequest, w.Code)
	})

	t.Run("error - invalid rkey", func(t *testing.T) {
		t.Parallel()
		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/xrpc/com.atproto.repo.getRecord?repo=did:plc:test&collection=app.bsky.feed.post&rkey=invalid/rkey", nil)
		req = addTestHostContext(srv, req)
		router.ServeHTTP(w, req)

		require.Equal(t, http.StatusBadRequest, w.Code)
	})

	t.Run("error - record not found", func(t *testing.T) {
		t.Parallel()
		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/xrpc/com.atproto.repo.getRecord?repo=did:plc:test&collection=app.bsky.feed.post&rkey=3jui7kd2xxxx2", nil)
		req = addTestHostContext(srv, req)
		router.ServeHTTP(w, req)

		require.Equal(t, http.StatusNotFound, w.Code)
	})

	t.Run("error - wrong cid parameter", func(t *testing.T) {
		t.Parallel()
		w := httptest.NewRecorder()
		// use a valid CID format but different from the actual record's CID
		wrongCID := "bafyreihx6qqvghcmvpqq33kg4s7ztnh6mlt5cqpynjjxgcoynvndx5cuee"
		url := fmt.Sprintf("/xrpc/com.atproto.repo.getRecord?repo=%s&collection=app.bsky.feed.post&rkey=%s&cid=%s", actor.Did, rkey, wrongCID)
		req := httptest.NewRequest(http.MethodGet, url, nil)
		req = addTestHostContext(srv, req)
		router.ServeHTTP(w, req)

		require.Equal(t, http.StatusNotFound, w.Code)
	})

	t.Run("error - invalid cid parameter", func(t *testing.T) {
		t.Parallel()
		w := httptest.NewRecorder()
		url := fmt.Sprintf("/xrpc/com.atproto.repo.getRecord?repo=%s&collection=app.bsky.feed.post&rkey=%s&cid=not-a-valid-cid", actor.Did, rkey)
		req := httptest.NewRequest(http.MethodGet, url, nil)
		req = addTestHostContext(srv, req)
		router.ServeHTTP(w, req)

		require.Equal(t, http.StatusBadRequest, w.Code)
	})
}
