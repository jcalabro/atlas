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
	"github.com/bluesky-social/indigo/atproto/identity"
	"github.com/bluesky-social/indigo/atproto/syntax"
	"github.com/jcalabro/atlas/internal/pds/db"
	"github.com/jcalabro/atlas/internal/types"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestHandleDescribeServer(t *testing.T) {
	t.Parallel()

	srv := testServer(t)
	w := httptest.NewRecorder()
	router := srv.router()

	req := httptest.NewRequest(http.MethodGet, "/xrpc/com.atproto.server.describeServer", nil)
	req = addTestHostContext(srv, req)
	router.ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	require.Equal(t, "application/json", w.Header().Get("Content-Type"))

	var resp atproto.ServerDescribeServer_Output
	err := json.NewDecoder(w.Body).Decode(&resp)
	require.NoError(t, err)

	// verify expected values from test server config
	require.Equal(t, "did:web:dev.atlaspds.dev", resp.Did)
	require.Equal(t, []string{".dev.atlaspds.dev"}, resp.AvailableUserDomains)
	require.NotNil(t, resp.Contact)
	require.NotNil(t, resp.Contact.Email)
	require.Equal(t, "webmaster@dev.atlaspds.dev", *resp.Contact.Email)
	require.NotNil(t, resp.Links)
	require.NotNil(t, resp.Links.PrivacyPolicy)
	require.Equal(t, "https://dev.atlaspds.dev/privacy", *resp.Links.PrivacyPolicy)
	require.NotNil(t, resp.Links.TermsOfService)
	require.Equal(t, "https://dev.atlaspds.dev/tos", *resp.Links.TermsOfService)
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
		router := srv.hostMiddleware(srv.router())

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
		req.Host = testPDSHost
		router.ServeHTTP(w, req)

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

func TestHandleDeleteRecord(t *testing.T) {
	t.Parallel()
	srv := testServer(t)
	ctx := context.WithValue(t.Context(), hostContextKey{}, srv.hosts[testPDSHost])

	t.Run("success - deletes record", func(t *testing.T) {
		t.Parallel()

		actor, session := setupTestActor(t, srv, "did:plc:deleterecord1", "deleterecord1@example.com", "deleterecord1.dev.atlaspds.dev")

		// create a record first
		tid, err := srv.db.NextTID(ctx, actor.Did)
		require.NoError(t, err)
		rkey := tid.String()

		createInput := map[string]any{
			"repo":       actor.Did,
			"collection": "app.bsky.feed.post",
			"rkey":       rkey,
			"record": map[string]any{
				"$type":     "app.bsky.feed.post",
				"text":      "Post to be deleted",
				"createdAt": time.Now().Format(time.RFC3339),
			},
		}

		createBody, err := json.Marshal(createInput)
		require.NoError(t, err)

		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/xrpc/com.atproto.repo.createRecord", bytes.NewReader(createBody))
		req = addAuthContext(t, ctx, srv, req, actor, session.AccessToken)
		srv.handleCreateRecord(w, req)
		require.Equal(t, http.StatusOK, w.Code)

		// verify record exists
		uri := fmt.Sprintf("at://%s/app.bsky.feed.post/%s", actor.Did, rkey)
		record, err := srv.db.GetRecord(ctx, uri)
		require.NoError(t, err)
		require.NotNil(t, record)

		// delete the record
		deleteInput := map[string]any{
			"repo":       actor.Did,
			"collection": "app.bsky.feed.post",
			"rkey":       rkey,
		}

		deleteBody, err := json.Marshal(deleteInput)
		require.NoError(t, err)

		w = httptest.NewRecorder()
		req = httptest.NewRequest(http.MethodPost, "/xrpc/com.atproto.repo.deleteRecord", bytes.NewReader(deleteBody))
		req = addAuthContext(t, ctx, srv, req, actor, session.AccessToken)
		srv.handleDeleteRecord(w, req)
		require.Equal(t, http.StatusOK, w.Code)

		// verify record is deleted
		_, err = srv.db.GetRecord(ctx, uri)
		require.ErrorIs(t, err, db.ErrNotFound)
	})

	t.Run("success - deletes record with swapRecord", func(t *testing.T) {
		t.Parallel()

		actor, session := setupTestActor(t, srv, "did:plc:deleterecord2", "deleterecord2@example.com", "deleterecord2.dev.atlaspds.dev")

		// create a record first
		tid, err := srv.db.NextTID(ctx, actor.Did)
		require.NoError(t, err)
		rkey := tid.String()

		createInput := map[string]any{
			"repo":       actor.Did,
			"collection": "app.bsky.feed.post",
			"rkey":       rkey,
			"record": map[string]any{
				"$type":     "app.bsky.feed.post",
				"text":      "Post to be deleted with swap",
				"createdAt": time.Now().Format(time.RFC3339),
			},
		}

		createBody, err := json.Marshal(createInput)
		require.NoError(t, err)

		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/xrpc/com.atproto.repo.createRecord", bytes.NewReader(createBody))
		req = addAuthContext(t, ctx, srv, req, actor, session.AccessToken)
		srv.handleCreateRecord(w, req)
		require.Equal(t, http.StatusOK, w.Code)

		var createOut atproto.RepoCreateRecord_Output
		err = json.Unmarshal(w.Body.Bytes(), &createOut)
		require.NoError(t, err)

		// delete the record with swapRecord
		deleteInput := map[string]any{
			"repo":       actor.Did,
			"collection": "app.bsky.feed.post",
			"rkey":       rkey,
			"swapRecord": createOut.Cid,
		}

		deleteBody, err := json.Marshal(deleteInput)
		require.NoError(t, err)

		w = httptest.NewRecorder()
		req = httptest.NewRequest(http.MethodPost, "/xrpc/com.atproto.repo.deleteRecord", bytes.NewReader(deleteBody))
		req = addAuthContext(t, ctx, srv, req, actor, session.AccessToken)
		srv.handleDeleteRecord(w, req)
		require.Equal(t, http.StatusOK, w.Code)

		// verify record is deleted
		uri := fmt.Sprintf("at://%s/app.bsky.feed.post/%s", actor.Did, rkey)
		_, err = srv.db.GetRecord(ctx, uri)
		require.ErrorIs(t, err, db.ErrNotFound)
	})

	t.Run("error - repo mismatch", func(t *testing.T) {
		t.Parallel()

		actor, session := setupTestActor(t, srv, "did:plc:deleterecord3", "deleterecord3@example.com", "deleterecord3.dev.atlaspds.dev")

		deleteInput := map[string]any{
			"repo":       "did:plc:someoneelse",
			"collection": "app.bsky.feed.post",
			"rkey":       "abc123",
		}

		deleteBody, err := json.Marshal(deleteInput)
		require.NoError(t, err)

		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/xrpc/com.atproto.repo.deleteRecord", bytes.NewReader(deleteBody))
		req = addAuthContext(t, ctx, srv, req, actor, session.AccessToken)
		srv.handleDeleteRecord(w, req)

		require.Equal(t, http.StatusForbidden, w.Code)
	})

	t.Run("error - record not found", func(t *testing.T) {
		t.Parallel()

		actor, session := setupTestActor(t, srv, "did:plc:deleterecord4", "deleterecord4@example.com", "deleterecord4.dev.atlaspds.dev")

		deleteInput := map[string]any{
			"repo":       actor.Did,
			"collection": "app.bsky.feed.post",
			"rkey":       "3jui7kd2xxxx2",
		}

		deleteBody, err := json.Marshal(deleteInput)
		require.NoError(t, err)

		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/xrpc/com.atproto.repo.deleteRecord", bytes.NewReader(deleteBody))
		req = addAuthContext(t, ctx, srv, req, actor, session.AccessToken)
		srv.handleDeleteRecord(w, req)

		require.Equal(t, http.StatusNotFound, w.Code)
	})

	t.Run("error - swapRecord mismatch", func(t *testing.T) {
		t.Parallel()

		actor, session := setupTestActor(t, srv, "did:plc:deleterecord5", "deleterecord5@example.com", "deleterecord5.dev.atlaspds.dev")

		// create a record first
		tid, err := srv.db.NextTID(ctx, actor.Did)
		require.NoError(t, err)
		rkey := tid.String()

		createInput := map[string]any{
			"repo":       actor.Did,
			"collection": "app.bsky.feed.post",
			"rkey":       rkey,
			"record": map[string]any{
				"$type":     "app.bsky.feed.post",
				"text":      "Post with swap mismatch",
				"createdAt": time.Now().Format(time.RFC3339),
			},
		}

		createBody, err := json.Marshal(createInput)
		require.NoError(t, err)

		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/xrpc/com.atproto.repo.createRecord", bytes.NewReader(createBody))
		req = addAuthContext(t, ctx, srv, req, actor, session.AccessToken)
		srv.handleCreateRecord(w, req)
		require.Equal(t, http.StatusOK, w.Code)

		// try to delete with wrong swapRecord
		deleteInput := map[string]any{
			"repo":       actor.Did,
			"collection": "app.bsky.feed.post",
			"rkey":       rkey,
			"swapRecord": "bafyreihx6qqvghcmvpqq33kg4s7ztnh6mlt5cqpynjjxgcoynvndx5cuee",
		}

		deleteBody, err := json.Marshal(deleteInput)
		require.NoError(t, err)

		w = httptest.NewRecorder()
		req = httptest.NewRequest(http.MethodPost, "/xrpc/com.atproto.repo.deleteRecord", bytes.NewReader(deleteBody))
		req = addAuthContext(t, ctx, srv, req, actor, session.AccessToken)
		srv.handleDeleteRecord(w, req)

		require.Equal(t, http.StatusConflict, w.Code)
	})

	t.Run("error - invalid collection NSID", func(t *testing.T) {
		t.Parallel()

		actor, session := setupTestActor(t, srv, "did:plc:deleterecord6", "deleterecord6@example.com", "deleterecord6.dev.atlaspds.dev")

		deleteInput := map[string]any{
			"repo":       actor.Did,
			"collection": "not-valid",
			"rkey":       "abc123",
		}

		deleteBody, err := json.Marshal(deleteInput)
		require.NoError(t, err)

		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/xrpc/com.atproto.repo.deleteRecord", bytes.NewReader(deleteBody))
		req = addAuthContext(t, ctx, srv, req, actor, session.AccessToken)
		srv.handleDeleteRecord(w, req)

		require.Equal(t, http.StatusBadRequest, w.Code)
	})

	t.Run("error - invalid rkey", func(t *testing.T) {
		t.Parallel()

		actor, session := setupTestActor(t, srv, "did:plc:deleterecord7", "deleterecord7@example.com", "deleterecord7.dev.atlaspds.dev")

		deleteInput := map[string]any{
			"repo":       actor.Did,
			"collection": "app.bsky.feed.post",
			"rkey":       "invalid/rkey/slashes",
		}

		deleteBody, err := json.Marshal(deleteInput)
		require.NoError(t, err)

		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/xrpc/com.atproto.repo.deleteRecord", bytes.NewReader(deleteBody))
		req = addAuthContext(t, ctx, srv, req, actor, session.AccessToken)
		srv.handleDeleteRecord(w, req)

		require.Equal(t, http.StatusBadRequest, w.Code)
	})

	t.Run("error - missing required fields", func(t *testing.T) {
		t.Parallel()

		actor, session := setupTestActor(t, srv, "did:plc:deleterecord8", "deleterecord8@example.com", "deleterecord8.dev.atlaspds.dev")

		testCases := []struct {
			name         string
			input        map[string]any
			expectedCode int
		}{
			// missing repo returns 403 because the ownership check happens first
			{"missing repo", map[string]any{"collection": "app.bsky.feed.post", "rkey": "abc"}, http.StatusForbidden},
			{"missing collection", map[string]any{"repo": actor.Did, "rkey": "abc"}, http.StatusBadRequest},
			{"missing rkey", map[string]any{"repo": actor.Did, "collection": "app.bsky.feed.post"}, http.StatusBadRequest},
		}

		for _, tc := range testCases {
			body, err := json.Marshal(tc.input)
			require.NoError(t, err)

			w := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodPost, "/xrpc/com.atproto.repo.deleteRecord", bytes.NewReader(body))
			req = addAuthContext(t, ctx, srv, req, actor, session.AccessToken)
			srv.handleDeleteRecord(w, req)

			require.Equal(t, tc.expectedCode, w.Code, "test case: %s", tc.name)
		}
	})

	t.Run("error - no auth", func(t *testing.T) {
		t.Parallel()
		router := srv.hostMiddleware(srv.router())

		deleteInput := map[string]any{
			"repo":       "did:plc:noauth",
			"collection": "app.bsky.feed.post",
			"rkey":       "abc123",
		}

		deleteBody, err := json.Marshal(deleteInput)
		require.NoError(t, err)

		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/xrpc/com.atproto.repo.deleteRecord", bytes.NewReader(deleteBody))
		req.Host = testPDSHost
		router.ServeHTTP(w, req)

		require.Equal(t, http.StatusUnauthorized, w.Code)
	})
}

func TestHandlePutRecord(t *testing.T) {
	t.Parallel()
	srv := testServer(t)
	ctx := context.WithValue(t.Context(), hostContextKey{}, srv.hosts[testPDSHost])

	t.Run("success - creates new record", func(t *testing.T) {
		t.Parallel()

		actor, session := setupTestActor(t, srv, "did:plc:putrecord1", "putrecord1@example.com", "putrecord1.dev.atlaspds.dev")

		tid, err := srv.db.NextTID(ctx, actor.Did)
		require.NoError(t, err)
		rkey := tid.String()

		input := map[string]any{
			"repo":       actor.Did,
			"collection": "app.bsky.feed.post",
			"rkey":       rkey,
			"record": map[string]any{
				"$type":     "app.bsky.feed.post",
				"text":      "New record via putRecord",
				"createdAt": time.Now().Format(time.RFC3339),
			},
		}

		body, err := json.Marshal(input)
		require.NoError(t, err)

		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/xrpc/com.atproto.repo.putRecord", bytes.NewReader(body))
		req = addAuthContext(t, ctx, srv, req, actor, session.AccessToken)
		srv.handlePutRecord(w, req)

		require.Equal(t, http.StatusOK, w.Code)

		var out atproto.RepoPutRecord_Output
		err = json.Unmarshal(w.Body.Bytes(), &out)
		require.NoError(t, err)

		require.NotEmpty(t, out.Uri)
		require.NotEmpty(t, out.Cid)
		require.Contains(t, out.Uri, actor.Did)
		require.Contains(t, out.Uri, rkey)
	})

	t.Run("success - updates existing record", func(t *testing.T) {
		t.Parallel()

		actor, session := setupTestActor(t, srv, "did:plc:putrecord2", "putrecord2@example.com", "putrecord2.dev.atlaspds.dev")

		tid, err := srv.db.NextTID(ctx, actor.Did)
		require.NoError(t, err)
		rkey := tid.String()

		// create initial record
		createInput := map[string]any{
			"repo":       actor.Did,
			"collection": "app.bsky.feed.post",
			"rkey":       rkey,
			"record": map[string]any{
				"$type":     "app.bsky.feed.post",
				"text":      "Original text",
				"createdAt": time.Now().Format(time.RFC3339),
			},
		}

		createBody, err := json.Marshal(createInput)
		require.NoError(t, err)

		w1 := httptest.NewRecorder()
		req1 := httptest.NewRequest(http.MethodPost, "/xrpc/com.atproto.repo.putRecord", bytes.NewReader(createBody))
		req1 = addAuthContext(t, ctx, srv, req1, actor, session.AccessToken)
		srv.handlePutRecord(w1, req1)
		require.Equal(t, http.StatusOK, w1.Code)

		var createOut atproto.RepoPutRecord_Output
		err = json.Unmarshal(w1.Body.Bytes(), &createOut)
		require.NoError(t, err)
		originalCid := createOut.Cid

		// reload actor to get updated head
		actor, err = srv.db.GetActorByDID(ctx, actor.Did)
		require.NoError(t, err)

		// update record with putRecord
		updateInput := map[string]any{
			"repo":       actor.Did,
			"collection": "app.bsky.feed.post",
			"rkey":       rkey,
			"record": map[string]any{
				"$type":     "app.bsky.feed.post",
				"text":      "Updated text",
				"createdAt": time.Now().Format(time.RFC3339),
			},
		}

		updateBody, err := json.Marshal(updateInput)
		require.NoError(t, err)

		w2 := httptest.NewRecorder()
		req2 := httptest.NewRequest(http.MethodPost, "/xrpc/com.atproto.repo.putRecord", bytes.NewReader(updateBody))
		req2 = addAuthContext(t, ctx, srv, req2, actor, session.AccessToken)
		srv.handlePutRecord(w2, req2)
		require.Equal(t, http.StatusOK, w2.Code)

		var updateOut atproto.RepoPutRecord_Output
		err = json.Unmarshal(w2.Body.Bytes(), &updateOut)
		require.NoError(t, err)

		// CID should change since content changed
		require.NotEqual(t, originalCid, updateOut.Cid)
		require.Contains(t, updateOut.Uri, rkey)

		// verify the record was updated in DB
		uri := fmt.Sprintf("at://%s/app.bsky.feed.post/%s", actor.Did, rkey)
		record, err := srv.db.GetRecord(ctx, uri)
		require.NoError(t, err)
		require.Equal(t, updateOut.Cid, record.Cid)
	})

	t.Run("success - updates with swapRecord validation", func(t *testing.T) {
		t.Parallel()

		actor, session := setupTestActor(t, srv, "did:plc:putrecord3", "putrecord3@example.com", "putrecord3.dev.atlaspds.dev")

		tid, err := srv.db.NextTID(ctx, actor.Did)
		require.NoError(t, err)
		rkey := tid.String()

		// create initial record
		createInput := map[string]any{
			"repo":       actor.Did,
			"collection": "app.bsky.feed.post",
			"rkey":       rkey,
			"record": map[string]any{
				"$type":     "app.bsky.feed.post",
				"text":      "Original for swapRecord test",
				"createdAt": time.Now().Format(time.RFC3339),
			},
		}

		createBody, err := json.Marshal(createInput)
		require.NoError(t, err)

		w1 := httptest.NewRecorder()
		req1 := httptest.NewRequest(http.MethodPost, "/xrpc/com.atproto.repo.putRecord", bytes.NewReader(createBody))
		req1 = addAuthContext(t, ctx, srv, req1, actor, session.AccessToken)
		srv.handlePutRecord(w1, req1)
		require.Equal(t, http.StatusOK, w1.Code)

		var createOut atproto.RepoPutRecord_Output
		err = json.Unmarshal(w1.Body.Bytes(), &createOut)
		require.NoError(t, err)

		// reload actor
		actor, err = srv.db.GetActorByDID(ctx, actor.Did)
		require.NoError(t, err)

		// update with correct swapRecord
		updateInput := map[string]any{
			"repo":       actor.Did,
			"collection": "app.bsky.feed.post",
			"rkey":       rkey,
			"swapRecord": createOut.Cid,
			"record": map[string]any{
				"$type":     "app.bsky.feed.post",
				"text":      "Updated with swapRecord",
				"createdAt": time.Now().Format(time.RFC3339),
			},
		}

		updateBody, err := json.Marshal(updateInput)
		require.NoError(t, err)

		w2 := httptest.NewRecorder()
		req2 := httptest.NewRequest(http.MethodPost, "/xrpc/com.atproto.repo.putRecord", bytes.NewReader(updateBody))
		req2 = addAuthContext(t, ctx, srv, req2, actor, session.AccessToken)
		srv.handlePutRecord(w2, req2)

		require.Equal(t, http.StatusOK, w2.Code)
	})

	t.Run("success - record can be retrieved after put", func(t *testing.T) {
		t.Parallel()

		actor, session := setupTestActor(t, srv, "did:plc:putrecord4", "putrecord4@example.com", "putrecord4.dev.atlaspds.dev")

		tid, err := srv.db.NextTID(ctx, actor.Did)
		require.NoError(t, err)
		rkey := tid.String()

		recordText := "Retrievable record via putRecord"
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
		req := httptest.NewRequest(http.MethodPost, "/xrpc/com.atproto.repo.putRecord", bytes.NewReader(body))
		req = addAuthContext(t, ctx, srv, req, actor, session.AccessToken)
		srv.handlePutRecord(w, req)
		require.Equal(t, http.StatusOK, w.Code)

		// verify record was saved to DB
		uri := fmt.Sprintf("at://%s/%s/%s", actor.Did, "app.bsky.feed.post", rkey)
		record, err := srv.db.GetRecord(ctx, uri)
		require.NoError(t, err)
		require.NotNil(t, record)
		require.Equal(t, actor.Did, record.Did)
		require.Equal(t, "app.bsky.feed.post", record.Collection)
		require.Equal(t, rkey, record.Rkey)
	})

	t.Run("error - repo mismatch", func(t *testing.T) {
		t.Parallel()

		actor, session := setupTestActor(t, srv, "did:plc:putrecord5", "putrecord5@example.com", "putrecord5.dev.atlaspds.dev")

		input := map[string]any{
			"repo":       "did:plc:someoneelse",
			"collection": "app.bsky.feed.post",
			"rkey":       "abc123",
			"record": map[string]any{
				"$type": "app.bsky.feed.post",
				"text":  "Trying to post as someone else",
			},
		}

		body, err := json.Marshal(input)
		require.NoError(t, err)

		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/xrpc/com.atproto.repo.putRecord", bytes.NewReader(body))
		req = addAuthContext(t, ctx, srv, req, actor, session.AccessToken)
		srv.handlePutRecord(w, req)

		require.Equal(t, http.StatusForbidden, w.Code)
	})

	t.Run("error - invalid collection NSID", func(t *testing.T) {
		t.Parallel()

		actor, session := setupTestActor(t, srv, "did:plc:putrecord6", "putrecord6@example.com", "putrecord6.dev.atlaspds.dev")

		input := map[string]any{
			"repo":       actor.Did,
			"collection": "not-a-valid-nsid",
			"rkey":       "abc123",
			"record": map[string]any{
				"$type": "not-a-valid-nsid",
				"text":  "Invalid collection",
			},
		}

		body, err := json.Marshal(input)
		require.NoError(t, err)

		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/xrpc/com.atproto.repo.putRecord", bytes.NewReader(body))
		req = addAuthContext(t, ctx, srv, req, actor, session.AccessToken)
		srv.handlePutRecord(w, req)

		require.Equal(t, http.StatusBadRequest, w.Code)
	})

	t.Run("error - invalid rkey", func(t *testing.T) {
		t.Parallel()

		actor, session := setupTestActor(t, srv, "did:plc:putrecord7", "putrecord7@example.com", "putrecord7.dev.atlaspds.dev")

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
		req := httptest.NewRequest(http.MethodPost, "/xrpc/com.atproto.repo.putRecord", bytes.NewReader(body))
		req = addAuthContext(t, ctx, srv, req, actor, session.AccessToken)
		srv.handlePutRecord(w, req)

		require.Equal(t, http.StatusBadRequest, w.Code)
	})

	t.Run("error - swapRecord mismatch", func(t *testing.T) {
		t.Parallel()

		actor, session := setupTestActor(t, srv, "did:plc:putrecord8", "putrecord8@example.com", "putrecord8.dev.atlaspds.dev")

		// create initial record
		tid, err := srv.db.NextTID(ctx, actor.Did)
		require.NoError(t, err)
		rkey := tid.String()

		createInput := map[string]any{
			"repo":       actor.Did,
			"collection": "app.bsky.feed.post",
			"rkey":       rkey,
			"record": map[string]any{
				"$type":     "app.bsky.feed.post",
				"text":      "Original for swap mismatch test",
				"createdAt": time.Now().Format(time.RFC3339),
			},
		}

		createBody, err := json.Marshal(createInput)
		require.NoError(t, err)

		w1 := httptest.NewRecorder()
		req1 := httptest.NewRequest(http.MethodPost, "/xrpc/com.atproto.repo.putRecord", bytes.NewReader(createBody))
		req1 = addAuthContext(t, ctx, srv, req1, actor, session.AccessToken)
		srv.handlePutRecord(w1, req1)
		require.Equal(t, http.StatusOK, w1.Code)

		// reload actor
		actor, err = srv.db.GetActorByDID(ctx, actor.Did)
		require.NoError(t, err)

		// try to update with wrong swapRecord
		wrongCID := "bafyreihx6qqvghcmvpqq33kg4s7ztnh6mlt5cqpynjjxgcoynvndx5cuee"
		updateInput := map[string]any{
			"repo":       actor.Did,
			"collection": "app.bsky.feed.post",
			"rkey":       rkey,
			"swapRecord": wrongCID,
			"record": map[string]any{
				"$type":     "app.bsky.feed.post",
				"text":      "This should fail",
				"createdAt": time.Now().Format(time.RFC3339),
			},
		}

		updateBody, err := json.Marshal(updateInput)
		require.NoError(t, err)

		w2 := httptest.NewRecorder()
		req2 := httptest.NewRequest(http.MethodPost, "/xrpc/com.atproto.repo.putRecord", bytes.NewReader(updateBody))
		req2 = addAuthContext(t, ctx, srv, req2, actor, session.AccessToken)
		srv.handlePutRecord(w2, req2)

		require.Equal(t, http.StatusConflict, w2.Code)
	})

	t.Run("error - swapRecord provided but record does not exist", func(t *testing.T) {
		t.Parallel()

		actor, session := setupTestActor(t, srv, "did:plc:putrecord9", "putrecord9@example.com", "putrecord9.dev.atlaspds.dev")

		tid, err := srv.db.NextTID(ctx, actor.Did)
		require.NoError(t, err)
		rkey := tid.String()

		// try to put with swapRecord but record doesn't exist
		input := map[string]any{
			"repo":       actor.Did,
			"collection": "app.bsky.feed.post",
			"rkey":       rkey,
			"swapRecord": "bafyreihx6qqvghcmvpqq33kg4s7ztnh6mlt5cqpynjjxgcoynvndx5cuee",
			"record": map[string]any{
				"$type":     "app.bsky.feed.post",
				"text":      "This should fail",
				"createdAt": time.Now().Format(time.RFC3339),
			},
		}

		body, err := json.Marshal(input)
		require.NoError(t, err)

		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/xrpc/com.atproto.repo.putRecord", bytes.NewReader(body))
		req = addAuthContext(t, ctx, srv, req, actor, session.AccessToken)
		srv.handlePutRecord(w, req)

		require.Equal(t, http.StatusInternalServerError, w.Code)
	})

	t.Run("error - missing required fields", func(t *testing.T) {
		t.Parallel()

		actor, session := setupTestActor(t, srv, "did:plc:putrecord10", "putrecord10@example.com", "putrecord10.dev.atlaspds.dev")

		testCases := []struct {
			name         string
			input        map[string]any
			expectedCode int
		}{
			// missing repo returns 403 because the ownership check happens first
			{"missing repo", map[string]any{"collection": "app.bsky.feed.post", "rkey": "abc", "record": map[string]any{"text": "test"}}, http.StatusForbidden},
			{"missing collection", map[string]any{"repo": actor.Did, "rkey": "abc", "record": map[string]any{"text": "test"}}, http.StatusBadRequest},
			{"missing rkey", map[string]any{"repo": actor.Did, "collection": "app.bsky.feed.post", "record": map[string]any{"text": "test"}}, http.StatusBadRequest},
			{"missing record", map[string]any{"repo": actor.Did, "collection": "app.bsky.feed.post", "rkey": "abc"}, http.StatusBadRequest},
		}

		for _, tc := range testCases {
			body, err := json.Marshal(tc.input)
			require.NoError(t, err)

			w := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodPost, "/xrpc/com.atproto.repo.putRecord", bytes.NewReader(body))
			req = addAuthContext(t, ctx, srv, req, actor, session.AccessToken)
			srv.handlePutRecord(w, req)

			require.Equal(t, tc.expectedCode, w.Code, "test case: %s", tc.name)
		}
	})

	t.Run("error - no auth", func(t *testing.T) {
		t.Parallel()
		router := srv.hostMiddleware(srv.router())

		input := map[string]any{
			"repo":       "did:plc:noauth",
			"collection": "app.bsky.feed.post",
			"rkey":       "abc123",
			"record": map[string]any{
				"$type": "app.bsky.feed.post",
				"text":  "No auth",
			},
		}

		body, err := json.Marshal(input)
		require.NoError(t, err)

		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/xrpc/com.atproto.repo.putRecord", bytes.NewReader(body))
		req.Host = testPDSHost
		router.ServeHTTP(w, req)

		require.Equal(t, http.StatusUnauthorized, w.Code)
	})

	t.Run("error - invalid swapRecord CID", func(t *testing.T) {
		t.Parallel()

		actor, session := setupTestActor(t, srv, "did:plc:putrecord11", "putrecord11@example.com", "putrecord11.dev.atlaspds.dev")

		input := map[string]any{
			"repo":       actor.Did,
			"collection": "app.bsky.feed.post",
			"rkey":       "abc123",
			"swapRecord": "not-a-valid-cid",
			"record": map[string]any{
				"$type": "app.bsky.feed.post",
				"text":  "Invalid swapRecord",
			},
		}

		body, err := json.Marshal(input)
		require.NoError(t, err)

		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/xrpc/com.atproto.repo.putRecord", bytes.NewReader(body))
		req = addAuthContext(t, ctx, srv, req, actor, session.AccessToken)
		srv.handlePutRecord(w, req)

		require.Equal(t, http.StatusBadRequest, w.Code)
	})
}

func TestConcurrentModificationDetection(t *testing.T) {
	t.Parallel()
	srv := testServer(t)
	ctx := context.WithValue(t.Context(), hostContextKey{}, srv.hosts[testPDSHost])

	t.Run("detects concurrent modification on createRecord", func(t *testing.T) {
		t.Parallel()

		actor, session := setupTestActor(t, srv, "did:plc:concurrent1", "concurrent1@example.com", "concurrent1.dev.atlaspds.dev")

		// simulate another server modifying the repo by directly changing the actor's Head
		originalHead := actor.Head
		actor.Head = "bafyreihx6qqvghcmvpqq33kg4s7ztnh6mlt5cqpynjjxgcoynvndx5cuee" // different CID
		err := srv.db.SaveActor(ctx, actor)
		require.NoError(t, err)

		// restore the actor's Head in our local copy (simulating stale state)
		actor.Head = originalHead

		// try to create a record with the stale actor state
		tid, err := srv.db.NextTID(ctx, actor.Did)
		require.NoError(t, err)
		input := map[string]any{
			"repo":       actor.Did,
			"collection": "app.bsky.feed.post",
			"rkey":       tid.String(),
			"record": map[string]any{
				"$type":     "app.bsky.feed.post",
				"text":      "This should fail due to concurrent modification",
				"createdAt": time.Now().Format(time.RFC3339),
			},
		}

		body, err := json.Marshal(input)
		require.NoError(t, err)

		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/xrpc/com.atproto.repo.createRecord", bytes.NewReader(body))
		req = addAuthContext(t, ctx, srv, req, actor, session.AccessToken)
		srv.handleCreateRecord(w, req)

		// should fail with conflict due to concurrent modification
		require.Equal(t, http.StatusConflict, w.Code)
	})

	t.Run("swapCommit rejects mismatched head", func(t *testing.T) {
		t.Parallel()

		actor, session := setupTestActor(t, srv, "did:plc:concurrent2", "concurrent2@example.com", "concurrent2.dev.atlaspds.dev")

		// create first record to get a known commit CID
		tid1, err := srv.db.NextTID(ctx, actor.Did)
		require.NoError(t, err)
		input1 := map[string]any{
			"repo":       actor.Did,
			"collection": "app.bsky.feed.post",
			"rkey":       tid1.String(),
			"record": map[string]any{
				"$type":     "app.bsky.feed.post",
				"text":      "First record",
				"createdAt": time.Now().Format(time.RFC3339),
			},
		}

		body1, err := json.Marshal(input1)
		require.NoError(t, err)

		w1 := httptest.NewRecorder()
		req1 := httptest.NewRequest(http.MethodPost, "/xrpc/com.atproto.repo.createRecord", bytes.NewReader(body1))
		req1 = addAuthContext(t, ctx, srv, req1, actor, session.AccessToken)
		srv.handleCreateRecord(w1, req1)
		require.Equal(t, http.StatusOK, w1.Code)

		var out1 atproto.RepoCreateRecord_Output
		err = json.Unmarshal(w1.Body.Bytes(), &out1)
		require.NoError(t, err)

		// reload actor to get current head
		actor, err = srv.db.GetActorByDID(ctx, actor.Did)
		require.NoError(t, err)

		// try to create another record with wrong swapCommit
		tid2, err := srv.db.NextTID(ctx, actor.Did)
		require.NoError(t, err)
		wrongSwapCommit := "bafyreihx6qqvghcmvpqq33kg4s7ztnh6mlt5cqpynjjxgcoynvndx5cuee"
		input2 := map[string]any{
			"repo":       actor.Did,
			"collection": "app.bsky.feed.post",
			"rkey":       tid2.String(),
			"swapCommit": wrongSwapCommit,
			"record": map[string]any{
				"$type":     "app.bsky.feed.post",
				"text":      "This should fail due to wrong swapCommit",
				"createdAt": time.Now().Format(time.RFC3339),
			},
		}

		body2, err := json.Marshal(input2)
		require.NoError(t, err)

		w2 := httptest.NewRecorder()
		req2 := httptest.NewRequest(http.MethodPost, "/xrpc/com.atproto.repo.createRecord", bytes.NewReader(body2))
		req2 = addAuthContext(t, ctx, srv, req2, actor, session.AccessToken)
		srv.handleCreateRecord(w2, req2)

		// should fail with conflict due to swapCommit mismatch
		require.Equal(t, http.StatusConflict, w2.Code)
	})

	t.Run("swapCommit accepts correct head", func(t *testing.T) {
		t.Parallel()

		actor, session := setupTestActor(t, srv, "did:plc:concurrent3", "concurrent3@example.com", "concurrent3.dev.atlaspds.dev")

		// create first record
		tid1, err := srv.db.NextTID(ctx, actor.Did)
		require.NoError(t, err)
		input1 := map[string]any{
			"repo":       actor.Did,
			"collection": "app.bsky.feed.post",
			"rkey":       tid1.String(),
			"record": map[string]any{
				"$type":     "app.bsky.feed.post",
				"text":      "First record",
				"createdAt": time.Now().Format(time.RFC3339),
			},
		}

		body1, err := json.Marshal(input1)
		require.NoError(t, err)

		w1 := httptest.NewRecorder()
		req1 := httptest.NewRequest(http.MethodPost, "/xrpc/com.atproto.repo.createRecord", bytes.NewReader(body1))
		req1 = addAuthContext(t, ctx, srv, req1, actor, session.AccessToken)
		srv.handleCreateRecord(w1, req1)
		require.Equal(t, http.StatusOK, w1.Code)

		var out1 atproto.RepoCreateRecord_Output
		err = json.Unmarshal(w1.Body.Bytes(), &out1)
		require.NoError(t, err)
		require.NotNil(t, out1.Commit)
		currentHead := out1.Commit.Cid

		// reload actor to get current state
		actor, err = srv.db.GetActorByDID(ctx, actor.Did)
		require.NoError(t, err)

		// create another record with correct swapCommit
		tid2, err := srv.db.NextTID(ctx, actor.Did)
		require.NoError(t, err)
		input2 := map[string]any{
			"repo":       actor.Did,
			"collection": "app.bsky.feed.post",
			"rkey":       tid2.String(),
			"swapCommit": currentHead,
			"record": map[string]any{
				"$type":     "app.bsky.feed.post",
				"text":      "Second record with correct swapCommit",
				"createdAt": time.Now().Format(time.RFC3339),
			},
		}

		body2, err := json.Marshal(input2)
		require.NoError(t, err)

		w2 := httptest.NewRecorder()
		req2 := httptest.NewRequest(http.MethodPost, "/xrpc/com.atproto.repo.createRecord", bytes.NewReader(body2))
		req2 = addAuthContext(t, ctx, srv, req2, actor, session.AccessToken)
		srv.handleCreateRecord(w2, req2)

		// should succeed
		require.Equal(t, http.StatusOK, w2.Code)
	})
}

func TestHandleApplyWrites(t *testing.T) {
	t.Parallel()
	srv := testServer(t)
	ctx := context.WithValue(t.Context(), hostContextKey{}, srv.hosts[testPDSHost])

	t.Run("success - single create", func(t *testing.T) {
		t.Parallel()

		actor, session := setupTestActor(t, srv, "did:plc:applywrites1", "applywrites1@example.com", "applywrites1.dev.atlaspds.dev")

		tid, err := srv.db.NextTID(ctx, actor.Did)
		require.NoError(t, err)
		rkey := tid.String()

		input := map[string]any{
			"repo": actor.Did,
			"writes": []map[string]any{
				{
					"$type":      "com.atproto.repo.applyWrites#create",
					"collection": "app.bsky.feed.post",
					"rkey":       rkey,
					"value": map[string]any{
						"$type":     "app.bsky.feed.post",
						"text":      "Hello from applyWrites",
						"createdAt": time.Now().Format(time.RFC3339),
					},
				},
			},
		}

		body, err := json.Marshal(input)
		require.NoError(t, err)

		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/xrpc/com.atproto.repo.applyWrites", bytes.NewReader(body))
		req = addAuthContext(t, ctx, srv, req, actor, session.AccessToken)
		srv.handleApplyWrites(w, req)

		require.Equal(t, http.StatusOK, w.Code)

		var out atproto.RepoApplyWrites_Output
		err = json.Unmarshal(w.Body.Bytes(), &out)
		require.NoError(t, err)

		require.NotNil(t, out.Commit)
		require.NotEmpty(t, out.Commit.Cid)
		require.NotEmpty(t, out.Commit.Rev)
		require.Len(t, out.Results, 1)
		require.NotNil(t, out.Results[0].RepoApplyWrites_CreateResult)
		require.Contains(t, out.Results[0].RepoApplyWrites_CreateResult.Uri, actor.Did)
		require.Contains(t, out.Results[0].RepoApplyWrites_CreateResult.Uri, rkey)
	})

	t.Run("success - multiple creates", func(t *testing.T) {
		t.Parallel()

		actor, session := setupTestActor(t, srv, "did:plc:applywrites2", "applywrites2@example.com", "applywrites2.dev.atlaspds.dev")

		writes := make([]map[string]any, 3)
		for i := range 3 {
			tid, err := srv.db.NextTID(ctx, actor.Did)
			require.NoError(t, err)
			writes[i] = map[string]any{
				"$type":      "com.atproto.repo.applyWrites#create",
				"collection": "app.bsky.feed.post",
				"rkey":       tid.String(),
				"value": map[string]any{
					"$type":     "app.bsky.feed.post",
					"text":      fmt.Sprintf("Post %d", i+1),
					"createdAt": time.Now().Format(time.RFC3339),
				},
			}
		}

		input := map[string]any{
			"repo":   actor.Did,
			"writes": writes,
		}

		body, err := json.Marshal(input)
		require.NoError(t, err)

		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/xrpc/com.atproto.repo.applyWrites", bytes.NewReader(body))
		req = addAuthContext(t, ctx, srv, req, actor, session.AccessToken)
		srv.handleApplyWrites(w, req)

		require.Equal(t, http.StatusOK, w.Code)

		var out atproto.RepoApplyWrites_Output
		err = json.Unmarshal(w.Body.Bytes(), &out)
		require.NoError(t, err)

		require.Len(t, out.Results, 3)
		for _, result := range out.Results {
			require.NotNil(t, result.RepoApplyWrites_CreateResult)
			require.Contains(t, result.RepoApplyWrites_CreateResult.Uri, actor.Did)
		}
	})

	t.Run("success - create and delete in one batch", func(t *testing.T) {
		t.Parallel()

		actor, session := setupTestActor(t, srv, "did:plc:applywrites3", "applywrites3@example.com", "applywrites3.dev.atlaspds.dev")

		// first create a record to delete
		tid1, err := srv.db.NextTID(ctx, actor.Did)
		require.NoError(t, err)
		rkey1 := tid1.String()

		createInput := map[string]any{
			"repo":       actor.Did,
			"collection": "app.bsky.feed.post",
			"rkey":       rkey1,
			"record": map[string]any{
				"$type":     "app.bsky.feed.post",
				"text":      "To be deleted",
				"createdAt": time.Now().Format(time.RFC3339),
			},
		}

		createBody, err := json.Marshal(createInput)
		require.NoError(t, err)

		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/xrpc/com.atproto.repo.createRecord", bytes.NewReader(createBody))
		req = addAuthContext(t, ctx, srv, req, actor, session.AccessToken)
		srv.handleCreateRecord(w, req)
		require.Equal(t, http.StatusOK, w.Code)

		// reload actor
		actor, err = srv.db.GetActorByDID(ctx, actor.Did)
		require.NoError(t, err)

		// now use applyWrites to create a new record and delete the existing one
		tid2, err := srv.db.NextTID(ctx, actor.Did)
		require.NoError(t, err)
		rkey2 := tid2.String()

		input := map[string]any{
			"repo": actor.Did,
			"writes": []map[string]any{
				{
					"$type":      "com.atproto.repo.applyWrites#create",
					"collection": "app.bsky.feed.post",
					"rkey":       rkey2,
					"value": map[string]any{
						"$type":     "app.bsky.feed.post",
						"text":      "New post",
						"createdAt": time.Now().Format(time.RFC3339),
					},
				},
				{
					"$type":      "com.atproto.repo.applyWrites#delete",
					"collection": "app.bsky.feed.post",
					"rkey":       rkey1,
				},
			},
		}

		body, err := json.Marshal(input)
		require.NoError(t, err)

		w = httptest.NewRecorder()
		req = httptest.NewRequest(http.MethodPost, "/xrpc/com.atproto.repo.applyWrites", bytes.NewReader(body))
		req = addAuthContext(t, ctx, srv, req, actor, session.AccessToken)
		srv.handleApplyWrites(w, req)

		require.Equal(t, http.StatusOK, w.Code)

		var out atproto.RepoApplyWrites_Output
		err = json.Unmarshal(w.Body.Bytes(), &out)
		require.NoError(t, err)

		require.Len(t, out.Results, 2)
		require.NotNil(t, out.Results[0].RepoApplyWrites_CreateResult)
		require.NotNil(t, out.Results[1].RepoApplyWrites_DeleteResult)

		// verify the old record is deleted
		uri1 := fmt.Sprintf("at://%s/app.bsky.feed.post/%s", actor.Did, rkey1)
		_, err = srv.db.GetRecord(ctx, uri1)
		require.ErrorIs(t, err, db.ErrNotFound)

		// verify the new record exists
		uri2 := fmt.Sprintf("at://%s/app.bsky.feed.post/%s", actor.Did, rkey2)
		record, err := srv.db.GetRecord(ctx, uri2)
		require.NoError(t, err)
		require.NotNil(t, record)
	})

	t.Run("success - update operation", func(t *testing.T) {
		t.Parallel()

		actor, session := setupTestActor(t, srv, "did:plc:applywrites4", "applywrites4@example.com", "applywrites4.dev.atlaspds.dev")

		// first create a record to update
		tid, err := srv.db.NextTID(ctx, actor.Did)
		require.NoError(t, err)
		rkey := tid.String()

		createInput := map[string]any{
			"repo":       actor.Did,
			"collection": "app.bsky.feed.post",
			"rkey":       rkey,
			"record": map[string]any{
				"$type":     "app.bsky.feed.post",
				"text":      "Original text",
				"createdAt": time.Now().Format(time.RFC3339),
			},
		}

		createBody, err := json.Marshal(createInput)
		require.NoError(t, err)

		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/xrpc/com.atproto.repo.createRecord", bytes.NewReader(createBody))
		req = addAuthContext(t, ctx, srv, req, actor, session.AccessToken)
		srv.handleCreateRecord(w, req)
		require.Equal(t, http.StatusOK, w.Code)

		var createOut atproto.RepoCreateRecord_Output
		err = json.Unmarshal(w.Body.Bytes(), &createOut)
		require.NoError(t, err)
		originalCid := createOut.Cid

		// reload actor
		actor, err = srv.db.GetActorByDID(ctx, actor.Did)
		require.NoError(t, err)

		// now use applyWrites to update the record
		input := map[string]any{
			"repo": actor.Did,
			"writes": []map[string]any{
				{
					"$type":      "com.atproto.repo.applyWrites#update",
					"collection": "app.bsky.feed.post",
					"rkey":       rkey,
					"value": map[string]any{
						"$type":     "app.bsky.feed.post",
						"text":      "Updated text",
						"createdAt": time.Now().Format(time.RFC3339),
					},
				},
			},
		}

		body, err := json.Marshal(input)
		require.NoError(t, err)

		w = httptest.NewRecorder()
		req = httptest.NewRequest(http.MethodPost, "/xrpc/com.atproto.repo.applyWrites", bytes.NewReader(body))
		req = addAuthContext(t, ctx, srv, req, actor, session.AccessToken)
		srv.handleApplyWrites(w, req)

		require.Equal(t, http.StatusOK, w.Code)

		var out atproto.RepoApplyWrites_Output
		err = json.Unmarshal(w.Body.Bytes(), &out)
		require.NoError(t, err)

		require.Len(t, out.Results, 1)
		require.NotNil(t, out.Results[0].RepoApplyWrites_UpdateResult)
		require.NotEqual(t, originalCid, out.Results[0].RepoApplyWrites_UpdateResult.Cid)
	})

	t.Run("success - create without rkey generates TID", func(t *testing.T) {
		t.Parallel()

		actor, session := setupTestActor(t, srv, "did:plc:applywrites5", "applywrites5@example.com", "applywrites5.dev.atlaspds.dev")

		input := map[string]any{
			"repo": actor.Did,
			"writes": []map[string]any{
				{
					"$type":      "com.atproto.repo.applyWrites#create",
					"collection": "app.bsky.feed.post",
					// no rkey - should be auto-generated
					"value": map[string]any{
						"$type":     "app.bsky.feed.post",
						"text":      "Auto-generated rkey",
						"createdAt": time.Now().Format(time.RFC3339),
					},
				},
			},
		}

		body, err := json.Marshal(input)
		require.NoError(t, err)

		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/xrpc/com.atproto.repo.applyWrites", bytes.NewReader(body))
		req = addAuthContext(t, ctx, srv, req, actor, session.AccessToken)
		srv.handleApplyWrites(w, req)

		require.Equal(t, http.StatusOK, w.Code)

		var out atproto.RepoApplyWrites_Output
		err = json.Unmarshal(w.Body.Bytes(), &out)
		require.NoError(t, err)

		require.Len(t, out.Results, 1)
		require.NotNil(t, out.Results[0].RepoApplyWrites_CreateResult)

		// verify the URI contains a valid TID-like rkey
		uri := out.Results[0].RepoApplyWrites_CreateResult.Uri
		require.Contains(t, uri, actor.Did)
		require.Contains(t, uri, "app.bsky.feed.post")
	})

	t.Run("error - repo mismatch", func(t *testing.T) {
		t.Parallel()

		actor, session := setupTestActor(t, srv, "did:plc:applywrites6", "applywrites6@example.com", "applywrites6.dev.atlaspds.dev")

		input := map[string]any{
			"repo": "did:plc:someoneelse",
			"writes": []map[string]any{
				{
					"$type":      "com.atproto.repo.applyWrites#create",
					"collection": "app.bsky.feed.post",
					"rkey":       "abc123",
					"value": map[string]any{
						"$type": "app.bsky.feed.post",
						"text":  "Forbidden",
					},
				},
			},
		}

		body, err := json.Marshal(input)
		require.NoError(t, err)

		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/xrpc/com.atproto.repo.applyWrites", bytes.NewReader(body))
		req = addAuthContext(t, ctx, srv, req, actor, session.AccessToken)
		srv.handleApplyWrites(w, req)

		require.Equal(t, http.StatusForbidden, w.Code)
	})

	t.Run("error - empty writes", func(t *testing.T) {
		t.Parallel()

		actor, session := setupTestActor(t, srv, "did:plc:applywrites7", "applywrites7@example.com", "applywrites7.dev.atlaspds.dev")

		input := map[string]any{
			"repo":   actor.Did,
			"writes": []map[string]any{},
		}

		body, err := json.Marshal(input)
		require.NoError(t, err)

		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/xrpc/com.atproto.repo.applyWrites", bytes.NewReader(body))
		req = addAuthContext(t, ctx, srv, req, actor, session.AccessToken)
		srv.handleApplyWrites(w, req)

		require.Equal(t, http.StatusBadRequest, w.Code)
	})

	t.Run("error - invalid write type", func(t *testing.T) {
		t.Parallel()

		actor, session := setupTestActor(t, srv, "did:plc:applywrites8", "applywrites8@example.com", "applywrites8.dev.atlaspds.dev")

		input := map[string]any{
			"repo": actor.Did,
			"writes": []map[string]any{
				{
					"$type":      "com.atproto.repo.applyWrites#invalid",
					"collection": "app.bsky.feed.post",
					"rkey":       "abc123",
					"value": map[string]any{
						"$type": "app.bsky.feed.post",
						"text":  "Invalid type",
					},
				},
			},
		}

		body, err := json.Marshal(input)
		require.NoError(t, err)

		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/xrpc/com.atproto.repo.applyWrites", bytes.NewReader(body))
		req = addAuthContext(t, ctx, srv, req, actor, session.AccessToken)
		srv.handleApplyWrites(w, req)

		require.Equal(t, http.StatusBadRequest, w.Code)
	})

	t.Run("error - create duplicate record", func(t *testing.T) {
		t.Parallel()

		actor, session := setupTestActor(t, srv, "did:plc:applywrites9", "applywrites9@example.com", "applywrites9.dev.atlaspds.dev")

		// first create a record
		tid, err := srv.db.NextTID(ctx, actor.Did)
		require.NoError(t, err)
		rkey := tid.String()

		createInput := map[string]any{
			"repo":       actor.Did,
			"collection": "app.bsky.feed.post",
			"rkey":       rkey,
			"record": map[string]any{
				"$type":     "app.bsky.feed.post",
				"text":      "First post",
				"createdAt": time.Now().Format(time.RFC3339),
			},
		}

		createBody, err := json.Marshal(createInput)
		require.NoError(t, err)

		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/xrpc/com.atproto.repo.createRecord", bytes.NewReader(createBody))
		req = addAuthContext(t, ctx, srv, req, actor, session.AccessToken)
		srv.handleCreateRecord(w, req)
		require.Equal(t, http.StatusOK, w.Code)

		// reload actor
		actor, err = srv.db.GetActorByDID(ctx, actor.Did)
		require.NoError(t, err)

		// try to create with same rkey via applyWrites
		input := map[string]any{
			"repo": actor.Did,
			"writes": []map[string]any{
				{
					"$type":      "com.atproto.repo.applyWrites#create",
					"collection": "app.bsky.feed.post",
					"rkey":       rkey,
					"value": map[string]any{
						"$type":     "app.bsky.feed.post",
						"text":      "Duplicate",
						"createdAt": time.Now().Format(time.RFC3339),
					},
				},
			},
		}

		body, err := json.Marshal(input)
		require.NoError(t, err)

		w = httptest.NewRecorder()
		req = httptest.NewRequest(http.MethodPost, "/xrpc/com.atproto.repo.applyWrites", bytes.NewReader(body))
		req = addAuthContext(t, ctx, srv, req, actor, session.AccessToken)
		srv.handleApplyWrites(w, req)

		require.Equal(t, http.StatusConflict, w.Code)
	})

	t.Run("error - invalid collection NSID", func(t *testing.T) {
		t.Parallel()

		actor, session := setupTestActor(t, srv, "did:plc:applywrites10", "applywrites10@example.com", "applywrites10.dev.atlaspds.dev")

		input := map[string]any{
			"repo": actor.Did,
			"writes": []map[string]any{
				{
					"$type":      "com.atproto.repo.applyWrites#create",
					"collection": "not-valid",
					"rkey":       "abc123",
					"value": map[string]any{
						"$type": "not-valid",
						"text":  "Invalid collection",
					},
				},
			},
		}

		body, err := json.Marshal(input)
		require.NoError(t, err)

		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/xrpc/com.atproto.repo.applyWrites", bytes.NewReader(body))
		req = addAuthContext(t, ctx, srv, req, actor, session.AccessToken)
		srv.handleApplyWrites(w, req)

		require.Equal(t, http.StatusBadRequest, w.Code)
	})

	t.Run("error - no auth", func(t *testing.T) {
		t.Parallel()
		router := srv.hostMiddleware(srv.router())

		input := map[string]any{
			"repo": "did:plc:noauth",
			"writes": []map[string]any{
				{
					"$type":      "com.atproto.repo.applyWrites#create",
					"collection": "app.bsky.feed.post",
					"rkey":       "abc123",
					"value": map[string]any{
						"$type": "app.bsky.feed.post",
						"text":  "No auth",
					},
				},
			},
		}

		body, err := json.Marshal(input)
		require.NoError(t, err)

		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/xrpc/com.atproto.repo.applyWrites", bytes.NewReader(body))
		req.Host = testPDSHost
		router.ServeHTTP(w, req)

		require.Equal(t, http.StatusUnauthorized, w.Code)
	})
}

func TestHandleDescribeRepo(t *testing.T) {
	t.Parallel()
	srv := testServer(t)
	router := srv.router()
	ctx := context.WithValue(t.Context(), hostContextKey{}, srv.hosts[testPDSHost])

	dir, ok := srv.directory.(*identity.MockDirectory)
	require.True(t, ok, "directory must be a MockDirectory")

	t.Run("success - by DID", func(t *testing.T) {
		t.Parallel()

		actor, _ := setupTestActor(t, srv, "did:plc:describerepo1", "describerepo1@example.com", "describerepo1.dev.atlaspds.dev")

		// add to mock directory
		handle, err := syntax.ParseHandle(actor.Handle)
		require.NoError(t, err)
		did, err := syntax.ParseDID(actor.Did)
		require.NoError(t, err)
		dir.Insert(identity.Identity{
			DID:         did,
			Handle:      handle,
			AlsoKnownAs: []string{"at://" + actor.Handle},
		})

		w := httptest.NewRecorder()
		url := fmt.Sprintf("/xrpc/com.atproto.repo.describeRepo?repo=%s", actor.Did)
		req := httptest.NewRequest(http.MethodGet, url, nil)
		router.ServeHTTP(w, req)

		require.Equal(t, http.StatusOK, w.Code)
		require.Equal(t, "application/json", w.Header().Get("Content-Type"))

		var out atproto.RepoDescribeRepo_Output
		err = json.Unmarshal(w.Body.Bytes(), &out)
		require.NoError(t, err)

		require.Equal(t, actor.Did, out.Did)
		require.Equal(t, actor.Handle, out.Handle)
		require.True(t, out.HandleIsCorrect)
		require.NotNil(t, out.DidDoc)
		require.NotNil(t, out.Collections)
		require.Empty(t, out.Collections) // no records yet
	})

	t.Run("success - by handle", func(t *testing.T) {
		t.Parallel()

		actor, _ := setupTestActor(t, srv, "did:plc:describerepo2", "describerepo2@example.com", "describerepo2.dev.atlaspds.dev")

		// add to mock directory
		handle, err := syntax.ParseHandle(actor.Handle)
		require.NoError(t, err)
		did, err := syntax.ParseDID(actor.Did)
		require.NoError(t, err)
		dir.Insert(identity.Identity{
			DID:         did,
			Handle:      handle,
			AlsoKnownAs: []string{"at://" + actor.Handle},
		})

		w := httptest.NewRecorder()
		url := fmt.Sprintf("/xrpc/com.atproto.repo.describeRepo?repo=%s", actor.Handle)
		req := httptest.NewRequest(http.MethodGet, url, nil)
		router.ServeHTTP(w, req)

		require.Equal(t, http.StatusOK, w.Code)

		var out atproto.RepoDescribeRepo_Output
		err = json.Unmarshal(w.Body.Bytes(), &out)
		require.NoError(t, err)

		require.Equal(t, actor.Did, out.Did)
		require.Equal(t, actor.Handle, out.Handle)
		require.True(t, out.HandleIsCorrect)
	})

	t.Run("success - with collections", func(t *testing.T) {
		t.Parallel()

		actor, session := setupTestActor(t, srv, "did:plc:describerepo3", "describerepo3@example.com", "describerepo3.dev.atlaspds.dev")

		// add to mock directory
		handle, err := syntax.ParseHandle(actor.Handle)
		require.NoError(t, err)
		did, err := syntax.ParseDID(actor.Did)
		require.NoError(t, err)
		dir.Insert(identity.Identity{
			DID:         did,
			Handle:      handle,
			AlsoKnownAs: []string{"at://" + actor.Handle},
		})

		// create records in different collections
		collections := []string{"app.bsky.feed.post", "app.bsky.actor.profile", "app.bsky.feed.like"}
		for _, collection := range collections {
			tid, err := srv.db.NextTID(ctx, actor.Did)
			require.NoError(t, err)

			input := map[string]any{
				"repo":       actor.Did,
				"collection": collection,
				"rkey":       tid.String(),
				"record": map[string]any{
					"$type":     collection,
					"createdAt": time.Now().Format(time.RFC3339),
				},
			}

			body, err := json.Marshal(input)
			require.NoError(t, err)

			// reload actor for each request
			actor, err = srv.db.GetActorByDID(ctx, actor.Did)
			require.NoError(t, err)

			w := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodPost, "/xrpc/com.atproto.repo.createRecord", bytes.NewReader(body))
			req = addAuthContext(t, ctx, srv, req, actor, session.AccessToken)
			srv.handleCreateRecord(w, req)
			require.Equal(t, http.StatusOK, w.Code)
		}

		// now describe the repo
		w := httptest.NewRecorder()
		url := fmt.Sprintf("/xrpc/com.atproto.repo.describeRepo?repo=%s", actor.Did)
		req := httptest.NewRequest(http.MethodGet, url, nil)
		router.ServeHTTP(w, req)

		require.Equal(t, http.StatusOK, w.Code)

		var out atproto.RepoDescribeRepo_Output
		err = json.Unmarshal(w.Body.Bytes(), &out)
		require.NoError(t, err)

		require.Equal(t, actor.Did, out.Did)
		require.Len(t, out.Collections, 3)

		// verify all collections are present (order may vary)
		collectionSet := make(map[string]bool)
		for _, c := range out.Collections {
			collectionSet[c] = true
		}
		for _, expected := range collections {
			require.True(t, collectionSet[expected], "expected collection %s to be present", expected)
		}
	})

	t.Run("success - handle invalid", func(t *testing.T) {
		t.Parallel()

		actor, _ := setupTestActor(t, srv, "did:plc:describerepo4", "describerepo4@example.com", "describerepo4.dev.atlaspds.dev")

		// add to mock directory with invalid handle (bi-directional verification failed)
		did, err := syntax.ParseDID(actor.Did)
		require.NoError(t, err)
		dir.Insert(identity.Identity{
			DID:         did,
			Handle:      syntax.HandleInvalid,
			AlsoKnownAs: []string{"at://" + actor.Handle},
		})

		w := httptest.NewRecorder()
		url := fmt.Sprintf("/xrpc/com.atproto.repo.describeRepo?repo=%s", actor.Did)
		req := httptest.NewRequest(http.MethodGet, url, nil)
		router.ServeHTTP(w, req)

		require.Equal(t, http.StatusOK, w.Code)

		var out atproto.RepoDescribeRepo_Output
		err = json.Unmarshal(w.Body.Bytes(), &out)
		require.NoError(t, err)

		require.Equal(t, actor.Did, out.Did)
		require.Equal(t, "handle.invalid", out.Handle)
		require.False(t, out.HandleIsCorrect)
	})

	t.Run("error - missing repo parameter", func(t *testing.T) {
		t.Parallel()

		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/xrpc/com.atproto.repo.describeRepo", nil)
		router.ServeHTTP(w, req)

		require.Equal(t, http.StatusBadRequest, w.Code)
	})

	t.Run("error - invalid repo format", func(t *testing.T) {
		t.Parallel()

		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/xrpc/com.atproto.repo.describeRepo?repo=not-valid!", nil)
		router.ServeHTTP(w, req)

		require.Equal(t, http.StatusBadRequest, w.Code)
	})

	t.Run("error - repo not found", func(t *testing.T) {
		t.Parallel()

		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/xrpc/com.atproto.repo.describeRepo?repo=did:plc:nonexistent", nil)
		router.ServeHTTP(w, req)

		require.Equal(t, http.StatusNotFound, w.Code)
	})
}

func TestHandleListRecords(t *testing.T) {
	t.Parallel()
	srv := testServer(t)
	router := srv.router()
	ctx := context.WithValue(t.Context(), hostContextKey{}, srv.hosts[testPDSHost])

	// use timestamp suffix to make DIDs unique across test runs
	suffix := fmt.Sprintf("%d", time.Now().UnixNano())

	t.Run("success - lists records in collection", func(t *testing.T) {
		t.Parallel()

		actor, session := setupTestActor(t, srv, "did:plc:listrecords1"+suffix, "listrecords1@example.com", "listrecords1"+suffix+".dev.atlaspds.dev")

		// create some records
		for i := range 5 {
			tid, err := srv.db.NextTID(ctx, actor.Did)
			require.NoError(t, err)

			input := map[string]any{
				"repo":       actor.Did,
				"collection": "app.bsky.feed.post",
				"rkey":       tid.String(),
				"record": map[string]any{
					"$type":     "app.bsky.feed.post",
					"text":      fmt.Sprintf("Post %d for listRecords", i),
					"createdAt": time.Now().Format(time.RFC3339),
				},
			}

			body, err := json.Marshal(input)
			require.NoError(t, err)

			// reload actor before each create
			actor, err = srv.db.GetActorByDID(ctx, actor.Did)
			require.NoError(t, err)

			w := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodPost, "/xrpc/com.atproto.repo.createRecord", bytes.NewReader(body))
			req = addAuthContext(t, ctx, srv, req, actor, session.AccessToken)
			srv.handleCreateRecord(w, req)
			require.Equal(t, http.StatusOK, w.Code)
		}

		// list the records
		w := httptest.NewRecorder()
		url := fmt.Sprintf("/xrpc/com.atproto.repo.listRecords?repo=%s&collection=app.bsky.feed.post", actor.Did)
		req := httptest.NewRequest(http.MethodGet, url, nil)
		req = addTestHostContext(srv, req)
		router.ServeHTTP(w, req)

		require.Equal(t, http.StatusOK, w.Code)
		require.Equal(t, "application/json", w.Header().Get("Content-Type"))

		var out struct {
			Cursor  *string `json:"cursor"`
			Records []struct {
				Uri   string         `json:"uri"`
				Cid   string         `json:"cid"`
				Value map[string]any `json:"value"`
			} `json:"records"`
		}
		err := json.Unmarshal(w.Body.Bytes(), &out)
		require.NoError(t, err)

		require.Len(t, out.Records, 5)
		for _, record := range out.Records {
			require.Contains(t, record.Uri, actor.Did)
			require.Contains(t, record.Uri, "app.bsky.feed.post")
			require.NotEmpty(t, record.Cid)
			require.NotNil(t, record.Value)
			require.Equal(t, "app.bsky.feed.post", record.Value["$type"])
		}
	})

	t.Run("success - respects limit parameter", func(t *testing.T) {
		t.Parallel()

		actor, session := setupTestActor(t, srv, "did:plc:listrecords2"+suffix, "listrecords2@example.com", "listrecords2"+suffix+".dev.atlaspds.dev")

		// create 5 records
		for i := range 5 {
			tid, err := srv.db.NextTID(ctx, actor.Did)
			require.NoError(t, err)

			input := map[string]any{
				"repo":       actor.Did,
				"collection": "app.bsky.feed.post",
				"rkey":       tid.String(),
				"record": map[string]any{
					"$type":     "app.bsky.feed.post",
					"text":      fmt.Sprintf("Post %d", i),
					"createdAt": time.Now().Format(time.RFC3339),
				},
			}

			body, err := json.Marshal(input)
			require.NoError(t, err)

			actor, err = srv.db.GetActorByDID(ctx, actor.Did)
			require.NoError(t, err)

			w := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodPost, "/xrpc/com.atproto.repo.createRecord", bytes.NewReader(body))
			req = addAuthContext(t, ctx, srv, req, actor, session.AccessToken)
			srv.handleCreateRecord(w, req)
			require.Equal(t, http.StatusOK, w.Code)
		}

		// list with limit of 2
		w := httptest.NewRecorder()
		url := fmt.Sprintf("/xrpc/com.atproto.repo.listRecords?repo=%s&collection=app.bsky.feed.post&limit=2", actor.Did)
		req := httptest.NewRequest(http.MethodGet, url, nil)
		req = addTestHostContext(srv, req)
		router.ServeHTTP(w, req)

		require.Equal(t, http.StatusOK, w.Code)

		var out struct {
			Cursor  *string `json:"cursor"`
			Records []any   `json:"records"`
		}
		err := json.Unmarshal(w.Body.Bytes(), &out)
		require.NoError(t, err)

		require.Len(t, out.Records, 2)
		require.NotNil(t, out.Cursor, "cursor should be set when there are more records")
	})

	t.Run("success - pagination with cursor", func(t *testing.T) {
		t.Parallel()

		actor, session := setupTestActor(t, srv, "did:plc:listrecords3"+suffix, "listrecords3@example.com", "listrecords3"+suffix+".dev.atlaspds.dev")

		// create 5 records
		for i := range 5 {
			tid, err := srv.db.NextTID(ctx, actor.Did)
			require.NoError(t, err)

			input := map[string]any{
				"repo":       actor.Did,
				"collection": "app.bsky.feed.post",
				"rkey":       tid.String(),
				"record": map[string]any{
					"$type":     "app.bsky.feed.post",
					"text":      fmt.Sprintf("Post %d", i),
					"createdAt": time.Now().Format(time.RFC3339),
				},
			}

			body, err := json.Marshal(input)
			require.NoError(t, err)

			actor, err = srv.db.GetActorByDID(ctx, actor.Did)
			require.NoError(t, err)

			w := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodPost, "/xrpc/com.atproto.repo.createRecord", bytes.NewReader(body))
			req = addAuthContext(t, ctx, srv, req, actor, session.AccessToken)
			srv.handleCreateRecord(w, req)
			require.Equal(t, http.StatusOK, w.Code)
		}

		// first page
		w1 := httptest.NewRecorder()
		url := fmt.Sprintf("/xrpc/com.atproto.repo.listRecords?repo=%s&collection=app.bsky.feed.post&limit=2", actor.Did)
		req := httptest.NewRequest(http.MethodGet, url, nil)
		req = addTestHostContext(srv, req)
		router.ServeHTTP(w1, req)

		require.Equal(t, http.StatusOK, w1.Code)

		var out1 struct {
			Cursor  *string `json:"cursor"`
			Records []struct {
				Uri string `json:"uri"`
			} `json:"records"`
		}
		err := json.Unmarshal(w1.Body.Bytes(), &out1)
		require.NoError(t, err)

		require.Len(t, out1.Records, 2)
		require.NotNil(t, out1.Cursor)

		// second page using cursor
		w2 := httptest.NewRecorder()
		url = fmt.Sprintf("/xrpc/com.atproto.repo.listRecords?repo=%s&collection=app.bsky.feed.post&limit=2&cursor=%s", actor.Did, *out1.Cursor)
		req = httptest.NewRequest(http.MethodGet, url, nil)
		req = addTestHostContext(srv, req)
		router.ServeHTTP(w2, req)

		require.Equal(t, http.StatusOK, w2.Code)

		var out2 struct {
			Cursor  *string `json:"cursor"`
			Records []struct {
				Uri string `json:"uri"`
			} `json:"records"`
		}
		err = json.Unmarshal(w2.Body.Bytes(), &out2)
		require.NoError(t, err)

		require.Len(t, out2.Records, 2)

		// verify different records
		require.NotEqual(t, out1.Records[0].Uri, out2.Records[0].Uri)
	})

	t.Run("success - empty collection", func(t *testing.T) {
		t.Parallel()

		actor, _ := setupTestActor(t, srv, "did:plc:listrecords4"+suffix, "listrecords4@example.com", "listrecords4"+suffix+".dev.atlaspds.dev")

		w := httptest.NewRecorder()
		url := fmt.Sprintf("/xrpc/com.atproto.repo.listRecords?repo=%s&collection=app.bsky.feed.post", actor.Did)
		req := httptest.NewRequest(http.MethodGet, url, nil)
		req = addTestHostContext(srv, req)
		router.ServeHTTP(w, req)

		require.Equal(t, http.StatusOK, w.Code)

		var out struct {
			Cursor  *string `json:"cursor"`
			Records []any   `json:"records"`
		}
		err := json.Unmarshal(w.Body.Bytes(), &out)
		require.NoError(t, err)

		require.Len(t, out.Records, 0)
		require.Nil(t, out.Cursor)
	})

	t.Run("success - caps limit at 100", func(t *testing.T) {
		t.Parallel()

		actor, _ := setupTestActor(t, srv, "did:plc:listrecords5"+suffix, "listrecords5@example.com", "listrecords5"+suffix+".dev.atlaspds.dev")

		w := httptest.NewRecorder()
		url := fmt.Sprintf("/xrpc/com.atproto.repo.listRecords?repo=%s&collection=app.bsky.feed.post&limit=9999", actor.Did)
		req := httptest.NewRequest(http.MethodGet, url, nil)
		req = addTestHostContext(srv, req)
		router.ServeHTTP(w, req)

		// should succeed (limit is capped internally)
		require.Equal(t, http.StatusOK, w.Code)
	})

	t.Run("success - reverse order", func(t *testing.T) {
		t.Parallel()

		actor, session := setupTestActor(t, srv, "did:plc:listrecords6"+suffix, "listrecords6@example.com", "listrecords6"+suffix+".dev.atlaspds.dev")

		// create 3 records with known rkeys
		rkeys := make([]string, 3)
		for i := range 3 {
			tid, err := srv.db.NextTID(ctx, actor.Did)
			require.NoError(t, err)
			rkeys[i] = tid.String()

			input := map[string]any{
				"repo":       actor.Did,
				"collection": "app.bsky.feed.post",
				"rkey":       rkeys[i],
				"record": map[string]any{
					"$type":     "app.bsky.feed.post",
					"text":      fmt.Sprintf("Post %d", i),
					"createdAt": time.Now().Format(time.RFC3339),
				},
			}

			body, err := json.Marshal(input)
			require.NoError(t, err)

			actor, err = srv.db.GetActorByDID(ctx, actor.Did)
			require.NoError(t, err)

			w := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodPost, "/xrpc/com.atproto.repo.createRecord", bytes.NewReader(body))
			req = addAuthContext(t, ctx, srv, req, actor, session.AccessToken)
			srv.handleCreateRecord(w, req)
			require.Equal(t, http.StatusOK, w.Code)
		}

		// list forward
		w1 := httptest.NewRecorder()
		url := fmt.Sprintf("/xrpc/com.atproto.repo.listRecords?repo=%s&collection=app.bsky.feed.post", actor.Did)
		req := httptest.NewRequest(http.MethodGet, url, nil)
		req = addTestHostContext(srv, req)
		router.ServeHTTP(w1, req)
		require.Equal(t, http.StatusOK, w1.Code)

		var out1 struct {
			Records []struct {
				Uri string `json:"uri"`
			} `json:"records"`
		}
		err := json.Unmarshal(w1.Body.Bytes(), &out1)
		require.NoError(t, err)

		// list reverse
		w2 := httptest.NewRecorder()
		url = fmt.Sprintf("/xrpc/com.atproto.repo.listRecords?repo=%s&collection=app.bsky.feed.post&reverse=true", actor.Did)
		req = httptest.NewRequest(http.MethodGet, url, nil)
		req = addTestHostContext(srv, req)
		router.ServeHTTP(w2, req)
		require.Equal(t, http.StatusOK, w2.Code)

		var out2 struct {
			Records []struct {
				Uri string `json:"uri"`
			} `json:"records"`
		}
		err = json.Unmarshal(w2.Body.Bytes(), &out2)
		require.NoError(t, err)

		// verify order is reversed
		require.Len(t, out1.Records, 3)
		require.Len(t, out2.Records, 3)
		require.Equal(t, out1.Records[0].Uri, out2.Records[2].Uri)
		require.Equal(t, out1.Records[2].Uri, out2.Records[0].Uri)
	})

	t.Run("error - missing repo parameter", func(t *testing.T) {
		t.Parallel()

		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/xrpc/com.atproto.repo.listRecords?collection=app.bsky.feed.post", nil)
		req = addTestHostContext(srv, req)
		router.ServeHTTP(w, req)

		require.Equal(t, http.StatusBadRequest, w.Code)
	})

	t.Run("error - missing collection parameter", func(t *testing.T) {
		t.Parallel()

		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/xrpc/com.atproto.repo.listRecords?repo=did:plc:test", nil)
		req = addTestHostContext(srv, req)
		router.ServeHTTP(w, req)

		require.Equal(t, http.StatusBadRequest, w.Code)
	})

	t.Run("error - invalid collection nsid", func(t *testing.T) {
		t.Parallel()

		actor, _ := setupTestActor(t, srv, "did:plc:listrecords7"+suffix, "listrecords7@example.com", "listrecords7"+suffix+".dev.atlaspds.dev")

		w := httptest.NewRecorder()
		url := fmt.Sprintf("/xrpc/com.atproto.repo.listRecords?repo=%s&collection=not-a-nsid", actor.Did)
		req := httptest.NewRequest(http.MethodGet, url, nil)
		req = addTestHostContext(srv, req)
		router.ServeHTTP(w, req)

		require.Equal(t, http.StatusBadRequest, w.Code)
	})

	t.Run("error - invalid limit (negative)", func(t *testing.T) {
		t.Parallel()

		actor, _ := setupTestActor(t, srv, "did:plc:listrecords8"+suffix, "listrecords8@example.com", "listrecords8"+suffix+".dev.atlaspds.dev")

		w := httptest.NewRecorder()
		url := fmt.Sprintf("/xrpc/com.atproto.repo.listRecords?repo=%s&collection=app.bsky.feed.post&limit=-1", actor.Did)
		req := httptest.NewRequest(http.MethodGet, url, nil)
		req = addTestHostContext(srv, req)
		router.ServeHTTP(w, req)

		require.Equal(t, http.StatusBadRequest, w.Code)
	})

	t.Run("error - invalid limit (zero)", func(t *testing.T) {
		t.Parallel()

		actor, _ := setupTestActor(t, srv, "did:plc:listrecords9"+suffix, "listrecords9@example.com", "listrecords9"+suffix+".dev.atlaspds.dev")

		w := httptest.NewRecorder()
		url := fmt.Sprintf("/xrpc/com.atproto.repo.listRecords?repo=%s&collection=app.bsky.feed.post&limit=0", actor.Did)
		req := httptest.NewRequest(http.MethodGet, url, nil)
		req = addTestHostContext(srv, req)
		router.ServeHTTP(w, req)

		require.Equal(t, http.StatusBadRequest, w.Code)
	})

	t.Run("error - repo not found", func(t *testing.T) {
		t.Parallel()

		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/xrpc/com.atproto.repo.listRecords?repo=did:plc:nonexistent&collection=app.bsky.feed.post", nil)
		req = addTestHostContext(srv, req)
		router.ServeHTTP(w, req)

		require.Equal(t, http.StatusNotFound, w.Code)
	})
}
