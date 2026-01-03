package pds

import (
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
		router.ServeHTTP(w, req)

		require.Equal(t, http.StatusBadRequest, w.Code)
		require.Equal(t, "application/json", w.Header().Get("Content-Type"))
	})

	t.Run("error - invalid limit (non-numeric)", func(t *testing.T) {
		t.Parallel()
		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/xrpc/com.atproto.sync.listRepos?limit=abc", nil)
		router.ServeHTTP(w, req)

		require.Equal(t, http.StatusBadRequest, w.Code)
		require.Equal(t, "application/json", w.Header().Get("Content-Type"))
	})

	t.Run("error - invalid cursor (not a DID)", func(t *testing.T) {
		t.Parallel()
		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/xrpc/com.atproto.sync.listRepos?cursor=not-a-did", nil)
		router.ServeHTTP(w, req)

		require.Equal(t, http.StatusBadRequest, w.Code)
		require.Equal(t, "application/json", w.Header().Get("Content-Type"))
	})
}
