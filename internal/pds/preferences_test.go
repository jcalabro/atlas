package pds

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestHandleGetPreferences(t *testing.T) {
	t.Parallel()

	signingKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	srv := testServer(t)
	srv.hosts[testPDSHost].signingKey = signingKey
	srv.hosts[testPDSHost].serviceDID = "did:plc:test-service-12345"

	addHostContext := func(req *http.Request) *http.Request {
		ctx := context.WithValue(req.Context(), hostContextKey{}, srv.hosts[testPDSHost])
		return req.WithContext(ctx)
	}

	t.Run("returns empty preferences for new actor", func(t *testing.T) {
		t.Parallel()

		_, session := setupTestActor(t, srv, "did:plc:getprefs1", "getprefs1@example.com", "getprefs1.dev.atlaspds.dev")

		w := httptest.NewRecorder()
		router := srv.router()

		req := httptest.NewRequest(http.MethodGet, "/xrpc/app.bsky.actor.getPreferences", nil)
		req.Header.Set("Authorization", "Bearer "+session.AccessToken)
		req = addHostContext(req)
		router.ServeHTTP(w, req)

		require.Equal(t, http.StatusOK, w.Code)

		var resp map[string]any
		err := json.NewDecoder(w.Body).Decode(&resp)
		require.NoError(t, err)

		prefs, ok := resp["preferences"].([]any)
		require.True(t, ok)
		require.Empty(t, prefs)
	})

	t.Run("returns saved preferences", func(t *testing.T) {
		t.Parallel()

		actor, session := setupTestActor(t, srv, "did:plc:getprefs2", "getprefs2@example.com", "getprefs2.dev.atlaspds.dev")

		// save some preferences
		prefsData := map[string]any{
			"preferences": []any{
				map[string]any{
					"$type":   "app.bsky.actor.defs#adultContentPref",
					"enabled": true,
				},
				map[string]any{
					"$type": "app.bsky.actor.defs#savedFeedsPrefV2",
					"items": []any{
						map[string]any{
							"type":  "feed",
							"value": "at://did:plc:example/app.bsky.feed.generator/whats-hot",
						},
					},
				},
			},
		}
		prefsBytes, err := json.Marshal(prefsData)
		require.NoError(t, err)

		actor.Preferences = prefsBytes
		ctx := context.WithValue(t.Context(), hostContextKey{}, srv.hosts[testPDSHost])
		err = srv.db.SaveActor(ctx, actor)
		require.NoError(t, err)

		w := httptest.NewRecorder()
		router := srv.router()

		req := httptest.NewRequest(http.MethodGet, "/xrpc/app.bsky.actor.getPreferences", nil)
		req.Header.Set("Authorization", "Bearer "+session.AccessToken)
		req = addHostContext(req)
		router.ServeHTTP(w, req)

		require.Equal(t, http.StatusOK, w.Code)

		var resp map[string]any
		err = json.NewDecoder(w.Body).Decode(&resp)
		require.NoError(t, err)

		prefs, ok := resp["preferences"].([]any)
		require.True(t, ok)
		require.Len(t, prefs, 2)

		// verify the adult content pref
		adultPref, ok := prefs[0].(map[string]any)
		require.True(t, ok)
		require.Equal(t, "app.bsky.actor.defs#adultContentPref", adultPref["$type"])
		require.Equal(t, true, adultPref["enabled"])
	})

	t.Run("requires authentication", func(t *testing.T) {
		t.Parallel()

		w := httptest.NewRecorder()
		router := srv.router()

		req := httptest.NewRequest(http.MethodGet, "/xrpc/app.bsky.actor.getPreferences", nil)
		req = addHostContext(req)
		router.ServeHTTP(w, req)

		require.Equal(t, http.StatusUnauthorized, w.Code)
	})
}

func TestHandlePutPreferences(t *testing.T) {
	t.Parallel()

	signingKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	srv := testServer(t)
	srv.hosts[testPDSHost].signingKey = signingKey
	srv.hosts[testPDSHost].serviceDID = "did:plc:test-service-12345"

	addHostContext := func(req *http.Request) *http.Request {
		ctx := context.WithValue(req.Context(), hostContextKey{}, srv.hosts[testPDSHost])
		return req.WithContext(ctx)
	}

	t.Run("saves preferences", func(t *testing.T) {
		t.Parallel()

		actor, session := setupTestActor(t, srv, "did:plc:putprefs1", "putprefs1@example.com", "putprefs1.dev.atlaspds.dev")

		prefsBody := `{
			"preferences": [
				{
					"$type": "app.bsky.actor.defs#adultContentPref",
					"enabled": false
				}
			]
		}`

		w := httptest.NewRecorder()
		router := srv.router()

		req := httptest.NewRequest(http.MethodPost, "/xrpc/app.bsky.actor.putPreferences", strings.NewReader(prefsBody))
		req.Header.Set("Authorization", "Bearer "+session.AccessToken)
		req.Header.Set("Content-Type", "application/json")
		req = addHostContext(req)
		router.ServeHTTP(w, req)

		require.Equal(t, http.StatusOK, w.Code)

		// verify the preferences were saved
		ctx := context.WithValue(t.Context(), hostContextKey{}, srv.hosts[testPDSHost])
		updatedActor, err := srv.db.GetActorByDID(ctx, actor.Did)
		require.NoError(t, err)
		require.NotEmpty(t, updatedActor.Preferences)

		var savedPrefs map[string]any
		err = json.Unmarshal(updatedActor.Preferences, &savedPrefs)
		require.NoError(t, err)

		prefs, ok := savedPrefs["preferences"].([]any)
		require.True(t, ok)
		require.Len(t, prefs, 1)

		adultPref, ok := prefs[0].(map[string]any)
		require.True(t, ok)
		require.Equal(t, "app.bsky.actor.defs#adultContentPref", adultPref["$type"])
		require.Equal(t, false, adultPref["enabled"])
	})

	t.Run("overwrites existing preferences", func(t *testing.T) {
		t.Parallel()

		actor, session := setupTestActor(t, srv, "did:plc:putprefs2", "putprefs2@example.com", "putprefs2.dev.atlaspds.dev")

		// set initial preferences
		initialPrefs := map[string]any{
			"preferences": []any{
				map[string]any{
					"$type":   "app.bsky.actor.defs#adultContentPref",
					"enabled": true,
				},
			},
		}
		initialBytes, err := json.Marshal(initialPrefs)
		require.NoError(t, err)

		actor.Preferences = initialBytes
		ctx := context.WithValue(t.Context(), hostContextKey{}, srv.hosts[testPDSHost])
		err = srv.db.SaveActor(ctx, actor)
		require.NoError(t, err)

		// update preferences
		newPrefsBody := `{
			"preferences": [
				{
					"$type": "app.bsky.actor.defs#contentLabelPref",
					"label": "nsfw",
					"visibility": "hide"
				}
			]
		}`

		w := httptest.NewRecorder()
		router := srv.router()

		req := httptest.NewRequest(http.MethodPost, "/xrpc/app.bsky.actor.putPreferences", strings.NewReader(newPrefsBody))
		req.Header.Set("Authorization", "Bearer "+session.AccessToken)
		req.Header.Set("Content-Type", "application/json")
		req = addHostContext(req)
		router.ServeHTTP(w, req)

		require.Equal(t, http.StatusOK, w.Code)

		// verify new preferences replaced old
		updatedActor, err := srv.db.GetActorByDID(ctx, actor.Did)
		require.NoError(t, err)

		var savedPrefs map[string]any
		err = json.Unmarshal(updatedActor.Preferences, &savedPrefs)
		require.NoError(t, err)

		prefs, ok := savedPrefs["preferences"].([]any)
		require.True(t, ok)
		require.Len(t, prefs, 1)

		// should be the new content label pref, not adult content pref
		labelPref, ok := prefs[0].(map[string]any)
		require.True(t, ok)
		require.Equal(t, "app.bsky.actor.defs#contentLabelPref", labelPref["$type"])
	})

	t.Run("rejects invalid json", func(t *testing.T) {
		t.Parallel()

		_, session := setupTestActor(t, srv, "did:plc:putprefs3", "putprefs3@example.com", "putprefs3.dev.atlaspds.dev")

		w := httptest.NewRecorder()
		router := srv.router()

		req := httptest.NewRequest(http.MethodPost, "/xrpc/app.bsky.actor.putPreferences", strings.NewReader("not valid json"))
		req.Header.Set("Authorization", "Bearer "+session.AccessToken)
		req.Header.Set("Content-Type", "application/json")
		req = addHostContext(req)
		router.ServeHTTP(w, req)

		require.Equal(t, http.StatusBadRequest, w.Code)
	})

	t.Run("requires authentication", func(t *testing.T) {
		t.Parallel()

		w := httptest.NewRecorder()
		router := srv.router()

		req := httptest.NewRequest(http.MethodPost, "/xrpc/app.bsky.actor.putPreferences", strings.NewReader(`{"preferences":[]}`))
		req.Header.Set("Content-Type", "application/json")
		req = addHostContext(req)
		router.ServeHTTP(w, req)

		require.Equal(t, http.StatusUnauthorized, w.Code)
	})
}
