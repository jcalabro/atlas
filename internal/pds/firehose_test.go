package pds

import (
	"context"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/bluesky-social/indigo/atproto/atdata"
	"github.com/gorilla/websocket"
	"github.com/jcalabro/atlas/internal/at"
	"github.com/jcalabro/atlas/internal/types"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestFirehoseSubscribeRepos(t *testing.T) {
	t.Parallel()

	srv := testServer(t)
	// initialize firehose for the test server
	srv.firehose = newFirehose(srv.log, srv.db)

	// start firehose in background
	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel) // use Cleanup so it runs after subtests complete
	go srv.firehose.Run(ctx)

	// start HTTP test server with full middleware chain
	handler := srv.observabilityMiddleware(srv.hostMiddleware(srv.router()))
	ts := httptest.NewServer(handler)
	t.Cleanup(ts.Close) // use Cleanup so it runs after subtests complete

	// register the test server's address as a valid host so hostMiddleware passes
	// extract host:port from the test server URL and add just the IP as a host
	tsHost := strings.TrimPrefix(ts.URL, "http://")
	if idx := strings.LastIndex(tsHost, ":"); idx != -1 {
		tsHost = tsHost[:idx]
	}
	srv.hosts[tsHost] = srv.hosts[testPDSHost]

	t.Run("websocket connection established", func(t *testing.T) {
		t.Parallel()

		// create a test actor with repo
		actor, _ := setupTestActor(t, srv, "did:plc:firehosetest1", "firehose1@example.com", "firehose1.dev.atlaspds.dev")

		// connect to subscribeRepos via websocket
		wsURL := "ws" + strings.TrimPrefix(ts.URL, "http") + "/xrpc/com.atproto.sync.subscribeRepos"

		conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
		require.NoError(t, err)
		defer conn.Close() //nolint:errcheck

		// create a record to trigger an event
		record := map[string]any{
			"$type":     "app.bsky.feed.post",
			"text":      "Hello from firehose test!",
			"createdAt": time.Now().Format(time.RFC3339),
		}

		// create record via direct db call to trigger event
		createTestRecordDirect(t, srv, actor, "app.bsky.feed.post", record)

		// read the event from websocket with timeout
		conn.SetReadDeadline(time.Now().Add(5 * time.Second)) //nolint:errcheck
		msgType, data, err := conn.ReadMessage()
		require.NoError(t, err)
		require.Equal(t, websocket.BinaryMessage, msgType)
		require.NotEmpty(t, data)
	})

	t.Run("cursor replay from beginning", func(t *testing.T) {
		t.Parallel()

		// create a test actor with repo
		actor, _ := setupTestActor(t, srv, "did:plc:firehosetest2", "firehose2@example.com", "firehose2.dev.atlaspds.dev")

		// create some records to generate events
		for i := range 3 {
			record := map[string]any{
				"$type":     "app.bsky.feed.post",
				"text":      "Cursor test post " + string(rune('0'+i)),
				"createdAt": time.Now().Format(time.RFC3339),
			}
			createTestRecordDirect(t, srv, actor, "app.bsky.feed.post", record)
		}

		// connect with cursor=0 to replay from beginning
		wsURL := "ws" + strings.TrimPrefix(ts.URL, "http") + "/xrpc/com.atproto.sync.subscribeRepos?cursor=0"

		conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
		require.NoError(t, err)
		defer conn.Close() //nolint:errcheck

		// should receive at least one event
		conn.SetReadDeadline(time.Now().Add(5 * time.Second)) //nolint:errcheck
		msgType, data, err := conn.ReadMessage()
		require.NoError(t, err)
		require.Equal(t, websocket.BinaryMessage, msgType)
		require.NotEmpty(t, data)
	})
}

func TestFirehoseEventGeneration(t *testing.T) {
	t.Parallel()

	srv := testServer(t)
	ctx := t.Context()

	t.Run("create record generates event", func(t *testing.T) {
		t.Parallel()

		// create a test actor with repo
		actor, _ := setupTestActor(t, srv, "did:plc:firehoseevent1", "firehoseevent1@example.com", "firehoseevent1.dev.atlaspds.dev")

		// get initial cursor
		initialCursor, err := srv.db.GetLatestSeq(ctx)
		require.NoError(t, err)

		// create a record
		record := map[string]any{
			"$type":     "app.bsky.feed.post",
			"text":      "Event generation test",
			"createdAt": time.Now().Format(time.RFC3339),
		}
		createTestRecordDirect(t, srv, actor, "app.bsky.feed.post", record)

		// verify an event was generated
		newCursor, err := srv.db.GetLatestSeq(ctx)
		require.NoError(t, err)
		require.NotEqual(t, initialCursor, newCursor, "cursor should change after creating a record")
	})

	t.Run("put record generates event", func(t *testing.T) {
		t.Parallel()

		// create a test actor with repo
		actor, _ := setupTestActor(t, srv, "did:plc:firehoseevent2", "firehoseevent2@example.com", "firehoseevent2.dev.atlaspds.dev")

		// get initial cursor
		initialCursor, err := srv.db.GetLatestSeq(ctx)
		require.NoError(t, err)

		// put a record (create via put)
		record := map[string]any{
			"$type":     "app.bsky.feed.post",
			"text":      "Put event test",
			"createdAt": time.Now().Format(time.RFC3339),
		}
		putTestRecordDirect(t, srv, actor, "app.bsky.feed.post", "testkey123", record)

		// verify an event was generated
		newCursor, err := srv.db.GetLatestSeq(ctx)
		require.NoError(t, err)
		require.NotEqual(t, initialCursor, newCursor, "cursor should change after put record")
	})

	t.Run("delete record generates event", func(t *testing.T) {
		t.Parallel()

		// create a test actor with repo
		actor, _ := setupTestActor(t, srv, "did:plc:firehoseevent3", "firehoseevent3@example.com", "firehoseevent3.dev.atlaspds.dev")

		// create a record first
		record := map[string]any{
			"$type":     "app.bsky.feed.post",
			"text":      "To be deleted",
			"createdAt": time.Now().Format(time.RFC3339),
		}
		rkey := createTestRecordDirect(t, srv, actor, "app.bsky.feed.post", record)

		// get cursor before delete
		cursorBeforeDelete, err := srv.db.GetLatestSeq(ctx)
		require.NoError(t, err)

		// delete the record
		deleteTestRecordDirect(t, srv, actor, "app.bsky.feed.post", rkey)

		// verify an event was generated
		newCursor, err := srv.db.GetLatestSeq(ctx)
		require.NoError(t, err)
		require.NotEqual(t, cursorBeforeDelete, newCursor, "cursor should change after deleting a record")
	})
}

func TestFirehoseEventContent(t *testing.T) {
	t.Parallel()

	srv := testServer(t)
	ctx := t.Context()

	t.Run("event contains correct fields", func(t *testing.T) {
		t.Parallel()

		// create a test actor with repo
		actor, _ := setupTestActor(t, srv, "did:plc:firehosecontent1", "firehosecontent1@example.com", "firehosecontent1.dev.atlaspds.dev")

		// create a record
		record := map[string]any{
			"$type":     "app.bsky.feed.post",
			"text":      "Content test",
			"createdAt": time.Now().Format(time.RFC3339),
		}
		createTestRecordDirect(t, srv, actor, "app.bsky.feed.post", record)

		// get the event
		events, _, err := srv.db.GetEventsSince(ctx, nil, 100)
		require.NoError(t, err)
		require.NotEmpty(t, events)

		// find our event (last one for this actor)
		var foundEvent *types.RepoEvent
		for i := len(events) - 1; i >= 0; i-- {
			if events[i].Repo == actor.Did {
				foundEvent = events[i]
				break
			}
		}
		require.NotNil(t, foundEvent, "should find event for our actor")

		// verify event fields
		require.Equal(t, actor.Did, foundEvent.Repo)
		require.Equal(t, actor.PdsHost, foundEvent.PdsHost)
		require.NotEmpty(t, foundEvent.Rev)
		require.NotEmpty(t, foundEvent.Commit)
		require.NotEmpty(t, foundEvent.Blocks)
		require.Len(t, foundEvent.Ops, 1)
		require.Equal(t, "create", foundEvent.Ops[0].Action)
		require.Contains(t, foundEvent.Ops[0].Path, "app.bsky.feed.post/")
		require.NotEmpty(t, foundEvent.Ops[0].Cid)
	})
}

// createTestRecordDirect creates a record directly through the db layer
func createTestRecordDirect(t *testing.T, srv *server, actor *types.Actor, collection string, recordData map[string]any) string {
	t.Helper()
	ctx := context.WithValue(t.Context(), hostContextKey{}, srv.hosts[testPDSHost])

	// convert to CBOR
	cborBytes, err := atdata.MarshalCBOR(recordData)
	require.NoError(t, err)

	// generate rkey
	rkey, err := srv.db.NextTID(ctx, actor.Did)
	require.NoError(t, err)

	record := &types.Record{
		Did:        actor.Did,
		Collection: collection,
		Rkey:       rkey.String(),
		Value:      cborBytes,
		CreatedAt:  timestamppb.Now(),
	}

	// reload actor to get latest head
	actor, err = srv.db.GetActorByDID(ctx, actor.Did)
	require.NoError(t, err)

	result, err := srv.db.CreateRecord(ctx, actor, record, cborBytes, nil)
	require.NoError(t, err)
	require.NotEmpty(t, result.RecordCID)

	return rkey.String()
}

// putTestRecordDirect creates/updates a record directly through the db layer
func putTestRecordDirect(t *testing.T, srv *server, actor *types.Actor, collection, rkey string, recordData map[string]any) {
	t.Helper()
	ctx := context.WithValue(t.Context(), hostContextKey{}, srv.hosts[testPDSHost])

	// convert to CBOR
	cborBytes, err := atdata.MarshalCBOR(recordData)
	require.NoError(t, err)

	record := &types.Record{
		Did:        actor.Did,
		Collection: collection,
		Rkey:       rkey,
		Value:      cborBytes,
		CreatedAt:  timestamppb.Now(),
	}

	// reload actor to get latest head
	actor, err = srv.db.GetActorByDID(ctx, actor.Did)
	require.NoError(t, err)

	result, err := srv.db.PutRecord(ctx, actor, record, cborBytes, nil, nil)
	require.NoError(t, err)
	require.NotEmpty(t, result.RecordCID)
}

// deleteTestRecordDirect deletes a record directly through the db layer
func deleteTestRecordDirect(t *testing.T, srv *server, actor *types.Actor, collection, rkey string) {
	t.Helper()
	ctx := context.WithValue(t.Context(), hostContextKey{}, srv.hosts[testPDSHost])

	// reload actor to get latest head
	actor, err := srv.db.GetActorByDID(ctx, actor.Did)
	require.NoError(t, err)

	uri := &at.URI{
		Repo:       actor.Did,
		Collection: collection,
		Rkey:       rkey,
	}

	result, err := srv.db.DeleteRecord(ctx, actor, uri, nil)
	require.NoError(t, err)
	require.NotEmpty(t, result.CommitCID)
}
