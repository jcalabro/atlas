package db

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNextTID(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	db := testDB(t)

	did := "did:plc:tidtest1"

	// generate first TID
	tid1, err := db.NextTID(ctx, did)
	require.NoError(t, err)
	require.NotEmpty(t, tid1.String())
	require.Len(t, tid1.String(), 13) // TIDs are always 13 characters

	// generate second TID - should be greater
	tid2, err := db.NextTID(ctx, did)
	require.NoError(t, err)
	require.NotEmpty(t, tid2.String())
	require.Greater(t, tid2.Integer(), tid1.Integer(), "second TID should be greater than first")

	// generate third TID - should be greater than second
	tid3, err := db.NextTID(ctx, did)
	require.NoError(t, err)
	require.Greater(t, tid3.Integer(), tid2.Integer(), "third TID should be greater than second")

	// generate a few more TIDs to verify they're all strictly increasing
	lastTID := tid3.Integer()
	for range 5 {
		tid, err := db.NextTID(ctx, did)
		require.NoError(t, err)

		tidInt := tid.Integer()
		require.Greater(t, tidInt, lastTID)
		lastTID = tidInt
	}
}

func TestNextTID_DifferentRepos(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	db := testDB(t)

	did1 := "did:plc:tidtest2a"
	did2 := "did:plc:tidtest2b"

	// generate TIDs for different repos
	tid1a, err := db.NextTID(ctx, did1)
	require.NoError(t, err)

	tid2a, err := db.NextTID(ctx, did2)
	require.NoError(t, err)

	// generate more TIDs
	tid1b, err := db.NextTID(ctx, did1)
	require.NoError(t, err)

	tid2b, err := db.NextTID(ctx, did2)
	require.NoError(t, err)

	// each repo's TIDs should be monotonically increasing
	require.Greater(t, tid1b.Integer(), tid1a.Integer(), "did1 TIDs should be monotonic")
	require.Greater(t, tid2b.Integer(), tid2a.Integer(), "did2 TIDs should be monotonic")
}

func TestNextTID_Concurrent(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	db := testDB(t)

	did := "did:plc:tidtest3"
	numGoroutines := 10
	tidsPerGoroutine := 10

	var wg sync.WaitGroup
	tidChan := make(chan uint64, numGoroutines*tidsPerGoroutine)

	// spawn concurrent goroutines generating TIDs
	for range numGoroutines {
		wg.Go(func() {
			for range tidsPerGoroutine {
				tid, err := db.NextTID(ctx, did)
				require.NoError(t, err)
				tidChan <- tid.Integer()
			}
		})
	}

	wg.Wait()
	close(tidChan)

	// collect all TIDs
	tids := make([]uint64, 0, numGoroutines*tidsPerGoroutine)
	for tid := range tidChan {
		tids = append(tids, tid)
	}

	// verify all TIDs are unique
	seen := make(map[uint64]bool)
	for _, tid := range tids {
		require.False(t, seen[tid], "TID %d was generated more than once", tid)
		seen[tid] = true
	}

	require.Len(t, seen, numGoroutines*tidsPerGoroutine, "should have generated expected number of unique TIDs")
}
