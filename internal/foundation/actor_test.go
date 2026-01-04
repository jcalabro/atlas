package foundation

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/jcalabro/atlas/internal/types"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const testPDSHost = "test.atlaspds.net"

func TestSaveActor(t *testing.T) {
	t.Parallel()
	db := testDB(t)
	ctx := context.Background()

	t.Run("successfully saves a new actor", func(t *testing.T) {
		actor := &types.Actor{
			Did:                   "did:plc:test123",
			Email:                 "test@example.com",
			CreatedAt:             timestamppb.New(time.Now()),
			EmailVerificationCode: "verification123",
			EmailConfirmed:        false,
			PasswordHash:          []byte("hashed_password"),
			SigningKey:            []byte("signing_key"),
			Handle:                "test.dev.atlaspds.net",
			Active:                true,
			RotationKeys:          [][]byte{[]byte("rotation_key")},
			RefreshTokens:         []*types.RefreshToken{{Token: "refresh_token"}},
			PdsHost:               testPDSHost,
		}

		err := db.SaveActor(ctx, actor)
		require.NoError(t, err)
	})

	t.Run("successfully overwrites an existing actor", func(t *testing.T) {
		actor := &types.Actor{
			Did:                   "did:plc:test456",
			Email:                 "update@example.com",
			CreatedAt:             timestamppb.New(time.Now()),
			EmailVerificationCode: "code1",
			EmailConfirmed:        false,
			PasswordHash:          []byte("password1"),
			SigningKey:            []byte("key1"),
			Handle:                "user1.dev.atlaspds.net",
			Active:                true,
			RotationKeys:          [][]byte{[]byte("rotation_key")},
			RefreshTokens:         []*types.RefreshToken{{Token: "refresh_token"}},
			PdsHost:               testPDSHost,
		}

		err := db.SaveActor(ctx, actor)
		require.NoError(t, err)

		// Update the actor
		actor.EmailConfirmed = true
		actor.EmailVerificationCode = ""
		actor.Handle = "updated.dev.atlaspds.net"

		err = db.SaveActor(ctx, actor)
		require.NoError(t, err)

		// Verify the update persisted
		retrieved, err := db.GetActorByEmail(ctx, testPDSHost, actor.Email)
		require.NoError(t, err)
		require.Equal(t, true, retrieved.EmailConfirmed)
		require.Equal(t, "", retrieved.EmailVerificationCode)
		require.Equal(t, "updated.dev.atlaspds.net", retrieved.Handle)
	})

	t.Run("handles actor with minimal fields", func(t *testing.T) {
		actor := &types.Actor{
			Did:           "did:plc:minimal",
			Email:         "minimal@example.com",
			Handle:        "minimal.dev.atlaspds.net",
			CreatedAt:     timestamppb.New(time.Now()),
			PasswordHash:  []byte("hash"),
			SigningKey:    []byte("key"),
			RotationKeys:  [][]byte{[]byte("rotation")},
			RefreshTokens: []*types.RefreshToken{},
			PdsHost:       testPDSHost,
		}

		err := db.SaveActor(ctx, actor)
		require.NoError(t, err)
	})
}

func TestGetActorByEmail(t *testing.T) {
	t.Parallel()
	db := testDB(t)
	ctx := context.Background()

	t.Run("successfully retrieves a saved actor", func(t *testing.T) {
		now := time.Now()
		actor := &types.Actor{
			Did:                   "did:plc:retrieve123",
			Email:                 "retrieve@example.com",
			CreatedAt:             timestamppb.New(now),
			EmailVerificationCode: "verify789",
			EmailConfirmed:        true,
			PasswordHash:          []byte("secure_hash"),
			SigningKey:            []byte("signing_key_bytes"),
			Handle:                "retrieve.dev.atlaspds.net",
			Active:                true,
			RotationKeys:          [][]byte{[]byte("rotation_key")},
			RefreshTokens:         []*types.RefreshToken{{Token: "refresh_token"}},
			PdsHost:               testPDSHost,
		}

		err := db.SaveActor(ctx, actor)
		require.NoError(t, err)

		retrieved, err := db.GetActorByEmail(ctx, testPDSHost, "retrieve@example.com")
		require.NoError(t, err)
		require.NotNil(t, retrieved)

		require.Equal(t, actor.Did, retrieved.Did)
		require.Equal(t, actor.Email, retrieved.Email)
		require.Equal(t, actor.EmailVerificationCode, retrieved.EmailVerificationCode)
		require.Equal(t, actor.EmailConfirmed, retrieved.EmailConfirmed)
		require.Equal(t, actor.PasswordHash, retrieved.PasswordHash)
		require.Equal(t, actor.SigningKey, retrieved.SigningKey)
		require.Equal(t, actor.Handle, retrieved.Handle)
		require.Equal(t, actor.Active, retrieved.Active)
		require.True(t, actor.CreatedAt.AsTime().Equal(retrieved.CreatedAt.AsTime()))
	})

	t.Run("returns ErrNotFound for non-existent email", func(t *testing.T) {
		actor, err := db.GetActorByEmail(ctx, testPDSHost, "nonexistent@example.com")
		require.ErrorIs(t, err, ErrNotFound)
		require.Nil(t, actor)
	})

	t.Run("retrieves correct actor when multiple exist", func(t *testing.T) {
		actor1 := &types.Actor{
			Did:           "did:plc:multi1",
			Email:         "user1@example.com",
			Handle:        "user1.dev.atlaspds.net",
			CreatedAt:     timestamppb.New(time.Now()),
			PasswordHash:  []byte("hash1"),
			SigningKey:    []byte("key1"),
			RotationKeys:  [][]byte{[]byte("rotation1")},
			RefreshTokens: []*types.RefreshToken{},
			PdsHost:       testPDSHost,
		}
		actor2 := &types.Actor{
			Did:           "did:plc:multi2",
			Email:         "user2@example.com",
			Handle:        "user2.dev.atlaspds.net",
			CreatedAt:     timestamppb.New(time.Now()),
			PasswordHash:  []byte("hash2"),
			SigningKey:    []byte("key2"),
			RotationKeys:  [][]byte{[]byte("rotation2")},
			RefreshTokens: []*types.RefreshToken{},
			PdsHost:       testPDSHost,
		}
		actor3 := &types.Actor{
			Did:           "did:plc:multi3",
			Email:         "user3@example.com",
			Handle:        "user3.dev.atlaspds.net",
			CreatedAt:     timestamppb.New(time.Now()),
			PasswordHash:  []byte("hash3"),
			SigningKey:    []byte("key3"),
			RotationKeys:  [][]byte{[]byte("rotation3")},
			RefreshTokens: []*types.RefreshToken{},
			PdsHost:       testPDSHost,
		}

		err := db.SaveActor(ctx, actor1)
		require.NoError(t, err)
		err = db.SaveActor(ctx, actor2)
		require.NoError(t, err)
		err = db.SaveActor(ctx, actor3)
		require.NoError(t, err)

		retrieved, err := db.GetActorByEmail(ctx, testPDSHost, "user2@example.com")
		require.NoError(t, err)
		require.Equal(t, "did:plc:multi2", retrieved.Did)
		require.Equal(t, "user2.dev.atlaspds.net", retrieved.Handle)
	})
}

func TestGetActorByDID(t *testing.T) {
	t.Parallel()
	db := testDB(t)
	ctx := context.Background()

	t.Run("successfully retrieves a saved actor", func(t *testing.T) {
		now := time.Now()
		actor := &types.Actor{
			Did:                   "did:plc:testdid123",
			Email:                 "testdid@example.com",
			CreatedAt:             timestamppb.New(now),
			EmailVerificationCode: "verify123",
			EmailConfirmed:        true,
			PasswordHash:          []byte("hash123"),
			SigningKey:            []byte("key123"),
			Handle:                "testdid.dev.atlaspds.net",
			Active:                true,
			RotationKeys:          [][]byte{[]byte("rotation_key")},
			RefreshTokens:         []*types.RefreshToken{{Token: "refresh_token"}},
			PdsHost:               testPDSHost,
		}

		err := db.SaveActor(ctx, actor)
		require.NoError(t, err)

		retrieved, err := db.GetActorByDID(ctx, "did:plc:testdid123")
		require.NoError(t, err)
		require.NotNil(t, retrieved)

		require.Equal(t, actor.Did, retrieved.Did)
		require.Equal(t, actor.Email, retrieved.Email)
		require.Equal(t, actor.EmailVerificationCode, retrieved.EmailVerificationCode)
		require.Equal(t, actor.EmailConfirmed, retrieved.EmailConfirmed)
		require.Equal(t, actor.PasswordHash, retrieved.PasswordHash)
		require.Equal(t, actor.SigningKey, retrieved.SigningKey)
		require.Equal(t, actor.Handle, retrieved.Handle)
		require.Equal(t, actor.Active, retrieved.Active)
		require.True(t, actor.CreatedAt.AsTime().Equal(retrieved.CreatedAt.AsTime()))
	})

	t.Run("returns ErrNotFound for non-existent DID", func(t *testing.T) {
		actor, err := db.GetActorByDID(ctx, "did:plc:nonexistent")
		require.ErrorIs(t, err, ErrNotFound)
		require.Nil(t, actor)
	})

	t.Run("retrieves correct actor when multiple exist", func(t *testing.T) {
		actor1 := &types.Actor{
			Did:           "did:plc:multidid1",
			Email:         "multidid1@example.com",
			Handle:        "multidid1.dev.atlaspds.net",
			CreatedAt:     timestamppb.New(time.Now()),
			PasswordHash:  []byte("hash1"),
			SigningKey:    []byte("key1"),
			RotationKeys:  [][]byte{[]byte("rotation1")},
			RefreshTokens: []*types.RefreshToken{},
			PdsHost:       testPDSHost,
		}
		actor2 := &types.Actor{
			Did:           "did:plc:multidid2",
			Email:         "multidid2@example.com",
			Handle:        "multidid2.dev.atlaspds.net",
			CreatedAt:     timestamppb.New(time.Now()),
			PasswordHash:  []byte("hash2"),
			SigningKey:    []byte("key2"),
			RotationKeys:  [][]byte{[]byte("rotation2")},
			RefreshTokens: []*types.RefreshToken{},
			PdsHost:       testPDSHost,
		}
		actor3 := &types.Actor{
			Did:           "did:plc:multidid3",
			Email:         "multidid3@example.com",
			Handle:        "multidid3.dev.atlaspds.net",
			CreatedAt:     timestamppb.New(time.Now()),
			PasswordHash:  []byte("hash3"),
			SigningKey:    []byte("key3"),
			RotationKeys:  [][]byte{[]byte("rotation3")},
			RefreshTokens: []*types.RefreshToken{},
			PdsHost:       testPDSHost,
		}

		err := db.SaveActor(ctx, actor1)
		require.NoError(t, err)
		err = db.SaveActor(ctx, actor2)
		require.NoError(t, err)
		err = db.SaveActor(ctx, actor3)
		require.NoError(t, err)

		retrieved, err := db.GetActorByDID(ctx, "did:plc:multidid2")
		require.NoError(t, err)
		require.Equal(t, "did:plc:multidid2", retrieved.Did)
		require.Equal(t, "multidid2@example.com", retrieved.Email)
		require.Equal(t, "multidid2.dev.atlaspds.net", retrieved.Handle)
	})
}

func TestGetActorByHandle(t *testing.T) {
	t.Parallel()
	db := testDB(t)
	ctx := context.Background()

	t.Run("successfully retrieves a saved actor", func(t *testing.T) {
		now := time.Now()
		actor := &types.Actor{
			Did:                   "did:plc:handletest123",
			Email:                 "handletest@example.com",
			CreatedAt:             timestamppb.New(now),
			EmailVerificationCode: "verify456",
			EmailConfirmed:        true,
			PasswordHash:          []byte("hash456"),
			SigningKey:            []byte("key456"),
			Handle:                "handletest.dev.atlaspds.net",
			Active:                true,
			RotationKeys:          [][]byte{[]byte("rotation_key")},
			RefreshTokens:         []*types.RefreshToken{{Token: "refresh_token"}},
			PdsHost:               testPDSHost,
		}

		err := db.SaveActor(ctx, actor)
		require.NoError(t, err)

		retrieved, err := db.GetActorByHandle(ctx, "handletest.dev.atlaspds.net")
		require.NoError(t, err)
		require.NotNil(t, retrieved)

		require.Equal(t, actor.Did, retrieved.Did)
		require.Equal(t, actor.Email, retrieved.Email)
		require.Equal(t, actor.EmailVerificationCode, retrieved.EmailVerificationCode)
		require.Equal(t, actor.EmailConfirmed, retrieved.EmailConfirmed)
		require.Equal(t, actor.PasswordHash, retrieved.PasswordHash)
		require.Equal(t, actor.SigningKey, retrieved.SigningKey)
		require.Equal(t, actor.Handle, retrieved.Handle)
		require.Equal(t, actor.Active, retrieved.Active)
		require.True(t, actor.CreatedAt.AsTime().Equal(retrieved.CreatedAt.AsTime()))
	})

	t.Run("returns ErrNotFound for non-existent handle", func(t *testing.T) {
		actor, err := db.GetActorByHandle(ctx, "nonexistent.dev.atlaspds.net")
		require.ErrorIs(t, err, ErrNotFound)
		require.Nil(t, actor)
	})

	t.Run("retrieves correct actor when multiple exist", func(t *testing.T) {
		actor1 := &types.Actor{
			Did:           "did:plc:multihandle1",
			Email:         "multihandle1@example.com",
			Handle:        "multihandle1.dev.atlaspds.net",
			CreatedAt:     timestamppb.New(time.Now()),
			PasswordHash:  []byte("hash1"),
			SigningKey:    []byte("key1"),
			RotationKeys:  [][]byte{[]byte("rotation1")},
			RefreshTokens: []*types.RefreshToken{},
			PdsHost:       testPDSHost,
		}
		actor2 := &types.Actor{
			Did:           "did:plc:multihandle2",
			Email:         "multihandle2@example.com",
			Handle:        "multihandle2.dev.atlaspds.net",
			CreatedAt:     timestamppb.New(time.Now()),
			PasswordHash:  []byte("hash2"),
			SigningKey:    []byte("key2"),
			RotationKeys:  [][]byte{[]byte("rotation2")},
			RefreshTokens: []*types.RefreshToken{},
			PdsHost:       testPDSHost,
		}
		actor3 := &types.Actor{
			Did:           "did:plc:multihandle3",
			Email:         "multihandle3@example.com",
			Handle:        "multihandle3.dev.atlaspds.net",
			CreatedAt:     timestamppb.New(time.Now()),
			PasswordHash:  []byte("hash3"),
			SigningKey:    []byte("key3"),
			RotationKeys:  [][]byte{[]byte("rotation3")},
			RefreshTokens: []*types.RefreshToken{},
			PdsHost:       testPDSHost,
		}

		err := db.SaveActor(ctx, actor1)
		require.NoError(t, err)
		err = db.SaveActor(ctx, actor2)
		require.NoError(t, err)
		err = db.SaveActor(ctx, actor3)
		require.NoError(t, err)

		retrieved, err := db.GetActorByHandle(ctx, "multihandle2.dev.atlaspds.net")
		require.NoError(t, err)
		require.Equal(t, "did:plc:multihandle2", retrieved.Did)
		require.Equal(t, "multihandle2@example.com", retrieved.Email)
		require.Equal(t, "multihandle2.dev.atlaspds.net", retrieved.Handle)
	})

	t.Run("handle lookup is updated when actor handle changes", func(t *testing.T) {
		actor := &types.Actor{
			Did:           "did:plc:changehandle",
			Email:         "changehandle@example.com",
			Handle:        "original.dev.atlaspds.net",
			CreatedAt:     timestamppb.New(time.Now()),
			PasswordHash:  []byte("hash"),
			SigningKey:    []byte("key"),
			RotationKeys:  [][]byte{[]byte("rotation")},
			RefreshTokens: []*types.RefreshToken{},
			PdsHost:       testPDSHost,
		}

		err := db.SaveActor(ctx, actor)
		require.NoError(t, err)

		// Verify we can retrieve by original handle
		retrieved, err := db.GetActorByHandle(ctx, "original.dev.atlaspds.net")
		require.NoError(t, err)
		require.Equal(t, "did:plc:changehandle", retrieved.Did)

		// Update the handle
		actor.Handle = "updated.dev.atlaspds.net"
		err = db.SaveActor(ctx, actor)
		require.NoError(t, err)

		// Verify we can retrieve by new handle
		retrieved, err = db.GetActorByHandle(ctx, "updated.dev.atlaspds.net")
		require.NoError(t, err)
		require.Equal(t, "did:plc:changehandle", retrieved.Did)
		require.Equal(t, "updated.dev.atlaspds.net", retrieved.Handle)

		// Old handle should still point to the same DID (stale index)
		// This is expected behavior with the current implementation
		retrieved, err = db.GetActorByHandle(ctx, "original.dev.atlaspds.net")
		require.NoError(t, err)
		require.NotNil(t, retrieved)
		require.Equal(t, "did:plc:changehandle", retrieved.Did)
		// But the actor's current handle should be the updated one
		require.Equal(t, "updated.dev.atlaspds.net", retrieved.Handle)
	})
}

func TestActorIndexConsistency(t *testing.T) {
	t.Parallel()
	db := testDB(t)
	ctx := context.Background()

	t.Run("all three lookups return the same actor", func(t *testing.T) {
		actor := &types.Actor{
			Did:           "did:plc:consistency123",
			Email:         "consistency@example.com",
			Handle:        "consistency.dev.atlaspds.net",
			CreatedAt:     timestamppb.New(time.Now()),
			PasswordHash:  []byte("hash"),
			SigningKey:    []byte("key"),
			RotationKeys:  [][]byte{[]byte("rotation")},
			RefreshTokens: []*types.RefreshToken{},
			PdsHost:       testPDSHost,
		}

		err := db.SaveActor(ctx, actor)
		require.NoError(t, err)

		byDID, err := db.GetActorByDID(ctx, "did:plc:consistency123")
		require.NoError(t, err)
		require.NotNil(t, byDID)

		byEmail, err := db.GetActorByEmail(ctx, testPDSHost, "consistency@example.com")
		require.NoError(t, err)
		require.NotNil(t, byEmail)

		byHandle, err := db.GetActorByHandle(ctx, "consistency.dev.atlaspds.net")
		require.NoError(t, err)
		require.NotNil(t, byHandle)

		// All three should return the same actor
		require.Equal(t, byDID.Did, byEmail.Did)
		require.Equal(t, byDID.Did, byHandle.Did)
		require.Equal(t, byDID.Email, byEmail.Email)
		require.Equal(t, byDID.Email, byHandle.Email)
		require.Equal(t, byDID.Handle, byEmail.Handle)
		require.Equal(t, byDID.Handle, byHandle.Handle)
	})
}

func TestListActors(t *testing.T) {
	t.Parallel()
	db := testDB(t)

	t.Run("returns all actors when limit exceeds total count", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()

		// use a unique prefix that's unlikely to conflict
		prefix := "did:plc:zlist"

		// create 3 actors
		for i := 1; i <= 3; i++ {
			actor := &types.Actor{
				Did:           fmt.Sprintf("%s%03d", prefix, i),
				Email:         fmt.Sprintf("zlist%d@example.com", i),
				Handle:        fmt.Sprintf("zlist%d.dev.atlaspds.net", i),
				PdsHost:       testPDSHost,
				CreatedAt:     timestamppb.New(time.Now()),
				PasswordHash:  fmt.Appendf(nil, "hash%d", i),
				SigningKey:    fmt.Appendf(nil, "key%d", i),
				RotationKeys:  [][]byte{fmt.Appendf(nil, "rotation%d", i)},
				RefreshTokens: []*types.RefreshToken{},
			}
			err := db.SaveActor(ctx, actor)
			require.NoError(t, err)
		}

		// query starting from our prefix
		actors, nextCursor, err := db.ListActors(ctx, testPDSHost, "did:plc:zlist000", 10)
		require.NoError(t, err)
		require.GreaterOrEqual(t, len(actors), 3)
		// verify our actors are in the results
		require.Equal(t, prefix+"001", actors[0].Did)
		require.Equal(t, prefix+"002", actors[1].Did)
		require.Equal(t, prefix+"003", actors[2].Did)
		// next cursor could be our last actor or something after if there are more
		_ = nextCursor
	})

	t.Run("paginates correctly with first page", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()

		prefix := "did:plc:zzzzzpage"

		// create 5 actors with lexicographically ordered DIDs
		for i := 1; i <= 5; i++ {
			actor := &types.Actor{
				Did:           fmt.Sprintf("%s%03d", prefix, i),
				Email:         fmt.Sprintf("zzpage%d@example.com", i),
				Handle:        fmt.Sprintf("zzpage%d.dev.atlaspds.net", i),
				PdsHost:       testPDSHost,
				CreatedAt:     timestamppb.New(time.Now()),
				PasswordHash:  fmt.Appendf(nil, "hash%d", i),
				SigningKey:    fmt.Appendf(nil, "key%d", i),
				RotationKeys:  [][]byte{fmt.Appendf(nil, "rotation%d", i)},
				RefreshTokens: []*types.RefreshToken{},
			}
			err := db.SaveActor(ctx, actor)
			require.NoError(t, err)
		}

		// fetch first page with limit 2, starting just before our data
		actors, nextCursor, err := db.ListActors(ctx, testPDSHost, prefix+"000", 2)
		require.NoError(t, err)
		require.Len(t, actors, 2)
		require.NotEmpty(t, nextCursor)
		require.Equal(t, prefix+"001", actors[0].Did)
		require.Equal(t, prefix+"002", actors[1].Did)
		require.Equal(t, prefix+"002", nextCursor)
	})

	t.Run("paginates correctly with middle page", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()

		prefix := "did:plc:zzzzzmid"

		// create 5 actors
		for i := 1; i <= 5; i++ {
			actor := &types.Actor{
				Did:           fmt.Sprintf("%s%03d", prefix, i),
				Email:         fmt.Sprintf("zzmid%d@example.com", i),
				Handle:        fmt.Sprintf("zzmid%d.dev.atlaspds.net", i),
				PdsHost:       testPDSHost,
				CreatedAt:     timestamppb.New(time.Now()),
				PasswordHash:  fmt.Appendf(nil, "hash%d", i),
				SigningKey:    fmt.Appendf(nil, "key%d", i),
				RotationKeys:  [][]byte{fmt.Appendf(nil, "rotation%d", i)},
				RefreshTokens: []*types.RefreshToken{},
			}
			err := db.SaveActor(ctx, actor)
			require.NoError(t, err)
		}

		// fetch second page with cursor
		actors, nextCursor, err := db.ListActors(ctx, testPDSHost, prefix+"002", 2)
		require.NoError(t, err)
		require.Len(t, actors, 2)
		require.NotEmpty(t, nextCursor)
		require.Equal(t, prefix+"003", actors[0].Did)
		require.Equal(t, prefix+"004", actors[1].Did)
		require.Equal(t, prefix+"004", nextCursor)
	})

	t.Run("paginates correctly with last page", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()

		prefix := "did:plc:zzzzzlast"

		// create 5 actors
		for i := 1; i <= 5; i++ {
			actor := &types.Actor{
				Did:           fmt.Sprintf("%s%03d", prefix, i),
				Email:         fmt.Sprintf("zzlast%d@example.com", i),
				Handle:        fmt.Sprintf("zzlast%d.dev.atlaspds.net", i),
				PdsHost:       testPDSHost,
				CreatedAt:     timestamppb.New(time.Now()),
				PasswordHash:  fmt.Appendf(nil, "hash%d", i),
				SigningKey:    fmt.Appendf(nil, "key%d", i),
				RotationKeys:  [][]byte{fmt.Appendf(nil, "rotation%d", i)},
				RefreshTokens: []*types.RefreshToken{},
			}
			err := db.SaveActor(ctx, actor)
			require.NoError(t, err)
		}

		// fetch last page - should have our last actor
		actors, _, err := db.ListActors(ctx, testPDSHost, prefix+"004", 2)
		require.NoError(t, err)
		require.GreaterOrEqual(t, len(actors), 1)
		require.Equal(t, prefix+"005", actors[0].Did)
	})

	t.Run("works with limit of 1", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()

		prefix := "did:plc:zzzzzone"

		// create 3 actors
		for i := 1; i <= 3; i++ {
			actor := &types.Actor{
				Did:           fmt.Sprintf("%s%03d", prefix, i),
				Email:         fmt.Sprintf("zzone%d@example.com", i),
				Handle:        fmt.Sprintf("zzone%d.dev.atlaspds.net", i),
				PdsHost:       testPDSHost,
				CreatedAt:     timestamppb.New(time.Now()),
				PasswordHash:  fmt.Appendf(nil, "hash%d", i),
				SigningKey:    fmt.Appendf(nil, "key%d", i),
				RotationKeys:  [][]byte{fmt.Appendf(nil, "rotation%d", i)},
				RefreshTokens: []*types.RefreshToken{},
			}
			err := db.SaveActor(ctx, actor)
			require.NoError(t, err)
		}

		actors, nextCursor, err := db.ListActors(ctx, testPDSHost, prefix+"000", 1)
		require.NoError(t, err)
		require.Len(t, actors, 1)
		require.NotEmpty(t, nextCursor)
		require.Equal(t, prefix+"001", actors[0].Did)
		require.Equal(t, prefix+"001", nextCursor)
	})

	t.Run("actors are returned in lexicographic DID order", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()

		prefix := "did:plc:zzzzzord"

		// create actors with non-sequential suffixes
		dids := []string{prefix + "zzz", prefix + "aaa", prefix + "mmm"}
		for _, did := range dids {
			actor := &types.Actor{
				Did:           did,
				Email:         did + "@example.com",
				Handle:        did + ".dev.atlaspds.net",
				PdsHost:       testPDSHost,
				CreatedAt:     timestamppb.New(time.Now()),
				PasswordHash:  []byte("hash"),
				SigningKey:    []byte("key"),
				RotationKeys:  [][]byte{[]byte("rotation")},
				RefreshTokens: []*types.RefreshToken{},
			}
			err := db.SaveActor(ctx, actor)
			require.NoError(t, err)
		}

		actors, _, err := db.ListActors(ctx, testPDSHost, prefix+"000", 10)
		require.NoError(t, err)
		require.GreaterOrEqual(t, len(actors), 3)
		// verify our actors are returned in lexicographic order
		require.Equal(t, prefix+"aaa", actors[0].Did)
		require.Equal(t, prefix+"mmm", actors[1].Did)
		require.Equal(t, prefix+"zzz", actors[2].Did)
	})

	t.Run("handles cursor beyond last actor", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()

		prefix := "did:plc:zzzzzbeyond"

		// create actors
		for i := 1; i <= 3; i++ {
			actor := &types.Actor{
				Did:           fmt.Sprintf("%s%03d", prefix, i),
				Email:         fmt.Sprintf("zzbeyond%d@example.com", i),
				Handle:        fmt.Sprintf("zzbeyond%d.dev.atlaspds.net", i),
				PdsHost:       testPDSHost,
				CreatedAt:     timestamppb.New(time.Now()),
				PasswordHash:  fmt.Appendf(nil, "hash%d", i),
				SigningKey:    fmt.Appendf(nil, "key%d", i),
				RotationKeys:  [][]byte{fmt.Appendf(nil, "rotation%d", i)},
				RefreshTokens: []*types.RefreshToken{},
			}
			err := db.SaveActor(ctx, actor)
			require.NoError(t, err)
		}

		// use a cursor beyond our last actor
		actors, nextCursor, err := db.ListActors(ctx, testPDSHost, prefix+"999", 10)
		require.NoError(t, err)
		// no actors from our test set, but there could be other actors in the shared DB
		for _, a := range actors {
			require.NotContains(t, a.Did, prefix, "should not contain any of our test actors")
		}
		_ = nextCursor
	})

	t.Run("full pagination walkthrough", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()

		prefix := "did:plc:zzzzzwalk"

		// create 7 actors
		expectedDIDs := make([]string, 7)
		for i := 1; i <= 7; i++ {
			did := fmt.Sprintf("%s%03d", prefix, i)
			expectedDIDs[i-1] = did
			actor := &types.Actor{
				Did:           did,
				Email:         fmt.Sprintf("zzwalk%d@example.com", i),
				Handle:        fmt.Sprintf("zzwalk%d.dev.atlaspds.net", i),
				PdsHost:       testPDSHost,
				CreatedAt:     timestamppb.New(time.Now()),
				PasswordHash:  fmt.Appendf(nil, "hash%d", i),
				SigningKey:    fmt.Appendf(nil, "key%d", i),
				RotationKeys:  [][]byte{fmt.Appendf(nil, "rotation%d", i)},
				RefreshTokens: []*types.RefreshToken{},
			}
			err := db.SaveActor(ctx, actor)
			require.NoError(t, err)
		}

		var ourActors []*types.Actor
		cursor := prefix + "000" // start just before our actors
		pageSize := int64(3)

		// page 1: actors 0,1,2
		actors, nextCursor, err := db.ListActors(ctx, testPDSHost, cursor, pageSize)
		require.NoError(t, err)
		require.GreaterOrEqual(t, len(actors), 3)
		require.NotEmpty(t, nextCursor)
		// collect only our actors
		for _, a := range actors {
			if len(ourActors) < 3 && a.Did >= prefix+"001" && a.Did <= prefix+"999" {
				ourActors = append(ourActors, a)
			}
		}
		cursor = nextCursor

		// page 2: actors 3,4,5
		actors, nextCursor, err = db.ListActors(ctx, testPDSHost, cursor, pageSize)
		require.NoError(t, err)
		require.GreaterOrEqual(t, len(actors), 1)
		require.NotEmpty(t, nextCursor)
		// collect only our actors
		for _, a := range actors {
			if len(ourActors) < 6 && a.Did >= prefix+"001" && a.Did <= prefix+"999" {
				ourActors = append(ourActors, a)
			}
		}
		cursor = nextCursor

		// page 3: actor 6 (last page, only 1 actor from our set)
		actors, _, err = db.ListActors(ctx, testPDSHost, cursor, pageSize)
		require.NoError(t, err)
		// collect only our actors
		for _, a := range actors {
			if len(ourActors) < 7 && a.Did >= prefix+"001" && a.Did <= prefix+"999" {
				ourActors = append(ourActors, a)
			}
		}

		// verify we got all 7 actors in order
		require.Len(t, ourActors, 7)
		for i, actor := range ourActors {
			require.Equal(t, expectedDIDs[i], actor.Did)
		}
	})
}
