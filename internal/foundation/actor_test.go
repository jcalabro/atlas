package foundation

import (
	"context"
	"testing"
	"time"

	"github.com/jcalabro/atlas/internal/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"
)

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
			Handle:                "test.bsky.social",
			Active:                true,
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
			Handle:                "user1.bsky.social",
			Active:                true,
		}

		err := db.SaveActor(ctx, actor)
		require.NoError(t, err)

		// Update the actor
		actor.EmailConfirmed = true
		actor.EmailVerificationCode = ""
		actor.Handle = "updated.bsky.social"

		err = db.SaveActor(ctx, actor)
		require.NoError(t, err)

		// Verify the update persisted
		retrieved, err := db.GetActorByEmail(ctx, actor.Email)
		require.NoError(t, err)
		assert.Equal(t, true, retrieved.EmailConfirmed)
		assert.Equal(t, "", retrieved.EmailVerificationCode)
		assert.Equal(t, "updated.bsky.social", retrieved.Handle)
	})

	t.Run("handles actor with minimal fields", func(t *testing.T) {
		actor := &types.Actor{
			Did:   "did:plc:minimal",
			Email: "minimal@example.com",
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
			Handle:                "retrieve.bsky.social",
			Active:                true,
		}

		err := db.SaveActor(ctx, actor)
		require.NoError(t, err)

		retrieved, err := db.GetActorByEmail(ctx, "retrieve@example.com")
		require.NoError(t, err)
		require.NotNil(t, retrieved)

		assert.Equal(t, actor.Did, retrieved.Did)
		assert.Equal(t, actor.Email, retrieved.Email)
		assert.Equal(t, actor.EmailVerificationCode, retrieved.EmailVerificationCode)
		assert.Equal(t, actor.EmailConfirmed, retrieved.EmailConfirmed)
		assert.Equal(t, actor.PasswordHash, retrieved.PasswordHash)
		assert.Equal(t, actor.SigningKey, retrieved.SigningKey)
		assert.Equal(t, actor.Handle, retrieved.Handle)
		assert.Equal(t, actor.Active, retrieved.Active)
		assert.True(t, actor.CreatedAt.AsTime().Equal(retrieved.CreatedAt.AsTime()))
	})

	t.Run("returns empty actor for non-existent email", func(t *testing.T) {
		actor, err := db.GetActorByEmail(ctx, "nonexistent@example.com")
		require.NoError(t, err)
		// When email doesn't exist, we get an empty actor
		assert.Equal(t, "", actor.Did)
		assert.Equal(t, "", actor.Email)
	})

	t.Run("retrieves correct actor when multiple exist", func(t *testing.T) {
		actor1 := &types.Actor{
			Did:    "did:plc:multi1",
			Email:  "user1@example.com",
			Handle: "user1.bsky.social",
		}
		actor2 := &types.Actor{
			Did:    "did:plc:multi2",
			Email:  "user2@example.com",
			Handle: "user2.bsky.social",
		}
		actor3 := &types.Actor{
			Did:    "did:plc:multi3",
			Email:  "user3@example.com",
			Handle: "user3.bsky.social",
		}

		err := db.SaveActor(ctx, actor1)
		require.NoError(t, err)
		err = db.SaveActor(ctx, actor2)
		require.NoError(t, err)
		err = db.SaveActor(ctx, actor3)
		require.NoError(t, err)

		retrieved, err := db.GetActorByEmail(ctx, "user2@example.com")
		require.NoError(t, err)
		assert.Equal(t, "did:plc:multi2", retrieved.Did)
		assert.Equal(t, "user2.bsky.social", retrieved.Handle)
	})
}
