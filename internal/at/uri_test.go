package at

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseURI(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		uri      string
		wantRepo string
		wantColl string
		wantRkey string
		wantErr  string
	}{
		{
			name:     "valid URI with at:// prefix",
			uri:      "at://did:plc:test123/app.bsky.feed.post/3jui7kd2xs22b",
			wantRepo: "did:plc:test123",
			wantColl: "app.bsky.feed.post",
			wantRkey: "3jui7kd2xs22b",
		},
		{
			name:     "valid URI without prefix",
			uri:      "did:plc:abc/app.bsky.graph.follow/xyz",
			wantRepo: "did:plc:abc",
			wantColl: "app.bsky.graph.follow",
			wantRkey: "xyz",
		},
		{
			name:     "valid URI with did:web",
			uri:      "at://did:web:example.com/com.example.record/key123",
			wantRepo: "did:web:example.com",
			wantColl: "com.example.record",
			wantRkey: "key123",
		},
		{
			name:    "not enough parts - missing rkey",
			uri:     "at://did:plc:test/app.bsky.feed.post",
			wantErr: "not enough component parts",
		},
		{
			name:    "not enough parts - only repo",
			uri:     "at://did:plc:test",
			wantErr: "not enough component parts",
		},
		{
			name:    "empty string",
			uri:     "",
			wantErr: "not enough component parts",
		},
		{
			name:    "empty repo",
			uri:     "at:///app.bsky.feed.post/rkey",
			wantErr: "repo must not be empty",
		},
		{
			name:    "empty collection",
			uri:     "at://did:plc:test//rkey",
			wantErr: "collection must not be empty",
		},
		{
			name:    "empty rkey",
			uri:     "at://did:plc:test/app.bsky.feed.post/",
			wantErr: "rkey must not be empty",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			uri, err := ParseURI(tt.uri)

			if tt.wantErr != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tt.wantErr)
				require.Nil(t, uri)
				return
			}

			require.NoError(t, err)
			require.NotNil(t, uri)
			require.Equal(t, tt.wantRepo, uri.Repo)
			require.Equal(t, tt.wantColl, uri.Collection)
			require.Equal(t, tt.wantRkey, uri.Rkey)
		})
	}
}

func TestURI_String(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		uri  URI
		want string
	}{
		{
			name: "basic URI",
			uri: URI{
				Repo:       "did:plc:test123",
				Collection: "app.bsky.feed.post",
				Rkey:       "3jui7kd2xs22b",
			},
			want: "at://did:plc:test123/app.bsky.feed.post/3jui7kd2xs22b",
		},
		{
			name: "did:web repo",
			uri: URI{
				Repo:       "did:web:example.com",
				Collection: "com.example.record",
				Rkey:       "key123",
			},
			want: "at://did:web:example.com/com.example.record/key123",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tt.want, tt.uri.String())
		})
	}
}

func TestParseURI_RoundTrip(t *testing.T) {
	t.Parallel()

	original := "at://did:plc:test123/app.bsky.feed.post/3jui7kd2xs22b"

	uri, err := ParseURI(original)
	require.NoError(t, err)

	roundTripped := uri.String()
	require.Equal(t, original, roundTripped)
}
