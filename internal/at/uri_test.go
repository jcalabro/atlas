package at

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseURI(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		uri          string
		expectedRepo string
		expectedColl string
		expectedRkey string
		expectedErr  string
	}{
		{
			name:         "valid URI with at:// prefix",
			uri:          "at://did:plc:test123/app.bsky.feed.post/3jui7kd2xs22b",
			expectedRepo: "did:plc:test123",
			expectedColl: "app.bsky.feed.post",
			expectedRkey: "3jui7kd2xs22b",
		},
		{
			name:         "valid URI without prefix",
			uri:          "did:plc:abc/app.bsky.graph.follow/xyz",
			expectedRepo: "did:plc:abc",
			expectedColl: "app.bsky.graph.follow",
			expectedRkey: "xyz",
		},
		{
			name:         "valid URI with did:web",
			uri:          "at://did:web:example.com/com.example.record/key123",
			expectedRepo: "did:web:example.com",
			expectedColl: "com.example.record",
			expectedRkey: "key123",
		},
		{
			name:        "not enough parts - missing rkey",
			uri:         "at://did:plc:test/app.bsky.feed.post",
			expectedErr: "not enough component parts",
		},
		{
			name:        "not enough parts - only repo",
			uri:         "at://did:plc:test",
			expectedErr: "not enough component parts",
		},
		{
			name:        "empty string",
			uri:         "",
			expectedErr: "not enough component parts",
		},
		{
			name:        "empty repo",
			uri:         "at:///app.bsky.feed.post/rkey",
			expectedErr: "repo must not be empty",
		},
		{
			name:        "empty collection",
			uri:         "at://did:plc:test//rkey",
			expectedErr: "collection must not be empty",
		},
		{
			name:        "empty rkey",
			uri:         "at://did:plc:test/app.bsky.feed.post/",
			expectedErr: "rkey must not be empty",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			uri, err := ParseURI(tt.uri)

			if tt.expectedErr != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tt.expectedErr)
				require.Nil(t, uri)
				return
			}

			require.NoError(t, err)
			require.NotNil(t, uri)
			require.Equal(t, tt.expectedRepo, uri.Repo)
			require.Equal(t, tt.expectedColl, uri.Collection)
			require.Equal(t, tt.expectedRkey, uri.Rkey)
		})
	}
}

func TestURI_String(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		uri      URI
		expected string
	}{
		{
			name: "basic URI",
			uri: URI{
				Repo:       "did:plc:test123",
				Collection: "app.bsky.feed.post",
				Rkey:       "3jui7kd2xs22b",
			},
			expected: "at://did:plc:test123/app.bsky.feed.post/3jui7kd2xs22b",
		},
		{
			name: "did:web repo",
			uri: URI{
				Repo:       "did:web:example.com",
				Collection: "com.example.record",
				Rkey:       "key123",
			},
			expected: "at://did:web:example.com/com.example.record/key123",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tt.expected, tt.uri.String())

			str := FormatURI(tt.uri.Repo, tt.uri.Collection, tt.uri.Rkey)
			require.Equal(t, tt.expected, str)
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
