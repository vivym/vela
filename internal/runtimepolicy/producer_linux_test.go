//go:build linux

package runtimepolicy

import (
	"context"
	"crypto/ed25519"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestAuthorizationSignerAndPublisherAreIdempotentAndImmutable(t *testing.T) {
	directory := t.TempDir()
	publicKey, privateKey, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	now := time.Now().UTC().Truncate(time.Microsecond)
	signer, err := NewAuthorizationSigner(privateKey, func() time.Time { return now })
	require.NoError(t, err)
	request := Request{Version: ProtocolVersion, OperationID: uuid.New(), JournalID: uuid.New(), RequestDigest: [32]byte{1}, ReservationDigest: [32]byte{2}}
	authorization, err := signer.Sign(context.Background(), request)
	require.NoError(t, err)
	require.NoError(t, signer.Publish(context.Background(), directory, authorization))
	require.NoError(t, signer.Publish(context.Background(), directory, authorization))
	path := AuthorizationPath(directory, request.OperationID)
	_, err = os.Stat(path)
	require.NoError(t, err)
	mutated := authorization
	mutated.RequestDigest[0]++
	require.Error(t, signer.Publish(context.Background(), directory, mutated))
	require.Equal(t, filepath.Join(directory, request.OperationID.String()+".json"), path)
	require.NoError(t, VerifyAuthorization(authorization, request, publicKey, now))
}

func TestAuthorizationSignerAtIsByteStable(t *testing.T) {
	_, privateKey, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	signer, err := NewAuthorizationSigner(privateKey, func() time.Time { return time.Now().UTC().Add(time.Hour) })
	require.NoError(t, err)
	request := Request{Version: ProtocolVersion, OperationID: uuid.New(), JournalID: uuid.New(), RequestDigest: [32]byte{11}, ReservationDigest: [32]byte{12}}
	reservedAt := time.Now().UTC().Truncate(time.Microsecond)
	first, err := signer.SignAt(context.Background(), request, reservedAt)
	require.NoError(t, err)
	second, err := signer.SignAt(context.Background(), request, reservedAt)
	require.NoError(t, err)
	require.Equal(t, first, second)
}

func TestFileReplyCacheRoundTripsSignedReply(t *testing.T) {
	directory := t.TempDir()
	publicKey, privateKey, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	cache, err := NewFileReplyCache(directory)
	require.NoError(t, err)
	now := time.Now().UTC().Truncate(time.Microsecond)
	request := Request{Version: ProtocolVersion, OperationID: uuid.New(), JournalID: uuid.New(), RequestDigest: [32]byte{3}, ReservationDigest: [32]byte{4}}
	reply := Reply{Version: ProtocolVersion, OperationID: request.OperationID, JournalID: request.JournalID, RequestDigest: request.RequestDigest, ReservationDigest: request.ReservationDigest, EvidenceDigest: [32]byte{5}, IssuedAt: now, ExpiresAt: now.Add(time.Minute)}
	wire, err := SigningBytes(reply)
	require.NoError(t, err)
	reply.Signature = ed25519.Sign(privateKey, wire)
	require.NoError(t, cache.Store(context.Background(), request, reply))
	got, found, err := cache.Load(context.Background(), request, publicKey)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, reply, got)
	request.RequestDigest[0]++
	_, found, err = cache.Load(context.Background(), request, publicKey)
	require.NoError(t, err)
	require.False(t, found)
}

func TestFileReplyCacheSurvivesIssuerRestartAndExpires(t *testing.T) {
	directory := t.TempDir()
	publicKey, privateKey, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	issued := time.Now().UTC().Truncate(time.Microsecond)
	nowUTC = func() time.Time { return issued }
	t.Cleanup(func() { nowUTC = func() time.Time { return time.Now().UTC() } })
	request := Request{Version: ProtocolVersion, OperationID: uuid.New(), JournalID: uuid.New(), RequestDigest: [32]byte{7}, ReservationDigest: [32]byte{8}}
	reply := Reply{Version: ProtocolVersion, OperationID: request.OperationID, JournalID: request.JournalID, RequestDigest: request.RequestDigest, ReservationDigest: request.ReservationDigest, EvidenceDigest: [32]byte{9}, IssuedAt: issued, ExpiresAt: issued.Add(time.Minute)}
	wire, err := SigningBytes(reply)
	require.NoError(t, err)
	reply.Signature = ed25519.Sign(privateKey, wire)
	first, err := NewFileReplyCache(directory)
	require.NoError(t, err)
	require.NoError(t, first.Store(context.Background(), request, reply))
	second, err := NewFileReplyCache(directory)
	require.NoError(t, err)
	got, found, err := second.Load(context.Background(), request, publicKey)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, reply, got)
	nowUTC = func() time.Time { return issued.Add(2 * time.Minute) }
	_, found, err = second.Load(context.Background(), request, publicKey)
	require.Error(t, err)
	require.False(t, found)
}
