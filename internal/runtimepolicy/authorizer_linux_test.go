//go:build linux

package runtimepolicy

import (
	"context"
	"crypto/ed25519"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestFileAuthorizerConsumesFleetAuthorizationOnce(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("securefile authorizer test requires root-owned fixture")
	}
	directory := t.TempDir()
	_ = os.Chmod(directory, 0o700)
	publicKey, privateKey, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	request := Request{Version: ProtocolVersion, OperationID: uuid.New(), JournalID: uuid.New(), RequestDigest: [32]byte{1}, ReservationDigest: [32]byte{2}}
	now := time.Now().UTC()
	authorization := Authorization{Version: ProtocolVersion, OperationID: request.OperationID, JournalID: request.JournalID, RequestDigest: request.RequestDigest, ReservationDigest: request.ReservationDigest, IssuedAt: now, ExpiresAt: now.Add(time.Minute)}
	wire, err := AuthorizationSigningBytes(authorization)
	require.NoError(t, err)
	authorization.Signature = ed25519.Sign(privateKey, wire)
	encoded, err := json.Marshal(authorization)
	require.NoError(t, err)
	path := filepath.Join(directory, request.OperationID.String()+".json")
	require.NoError(t, os.WriteFile(path, encoded, 0o600))
	authorizer, err := NewFileAuthorizer(directory, publicKey)
	require.NoError(t, err)
	nowUTC = func() time.Time { return now }
	t.Cleanup(func() { nowUTC = func() time.Time { return time.Now().UTC() } })
	_, err = authorizer.Authorize(context.Background(), request)
	require.NoError(t, err)
	_, err = authorizer.Authorize(context.Background(), request)
	require.ErrorContains(t, err, "already consumed")
}
