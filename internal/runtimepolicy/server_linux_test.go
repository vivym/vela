//go:build linux

package runtimepolicy

import (
	"context"
	"crypto/ed25519"
	"encoding/json"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

type approvingAuthorizer struct{}

func (approvingAuthorizer) Authorize(context.Context, Request) (time.Time, error) {
	return time.Now().UTC().Add(time.Minute), nil
}

func TestServerSignsOnlyApprovedRequest(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("root peer authentication test requires root")
	}
	directory := t.TempDir()
	_ = os.Chmod(directory, 0o700)
	path := filepath.Join(directory, "issuer.sock")
	listener, err := net.ListenUnix("unixpacket", &net.UnixAddr{Name: path, Net: "unixpacket"})
	require.NoError(t, err)
	publicKey, privateKey, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	now := time.Now().UTC()
	server := &Server{Listener: listener, PrivateKey: privateKey, Authorizer: approvingAuthorizer{}, Now: func() time.Time { return now }}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- server.Serve(ctx) }()
	connection, err := net.DialUnix("unixpacket", nil, &net.UnixAddr{Name: path, Net: "unixpacket"})
	require.NoError(t, err)
	defer connection.Close()
	request := Request{Version: ProtocolVersion, OperationID: uuid.New(), JournalID: uuid.New(), RequestDigest: [32]byte{8}, ReservationDigest: [32]byte{9}}
	wire, err := json.Marshal(request)
	require.NoError(t, err)
	_, err = connection.Write(wire)
	require.NoError(t, err)
	buffer := make([]byte, 64<<10)
	n, err := connection.Read(buffer)
	require.NoError(t, err)
	var reply Reply
	require.NoError(t, json.Unmarshal(buffer[:n], &reply))
	require.NoError(t, VerifyReply(reply, request, publicKey, now))
	cancel()
	require.Eventually(t, func() bool {
		select {
		case <-done:
			return true
		default:
			return false
		}
	}, time.Second, time.Millisecond)
}
