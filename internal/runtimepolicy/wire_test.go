package runtimepolicy

import (
	"crypto/ed25519"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestReplyIsOperationAndReservationBoundAndSigned(t *testing.T) {
	publicKey, privateKey, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	request := Request{Version: ProtocolVersion, OperationID: uuid.New(), JournalID: uuid.New(), RequestDigest: [32]byte{1}, ReservationDigest: [32]byte{2}}
	issued := time.Now().UTC()
	reply := Reply{Version: ProtocolVersion, OperationID: request.OperationID, JournalID: request.JournalID, RequestDigest: request.RequestDigest, ReservationDigest: request.ReservationDigest, EvidenceDigest: [32]byte{3}, IssuedAt: issued, ExpiresAt: issued.Add(time.Minute)}
	wire, err := SigningBytes(reply)
	require.NoError(t, err)
	reply.Signature = ed25519.Sign(privateKey, wire)
	require.NoError(t, VerifyReply(reply, request, publicKey, issued))
	reply.RequestDigest[0]++
	require.Error(t, VerifyReply(reply, request, publicKey, issued))
}

func TestRequestAndReplyRejectMissingReservationBinding(t *testing.T) {
	request := Request{Version: ProtocolVersion, OperationID: uuid.New(), JournalID: uuid.New(), RequestDigest: [32]byte{1}}
	require.False(t, request.Valid())
}

func TestAuthorizationIsIndependentAndBoundToExactRequest(t *testing.T) {
	publicKey, privateKey, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	request := Request{Version: ProtocolVersion, OperationID: uuid.New(), JournalID: uuid.New(), RequestDigest: [32]byte{4}, ReservationDigest: [32]byte{5}}
	issued := time.Now().UTC()
	authorization := Authorization{Version: ProtocolVersion, OperationID: request.OperationID, JournalID: request.JournalID, RequestDigest: request.RequestDigest, ReservationDigest: request.ReservationDigest, IssuedAt: issued, ExpiresAt: issued.Add(time.Minute)}
	wire, err := AuthorizationSigningBytes(authorization)
	require.NoError(t, err)
	authorization.Signature = ed25519.Sign(privateKey, wire)
	require.NoError(t, VerifyAuthorization(authorization, request, publicKey, issued))
	authorization.ReservationDigest[0]++
	require.Error(t, VerifyAuthorization(authorization, request, publicKey, issued))
}

func TestAuthorizationRejectsLongOrFutureWindow(t *testing.T) {
	request := Request{Version: ProtocolVersion, OperationID: uuid.New(), JournalID: uuid.New(), RequestDigest: [32]byte{6}, ReservationDigest: [32]byte{7}}
	now := time.Now().UTC()
	for _, authorization := range []Authorization{
		{Version: ProtocolVersion, OperationID: request.OperationID, JournalID: request.JournalID, RequestDigest: request.RequestDigest, ReservationDigest: request.ReservationDigest, IssuedAt: now, ExpiresAt: now.Add(6 * time.Minute)},
		{Version: ProtocolVersion, OperationID: request.OperationID, JournalID: request.JournalID, RequestDigest: request.RequestDigest, ReservationDigest: request.ReservationDigest, IssuedAt: now.Add(2 * time.Second), ExpiresAt: now.Add(3 * time.Minute)},
	} {
		require.False(t, authorization.ValidFor(request, now))
	}
}

func TestReservationBindingDigestIsStableAcrossNodeReceiptTime(t *testing.T) {
	operationID, journalID := uuid.New(), uuid.New()
	requestDigest := [32]byte{9}
	reservedAt := time.Now().UTC().Truncate(time.Microsecond)
	first, err := ReservationBindingDigest(operationID, journalID, requestDigest, reservedAt)
	require.NoError(t, err)
	second, err := ReservationBindingDigest(operationID, journalID, requestDigest, reservedAt)
	require.NoError(t, err)
	require.Equal(t, first, second)
	changed, err := ReservationBindingDigest(operationID, journalID, requestDigest, reservedAt.Add(time.Nanosecond))
	require.NoError(t, err)
	require.NotEqual(t, first, changed)
}
