//go:build integration

package integration_test

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pressly/goose/v3"
	"github.com/stretchr/testify/require"
	"github.com/vivym/vela/internal/fleet"
	"github.com/vivym/vela/internal/runtimepolicy"
)

type startupAuthorizationProducerFunc func(context.Context, runtimepolicy.Request) ([]byte, error)

func (produce startupAuthorizationProducerFunc) Produce(ctx context.Context, request runtimepolicy.Request) ([]byte, error) {
	return produce(ctx, request)
}

func newStartupAuthorizationSigner(t *testing.T) (*fleet.SignedRuntimeStartupAuthorizationProducer, ed25519.PublicKey) {
	t.Helper()
	publicKey, privateKey, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	signer, err := fleet.NewSignedRuntimeStartupAuthorizationProducer(privateKey, nil)
	require.NoError(t, err)
	return signer, publicKey
}

func verifyStartupAuthorization(t *testing.T, result fleet.RuntimeStartupReservation, publicKey ed25519.PublicKey) {
	t.Helper()
	var authorization runtimepolicy.Authorization
	require.NoError(t, json.Unmarshal(result.PolicyAuthorization, &authorization))
	wire, err := json.Marshal(result.RuntimeStartupRequest)
	require.NoError(t, err)
	requestDigest := sha256.Sum256(wire)
	reservationDigest, err := runtimepolicy.ReservationBindingDigest(result.RequestID, result.RuntimeJournalID, requestDigest, result.ReservedAt)
	require.NoError(t, err)
	request := runtimepolicy.Request{Version: runtimepolicy.ProtocolVersion, OperationID: result.RequestID,
		JournalID: result.RuntimeJournalID, RequestDigest: requestDigest, ReservationDigest: reservationDigest}
	require.NoError(t, runtimepolicy.VerifyAuthorization(authorization, request, publicKey, time.Now().UTC()))
	require.True(t, authorization.IssuedAt.Equal(result.ReservedAt), "issuance must use the committed reservation time")
}

func TestRuntimeStartupAuthorizationPersistsAcrossFleetRestart(t *testing.T) {
	database, service, bootstrap := newWorkerBootstrapFixture(t)
	request := runtimeStartupRequest(t, service, bootstrap)
	signer, publicKey := newStartupAuthorizationSigner(t)
	require.NoError(t, service.SetRuntimeStartupAuthorizationSource(signer))
	first, err := service.ReserveRuntimeStartup(t.Context(), request)
	require.NoError(t, err)
	require.True(t, first.Fresh)
	verifyStartupAuthorization(t, first, publicKey)
	var stored []byte
	require.NoError(t, database.Admin.QueryRow("SELECT authorization_bytes FROM runtime_startup_authorizations WHERE request_id=$1", request.RequestID).Scan(&stored))
	require.Equal(t, first.PolicyAuthorization, stored)

	restarted, err := fleet.NewService(newRolePool(t, database.DSN, "vela_fleet_login", "vela-fleet-password"))
	require.NoError(t, err)
	var calls atomic.Int32
	require.NoError(t, restarted.SetRuntimeStartupAuthorizationSource(startupAuthorizationProducerFunc(func(context.Context, runtimepolicy.Request) ([]byte, error) {
		calls.Add(1)
		return nil, errors.New("signer unavailable after restart")
	})))
	replay, err := restarted.ReserveRuntimeStartup(t.Context(), request)
	require.NoError(t, err)
	require.False(t, replay.Fresh)
	require.Equal(t, stored, replay.PolicyAuthorization)
	history, err := restarted.LookupRuntimeStartup(t.Context(), fleet.WorkerBootstrapLookup{
		RequestID: request.RequestID, NodeIdentity: request.NodeIdentity, ActorIdentity: request.ActorIdentity})
	require.NoError(t, err)
	require.False(t, history.Fresh)
	require.Equal(t, stored, history.PolicyAuthorization)
	require.Zero(t, calls.Load(), "history must use durable bytes without invoking the signer")

	for _, mutation := range []string{
		"UPDATE runtime_startup_authorizations SET authorization_bytes=decode('ff','hex')",
		"DELETE FROM runtime_startup_authorizations",
		"TRUNCATE runtime_startup_authorizations",
	} {
		_, err := database.Admin.Exec(mutation)
		require.ErrorContains(t, err, "immutable")
	}
	role := newRolePool(t, database.DSN, "vela_fleet_login", "vela-fleet-password")
	require.NoError(t, role.QueryRow(t.Context(), "SELECT vela_store_runtime_startup_authorization($1,$2)", request.RequestID, []byte("replacement")).Scan(&stored))
	require.True(t, bytes.Equal(first.PolicyAuthorization, stored), "a second store must retain the first wire")
	_, err = role.Exec(t.Context(), "SELECT * FROM runtime_startup_authorizations")
	require.ErrorContains(t, err, "permission denied")
	require.ErrorContains(t, goose.DownTo(database.Admin, filepath.Join(repositoryRoot(t), "db", "migrations"), 95), "authorization history prohibits rollback")
}

func TestRuntimeStartupAuthorizationRecoversReservationWithoutAuthorization(t *testing.T) {
	database, service, bootstrap := newWorkerBootstrapFixture(t)
	request := runtimeStartupRequest(t, service, bootstrap)
	signer, publicKey := newStartupAuthorizationSigner(t)
	require.NoError(t, service.SetRuntimeStartupAuthorizationSource(startupAuthorizationProducerFunc(func(context.Context, runtimepolicy.Request) ([]byte, error) {
		return nil, errors.New("injected failure after reservation commit")
	})))
	first, err := service.ReserveRuntimeStartup(t.Context(), request)
	require.ErrorContains(t, err, "injected failure")
	require.Equal(t, fleet.RuntimeStartupReservation{}, first)
	var reservations, authorizations int
	require.NoError(t, database.Admin.QueryRow("SELECT (SELECT count(*) FROM runtime_startup_reservations), (SELECT count(*) FROM runtime_startup_authorizations)").Scan(&reservations, &authorizations))
	require.Equal(t, 1, reservations)
	require.Zero(t, authorizations)

	unstable, err := fleet.NewService(newRolePool(t, database.DSN, "vela_fleet_login", "vela-fleet-password"))
	require.NoError(t, err)
	var calls atomic.Int32
	require.NoError(t, unstable.SetRuntimeStartupAuthorizationSource(startupAuthorizationProducerFunc(func(context.Context, runtimepolicy.Request) ([]byte, error) {
		calls.Add(1)
		return []byte("freshly issued bytes"), nil
	})))
	_, err = unstable.ReserveRuntimeStartup(t.Context(), request)
	require.Error(t, err)
	require.Zero(t, calls.Load(), "recovery must not mint a new validity window")

	restarted, err := fleet.NewService(newRolePool(t, database.DSN, "vela_fleet_login", "vela-fleet-password"))
	require.NoError(t, err)
	require.NoError(t, restarted.SetRuntimeStartupAuthorizationSource(signer))
	history, err := restarted.LookupRuntimeStartup(t.Context(), fleet.WorkerBootstrapLookup{
		RequestID: request.RequestID, NodeIdentity: request.NodeIdentity, ActorIdentity: request.ActorIdentity})
	require.NoError(t, err)
	require.False(t, history.Fresh)
	require.Empty(t, history.PolicyAuthorization, "read-only history cannot create missing authorization")
	recovered, err := restarted.ReserveRuntimeStartup(t.Context(), request)
	require.NoError(t, err)
	require.False(t, recovered.Fresh, "recovery must never renew first-use permission")
	require.Equal(t, history.ReservedAt.UTC(), recovered.ReservedAt)
	verifyStartupAuthorization(t, recovered, publicKey)
	replay, err := restarted.ReserveRuntimeStartup(t.Context(), request)
	require.NoError(t, err)
	require.Equal(t, recovered, replay)
}

func TestRuntimeStartupAuthorizationRequiresCommittedQuorum(t *testing.T) {
	database, service, bootstrap := newWorkerBootstrapFixture(t)
	request := runtimeStartupRequest(t, service, bootstrap)
	// Establish a committed reservation before the signing phase is available.
	reservation, err := service.ReserveRuntimeStartup(t.Context(), request)
	require.NoError(t, err)
	require.True(t, reservation.Fresh)
	enableStageQuorumRequirement(t, database)
	guarded, err := fleet.NewService(newRolePool(t, database.DSN, "vela_fleet_login", "vela-fleet-password"))
	require.NoError(t, err)
	signer, _ := newStartupAuthorizationSigner(t)
	require.NoError(t, guarded.SetRuntimeStartupAuthorizationSource(signer))
	result, err := guarded.ReserveRuntimeStartup(t.Context(), request)
	require.ErrorContains(t, err, "synchronous replication quorum is unavailable")
	require.Equal(t, fleet.RuntimeStartupReservation{}, result)
	var count int
	require.NoError(t, database.Admin.QueryRow("SELECT count(*) FROM runtime_startup_authorizations").Scan(&count))
	require.Zero(t, count, "a failed authorization commit must expose no bytes or durable row")
}
