//go:build integration

package integration_test

import (
	"context"
	"crypto/tls"
	"reflect"
	"sync/atomic"
	"testing"

	"github.com/vivym/vela/internal/fleet"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func TestRuntimeStartupMutualTLSReservationPreservesLostResponse(t *testing.T) {
	for _, loseResponse := range []bool{false, true} {
		t.Run(map[bool]string{false: "normal", true: "committed-response-lost"}[loseResponse], func(t *testing.T) {
			database, service, bootstrap := newWorkerBootstrapFixture(t)
			var dropped atomic.Bool
			var committedCalls atomic.Int32
			var tlsVersion atomic.Uint32
			clients := bootstrapMutualTLSClients(t, service, func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
				if p, ok := peer.FromContext(ctx); ok {
					if auth, ok := p.AuthInfo.(credentials.TLSInfo); ok {
						tlsVersion.Store(uint32(auth.State.Version))
					}
				}
				result, err := handler(ctx, req)
				if err == nil && info.FullMethod == velav1.FleetMaintenanceService_ReserveRuntimeStartup_FullMethodName {
					committedCalls.Add(1)
					if loseResponse && dropped.CompareAndSwap(false, true) {
						return nil, status.Error(codes.Unavailable, "committed startup reservation response lost")
					}
				}
				return result, err
			})
			// Real immutable Registry rows, with fixture journal/owner observations.
			// The production reservation call below derives identity solely from TLS.
			bootstrap.ActorIdentity = clients[0].bootstrap.ActorIdentity()
			request := runtimeStartupRequest(t, service, bootstrap)
			wire := &velav1.ReserveRuntimeStartupRequest{RequestId: request.RequestID.String(), BootstrapRequestId: request.BootstrapRequestID.String(),
				RuntimeJournalId: request.RuntimeJournalID.String(), RuntimeScope: request.RuntimeScope, IncarnationId: request.IncarnationID.String(),
				LaunchDigest: request.LaunchDigest, OwnerObservationDigest: request.OwnerObservationDigest}
			for _, epoch := range request.Epochs {
				wire.Epochs = append(wire.Epochs, &velav1.RuntimeStartupEpoch{ModelResidencyId: epoch.ModelResidencyID.String(),
					ModelRuntimeIdentity: epoch.ModelRuntimeIdentity, StageProfileRevisionId: epoch.StageProfileRevisionID.String(), ModelRuntimeEpoch: epoch.ModelRuntimeEpoch})
			}
			for index, code := range map[int]codes.Code{1: codes.NotFound, 2: codes.NotFound, 3: codes.Unauthenticated} {
				if _, err := clients[index].rpc.ReserveRuntimeStartup(t.Context(), wire); status.Code(err) != code {
					t.Fatalf("other-node/other-agent/unregistered caller %d: %v", index, err)
				}
			}
			unknown := proto.CloneOf(wire)
			unknown.ProtoReflect().SetUnknown([]byte{0x48, 1})
			if _, err := clients[0].rpc.ReserveRuntimeStartup(t.Context(), unknown); status.Code(err) != codes.InvalidArgument {
				t.Fatalf("caller-supplied extension accepted: %v", err)
			}
			badEpoch := proto.CloneOf(wire)
			badEpoch.Epochs[0].ModelRuntimeEpoch++
			if _, err := clients[0].rpc.ReserveRuntimeStartup(t.Context(), badEpoch); status.Code(err) != codes.Aborted {
				t.Fatalf("unapproved epoch accepted: %v", err)
			}
			var count int
			if err := database.Admin.QueryRow("SELECT count(*) FROM runtime_startup_reservations").Scan(&count); err != nil || count != 0 {
				t.Fatalf("rejected callers consumed startup: %d %v", count, err)
			}
			result, err := clients[0].bootstrap.ReserveRuntimeStartup(t.Context(), request)
			if loseResponse {
				if status.Code(err) != codes.Unavailable || !dropped.Load() || !reflect.DeepEqual(result, fleet.RuntimeStartupReservation{}) {
					t.Fatalf("lost committed response exposed permission: %+v %v", result, err)
				}
			} else if err != nil || !result.Fresh || !reflect.DeepEqual(result.RuntimeStartupRequest, request) {
				t.Fatalf("authenticated startup reservation: %+v %v", result, err)
			}
			if committedCalls.Load() != 1 || tlsVersion.Load() != tls.VersionTLS13 {
				t.Fatalf("client retried or wrong TLS version: calls=%d TLS=%x", committedCalls.Load(), tlsVersion.Load())
			}
			if err := database.Admin.QueryRow("SELECT count(*) FROM runtime_startup_reservations").Scan(&count); err != nil || count != 1 {
				t.Fatalf("committed reservation missing or duplicated: %d %v", count, err)
			}
			history, err := clients[0].bootstrap.LookupRuntimeStartup(t.Context(), request.RequestID)
			if err != nil || history.Fresh || !reflect.DeepEqual(history.RuntimeStartupRequest, request) {
				t.Fatalf("history changed identity or minted permission: %+v %v", history, err)
			}
			again, err := clients[0].bootstrap.ReserveRuntimeStartup(t.Context(), request)
			if err != nil || again.Fresh || !reflect.DeepEqual(again, history) {
				t.Fatalf("retry reissued first-use authority: %+v %v", again, err)
			}
			for index, code := range map[int]codes.Code{1: codes.NotFound, 2: codes.NotFound, 3: codes.Unauthenticated} {
				if _, err := clients[index].bootstrap.LookupRuntimeStartup(t.Context(), request.RequestID); status.Code(err) != code {
					t.Fatalf("other principal read history %d: %v", index, err)
				}
			}
			changed := request
			changed.OwnerObservationDigest = sha256Bytes([]byte("replacement owner"))
			if _, err := clients[0].bootstrap.ReserveRuntimeStartup(t.Context(), changed); status.Code(err) != codes.Aborted {
				t.Fatalf("replacement owner reused committed request: %v", err)
			}
			t.Logf("TLS1.3: lost=%v, one durable reservation, exact history/retry carry no Fresh, other principals cannot reserve/read", loseResponse)
		})
	}
}
