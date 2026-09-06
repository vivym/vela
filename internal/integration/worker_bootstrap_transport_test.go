//go:build integration

package integration_test

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"database/sql"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/pressly/goose/v3"
	"github.com/vivym/vela/internal/fleet"
	"github.com/vivym/vela/internal/fleettransport"
	"github.com/vivym/vela/internal/nodeagent"
	"github.com/vivym/vela/internal/workerbootstrap"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

func TestWorkerBootstrapHistoryIsReadOnlyAndPrincipalScoped(t *testing.T) {
	database, service, request := newWorkerBootstrapFixture(t)
	lookup := fleet.WorkerBootstrapLookup{RequestID: request.RequestID, NodeIdentity: "h3-node-01", ActorIdentity: request.ActorIdentity}
	_, err := service.LookupWorkerBootstrap(t.Context(), lookup)
	assertFleetFailure(t, err, fleet.FailureNotFound)
	var count int
	if err := database.Admin.QueryRow("SELECT count(*) FROM worker_bootstrap_claims").Scan(&count); err != nil || count != 0 {
		t.Fatalf("missing history lookup consumed first use: %d %v", count, err)
	}
	claim, err := service.ClaimWorkerBootstrap(t.Context(), request)
	if err != nil || !claim.Fresh {
		t.Fatalf("claim: %+v %v", claim, err)
	}
	history, err := service.LookupWorkerBootstrap(t.Context(), lookup)
	claim.Fresh = false
	if err != nil || !reflect.DeepEqual(history.Claim, claim) || history.Receipt != nil || !history.RecordedAt.IsZero() {
		t.Fatalf("lookup changed claim or granted permission: %+v %v", history, err)
	}
	for _, wrong := range []fleet.WorkerBootstrapLookup{
		{RequestID: request.RequestID, NodeIdentity: "h3-node-02", ActorIdentity: request.ActorIdentity},
		{RequestID: request.RequestID, NodeIdentity: "h3-node-01", ActorIdentity: "other-actor"},
		{RequestID: uuid.New(), NodeIdentity: "h3-node-01", ActorIdentity: request.ActorIdentity},
	} {
		_, err := service.LookupWorkerBootstrap(t.Context(), wrong)
		assertFleetFailure(t, err, fleet.FailureNotFound)
	}
	tx, err := database.Admin.BeginTx(t.Context(), &sql.TxOptions{ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.QueryRow("SELECT count(*) FROM vela_lookup_worker_bootstrap($1, $2, $3)", lookup.RequestID, lookup.NodeIdentity, lookup.ActorIdentity).Scan(&count); err != nil || count != 1 {
		_ = tx.Rollback()
		t.Fatalf("history could not run in a read-only transaction: %d %v", count, err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	internal := newRolePool(t, database.DSN, "vela_internal_login", "vela-internal-password")
	if _, err := internal.Exec(t.Context(), "SELECT * FROM vela_lookup_worker_bootstrap($1, $2, $3)", lookup.RequestID, lookup.NodeIdentity, lookup.ActorIdentity); err == nil || !strings.Contains(err.Error(), "permission denied") {
		t.Fatalf("broad internal role could inspect bootstrap history: %v", err)
	}
	receipt := fleet.WorkerBootstrapReceipt{RequestID: request.RequestID, WorkerJournalID: uuid.New(), RuntimeJournalID: uuid.New(),
		WorkerScope: bytes.Repeat([]byte{1}, 32), RuntimeScope: bytes.Repeat([]byte{2}, 32), ActorIdentity: request.ActorIdentity}
	when, err := service.RecordWorkerBootstrapReceipt(t.Context(), receipt)
	if err != nil {
		t.Fatal(err)
	}
	history, err = service.LookupWorkerBootstrap(t.Context(), lookup)
	if err != nil || history.Receipt == nil || !reflect.DeepEqual(*history.Receipt, receipt) || !history.RecordedAt.Equal(when) || history.Claim.Fresh {
		t.Fatalf("receipt lookup: %+v %v", history, err)
	}
	migrations := filepath.Join(repositoryRoot(t), "db", "migrations")
	if err := goose.DownTo(database.Admin, migrations, 91); err != nil {
		t.Fatal(err)
	}
	if err := goose.UpTo(database.Admin, migrations, 92); err != nil {
		t.Fatal(err)
	}
	restored, err := service.LookupWorkerBootstrap(t.Context(), lookup)
	if err != nil || !reflect.DeepEqual(restored, history) {
		t.Fatalf("history reader migration changed immutable authority: %+v %v", restored, err)
	}
}

func TestWorkerBootstrapMutualTLSBindsNodeAndPreservesLostResponses(t *testing.T) {
	for _, lostMethod := range []string{"", velav1.FleetMaintenanceService_ClaimWorkerBootstrap_FullMethodName, velav1.FleetMaintenanceService_RecordWorkerBootstrapReceipt_FullMethodName} {
		t.Run("lost="+lostMethod, func(t *testing.T) {
			database, service, request := newWorkerBootstrapFixture(t)
			var dropped atomic.Bool
			var observedTLS atomic.Uint32
			clients := bootstrapMutualTLSClients(t, service, func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
				if p, ok := peer.FromContext(ctx); ok {
					if auth, ok := p.AuthInfo.(credentials.TLSInfo); ok {
						observedTLS.Store(uint32(auth.State.Version))
					}
				}
				response, err := handler(ctx, req)
				if err == nil && info.FullMethod == lostMethod && dropped.CompareAndSwap(false, true) {
					return nil, status.Error(codes.Unavailable, "committed response lost")
				}
				return response, err
			})
			claimRPC := &velav1.ClaimWorkerBootstrapRequest{RequestId: request.RequestID.String(), WorkerInstanceId: request.WorkerInstanceID.String(),
				WorkerInstanceEpoch: request.WorkerInstanceEpoch, WorkerMemberId: request.WorkerMemberID.String(), BundleManifest: request.BundleManifest}
			// Bypass local client validation to test the real server authority boundary.
			for _, attempt := range []struct {
				client int
				code   codes.Code
			}{{1, codes.PermissionDenied}, {3, codes.Unauthenticated}} {
				_, err := clients[attempt.client].rpc.ClaimWorkerBootstrap(t.Context(), claimRPC)
				if status.Code(err) != attempt.code {
					t.Fatalf("wrong-node or unregistered TLS caller: %v", err)
				}
			}
			var count int
			if err := database.Admin.QueryRow("SELECT count(*) FROM worker_bootstrap_claims").Scan(&count); err != nil || count != 0 {
				t.Fatalf("rejected TLS callers consumed permission: %d %v", count, err)
			}
			config := localBootstrapConfig(t, request)
			config.ActorIdentity = clients[0].bootstrap.ActorIdentity()
			result, err := workerbootstrap.Prepare(t.Context(), config, clients[0].bootstrap)
			if lostMethod == "" && err != nil || lostMethod != "" && (err == nil || result != (workerbootstrap.Result{}) || !dropped.Load()) {
				t.Fatalf("bootstrap result at response loss: %+v %v", result, err)
			}
			var operationID uuid.UUID
			if err := database.Admin.QueryRow("SELECT request_id FROM worker_bootstrap_claims WHERE worker_member_id = $1", request.WorkerMemberID).Scan(&operationID); err != nil {
				t.Fatal(err)
			}
			history, err := clients[0].bootstrap.LookupWorkerBootstrap(t.Context(), operationID)
			if err != nil || history.Claim.Fresh || history.Claim.RequestID != operationID || history.ActorIdentity != config.ActorIdentity || observedTLS.Load() != tls.VersionTLS13 {
				t.Fatalf("authenticated history: %+v %v TLS=%x", history, err, observedTLS.Load())
			}
			for _, other := range clients[1:3] {
				if _, err := other.bootstrap.LookupWorkerBootstrap(t.Context(), operationID); status.Code(err) != codes.NotFound {
					t.Fatalf("other principal read original operation: %v", err)
				}
				_, err := other.rpc.RecordWorkerBootstrapReceipt(t.Context(), &velav1.RecordWorkerBootstrapReceiptRequest{
					RequestId: operationID.String(), WorkerJournalId: uuid.NewString(), RuntimeJournalId: uuid.NewString(), WorkerScope: make([]byte, 32), RuntimeScope: make([]byte, 32)})
				if status.Code(err) != codes.NotFound {
					t.Fatalf("other principal reported original operation: %v", err)
				}
			}
			restarted, err := workerbootstrap.Prepare(t.Context(), config, clients[0].bootstrap)
			if lostMethod == velav1.FleetMaintenanceService_ClaimWorkerBootstrap_FullMethodName {
				if err == nil || restarted != (workerbootstrap.Result{}) || history.Receipt != nil {
					t.Fatalf("lost fresh grant reinitialized journals: %+v %v", restarted, err)
				}
				for _, name := range []string{"worker-admission", "runtime-admission", "inputs", "outputs"} {
					entries, err := os.ReadDir(filepath.Join(config.ScratchDirectory, name))
					if err != nil || len(entries) != 0 {
						t.Fatalf("lost claim initialized %s: %v", name, err)
					}
				}
			} else if err != nil || history.Receipt == nil || restarted.RequestID != operationID || restarted.Worker.JournalID != history.Receipt.WorkerJournalID ||
				restarted.Runtime.JournalID != history.Receipt.RuntimeJournalID || !restarted.RecordedAt.Equal(history.RecordedAt) {
				t.Fatalf("TLS receipt recovery changed journal identity: %+v %v", restarted, err)
			}
			// A direct RPC replay also returns history, never a second fresh grant.
			claimRPC.RequestId = operationID.String()
			replay, err := clients[0].rpc.ClaimWorkerBootstrap(t.Context(), claimRPC)
			if err != nil || replay.GetFresh() {
				t.Fatalf("direct TLS retry reissued first use: %+v %v", replay, err)
			}
		})
	}
}

type bootstrapTLSClient struct {
	bootstrap *fleettransport.BootstrapClient
	rpc       velav1.FleetMaintenanceServiceClient
}

func bootstrapMutualTLSClients(t *testing.T, service *fleet.Service, interceptor grpc.UnaryServerInterceptor) []bootstrapTLSClient {
	t.Helper()
	identities := []nodeagent.NodeAgentIdentity{
		{NodeIdentity: "h3-node-01", AgentID: uuid.New(), AgentEpoch: 1},
		{NodeIdentity: "h3-node-02", AgentID: uuid.New(), AgentEpoch: 1},
		{NodeIdentity: "h3-node-01", AgentID: uuid.New(), AgentEpoch: 1},
		{NodeIdentity: "h3-node-01", AgentID: uuid.New(), AgentEpoch: 1},
	}
	var registrations []fleettransport.NodeAgentRegistration
	for _, identity := range identities[:3] {
		registrations = append(registrations, fleettransport.NodeAgentRegistration{
			NodeIdentity: identity.NodeIdentity, AgentID: identity.AgentID, SPIFFEIdentity: nodeagent.NodeAgentSPIFFEIdentity(identity)})
	}
	server, err := fleettransport.NewServer(service, fleettransport.Config{SPIFFEIdentity: "spiffe://vela.internal/fleet-controller/primary",
		ActorIdentity: "fleet/primary", NodeAgentRegistrations: registrations, BootstrapService: service})
	if err != nil {
		t.Fatal(err)
	}
	ca, caKey, caPEM := issueWorkerTransportTestCA(t)
	serverName := "bootstrap.fleet.internal"
	cert, key := issueWorkerTransportTestCertificate(t, ca, caKey, pkix.Name{CommonName: serverName}, []string{serverName}, nil, []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth})
	root := t.TempDir()
	write := func(name string, value []byte) string {
		t.Helper()
		path := filepath.Join(root, name)
		if err := os.WriteFile(path, value, 0o600); err != nil {
			t.Fatal(err)
		}
		return path
	}
	caPath := write("ca.pem", caPEM)
	serverTLS, err := fleettransport.NewServerTLSCredentials(write("server.pem", cert), write("server.key", key), caPath)
	if err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	grpcServer := grpc.NewServer(grpc.Creds(serverTLS), grpc.UnaryInterceptor(interceptor), grpc.MaxRecvMsgSize(fleettransport.MaximumMessageBytes))
	velav1.RegisterFleetMaintenanceServiceServer(grpcServer, server)
	done := make(chan error, 1)
	go func() { done <- grpcServer.Serve(listener) }()
	t.Cleanup(func() {
		grpcServer.Stop()
		_ = listener.Close()
		<-done
	})
	var clients []bootstrapTLSClient
	for _, identity := range identities {
		spiffe := nodeagent.NodeAgentSPIFFEIdentity(identity)
		cert, key := issueWorkerTransportTestCertificate(t, ca, caKey, pkix.Name{CommonName: "node-agent"}, nil,
			[]*url.URL{mustParseWorkerSPIFFEID(t, spiffe)}, []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth})
		prefix := identity.AgentID.String()
		clientTLS, err := fleettransport.NewClientTLSCredentials(write(prefix+".pem", cert), write(prefix+".key", key), caPath, serverName)
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		base, err := fleettransport.DialClient(ctx, listener.Addr().String(), clientTLS)
		cancel()
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = base.Close() })
		bootstrap, err := base.WorkerBootstrap(spiffe)
		if err != nil {
			t.Fatal(err)
		}
		connection, err := grpc.NewClient(listener.Addr().String(), grpc.WithTransportCredentials(clientTLS))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = connection.Close() })
		clients = append(clients, bootstrapTLSClient{bootstrap: bootstrap, rpc: velav1.NewFleetMaintenanceServiceClient(connection)})
	}
	return clients
}
