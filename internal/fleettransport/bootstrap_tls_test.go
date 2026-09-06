package fleettransport

import (
	"context"
	"crypto/sha256"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/fleet"
	"github.com/vivym/vela/internal/fleetcontroller"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/grpc"
)

func TestBootstrapLargeBundleUsesAuthenticatedBoundedTransport(t *testing.T) {
	request, identity := bootstrapTransportFixture(t)
	bundle, err := fleetcontroller.ParseWorkerBundleActuationManifest(request.BundleManifest)
	mustBootstrap(t, err)
	runtime := &bundle.WorkerInstances[0].ModelRuntimes[0]
	for i := range 127 {
		runtime.Command = append(runtime.Command, strings.Repeat(string(rune('a'+i%26)), 4096))
	}
	for i := range 64 {
		runtime.Environment = append(runtime.Environment, fmt.Sprintf("VAR%03d=", i)+strings.Repeat("x", 4089))
	}
	encoded, err := json.Marshal(bundle.WorkerInstances[0])
	mustBootstrap(t, err)
	var second fleetcontroller.WorkerInstanceActuation
	mustBootstrap(t, json.Unmarshal(encoded, &second))
	second.ID, second.Members[0].ID, second.Members[0].DeviceConstraints[0].DeviceID = uuid.New(), uuid.New(), uuid.New()
	memberDigest := sha256.Sum256([]byte("spiffe://vela.internal/stage-worker/" + second.Members[0].ID.String()))
	second.Members[0].IdentityDigest = hex.EncodeToString(memberDigest[:])
	second.ModelRuntimes[0].ModelResidencyID = uuid.New()
	bundle.WorkerInstances = append(bundle.WorkerInstances, second)
	bundle.RevisionDigest, err = fleetcontroller.ComputeWorkerBundleActuationDigest(bundle)
	mustBootstrap(t, err)
	request.BundleManifest, err = fleetcontroller.WorkerBundleActuationManifest(bundle)
	mustBootstrap(t, err)
	if len(request.BundleManifest) <= maximumWorkerRegistryPayloadBytes || len(request.BundleManifest) > fleet.MaximumWorkerBootstrapManifestBytes {
		t.Fatalf("fixture does not exceed ordinary Fleet limit: %d", len(request.BundleManifest))
	}
	service := &bootstrapServiceStub{}
	principal, _ := parseNodeAgentSPIFFEIdentity(identity)
	server, err := NewServer(service, Config{SPIFFEIdentity: fleetSPIFFE, ActorIdentity: "fleet/primary", BootstrapService: service,
		NodeAgentRegistrations: []NodeAgentRegistration{{NodeIdentity: principal.NodeIdentity, AgentID: principal.AgentID, SPIFFEIdentity: identity}}})
	mustBootstrap(t, err)
	ca, caKey, caPEM := issueFleetTestCA(t)
	serverName := "bootstrap.fleet.internal"
	serverCert, serverKey := issueFleetTestCertificate(t, ca, caKey, pkix.Name{CommonName: serverName}, []string{serverName}, nil, []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth})
	uri, err := url.Parse(identity)
	mustBootstrap(t, err)
	clientCert, clientKey := issueFleetTestCertificate(t, ca, caKey, pkix.Name{CommonName: "node-agent"}, nil, []*url.URL{uri}, []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth})
	directory := t.TempDir()
	write := func(name string, value []byte) string {
		t.Helper()
		path := filepath.Join(directory, name)
		mustBootstrap(t, os.WriteFile(path, value, 0o600))
		return path
	}
	caPath := write("ca.pem", caPEM)
	serverTLS, err := NewServerTLSCredentials(write("server.pem", serverCert), write("server.key", serverKey), caPath)
	mustBootstrap(t, err)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	mustBootstrap(t, err)
	rpcServer := grpc.NewServer(grpc.Creds(serverTLS), grpc.MaxRecvMsgSize(MaximumMessageBytes))
	velav1.RegisterFleetMaintenanceServiceServer(rpcServer, server)
	done := make(chan error, 1)
	go func() { done <- rpcServer.Serve(listener) }()
	t.Cleanup(func() {
		rpcServer.Stop()
		_ = listener.Close()
		<-done
	})
	clientTLS, err := NewClientTLSCredentials(write("client.pem", clientCert), write("client.key", clientKey), caPath, serverName)
	mustBootstrap(t, err)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	base, err := DialClient(ctx, listener.Addr().String(), clientTLS)
	mustBootstrap(t, err)
	t.Cleanup(func() { _ = base.Close() })
	client, err := base.WorkerBootstrap(identity)
	mustBootstrap(t, err)
	claim, err := client.ClaimWorkerBootstrap(ctx, request)
	mustBootstrap(t, err)
	if !claim.Fresh || claim.RequestID != request.RequestID {
		t.Fatalf("large authenticated bundle lost authority: %+v", claim)
	}
	t.Logf("authenticated bundle bytes=%d", len(request.BundleManifest))
	request.BundleManifest = make([]byte, fleet.MaximumWorkerBootstrapManifestBytes+1)
	if result, err := client.ClaimWorkerBootstrap(ctx, request); err == nil || result.Fresh {
		t.Fatalf("oversized bundle accepted: %+v %v", result, err)
	}
	// Raising the gRPC envelope must not widen the existing plan payload contract.
	if _, err := base.Apply(ctx, fleet.ApprovedResidencyPlan{SchemaVersion: 1, ApprovedBy: strings.Repeat("x", maximumWorkerRegistryPayloadBytes)}); err == nil {
		t.Fatal("ordinary Fleet plan limit was widened")
	}
}
