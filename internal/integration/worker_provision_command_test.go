//go:build integration

package integration_test

import (
	"archive/tar"
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/workerbootstrap"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

func TestProtectedProvisioningCommandPostgres(t *testing.T) {
	if os.Getenv("VELA_TEST_PROVISION_SANDBOX") != "1" {
		t.Skip("set VELA_TEST_PROVISION_SANDBOX=1 for the Linux command/PostgreSQL/mTLS campaign")
	}
	image := provisionCommandImage(t)
	for _, scenario := range []struct {
		name, method, signal string
		principal            int
	}{
		{name: "normal"},
		{name: "lost-claim", method: velav1.FleetMaintenanceService_ClaimWorkerBootstrap_FullMethodName},
		{name: "lost-receipt", method: velav1.FleetMaintenanceService_RecordWorkerBootstrapReceipt_FullMethodName},
		{name: "claim-kill", method: velav1.FleetMaintenanceService_ClaimWorkerBootstrap_FullMethodName, signal: "KILL"},
		{name: "receipt-kill", method: velav1.FleetMaintenanceService_RecordWorkerBootstrapReceipt_FullMethodName, signal: "KILL"},
		{name: "receipt-term", method: velav1.FleetMaintenanceService_RecordWorkerBootstrapReceipt_FullMethodName, signal: "TERM"},
		{name: "wrong-node", principal: 1},
		{name: "unregistered-principal", principal: 3},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			database, service, request := newWorkerBootstrapFixture(t)
			var claims, receipts atomic.Int32
			var observedTLS atomic.Uint32
			var dropped atomic.Bool
			committed := make(chan struct{})
			// A container reaches this ephemeral, certificate-authenticated test
			// listener through Docker's host gateway. No production listener is used.
			clients := bootstrapMutualTLSClientsOn(t, service, func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
				if p, ok := peer.FromContext(ctx); ok {
					if auth, ok := p.AuthInfo.(credentials.TLSInfo); ok {
						observedTLS.Store(uint32(auth.State.Version))
					}
				}
				if info.FullMethod == velav1.FleetMaintenanceService_ClaimWorkerBootstrap_FullMethodName {
					claims.Add(1)
				}
				if info.FullMethod == velav1.FleetMaintenanceService_RecordWorkerBootstrapReceipt_FullMethodName {
					receipts.Add(1)
				}
				response, err := handler(ctx, req)
				if err == nil && info.FullMethod == scenario.method && dropped.CompareAndSwap(false, true) {
					close(committed)
					if scenario.signal != "" {
						<-ctx.Done()
					}
					return nil, status.Error(codes.Unavailable, "committed provisioning response lost")
				}
				return response, err
			}, "0.0.0.0:0")
			preparation, _ := workerBootstrapCommandFiles(t, request)
			arguments := append(append([]string(nil), clients[scenario.principal].arguments...), preparation...)
			files := make(map[string]string)
			for i := 0; i < len(arguments); i += 2 {
				switch arguments[i] {
				case "--fleet-address":
					_, port, err := net.SplitHostPort(arguments[i+1])
					if err != nil {
						t.Fatal(err)
					}
					arguments[i+1] = net.JoinHostPort("host.docker.internal", port)
				case "--action":
					arguments[i+1] = "provision"
				case "--scratch-directory":
					arguments[i], arguments[i+1] = "--node-state-directory", "/node-state"
				default:
					if strings.HasSuffix(arguments[i], "-file") {
						name := "/fixtures/" + strings.TrimPrefix(arguments[i], "--")
						files[name], arguments[i+1] = arguments[i+1], name
					}
				}
			}
			create := []string{"create", "--pull", "never", "--add-host", "host.docker.internal:host-gateway", "--pids-limit", "64", "--memory", "256m", "--cpus", "2",
				"--env", "VELA_NODE_AGENT_ID=", "--env", "VELA_NODE_AGENT_NVIDIA_SMI_PATH=/nonexistent", image, "bootstrap"}
			container := strings.TrimSpace(string(provisionDocker(t, append(create, arguments...)...)))
			t.Cleanup(func() { provisionDocker(t, "rm", "--force", "--volumes", container) })
			provisionCommandFiles(t, container, files)
			beforeStart := provisionCommandSnapshot(t, container)
			ctx, cancel := context.WithTimeout(t.Context(), 45*time.Second)
			defer cancel()
			command := exec.CommandContext(ctx, "docker", "start", "--attach", container)
			var stdout, stderr bytes.Buffer
			command.Stdout, command.Stderr = &stdout, &stderr
			if err := command.Start(); err != nil {
				t.Fatal(err)
			}
			if scenario.signal != "" {
				select {
				case <-committed:
					provisionDocker(t, "kill", "--signal", scenario.signal, container)
				case <-ctx.Done():
					_ = command.Wait()
					t.Fatal("Node command never reached the committed interruption boundary")
				}
			}
			commandErr := command.Wait()
			exitCode := strings.TrimSpace(string(provisionDocker(t, "inspect", "--format", "{{.State.ExitCode}}", container)))
			success := scenario.method == "" && scenario.principal == 0
			if success && (commandErr != nil || exitCode != "0") || !success && exitCode == "0" {
				t.Fatalf("unexpected command outcome: exit=%s err=%v stderr=%s", exitCode, commandErr, stderr.Bytes())
			}
			if scenario.signal == "KILL" && exitCode != "137" || scenario.signal == "TERM" && exitCode != "1" {
				t.Fatalf("command did not observe requested process interruption: exit=%s", exitCode)
			}
			if !success && stdout.Len() != 0 {
				t.Fatal("failed provisioning published successful stdout")
			}
			var count int
			if err := database.Admin.QueryRow("SELECT count(*) FROM worker_bootstrap_claims WHERE worker_member_id=$1", request.WorkerMemberID).Scan(&count); err != nil {
				t.Fatal(err)
			}
			snapshot := provisionCommandSnapshot(t, container)
			if scenario.principal != 0 {
				if count != 0 || receipts.Load() != 0 {
					t.Fatal("wrong TLS identity consumed Registry first use or recorded a pair")
				}
				if scenario.principal == 1 {
					if claims.Load() != 0 || !reflect.DeepEqual(snapshot, beforeStart) {
						t.Fatal("wrong Node scope reached Registry or changed local state")
					}
				} else {
					if claims.Load() != 1 || observedTLS.Load() != tls.VersionTLS13 {
						t.Fatal("unregistered principal did not reach authenticated Registry rejection")
					}
					if _, exists := snapshot["node-state/provision-intent.json"]; !exists {
						t.Fatal("Registry rejection lost the original local attempt")
					}
					if _, exists := snapshot["node-state/provision-handover.json"]; exists {
						t.Fatal("Registry rejection published completed handover")
					}
				}
				beforeClaims := claims.Load()
				provisionDocker(t, "start", container)
				if value := strings.TrimSpace(string(provisionDocker(t, "wait", container))); value != "1" ||
					claims.Load() != beforeClaims || !reflect.DeepEqual(provisionCommandSnapshot(t, container), snapshot) {
					t.Fatal("rejected principal restart changed evidence or retried Registry first use")
				}
				t.Log("real command: unauthorized scope rejected; Registry unchanged; restart rejected")
				return
			}
			if count != 1 || claims.Load() != 1 || observedTLS.Load() != tls.VersionTLS13 || scenario.method != "" && !dropped.Load() {
				t.Fatalf("missing actual PostgreSQL/mTLS evidence: rows=%d claims=%d TLS=%x", count, claims.Load(), observedTLS.Load())
			}
			var operationID uuid.UUID
			if err := database.Admin.QueryRow("SELECT request_id FROM worker_bootstrap_claims WHERE worker_member_id=$1", request.WorkerMemberID).Scan(&operationID); err != nil {
				t.Fatal(err)
			}
			history, err := clients[0].bootstrap.LookupWorkerBootstrap(t.Context(), operationID)
			if err != nil {
				t.Fatal(err)
			}
			wantReceipt := scenario.method != velav1.FleetMaintenanceService_ClaimWorkerBootstrap_FullMethodName
			if (history.Receipt != nil) != wantReceipt || history.Claim.Fresh {
				t.Fatal("response loss changed committed Registry outcome")
			}
			if success {
				var result workerbootstrap.ProvisionedJournals
				if err := json.Unmarshal(stdout.Bytes(), &result); err != nil || result.RequestID != operationID || result.ProvisionID == uuid.Nil {
					t.Fatalf("unbound provisioning stdout: %v", err)
				}
				validateProvisionCommandCompletion(t, snapshot, result, history.Receipt.WorkerJournalID, history.Receipt.RuntimeJournalID)
			} else if _, exists := snapshot["node-state/provision-handover.json"]; exists {
				t.Fatal("interrupted provisioning published handover")
			}
			beforeClaims, beforeReceipts := claims.Load(), receipts.Load()
			provisionDocker(t, "start", container)
			if value := strings.TrimSpace(string(provisionDocker(t, "wait", container))); value != "1" {
				t.Fatalf("restarted command did not reject retained operation: %s", value)
			}
			after, err := clients[0].bootstrap.LookupWorkerBootstrap(t.Context(), operationID)
			if err != nil || claims.Load() != beforeClaims || receipts.Load() != beforeReceipts || !reflect.DeepEqual(after, history) ||
				!reflect.DeepEqual(provisionCommandSnapshot(t, container), snapshot) {
				t.Fatal("restarted command changed retained evidence or Registry authority")
			}
			t.Logf("real command: TLS=1.3 claim_count=1 recorded_pair=%t success=%t restart_rejected=true", wantReceipt, success)
		})
	}
}

type provisionArchiveEntry struct {
	UID, GID int
	Mode     int64
	Type     byte
	Data     string
}

// Copy an explicit root-owned archive: Docker Desktop can otherwise preserve
// the macOS source UID, which the real Node command correctly rejects.
func provisionCommandFiles(t *testing.T, container string, files map[string]string) {
	t.Helper()
	var archive bytes.Buffer
	writer := tar.NewWriter(&archive)
	for destination, source := range files {
		data, err := os.ReadFile(source)
		if err != nil {
			t.Fatal(err)
		}
		if err := writer.WriteHeader(&tar.Header{Name: filepath.Base(destination), Mode: 0o600, Size: int64(len(data)), Typeflag: tar.TypeReg}); err != nil {
			t.Fatal(err)
		}
		if _, err := writer.Write(data); err != nil {
			t.Fatal(err)
		}
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, "docker", "cp", "-", container+":/fixtures")
	command.Stdin = &archive
	if err := command.Run(); err != nil {
		t.Fatal("copy private command fixtures:", err)
	}
}

func provisionCommandSnapshot(t *testing.T, container string) map[string]provisionArchiveEntry {
	t.Helper()
	reader := tar.NewReader(bytes.NewReader(provisionDocker(t, "cp", container+":/node-state", "-")))
	entries := make(map[string]provisionArchiveEntry)
	for {
		header, err := reader.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		if header.Size > 1<<20 || len(entries) >= 32 || header.Typeflag != tar.TypeReg && header.Typeflag != tar.TypeDir {
			t.Fatal("unexpected Node state archive entry")
		}
		data, err := io.ReadAll(reader)
		if err != nil {
			t.Fatal(err)
		}
		name := strings.TrimSuffix(header.Name, "/")
		if _, exists := entries[name]; exists {
			t.Fatal("duplicate Node state archive entry")
		}
		entries[name] = provisionArchiveEntry{UID: header.Uid, GID: header.Gid, Mode: header.Mode, Type: header.Typeflag, Data: string(data)}
	}
	return entries
}

func validateProvisionCommandCompletion(t *testing.T, entries map[string]provisionArchiveEntry, result workerbootstrap.ProvisionedJournals, workerID, runtimeID uuid.UUID) {
	t.Helper()
	origin, exists := entries["node-state/provision-origin.json"]
	if !exists || sha256.Sum256([]byte(origin.Data)) != result.OriginDigest || len(entries) != 19 {
		t.Fatal("command result differs from full protected storage inventory")
	}
	for name, entry := range entries {
		uid := 0
		if strings.HasPrefix(name, "node-state/scratch") {
			uid = 10001
		}
		mode := int64(0o600)
		if entry.Type == tar.TypeDir {
			mode = 0o700
		}
		if entry.UID != uid || entry.GID != uid || entry.Mode != mode {
			t.Fatalf("incorrect command ownership handover for %s", name)
		}
	}
	for path, expected := range map[string]uuid.UUID{
		"node-state/scratch/worker-admission/assignment-admission.json": workerID,
		"node-state/scratch/runtime-admission/execution-admission.json": runtimeID,
	} {
		var journal struct {
			ID uuid.UUID `json:"journal_id"`
		}
		if err := json.Unmarshal([]byte(entries[path].Data), &journal); err != nil || journal.ID != expected {
			t.Fatalf("actual journal differs from committed PostgreSQL pair: %s %v", path, err)
		}
	}
}

func provisionCommandImage(t *testing.T) string {
	t.Helper()
	var server struct{ Os, Arch, Version string }
	if err := json.Unmarshal(provisionDocker(t, "version", "--format", "{{json .Server}}"), &server); err != nil ||
		server.Os != "linux" || server.Arch != "amd64" && server.Arch != "arm64" {
		t.Fatalf("unsupported Docker CPU server: %+v %v", server, err)
	}
	directory := t.TempDir()
	for _, name := range []string{"fixtures", "node-state"} {
		if err := os.MkdirAll(filepath.Join(directory, "rootfs", name), 0o700); err != nil {
			t.Fatal(err)
		}
	}
	build := exec.CommandContext(t.Context(), "go", "build", "-o", filepath.Join(directory, "rootfs", "vela-node-agent"), "./cmd/vela-node-agent")
	build.Dir = repositoryRoot(t)
	build.Env = append(os.Environ(), "CGO_ENABLED=0", "GOOS=linux", "GOARCH="+server.Arch)
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build actual Linux Node command: %s %v", output, err)
	}
	if err := os.WriteFile(filepath.Join(directory, "Dockerfile"), []byte("FROM scratch\nCOPY rootfs /\nENTRYPOINT [\"/vela-node-agent\"]\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	image := strings.TrimSpace(string(provisionDocker(t, "build", "--quiet", "--network", "none", directory)))
	if !strings.HasPrefix(image, "sha256:") || len(image) != 71 {
		t.Fatalf("no immutable command fixture image: %q", image)
	}
	t.Logf("actual Node command image=%s platform=%s/%s Docker=%s", image, server.Os, server.Arch, server.Version)
	return image
}

func provisionDocker(t *testing.T, arguments ...string) []byte {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	output, err := exec.CommandContext(ctx, "docker", arguments...).Output()
	if err != nil {
		// Do not print copy arguments or output: archives can contain test keys.
		t.Fatal(fmt.Errorf("Docker %s failed: %w", arguments[0], err))
	}
	return output
}
