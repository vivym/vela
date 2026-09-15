//go:build integration

package integration_test

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"crypto/tls"
	"encoding/json"
	"net"
	"os"
	"os/exec"
	"reflect"
	"regexp"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/vivym/vela/internal/fleet"
	"github.com/vivym/vela/internal/journalbinding"
	"github.com/vivym/vela/internal/stageworkeragent"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

// The image must contain /nodeagent.test built from this checkout. Test
// credentials enter via stdin, never an environment variable or host mount.
func TestRuntimeStartupNodeProcessPostgresTLS(t *testing.T) {
	image := os.Getenv("VELA_RUNTIME_STARTUP_NODE_IMAGE")
	if image == "" {
		t.Skip("set VELA_RUNTIME_STARTUP_NODE_IMAGE to a source-matched native test image")
	}
	if !regexp.MustCompile(`^sha256:[0-9a-f]{64}$`).MatchString(image) {
		t.Fatal("requires exact local image ID")
	}
	for _, scenario := range []string{"normal", "committed-response-lost", "missing-ptrace"} {
		t.Run(scenario, func(t *testing.T) {
			lose, missingPtrace := scenario == "committed-response-lost", scenario == "missing-ptrace"
			database, service, request := newWorkerBootstrapFixture(t)
			seed := bytes.Repeat([]byte{27}, ed25519.SeedSize)
			signer, err := journalbinding.NewSigner("registry", seed)
			if err != nil {
				t.Fatal(err)
			}
			var calls, tlsVersion atomic.Uint32
			var dropped atomic.Bool
			clients := bootstrapMutualTLSClientsOn(t, service, func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
				if info.FullMethod == velav1.FleetMaintenanceService_ReserveRuntimeStartup_FullMethodName {
					calls.Add(1)
					if p, ok := peer.FromContext(ctx); ok {
						if auth, ok := p.AuthInfo.(credentials.TLSInfo); ok {
							tlsVersion.Store(uint32(auth.State.Version))
						}
					}
				}
				result, err := handler(ctx, req)
				if err == nil && info.FullMethod == velav1.FleetMaintenanceService_ReserveRuntimeStartup_FullMethodName && lose && dropped.CompareAndSwap(false, true) {
					return nil, status.Error(codes.Unavailable, "committed Node reservation response lost")
				}
				return result, err
			}, "0.0.0.0:0", signer)
			args := map[string]string{}
			for i := 0; i < len(clients[0].arguments); i += 2 {
				args[clients[0].arguments[i]] = clients[0].arguments[i+1]
			}
			read := func(flag string) []byte {
				t.Helper()
				data, err := os.ReadFile(args[flag])
				if err != nil {
					t.Fatal(err)
				}
				return data
			}
			_, port, err := net.SplitHostPort(args["--fleet-address"])
			if err != nil {
				t.Fatal(err)
			}
			actor := clients[0].bootstrap.ActorIdentity()
			// Actor's canonical SPIFFE form is retained by the authenticated client.
			spiffe := "spiffe://vela.internal/" + actor
			input := struct {
				Request                     fleet.WorkerBootstrapRequest
				Address, ServerName, SPIFFE string
				CA, Certificate, Key        []byte
				PublicKeys                  map[string][]byte
				LoseResponse                bool
				MissingPtrace               bool
			}{request, net.JoinHostPort("host.docker.internal", port), args["--fleet-server-name"], spiffe, read("--fleet-ca-file"), read("--client-cert-file"), read("--client-key-file"), map[string][]byte{"registry": ed25519.NewKeyFromSeed(seed).Public().(ed25519.PublicKey)}, lose, missingPtrace}
			wire, err := json.Marshal(input)
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(t.Context(), 60*time.Second)
			defer cancel()
			name := "vela-startup-node-" + request.RequestID.String()
			t.Cleanup(func() {
				cleanup, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				_ = exec.CommandContext(cleanup, "docker", "rm", "-f", name).Run()
			})
			dockerArgs := []string{"run", "--rm", "--pull", "never", "--name", name, "-i", "--add-host", "host.docker.internal:host-gateway", "--cap-add", "SYS_ADMIN", "--security-opt", "seccomp=unconfined", "--cpus", "4", "--memory", "4g", "--pids-limit", "256", "-e", "VELA_RUNTIME_STARTUP_FLEET_HELPER=1"}
			if !missingPtrace {
				dockerArgs = append(dockerArgs, "--cap-add", "SYS_PTRACE")
			} else {
				dockerArgs = append(dockerArgs, "--cap-drop", "SYS_PTRACE")
			}
			dockerArgs = append(dockerArgs, image, "/nodeagent.test", "-test.run=^TestRuntimeStartupFleetProcessHelper$", "-test.v", "-test.timeout=45s")
			command := exec.CommandContext(ctx, "docker", dockerArgs...)
			command.Stdin = bytes.NewReader(wire)
			output, err := command.CombinedOutput()
			if err != nil {
				t.Fatalf("native Node process: %v\n%s", err, output)
			}
			if !bytes.Contains(output, []byte("--- PASS: TestRuntimeStartupFleetProcessHelper ")) || bytes.Contains(output, []byte("--- SKIP:")) || bytes.Contains(output, []byte("DATA RACE")) {
				t.Fatalf("missing native test success: %s", output)
			}
			t.Logf("native process evidence (no TLS private keys):\n%s", output)
			if missingPtrace {
				var count int
				if err := database.Admin.QueryRow("SELECT count(*) FROM runtime_startup_reservations").Scan(&count); err != nil || count != 0 || calls.Load() != 0 || !bytes.Contains(output, []byte("VELA_PROTECTED_CALLER_REJECTED_BEFORE_RESERVATION")) {
					t.Fatalf("missing Node capability reached reservation: rows=%d calls=%d err=%v", count, calls.Load(), err)
				}
				return
			}
			var report struct {
				Request             fleet.RuntimeStartupRequest
				Record              json.RawMessage
				HasReceipt          bool
				WorkerJournal       stageworkeragent.AssignmentJournalStatus
				Activated, Revoked  bool
				PostActivationFloor int64
				GrantAttempt        *struct {
					OperationID         string            `json:"operation_id"`
					JournalID           string            `json:"journal_id"`
					StartupDigest       [sha256.Size]byte `json:"startup_digest"`
					AuthorizationDigest [sha256.Size]byte `json:"authorization_digest"`
				}
				CompositionReceipt json.RawMessage
			}
			found := false
			for _, line := range strings.Split(string(output), "\n") {
				if text, ok := strings.CutPrefix(line, "VELA_RESERVATION_REPORT="); ok {
					if found {
						t.Fatal("duplicate report")
					}
					if err := json.Unmarshal([]byte(text), &report); err != nil {
						t.Fatal(err)
					}
					found = true
				}
			}
			if !found || report.HasReceipt == lose || calls.Load() != 1 || tlsVersion.Load() != tls.VersionTLS13 || dropped.Load() != lose {
				t.Fatalf("Node result/call count/TLS/loss mismatch: found=%v calls=%d TLS=%x", found, calls.Load(), tlsVersion.Load())
			}
			if report.Activated != !lose || report.Revoked != !lose || !lose && report.PostActivationFloor != 1 || lose && report.PostActivationFloor != 0 {
				t.Fatalf("activation/write/revocation mismatch: activated=%v revoked=%v floor=%d", report.Activated, report.Revoked, report.PostActivationFloor)
			}
			bootstrapHistory, err := clients[0].bootstrap.LookupWorkerBootstrap(t.Context(), request.RequestID)
			if err != nil || bootstrapHistory.Receipt == nil || bootstrapHistory.Receipt.WorkerJournalID != report.WorkerJournal.JournalID || !bytes.Equal(bootstrapHistory.Receipt.WorkerScope, report.WorkerJournal.Scope[:]) || !report.WorkerJournal.Storage.Valid() || report.WorkerJournal.SchemaVersion != 5 {
				t.Fatalf("Registry pair is not bound to actual held Worker journal: %v", err)
			}
			digest := sha256.Sum256(report.Record)
			if lose {
				if report.GrantAttempt != nil {
					t.Fatal("lost reservation reply created a grant fence")
				}
			} else if report.GrantAttempt == nil || report.GrantAttempt.OperationID != report.Request.RequestID.String() ||
				report.GrantAttempt.JournalID != report.Request.RuntimeJournalID.String() || report.GrantAttempt.StartupDigest != digest ||
				report.GrantAttempt.AuthorizationDigest != sha256.Sum256([]byte("fixture startup authorization evidence; no permit")) {
				t.Fatal("grant fence is not bound to original Node/Fleet operation and fixture evidence")
			} else if len(report.CompositionReceipt) == 0 || string(report.CompositionReceipt) == "null" {
				t.Fatalf("missing composition receipt: %s", report.CompositionReceipt)
			} else {
				var receipt struct {
					Permit  bool   `json:"permit"`
					Outcome string `json:"outcome"`
				}
				if err := json.Unmarshal(report.CompositionReceipt, &receipt); err != nil || !receipt.Permit || receipt.Outcome != "permitted" {
					t.Fatalf("invalid composition receipt: %s (%v)", report.CompositionReceipt, err)
				}
			}
			if !bytes.Equal(digest[:], report.Request.OwnerObservationDigest) {
				t.Fatal("database owner digest does not bind the full original Node record")
			}
			history, err := clients[0].bootstrap.LookupRuntimeStartup(t.Context(), report.Request.RequestID)
			if err != nil || history.Fresh || !reflect.DeepEqual(history.RuntimeStartupRequest, report.Request) {
				t.Fatalf("host database history mismatch: %v", err)
			}
			var count int
			if err := database.Admin.QueryRow("SELECT count(*) FROM runtime_startup_reservations").Scan(&count); err != nil || count != 1 {
				t.Fatalf("expected one immutable reservation: %d %v", count, err)
			}
			t.Logf("actual root Node/non-root PID-1 -> TLS1.3 -> PostgreSQL95; lost=%v; calls=1 rows=1 receipt=%v; original record SHA256=%x; consumption fence=%v activated=%v revoked=%v floor=%d; fixture issuer, no backend Permit", lose, report.HasReceipt, digest, report.GrantAttempt != nil, report.Activated, report.Revoked, report.PostActivationFloor)
		})
	}
}
