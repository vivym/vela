//go:build integration

package integration_test

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/fleet"
	"github.com/vivym/vela/internal/modelruntime"
	"github.com/vivym/vela/internal/stageauthority"
	"github.com/vivym/vela/internal/stageworkeragent"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestWorkerBootstrapCommandPersistsAuthorityAcrossProcesses(t *testing.T) {
	binary := filepath.Join(t.TempDir(), "vela-node-agent")
	build := exec.CommandContext(t.Context(), "go", "build", "-o", binary, "./cmd/vela-node-agent")
	build.Dir = repositoryRoot(t)
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build Node Agent: %s %v", output, err)
	}
	for _, scenario := range []struct{ name, method, interruption string }{
		{name: "normal"},
		{name: "lost-claim", method: velav1.FleetMaintenanceService_ClaimWorkerBootstrap_FullMethodName},
		{name: "lost-receipt", method: velav1.FleetMaintenanceService_RecordWorkerBootstrapReceipt_FullMethodName},
		{name: "claim-timeout", method: velav1.FleetMaintenanceService_ClaimWorkerBootstrap_FullMethodName, interruption: "timeout"},
		{name: "claim-sigterm", method: velav1.FleetMaintenanceService_ClaimWorkerBootstrap_FullMethodName, interruption: "signal"},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			lostMethod := scenario.method
			database, service, request := newWorkerBootstrapFixture(t)
			var dropped atomic.Bool
			var claimCalls atomic.Int32
			var receiptCalls atomic.Int32
			committed := make(chan struct{})
			clients := bootstrapMutualTLSClients(t, service, func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
				if info.FullMethod == velav1.FleetMaintenanceService_ClaimWorkerBootstrap_FullMethodName {
					claimCalls.Add(1)
				}
				if info.FullMethod == velav1.FleetMaintenanceService_RecordWorkerBootstrapReceipt_FullMethodName {
					receiptCalls.Add(1)
				}
				response, err := handler(ctx, req)
				if err == nil && info.FullMethod == lostMethod && dropped.CompareAndSwap(false, true) {
					if scenario.interruption != "" {
						close(committed)
						<-ctx.Done()
						return nil, status.FromContextError(ctx.Err()).Err()
					}
					return nil, status.Error(codes.Unavailable, "committed command response lost")
				}
				return response, err
			})
			preparation, scratch := workerBootstrapCommandFiles(t, request)
			reconciliation := append([]string(nil), preparation...)
			reconciliation[1] = "reconcile-pair"
			invoke := func(principal int, args ...string) ([]byte, error) {
				t.Helper()
				ctx, cancel := context.WithTimeout(t.Context(), 45*time.Second)
				defer cancel()
				arguments := append([]string{"bootstrap"}, clients[principal].arguments...)
				if scenario.interruption == "timeout" {
					arguments = append(arguments, "--timeout", "1s")
				}
				command := exec.CommandContext(ctx, binary, append(arguments, args...)...)
				// Bootstrap must work without daemon configuration or device probing.
				command.Env = append(os.Environ(), "VELA_NODE_AGENT_ID=", "VELA_NODE_AGENT_NVIDIA_SMI_PATH=/nonexistent")
				var stdout, stderr bytes.Buffer
				command.Stdout, command.Stderr = &stdout, &stderr
				shouldInterrupt := scenario.interruption == "signal" && principal == 0 && !dropped.Load() &&
					len(args) >= 2 && args[0] == "--action" && args[1] == "prepare"
				if err := command.Start(); err != nil {
					t.Fatal(err)
				}
				var interrupted chan error
				if shouldInterrupt {
					interrupted = make(chan error, 1)
					go func() {
						select {
						case <-committed:
							interrupted <- command.Process.Signal(syscall.SIGTERM)
						case <-ctx.Done():
							interrupted <- ctx.Err()
						}
					}()
				}
				err := command.Wait()
				cancel()
				if interrupted != nil {
					if signalErr := <-interrupted; signalErr != nil {
						t.Fatalf("interrupt committed command: %v", signalErr)
					}
					if command.ProcessState.ExitCode() != 1 {
						t.Fatalf("SIGTERM bypassed graceful cancellation: %s", command.ProcessState)
					}
				}
				if err != nil && stdout.Len() != 0 {
					t.Fatalf("failed command printed a success result: %s %s", stdout.Bytes(), stderr.Bytes())
				}
				if err != nil {
					t.Logf("rejected command: %s", stderr.Bytes())
				}
				return stdout.Bytes(), err
			}
			// A valid certificate for another node cannot even create local state.
			if _, err := invoke(1, preparation...); err == nil {
				t.Fatal("different-node command succeeded")
			}
			if entries, err := os.ReadDir(filepath.Join(scratch, "bootstrap")); err != nil || len(entries) != 0 || claimCalls.Load() != 0 {
				t.Fatalf("different node reached first use: %v %v", entries, err)
			}
			if _, err := invoke(0, reconciliation...); err == nil {
				t.Fatal("reconciliation created a missing bootstrap operation")
			}
			if entries, err := os.ReadDir(filepath.Join(scratch, "bootstrap")); err != nil || len(entries) != 0 || claimCalls.Load() != 0 {
				t.Fatalf("reconciliation initialized local state: %v %v", entries, err)
			}
			first, err := invoke(0, preparation...)
			if (err != nil) != (lostMethod != "") || lostMethod != "" && !dropped.Load() {
				t.Fatalf("first command result: %s %v", first, err)
			}
			var operation struct {
				RequestID uuid.UUID `json:"request_id"`
			}
			operationPath := filepath.Join(scratch, "bootstrap", "operation.json")
			operationBefore, err := os.ReadFile(operationPath)
			if err != nil {
				t.Fatal(err)
			}
			if err := json.Unmarshal(operationBefore, &operation); err != nil || operation.RequestID == uuid.Nil {
				t.Fatalf("retained command identity: %v", err)
			}
			lookup := []string{"--action", "history", "--request-id", operation.RequestID.String()}
			before, err := clients[0].bootstrap.LookupWorkerBootstrap(t.Context(), operation.RequestID)
			if err != nil {
				t.Fatal(err)
			}
			wire, err := invoke(0, lookup...)
			var history struct {
				Action    string          `json:"action"`
				RequestID uuid.UUID       `json:"request_id"`
				Pair      json.RawMessage `json:"pair"`
			}
			if err != nil || json.Unmarshal(wire, &history) != nil || history.Action != "history" || history.RequestID != operation.RequestID ||
				(len(history.Pair) != 0) != (before.Receipt != nil) || bytes.Contains(bytes.ToLower(wire), []byte("fresh")) {
				t.Fatalf("command history: %s %v", wire, err)
			}
			if _, err := invoke(2, lookup...); err == nil {
				t.Fatal("another Agent on the same node read original history")
			}
			if _, err := invoke(0, "--action", "history", "--request-id", uuid.NewString()); err == nil {
				t.Fatal("missing history returned success")
			}
			after, err := clients[0].bootstrap.LookupWorkerBootstrap(t.Context(), operation.RequestID)
			if err != nil || !reflect.DeepEqual(after, before) || claimCalls.Load() != 1 {
				t.Fatalf("history command mutated authority: %+v %v", after, err)
			}
			retry, err := invoke(0, preparation...)
			if lostMethod == velav1.FleetMaintenanceService_ClaimWorkerBootstrap_FullMethodName {
				if err == nil || before.Receipt != nil {
					t.Fatal("lost Claim response regained initialization permission")
				}
				if _, err := invoke(0, reconciliation...); err == nil {
					t.Fatal("reconciliation completed unrecorded initialization")
				}
				for _, name := range []string{"worker-admission", "runtime-admission", "inputs", "outputs"} {
					if entries, err := os.ReadDir(filepath.Join(scratch, name)); err != nil || len(entries) != 0 {
						t.Fatalf("lost Claim initialized %s: %v %v", name, entries, err)
					}
				}
			} else {
				var result struct {
					RequestID  uuid.UUID                                `json:"request_id"`
					Worker     stageworkeragent.AssignmentJournalStatus `json:"worker"`
					Runtime    modelruntime.ExecutionJournalStatus      `json:"runtime"`
					RecordedAt time.Time                                `json:"recorded_at"`
				}
				if err != nil || json.Unmarshal(retry, &result) != nil || before.Receipt == nil || result.RequestID != operation.RequestID ||
					result.Worker.JournalID != before.Receipt.WorkerJournalID || result.Runtime.JournalID != before.Receipt.RuntimeJournalID ||
					!result.RecordedAt.Equal(before.RecordedAt) || result.Worker.SchemaVersion != 5 || result.Runtime.SchemaVersion != 4 {
					t.Fatalf("command replay changed journal pair: %s %v", retry, err)
				}
				if lostMethod == "" && !bytes.Equal(first, retry) {
					t.Fatal("ordinary command retry changed its result")
				}
				// Missing local pair proof cannot be regenerated by replaying Claim.
				if err := os.Remove(filepath.Join(scratch, "bootstrap", "pair.json")); err != nil {
					t.Fatal(err)
				}
				if _, err := invoke(0, preparation...); err == nil {
					t.Fatal("incomplete local pair was reinitialized")
				}
				receiptsBefore := receiptCalls.Load()
				reconciled, err := invoke(0, reconciliation...)
				var recovered struct {
					Action     string                                   `json:"action"`
					RequestID  uuid.UUID                                `json:"request_id"`
					Worker     stageworkeragent.AssignmentJournalStatus `json:"worker"`
					Runtime    modelruntime.ExecutionJournalStatus      `json:"runtime"`
					RecordedAt time.Time                                `json:"recorded_at"`
				}
				if err != nil || json.Unmarshal(reconciled, &recovered) != nil || recovered.Action != "reconcile-pair" ||
					recovered.RequestID != result.RequestID || recovered.Worker != result.Worker || recovered.Runtime != result.Runtime ||
					!recovered.RecordedAt.Equal(result.RecordedAt) || receiptCalls.Load() != receiptsBefore {
					t.Fatalf("authenticated reconciliation changed journals or mutated Registry: %s %v", reconciled, err)
				}
				if replay, err := invoke(0, reconciliation...); err != nil || !bytes.Equal(reconciled, replay) {
					t.Fatalf("reconciliation replay changed its result: %s %v", replay, err)
				}
				unchanged, err := clients[0].bootstrap.LookupWorkerBootstrap(t.Context(), operation.RequestID)
				if err != nil || !reflect.DeepEqual(unchanged, before) || receiptCalls.Load() != receiptsBefore {
					t.Fatalf("reconciliation wrote Registry history: %+v %v", unchanged, err)
				}
			}
			if current, err := os.ReadFile(operationPath); err != nil || !bytes.Equal(current, operationBefore) || claimCalls.Load() != 1 {
				t.Fatalf("command retries replaced durable operation or repeated Claim: %v", err)
			}
			var count int
			if err := database.Admin.QueryRow("SELECT count(*) FROM worker_bootstrap_claims").Scan(&count); err != nil || count != 1 {
				t.Fatalf("command/history created extra authority: %d %v", count, err)
			}
		})
	}
}

func workerBootstrapCommandFiles(t *testing.T, request fleet.WorkerBootstrapRequest) ([]string, string) {
	t.Helper()
	config := localBootstrapConfig(t, request)
	root := t.TempDir()
	write := func(name string, data []byte) string {
		t.Helper()
		path := filepath.Join(root, name)
		if err := os.WriteFile(path, data, 0o600); err != nil {
			t.Fatal(err)
		}
		return path
	}
	launch, err := modelruntime.EncodeLaunchManifest(config.Launch)
	if err != nil {
		t.Fatal(err)
	}
	keys, err := stageauthority.DeriveVerifierKeyring(map[string][]byte{"bootstrap-key": bytes.Repeat([]byte{3}, 32)})
	if err != nil {
		t.Fatal(err)
	}
	defer stageauthority.ClearKeyring(keys)
	keyring, err := json.Marshal(keys)
	if err != nil {
		t.Fatal(err)
	}
	return []string{"--action", "prepare", "--bundle-manifest-file", write("bundle.json", request.BundleManifest),
		"--launch-manifest-file", write("launch.json", launch), "--verifier-keyring-file", write("verifier.json", keyring),
		"--scratch-directory", config.ScratchDirectory, "--max-records", "4"}, config.ScratchDirectory
}
