//go:build integration

package integration_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/journalbinding"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestWorkerBootstrapAbandonmentCommandPreservesUncertainLocalState(t *testing.T) {
	binary := filepath.Join(t.TempDir(), "vela-node-agent")
	build := exec.CommandContext(t.Context(), "go", "build", "-o", binary, "./cmd/vela-node-agent")
	build.Dir = repositoryRoot(t)
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build Node Agent: %s %v", output, err)
	}
	for _, lostResponse := range []bool{false, true} {
		t.Run(map[bool]string{false: "normal", true: "lost-abandonment"}[lostResponse], func(t *testing.T) {
			database, service, request := newWorkerBootstrapFixture(t)
			var claimCalls atomic.Int32
			var abandonmentDropped atomic.Bool
			signer, err := journalbinding.NewSigner("test-registry", bytes.Repeat([]byte{8}, 32))
			if err != nil {
				t.Fatal(err)
			}
			clients := bootstrapMutualTLSClients(t, service, func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
				response, err := handler(ctx, req)
				if info.FullMethod == velav1.FleetMaintenanceService_ClaimWorkerBootstrap_FullMethodName {
					claimCalls.Add(1)
					if err == nil {
						return nil, status.Error(codes.Unavailable, "committed Claim response lost")
					}
				}
				if err == nil && info.FullMethod == velav1.FleetMaintenanceService_AbandonWorkerBootstrap_FullMethodName && lostResponse && abandonmentDropped.CompareAndSwap(false, true) {
					return nil, status.Error(codes.Unavailable, "committed abandonment response lost")
				}
				return response, err
			}, signer)
			preparation, scratch := workerBootstrapCommandFiles(t, request)
			invoke := func(principal int, arguments ...string) ([]byte, error) {
				t.Helper()
				ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
				defer cancel()
				args := append([]string{"bootstrap"}, clients[principal].arguments...)
				command := exec.CommandContext(ctx, binary, append(args, arguments...)...)
				command.Env = append(os.Environ(), "VELA_NODE_AGENT_ID=", "VELA_NODE_AGENT_NVIDIA_SMI_PATH=/nonexistent")
				var stdout, stderr bytes.Buffer
				command.Stdout, command.Stderr = &stdout, &stderr
				err := command.Run()
				if err != nil && stdout.Len() != 0 {
					t.Fatalf("failed command printed successful outcome: %s %s", stdout.Bytes(), stderr.Bytes())
				}
				if err != nil {
					t.Logf("rejected command: %s", stderr.Bytes())
				}
				return stdout.Bytes(), err
			}
			if _, err := invoke(0, preparation...); err == nil || claimCalls.Load() != 1 {
				t.Fatalf("lost Claim response did not interrupt preparation: %v", err)
			}
			var operation struct {
				RequestID uuid.UUID `json:"request_id"`
			}
			wire, err := os.ReadFile(filepath.Join(scratch, "bootstrap", "operation.json"))
			if err != nil || json.Unmarshal(wire, &operation) != nil || operation.RequestID == uuid.Nil {
				t.Fatalf("missing original operation identity: %v", err)
			}
			if err := os.WriteFile(filepath.Join(scratch, "inputs", "uncertain-input"), []byte("preserve unresolved local history"), 0o600); err != nil {
				t.Fatal(err)
			}
			before := bootstrapCommandScratchSnapshot(t, scratch)
			abandon := []string{"--action", "abandon", "--request-id", operation.RequestID.String()}
			for _, principal := range []int{1, 2} {
				if _, err := invoke(principal, abandon...); err == nil {
					t.Fatal("another Node Agent abandoned the original operation")
				}
			}
			assertBootstrapWorkerState(t, database, request.WorkerInstanceID, "PROVISIONING", 1)
			first, err := invoke(0, abandon...)
			if (err != nil) != lostResponse || lostResponse && !abandonmentDropped.Load() {
				t.Fatalf("abandonment command did not cross the requested response boundary: %s %v", first, err)
			}
			history, err := clients[0].bootstrap.LookupWorkerBootstrap(t.Context(), operation.RequestID)
			if err != nil || history.Abandonment == nil || history.Receipt != nil {
				t.Fatalf("committed abandonment is missing: %+v %v", history, err)
			}
			replay, err := invoke(0, abandon...)
			var result struct {
				Action      string    `json:"action"`
				RequestID   uuid.UUID `json:"request_id"`
				Abandonment struct {
					FencedInstanceEpoch int64     `json:"fenced_instance_epoch"`
					AbandonedAt         time.Time `json:"abandoned_at"`
				} `json:"abandonment"`
			}
			if err != nil || json.Unmarshal(replay, &result) != nil || result.Action != "abandon" || result.RequestID != operation.RequestID ||
				result.Abandonment.FencedInstanceEpoch != 2 || !result.Abandonment.AbandonedAt.Equal(history.Abandonment.AbandonedAt) ||
				!lostResponse && !bytes.Equal(first, replay) {
				t.Fatalf("command replay changed original abandonment: %s %v", replay, err)
			}
			if _, err := invoke(0, preparation...); err == nil {
				t.Fatal("abandonment authorized another initialization")
			}
			if _, err := clients[0].rpc.LookupWorkerBootstrapBinding(t.Context(), &velav1.LookupWorkerBootstrapBindingRequest{RequestId: operation.RequestID.String()}); status.Code(err) != codes.FailedPrecondition {
				t.Fatalf("abandonment produced a signed journal binding: %v", err)
			}
			if after := bootstrapCommandScratchSnapshot(t, scratch); !reflect.DeepEqual(before, after) || claimCalls.Load() != 1 {
				t.Fatal("abandonment/replay changed local files or repeated first use")
			}
			assertBootstrapWorkerState(t, database, request.WorkerInstanceID, "FENCED", 2)
		})
	}
}

func bootstrapCommandScratchSnapshot(t *testing.T, root string) map[string]string {
	t.Helper()
	result := make(map[string]string)
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return nil
		}
		wire, err := os.ReadFile(path)
		if err == nil {
			result[path] = string(wire)
		}
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	return result
}
