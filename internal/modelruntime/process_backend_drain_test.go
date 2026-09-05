//go:build darwin || linux

package modelruntime

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/vivym/vela/internal/driverdrain"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
)

func TestProcessBackendDrainFailuresPreserveInspectionAndResidentCommands(t *testing.T) {
	for _, mode := range []string{"silent", "malformed", "late"} {
		t.Run(mode, func(t *testing.T) {
			backend, logPath := startInspectionProcess(t, "drain-"+mode)
			authority := processBackendAuthority(1)
			authority.Authority.ExecutionSequence = 7
			if err := backend.Prepare(t.Context(), authority, &velav1.StageExecutionSpec{}); err != nil {
				t.Fatal(err)
			}
			if err := backend.Start(t.Context(), authority); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(t.Context(), 200*time.Millisecond)
			defer cancel()
			done := make(chan error, 1)
			go func() {
				result, err := backend.DrainExecution(ctx, authority)
				if result != (BackendDrain{}) {
					err = errors.New("failed call returned drain evidence")
				}
				done <- err
			}()
			waitForDriverEvent(t, logPath, "drain")
			if mode == "late" {
				if err := <-done; !errors.Is(err, context.DeadlineExceeded) {
					t.Fatalf("drain did not expire: %v", err)
				}
			}
			commandCtx, commandCancel := context.WithTimeout(t.Context(), 200*time.Millisecond)
			defer commandCancel()
			if err := backend.Cancel(commandCtx, authority, velav1.ModelRuntimeCancelReason_MODEL_RUNTIME_CANCEL_REASON_CONTROL_PLANE_STOP); err != nil {
				t.Fatal(err)
			}
			if mode != "late" {
				if err := <-done; !errors.Is(err, ErrExecutionDrainUnproven) {
					t.Fatalf("failed drain accepted: %v", err)
				}
			}
			if observation, err := backend.InspectExecution(t.Context(), authority); err != nil || !observation.Known {
				t.Fatalf("drain damaged inspection: %+v %v", observation, err)
			}
			if probe, err := backend.Probe(t.Context(), velav1.ModelRuntimeReadinessCheck_MODEL_RUNTIME_READINESS_CHECK_MODEL_WARMUP); err != nil || !probe.Ready || backend.Err() != nil {
				t.Fatalf("drain unloaded driver: %+v %v", probe, err)
			}
			if mode == "late" {
				if result, err := backend.DrainExecution(t.Context(), authority); err != nil || validateBackendDrain(result, authority) != nil {
					t.Fatalf("late reply poisoned retry: %+v %v", result, err)
				}
			}
			if err := backend.Close(); err != nil {
				t.Fatalf("shutdown failed: %v", err)
			}
		})
	}
}

func TestProcessBackendDrainNegotiationPreservesLegacyWireShape(t *testing.T) {
	authority := processBackendAuthority(1)
	authority.Authority.ExecutionSequence = 7
	for _, protocol := range []string{"", "unknown-v1", driverdrain.Protocol} {
		backend := &ProcessBackend{drainProtocol: protocol}
		encoded, err := json.Marshal(backend.stageIdentity(authority))
		if err != nil {
			t.Fatal(err)
		}
		if bytes.Contains(encoded, []byte("execution_sequence")) != (protocol == driverdrain.Protocol) {
			t.Fatalf("wire compatibility changed: %s", encoded)
		}
		if protocol != driverdrain.Protocol {
			if result, err := backend.DrainExecution(t.Context(), authority); result != (BackendDrain{}) || !errors.Is(err, ErrExecutionDrainUnproven) {
				t.Fatal("unnegotiated drain accepted")
			}
		}
	}
}

func serveFaultyDrain(mode string, released <-chan struct{}) {
	conn, err := driverdrain.OpenInherited(os.Getenv(driverdrain.Environment))
	if err != nil || conn == nil {
		return
	}
	defer func() { _ = conn.Close() }()
	packet := make([]byte, 1025)
	for {
		n, err := conn.Read(packet)
		if err != nil {
			return
		}
		appendDriverEvent(os.Getenv("VELA_TEST_INSPECTION_LOG"), "drain")
		if mode == "silent" {
			continue
		}
		if mode == "malformed" {
			_, _ = conn.Write([]byte("{"))
			continue
		}
		var query struct {
			SchemaVersion int                  `json:"schema_version"`
			RequestID     uint64               `json:"request_id"`
			Identity      driverdrain.Identity `json:"identity"`
		}
		if json.Unmarshal(packet[:n], &query) != nil {
			return
		}
		<-released
		encoded, _ := json.Marshal(map[string]any{"schema_version": 1, "request_id": query.RequestID,
			"identity": query.Identity, "drained": true, "contract": driverdrain.Contract})
		if _, err := conn.Write(encoded); err != nil {
			return
		}
	}
}
