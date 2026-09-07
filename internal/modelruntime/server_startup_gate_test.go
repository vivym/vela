package modelruntime_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/vivym/vela/internal/modelruntime"
	"github.com/vivym/vela/internal/stageauthority"
	"google.golang.org/protobuf/proto"
)

func TestRuntimeServerGatesFirstFactoryWithHeldStartupIntent(t *testing.T) {
	for _, scenario := range []string{"permit", "deny", "cancel-after-permit", "changed-journal", "missing-gate"} {
		t.Run(scenario, func(t *testing.T) {
			config := journalRuntimeServerConfig(t)
			journal, err := modelruntime.PrepareExecutionJournal(t.Context(), config.Manifest, config.Validator, *config.ExecutionFloor.State)
			if err != nil {
				t.Fatal(err)
			}
			config.ExecutionFloor.State.Initialize = false
			config.RegistryBinding, config.RegistryVerifier = runtimeRegistryBinding(t, config, journal, nil)
			manifest, err := modelruntime.EncodeLaunchManifest(config.Manifest)
			if err != nil {
				t.Fatal(err)
			}
			registry, err := proto.MarshalOptions{Deterministic: true}.Marshal(config.RegistryBinding)
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			gateCalls, factoryCalls, epochCalls := 0, 0, 0
			factory, epochs := config.BackendFactory, config.EpochStore
			config.EpochStore = modelruntime.EpochStoreFunc(func(binding stageauthority.RuntimeBinding) (int64, error) {
				epochCalls++
				return epochs.Next(binding)
			})
			config.BackendFactory = func(ctx context.Context, runtime modelruntime.LaunchRuntime, binding stageauthority.RuntimeBinding, backend modelruntime.ProcessBackendConfig) (modelruntime.Backend, error) {
				factoryCalls++
				if gateCalls != 1 {
					t.Fatal("factory dispatched without the single member-wide gate")
				}
				return factory(ctx, runtime, binding, backend)
			}
			config.BackendStartupGate = func(ctx context.Context, request modelruntime.BackendStartupRequest) error {
				gateCalls++
				state := readDurableExecutionState(t, config.ExecutionFloor.State.Directory)
				if factoryCalls != 0 || request.Validate() != nil || request.JournalID != journal.JournalID || request.JournalScope != journal.Scope ||
					request.RegistryBindingDigest != sha256.Sum256(registry) || request.NodeIdentity != config.RegistryBinding.Claim.NodeIdentity ||
					state.BackendLifecycle == nil || state.BackendLifecycle.State != modelruntime.BackendLifecycleUnresolved ||
					request.IncarnationID != state.BackendLifecycle.IncarnationID || request.LaunchDigest != sha256.Sum256(manifest) || request.LaunchDigest != state.BackendLifecycle.LaunchDigest {
					t.Fatalf("gate did not observe the original persisted startup intent: %+v", request)
				}
				if deadline, ok := ctx.Deadline(); !ok || time.Until(deadline) > 45*time.Second {
					t.Fatal("startup gate did not receive a bounded context")
				}
				if _, err := modelruntime.PrepareExecutionJournal(ctx, config.Manifest, config.Validator, *config.ExecutionFloor.State); err == nil {
					t.Fatal("startup gate released the original journal lock")
				}
				switch scenario {
				case "deny":
					return errors.New("Node authorization fixture denied startup")
				case "cancel-after-permit":
					cancel()
				case "changed-journal":
					file := filepath.Join(config.ExecutionFloor.State.Directory, durableStateFileName)
					document, err := os.ReadFile(file)
					if err != nil {
						t.Fatal(err)
					}
					if err := os.WriteFile(file, append(document, ' '), 0o600); err != nil {
						t.Fatal(err)
					}
				}
				return nil
			}
			if scenario == "missing-gate" {
				config.BackendStartupGate = nil
			}
			server, err := modelruntime.StartRuntimeServer(ctx, config)
			if scenario == "permit" {
				if err != nil || factoryCalls != len(config.Manifest.Runtimes) || gateCalls != 1 {
					t.Fatalf("permitted AUX startup failed: factories=%d gates=%d err=%v", factoryCalls, gateCalls, err)
				}
				if err := server.Close(); err != nil {
					t.Fatal(err)
				}
			} else {
				if err == nil || server != nil || factoryCalls != 0 {
					t.Fatalf("failed gate dispatched a factory: factories=%d err=%v", factoryCalls, err)
				}
				if scenario == "missing-gate" && (!errors.Is(err, modelruntime.ErrBackendStartupGateRequired) || gateCalls != 0 || epochCalls != 0) {
					t.Fatalf("missing gate rejected after startup side effects: %v epochs=%d", err, epochCalls)
				}
			}
			if scenario == "changed-journal" {
				return
			}
			retained, err := modelruntime.PrepareExecutionJournal(t.Context(), config.Manifest, config.Validator, *config.ExecutionFloor.State)
			if err != nil {
				t.Fatal(err)
			}
			if scenario == "missing-gate" {
				if retained.BackendLifecycle.State != modelruntime.BackendLifecycleUnstarted {
					t.Fatal("missing gate configuration consumed startup intent")
				}
				return
			}
			if retained.BackendLifecycle.State != modelruntime.BackendLifecycleUnresolved {
				t.Fatal("gate outcome erased unresolved startup intent")
			}
			before := readDurableExecutionState(t, config.ExecutionFloor.State.Directory).BackendLifecycle
			priorCalls := factoryCalls
			config.BackendStartupGate = nil
			recovered, err := modelruntime.StartRuntimeServer(t.Context(), config)
			if err != nil {
				t.Fatalf("recovery required a Node gate: %v", err)
			}
			if err := recovered.Close(); err != nil {
				t.Fatal(err)
			}
			after := readDurableExecutionState(t, config.ExecutionFloor.State.Directory).BackendLifecycle
			if factoryCalls != priorCalls || *before != *after {
				t.Fatal("recovery restarted a backend or replaced its retained startup nonce")
			}
		})
	}
}

func TestBackendStartupRequestRequiresCanonicalBinding(t *testing.T) {
	config := journalRuntimeServerConfig(t)
	journal, err := modelruntime.PrepareExecutionJournal(t.Context(), config.Manifest, config.Validator, *config.ExecutionFloor.State)
	if err != nil {
		t.Fatal(err)
	}
	config.ExecutionFloor.State.Initialize = false
	config.RegistryBinding, config.RegistryVerifier = runtimeRegistryBinding(t, config, journal, nil)
	var encoded []byte
	config.BackendStartupGate = func(_ context.Context, request modelruntime.BackendStartupRequest) error {
		var err error
		encoded, err = modelruntime.EncodeBackendStartupRequest(request)
		if err != nil {
			t.Fatal(err)
		}
		decoded, err := modelruntime.ParseBackendStartupRequest(encoded)
		if err != nil || decoded != request {
			t.Fatalf("canonical startup request changed: %+v %v", decoded, err)
		}
		return modelruntime.ErrBackendStartupDenied
	}
	if _, err := modelruntime.StartRuntimeServer(t.Context(), config); !errors.Is(err, modelruntime.ErrBackendStartupDenied) {
		t.Fatal(err)
	}
	for _, document := range [][]byte{nil, append(bytes.Clone(encoded), ' '), append(bytes.Clone(encoded), encoded...),
		bytes.Replace(encoded, []byte(`"schema_version":1`), []byte(`"schema_version":1,"schema_version":1`), 1),
		bytes.Replace(encoded, []byte(`"schema_version":1`), []byte(`"schema_version":2`), 1),
		bytes.Replace(encoded, []byte(`"schema_version":1`), []byte(`"schema_version":1,"unknown":true`), 1), bytes.Repeat([]byte{' '}, 4097)} {
		if request, err := modelruntime.ParseBackendStartupRequest(document); err == nil || request != (modelruntime.BackendStartupRequest{}) {
			t.Fatalf("untrusted startup declaration accepted: %q", document)
		}
	}
}
