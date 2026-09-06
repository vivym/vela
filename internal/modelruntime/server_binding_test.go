package modelruntime_test

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/journalbinding"
	"github.com/vivym/vela/internal/modelruntime"
	"github.com/vivym/vela/internal/stageauthority"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func runtimeRegistryBinding(t *testing.T, config modelruntime.RuntimeServerConfig, journal modelruntime.ExecutionJournalStatus, mutate func(*velav1.WorkerBootstrapBinding)) (*velav1.WorkerBootstrapBinding, *journalbinding.Verifier) {
	t.Helper()
	seed := bytes.Repeat([]byte{17}, ed25519.SeedSize)
	signer, err := journalbinding.NewSigner("registry", seed)
	if err != nil {
		t.Fatal(err)
	}
	verifier, err := journalbinding.NewVerifier(map[string][]byte{"registry": ed25519.NewKeyFromSeed(seed).Public().(ed25519.PublicKey)})
	if err != nil {
		t.Fatal(err)
	}
	requestID := uuid.NewString()
	value := &velav1.WorkerBootstrapBinding{SchemaVersion: journalbinding.SchemaVersion,
		Claim: &velav1.WorkerBootstrapClaim{RequestId: requestID,
			WorkerInstanceId: config.Manifest.WorkerInstanceID, WorkerInstanceEpoch: config.Manifest.WorkerInstanceEpoch,
			WorkerMemberId: config.Manifest.WorkerMemberID, WorkerMemberEpoch: config.Manifest.WorkerMemberEpoch,
			NodeIdentity: "cpu-node", ActorIdentity: "node-agent/cpu-node", BundleDigest: bytes.Repeat([]byte{1}, 32), ClaimedAt: timestamppb.Now()},
		Pair: &velav1.WorkerBootstrapJournalPair{RequestId: requestID, ActorIdentity: "node-agent/cpu-node",
			WorkerJournalId: uuid.NewString(), WorkerScope: bytes.Repeat([]byte{2}, 32), RuntimeJournalId: journal.JournalID.String(),
			RuntimeScope: journal.Scope[:], RecordedAt: timestamppb.Now()}}
	if mutate != nil {
		mutate(value)
	}
	signed, err := signer.Sign(value)
	if err != nil {
		t.Fatal(err)
	}
	return signed, verifier
}

func TestRuntimeServerRejectsUnboundJournalBeforeEpochAndBackend(t *testing.T) {
	for _, fault := range []string{"signature", "journal-id", "scope", "worker", "worker-epoch", "member", "member-epoch", "verifier", "binding", "no-state", "initialize", "upgrade", "replacement"} {
		t.Run(fault, func(t *testing.T) {
			config := journalRuntimeServerConfig(t)
			journal, err := modelruntime.PrepareExecutionJournal(t.Context(), config.Manifest, config.Validator, *config.ExecutionFloor.State)
			if err != nil {
				t.Fatal(err)
			}
			config.ExecutionFloor.State.Initialize = false
			config.RegistryBinding, config.RegistryVerifier = runtimeRegistryBinding(t, config, journal, func(v *velav1.WorkerBootstrapBinding) {
				switch fault {
				case "journal-id":
					v.Pair.RuntimeJournalId = uuid.NewString()
				case "scope":
					v.Pair.RuntimeScope[0] ^= 1
				case "worker":
					v.Claim.WorkerInstanceId = uuid.NewString()
				case "worker-epoch":
					v.Claim.WorkerInstanceEpoch++
				case "member":
					v.Claim.WorkerMemberId = uuid.NewString()
				case "member-epoch":
					v.Claim.WorkerMemberEpoch++
				}
			})
			state := *config.ExecutionFloor.State
			switch fault {
			case "signature":
				config.RegistryBinding.Signature[0] ^= 1
			case "verifier":
				config.RegistryVerifier = nil
			case "binding":
				config.RegistryBinding = nil
			case "no-state":
				config.ExecutionFloor = nil
			case "initialize":
				config.ExecutionFloor.State.Initialize = true
			case "upgrade":
				config.ExecutionFloor.State.UpgradeV3 = true
			case "replacement":
				state.Directory = privateExecutionStateDirectory(t)
				if _, err := modelruntime.PrepareExecutionJournal(t.Context(), config.Manifest, config.Validator, modelruntime.ExecutionFloorStateConfig{Directory: state.Directory, Initialize: true}); err != nil {
					t.Fatal(err)
				}
				config.ExecutionFloor.State.Directory = state.Directory
			}
			before, err := os.ReadFile(filepath.Join(state.Directory, "execution-admission.json"))
			if err != nil {
				t.Fatal(err)
			}
			epochCalls, backendCalls := 0, 0
			config.EpochStore = modelruntime.EpochStoreFunc(func(stageauthority.RuntimeBinding) (int64, error) {
				epochCalls++
				return 1, nil
			})
			config.BackendFactory = func(context.Context, modelruntime.LaunchRuntime, stageauthority.RuntimeBinding, modelruntime.ProcessBackendConfig) (modelruntime.Backend, error) {
				backendCalls++
				return modelruntime.NewFakeEncoderRuntime(), nil
			}
			server, err := modelruntime.StartRuntimeServer(t.Context(), config)
			if server != nil {
				_ = server.Close()
			}
			if err == nil || server != nil || epochCalls != 0 || backendCalls != 0 {
				t.Fatalf("binding rejection happened after execution startup: %v epochs=%d backends=%d", err, epochCalls, backendCalls)
			}
			if _, err := os.Lstat(config.SocketPath); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("rejected startup published socket: %v", err)
			}
			after, err := os.ReadFile(filepath.Join(state.Directory, "execution-admission.json"))
			if err != nil || !bytes.Equal(before, after) {
				t.Fatal("failed binding changed journal history")
			}
			if _, err := modelruntime.PrepareExecutionJournal(t.Context(), config.Manifest, config.Validator, state); err != nil {
				t.Fatalf("failed startup retained journal lock: %v", err)
			}
		})
	}
}

func TestRuntimeServerRetainsRegistryBoundJournalThroughStartupAndServing(t *testing.T) {
	config := journalRuntimeServerConfig(t)
	journal, err := modelruntime.PrepareExecutionJournal(t.Context(), config.Manifest, config.Validator, *config.ExecutionFloor.State)
	if err != nil {
		t.Fatal(err)
	}
	config.ExecutionFloor.State.Initialize = false
	config.RegistryBinding, config.RegistryVerifier = runtimeRegistryBinding(t, config, journal, nil)
	assertLocked := func() {
		t.Helper()
		if _, err := modelruntime.PrepareExecutionJournal(t.Context(), config.Manifest, config.Validator, *config.ExecutionFloor.State); err == nil {
			t.Fatal("Registry-bound journal was not held exclusively")
		}
	}
	epochStore, factory := config.EpochStore, config.BackendFactory
	epochs, backends := 0, 0
	config.EpochStore = modelruntime.EpochStoreFunc(func(binding stageauthority.RuntimeBinding) (int64, error) {
		epochs++
		assertLocked()
		return epochStore.Next(binding)
	})
	config.BackendFactory = func(ctx context.Context, runtime modelruntime.LaunchRuntime, binding stageauthority.RuntimeBinding, backend modelruntime.ProcessBackendConfig) (modelruntime.Backend, error) {
		backends++
		assertLocked()
		return factory(ctx, runtime, binding, backend)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	server, err := modelruntime.StartRuntimeServer(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = server.Close() })
	assertLocked()
	if epochs != len(config.Manifest.Runtimes) || backends != epochs {
		t.Fatalf("bound runtimes did not start: %d %d", epochs, backends)
	}
	if err := server.Close(); err != nil {
		t.Fatal(err)
	}
	recovered, err := modelruntime.PrepareExecutionJournal(t.Context(), config.Manifest, config.Validator, *config.ExecutionFloor.State)
	if err != nil || recovered != journal {
		t.Fatalf("shutdown changed bound journal or retained ownership: %+v %v", recovered, err)
	}
}
