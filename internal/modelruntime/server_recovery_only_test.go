package modelruntime_test

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/modelruntime"
	"github.com/vivym/vela/internal/modelruntimetransport"
	"github.com/vivym/vela/internal/stageauthority"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/protobuf/proto"
)

func TestRuntimeServerRecoveryWithholdsAllBackendsUntilHistoryIsDrained(t *testing.T) {
	for _, state := range []string{"pending", "terminal-pending", "legacy-pending", "profile-replacement", "mixed", "empty", "drained"} {
		t.Run(state, func(t *testing.T) {
			directory := privateExecutionStateDirectory(t)
			f := durableExecutionFixture(t, directory, true, "", 9, time.Now())
			switch state {
			case "empty":
			case "drained", "mixed":
				readyDrainOutput(t, f, f.backend.FakeRuntime, f.authorities[0])
				sealed, err := f.supervisor.SealOutput(t.Context(), &velav1.ModelRuntimeServiceSealOutputRequest{Authority: f.authorities[0]})
				if err != nil || sealed.GetReceipt() == nil {
					t.Fatalf("initial drain: %v %v", sealed, err)
				}
				if state == "mixed" {
					prepareFloorRuntime(t, f.supervisor, f.authorities[1])
				}
			default:
				prepareFloorRuntime(t, f.supervisor, f.authorities[0])
				if state == "terminal-pending" {
					if _, err := f.supervisor.InstallExecutionFloor(t.Context(), f.disposition(t)); err != nil {
						t.Fatal(err)
					}
				}
			}
			if err := f.supervisor.Shutdown(); err != nil {
				t.Fatal(err)
			}
			config := recoveredRuntimeServerConfig(t, f, directory)
			if state == "legacy-pending" {
				legacy := readDurableExecutionState(t, directory)
				legacy.SchemaVersion = 4
				legacy.Executions = withoutRetainedCandidates(t, legacy.Executions)
				if err := os.WriteFile(filepath.Join(directory, durableStateFileName), encodeDurableExecutionState(t, legacy), 0o600); err != nil {
					t.Fatal(err)
				}
				upgrade := *config.ExecutionFloor.State
				upgrade.UpgradeV4 = true
				if _, err := modelruntime.PrepareExecutionJournal(t.Context(), config.Manifest, config.Validator, upgrade); err != nil {
					t.Fatal(err)
				}
			}
			if state == "profile-replacement" {
				for index := range config.Manifest.Runtimes {
					config.Manifest.Runtimes[index].ModelResidencyID = uuid.NewString()
					config.Manifest.Runtimes[index].StageProfileRevisionID = uuid.NewString()
					config.Manifest.Runtimes[index].RuntimeIdentity += "-replacement"
				}
			}
			journal, err := modelruntime.PrepareExecutionJournal(t.Context(), config.Manifest, config.Validator, *config.ExecutionFloor.State)
			if err != nil {
				t.Fatal(err)
			}
			config.RegistryBinding, config.RegistryVerifier = runtimeRegistryBinding(t, config, journal, nil)
			factory, calls := config.BackendFactory, 0
			config.BackendFactory = func(ctx context.Context, runtime modelruntime.LaunchRuntime, binding stageauthority.RuntimeBinding, backend modelruntime.ProcessBackendConfig) (modelruntime.Backend, error) {
				calls++
				return factory(ctx, runtime, binding, backend)
			}
			before, err := os.ReadFile(filepath.Join(directory, durableStateFileName))
			if err != nil {
				t.Fatal(err)
			}
			server, err := modelruntime.StartRuntimeServer(t.Context(), config)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = server.Close() })
			client, err := modelruntimetransport.Dial(t.Context(), modelruntimetransport.Config{SocketPath: config.SocketPath, ExpectedUID: uint32(os.Geteuid())})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = client.Close() })
			identities, err := client.DiscoverRuntimeIdentities(t.Context(), &velav1.ModelRuntimeServiceDiscoverRuntimeIdentitiesRequest{
				WorkerInstanceId: config.Manifest.WorkerInstanceID, WorkerInstanceEpoch: config.Manifest.WorkerInstanceEpoch,
				WorkerMemberId: config.Manifest.WorkerMemberID, WorkerMemberEpoch: config.Manifest.WorkerMemberEpoch,
			})
			if err != nil || len(identities.GetIdentities()) != 2 || !proto.Equal(identities.GetJournalBinding(), config.RegistryBinding) {
				t.Fatalf("recovery identity or Registry binding unavailable: %v %v", identities, err)
			}
			pending := state != "empty" && state != "drained"
			wantCalls := 0
			if !pending {
				wantCalls = len(config.Manifest.Runtimes)
			}
			if calls != wantCalls {
				t.Fatalf("startup ignored pending history: calls=%d want=%d", calls, wantCalls)
			}
			if _, err := modelruntime.PrepareExecutionJournal(t.Context(), config.Manifest, config.Validator, *config.ExecutionFloor.State); err == nil {
				t.Fatal("recovery server released its journal lifetime lock")
			}
			for _, identity := range identities.GetIdentities() {
				for _, check := range []velav1.ModelRuntimeReadinessCheck{
					velav1.ModelRuntimeReadinessCheck_MODEL_RUNTIME_READINESS_CHECK_DEVICE,
					velav1.ModelRuntimeReadinessCheck_MODEL_RUNTIME_READINESS_CHECK_BACKEND,
					velav1.ModelRuntimeReadinessCheck_MODEL_RUNTIME_READINESS_CHECK_MODEL_WARMUP,
					velav1.ModelRuntimeReadinessCheck_MODEL_RUNTIME_READINESS_CHECK_CANARY,
				} {
					ready, err := client.ProbeReadiness(t.Context(), &velav1.ModelRuntimeServiceProbeReadinessRequest{Identity: identity, Check: check})
					if err != nil || ready.GetReady() == pending || pending && !strings.Contains(ready.GetDetail(), modelruntime.ErrExecutionDrainUnproven.Error()) {
						t.Fatalf("recovery readiness changed: %v %v", ready, err)
					}
				}
				if pending {
					assertRecoveryServerDeniesExecution(t, f, client, identity)
				}
			}
			if state != "empty" {
				original := f.authorities[0]
				if state == "mixed" {
					original = f.authorities[1]
				}
				scope := &velav1.ModelRuntimeExecutionDrainScope{SchemaVersion: 1, Identity: identities.Identities[0], Authority: original}
				read, err := client.InspectStageAllocationAuthorities(t.Context(), &velav1.ModelRuntimeServiceInspectStageAllocationAuthoritiesRequest{Scope: scope})
				if err != nil || !proto.Equal(read.GetAuthorities().GetOriginal(), original) || state == "legacy-pending" && (read.GetAuthorities().GetAccepted() != nil || read.GetAuthorities().GetConfirmed() != nil) {
					t.Fatalf("recovery lost history: %v %v", read, err)
				}
				drain, err := client.InspectStageAllocationDrain(t.Context(), &velav1.ModelRuntimeServiceInspectStageAllocationDrainRequest{Scope: scope})
				if err != nil || (drain.GetResult().GetCheckpoint() == nil) != pending {
					t.Fatalf("recovery changed physical drain evidence: %v %v", drain, err)
				}
			}
			after, err := os.ReadFile(filepath.Join(directory, durableStateFileName))
			if err != nil || !bytes.Equal(before, after) {
				t.Fatalf("recovery reads or denied execution changed history: %v", err)
			}
			if err := server.Close(); err != nil {
				t.Fatal(err)
			}
			retained, err := modelruntime.PrepareExecutionJournal(t.Context(), config.Manifest, config.Validator, *config.ExecutionFloor.State)
			if err != nil || retained != journal || calls != wantCalls {
				t.Fatalf("recovery shutdown changed retained state: %+v %v calls=%d", retained, err, calls)
			}
		})
	}
}

func assertRecoveryServerDeniesExecution(t *testing.T, f *executionFloorFixture, client velav1.ModelRuntimeServiceClient, identity *velav1.ModelRuntimeIdentity) {
	t.Helper()
	query := f.authority(t, 0, 12)
	query.Members[0].ModelRuntimeEpoch = identity.ModelRuntimeEpoch
	query.ModelResidencyId, query.ModelRuntimeIdentity, query.StageProfileRevisionId = identity.ModelResidencyId, identity.RuntimeIdentity, identity.StageProfileRevisionId
	query, err := f.signer.Sign(query)
	if err != nil {
		t.Fatal(err)
	}
	prepared, err := client.PrepareStage(t.Context(), &velav1.ModelRuntimeServicePrepareStageRequest{Authority: query, ExecutionSpec: runtimeExecutionSpec()})
	if err != nil || prepared.GetDecision() != velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_REJECTED || !strings.Contains(prepared.GetDetail(), modelruntime.ErrExecutionDrainUnproven.Error()) {
		t.Fatalf("recovery reader admitted a fresh current-epoch Prepare: %v %v", prepared, err)
	}
	started, err := client.StartStage(t.Context(), &velav1.ModelRuntimeServiceStartStageRequest{Authority: query})
	if err != nil || started.GetDecision() != velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_STALE {
		t.Fatalf("recovery reader started execution: %v %v", started, err)
	}
	observed, err := client.Status(t.Context(), &velav1.ModelRuntimeServiceStatusRequest{Authority: query})
	if err != nil || observed.GetDecision() != velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_STALE {
		t.Fatalf("recovery reader renewed execution: %v %v", observed, err)
	}
	canceled, err := client.CancelStage(t.Context(), &velav1.ModelRuntimeServiceCancelStageRequest{Authority: query, Reason: velav1.ModelRuntimeCancelReason_MODEL_RUNTIME_CANCEL_REASON_CONTROL_PLANE_STOP})
	if err != nil || canceled.GetCancellationAcknowledged() {
		t.Fatalf("recovery reader claimed cancellation: %v %v", canceled, err)
	}
	sealed, err := client.SealOutput(t.Context(), &velav1.ModelRuntimeServiceSealOutputRequest{Authority: query})
	if err != nil || sealed.GetReceipt() != nil {
		t.Fatalf("recovery reader claimed output sealing: %v %v", sealed, err)
	}
}
