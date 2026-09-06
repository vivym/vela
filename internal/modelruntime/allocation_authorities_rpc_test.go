package modelruntime_test

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/vivym/vela/internal/modelruntime"
	"github.com/vivym/vela/internal/modelruntimetransport"
	"github.com/vivym/vela/internal/stageauthority"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func TestAllocationAuthoritiesRPCRecoversHistoryWithoutBackendEntry(t *testing.T) {
	for _, outcome := range []string{"not-applied", "applied-response-lost", "confirmed"} {
		t.Run(outcome, func(t *testing.T) {
			directory := privateExecutionStateDirectory(t)
			backend := &renewalRecoveryBackend{FakeRuntime: modelruntime.NewFakeDiTRuntime(), failStatus: outcome != "confirmed",
				applyRenewal: outcome != "not-applied", calls: make(chan cancellationAuthorityCall, 2)}
			f := newExecutionDrainFixture(t, directory, backend)
			original := f.authorities[0]
			prepareFloorRuntime(t, f.supervisor, original)
			f.clock.Advance(time.Second)
			accepted := renewWatchdogAuthority(t, f.signer, original, f.clock.Now())
			response, err := f.supervisor.Status(t.Context(), &velav1.ModelRuntimeServiceStatusRequest{Authority: accepted})
			if err != nil || (response.GetDecision() == velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_ACCEPTED) != (outcome == "confirmed") {
				t.Fatalf("renewal outcome: %v %v", response, err)
			}
			confirmed := original
			if outcome == "confirmed" {
				confirmed = accepted
			}
			f.supervisor.Close()
			recovered := durableExecutionFixture(t, directory, false, "", 10, f.clock.Now().Add(2*time.Minute))
			client := dialExecutionFloorServer(t, recovered.supervisor)
			query := proto.Clone(accepted).(*velav1.StageAuthority)
			query.StageVersion++
			query, err = recovered.signer.Sign(query)
			if err != nil {
				t.Fatal(err)
			}
			scope := &velav1.ModelRuntimeExecutionDrainScope{SchemaVersion: 1,
				Identity: discoverExecutionFloorIdentity(t, client, recovered.bindings[0]), Authority: query}
			digest, err := stageauthority.Digest(query)
			if err != nil {
				t.Fatal(err)
			}
			before, err := os.ReadFile(filepath.Join(directory, durableStateFileName))
			if err != nil {
				t.Fatal(err)
			}
			for range 2 {
				read, err := client.InspectStageAllocationAuthorities(t.Context(), &velav1.ModelRuntimeServiceInspectStageAllocationAuthoritiesRequest{Scope: scope})
				if err != nil || modelruntimetransport.ValidateAllocationAuthoritiesResponse(recovered.validator, scope, digest, read) != nil ||
					!proto.Equal(read.GetAuthorities().GetOriginal(), original) || !proto.Equal(read.GetAuthorities().GetAccepted(), accepted) ||
					!proto.Equal(read.GetAuthorities().GetConfirmed(), confirmed) {
					t.Fatalf("historical candidates changed during UDS recovery: %v %v", read, err)
				}
				read.Authorities.Original.Signature[0] ^= 1
				read.Authorities.Accepted.Signature[0] ^= 1
				read.Authorities.Confirmed.Signature[0] ^= 1
			}
			after, err := os.ReadFile(filepath.Join(directory, durableStateFileName))
			if err != nil || !bytes.Equal(before, after) {
				t.Fatalf("history inspection modified the journal: %v", err)
			}
			drain, err := client.InspectStageAllocationDrain(t.Context(), &velav1.ModelRuntimeServiceInspectStageAllocationDrainRequest{Scope: scope})
			if err != nil || drain.GetResult().GetCheckpoint() != nil {
				t.Fatalf("candidate history became drain proof: %v %v", drain, err)
			}
			if response, err := client.DrainStageExecution(t.Context(), &velav1.ModelRuntimeServiceDrainStageExecutionRequest{Scope: scope}); response != nil || status.Code(err) != codes.InvalidArgument {
				t.Fatalf("historical candidates entered a replacement backend: %v %v", response, err)
			}
			live, err := client.InspectAllocationExecution(t.Context(), allocationInspectionRequest(query))
			if err != nil || live.GetObservedAuthority() != nil || live.GetInspection() != nil {
				t.Fatalf("journal history became a live observation: %v %v", live, err)
			}
			assertRecoveryDrainBlocks(t, recovered, recovered.authority(t, 1, 12))
			if recovered.backend.calls.Load() != 0 {
				t.Fatal("history recovery entered the replacement backend")
			}
			scope.Authority = proto.Clone(query).(*velav1.StageAuthority)
			scope.Authority.ExecutionNonce[0] ^= 1
			scope.Authority, err = recovered.signer.Sign(scope.Authority)
			if err != nil {
				t.Fatal(err)
			}
			unknown, err := client.InspectStageAllocationAuthorities(t.Context(), &velav1.ModelRuntimeServiceInspectStageAllocationAuthoritiesRequest{Scope: scope})
			if err != nil || unknown.GetDecision() != velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_ACCEPTED || unknown.GetAuthorities() != nil {
				t.Fatalf("unseen execution inherited historical candidates: %v %v", unknown, err)
			}
		})
	}
}

func TestAllocationAuthoritiesRPCLegacyRemainsOriginalOnly(t *testing.T) {
	directory := privateExecutionStateDirectory(t)
	f := durableExecutionFixture(t, directory, true, "", 9, time.Time{})
	prepareFloorRuntime(t, f.supervisor, f.authorities[0])
	f.supervisor.Close()
	legacy := readDurableExecutionState(t, directory)
	legacy.SchemaVersion = 4
	legacy.Executions = withoutRetainedCandidates(t, legacy.Executions)
	if err := os.WriteFile(filepath.Join(directory, durableStateFileName), encodeDurableExecutionState(t, legacy), 0o600); err != nil {
		t.Fatal(err)
	}
	upgraded, err := executionFloorFixtureWithState(t, "", &modelruntime.ExecutionFloorStateConfig{Directory: directory, UpgradeV4: true}, 10, f.clock.Now())
	if err != nil {
		t.Fatal(err)
	}
	client := dialExecutionFloorServer(t, upgraded.supervisor)
	scope := &velav1.ModelRuntimeExecutionDrainScope{SchemaVersion: 1, Identity: discoverExecutionFloorIdentity(t, client, upgraded.bindings[0]), Authority: f.authorities[0]}
	read, err := client.InspectStageAllocationAuthorities(t.Context(), &velav1.ModelRuntimeServiceInspectStageAllocationAuthoritiesRequest{Scope: scope})
	if err != nil || !proto.Equal(read.GetAuthorities().GetOriginal(), f.authorities[0]) || read.GetAuthorities().GetAccepted() != nil || read.GetAuthorities().GetConfirmed() != nil {
		t.Fatalf("legacy UDS read inferred renewal history: %v %v", read, err)
	}
	assertRecoveryDrainBlocks(t, upgraded, upgraded.authority(t, 1, 12))
}

func TestAllocationAuthoritiesRPCRejectsInvalidScopeAndUnavailableJournal(t *testing.T) {
	for _, fault := range []string{"nil", "unknown-request", "missing-scope", "schema", "unknown-scope", "signature", "size", "future", "stale-owner", "topology", "cancel", "closed-journal"} {
		t.Run(fault, func(t *testing.T) {
			f := durableExecutionFixture(t, privateExecutionStateDirectory(t), true, "", 9, time.Time{})
			client := dialExecutionFloorServer(t, f.supervisor)
			request := &velav1.ModelRuntimeServiceInspectStageAllocationAuthoritiesRequest{Scope: &velav1.ModelRuntimeExecutionDrainScope{
				SchemaVersion: 1, Identity: discoverExecutionFloorIdentity(t, client, f.bindings[0]), Authority: proto.Clone(f.authorities[0]).(*velav1.StageAuthority),
			}}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			switch fault {
			case "nil":
				request = nil
			case "unknown-request":
				request.ProtoReflect().SetUnknown([]byte{0x78, 1})
			case "missing-scope":
				request.Scope = nil
			case "schema":
				request.Scope.SchemaVersion++
			case "unknown-scope":
				request.Scope.ProtoReflect().SetUnknown([]byte{0x78, 1})
			case "signature":
				request.Scope.Authority.Signature[0] ^= 1
			case "size":
				request.Scope.Authority.Signature = make([]byte, 65<<10)
			case "future":
				request.Scope.Authority = renewWatchdogAuthority(t, f.signer, request.Scope.Authority, f.clock.Now().Add(time.Hour))
			case "stale-owner":
				request.Scope.Identity.ModelRuntimeEpoch++
			case "topology":
				request.Scope.Authority.Devices[0].DeviceEpoch++
				var err error
				request.Scope.Authority, err = f.signer.Sign(request.Scope.Authority)
				if err != nil {
					t.Fatal(err)
				}
			case "cancel":
				cancel()
			case "closed-journal":
				f.supervisor.Close()
			}
			response, err := f.supervisor.InspectStageAllocationAuthorities(ctx, request)
			if err == nil || response != nil || f.backend.calls.Load() != 0 {
				t.Fatalf("invalid history query accepted: %v %v", response, err)
			}
		})
	}
}
