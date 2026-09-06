package stageworkeragent_test

import (
	"context"
	"testing"
	"time"

	"github.com/vivym/vela/internal/stageauthority"
	"github.com/vivym/vela/internal/stageworkeragent"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestTerminalRetirementRecoversLiveMembersWithDifferentRenewalOutcomes(t *testing.T) {
	for _, fault := range []string{"complete", "partial-drain", "stop-inspection"} {
		t.Run(fault, func(t *testing.T) {
			f := newAssignmentFloorFixture(t)
			gate := f.open(t)
			completeAdmissionInputs(t, beginAdmission(t, gate, f.assignment, f.acquireID))
			group := startFloorCollectorRuntimes(t, f, t.TempDir(), true, false)
			first := proto.Clone(f.assignment.Authority).(*velav1.StageAuthority)
			for _, client := range group.clients {
				if response, err := client.PrepareStage(t.Context(), &velav1.ModelRuntimeServicePrepareStageRequest{Authority: first, ExecutionSpec: f.assignment.ExecutionSpec}); err != nil || response.GetDecision() != velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_ACCEPTED {
					t.Fatalf("prepare: %v %v", response, err)
				}
			}
			renewal := proto.Clone(f.assignment).(*velav1.StageAssignment)
			renewal.Authority.StageVersion++
			renewal.Authority.IssuedAt = timestamppb.New(first.IssuedAt.AsTime().Add(time.Second))
			renewal.Authority.ExpiresAt = timestamppb.New(first.ExpiresAt.AsTime().Add(time.Second))
			f.clock.Add(int64(time.Second))
			f.sign(t, renewal)
			completeAdmissionInputs(t, beginAdmission(t, gate, renewal, f.acquireID))
			for index, client := range group.clients {
				group.activeBackends[index].renewalResponseFault.Store(int32(index + 1))
				group.activeBackends[index].stopOnCancel.Store(true)
				if response, err := client.Status(t.Context(), &velav1.ModelRuntimeServiceStatusRequest{Authority: renewal.Authority}); err != nil || response.GetDecision() != velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_REJECTED {
					t.Fatalf("renewal fault: %v %v", response, err)
				}
			}
			digest, err := stageauthority.Digest(renewal.Authority)
			if err != nil {
				t.Fatal(err)
			}
			f.disposition.OriginalAuthorityDigest = digest[:]
			f.disposition.StageVersion++
			f.disposition.ObservedAt = renewal.Authority.IssuedAt
			f.signDisposition(t)
			paths := retirementScratch(t, f)
			queries := map[string]*velav1.StageAuthority{first.StageAllocationId: renewal.Authority}
			retirer := terminalRetirer(t, gate, f)
			if fault != "complete" {
				group.activeBackends[1].failDrain.Store(fault == "partial-drain")
				group.activeBackends[1].failInspectAfterDrain.Store(fault == "stop-inspection")
				result, err := retirer.Retire(t.Context(), f.disposition, queries, drainCollectorTargets(f))
				if err == nil || result.Phase != stageworkeragent.TerminalRetirementIntent {
					t.Fatalf("partial proof retired scratch: %+v %v", result, err)
				}
				assertRetirementScratch(t, paths, true)
				group.activeBackends[1].failDrain.Store(false)
				group.activeBackends[1].failInspectAfterDrain.Store(false)
			}
			result, err := retirer.Retire(t.Context(), f.disposition, queries, drainCollectorTargets(f))
			if err != nil || result.Phase != stageworkeragent.TerminalRetirementRetired {
				t.Fatalf("live backend recovery did not retire with complete exact proof: %+v %v", result, err)
			}
			assertRetirementScratch(t, paths, false)
			for index, client := range group.clients {
				read, err := client.InspectStageAllocationDrain(t.Context(), &velav1.ModelRuntimeServiceInspectStageAllocationDrainRequest{
					Scope: &velav1.ModelRuntimeExecutionDrainScope{SchemaVersion: 1, Identity: drainCollectorTargets(f)[f.config.Members[index].ID], Authority: renewal.Authority},
				})
				actual := first
				if index == 1 {
					actual = renewal.Authority
				}
				if err != nil || !proto.Equal(read.GetResult().GetCheckpoint().GetAuthority(), actual) {
					t.Fatalf("member %d lost actual signed drain identity: %v %v", index, read, err)
				}
				if group.activeBackends[index].closed.Load() || group.activeBackends[index].cancelCalls.Load() != 1 {
					t.Fatal("recovery unloaded or repeatedly canceled backend")
				}
			}
			if group.activeBackends[0].drainCalls.Load() != 1 {
				t.Fatal("partial recovery ignored the first member's durable checkpoint")
			}
			if fault == "stop-inspection" && group.activeBackends[1].drainCalls.Load() != 1 {
				t.Fatal("stop observation retry repeated a persisted drain")
			}
			group.close()
			if err := gate.Close(); err != nil {
				t.Fatal(err)
			}
			gate = f.open(t)
			if recovered, err := terminalRetirer(t, gate, f).Resume(t.Context(), result.StageRunID); err != nil || recovered.Phase != stageworkeragent.TerminalRetirementRetired {
				t.Fatalf("complete proof did not resume offline: %+v %v", recovered, err)
			}
		})
	}
}

func TestTerminalLiveRecoveryRequiresInputExclusionAndAllFloors(t *testing.T) {
	for _, fault := range []string{"active-input", "unknown-input", "lost-floor-reply"} {
		t.Run(fault, func(t *testing.T) {
			f := newAssignmentFloorFixture(t)
			gate := f.open(t)
			handle := beginAdmission(t, gate, f.assignment, f.acquireID)
			switch fault {
			case "unknown-input":
				handle.Release()
			case "lost-floor-reply":
				completeAdmissionInputs(t, handle)
			}
			group := startFloorCollectorRuntimes(t, f, t.TempDir(), true, fault == "lost-floor-reply")
			for index, client := range group.clients {
				if response, err := client.PrepareStage(t.Context(), &velav1.ModelRuntimeServicePrepareStageRequest{Authority: f.assignment.Authority, ExecutionSpec: f.assignment.ExecutionSpec}); err != nil || response.GetDecision() != velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_ACCEPTED {
					t.Fatalf("prepare: %v %v", response, err)
				}
				group.activeBackends[index].stopOnCancel.Store(true)
			}
			paths := retirementScratch(t, f)
			queries := map[string]*velav1.StageAuthority{f.assignment.Authority.StageAllocationId: f.assignment.Authority}
			retirer := terminalRetirer(t, gate, f)
			ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
			defer cancel()
			result, err := retirer.Retire(ctx, f.disposition, queries, drainCollectorTargets(f))
			if err == nil || result.Phase != stageworkeragent.TerminalRetirementIntent {
				t.Fatalf("missing prerequisite retired scratch: %+v %v", result, err)
			}
			assertRetirementScratch(t, paths, true)
			for _, backend := range group.activeBackends {
				if backend.inspectCalls.Load() != 0 || backend.cancelCalls.Load() != 0 || backend.drainCalls.Load() != 0 {
					t.Fatal("live recovery reached backend before input exclusion and every floor acknowledgement")
				}
			}
			if fault == "unknown-input" {
				return
			}
			if fault == "active-input" {
				completeAdmissionInputs(t, handle)
			}
			if result, err := retirer.Retire(t.Context(), f.disposition, queries, drainCollectorTargets(f)); err != nil || result.Phase != stageworkeragent.TerminalRetirementRetired {
				t.Fatalf("completed prerequisites did not permit recovery: %+v %v", result, err)
			}
			assertRetirementScratch(t, paths, false)
		})
	}
}
