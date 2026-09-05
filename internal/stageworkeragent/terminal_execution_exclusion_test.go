package stageworkeragent_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/vivym/vela/internal/modelruntimetransport"
	"github.com/vivym/vela/internal/stageworkeragent"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestTerminalExecutionExclusionRPCCombinesDrainAndNeverAdmittedAcrossRecovery(t *testing.T) {
	f := newFloorCollectorFixture(t)
	queries, targets := terminalDrainQueries(t, f), drainCollectorTargets(f)
	base := t.TempDir()
	group := startFloorCollectorRuntimes(t, f, base, true, false)
	agent := f.agent(t)
	a := f.assignment.Authority
	client := group.clients[0]
	prepared, err := client.PrepareStage(t.Context(), &velav1.ModelRuntimeServicePrepareStageRequest{Authority: a, ExecutionSpec: f.assignment.ExecutionSpec})
	if err != nil || prepared.GetDecision() != velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_ACCEPTED {
		t.Fatalf("prepare: %v %v", prepared, err)
	}
	started, err := client.StartStage(t.Context(), &velav1.ModelRuntimeServiceStartStageRequest{Authority: a})
	if err != nil || started.GetDecision() != velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_ACCEPTED {
		t.Fatalf("start: %v %v", started, err)
	}
	group.activeBackends[0].MarkOutputReady([]byte(`{"kind":"LATENT","path":"/local/dit.bin"}`))
	sealed, err := client.SealOutput(t.Context(), &velav1.ModelRuntimeServiceSealOutputRequest{Authority: a})
	if err != nil || sealed.GetReceipt() == nil {
		t.Fatalf("seal: %v %v", sealed, err)
	}
	if result, err := agent.CheckpointTerminalExecutionExclusions(t.Context(), f.disposition, queries, targets); err == nil || result.AllExcluded {
		t.Fatalf("missing floors claimed exclusion: %+v %v", result, err)
	}
	if floor, err := agent.InstallExecutionFloor(t.Context(), f.disposition); err != nil || !floor.AllInstalled {
		t.Fatalf("install floor: %+v %v", floor, err)
	}
	if result, err := agent.InspectTerminalExecutionExclusions(t.Context(), f.disposition, queries, targets); err == nil || result.AllExcluded {
		t.Fatalf("read-only inspection created absence proof: %+v %v", result, err)
	}
	group.dropNonAdmissionResponse.Store(true)
	if result, err := agent.CheckpointTerminalExecutionExclusions(t.Context(), f.disposition, queries, targets); err == nil || result.AllExcluded {
		t.Fatalf("lost proof reply claimed complete: %+v %v", result, err)
	}
	result, err := agent.InspectTerminalExecutionExclusions(t.Context(), f.disposition, queries, targets)
	if err != nil || !result.AllExcluded || result.RequiredAllocations != 2 || result.RequiredMembers != 2 {
		t.Fatalf("read persisted mixed proof: %+v %v", result, err)
	}
	drains, absent := 0, 0
	for _, allocation := range result.Allocations {
		for _, proof := range allocation {
			if (proof.Drain == nil) == (proof.NeverAdmitted == nil) {
				t.Fatal("exclusion merged distinct proof types")
			}
			if proof.Drain != nil {
				drains++
			} else {
				absent++
			}
		}
	}
	if drains != 1 || absent != 3 {
		t.Fatalf("proof kinds: drain=%d absent=%d", drains, absent)
	}
	if exact, err := agent.InspectTerminalExecutionDrains(t.Context(), f.disposition, queries, targets); err == nil || exact.AllDrained {
		t.Fatalf("never-admitted was converted into drain: %+v %v", exact, err)
	}
	again, err := agent.CheckpointTerminalExecutionExclusions(t.Context(), f.disposition, queries, targets)
	if err != nil || !again.AllExcluded {
		t.Fatalf("idempotent checkpoint: %+v %v", again, err)
	}
	for _, backend := range group.activeBackends {
		if backend.closed.Load() {
			t.Fatal("proof collection unloaded models")
		}
	}
	group.close()
	f.config.ExecutionFloor.Bindings = f.config.ExecutionFloor.Bindings[2:]
	for index := range f.config.ExecutionFloor.Bindings {
		f.config.ExecutionFloor.Bindings[index].Runtime.ModelRuntimeEpoch++
	}
	startFloorCollectorRuntimes(t, f, base, false, false)
	recovered, err := f.agent(t).InspectTerminalExecutionExclusions(t.Context(), f.disposition, queries, drainCollectorTargets(f))
	if err != nil || !recovered.AllExcluded {
		t.Fatalf("mixed proof recovery: %+v %v", recovered, err)
	}
	for id, allocation := range result.Allocations {
		for member, proof := range allocation {
			after := recovered.Allocations[id][member]
			if proof.Drain != nil {
				if !proto.Equal(proof.Drain.GetCheckpoint(), after.Drain.GetCheckpoint()) {
					t.Fatal("recovered drain changed")
				}
			} else if !proto.Equal(proof.NeverAdmitted.GetCheckpoint(), after.NeverAdmitted.GetCheckpoint()) {
				t.Fatal("recovered non-admission changed")
			}
		}
	}
}

func TestTerminalExecutionExclusionRejectsBadProofWithoutAlternativeSuccess(t *testing.T) {
	for _, fault := range []string{"drain-error", "drain-digest", "signature", "floor", "contract", "unknown", "late", "request-mutation"} {
		t.Run(fault, func(t *testing.T) {
			f := newFloorCollectorFixture(t)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			var writes, reads atomic.Int64
			for index := range f.config.Members {
				f.config.Members[index].Client = &exclusionCollectorClient{
					drain: func(scope *velav1.ModelRuntimeExecutionDrainScope) (*velav1.ModelRuntimeExecutionDrainResult, error) {
						result := drainCollectorReply(scope)
						if fault == "drain-error" {
							return nil, errors.New("transport unavailable")
						}
						result.Checkpoint = nil
						if fault == "drain-digest" {
							result.AuthorityDigest[0] ^= 1
						}
						return result, nil
					},
					read: func(scope *velav1.ModelRuntimeExecutionDrainScope) (*velav1.ModelRuntimeExecutionNonAdmissionResult, error) {
						reads.Add(1)
						result := exclusionNonAdmissionReply(scope)
						switch fault {
						case "signature":
							result.Checkpoint.Authority.Signature[0] ^= 1
						case "floor":
							result.Checkpoint.InstalledCutoff = 0
						case "contract":
							result.Checkpoint.Contract = "STOPPED"
						case "unknown":
							result.ProtoReflect().SetUnknown([]byte{0x78, 1})
						case "late":
							cancel()
						case "request-mutation":
							scope.Identity.ModelRuntimeEpoch++
							result.Identity.ModelRuntimeEpoch++
						}
						return result, nil
					},
					checkpoint: func(scope *velav1.ModelRuntimeExecutionDrainScope) (*velav1.ModelRuntimeExecutionNonAdmissionResult, error) {
						writes.Add(1)
						return exclusionNonAdmissionReply(scope), nil
					},
				}
			}
			result, err := f.agent(t).CheckpointTerminalExecutionExclusions(ctx, f.disposition, terminalDrainQueries(t, f), drainCollectorTargets(f))
			if err == nil || result.AllExcluded || writes.Load() != 0 {
				t.Fatalf("invalid proof used alternate success: %+v %v writes=%d", result, err, writes.Load())
			}
			if (fault == "drain-error" || fault == "drain-digest") && reads.Load() != 0 {
				t.Fatal("invalid drain inspection fell through to another proof")
			}
		})
	}
}

func TestTerminalExecutionExclusionValidatesHistoryBeforeRPC(t *testing.T) {
	f := newFloorCollectorFixture(t)
	var calls atomic.Int64
	for index := range f.config.Members {
		f.config.Members[index].Client = &exclusionCollectorClient{drain: func(scope *velav1.ModelRuntimeExecutionDrainScope) (*velav1.ModelRuntimeExecutionDrainResult, error) {
			calls.Add(1)
			return drainCollectorReply(scope), nil
		}}
	}
	queries := terminalDrainQueries(t, f)
	delete(queries, f.disposition.Allocations[1].StageAllocationId)
	agent := f.agent(t)
	for _, operation := range []func(context.Context, *velav1.StageTerminalDisposition, map[string]*velav1.StageAuthority, map[string]*velav1.ModelRuntimeIdentity) (stageworkeragent.TerminalExecutionExclusionResult, error){agent.InspectTerminalExecutionExclusions, agent.CheckpointTerminalExecutionExclusions} {
		if result, err := operation(t.Context(), f.disposition, queries, drainCollectorTargets(f)); err == nil || result.AllExcluded || calls.Load() != 0 {
			t.Fatalf("incomplete history dispatched: %+v %v", result, err)
		}
	}
}

type exclusionCollectorClient struct {
	velav1.ModelRuntimeServiceClient
	drain      func(*velav1.ModelRuntimeExecutionDrainScope) (*velav1.ModelRuntimeExecutionDrainResult, error)
	read       func(*velav1.ModelRuntimeExecutionDrainScope) (*velav1.ModelRuntimeExecutionNonAdmissionResult, error)
	checkpoint func(*velav1.ModelRuntimeExecutionDrainScope) (*velav1.ModelRuntimeExecutionNonAdmissionResult, error)
}

func (client *exclusionCollectorClient) InspectStageAllocationDrain(_ context.Context, request *velav1.ModelRuntimeServiceInspectStageAllocationDrainRequest, _ ...grpc.CallOption) (*velav1.ModelRuntimeServiceInspectStageAllocationDrainResponse, error) {
	result, err := client.drain(request.Scope)
	return &velav1.ModelRuntimeServiceInspectStageAllocationDrainResponse{Result: result}, err
}
func (client *exclusionCollectorClient) InspectStageNonAdmission(_ context.Context, request *velav1.ModelRuntimeServiceInspectStageNonAdmissionRequest, _ ...grpc.CallOption) (*velav1.ModelRuntimeServiceInspectStageNonAdmissionResponse, error) {
	result, err := client.read(request.Scope)
	return &velav1.ModelRuntimeServiceInspectStageNonAdmissionResponse{Result: result}, err
}
func (client *exclusionCollectorClient) CheckpointStageNonAdmission(_ context.Context, request *velav1.ModelRuntimeServiceCheckpointStageNonAdmissionRequest, _ ...grpc.CallOption) (*velav1.ModelRuntimeServiceCheckpointStageNonAdmissionResponse, error) {
	result, err := client.checkpoint(request.Scope)
	return &velav1.ModelRuntimeServiceCheckpointStageNonAdmissionResponse{Result: result}, err
}

func exclusionNonAdmissionReply(scope *velav1.ModelRuntimeExecutionDrainScope) *velav1.ModelRuntimeExecutionNonAdmissionResult {
	drain := drainCollectorReply(scope)
	return &velav1.ModelRuntimeExecutionNonAdmissionResult{SchemaVersion: 1, Identity: proto.Clone(scope.Identity).(*velav1.ModelRuntimeIdentity), AuthorityDigest: drain.AuthorityDigest,
		Decision: velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_ACCEPTED,
		Checkpoint: &velav1.ModelRuntimeExecutionNonAdmissionCheckpoint{SchemaVersion: 1, Authority: proto.Clone(scope.Authority).(*velav1.StageAuthority),
			AuthorityDigest: drain.Checkpoint.AuthorityDigest, WorkerMemberId: scope.Identity.WorkerMemberId, ExecutionSequence: scope.Authority.ExecutionSequence,
			InstalledCutoff: scope.Authority.ExecutionSequence, Contract: modelruntimetransport.ExecutionNonAdmissionContract, ObservedAt: timestamppb.New(time.Now())}}
}

func TestTerminalExecutionExclusionDiscardsLateCheckpointReply(t *testing.T) {
	f := newFloorCollectorFixture(t)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	var checkpoints atomic.Int64
	for index := range f.config.Members {
		f.config.Members[index].Client = &exclusionCollectorClient{
			drain: func(scope *velav1.ModelRuntimeExecutionDrainScope) (*velav1.ModelRuntimeExecutionDrainResult, error) {
				result := drainCollectorReply(scope)
				result.Checkpoint = nil
				return result, nil
			},
			read: func(scope *velav1.ModelRuntimeExecutionDrainScope) (*velav1.ModelRuntimeExecutionNonAdmissionResult, error) {
				result := exclusionNonAdmissionReply(scope)
				result.Checkpoint = nil
				return result, nil
			},
			checkpoint: func(scope *velav1.ModelRuntimeExecutionDrainScope) (*velav1.ModelRuntimeExecutionNonAdmissionResult, error) {
				checkpoints.Add(1)
				cancel()
				return exclusionNonAdmissionReply(scope), nil
			},
		}
	}
	result, err := f.agent(t).CheckpointTerminalExecutionExclusions(ctx, f.disposition, terminalDrainQueries(t, f), drainCollectorTargets(f))
	if !errors.Is(err, context.Canceled) || result.AllExcluded || checkpoints.Load() == 0 {
		t.Fatalf("late checkpoint escaped cancellation: %+v %v", result, err)
	}
}

func TestTerminalExecutionExclusionCheckpointsResidentMemberAfterPeerProfileRetires(t *testing.T) {
	for _, peerDrained := range []bool{true, false} {
		t.Run(map[bool]string{true: "peer-drained", false: "peer-unknown"}[peerDrained], func(t *testing.T) {
			f := newFloorCollectorFixture(t)
			queries := terminalDrainQueries(t, f)
			peerID, residentID := f.config.Members[0].ID, f.config.Members[1].ID
			// The peer retains only the second profile. Its first allocation can
			// supply historical proof, but cannot create a new absence checkpoint.
			f.config.ExecutionFloor.Bindings = f.config.ExecutionFloor.Bindings[1:]
			var peerWrites, residentWrites atomic.Int64
			for index := range f.config.Members {
				id := f.config.Members[index].ID
				f.config.Members[index].Client = &exclusionCollectorClient{
					drain: func(scope *velav1.ModelRuntimeExecutionDrainScope) (*velav1.ModelRuntimeExecutionDrainResult, error) {
						result := drainCollectorReply(scope)
						if id == residentID || !peerDrained {
							result.Checkpoint = nil
						}
						return result, nil
					},
					read: func(scope *velav1.ModelRuntimeExecutionDrainScope) (*velav1.ModelRuntimeExecutionNonAdmissionResult, error) {
						result := exclusionNonAdmissionReply(scope)
						result.Checkpoint = nil
						return result, nil
					},
					checkpoint: func(scope *velav1.ModelRuntimeExecutionDrainScope) (*velav1.ModelRuntimeExecutionNonAdmissionResult, error) {
						if id == peerID {
							peerWrites.Add(1)
						} else {
							residentWrites.Add(1)
						}
						if scope.Identity.ModelResidencyId != scope.Authority.ModelResidencyId || scope.Identity.RuntimeIdentity != scope.Authority.ModelRuntimeIdentity {
							return nil, errors.New("checkpoint used historical reader instead of original resident")
						}
						return exclusionNonAdmissionReply(scope), nil
					},
				}
			}
			result, err := f.agent(t).CheckpointTerminalExecutionExclusions(t.Context(), f.disposition, queries, drainCollectorTargets(f))
			if result.AllExcluded != peerDrained || (err == nil) != peerDrained || residentWrites.Load() != 2 {
				t.Fatalf("resident proof depended on peer residency: allExcluded=%v err=%v writes=%d", result.AllExcluded, err, residentWrites.Load())
			}
			for _, allocation := range result.Allocations {
				if allocation[residentID].NeverAdmitted == nil || allocation[residentID].Drain != nil {
					t.Fatal("resident member lost its independent non-admission proof")
				}
			}
			if peerDrained {
				if peerWrites.Load() != 0 {
					t.Fatal("existing drain triggered an unnecessary absence checkpoint")
				}
			} else if peerWrites.Load() != 1 || result.Allocations[f.assignment.Authority.StageAllocationId][peerID].NeverAdmitted != nil {
				t.Fatal("retired profile inferred a new historical absence proof")
			}
		})
	}
}
