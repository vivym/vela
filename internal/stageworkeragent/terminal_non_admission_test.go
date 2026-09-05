package stageworkeragent_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/vivym/vela/internal/modelruntimetransport"
	"github.com/vivym/vela/internal/stageworkeragent"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestTerminalNonAdmissionCollectorCombinesUnsignedHistoryWithDrainAndRecovery(t *testing.T) {
	f := newFloorCollectorFixture(t)
	// Never construct an execution envelope for the second allocation.
	queries := map[string]*velav1.StageAuthority{f.assignment.Authority.StageAllocationId: proto.Clone(f.assignment.Authority).(*velav1.StageAuthority)}
	base := t.TempDir()
	group := startFloorCollectorRuntimes(t, f, base, true, false)
	agent, targets := f.agent(t), drainCollectorTargets(f)
	a := f.assignment.Authority
	prepared, err := group.clients[0].PrepareStage(t.Context(), &velav1.ModelRuntimeServicePrepareStageRequest{Authority: a, ExecutionSpec: f.assignment.ExecutionSpec})
	if err != nil || prepared.GetDecision() != velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_ACCEPTED {
		t.Fatalf("prepare: %v %v", prepared, err)
	}
	started, err := group.clients[0].StartStage(t.Context(), &velav1.ModelRuntimeServiceStartStageRequest{Authority: a})
	if err != nil || started.GetDecision() != velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_ACCEPTED {
		t.Fatalf("start: %v %v", started, err)
	}
	group.activeBackends[0].MarkOutputReady([]byte(`{"kind":"LATENT","path":"/local/dit.bin"}`))
	sealed, err := group.clients[0].SealOutput(t.Context(), &velav1.ModelRuntimeServiceSealOutputRequest{Authority: a})
	if err != nil || sealed.GetReceipt() == nil {
		t.Fatalf("seal: %v %v", sealed, err)
	}
	if result, err := agent.CheckpointTerminalExecutionExclusions(t.Context(), f.disposition, queries, targets); err == nil || result.AllExcluded {
		t.Fatalf("missing floors: %+v %v", result, err)
	}
	if floor, err := agent.InstallExecutionFloor(t.Context(), f.disposition); err != nil || !floor.AllInstalled {
		t.Fatalf("floor: %+v %v", floor, err)
	}
	if result, err := agent.InspectTerminalExecutionExclusions(t.Context(), f.disposition, queries, targets); err == nil || result.AllExcluded {
		t.Fatalf("inspection inferred missing proof: %+v %v", result, err)
	}
	group.dropNonAdmissionResponse.Store(true)
	if result, err := agent.CheckpointTerminalExecutionExclusions(t.Context(), f.disposition, queries, targets); err == nil || result.AllExcluded {
		t.Fatalf("lost reply claimed full exclusion: %+v %v", result, err)
	}
	result, err := agent.InspectTerminalExecutionExclusions(t.Context(), f.disposition, queries, targets)
	if err != nil || !result.AllExcluded || result.RequiredAllocations != 2 || result.RequiredMembers != 2 {
		t.Fatalf("complete unsigned history: all=%t allocations=%d required=%d members=%d error=%v", result.AllExcluded, len(result.Allocations), result.RequiredAllocations, result.RequiredMembers, err)
	}
	drains, envelopes, terminal := 0, 0, 0
	for _, members := range result.Allocations {
		for _, proof := range members {
			kinds := 0
			if proof.Drain != nil {
				drains++
				kinds++
			}
			if proof.NeverAdmitted != nil {
				envelopes++
				kinds++
			}
			if proof.TerminalNeverAdmitted != nil {
				terminal++
				kinds++
			}
			if kinds != 1 {
				t.Fatal("collector merged distinct proof contracts")
			}
		}
	}
	if drains != 1 || envelopes != 1 || terminal != 2 {
		t.Fatalf("proof counts: drain=%d envelope=%d terminal=%d", drains, envelopes, terminal)
	}
	if read, err := agent.InspectTerminalExecutionDrains(t.Context(), f.disposition, queries, targets); err == nil || read.AllDrained {
		t.Fatalf("non-admission became backend drain: %+v %v", read, err)
	}
	for _, byResidency := range group.backends {
		for _, backend := range byResidency {
			if backend.closed.Load() {
				t.Fatal("collector unloaded resident model")
			}
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
		t.Fatalf("unsigned history recovery: %+v %v", recovered, err)
	}
	for id, members := range result.Allocations {
		for member, before := range members {
			after := recovered.Allocations[id][member]
			if before.Drain != nil && !proto.Equal(before.Drain.GetCheckpoint(), after.Drain.GetCheckpoint()) ||
				before.NeverAdmitted != nil && !proto.Equal(before.NeverAdmitted.GetCheckpoint(), after.NeverAdmitted.GetCheckpoint()) ||
				before.TerminalNeverAdmitted != nil && !proto.Equal(before.TerminalNeverAdmitted.GetCheckpoint(), after.TerminalNeverAdmitted.GetCheckpoint()) {
				t.Fatal("recovery replaced original proof")
			}
		}
	}
}

func TestTerminalNonAdmissionCollectorCannotInferOldEpochAbsence(t *testing.T) {
	f := newFloorCollectorFixture(t)
	base := t.TempDir()
	group := startFloorCollectorRuntimes(t, f, base, true, false)
	if floor, err := f.agent(t).InstallExecutionFloor(t.Context(), f.disposition); err != nil || !floor.AllInstalled {
		t.Fatalf("floor: %+v %v", floor, err)
	}
	group.close()
	for index := range f.config.ExecutionFloor.Bindings {
		f.config.ExecutionFloor.Bindings[index].Runtime.ModelRuntimeEpoch++
	}
	startFloorCollectorRuntimes(t, f, base, false, false)
	if result, err := f.agent(t).CheckpointTerminalExecutionExclusions(t.Context(), f.disposition, nil, drainCollectorTargets(f)); err == nil || result.AllExcluded {
		t.Fatalf("restart invented never-admitted proof: %+v %v", result, err)
	}
}

func TestTerminalNonAdmissionCollectorRecoversProofWhenExecutionEnvelopeBecomesAvailable(t *testing.T) {
	f := newFloorCollectorFixture(t)
	queries := terminalDrainQueries(t, f)
	base := t.TempDir()
	group := startFloorCollectorRuntimes(t, f, base, true, false)
	agent := f.agent(t)
	if floor, err := agent.InstallExecutionFloor(t.Context(), f.disposition); err != nil || !floor.AllInstalled {
		t.Fatalf("floor: %+v %v", floor, err)
	}
	// The first collector has terminal history but has not recovered envelopes.
	before, err := agent.CheckpointTerminalExecutionExclusions(t.Context(), f.disposition, nil, drainCollectorTargets(f))
	if err != nil || !before.AllExcluded {
		t.Fatalf("terminal checkpoints: %+v %v", before, err)
	}
	group.close()
	f.config.ExecutionFloor.Bindings = f.config.ExecutionFloor.Bindings[2:]
	for index := range f.config.ExecutionFloor.Bindings {
		f.config.ExecutionFloor.Bindings[index].Runtime.ModelRuntimeEpoch++
	}
	startFloorCollectorRuntimes(t, f, base, false, false)
	agent = f.agent(t)
	for _, collect := range []func(context.Context, *velav1.StageTerminalDisposition, map[string]*velav1.StageAuthority, map[string]*velav1.ModelRuntimeIdentity) (stageworkeragent.TerminalExecutionExclusionResult, error){agent.InspectTerminalExecutionExclusions, agent.CheckpointTerminalExecutionExclusions} {
		after, err := collect(t.Context(), f.disposition, queries, drainCollectorTargets(f))
		if err != nil || !after.AllExcluded {
			t.Fatalf("available envelopes hid persisted terminal proof: %+v %v", after, err)
		}
		for allocation, members := range before.Allocations {
			for member, proof := range members {
				recovered := after.Allocations[allocation][member]
				if recovered.Drain != nil || recovered.NeverAdmitted != nil || recovered.TerminalNeverAdmitted == nil ||
					!proto.Equal(proof.TerminalNeverAdmitted.GetCheckpoint(), recovered.TerminalNeverAdmitted.GetCheckpoint()) {
					t.Fatal("envelope recovery changed the original proof contract")
				}
			}
		}
	}
}

func TestTerminalNonAdmissionCollectorPreflightsCompleteHistory(t *testing.T) {
	for _, fault := range []string{"missing reader", "reader epoch", "member identity", "subset", "devices", "signature", "nil authority"} {
		t.Run(fault, func(t *testing.T) {
			f := newFloorCollectorFixture(t)
			targets := drainCollectorTargets(f)
			var calls atomic.Int64
			for index := range f.config.Members {
				f.config.Members[index].Client = &terminalExclusionClient{read: func(*velav1.ModelRuntimeTerminalAllocationScope) (*velav1.ModelRuntimeTerminalNonAdmissionResult, error) {
					calls.Add(1)
					return nil, errors.New("unexpected RPC")
				}}
			}
			var authorities map[string]*velav1.StageAuthority
			switch fault {
			case "missing reader":
				delete(targets, f.config.Members[1].ID)
			case "reader epoch":
				targets[f.config.Members[1].ID].ModelRuntimeEpoch++
			case "member identity":
				for index := range f.config.ExecutionFloor.Bindings {
					if f.config.ExecutionFloor.Bindings[index].Runtime.WorkerMemberID == f.config.Members[1].ID {
						f.config.ExecutionFloor.Bindings[index].IdentityDigest[0] ^= 1
					}
				}
			case "subset":
				for index := range f.config.ExecutionFloor.Bindings {
					if f.config.ExecutionFloor.Bindings[index].Runtime.WorkerMemberID == f.config.Members[1].ID {
						f.config.ExecutionFloor.Bindings[index].DeviceSubsetDigest[0] ^= 1
					}
				}
			case "devices":
				for index := range f.config.ExecutionFloor.Bindings {
					f.config.ExecutionFloor.Bindings[index].Runtime.Devices[0].Epoch++
				}
			case "signature":
				f.disposition.Signature[0] ^= 1
			case "nil authority":
				authorities = map[string]*velav1.StageAuthority{f.disposition.StageAllocationId: nil}
			}
			if result, err := f.agent(t).CheckpointTerminalExecutionExclusions(t.Context(), f.disposition, authorities, targets); err == nil || result.AllExcluded || calls.Load() != 0 {
				t.Fatalf("invalid history dispatched: %+v %v calls=%d", result, err, calls.Load())
			}
		})
	}
}

func TestTerminalNonAdmissionCollectorRejectsBadReadsBeforeCheckpointing(t *testing.T) {
	for _, fault := range []string{"error", "nil", "digest", "signature", "floor", "contract", "mutation", "late", "rejected"} {
		for _, withEnvelope := range []bool{false, true} {
			t.Run(fault+map[bool]string{false: "/missing-envelope", true: "/known-envelope"}[withEnvelope], func(t *testing.T) {
				f := newFloorCollectorFixture(t)
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				var writes atomic.Int64
				for index := range f.config.Members {
					f.config.Members[index].Client = &terminalExclusionClient{
						ModelRuntimeServiceClient: &exclusionCollectorClient{
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
								writes.Add(1)
								return exclusionNonAdmissionReply(scope), nil
							},
						},
						read: func(scope *velav1.ModelRuntimeTerminalAllocationScope) (*velav1.ModelRuntimeTerminalNonAdmissionResult, error) {
							result := terminalExclusionReply(t, f, scope)
							switch fault {
							case "error":
								return result, errors.New("transport failure with proof")
							case "nil":
								return nil, nil
							case "digest":
								result.DispositionDigest[0] ^= 1
							case "signature":
								result.Checkpoint.Disposition.Signature[0] ^= 1
							case "floor":
								result.Checkpoint.InstalledCutoff = 0
							case "contract":
								result.Checkpoint.Contract = modelruntimetransport.ExecutionDrainContract
							case "mutation":
								scope.Identity.ModelRuntimeEpoch++
								result.Identity.ModelRuntimeEpoch++
							case "late":
								cancel()
							case "rejected":
								result.Decision = velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_REJECTED
								result.Checkpoint = nil
							}
							return result, nil
						},
						checkpoint: func(scope *velav1.ModelRuntimeTerminalAllocationScope) (*velav1.ModelRuntimeTerminalNonAdmissionResult, error) {
							writes.Add(1)
							return terminalExclusionReply(t, f, scope), nil
						},
					}
				}
				var queries map[string]*velav1.StageAuthority
				if withEnvelope {
					queries = terminalDrainQueries(t, f)
				}
				if result, err := f.agent(t).CheckpointTerminalExecutionExclusions(ctx, f.disposition, queries, drainCollectorTargets(f)); err == nil || result.AllExcluded || writes.Load() != 0 {
					t.Fatalf("invalid read fell through to checkpoint: %+v %v writes=%d", result, err, writes.Load())
				}
			})
		}
	}
}

type terminalExclusionClient struct {
	velav1.ModelRuntimeServiceClient
	read       func(*velav1.ModelRuntimeTerminalAllocationScope) (*velav1.ModelRuntimeTerminalNonAdmissionResult, error)
	checkpoint func(*velav1.ModelRuntimeTerminalAllocationScope) (*velav1.ModelRuntimeTerminalNonAdmissionResult, error)
}

func (client *terminalExclusionClient) InspectStageTerminalNonAdmission(_ context.Context, request *velav1.ModelRuntimeServiceInspectStageTerminalNonAdmissionRequest, _ ...grpc.CallOption) (*velav1.ModelRuntimeServiceInspectStageTerminalNonAdmissionResponse, error) {
	result, err := client.read(request.Scope)
	return &velav1.ModelRuntimeServiceInspectStageTerminalNonAdmissionResponse{Result: result}, err
}
func (client *terminalExclusionClient) CheckpointStageTerminalNonAdmission(_ context.Context, request *velav1.ModelRuntimeServiceCheckpointStageTerminalNonAdmissionRequest, _ ...grpc.CallOption) (*velav1.ModelRuntimeServiceCheckpointStageTerminalNonAdmissionResponse, error) {
	result, err := client.checkpoint(request.Scope)
	return &velav1.ModelRuntimeServiceCheckpointStageTerminalNonAdmissionResponse{Result: result}, err
}
func terminalExclusionReply(t *testing.T, f *floorCollectorFixture, scope *velav1.ModelRuntimeTerminalAllocationScope) *velav1.ModelRuntimeTerminalNonAdmissionResult {
	t.Helper()
	verified, err := f.config.ExecutionFloor.Validator.ValidateTerminalDispositionSignature(scope.Disposition)
	if err != nil {
		t.Fatal(err)
	}
	var sequence int64
	for _, allocation := range scope.Disposition.Allocations {
		if allocation.StageAllocationId == scope.StageAllocationId {
			sequence = allocation.ExecutionSequence
		}
	}
	return &velav1.ModelRuntimeTerminalNonAdmissionResult{SchemaVersion: 1, Identity: proto.Clone(scope.Identity).(*velav1.ModelRuntimeIdentity),
		DispositionDigest: append([]byte(nil), verified.Digest[:]...), StageAllocationId: scope.StageAllocationId, Decision: velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_ACCEPTED,
		Checkpoint: &velav1.ModelRuntimeTerminalNonAdmissionCheckpoint{SchemaVersion: 1, Disposition: proto.Clone(scope.Disposition).(*velav1.StageTerminalDisposition),
			DispositionDigest: append([]byte(nil), verified.Digest[:]...), WorkerMemberId: scope.Identity.WorkerMemberId, StageAllocationId: scope.StageAllocationId,
			ExecutionSequence: sequence, InstalledCutoff: scope.Disposition.Cutoff, Contract: modelruntimetransport.TerminalNonAdmissionContract, ObservedAt: timestamppb.New(scope.Disposition.ObservedAt.AsTime())}}
}
