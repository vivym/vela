package stageworkeragent_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/vivym/vela/internal/modelruntimetransport"
	"github.com/vivym/vela/internal/stageauthority"
	"github.com/vivym/vela/internal/stageworkeragent"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestExecutionDrainCollectionRPCRequiresEveryMemberAndRecoversRetiredProfiles(t *testing.T) {
	f := newFloorCollectorFixture(t)
	root := t.TempDir()
	runtimes := startFloorCollectorRuntimes(t, f, root, true, false)
	agent := f.agent(t)
	for _, client := range runtimes.clients {
		prepared, err := client.PrepareStage(t.Context(), &velav1.ModelRuntimeServicePrepareStageRequest{Authority: f.assignment.Authority, ExecutionSpec: f.assignment.ExecutionSpec})
		if err != nil || prepared.GetDecision() != velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_ACCEPTED {
			t.Fatalf("prepare: %v %v", prepared, err)
		}
		canceled, err := client.CancelStage(t.Context(), &velav1.ModelRuntimeServiceCancelStageRequest{Authority: f.assignment.Authority, Reason: velav1.ModelRuntimeCancelReason_MODEL_RUNTIME_CANCEL_REASON_CONTROL_PLANE_STOP})
		if err != nil || !canceled.GetCancellationAcknowledged() {
			t.Fatalf("cancel: %v %v", canceled, err)
		}
	}
	runtimes.activeBackends[0].FinishStop()
	partial, err := agent.DrainExecution(t.Context(), f.assignment.Authority)
	if err == nil || partial.AllDrained || partial.RequiredMembers != 2 || len(partial.Members) != 1 {
		t.Fatalf("partial drain counted as complete: %+v %v", partial, err)
	}
	runtimes.activeBackends[1].FinishStop()
	complete, err := agent.DrainExecution(t.Context(), f.assignment.Authority)
	if err != nil || !complete.AllDrained || len(complete.Members) != 2 {
		t.Fatalf("complete drain: %+v %v", complete, err)
	}
	for _, backend := range runtimes.activeBackends {
		if backend.closed.Load() {
			t.Fatal("drain unloaded model")
		}
	}
	for id, earlier := range partial.Members {
		if !proto.Equal(earlier, complete.Members[id]) {
			t.Fatal("retry replaced immutable checkpoint")
		}
	}
	runtimes.close()
	// The original profile is retired. Another configured resident profile reads
	// the shared member journal without entering the old or replacement backend.
	f.config.ExecutionFloor.Bindings = f.config.ExecutionFloor.Bindings[2:]
	for index := range f.config.ExecutionFloor.Bindings {
		f.config.ExecutionFloor.Bindings[index].Runtime.ModelRuntimeEpoch++
	}
	startFloorCollectorRuntimes(t, f, root, false, false)
	agent = f.agent(t)
	targets := drainCollectorTargets(f)
	read, err := agent.InspectExecutionDrain(t.Context(), f.assignment.Authority, targets)
	if err != nil || !read.AllDrained || len(read.Members) != 2 {
		t.Fatalf("retired-profile read: %+v %v", read, err)
	}
	for id, old := range complete.Members {
		if !proto.Equal(old.GetCheckpoint(), read.Members[id].GetCheckpoint()) || proto.Equal(old.GetIdentity(), read.Members[id].GetIdentity()) {
			t.Fatal("recovery mixed journal owner and execution identity")
		}
	}
	if result, err := agent.DrainExecution(t.Context(), f.assignment.Authority); err == nil || result.AllDrained {
		t.Fatal("retired execution was sent to a replacement backend")
	}
	unseen := proto.Clone(f.assignment.Authority).(*velav1.StageAuthority)
	unseen.StageVersion++
	unseen, err = f.signer.Sign(unseen)
	if err != nil {
		t.Fatal(err)
	}
	if result, err := agent.InspectExecutionDrain(t.Context(), unseen, targets); err == nil || result.AllDrained || len(result.Members) != 0 {
		t.Fatalf("unseen renewal inherited proof: %+v %v", result, err)
	}
}

func TestExecutionDrainCollectionRejectsIncompleteOrMismatchedProof(t *testing.T) {
	for _, historical := range []bool{false, true} {
		for _, fault := range []string{"missing", "unknown", "rejected", "digest", "sequence", "member", "owner", "contract", "authority", "request-mutation", "error", "late"} {
			t.Run(fault+"/read="+map[bool]string{false: "false", true: "true"}[historical], func(t *testing.T) {
				f := newFloorCollectorFixture(t)
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				for index := range f.config.Members {
					f.config.Members[index].Client = &drainCollectorClient{reply: func(ctx context.Context, scope *velav1.ModelRuntimeExecutionDrainScope) (*velav1.ModelRuntimeExecutionDrainResult, error) {
						result := drainCollectorReply(scope)
						if index == 1 {
							switch fault {
							case "missing":
								return nil, nil
							case "unknown":
								result.Checkpoint = nil
							case "rejected":
								result.Checkpoint, result.Decision = nil, velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_REJECTED
							case "digest":
								result.AuthorityDigest[0] ^= 1
							case "sequence":
								result.Checkpoint.ExecutionSequence++
							case "member":
								result.Checkpoint.WorkerMemberId = f.config.Members[0].ID
							case "owner":
								result.Identity.ModelRuntimeEpoch++
							case "contract":
								result.Checkpoint.Contract = "STOPPED"
							case "authority":
								result.Checkpoint.Authority.StageVersion++
							case "request-mutation":
								scope.Identity.ModelRuntimeEpoch++
								result.Identity.ModelRuntimeEpoch++
							case "error":
								return nil, errors.New("member unavailable")
							case "late":
								cancel()
							}
						}
						return result, nil
					}}
				}
				agent := f.agent(t)
				var result stageworkeragent.ExecutionDrainResult
				var err error
				if historical {
					result, err = agent.InspectExecutionDrain(ctx, f.assignment.Authority, drainCollectorTargets(f))
				} else {
					result, err = agent.DrainExecution(ctx, f.assignment.Authority)
				}
				if err == nil || result.AllDrained || len(result.Members) > 1 {
					t.Fatalf("invalid member completed collection: %+v %v", result, err)
				}
			})
		}
	}
}

func TestExecutionDrainCollectionValidatesCompleteTrustedScopeBeforeRPC(t *testing.T) {
	for _, fault := range []string{"signature", "member-identity", "missing-binding", "duplicate-binding", "target-missing", "target-extra", "target-epoch", "devices"} {
		t.Run(fault, func(t *testing.T) {
			f := newFloorCollectorFixture(t)
			var calls atomic.Int64
			for index := range f.config.Members {
				f.config.Members[index].Client = &drainCollectorClient{reply: func(_ context.Context, scope *velav1.ModelRuntimeExecutionDrainScope) (*velav1.ModelRuntimeExecutionDrainResult, error) {
					calls.Add(1)
					return drainCollectorReply(scope), nil
				}}
			}
			authority := proto.Clone(f.assignment.Authority).(*velav1.StageAuthority)
			targets := drainCollectorTargets(f)
			switch fault {
			case "signature":
				authority.Signature[0] ^= 1
			case "member-identity":
				f.config.ExecutionFloor.Bindings[0].IdentityDigest[0] ^= 1
			case "missing-binding":
				f.config.ExecutionFloor.Bindings = f.config.ExecutionFloor.Bindings[1:]
			case "duplicate-binding":
				f.config.ExecutionFloor.Bindings = append(f.config.ExecutionFloor.Bindings, f.config.ExecutionFloor.Bindings[0])
			case "target-missing":
				delete(targets, f.config.Members[1].ID)
			case "target-extra":
				targets["extra"] = targets[f.config.Members[0].ID]
			case "target-epoch":
				targets[f.config.Members[0].ID].ModelRuntimeEpoch++
			case "devices":
				f.config.ExecutionFloor.Bindings[0].Runtime.Devices = nil
			}
			agent := f.agent(t)
			if result, err := agent.InspectExecutionDrain(t.Context(), authority, targets); err == nil || result.AllDrained || calls.Load() != 0 {
				t.Fatalf("invalid trusted scope reached RPC: %+v %v calls=%d", result, err, calls.Load())
			}
		})
	}
}

func TestExecutionDrainCollectionBoundsUnresponsiveMembersAndDropsLateProof(t *testing.T) {
	f := newFloorCollectorFixture(t)
	f.config.ExecutionFloor.Timeout = 30 * time.Millisecond
	entered, release, exited := make(chan struct{}), make(chan struct{}), make(chan struct{})
	for index := range f.config.Members {
		f.config.Members[index].Client = &drainCollectorClient{reply: func(_ context.Context, scope *velav1.ModelRuntimeExecutionDrainScope) (*velav1.ModelRuntimeExecutionDrainResult, error) {
			if index == 1 {
				close(entered)
				<-release
				defer close(exited)
			}
			return drainCollectorReply(scope), nil
		}}
	}
	agent := f.agent(t)
	result, err := agent.DrainExecution(t.Context(), f.assignment.Authority)
	close(release)
	select {
	case <-exited:
	case <-time.After(time.Second):
		t.Fatal("unresponsive member did not finish")
	}
	if !errors.Is(err, context.DeadlineExceeded) || result.AllDrained {
		t.Fatalf("late proof escaped deadline: %+v %v", result, err)
	}
	select {
	case <-entered:
	default:
		t.Fatal("unresponsive member was not entered")
	}
}

func drainCollectorTargets(f *floorCollectorFixture) map[string]*velav1.ModelRuntimeIdentity {
	targets := make(map[string]*velav1.ModelRuntimeIdentity)
	for _, configured := range f.config.ExecutionFloor.Bindings {
		b := configured.Runtime
		if targets[b.WorkerMemberID] == nil {
			targets[b.WorkerMemberID] = &velav1.ModelRuntimeIdentity{WorkerInstanceId: b.WorkerInstanceID, WorkerInstanceEpoch: b.WorkerInstanceEpoch, WorkerMemberId: b.WorkerMemberID, WorkerMemberEpoch: b.WorkerMemberEpoch, DeviceSetDigest: append([]byte(nil), b.DeviceSetDigest...), MembershipDigest: append([]byte(nil), b.MembershipDigest...), ModelResidencyId: b.ModelResidencyID, RuntimeIdentity: b.ModelRuntimeIdentity, ModelRuntimeEpoch: b.ModelRuntimeEpoch, StageProfileRevisionId: b.StageProfileRevisionID}
		}
	}
	return targets
}

func drainCollectorReply(scope *velav1.ModelRuntimeExecutionDrainScope) *velav1.ModelRuntimeExecutionDrainResult {
	digest, _ := stageauthority.Digest(scope.Authority)
	return &velav1.ModelRuntimeExecutionDrainResult{SchemaVersion: 1, Identity: proto.Clone(scope.Identity).(*velav1.ModelRuntimeIdentity), AuthorityDigest: append([]byte(nil), digest[:]...), Decision: velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_ACCEPTED,
		Checkpoint: &velav1.ModelRuntimeExecutionDrainCheckpoint{SchemaVersion: 1, Authority: proto.Clone(scope.Authority).(*velav1.StageAuthority), AuthorityDigest: append([]byte(nil), digest[:]...), WorkerMemberId: scope.Identity.WorkerMemberId, ExecutionSequence: scope.Authority.ExecutionSequence, Contract: modelruntimetransport.ExecutionDrainContract, DrainedAt: timestamppb.New(time.Now())}}
}

type drainCollectorClient struct {
	velav1.ModelRuntimeServiceClient
	reply func(context.Context, *velav1.ModelRuntimeExecutionDrainScope) (*velav1.ModelRuntimeExecutionDrainResult, error)
}

func (client *drainCollectorClient) DrainStageExecution(ctx context.Context, request *velav1.ModelRuntimeServiceDrainStageExecutionRequest, _ ...grpc.CallOption) (*velav1.ModelRuntimeServiceDrainStageExecutionResponse, error) {
	result, err := client.reply(ctx, request.Scope)
	return &velav1.ModelRuntimeServiceDrainStageExecutionResponse{Result: result}, err
}
func (client *drainCollectorClient) InspectStageExecutionDrain(ctx context.Context, request *velav1.ModelRuntimeServiceInspectStageExecutionDrainRequest, _ ...grpc.CallOption) (*velav1.ModelRuntimeServiceInspectStageExecutionDrainResponse, error) {
	result, err := client.reply(ctx, request.Scope)
	return &velav1.ModelRuntimeServiceInspectStageExecutionDrainResponse{Result: result}, err
}
