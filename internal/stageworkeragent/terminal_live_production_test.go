package stageworkeragent_test

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/modelruntime"
	"github.com/vivym/vela/internal/stageauthority"
	"github.com/vivym/vela/internal/stageworkeragent"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestTerminalLiveProductionRecoversBeforeAdvertisingCapacity(t *testing.T) {
	for _, applied := range []bool{false, true} {
		for _, fault := range []string{"complete", "journal-rejection", "discovery-malformed", "cancel-malformed", "drain-malformed", "drain-lost", "stop-inspection"} {
			t.Run(map[bool]string{false: "not-applied", true: "applied"}[applied]+"/"+fault, func(t *testing.T) {
				f := terminalMaterializationFixture(t)
				gate := f.open(t)
				completeAdmissionInputs(t, beginAdmission(t, gate, f.assignment, f.acquireID))
				group := startFloorCollectorRuntimes(t, f, t.TempDir(), true, false)
				client, backend := group.clients[0], group.activeBackends[0]
				if fault == "journal-rejection" {
					var err error
					client, err = modelruntime.NewJournalWorkerClient(client, admittedTerminalJournalWriter{allocationID: f.assignment.Authority.StageAllocationId})
					if err != nil {
						t.Fatal(err)
					}
					fault = "complete"
				}
				first := f.assignment.Authority
				if response, err := client.PrepareStage(t.Context(), &velav1.ModelRuntimeServicePrepareStageRequest{Authority: first, ExecutionSpec: f.assignment.ExecutionSpec}); err != nil || response.GetDecision() != velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_ACCEPTED {
					t.Fatalf("prepare: %v %v", response, err)
				}
				renewal := proto.Clone(f.assignment).(*velav1.StageAssignment)
				renewal.Authority.StageVersion++
				renewal.Authority.IssuedAt = timestamppb.New(first.IssuedAt.AsTime().Add(time.Second))
				renewal.Authority.ExpiresAt = timestamppb.New(first.ExpiresAt.AsTime().Add(time.Second))
				f.clock.Add(int64(time.Second))
				f.sign(t, renewal)
				completeAdmissionInputs(t, beginAdmission(t, gate, renewal, f.acquireID))
				backend.renewalResponseFault.Store(1)
				if applied {
					backend.renewalResponseFault.Store(2)
				}
				backend.stopOnCancel.Store(true)
				if response, err := client.Status(t.Context(), &velav1.ModelRuntimeServiceStatusRequest{Authority: renewal.Authority}); err != nil || response.GetDecision() != velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_REJECTED {
					t.Fatalf("renewal fault: %v %v", response, err)
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
				faultClient := &terminalLiveFaultClient{ModelRuntimeServiceClient: client, fault: fault}
				f.config.Members[0].Client = faultClient
				backend.failInspectAfterDrain.Store(fault == "stop-inspection")
				_, validator := terminalMaterializationRecord(t, f, "sealed")
				journal := terminalRecoveryJournal(t)
				guard := &noTerminalMaterializationIO{}
				identity := runtimeIdentityFromAuthority(first)
				control := &terminalRecoverySessionControl{productionControl: &productionControl{identity: identity, controlSessionEpoch: 7}}
				config := terminalMaterializationConfig(t, f, gate, journal, validator, guard)
				config.Control = control
				queries := 0
				stream := automaticTerminalStream(t, config, func(_ context.Context, _ *stageauthority.Validator, authority *velav1.StageAuthority, _ string, acquire uuid.UUID) (*stageauthority.VerifiedTerminalDisposition, error) {
					queries++
					if !proto.Equal(authority, renewal.Authority) || acquire != f.acquireID || control.capacity == nil {
						t.Fatal("history query lost latest grant, original Acquire or capacity withdrawal")
					}
					for _, value := range control.capacity.CapacityVector {
						if value != 0 {
							t.Fatal("recovery queried history while advertising capacity")
						}
					}
					return fixtureTerminalResponse(t, f), nil
				})
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				waits, failures, prepared := 0, 0, false
				production, err := stageworkeragent.NewProductionAgent(stageworkeragent.ProductionConfig{
					Control: control, Runtime: client, Stream: stream, RuntimeIdentity: identity,
					Devices: first.Devices, Members: first.Members, CapacityVector: first.CapacityVector,
					CapacityTTL: time.Minute, HeartbeatInterval: time.Second, RetryMinimum: time.Millisecond, RetryMaximum: time.Second,
					ObservationSequenceSource: &capacitySequenceSource{values: []int64{1, 2, 3, 4}},
					Now:                       func() time.Time { return time.Unix(0, f.clock.Load()) },
					Wait: func(context.Context, time.Duration) error {
						waits++
						state := admissionSnapshot(t, gate)
						if control.acquire == nil {
							failures++
							if waits > 1 || fault == "complete" || len(state.Retirements) != 1 || state.Retirements[0].Phase != stageworkeragent.TerminalRetirementIntent {
								t.Fatalf("recovery did not retain a retryable INTENT: %+v waits=%d", state, waits)
							}
							assertRetirementScratch(t, paths, true)
							for _, value := range control.capacity.CapacityVector {
								if value != 0 {
									t.Fatal("partial recovery advertised capacity")
								}
							}
							backend.failInspectAfterDrain.Store(false)
							return nil
						}
						if len(state.Retirements) != 1 || state.Retirements[0].Phase != stageworkeragent.TerminalRetirementRetired || len(control.capacities) < 2 {
							t.Fatal("Acquire preceded complete terminal retirement")
						}
						for resource, value := range first.CapacityVector {
							if control.capacity.CapacityVector[resource] != value {
								t.Fatal("complete recovery did not restore configured capacity")
							}
						}
						assertRetirementScratch(t, paths, false)
						next := f.next(t, 8)
						next.Authority.StageRunId = uuid.NewString()
						f.sign(t, next)
						completeAdmissionInputs(t, beginAdmission(t, gate, next, uuid.New()))
						if response, err := client.PrepareStage(ctx, &velav1.ModelRuntimeServicePrepareStageRequest{Authority: next.Authority, ExecutionSpec: next.ExecutionSpec}); err != nil || response.GetDecision() != velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_ACCEPTED {
							t.Fatalf("restored capacity had no usable Runtime slot: %v %v", response, err)
						}
						prepared = true
						cancel()
						return nil
					},
				})
				if err != nil {
					t.Fatal(err)
				}
				if err := production.Run(ctx); err != nil {
					t.Fatal(err)
				}
				if !prepared || guard.calls != 0 || queries != failures+1 || backend.cancelCalls.Load() != 1 || backend.drainCalls.Load() != 1 || backend.closed.Load() {
					t.Fatalf("incomplete or repeated recovery: prepared=%t io=%d queries=%d failures=%d cancel=%d drain=%d", prepared, guard.calls, queries, failures, backend.cancelCalls.Load(), backend.drainCalls.Load())
				}
				if (fault != "complete") != (failures == 1) {
					t.Fatalf("fault did not cause exactly one failed recovery: %d", failures)
				}
				actual := first
				if applied {
					actual = renewal.Authority
				}
				read, err := client.InspectStageAllocationDrain(t.Context(), &velav1.ModelRuntimeServiceInspectStageAllocationDrainRequest{
					Scope: &velav1.ModelRuntimeExecutionDrainScope{SchemaVersion: 1, Identity: identity, Authority: renewal.Authority},
				})
				if err != nil || !proto.Equal(read.GetResult().GetCheckpoint().GetAuthority(), actual) {
					t.Fatalf("automatic recovery lost actual signed checkpoint: %v %v", read, err)
				}
			})
		}
	}
}

// The real owner rejection is tested in modelruntime. Here the production
// recovery loop must continue from that rejection to observed stop, durable
// drain, retirement and a genuinely usable slot. The local Runtime fixture
// owns its floor journal, so its floor RPC still performs the real write.
type admittedTerminalJournalWriter struct{ allocationID string }

func (w admittedTerminalJournalWriter) Apply(_ context.Context, command modelruntime.JournalCommand) (modelruntime.JournalMutationReceipt, error) {
	if command.NonAdmission != nil || (command.TerminalNonAdmission != nil && command.TerminalNonAdmission.Allocation == w.allocationID) {
		return modelruntime.JournalMutationReceipt{}, modelruntime.ErrJournalRejected
	}
	return modelruntime.JournalMutationReceipt{}, nil
}

type terminalLiveFaultClient struct {
	velav1.ModelRuntimeServiceClient
	fault string
	used  atomic.Bool
}

func (client *terminalLiveFaultClient) InspectAllocationExecution(ctx context.Context, request *velav1.ModelRuntimeServiceInspectAllocationExecutionRequest, options ...grpc.CallOption) (*velav1.ModelRuntimeServiceInspectAllocationExecutionResponse, error) {
	response, err := client.ModelRuntimeServiceClient.InspectAllocationExecution(ctx, request, options...)
	if err == nil && client.fault == "discovery-malformed" && !client.used.Swap(true) {
		response.AuthorityDigest[0] ^= 1
	}
	return response, err
}

func (client *terminalLiveFaultClient) CancelStage(ctx context.Context, request *velav1.ModelRuntimeServiceCancelStageRequest, options ...grpc.CallOption) (*velav1.ModelRuntimeServiceCancelStageResponse, error) {
	response, err := client.ModelRuntimeServiceClient.CancelStage(ctx, request, options...)
	if err == nil && client.fault == "cancel-malformed" && !client.used.Swap(true) {
		response.RuntimeIdentity.ModelRuntimeEpoch++
	}
	return response, err
}

func (client *terminalLiveFaultClient) DrainStageExecution(ctx context.Context, request *velav1.ModelRuntimeServiceDrainStageExecutionRequest, options ...grpc.CallOption) (*velav1.ModelRuntimeServiceDrainStageExecutionResponse, error) {
	response, err := client.ModelRuntimeServiceClient.DrainStageExecution(ctx, request, options...)
	if err == nil && strings.HasPrefix(client.fault, "drain-") && !client.used.Swap(true) {
		if client.fault == "drain-lost" {
			return nil, errors.New("injected drain response loss after persistence")
		}
		response.Result.Checkpoint.AuthorityDigest[0] ^= 1
	}
	return response, err
}
