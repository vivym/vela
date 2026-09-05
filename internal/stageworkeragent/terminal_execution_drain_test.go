package stageworkeragent_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/stageauthority"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func terminalDrainQueries(t *testing.T, f *floorCollectorFixture) map[string]*velav1.StageAuthority {
	t.Helper()
	queries := make(map[string]*velav1.StageAuthority)
	for _, allocation := range f.disposition.Allocations {
		queries[allocation.StageAllocationId] = collectorHistoryAuthority(t, f, allocation)
	}
	return queries
}

func TestTerminalExecutionDrainRPCCollectsAllAllocationsWithPartialRenewalAndRecovery(t *testing.T) {
	f := newFloorCollectorFixture(t)
	queries := terminalDrainQueries(t, f)
	base := t.TempDir()
	group := startFloorCollectorRuntimes(t, f, base, true, false)
	agent, targets := f.agent(t), drainCollectorTargets(f)
	for allocationIndex, allocation := range f.disposition.Allocations {
		original := queries[allocation.StageAllocationId]
		for index, client := range group.clients {
			prepared, err := client.PrepareStage(t.Context(), &velav1.ModelRuntimeServicePrepareStageRequest{Authority: original, ExecutionSpec: f.assignment.ExecutionSpec})
			if err != nil || prepared.GetDecision() != velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_ACCEPTED {
				t.Fatalf("prepare: %v %v", prepared, err)
			}
			started, err := client.StartStage(t.Context(), &velav1.ModelRuntimeServiceStartStageRequest{Authority: original})
			if err != nil || started.GetDecision() != velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_ACCEPTED {
				t.Fatalf("start: %v %v", started, err)
			}
			group.backends[f.config.Members[index].ID][allocation.ModelResidencyId].MarkOutputReady([]byte(`{"kind":"LATENT","path":"/local/dit.bin"}`))
			drained := proto.Clone(original).(*velav1.StageAuthority)
			if index == 1 {
				f.clock.Add(int64(time.Second))
				drained.StageVersion++
				drained.IssuedAt = timestamppb.New(time.Unix(0, f.clock.Load()))
				drained.ExpiresAt = timestamppb.New(original.ExpiresAt.AsTime().Add(5 * time.Second))
				drained, err = f.signer.Sign(drained)
				if err != nil {
					t.Fatal(err)
				}
			}
			sealed, err := client.SealOutput(t.Context(), &velav1.ModelRuntimeServiceSealOutputRequest{Authority: drained})
			if err != nil || sealed.GetReceipt() == nil {
				t.Fatalf("seal: %v %v", sealed, err)
			}
		}
		exact, err := agent.InspectExecutionDrain(t.Context(), original, targets)
		if err == nil || exact.AllDrained || len(exact.Members) != 1 {
			t.Fatalf("different renewal unexpectedly proved exact-envelope drain: %+v %v", exact, err)
		}
		f.disposition.ObservedAt = timestamppb.New(time.Unix(0, f.clock.Load()))
		f.disposition.ExpiresAt = timestamppb.New(time.Unix(0, f.clock.Load()).Add(time.Minute))
		f.disposition.StageVersion = original.StageVersion + 2
		f.signDisposition(t)
		read, err := agent.InspectTerminalExecutionDrains(t.Context(), f.disposition, queries, targets)
		if allocationIndex == 0 {
			if err == nil || read.AllDrained || !read.Allocations[allocation.StageAllocationId].AllDrained {
				t.Fatalf("missing later allocation did not stay partial: %+v %v", read, err)
			}
		} else if err != nil || !read.AllDrained || read.RequiredAllocations != 2 {
			t.Fatalf("complete allocation history: %+v %v", read, err)
		}
	}
	before, err := agent.InspectTerminalExecutionDrains(t.Context(), f.disposition, queries, targets)
	if err != nil || !before.AllDrained {
		t.Fatalf("read history: %+v %v", before, err)
	}
	for _, allocation := range before.Allocations {
		first := allocation.Members[f.config.Members[0].ID].GetCheckpoint()
		second := allocation.Members[f.config.Members[1].ID].GetCheckpoint()
		if proto.Equal(first.Authority, second.Authority) || stageauthority.ValidateSameExecution(first.Authority, second.Authority) != nil {
			t.Fatal("collector replaced actual member checkpoint envelopes")
		}
	}
	group.close()
	// Retire the original profile and read both allocations through the other
	// profile at new Runtime epochs, preserving every original checkpoint.
	f.config.ExecutionFloor.Bindings = f.config.ExecutionFloor.Bindings[2:]
	for index := range f.config.ExecutionFloor.Bindings {
		f.config.ExecutionFloor.Bindings[index].Runtime.ModelRuntimeEpoch++
	}
	startFloorCollectorRuntimes(t, f, base, false, false)
	after, err := f.agent(t).InspectTerminalExecutionDrains(t.Context(), f.disposition, queries, drainCollectorTargets(f))
	if err != nil || !after.AllDrained {
		t.Fatalf("recovered terminal history: %+v %v", after, err)
	}
	for id, allocation := range before.Allocations {
		for member, result := range allocation.Members {
			if !proto.Equal(result.Checkpoint, after.Allocations[id].Members[member].Checkpoint) {
				t.Fatal("recovery changed persisted checkpoint")
			}
		}
	}
}

func TestTerminalExecutionDrainValidatesCompleteHistoryBeforeAnyRPC(t *testing.T) {
	for _, fault := range []string{"missing", "extra", "signature", "wrong-allocation", "nonce", "barrier", "root-digest", "reader", "subset", "expired", "cancel"} {
		t.Run(fault, func(t *testing.T) {
			f := newFloorCollectorFixture(t)
			queries, targets := terminalDrainQueries(t, f), drainCollectorTargets(f)
			var calls atomic.Int64
			for index := range f.config.Members {
				f.config.Members[index].Client = &allocationDrainCollectorClient{reply: func(_ context.Context, scope *velav1.ModelRuntimeExecutionDrainScope) (*velav1.ModelRuntimeExecutionDrainResult, error) {
					calls.Add(1)
					return drainCollectorReply(scope), nil
				}}
			}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			id := f.disposition.Allocations[1].StageAllocationId
			sign := false
			switch fault {
			case "missing":
				delete(queries, id)
			case "extra":
				queries[uuid.NewString()] = queries[id]
			case "signature":
				queries[id].Signature[0] ^= 1
			case "wrong-allocation":
				queries[id].StageAllocationId = uuid.NewString()
				sign = true
			case "nonce":
				queries[id].ExecutionNonce[0] ^= 1
				sign = true
			case "barrier":
				queries[id].ModelRuntimeBarrierGeneration++
				sign = true
			case "root-digest":
				f.disposition.OriginalAuthorityDigest[0] ^= 1
				f.signDisposition(t)
			case "reader":
				delete(targets, f.config.Members[1].ID)
			case "subset":
				f.config.ExecutionFloor.Bindings[0].DeviceSubsetDigest[0] ^= 1
			case "expired":
				f.clock.Add(int64(2 * time.Minute))
			case "cancel":
				cancel()
			}
			if sign {
				var err error
				queries[id], err = f.signer.Sign(queries[id])
				if err != nil {
					t.Fatal(err)
				}
			}
			result, err := f.agent(t).InspectTerminalExecutionDrains(ctx, f.disposition, queries, targets)
			if err == nil || result.AllDrained || calls.Load() != 0 {
				t.Fatalf("invalid complete history reached RPC: %+v %v calls=%d", result, err, calls.Load())
			}
		})
	}
}

func TestTerminalExecutionDrainRejectsUntrustedProofAndLateReplies(t *testing.T) {
	for _, fault := range []string{"missing", "signature", "different-execution", "nonce", "late", "mutation", "unknown"} {
		t.Run(fault, func(t *testing.T) {
			f := newFloorCollectorFixture(t)
			queries, targets := terminalDrainQueries(t, f), drainCollectorTargets(f)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			for index := range f.config.Members {
				f.config.Members[index].Client = &allocationDrainCollectorClient{reply: func(_ context.Context, scope *velav1.ModelRuntimeExecutionDrainScope) (*velav1.ModelRuntimeExecutionDrainResult, error) {
					result := drainCollectorReply(scope)
					if index == 1 {
						switch fault {
						case "missing":
							result.Checkpoint = nil
						case "signature":
							result.Checkpoint.Authority.Signature[0] ^= 1
						case "different-execution", "nonce":
							if fault == "nonce" {
								result.Checkpoint.Authority.ExecutionNonce[0] ^= 1
							} else {
								result.Checkpoint.Authority.StageAllocationId = uuid.NewString()
							}
							var err error
							result.Checkpoint.Authority, err = f.signer.Sign(result.Checkpoint.Authority)
							if err != nil {
								return nil, err
							}
							digest, _ := stageauthority.Digest(result.Checkpoint.Authority)
							result.Checkpoint.AuthorityDigest = digest[:]
						case "late":
							cancel()
						case "mutation":
							scope.Identity.ModelRuntimeEpoch++
							result.Identity.ModelRuntimeEpoch++
						case "unknown":
							result.ProtoReflect().SetUnknown([]byte{0x78, 1})
						}
					}
					return result, nil
				}}
			}
			result, err := f.agent(t).InspectTerminalExecutionDrains(ctx, f.disposition, queries, targets)
			if err == nil || result.AllDrained {
				t.Fatalf("invalid member proof completed history: %+v %v", result, err)
			}
			if fault == "late" && !errors.Is(err, context.Canceled) {
				t.Fatalf("late reply: %v", err)
			}
		})
	}
}

type allocationDrainCollectorClient struct {
	velav1.ModelRuntimeServiceClient
	reply func(context.Context, *velav1.ModelRuntimeExecutionDrainScope) (*velav1.ModelRuntimeExecutionDrainResult, error)
}

func TestTerminalExecutionDrainUsesOneDeadlineForCompleteHistory(t *testing.T) {
	f := newFloorCollectorFixture(t)
	f.config.ExecutionFloor.Timeout = 60 * time.Millisecond
	queries, targets := terminalDrainQueries(t, f), drainCollectorTargets(f)
	var calls atomic.Int64
	for index := range f.config.Members {
		f.config.Members[index].Client = &allocationDrainCollectorClient{reply: func(ctx context.Context, scope *velav1.ModelRuntimeExecutionDrainScope) (*velav1.ModelRuntimeExecutionDrainResult, error) {
			calls.Add(1)
			if scope.Authority.StageAllocationId == f.disposition.Allocations[1].StageAllocationId {
				<-ctx.Done()
				return drainCollectorReply(scope), nil
			}
			return drainCollectorReply(scope), nil
		}}
	}
	result, err := f.agent(t).InspectTerminalExecutionDrains(t.Context(), f.disposition, queries, targets)
	if !errors.Is(err, context.DeadlineExceeded) || result.AllDrained || calls.Load() != 4 ||
		!result.Allocations[f.disposition.Allocations[0].StageAllocationId].AllDrained {
		t.Fatalf("whole-history timeout lost partial proof or accepted late success: %+v %v calls=%d", result, err, calls.Load())
	}
}

func (client *allocationDrainCollectorClient) InspectStageAllocationDrain(ctx context.Context, request *velav1.ModelRuntimeServiceInspectStageAllocationDrainRequest, _ ...grpc.CallOption) (*velav1.ModelRuntimeServiceInspectStageAllocationDrainResponse, error) {
	result, err := client.reply(ctx, request.Scope)
	return &velav1.ModelRuntimeServiceInspectStageAllocationDrainResponse{Result: result}, err
}
