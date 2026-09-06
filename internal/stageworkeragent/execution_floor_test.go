package stageworkeragent_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/stageauthority"
	"github.com/vivym/vela/internal/stageworkeragent"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type floorCollectorFixture struct {
	admissionFixture
	config      stageworkeragent.Config
	disposition *velav1.StageTerminalDisposition
	clients     []*floorCollectorClient
}

type floorCollectorClient struct {
	velav1.ModelRuntimeServiceClient
	calls atomic.Int64
	reply func(context.Context, *velav1.ModelRuntimeServiceInstallStageExecutionFloorRequest) (*velav1.ModelRuntimeServiceInstallStageExecutionFloorResponse, error)
}

func (client *floorCollectorClient) InstallStageExecutionFloor(ctx context.Context, request *velav1.ModelRuntimeServiceInstallStageExecutionFloorRequest, _ ...grpc.CallOption) (*velav1.ModelRuntimeServiceInstallStageExecutionFloorResponse, error) {
	client.calls.Add(1)
	return client.reply(ctx, request)
}

func newFloorCollectorFixture(t *testing.T) *floorCollectorFixture {
	t.Helper()
	f := &floorCollectorFixture{admissionFixture: newAdmissionFixture(t)}
	a := f.assignment.Authority
	digest, err := stageauthority.Digest(a)
	if err != nil {
		t.Fatal(err)
	}
	f.config.ExecutionFloor = &stageworkeragent.ExecutionFloorConfig{Validator: f.admissionFixture.config.Validator}
	f.disposition = &velav1.StageTerminalDisposition{
		SchemaVersion: 1, InputDisposition: velav1.StageInputDisposition_STAGE_INPUT_DISPOSITION_INPUTS_UNUSED,
		OriginalAuthorityDigest: digest[:], OrganizationId: uuid.NewString(), ProjectId: uuid.NewString(),
		JobId: a.JobId, AttemptId: a.AttemptId, StageRunId: a.StageRunId,
		StageAttemptId: a.StageAttemptId, StageAllocationId: a.StageAllocationId, StageLeaseId: a.StageLeaseId,
		TerminalState: velav1.StageTerminalState_STAGE_TERMINAL_STATE_FAILED, StageFence: a.StageFence + 1, StageVersion: a.StageVersion + 1,
		WorkerInstanceId: a.WorkerInstanceId, WorkerInstanceEpoch: a.WorkerInstanceEpoch, WorkerMemberId: a.Members[0].WorkerMemberId,
		ControlSessionEpoch: 7, DeviceSetDigest: bytes.Clone(a.DeviceSetDigest), MembershipDigest: bytes.Clone(a.MembershipDigest),
		Devices: a.Devices, Cutoff: 7, ObservedAt: a.IssuedAt, ExpiresAt: timestamppb.New(a.IssuedAt.AsTime().Add(time.Minute)), SigningKeyId: a.SigningKeyId,
	}
	allocation := &velav1.StageTerminalAllocation{
		StageAttemptId: a.StageAttemptId, StageAllocationId: a.StageAllocationId, StageLeaseId: a.StageLeaseId,
		ExecutionSequence: a.ExecutionSequence, ExecutionNonce: bytes.Clone(a.ExecutionNonce),
		ModelResidencyId: a.ModelResidencyId, ModelRuntimeIdentity: a.ModelRuntimeIdentity,
		StageProfileRevisionId: a.StageProfileRevisionId, BarrierGeneration: a.ModelRuntimeBarrierGeneration,
	}
	for index, binding := range f.admissionFixture.config.Bindings {
		subset := [sha256.Size]byte(bytes.Repeat([]byte{byte(0x30 + index)}, sha256.Size))
		f.config.ExecutionFloor.Bindings = append(f.config.ExecutionFloor.Bindings, stageworkeragent.ExecutionFloorBinding{
			Runtime: binding.Runtime, IdentityDigest: binding.IdentityDigest, DeviceSubsetDigest: subset,
		})
		allocation.Members = append(allocation.Members, &velav1.StageTerminalMember{
			WorkerMemberId: binding.Runtime.WorkerMemberID, MemberEpoch: binding.Runtime.WorkerMemberEpoch,
			ModelRuntimeEpoch: binding.Runtime.ModelRuntimeEpoch, IdentityDigest: bytes.Clone(binding.IdentityDigest[:]), DeviceSubsetDigest: bytes.Clone(subset[:]),
		})
		client := &floorCollectorClient{reply: f.acknowledge}
		f.clients = append(f.clients, client)
		f.config.Members = append(f.config.Members, stageworkeragent.RuntimeMember{ID: binding.Runtime.WorkerMemberID, Client: client})
	}
	retry := proto.Clone(allocation).(*velav1.StageTerminalAllocation)
	retry.StageAttemptId, retry.StageAllocationId, retry.StageLeaseId = uuid.NewString(), uuid.NewString(), uuid.NewString()
	retry.ExecutionSequence, retry.BarrierGeneration = 7, 700
	retry.ModelResidencyId, retry.StageProfileRevisionId, retry.ModelRuntimeIdentity = uuid.NewString(), uuid.NewString(), "aux-runtime"
	for index, member := range retry.Members {
		member.ModelRuntimeEpoch = int64(19 + index)
		binding := f.config.ExecutionFloor.Bindings[index]
		binding.Runtime.ModelResidencyID, binding.Runtime.StageProfileRevisionID = retry.ModelResidencyId, retry.StageProfileRevisionId
		binding.Runtime.ModelRuntimeIdentity, binding.Runtime.ModelRuntimeEpoch = retry.ModelRuntimeIdentity, member.ModelRuntimeEpoch
		f.config.ExecutionFloor.Bindings = append(f.config.ExecutionFloor.Bindings, binding)
	}
	f.disposition.Allocations = []*velav1.StageTerminalAllocation{allocation, retry}
	f.signDisposition(t)
	return f
}

func (f *floorCollectorFixture) signDisposition(t *testing.T) {
	t.Helper()
	var err error
	f.disposition, err = f.signer.SignTerminalDisposition(f.disposition)
	if err != nil {
		t.Fatal(err)
	}
}

func (f *floorCollectorFixture) agent(t *testing.T) *stageworkeragent.Agent {
	t.Helper()
	agent, err := stageworkeragent.New(f.config)
	if err != nil {
		t.Fatal(err)
	}
	return agent
}

func (f *floorCollectorFixture) acknowledge(_ context.Context, request *velav1.ModelRuntimeServiceInstallStageExecutionFloorRequest) (*velav1.ModelRuntimeServiceInstallStageExecutionFloorResponse, error) {
	verified, err := f.admissionFixture.config.Validator.ValidateTerminalDispositionSignature(request.Disposition)
	if err != nil {
		return nil, err
	}
	return &velav1.ModelRuntimeServiceInstallStageExecutionFloorResponse{
		SchemaVersion: request.GetSchemaVersion(), Identity: proto.Clone(request.Identity).(*velav1.ModelRuntimeIdentity),
		DispositionDigest: verified.Digest[:], InstalledCutoff: request.Disposition.Cutoff, Durable: true,
		Decision: velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_ACCEPTED,
	}, nil
}

func TestExecutionFloorCollectionWaitsForAllMembersAndRetriesLostResponse(t *testing.T) {
	f := newFloorCollectorFixture(t)
	entered := make(chan string, 2)
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	var lost atomic.Bool
	for index, client := range f.clients {
		client.reply = func(ctx context.Context, request *velav1.ModelRuntimeServiceInstallStageExecutionFloorRequest) (*velav1.ModelRuntimeServiceInstallStageExecutionFloorResponse, error) {
			if !proto.Equal(request.Disposition, f.disposition) {
				return nil, errors.New("member received incomplete history")
			}
			if client.calls.Load() == 1 {
				entered <- request.Identity.WorkerMemberId
				select {
				case <-release:
				case <-ctx.Done():
					return nil, ctx.Err()
				}
			}
			response, err := f.acknowledge(ctx, request)
			if index == 1 && !lost.Swap(true) {
				return nil, errors.New("injected response loss after installation")
			}
			return response, err
		}
	}
	agent := f.agent(t)
	type outcome struct {
		result stageworkeragent.ExecutionFloorResult
		err    error
	}
	done := make(chan outcome, 1)
	go func() {
		result, err := agent.InstallExecutionFloor(t.Context(), f.disposition)
		done <- outcome{result, err}
	}()
	for range 2 {
		select {
		case <-entered:
		case <-time.After(5 * time.Second):
			t.Fatal("floor did not dispatch to both members")
		}
	}
	select {
	case <-done:
		t.Fatal("floor collection completed before acknowledgement")
	default:
	}
	// Sending two tokens releases both first calls while keeping cleanup simple.
	for range 2 {
		release <- struct{}{}
	}
	first := <-done
	if first.err == nil || first.result.AllInstalled || len(first.result.Acknowledgements) != 1 || first.result.RequiredMembers != 2 {
		t.Fatalf("partial installation accepted: %+v %v", first.result, first.err)
	}
	result, err := agent.InstallExecutionFloor(t.Context(), f.disposition)
	if err != nil || !result.AllInstalled || len(result.Acknowledgements) != 2 || result.Cutoff != 7 || result.DispositionDigest == ([sha256.Size]byte{}) {
		t.Fatalf("full retry: %+v %v", result, err)
	}
	for _, client := range f.clients {
		if client.calls.Load() != 2 {
			t.Fatal("retry reused an old acknowledgement")
		}
	}
}

func TestExecutionFloorRecoveryValidatesReadersAndReplyVersion(t *testing.T) {
	for _, fault := range []string{"missing", "unknown", "epoch", "topology", "duplicate-binding", "v1-reply"} {
		t.Run(fault, func(t *testing.T) {
			f := newFloorCollectorFixture(t)
			readers := drainCollectorTargets(f)
			id := f.config.Members[0].ID
			switch fault {
			case "missing":
				delete(readers, id)
			case "unknown":
				readers[uuid.NewString()] = readers[id]
				delete(readers, id)
			case "epoch":
				readers[id].ModelRuntimeEpoch++
			case "topology":
				readers[id].MembershipDigest[0] ^= 1
			case "duplicate-binding":
				f.config.ExecutionFloor.Bindings = append(f.config.ExecutionFloor.Bindings, f.config.ExecutionFloor.Bindings[0])
			case "v1-reply":
				f.clients[0].reply = func(ctx context.Context, request *velav1.ModelRuntimeServiceInstallStageExecutionFloorRequest) (*velav1.ModelRuntimeServiceInstallStageExecutionFloorResponse, error) {
					response, err := f.acknowledge(ctx, request)
					response.SchemaVersion = 1
					return response, err
				}
			}
			result, err := f.agent(t).InstallRecoveryExecutionFloor(t.Context(), f.disposition, readers)
			if err == nil || result.AllInstalled {
				t.Fatalf("invalid recovery floor accepted: %+v %v", result, err)
			}
			if fault != "v1-reply" {
				for _, client := range f.clients {
					if client.calls.Load() != 0 {
						t.Fatal("invalid reader reached an RPC")
					}
				}
				f.config.ExecutionFloor.CurrentReaders = readers
				if _, err := stageworkeragent.New(f.config); err == nil {
					t.Fatal("automatic recovery accepted invalid reader configuration")
				}
			}
		})
	}
}

func TestExecutionFloorCollectionValidatesCompleteHistoryBeforeAnyRPC(t *testing.T) {
	for _, mutation := range []string{"signature", "expired", "future", "unknown", "worker", "device", "membership digest", "missing member", "unconfigured member", "history profile", "local epoch", "barrier is not local epoch", "identity digest", "subset digest", "missing route", "duplicate route", "duplicate configured device", "duplicate configured member"} {
		t.Run(mutation, func(t *testing.T) {
			f := newFloorCollectorFixture(t)
			sign := true
			switch mutation {
			case "signature":
				f.disposition.Signature[0] ^= 1
				sign = false
			case "expired":
				f.clock.Add(int64(2 * time.Minute))
			case "future":
				f.clock.Add(-int64(time.Second))
			case "unknown":
				f.disposition.ProtoReflect().SetUnknown([]byte{0x78, 1})
				sign = false
			case "worker":
				f.disposition.WorkerInstanceEpoch++
			case "device":
				f.disposition.Devices[0].DeviceEpoch++
			case "membership digest":
				f.disposition.MembershipDigest[0] ^= 1
			case "missing member":
				for _, allocation := range f.disposition.Allocations {
					allocation.Members = allocation.Members[:1]
				}
			case "unconfigured member":
				for _, allocation := range f.disposition.Allocations {
					allocation.Members[1].WorkerMemberId = "ffffffff-ffff-ffff-ffff-ffffffffffff"
				}
			case "history profile":
				f.disposition.Allocations[1].StageProfileRevisionId = uuid.NewString()
			case "local epoch":
				f.disposition.Allocations[1].Members[1].ModelRuntimeEpoch++
			case "barrier is not local epoch":
				f.config.ExecutionFloor.Bindings[3].Runtime.ModelRuntimeEpoch = f.disposition.Allocations[1].BarrierGeneration
			case "identity digest", "subset digest":
				for _, allocation := range f.disposition.Allocations {
					if mutation == "identity digest" {
						allocation.Members[1].IdentityDigest[0] ^= 1
					} else {
						allocation.Members[1].DeviceSubsetDigest[0] ^= 1
					}
				}
			case "missing route":
				f.config.ExecutionFloor.Bindings = f.config.ExecutionFloor.Bindings[:3]
			case "duplicate route":
				f.config.ExecutionFloor.Bindings = append(f.config.ExecutionFloor.Bindings, f.config.ExecutionFloor.Bindings[0])
			case "duplicate configured device":
				f.config.ExecutionFloor.Bindings[0].Runtime.Devices[1] = f.config.ExecutionFloor.Bindings[0].Runtime.Devices[0]
			case "duplicate configured member":
				f.config.ExecutionFloor.Bindings[0].Runtime.Members[1] = f.config.ExecutionFloor.Bindings[0].Runtime.Members[0]
			}
			if sign {
				f.signDisposition(t)
			}
			result, err := f.agent(t).InstallExecutionFloor(t.Context(), f.disposition)
			if err == nil || result.AllInstalled || len(result.Acknowledgements) != 0 {
				t.Fatalf("invalid history accepted: %+v %v", result, err)
			}
			for _, client := range f.clients {
				if client.calls.Load() != 0 {
					t.Fatal("invalid history reached a Runtime")
				}
			}
		})
	}
}

func TestExecutionFloorCollectionRejectsUnboundAcknowledgements(t *testing.T) {
	for _, mutation := range []string{"nil", "error with success", "schema", "unknown", "identity unknown", "member", "runtime epoch", "digest", "cutoff", "non-durable", "decision", "mutated request"} {
		t.Run(mutation, func(t *testing.T) {
			f := newFloorCollectorFixture(t)
			original := proto.Clone(f.disposition)
			f.clients[1].reply = func(ctx context.Context, request *velav1.ModelRuntimeServiceInstallStageExecutionFloorRequest) (*velav1.ModelRuntimeServiceInstallStageExecutionFloorResponse, error) {
				if mutation == "mutated request" {
					request.Identity.ModelRuntimeEpoch++
					request.Disposition.Cutoff++
					return &velav1.ModelRuntimeServiceInstallStageExecutionFloorResponse{SchemaVersion: 1, Identity: request.Identity, Durable: true}, nil
				}
				response, err := f.acknowledge(ctx, request)
				if err != nil {
					return nil, err
				}
				switch mutation {
				case "nil":
					return nil, nil
				case "error with success":
					return response, errors.New("lost response")
				case "schema":
					response.SchemaVersion++
				case "unknown":
					response.ProtoReflect().SetUnknown([]byte{0x78, 1})
				case "identity unknown":
					response.Identity.ProtoReflect().SetUnknown([]byte{0x78, 1})
				case "member":
					response.Identity.WorkerMemberId = f.config.Members[0].ID
				case "runtime epoch":
					response.Identity.ModelRuntimeEpoch++
				case "digest":
					response.DispositionDigest[0] ^= 1
				case "cutoff":
					response.InstalledCutoff--
				case "non-durable":
					response.Durable = false
				case "decision":
					response.Decision = velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_STALE
				}
				return response, nil
			}
			result, err := f.agent(t).InstallExecutionFloor(t.Context(), f.disposition)
			if err == nil || result.AllInstalled || len(result.Acknowledgements) != 1 || !proto.Equal(original, f.disposition) {
				t.Fatalf("unbound response accepted: %+v %v", result, err)
			}
		})
	}
}

func TestExecutionFloorCollectionDeadlineRejectsLateSuccess(t *testing.T) {
	f := newFloorCollectorFixture(t)
	f.config.ExecutionFloor.Timeout = 50 * time.Millisecond
	lateDone := make(chan struct{})
	lateRelease := make(chan struct{})
	f.clients[1].reply = func(ctx context.Context, request *velav1.ModelRuntimeServiceInstallStageExecutionFloorRequest) (*velav1.ModelRuntimeServiceInstallStageExecutionFloorResponse, error) {
		defer close(lateDone)
		<-ctx.Done()
		<-lateRelease
		return f.acknowledge(ctx, request)
	}
	agent := f.agent(t)
	result, err := agent.InstallExecutionFloor(t.Context(), f.disposition)
	close(lateRelease)
	<-lateDone
	if !errors.Is(err, context.DeadlineExceeded) || result.AllInstalled {
		t.Fatalf("deadline became full installation: %+v %v", result, err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if result, err := agent.InstallExecutionFloor(ctx, f.disposition); !errors.Is(err, context.Canceled) || result.AllInstalled || f.clients[1].calls.Load() != 1 {
		t.Fatalf("canceled collection dispatched: %+v %v", result, err)
	}
}

func TestExecutionFloorCollectionRechecksCancellationAfterValidation(t *testing.T) {
	f := newFloorCollectorFixture(t)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	validator, err := stageauthority.NewValidator(map[string][]byte{"barrier-key": bytes.Repeat([]byte{0x6b}, 32)}, func() time.Time {
		cancel()
		return time.Unix(0, f.clock.Load())
	})
	if err != nil {
		t.Fatal(err)
	}
	f.config.ExecutionFloor.Validator = validator
	result, err := f.agent(t).InstallExecutionFloor(ctx, f.disposition)
	if !errors.Is(err, context.Canceled) || result.AllInstalled || result.RequiredMembers != 0 {
		t.Fatalf("cancellation during validation lost: %+v %v", result, err)
	}
	for _, client := range f.clients {
		if client.calls.Load() != 0 {
			t.Fatal("canceled validation reached a Runtime")
		}
	}
}

func TestExecutionFloorCollectionConfigurationIsExplicitAndCopied(t *testing.T) {
	for _, mutation := range []string{"disabled", "missing validator", "missing bindings", "missing member", "unknown member", "missing identity", "missing subset", "timeout"} {
		t.Run(mutation, func(t *testing.T) {
			f := newFloorCollectorFixture(t)
			switch mutation {
			case "disabled":
				f.config.ExecutionFloor = nil
			case "missing validator":
				f.config.ExecutionFloor.Validator = nil
			case "missing bindings":
				f.config.ExecutionFloor.Bindings = nil
			case "missing member":
				f.config.ExecutionFloor.Bindings = f.config.ExecutionFloor.Bindings[:1]
			case "unknown member":
				f.config.ExecutionFloor.Bindings[0].Runtime.WorkerMemberID = uuid.NewString()
			case "missing identity":
				f.config.ExecutionFloor.Bindings[0].IdentityDigest = [sha256.Size]byte{}
			case "missing subset":
				f.config.ExecutionFloor.Bindings[0].DeviceSubsetDigest = [sha256.Size]byte{}
			case "timeout":
				f.config.ExecutionFloor.Timeout = 2 * time.Minute
			}
			agent, err := stageworkeragent.New(f.config)
			if mutation == "disabled" && err == nil {
				_, err = agent.InstallExecutionFloor(t.Context(), f.disposition)
			}
			if err == nil {
				t.Fatal("invalid floor configuration accepted")
			}
		})
	}
	f := newFloorCollectorFixture(t)
	agent := f.agent(t)
	f.config.ExecutionFloor.Bindings[0].Runtime.Devices[0].Epoch++
	f.config.ExecutionFloor.Bindings[0].Runtime.Members[0].Epoch++
	f.config.ExecutionFloor.Bindings[0].Runtime.DeviceSetDigest[0] ^= 1
	f.config.ExecutionFloor.Bindings[0].Runtime.MembershipDigest[0] ^= 1
	f.config.ExecutionFloor.Bindings[0].Runtime.ModelRuntimeEpoch++
	if result, err := agent.InstallExecutionFloor(t.Context(), f.disposition); err != nil || !result.AllInstalled {
		t.Fatalf("caller altered trusted collector config: %+v %v", result, err)
	}
}
