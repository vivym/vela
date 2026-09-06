package stageworkermembertransport

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/modelruntime"
	"github.com/vivym/vela/internal/stageauthority"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestMemberAllocationAuthoritiesTLSUnixRecoversHistoricalCandidates(t *testing.T) {
	for index, outcome := range []string{"confirmed", "not-applied", "applied-response-lost"} {
		t.Run(outcome, func(t *testing.T) {
			f, floor, _ := newMemberFloorFixture(t)
			directory := privateMemberFloorDirectory(t)
			chain := startMemberFloorChain(t, f, floor.Command.Disposition, directory, true, false)
			if response, err := chain.client.PrepareStage(t.Context(), &velav1.ModelRuntimeServicePrepareStageRequest{Authority: f.authority, ExecutionSpec: f.spec}); err != nil || response.GetDecision() != velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_ACCEPTED {
				t.Fatalf("prepare: %v %v", response, err)
			}
			renewed := memberHistoryRenewal(t, f, f.authority, time.Second)
			chain.backend.renewalResponseFault.Store(int32(index))
			if response, err := chain.client.Status(t.Context(), &velav1.ModelRuntimeServiceStatusRequest{Authority: renewed}); err != nil || (response.GetDecision() == velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_ACCEPTED) != (index == 0) {
				t.Fatalf("renewal outcome: %v %v", response, err)
			}
			chain.close()
			f.runtime.identity.ModelRuntimeEpoch++
			f.runtime.identity.ModelResidencyId, f.runtime.identity.StageProfileRevisionId = uuid.NewString(), uuid.NewString()
			f.runtime.identity.RuntimeIdentity = "replacement-runtime"
			chain = startMemberFloorChain(t, f, floor.Command.Disposition, directory, false, false)
			scope := &velav1.ModelRuntimeExecutionDrainScope{SchemaVersion: 1, Identity: proto.Clone(f.runtime.identity).(*velav1.ModelRuntimeIdentity), Authority: renewed}
			request := &velav1.ModelRuntimeServiceInspectStageAllocationAuthoritiesRequest{Scope: scope}
			read, err := chain.client.InspectStageAllocationAuthorities(t.Context(), request)
			confirmed := f.authority
			if index == 0 {
				confirmed = renewed
			}
			if err != nil || !proto.Equal(read.GetAuthorities().GetOriginal(), f.authority) || !proto.Equal(read.GetAuthorities().GetAccepted(), renewed) || !proto.Equal(read.GetAuthorities().GetConfirmed(), confirmed) {
				t.Fatalf("TLS/UDS restart changed historical candidates: %v %v", read, err)
			}
			nonleader := chain.dial(t, chain.followerCredentials)
			if response, err := nonleader.InspectStageAllocationAuthorities(t.Context(), request); response != nil || status.Code(err) != codes.PermissionDenied {
				t.Fatalf("nonleader read retained history: %v %v", response, err)
			}
			stale := proto.Clone(request).(*velav1.ModelRuntimeServiceInspectStageAllocationAuthoritiesRequest)
			stale.Scope.Identity.ModelRuntimeEpoch--
			if response, err := chain.client.InspectStageAllocationAuthorities(t.Context(), stale); response != nil || status.Code(err) != codes.FailedPrecondition {
				t.Fatalf("stale reader impersonated current journal owner: %v %v", response, err)
			}
			if response, err := chain.client.DrainStageExecution(t.Context(), &velav1.ModelRuntimeServiceDrainStageExecutionRequest{Scope: scope}); response != nil || status.Code(err) != codes.FailedPrecondition {
				t.Fatalf("historical candidates entered replacement backend: %v %v", response, err)
			}
			drain, err := chain.client.InspectStageAllocationDrain(t.Context(), &velav1.ModelRuntimeServiceInspectStageAllocationDrainRequest{Scope: scope})
			if err != nil || drain.GetResult().GetCheckpoint() != nil {
				t.Fatalf("history read invented drain: %v %v", drain, err)
			}
			ready, err := chain.supervisor.ProbeReadiness(t.Context(), &velav1.ModelRuntimeServiceProbeReadinessRequest{
				Identity: scope.Identity, Check: velav1.ModelRuntimeReadinessCheck_MODEL_RUNTIME_READINESS_CHECK_DEVICE,
			})
			if err != nil || ready.GetReady() || !strings.Contains(ready.GetDetail(), modelruntime.ErrExecutionDrainUnproven.Error()) {
				t.Fatalf("history read released pending-writer restrictions: %v %v", ready, err)
			}
		})
	}
}

func TestMemberAllocationAuthoritiesRejectsInvalidResponsesAtBothBoundaries(t *testing.T) {
	faults := []string{"missing", "schema", "digest", "owner", "unknown", "detail-size", "detail-utf8", "decision", "rejected-history", "stale-history", "empty-history", "unknown-history", "no-original", "no-accepted", "unconfirmed-renewal", "accepted-regressed", "confirmed-after-accepted", "confirmed-before-original", "original-signature", "accepted-signature", "confirmed-signature", "accepted-size", "confirmed-size", "original-size", "signed-other-nonce", "signed-other-token", "signed-other-sequence", "signed-other-runtime", "future-candidate", "candidate-unknown", "late", "request-mutation", "wrapper-unknown", "wrapper-missing"}
	for _, boundary := range []string{"runtime", "member"} {
		for _, fault := range faults {
			if boundary == "runtime" && strings.HasPrefix(fault, "wrapper-") {
				continue
			}
			t.Run(boundary+"/"+fault, func(t *testing.T) {
				f, _, _ := newMemberFloorFixture(t)
				scope, result := memberHistoryResponse(t, f)
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				history := result.Authorities
				switch fault {
				case "missing", "wrapper-missing":
					result = nil
				case "schema":
					result.SchemaVersion++
				case "digest":
					result.AuthorityDigest[0] ^= 1
				case "owner":
					result.Identity.ModelRuntimeEpoch++
				case "unknown":
					result.ProtoReflect().SetUnknown([]byte{0x78, 1})
				case "detail-size":
					result.Detail = strings.Repeat("x", 1001)
				case "detail-utf8":
					result.Detail = string([]byte{0xff})
				case "decision":
					result.Decision = velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_REPLAYED
				case "rejected-history":
					result.Decision = velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_REJECTED
				case "stale-history":
					result.Decision = velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_STALE
				case "empty-history":
					result.Authorities = &velav1.ModelRuntimeRetainedAllocationAuthorities{}
				case "unknown-history":
					history.ProtoReflect().SetUnknown([]byte{0x78, 1})
				case "no-original":
					history.Original = nil
				case "no-accepted":
					history.Accepted = nil
				case "unconfirmed-renewal":
					history.Confirmed = nil
				case "accepted-regressed":
					history.Original, history.Accepted = history.Accepted, history.Original
				case "confirmed-after-accepted":
					history.Accepted, history.Confirmed = history.Original, history.Accepted
				case "confirmed-before-original":
					history.Original = history.Accepted
				case "original-signature":
					history.Original.Signature[0] ^= 1
				case "accepted-signature":
					history.Accepted.Signature[0] ^= 1
				case "confirmed-signature":
					history.Confirmed.Signature[0] ^= 1
				case "accepted-size":
					history.Accepted.Signature = make([]byte, 65<<10)
				case "confirmed-size":
					history.Confirmed.Signature = make([]byte, 65<<10)
				case "original-size":
					history.Original.Signature = make([]byte, 65<<10)
				case "candidate-unknown":
					history.Accepted.ProtoReflect().SetUnknown([]byte{0xf8, 7, 1})
				case "signed-other-nonce", "signed-other-token", "signed-other-sequence", "signed-other-runtime", "future-candidate":
					switch fault {
					case "signed-other-nonce":
						history.Accepted.ExecutionNonce[0] ^= 1
					case "signed-other-token":
						history.Accepted.LeaseToken[0] ^= 1
					case "signed-other-sequence":
						history.Accepted.ExecutionSequence++
					case "signed-other-runtime":
						history.Accepted.Members[1].ModelRuntimeEpoch++
					case "future-candidate":
						history.Accepted.IssuedAt = timestamppb.New(campaignNow().Add(time.Hour))
						history.Accepted.ExpiresAt = timestamppb.New(history.Accepted.IssuedAt.AsTime().Add(history.Accepted.MonotonicValidFor.AsDuration()))
					}
					var err error
					history.Accepted, err = f.signer.Sign(history.Accepted)
					if err != nil {
						t.Fatal(err)
					}
				}
				reply := func(received *velav1.ModelRuntimeExecutionDrainScope) *velav1.ModelRuntimeServiceInspectStageAllocationAuthoritiesResponse {
					if fault == "late" {
						cancel()
					}
					if fault == "request-mutation" {
						received.Identity.ModelRuntimeEpoch++
						result.Identity = proto.Clone(received.Identity).(*velav1.ModelRuntimeIdentity)
					}
					return result
				}
				request := &velav1.ModelRuntimeServiceInspectStageAllocationAuthoritiesRequest{Scope: scope}
				var err error
				if boundary == "runtime" {
					f.server.runtime = &allocationAuthoritiesRuntimeReply{reply: reply}
					_, err = f.server.InspectStageAllocationAuthorities(ctx, &velav1.StageWorkerMemberServiceInspectStageAllocationAuthoritiesRequest{TargetWorkerMemberId: f.local.ID, Command: request})
				} else {
					client := floorTestClient(f, &allocationAuthoritiesMemberReply{reply: reply, wrapperFault: fault})
					_, err = client.InspectStageAllocationAuthorities(ctx, request)
				}
				want := codes.DataLoss
				if fault == "late" {
					want = codes.Canceled
				}
				if status.Code(err) != want {
					t.Fatalf("invalid history escaped %s boundary: %v", boundary, err)
				}
			})
		}
	}
}

func TestMemberAllocationAuthoritiesAcceptsOnlyDefinedHistoryStates(t *testing.T) {
	for _, boundary := range []string{"runtime", "member"} {
		for _, state := range []string{"unknown", "legacy", "intent", "uncertain", "confirmed", "rejected", "stale"} {
			t.Run(boundary+"/"+state, func(t *testing.T) {
				f, _, _ := newMemberFloorFixture(t)
				scope, result := memberHistoryResponse(t, f)
				switch state {
				case "unknown", "rejected", "stale":
					result.Authorities = nil
					switch state {
					case "rejected":
						result.Decision = velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_REJECTED
					case "stale":
						result.Decision = velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_STALE
					}
				case "legacy":
					result.Authorities.Accepted, result.Authorities.Confirmed = nil, nil
				case "intent":
					result.Authorities.Accepted = proto.Clone(result.Authorities.Original).(*velav1.StageAuthority)
					result.Authorities.Confirmed = nil
				case "confirmed":
					result.Authorities.Confirmed = proto.Clone(result.Authorities.Accepted).(*velav1.StageAuthority)
				}
				reply := func(*velav1.ModelRuntimeExecutionDrainScope) *velav1.ModelRuntimeServiceInspectStageAllocationAuthoritiesResponse {
					return result
				}
				request := &velav1.ModelRuntimeServiceInspectStageAllocationAuthoritiesRequest{Scope: scope}
				var read *velav1.ModelRuntimeServiceInspectStageAllocationAuthoritiesResponse
				var err error
				if boundary == "runtime" {
					f.server.runtime = &allocationAuthoritiesRuntimeReply{reply: reply}
					var response *velav1.StageWorkerMemberServiceInspectStageAllocationAuthoritiesResponse
					response, err = f.server.InspectStageAllocationAuthorities(t.Context(), &velav1.StageWorkerMemberServiceInspectStageAllocationAuthoritiesRequest{TargetWorkerMemberId: f.local.ID, Command: request})
					read = response.GetResult()
				} else {
					client := floorTestClient(f, &allocationAuthoritiesMemberReply{reply: reply})
					read, err = client.InspectStageAllocationAuthorities(t.Context(), request)
				}
				if err != nil || !proto.Equal(read, result) {
					t.Fatalf("valid history state lost: %v %v", read, err)
				}
				read.Identity.ModelRuntimeEpoch++
				if proto.Equal(read, result) {
					t.Fatal("forwarded history aliases the source response")
				}
			})
		}
	}
}

func TestMemberAllocationAuthoritiesRejectsInvalidRequestsBeforeForwarding(t *testing.T) {
	for _, boundary := range []string{"runtime", "member"} {
		for _, fault := range []string{"nil", "unknown-request", "missing-scope", "schema", "unknown-scope", "missing-identity", "unknown-identity", "member", "epoch", "signature", "size", "nil-context", "cancel"} {
			t.Run(boundary+"/"+fault, func(t *testing.T) {
				f, _, _ := newMemberFloorFixture(t)
				scope, result := memberHistoryResponse(t, f)
				request := &velav1.ModelRuntimeServiceInspectStageAllocationAuthoritiesRequest{Scope: scope}
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
					scope.SchemaVersion++
				case "unknown-scope":
					scope.ProtoReflect().SetUnknown([]byte{0x78, 1})
				case "missing-identity":
					scope.Identity = nil
				case "unknown-identity":
					scope.Identity.ProtoReflect().SetUnknown([]byte{0x78, 1})
				case "member":
					scope.Identity.WorkerMemberId = f.leader.ID
				case "epoch":
					scope.Identity.WorkerInstanceEpoch++
				case "signature":
					scope.Authority.Signature[0] ^= 1
				case "size":
					scope.Authority.Signature = make([]byte, 66<<10)
				case "nil-context":
					ctx = nil
				case "cancel":
					cancel()
				}
				calls := 0
				reply := func(*velav1.ModelRuntimeExecutionDrainScope) *velav1.ModelRuntimeServiceInspectStageAllocationAuthoritiesResponse {
					calls++
					return result
				}
				var err error
				if boundary == "runtime" {
					f.server.runtime = &allocationAuthoritiesRuntimeReply{reply: reply}
					_, err = f.server.InspectStageAllocationAuthorities(ctx, &velav1.StageWorkerMemberServiceInspectStageAllocationAuthoritiesRequest{TargetWorkerMemberId: f.local.ID, Command: request})
				} else {
					client := floorTestClient(f, &allocationAuthoritiesMemberReply{reply: reply})
					_, err = client.InspectStageAllocationAuthorities(ctx, request)
				}
				if err == nil || calls != 0 {
					t.Fatalf("invalid history request reached next boundary: calls=%d err=%v", calls, err)
				}
			})
		}
	}
}

func TestMemberAllocationAuthoritiesRejectsCancellationDuringResultValidation(t *testing.T) {
	for _, boundary := range []string{"runtime", "member"} {
		t.Run(boundary, func(t *testing.T) {
			f, _, _ := newMemberFloorFixture(t)
			scope, result := memberHistoryResponse(t, f)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			validatingResponse := false
			validator, err := stageauthority.NewValidator(map[string][]byte{"authority-v1": []byte("0123456789abcdef0123456789abcdef")}, func() time.Time {
				if validatingResponse {
					cancel()
				}
				return campaignNow()
			})
			if err != nil {
				t.Fatal(err)
			}
			f.server.validator = validator
			reply := func(*velav1.ModelRuntimeExecutionDrainScope) *velav1.ModelRuntimeServiceInspectStageAllocationAuthoritiesResponse {
				validatingResponse = true
				return result
			}
			request := &velav1.ModelRuntimeServiceInspectStageAllocationAuthoritiesRequest{Scope: scope}
			if boundary == "runtime" {
				f.server.runtime = &allocationAuthoritiesRuntimeReply{reply: reply}
				_, err = f.server.InspectStageAllocationAuthorities(ctx, &velav1.StageWorkerMemberServiceInspectStageAllocationAuthoritiesRequest{TargetWorkerMemberId: f.local.ID, Command: request})
			} else {
				client := floorTestClient(f, &allocationAuthoritiesMemberReply{reply: reply})
				_, err = client.InspectStageAllocationAuthorities(ctx, request)
			}
			if status.Code(err) != codes.Canceled {
				t.Fatalf("history response escaped canceled validation: %v", err)
			}
		})
	}
}

func TestMemberAllocationAuthoritiesCancellationDuringRequestValidationStopsForwarding(t *testing.T) {
	for _, boundary := range []string{"runtime", "member"} {
		t.Run(boundary, func(t *testing.T) {
			f, _, _ := newMemberFloorFixture(t)
			scope, result := memberHistoryResponse(t, f)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			validator, err := stageauthority.NewValidator(map[string][]byte{"authority-v1": []byte("0123456789abcdef0123456789abcdef")}, func() time.Time {
				cancel()
				return campaignNow()
			})
			if err != nil {
				t.Fatal(err)
			}
			f.server.validator = validator
			calls := 0
			reply := func(*velav1.ModelRuntimeExecutionDrainScope) *velav1.ModelRuntimeServiceInspectStageAllocationAuthoritiesResponse {
				calls++
				return result
			}
			request := &velav1.ModelRuntimeServiceInspectStageAllocationAuthoritiesRequest{Scope: scope}
			if boundary == "runtime" {
				f.server.runtime = &allocationAuthoritiesRuntimeReply{reply: reply}
				_, err = f.server.InspectStageAllocationAuthorities(ctx, &velav1.StageWorkerMemberServiceInspectStageAllocationAuthoritiesRequest{TargetWorkerMemberId: f.local.ID, Command: request})
			} else {
				client := floorTestClient(f, &allocationAuthoritiesMemberReply{reply: reply})
				_, err = client.InspectStageAllocationAuthorities(ctx, request)
			}
			if status.Code(err) != codes.Canceled || calls != 0 {
				t.Fatalf("canceled history query forwarded: calls=%d err=%v", calls, err)
			}
		})
	}
}

func memberHistoryRenewal(t *testing.T, f *serverFixture, original *velav1.StageAuthority, delta time.Duration) *velav1.StageAuthority {
	t.Helper()
	authority := proto.Clone(original).(*velav1.StageAuthority)
	authority.StageVersion++
	authority.IssuedAt = timestamppb.New(authority.IssuedAt.AsTime().Add(delta))
	authority.ExpiresAt = timestamppb.New(authority.ExpiresAt.AsTime().Add(delta))
	renewed, err := f.signer.Sign(authority)
	if err != nil {
		t.Fatal(err)
	}
	return renewed
}

func memberHistoryResponse(t *testing.T, f *serverFixture) (*velav1.ModelRuntimeExecutionDrainScope, *velav1.ModelRuntimeServiceInspectStageAllocationAuthoritiesResponse) {
	t.Helper()
	renewed := memberHistoryRenewal(t, f, f.authority, time.Second)
	query := proto.Clone(renewed).(*velav1.StageAuthority)
	query.StageVersion++
	query, err := f.signer.Sign(query)
	if err != nil {
		t.Fatal(err)
	}
	digest, err := stageauthority.Digest(query)
	if err != nil {
		t.Fatal(err)
	}
	identity := proto.Clone(f.runtime.identity).(*velav1.ModelRuntimeIdentity)
	scope := &velav1.ModelRuntimeExecutionDrainScope{SchemaVersion: 1, Identity: identity, Authority: query}
	return scope, &velav1.ModelRuntimeServiceInspectStageAllocationAuthoritiesResponse{SchemaVersion: 1,
		Identity: proto.Clone(identity).(*velav1.ModelRuntimeIdentity), AuthorityDigest: digest[:],
		Decision: velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_ACCEPTED,
		Authorities: &velav1.ModelRuntimeRetainedAllocationAuthorities{Original: proto.Clone(f.authority).(*velav1.StageAuthority),
			Accepted: renewed, Confirmed: proto.Clone(f.authority).(*velav1.StageAuthority)},
	}
}

type allocationAuthoritiesRuntimeReply struct {
	velav1.ModelRuntimeServiceClient
	reply func(*velav1.ModelRuntimeExecutionDrainScope) *velav1.ModelRuntimeServiceInspectStageAllocationAuthoritiesResponse
}

func (runtime *allocationAuthoritiesRuntimeReply) InspectStageAllocationAuthorities(_ context.Context, request *velav1.ModelRuntimeServiceInspectStageAllocationAuthoritiesRequest, _ ...grpc.CallOption) (*velav1.ModelRuntimeServiceInspectStageAllocationAuthoritiesResponse, error) {
	return runtime.reply(request.GetScope()), nil
}

type allocationAuthoritiesMemberReply struct {
	velav1.StageWorkerMemberServiceClient
	reply        func(*velav1.ModelRuntimeExecutionDrainScope) *velav1.ModelRuntimeServiceInspectStageAllocationAuthoritiesResponse
	wrapperFault string
}

func (member *allocationAuthoritiesMemberReply) InspectStageAllocationAuthorities(_ context.Context, request *velav1.StageWorkerMemberServiceInspectStageAllocationAuthoritiesRequest, _ ...grpc.CallOption) (*velav1.StageWorkerMemberServiceInspectStageAllocationAuthoritiesResponse, error) {
	if member.wrapperFault == "wrapper-missing" {
		return nil, nil
	}
	response := &velav1.StageWorkerMemberServiceInspectStageAllocationAuthoritiesResponse{Result: member.reply(request.GetCommand().GetScope())}
	if member.wrapperFault == "wrapper-unknown" {
		response.ProtoReflect().SetUnknown([]byte{0x78, 1})
	}
	return response, nil
}
