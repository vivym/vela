package stageworkercontrol_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"testing"
	"time"

	"github.com/vivym/vela/internal/materializationauthority"
	"github.com/vivym/vela/internal/stageauthority"
	"github.com/vivym/vela/internal/stageworkercontrol"
	"github.com/vivym/vela/internal/stageworkertransport"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestHandlerRejectsStaleHeartbeatAndReattachBeforeExecution(t *testing.T) {
	now := time.Date(2026, 8, 30, 8, 0, 0, 0, time.UTC)
	keys := map[string][]byte{"control-key": bytes.Repeat([]byte{0x7c}, 32)}
	signer, err := stageauthority.NewSigner(keys)
	if err != nil {
		t.Fatalf("NewSigner: %v", err)
	}
	validator, err := stageauthority.NewValidator(keys, func() time.Time { return now })
	if err != nil {
		t.Fatalf("NewValidator: %v", err)
	}
	active, err := signer.Sign(controlAuthority(now))
	if err != nil {
		t.Fatalf("Sign active: %v", err)
	}
	activeDigest, err := stageauthority.Digest(active)
	if err != nil {
		t.Fatalf("Digest active: %v", err)
	}
	authorizer := &exactActiveAuthorizer{digest: activeDigest}
	materializationKeys := map[string][]byte{
		"materialization-key-v1": bytes.Repeat([]byte{0x7d}, 32),
	}
	materializationValidator, err := materializationauthority.NewValidator(
		materializationKeys, func() time.Time { return now },
	)
	if err != nil {
		t.Fatalf("New materialization Validator: %v", err)
	}
	executor := &recordingControlExecutor{}
	handler, err := stageworkercontrol.NewHandler(stageworkercontrol.Config{
		Validator: validator, Authorizer: authorizer,
		MaterializationValidator:  materializationValidator,
		MaterializationAuthorizer: &exactMaterializationAuthorizer{},
		Executor:                  executor,
	})
	if err != nil {
		t.Fatalf("NewHandler: %v", err)
	}
	identity := stageworkertransport.Identity{SPIFFEID: "spiffe://vela/worker/member-1"}

	for _, operation := range []struct {
		name    string
		request func(*velav1.StageAuthority) *velav1.StageWorkerControlServiceConnectRequest
	}{
		{
			name: "HeartbeatStage",
			request: func(authority *velav1.StageAuthority) *velav1.StageWorkerControlServiceConnectRequest {
				return &velav1.StageWorkerControlServiceConnectRequest{
					RequestId: "81000000-0000-0000-0000-000000000001",
					Operation: &velav1.StageWorkerControlServiceConnectRequest_HeartbeatStage{
						HeartbeatStage: &velav1.HeartbeatStageRequest{Authority: authority, Sequence: 1},
					},
				}
			},
		},
		{
			name: "ReattachStage",
			request: func(authority *velav1.StageAuthority) *velav1.StageWorkerControlServiceConnectRequest {
				return &velav1.StageWorkerControlServiceConnectRequest{
					RequestId: "81000000-0000-0000-0000-000000000002",
					Operation: &velav1.StageWorkerControlServiceConnectRequest_ReattachStage{
						ReattachStage: &velav1.ReattachStageRequest{Authority: authority},
					},
				}
			},
		},
	} {
		t.Run(operation.name, func(t *testing.T) {
			before := executor.calls
			response, handleErr := handler.Handle(
				context.Background(), identity, 11, operation.request(active),
			)
			if handleErr != nil || response.GetStageCommandResult().GetDecision() !=
				velav1.StageWorkerCommandDecision_STAGE_WORKER_COMMAND_DECISION_ACCEPTED {
				t.Fatalf("active Handle = %#v error=%v", response, handleErr)
			}
			if executor.calls != before+1 {
				t.Fatalf("active executor calls = %d, want %d", executor.calls, before+1)
			}

			stale := proto.Clone(active).(*velav1.StageAuthority)
			stale.Members[0].ModelRuntimeEpoch++
			stale.Signature = nil
			stale, err = signer.Sign(stale)
			if err != nil {
				t.Fatalf("Sign stale: %v", err)
			}
			response, handleErr = handler.Handle(
				context.Background(), identity, 12, operation.request(stale),
			)
			if handleErr != nil || response.GetStageCommandResult().GetDecision() !=
				velav1.StageWorkerCommandDecision_STAGE_WORKER_COMMAND_DECISION_STALE {
				t.Fatalf("stale Handle = %#v error=%v", response, handleErr)
			}
			if executor.calls != before+1 {
				t.Fatalf("stale authority reached executor: calls=%d", executor.calls)
			}

			tampered := proto.Clone(active).(*velav1.StageAuthority)
			tampered.StageFence++
			response, handleErr = handler.Handle(
				context.Background(), identity, 12, operation.request(tampered),
			)
			if handleErr != nil || response.GetStageCommandResult().GetDecision() !=
				velav1.StageWorkerCommandDecision_STAGE_WORKER_COMMAND_DECISION_STALE {
				t.Fatalf("tampered Handle = %#v error=%v", response, handleErr)
			}
			if executor.calls != before+1 {
				t.Fatalf("tampered authority reached executor: calls=%d", executor.calls)
			}
		})
	}
}

func TestHandlerLimitsExpiredAuthorityReplayToDurablyAuthorizedSeal(t *testing.T) {
	issuedAt := time.Date(2026, 8, 30, 9, 0, 0, 0, time.UTC)
	keys := map[string][]byte{"control-key": bytes.Repeat([]byte{0x7c}, 32)}
	signer, err := stageauthority.NewSigner(keys)
	if err != nil {
		t.Fatalf("NewSigner: %v", err)
	}
	signed, err := signer.Sign(controlAuthority(issuedAt))
	if err != nil {
		t.Fatalf("Sign: %v", err)
	}
	digest, err := stageauthority.Digest(signed)
	if err != nil {
		t.Fatalf("Digest: %v", err)
	}
	validator, err := stageauthority.NewValidator(
		keys,
		func() time.Time { return issuedAt.Add(2 * time.Minute) },
	)
	if err != nil {
		t.Fatalf("NewValidator: %v", err)
	}
	materializationValidator, err := materializationauthority.NewValidator(
		map[string][]byte{"materialization-key-v1": bytes.Repeat([]byte{0x7d}, 32)},
		func() time.Time { return issuedAt.Add(2 * time.Minute) },
	)
	if err != nil {
		t.Fatalf("New materialization Validator: %v", err)
	}
	authorizer := &exactActiveAuthorizer{digest: digest}
	executor := &recordingControlExecutor{}
	handler, err := stageworkercontrol.NewHandler(stageworkercontrol.Config{
		Validator: validator, Authorizer: authorizer,
		MaterializationValidator:  materializationValidator,
		MaterializationAuthorizer: &exactMaterializationAuthorizer{},
		Executor:                  executor,
	})
	if err != nil {
		t.Fatalf("NewHandler: %v", err)
	}
	identity := stageworkertransport.Identity{SPIFFEID: "spiffe://vela/worker/member-1"}
	sealRequest := func(requestID string, authority *velav1.StageAuthority) *velav1.StageWorkerControlServiceConnectRequest {
		return &velav1.StageWorkerControlServiceConnectRequest{
			RequestId: requestID,
			Operation: &velav1.StageWorkerControlServiceConnectRequest_SealStageOutput{
				SealStageOutput: &velav1.SealStageOutputRequest{
					Authority: authority, LocalReceipt: &velav1.LocalMaterializationReceipt{},
				},
			},
		}
	}

	response, err := handler.Handle(
		context.Background(), identity, 11,
		sealRequest("81000000-0000-0000-0000-000000000030", signed),
	)
	if err != nil || response.GetStageCommandResult().GetDecision() !=
		velav1.StageWorkerCommandDecision_STAGE_WORKER_COMMAND_DECISION_ACCEPTED ||
		authorizer.calls != 1 || executor.calls != 1 || executor.lastAuthorities.Stage == nil ||
		executor.lastAuthorities.Stage.MonotonicValidFor != 0 {
		t.Fatalf(
			"expired seal replay = %#v error=%v authorizer=%d executor=%d authorities=%#v",
			response, err, authorizer.calls, executor.calls, executor.lastAuthorities,
		)
	}

	tampered := proto.Clone(signed).(*velav1.StageAuthority)
	tampered.StageFence++
	response, err = handler.Handle(
		context.Background(), identity, 11,
		sealRequest("81000000-0000-0000-0000-000000000031", tampered),
	)
	if err != nil || response.GetStageCommandResult().GetDecision() !=
		velav1.StageWorkerCommandDecision_STAGE_WORKER_COMMAND_DECISION_STALE ||
		authorizer.calls != 1 || executor.calls != 1 {
		t.Fatalf("tampered expired seal = %#v error=%v", response, err)
	}

	response, err = handler.Handle(
		context.Background(), identity, 11,
		&velav1.StageWorkerControlServiceConnectRequest{
			RequestId: "81000000-0000-0000-0000-000000000032",
			Operation: &velav1.StageWorkerControlServiceConnectRequest_HeartbeatStage{
				HeartbeatStage: &velav1.HeartbeatStageRequest{Authority: signed, Sequence: 1},
			},
		},
	)
	if err != nil || response.GetStageCommandResult().GetDecision() !=
		velav1.StageWorkerCommandDecision_STAGE_WORKER_COMMAND_DECISION_STALE ||
		authorizer.calls != 1 || executor.calls != 1 {
		t.Fatalf("expired heartbeat = %#v error=%v", response, err)
	}

	authorizer.digest = [32]byte{0xff}
	response, err = handler.Handle(
		context.Background(), identity, 11,
		sealRequest("81000000-0000-0000-0000-000000000033", signed),
	)
	if err != nil || response.GetStageCommandResult().GetDecision() !=
		velav1.StageWorkerCommandDecision_STAGE_WORKER_COMMAND_DECISION_STALE ||
		authorizer.calls != 2 || executor.calls != 1 {
		t.Fatalf("durably inactive expired seal = %#v error=%v", response, err)
	}
}

func TestHandlerAuthorizesCommitWithMaterializationAuthorityAfterStageLeaseRevocation(t *testing.T) {
	now := time.Date(2026, 8, 30, 14, 0, 0, 0, time.UTC)
	stageKeys := map[string][]byte{"control-key": bytes.Repeat([]byte{0x7c}, 32)}
	stageValidator, err := stageauthority.NewValidator(stageKeys, func() time.Time { return now })
	if err != nil {
		t.Fatalf("New StageAuthority Validator: %v", err)
	}
	keys := map[string][]byte{"materialization-key-v1": bytes.Repeat([]byte{0x7d}, 32)}
	signer, err := materializationauthority.NewSigner(keys)
	if err != nil {
		t.Fatalf("New materialization Signer: %v", err)
	}
	validator, err := materializationauthority.NewValidator(keys, func() time.Time { return now })
	if err != nil {
		t.Fatalf("New materialization Validator: %v", err)
	}
	signed, err := signer.Sign(controlMaterializationAuthority(now))
	if err != nil {
		t.Fatalf("Sign MaterializationAuthority: %v", err)
	}
	digest, err := materializationauthority.Digest(signed)
	if err != nil {
		t.Fatalf("Digest MaterializationAuthority: %v", err)
	}
	stageAuthorizer := &exactActiveAuthorizer{}
	materializationAuthorizer := &exactMaterializationAuthorizer{digest: digest}
	executor := &recordingControlExecutor{}
	handler, err := stageworkercontrol.NewHandler(stageworkercontrol.Config{
		Validator: stageValidator, Authorizer: stageAuthorizer,
		MaterializationValidator:  validator,
		MaterializationAuthorizer: materializationAuthorizer,
		Executor:                  executor,
	})
	if err != nil {
		t.Fatalf("NewHandler: %v", err)
	}
	request := &velav1.StageWorkerControlServiceConnectRequest{
		RequestId: "81000000-0000-0000-0000-000000000010",
		Operation: &velav1.StageWorkerControlServiceConnectRequest_CommitStageMaterialization{
			CommitStageMaterialization: &velav1.CommitStageMaterializationRequest{
				MaterializationAuthority: signed,
				ObjectVersion:            "exact-version-v1",
			},
		},
	}
	response, err := handler.Handle(
		context.Background(),
		stageworkertransport.Identity{SPIFFEID: "spiffe://vela/worker/member-1"},
		11,
		request,
	)
	if err != nil || response.GetStageCommandResult().GetDecision() !=
		velav1.StageWorkerCommandDecision_STAGE_WORKER_COMMAND_DECISION_ACCEPTED {
		t.Fatalf("commit Handle = %#v error=%v", response, err)
	}
	if stageAuthorizer.calls != 0 || materializationAuthorizer.calls != 1 ||
		executor.lastAuthorities.Materialization == nil ||
		executor.lastAuthorities.Stage != nil {
		t.Fatalf(
			"authority calls stage=%d materialization=%d executor=%#v",
			stageAuthorizer.calls, materializationAuthorizer.calls, executor.lastAuthorities,
		)
	}

	tampered := proto.Clone(signed).(*velav1.MaterializationAuthority)
	tampered.ObjectKey += ".forged"
	request.GetCommitStageMaterialization().MaterializationAuthority = tampered
	request.RequestId = "81000000-0000-0000-0000-000000000011"
	response, err = handler.Handle(
		context.Background(),
		stageworkertransport.Identity{SPIFFEID: "spiffe://vela/worker/member-1"},
		11,
		request,
	)
	if err != nil || response.GetStageCommandResult().GetDecision() !=
		velav1.StageWorkerCommandDecision_STAGE_WORKER_COMMAND_DECISION_STALE ||
		materializationAuthorizer.calls != 1 || executor.calls != 1 {
		t.Fatalf("tampered commit Handle = %#v error=%v", response, err)
	}
}

func TestHandlerAuthorizesSourceLossWithMaterializationAuthorityNotStageAuthority(t *testing.T) {
	now := time.Date(2026, 8, 30, 14, 30, 0, 0, time.UTC)
	stageValidator, err := stageauthority.NewValidator(
		map[string][]byte{"control-key": bytes.Repeat([]byte{0x7c}, 32)},
		func() time.Time { return now },
	)
	if err != nil {
		t.Fatalf("New StageAuthority Validator: %v", err)
	}
	keys := map[string][]byte{"materialization-key-v1": bytes.Repeat([]byte{0x7d}, 32)}
	signer, err := materializationauthority.NewSigner(keys)
	if err != nil {
		t.Fatalf("New materialization Signer: %v", err)
	}
	validator, err := materializationauthority.NewValidator(keys, func() time.Time { return now })
	if err != nil {
		t.Fatalf("New materialization Validator: %v", err)
	}
	signed, err := signer.Sign(controlMaterializationAuthority(now))
	if err != nil {
		t.Fatalf("Sign MaterializationAuthority: %v", err)
	}
	digest, err := materializationauthority.Digest(signed)
	if err != nil {
		t.Fatalf("Digest MaterializationAuthority: %v", err)
	}
	stageAuthorizer := &exactActiveAuthorizer{}
	materializationAuthorizer := &exactMaterializationAuthorizer{digest: digest}
	executor := &recordingControlExecutor{}
	handler, err := stageworkercontrol.NewHandler(stageworkercontrol.Config{
		Validator: stageValidator, Authorizer: stageAuthorizer,
		MaterializationValidator: validator, MaterializationAuthorizer: materializationAuthorizer,
		Executor: executor,
	})
	if err != nil {
		t.Fatalf("NewHandler: %v", err)
	}
	response, err := handler.Handle(
		context.Background(),
		stageworkertransport.Identity{SPIFFEID: "spiffe://vela/worker/member-1"},
		12,
		&velav1.StageWorkerControlServiceConnectRequest{
			RequestId: "81000000-0000-0000-0000-000000000020",
			Operation: &velav1.StageWorkerControlServiceConnectRequest_ReportMaterializationSourceLost{
				ReportMaterializationSourceLost: &velav1.ReportMaterializationSourceLostRequest{
					MaterializationAuthority: signed,
					FailureFingerprint:       bytes.Repeat([]byte{0xa1}, 32),
					ConsumedResourceUnits:    100,
					LostAt:                   timestamppb.New(now.Add(time.Second)),
					RetryAt:                  timestamppb.New(now.Add(2 * time.Second)),
				},
			},
		},
	)
	if err != nil || response.GetStageCommandResult().GetDecision() !=
		velav1.StageWorkerCommandDecision_STAGE_WORKER_COMMAND_DECISION_ACCEPTED ||
		stageAuthorizer.calls != 0 || materializationAuthorizer.calls != 1 ||
		executor.lastAuthorities.Materialization == nil || executor.lastAuthorities.Stage != nil {
		t.Fatalf(
			"source-loss Handle = %#v error=%v stage_calls=%d materialization_calls=%d authorities=%#v",
			response, err, stageAuthorizer.calls, materializationAuthorizer.calls,
			executor.lastAuthorities,
		)
	}
}

type exactActiveAuthorizer struct {
	digest [32]byte
	calls  int
}

func (authorizer *exactActiveAuthorizer) IsActive(
	_ context.Context,
	_ stageworkertransport.Identity,
	_ int64,
	_ stageworkercontrol.Operation,
	authority stageauthority.Verified,
) (bool, error) {
	authorizer.calls++
	return authority.Digest == authorizer.digest, nil
}

type exactMaterializationAuthorizer struct {
	digest [32]byte
	calls  int
}

func (authorizer *exactMaterializationAuthorizer) IsActive(
	_ context.Context,
	_ stageworkertransport.Identity,
	_ int64,
	authority materializationauthority.Verified,
) (bool, error) {
	authorizer.calls++
	return authorizer.digest == ([32]byte{}) || authority.Digest == authorizer.digest, nil
}

type recordingControlExecutor struct {
	calls           int
	lastAuthorities stageworkercontrol.VerifiedAuthorities
}

func (executor *recordingControlExecutor) Execute(
	_ context.Context,
	_ stageworkertransport.Identity,
	_ int64,
	operation stageworkercontrol.Operation,
	request *velav1.StageWorkerControlServiceConnectRequest,
	authorities stageworkercontrol.VerifiedAuthorities,
) (*velav1.StageWorkerControlServiceConnectResponse, error) {
	executor.calls++
	executor.lastAuthorities = authorities
	return &velav1.StageWorkerControlServiceConnectResponse{
		RequestId: request.GetRequestId(),
		Result: &velav1.StageWorkerControlServiceConnectResponse_StageCommandResult{
			StageCommandResult: &velav1.StageCommandResult{
				Decision:  velav1.StageWorkerCommandDecision_STAGE_WORKER_COMMAND_DECISION_ACCEPTED,
				Operation: operation,
			},
		},
	}, nil
}

func controlMaterializationAuthority(now time.Time) *velav1.MaterializationAuthority {
	return &velav1.MaterializationAuthority{
		SchemaVersion: 1, StageAuthorityDigest: bytes.Repeat([]byte{0x91}, 32),
		StageMaterializationLeaseId: "49600000-0000-0000-0000-000000000001",
		StageArtifactId:             "49600000-0000-0000-0000-000000000002",
		ObjectKey:                   "artifacts/stage/org/project/attempt/encoder/output.bin",
		ContentType:                 "application/octet-stream", Sha256: bytes.Repeat([]byte{0x92}, 32),
		SizeBytes: 4096, LocalReceiptId: "encoder-receipt-v1",
		LocalReceiptDigest: bytes.Repeat([]byte{0x93}, 32),
		SigningKeyId:       "materialization-key-v1", IssuedAt: timestamppb.New(now),
		ExpiresAt:                 timestamppb.New(now.Add(30 * time.Minute)),
		SourceWorkerInstanceId:    "23000000-0000-0000-0000-000000000001",
		SourceWorkerInstanceEpoch: 5,
		SourceWorkerMemberId:      "43000000-0000-0000-0000-000000000001",
		SourceWorkerMemberEpoch:   8,
		SourceSpiffeIdDigest:      sha256Bytes("spiffe://vela/worker/member-1"),
	}
}

func sha256Bytes(value string) []byte {
	digest := sha256.Sum256([]byte(value))
	return digest[:]
}

func controlAuthority(now time.Time) *velav1.StageAuthority {
	return &velav1.StageAuthority{
		SchemaVersion: 1,
		JobId:         "13000000-0000-0000-0000-000000000001", AttemptId: "13000000-0000-0000-0000-000000000002",
		StageRunId: "13000000-0000-0000-0000-000000000003", StageAttemptId: "13000000-0000-0000-0000-000000000004",
		StageAllocationId: "13000000-0000-0000-0000-000000000005", StageLeaseId: "13000000-0000-0000-0000-000000000006",
		AttemptFence: 2, StageFence: 3, StageVersion: 4,
		WorkerInstanceId: "23000000-0000-0000-0000-000000000001", WorkerInstanceEpoch: 5,
		DeviceSetDigest: bytes.Repeat([]byte{0x81}, 32),
		Devices: []*velav1.StageAuthorityDeviceEpoch{{
			DeviceId: "33000000-0000-0000-0000-000000000001", DeviceEpoch: 7,
		}},
		MembershipDigest: bytes.Repeat([]byte{0x82}, 32),
		Members: []*velav1.StageAuthorityMemberEpoch{{
			WorkerMemberId: "43000000-0000-0000-0000-000000000001", MemberEpoch: 8, ModelRuntimeEpoch: 9,
			IdentityDigest: bytes.Repeat([]byte{0x86}, 32),
		}},
		ModelResidencyId: "53000000-0000-0000-0000-000000000001", ModelRuntimeIdentity: "dit-runtime-3",
		ModelRuntimeBarrierGeneration: 9,
		StageProfileRevisionId:        "63000000-0000-0000-0000-000000000001",
		CapacityObservationSequence:   10,
		CapacityVector:                map[string]int64{"active_stage_slots": 1, "gpu_count": 1},
		LeaseToken:                    bytes.Repeat([]byte{0x83}, 32), ExecutionNonce: bytes.Repeat([]byte{0x84}, 32),
		ExecutionSpecDigest: bytes.Repeat([]byte{0x85}, 32),
		SigningKeyId:        "control-key", IssuedAt: timestamppb.New(now),
		ExpiresAt: timestamppb.New(now.Add(time.Minute)), MonotonicValidFor: durationpb.New(time.Minute),
	}
}
