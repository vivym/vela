package main

import (
	"bytes"
	"context"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/vivym/vela/internal/authoritypolicy"
	"github.com/vivym/vela/internal/materializationauthority"
	"github.com/vivym/vela/internal/stageauthority"
	"github.com/vivym/vela/internal/stageworkercontrol"
	"github.com/vivym/vela/internal/stageworkertransport"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestStageWorkerAuthorityIngressBoundsStageAndMaterializationClockSkew(t *testing.T) {
	now := time.Date(2026, 9, 4, 9, 4, 9, 545117000, time.UTC)
	keyring := map[string][]byte{"control-key": bytes.Repeat([]byte{0x42}, 32)}
	signer, err := stageauthority.NewSigner(keyring)
	if err != nil {
		t.Fatalf("construct StageAuthority signer: %v", err)
	}
	stageValidator, err := stageauthority.NewValidator(keyring, func() time.Time { return now })
	if err != nil {
		t.Fatalf("construct StageAuthority validator: %v", err)
	}
	materializationValidator, err := materializationauthority.NewValidator(
		keyring,
		func() time.Time { return now },
	)
	if err != nil {
		t.Fatalf("construct MaterializationAuthority validator: %v", err)
	}
	materializationSigner, err := materializationauthority.NewSigner(keyring)
	if err != nil {
		t.Fatalf("construct MaterializationAuthority signer: %v", err)
	}
	assigned, err := signer.Sign(stageWorkerTestAuthority(now.Add(-time.Second)))
	if err != nil {
		t.Fatalf("sign assigned StageAuthority: %v", err)
	}
	renewalEnvelope := proto.Clone(assigned).(*velav1.StageAuthority)
	renewalEnvelope.StageVersion++
	renewalEnvelope.IssuedAt = timestamppb.New(now.Add(3200 * time.Microsecond))
	renewalEnvelope.ExpiresAt = timestamppb.New(now.Add(2 * time.Minute))
	renewalEnvelope.MonotonicValidFor = durationpb.New(90 * time.Second)
	renewalEnvelope.Signature = nil
	renewed, err := signer.Sign(renewalEnvelope)
	if err != nil {
		t.Fatalf("sign renewed StageAuthority: %v", err)
	}

	backend := &committedRenewalExecutor{renewed: renewed}
	authorizer := activeAuthorityAuthorizer{}
	handler, stopSource, err := newStageWorkerAuthorityIngress(
		stageValidator,
		authorizer,
		materializationValidator,
		activeMaterializationAuthorizer{},
		backend,
		func() time.Time { return now },
	)
	if err != nil {
		t.Fatalf("construct Stage Worker authority ingress: %v", err)
	}
	server, err := stageworkertransport.NewServer(stageworkertransport.ServerConfig{
		Authenticator: stageworkertransport.AuthenticatorFunc(func(context.Context) (stageworkertransport.Identity, error) {
			return stageworkertransport.Identity{SPIFFEID: "spiffe://vela/worker/member-1"}, nil
		}),
		Handler:    handler,
		StopSource: stopSource,
	})
	if err != nil {
		t.Fatalf("construct Stage Worker control server: %v", err)
	}
	address := serveStageWorkerAuthorityIngress(t, server)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	client, err := stageworkertransport.DialClient(ctx, stageworkertransport.ClientConfig{
		Address: address, TransportCredentials: insecure.NewCredentials(),
		InitialControlSessionEpoch: 9,
	})
	if err != nil {
		t.Fatalf("dial Stage Worker control: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })

	start, err := client.Exchange(ctx, &velav1.StageWorkerControlServiceConnectRequest{
		Operation: &velav1.StageWorkerControlServiceConnectRequest_StartStage{
			StartStage: &velav1.StartStageRequest{
				Authority: assigned,
				StartedAt: timestamppb.New(now),
			},
		},
	})
	if err != nil || start.GetStageCommandResult().GetDecision() !=
		velav1.StageWorkerCommandDecision_STAGE_WORKER_COMMAND_DECISION_ACCEPTED ||
		!proto.Equal(start.GetStageCommandResult().GetRenewedAuthority(), renewed) {
		t.Fatalf("Start response = %#v error=%v", start, err)
	}
	if !backend.Committed() {
		t.Fatal("Start response arrived before the renewal was committed")
	}

	heartbeat, err := client.Exchange(ctx, &velav1.StageWorkerControlServiceConnectRequest{
		Operation: &velav1.StageWorkerControlServiceConnectRequest_HeartbeatStage{
			HeartbeatStage: &velav1.HeartbeatStageRequest{
				Authority:  renewed,
				ObservedAt: timestamppb.New(now.Add(3200 * time.Microsecond)),
			},
		},
	})
	if err != nil || heartbeat.GetStageCommandResult().GetDecision() !=
		velav1.StageWorkerCommandDecision_STAGE_WORKER_COMMAND_DECISION_ACCEPTED {
		t.Fatalf("immediate Heartbeat response = %#v error=%v", heartbeat, err)
	}

	reattach, err := client.Exchange(ctx, &velav1.StageWorkerControlServiceConnectRequest{
		Operation: &velav1.StageWorkerControlServiceConnectRequest_ReattachStage{
			ReattachStage: &velav1.ReattachStageRequest{Authority: renewed},
		},
	})
	if err != nil || reattach.GetStageCommandResult().GetDecision() !=
		velav1.StageWorkerCommandDecision_STAGE_WORKER_COMMAND_DECISION_REPLAYED {
		t.Fatalf("Reattach response = %#v error=%v", reattach, err)
	}
	materialization, err := materializationSigner.Sign(
		stageWorkerTestMaterializationAuthority(now.Add(3200 * time.Microsecond)),
	)
	if err != nil {
		t.Fatalf("sign MaterializationAuthority: %v", err)
	}
	commit, err := client.Exchange(ctx, &velav1.StageWorkerControlServiceConnectRequest{
		Operation: &velav1.StageWorkerControlServiceConnectRequest_CommitStageMaterialization{
			CommitStageMaterialization: &velav1.CommitStageMaterializationRequest{
				MaterializationAuthority: materialization,
				ObjectVersion:            "object-version-v1",
			},
		},
	})
	if err != nil || commit.GetStageCommandResult().GetDecision() !=
		velav1.StageWorkerCommandDecision_STAGE_WORKER_COMMAND_DECISION_ACCEPTED {
		t.Fatalf("immediate CommitStageMaterialization response = %#v error=%v", commit, err)
	}
	if got := backend.Operations(); got !=
		"START_STAGE,HEARTBEAT_STAGE,REATTACH_STAGE,COMMIT_STAGE_MATERIALIZATION" {
		t.Fatalf("executed operations = %q", got)
	}

	overSkewEnvelope := proto.Clone(renewed).(*velav1.StageAuthority)
	overSkewEnvelope.StageVersion++
	overSkewEnvelope.IssuedAt = timestamppb.New(
		now.Add(authoritypolicy.ProductionMaxClockSkew + time.Millisecond),
	)
	overSkewEnvelope.ExpiresAt = timestamppb.New(overSkewEnvelope.GetIssuedAt().AsTime().Add(2 * time.Minute))
	overSkewEnvelope.Signature = nil
	overSkew, err := signer.Sign(overSkewEnvelope)
	if err != nil {
		t.Fatalf("sign over-skew StageAuthority: %v", err)
	}
	rejected, err := client.Exchange(ctx, &velav1.StageWorkerControlServiceConnectRequest{
		Operation: &velav1.StageWorkerControlServiceConnectRequest_HeartbeatStage{
			HeartbeatStage: &velav1.HeartbeatStageRequest{
				Authority: overSkew, ObservedAt: timestamppb.New(now),
			},
		},
	})
	if err != nil || rejected.GetStageCommandResult().GetDecision() !=
		velav1.StageWorkerCommandDecision_STAGE_WORKER_COMMAND_DECISION_STALE {
		t.Fatalf("over-skew Heartbeat response = %#v error=%v", rejected, err)
	}
	if got := backend.Operations(); got !=
		"START_STAGE,HEARTBEAT_STAGE,REATTACH_STAGE,COMMIT_STAGE_MATERIALIZATION" {
		t.Fatalf("over-skew operation reached backend: %q", got)
	}
	overSkewMaterialization, err := materializationSigner.Sign(
		stageWorkerTestMaterializationAuthority(
			now.Add(authoritypolicy.ProductionMaxClockSkew + time.Millisecond),
		),
	)
	if err != nil {
		t.Fatalf("sign over-skew MaterializationAuthority: %v", err)
	}
	rejected, err = client.Exchange(ctx, &velav1.StageWorkerControlServiceConnectRequest{
		Operation: &velav1.StageWorkerControlServiceConnectRequest_CommitStageMaterialization{
			CommitStageMaterialization: &velav1.CommitStageMaterializationRequest{
				MaterializationAuthority: overSkewMaterialization,
				ObjectVersion:            "object-version-v2",
			},
		},
	})
	if err != nil || rejected.GetStageCommandResult().GetDecision() !=
		velav1.StageWorkerCommandDecision_STAGE_WORKER_COMMAND_DECISION_STALE {
		t.Fatalf("over-skew CommitStageMaterialization response = %#v error=%v", rejected, err)
	}
	if got := backend.Operations(); got !=
		"START_STAGE,HEARTBEAT_STAGE,REATTACH_STAGE,COMMIT_STAGE_MATERIALIZATION" {
		t.Fatalf("over-skew materialization reached backend: %q", got)
	}
}

type activeAuthorityAuthorizer struct{}

func (activeAuthorityAuthorizer) IsActive(
	context.Context,
	stageworkertransport.Identity,
	int64,
	stageworkercontrol.Operation,
	stageauthority.Verified,
) (bool, error) {
	return true, nil
}

type activeMaterializationAuthorizer struct{}

func (activeMaterializationAuthorizer) IsActive(
	context.Context,
	stageworkertransport.Identity,
	int64,
	materializationauthority.Verified,
) (bool, error) {
	return true, nil
}

type committedRenewalExecutor struct {
	mu         sync.Mutex
	renewed    *velav1.StageAuthority
	committed  bool
	operations []stageworkercontrol.Operation
}

func (executor *committedRenewalExecutor) Execute(
	_ context.Context,
	_ stageworkertransport.Identity,
	_ int64,
	operation stageworkercontrol.Operation,
	request *velav1.StageWorkerControlServiceConnectRequest,
	_ stageworkercontrol.VerifiedAuthorities,
) (*velav1.StageWorkerControlServiceConnectResponse, error) {
	executor.mu.Lock()
	defer executor.mu.Unlock()
	executor.operations = append(executor.operations, operation)
	result := &velav1.StageCommandResult{Operation: operation}
	switch operation {
	case stageworkercontrol.OperationStartStage:
		executor.committed = true
		result.Decision = velav1.StageWorkerCommandDecision_STAGE_WORKER_COMMAND_DECISION_ACCEPTED
		result.RenewedAuthority = proto.Clone(executor.renewed).(*velav1.StageAuthority)
	case stageworkercontrol.OperationReattachStage:
		result.Decision = velav1.StageWorkerCommandDecision_STAGE_WORKER_COMMAND_DECISION_REPLAYED
	default:
		result.Decision = velav1.StageWorkerCommandDecision_STAGE_WORKER_COMMAND_DECISION_ACCEPTED
	}
	return &velav1.StageWorkerControlServiceConnectResponse{
		RequestId: request.GetRequestId(),
		Result: &velav1.StageWorkerControlServiceConnectResponse_StageCommandResult{
			StageCommandResult: result,
		},
	}, nil
}

func (executor *committedRenewalExecutor) Committed() bool {
	executor.mu.Lock()
	defer executor.mu.Unlock()
	return executor.committed
}

func (executor *committedRenewalExecutor) Operations() string {
	executor.mu.Lock()
	defer executor.mu.Unlock()
	var operations string
	for index, operation := range executor.operations {
		if index != 0 {
			operations += ","
		}
		operations += operation.String()[len("STAGE_WORKER_OPERATION_"):]
	}
	return operations
}

func serveStageWorkerAuthorityIngress(
	t *testing.T,
	server velav1.StageWorkerControlServiceServer,
) string {
	t.Helper()
	grpcServer := grpc.NewServer()
	velav1.RegisterStageWorkerControlServiceServer(grpcServer, server)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen for Stage Worker authority ingress: %v", err)
	}
	done := make(chan error, 1)
	go func() { done <- grpcServer.Serve(listener) }()
	t.Cleanup(func() {
		grpcServer.Stop()
		_ = listener.Close()
		if serveErr := <-done; serveErr != nil && serveErr != grpc.ErrServerStopped {
			t.Errorf("serve Stage Worker authority ingress: %v", serveErr)
		}
	})
	return listener.Addr().String()
}

func stageWorkerTestAuthority(issuedAt time.Time) *velav1.StageAuthority {
	return &velav1.StageAuthority{
		SchemaVersion:     1,
		JobId:             "13000000-0000-0000-0000-000000000001",
		AttemptId:         "13000000-0000-0000-0000-000000000002",
		StageRunId:        "13000000-0000-0000-0000-000000000003",
		StageAttemptId:    "13000000-0000-0000-0000-000000000004",
		StageAllocationId: "13000000-0000-0000-0000-000000000005",
		StageLeaseId:      "13000000-0000-0000-0000-000000000006",
		AttemptFence:      2, StageFence: 3, StageVersion: 4,
		WorkerInstanceId:    "23000000-0000-0000-0000-000000000001",
		WorkerInstanceEpoch: 5,
		DeviceSetDigest:     bytes.Repeat([]byte{0x81}, 32),
		Devices: []*velav1.StageAuthorityDeviceEpoch{{
			DeviceId: "33000000-0000-0000-0000-000000000001", DeviceEpoch: 7,
		}},
		MembershipDigest: bytes.Repeat([]byte{0x82}, 32),
		Members: []*velav1.StageAuthorityMemberEpoch{{
			WorkerMemberId: "43000000-0000-0000-0000-000000000001",
			MemberEpoch:    8, ModelRuntimeEpoch: 9,
			IdentityDigest: bytes.Repeat([]byte{0x86}, 32),
		}},
		ModelResidencyId:              "53000000-0000-0000-0000-000000000001",
		ModelRuntimeIdentity:          "dit-runtime-3",
		ModelRuntimeBarrierGeneration: 9,
		StageProfileRevisionId:        "63000000-0000-0000-0000-000000000001",
		CapacityObservationSequence:   10,
		CapacityVector:                map[string]int64{"active_stage_slots": 1, "gpu_count": 1},
		LeaseToken:                    bytes.Repeat([]byte{0x83}, 32),
		ExecutionNonce:                bytes.Repeat([]byte{0x84}, 32),
		ExecutionSpecDigest:           bytes.Repeat([]byte{0x85}, 32),
		SigningKeyId:                  "control-key",
		IssuedAt:                      timestamppb.New(issuedAt),
		ExpiresAt:                     timestamppb.New(issuedAt.Add(time.Minute)),
		MonotonicValidFor:             durationpb.New(time.Minute),
	}
}

func stageWorkerTestMaterializationAuthority(issuedAt time.Time) *velav1.MaterializationAuthority {
	return &velav1.MaterializationAuthority{
		SchemaVersion:               1,
		StageAuthorityDigest:        bytes.Repeat([]byte{0xb1}, 32),
		StageMaterializationLeaseId: "49600000-0000-0000-0000-000000000001",
		StageArtifactId:             "49600000-0000-0000-0000-000000000002",
		ObjectKey:                   "artifacts/stage/attempt/run/output.bin",
		ContentType:                 "application/octet-stream",
		Sha256:                      bytes.Repeat([]byte{0xb2}, 32),
		SizeBytes:                   4096,
		LocalReceiptId:              "encoder-receipt-v1",
		LocalReceiptDigest:          bytes.Repeat([]byte{0xb3}, 32),
		SigningKeyId:                "control-key",
		IssuedAt:                    timestamppb.New(issuedAt),
		ExpiresAt:                   timestamppb.New(issuedAt.Add(15 * time.Minute)),
		SourceWorkerInstanceId:      "23000000-0000-0000-0000-000000000001",
		SourceWorkerInstanceEpoch:   5,
		SourceWorkerMemberId:        "43000000-0000-0000-0000-000000000001",
		SourceWorkerMemberEpoch:     8,
		SourceSpiffeIdDigest:        bytes.Repeat([]byte{0xb4}, 32),
	}
}
