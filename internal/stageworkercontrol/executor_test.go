package stageworkercontrol_test

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/materializationauthority"
	"github.com/vivym/vela/internal/stageartifact"
	"github.com/vivym/vela/internal/stageauthority"
	"github.com/vivym/vela/internal/stageworkercontrol"
	"github.com/vivym/vela/internal/stageworkertransport"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestProductionExecutorDispatchesEveryStageWorkerOperation(t *testing.T) {
	now := time.Now().UTC()
	stageAuthority, stageDigest := verifiedExecutorStageAuthority(t, now)
	materializationAuthority := validExecutorMaterializationAuthority(now, stageDigest)
	materializationDigest, err := materializationauthority.Digest(materializationAuthority)
	if err != nil {
		t.Fatalf("materialization authority digest: %v", err)
	}
	stageAuthorities := stageworkercontrol.VerifiedAuthorities{
		Stage: &stageauthority.Verified{Authority: stageAuthority, Digest: stageDigest},
	}
	materializationAuthorities := stageworkercontrol.VerifiedAuthorities{
		Materialization: &materializationauthority.Verified{
			Authority: materializationAuthority, Digest: materializationDigest,
		},
	}
	workerID := uuid.MustParse("a1000000-0000-0000-0000-000000000001")
	assignment := validExecutorAssignment(t, now)
	backend := &recordingOperationBackend{
		readiness: stageworkercontrol.ReadinessResult{
			WorkerInstanceID: workerID, WorkerInstanceEpoch: 7, Ready: true, Reason: "READY",
		},
		acquire: stageworkercontrol.AcquireResult{Assignment: assignment},
		command: stageworkercontrol.CommandResult{
			Decision: velav1.StageWorkerCommandDecision_STAGE_WORKER_COMMAND_DECISION_ACCEPTED,
		},
		seal: stageworkercontrol.SealResult{
			Authority: materializationAuthority,
		},
		resolve: stageworkercontrol.ResolveInputTransferResult{
			Descriptor: &stageartifact.TransferDescriptor{
				TicketID:   uuid.MustParse("a3000000-0000-0000-0000-000000000001"),
				ArtifactID: uuid.MustParse("a3000000-0000-0000-0000-000000000002"),
				ObjectKey:  "stages/input.bin", ObjectVersion: "version-1",
				SHA256: sha256.Sum256([]byte("input")), SizeBytes: 5,
				ContentType: "application/octet-stream",
			},
		},
	}
	executor, err := stageworkercontrol.NewProductionExecutor(backend)
	if err != nil {
		t.Fatalf("NewProductionExecutor: %v", err)
	}
	requestID := "a2000000-0000-0000-0000-000000000001"
	identity := stageworkertransport.Identity{SPIFFEID: "spiffe://vela.test/stage-worker/dit-0"}
	tests := []struct {
		name        string
		operation   stageworkercontrol.Operation
		request     *velav1.StageWorkerControlServiceConnectRequest
		authorities stageworkercontrol.VerifiedAuthorities
		wantResult  string
	}{
		{
			name: "register", operation: stageworkercontrol.OperationRegisterWorkerEvidence,
			request: controlRequest(requestID, &velav1.StageWorkerControlServiceConnectRequest_RegisterWorkerEvidence{
				RegisterWorkerEvidence: &velav1.RegisterWorkerEvidenceRequest{
					RuntimeIdentity: &velav1.ModelRuntimeIdentity{}, CapacityObservationSequence: 1,
					Devices: []*velav1.StageAuthorityDeviceEpoch{{DeviceId: uuid.NewString(), DeviceEpoch: 1}},
					Members: []*velav1.StageAuthorityMemberEpoch{{
						WorkerMemberId: uuid.NewString(), MemberEpoch: 1, ModelRuntimeEpoch: 1,
						IdentityDigest: bytes.Repeat([]byte{0x86}, 32),
					}},
					ReadinessEvidence: []byte("ready"),
				},
			}),
			wantResult: "readiness",
		},
		{
			name: "capacity", operation: stageworkercontrol.OperationReportCapacityObservation,
			request: controlRequest(requestID, &velav1.StageWorkerControlServiceConnectRequest_ReportCapacityObservation{
				ReportCapacityObservation: &velav1.ReportStageCapacityObservationRequest{
					WorkerInstanceId: workerID.String(), WorkerInstanceEpoch: 7,
					ObservationSequence: 2, CapacityVector: map[string]int64{"active_stage_slots": 1},
					ObservedAt: timestamppb.New(now), ExpiresAt: timestamppb.New(now.Add(time.Minute)),
				},
			}),
			wantResult: "readiness",
		},
		{
			name: "acquire", operation: stageworkercontrol.OperationAcquireStage,
			request: controlRequest(requestID, &velav1.StageWorkerControlServiceConnectRequest_AcquireStage{
				AcquireStage: &velav1.AcquireStageRequest{
					WorkerInstanceId: workerID.String(), WorkerInstanceEpoch: 7,
					CapacityObservationSequence: 2, ModelResidencyId: uuid.NewString(),
					ModelRuntimeEpoch: 3, StageProfileRevisionId: uuid.NewString(),
				},
			}),
			wantResult: "assignment",
		},
		{
			name: "start", operation: stageworkercontrol.OperationStartStage,
			request: controlRequest(requestID, &velav1.StageWorkerControlServiceConnectRequest_StartStage{
				StartStage: &velav1.StartStageRequest{
					Authority: stageAuthority, StartedAt: timestamppb.New(now),
				},
			}),
			authorities: stageAuthorities, wantResult: "command-stage",
		},
		{
			name: "heartbeat", operation: stageworkercontrol.OperationHeartbeatStage,
			request: controlRequest(requestID, &velav1.StageWorkerControlServiceConnectRequest_HeartbeatStage{
				HeartbeatStage: &velav1.HeartbeatStageRequest{
					Authority: stageAuthority, Sequence: 1,
					RuntimeState:      velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_RUNNING,
					BoundedStatusJson: []byte(`{"state":"RUNNING"}`), ObservedAt: timestamppb.New(now),
				},
			}),
			authorities: stageAuthorities, wantResult: "command-stage",
		},
		{
			name: "seal", operation: stageworkercontrol.OperationSealStageOutput,
			request: controlRequest(requestID, &velav1.StageWorkerControlServiceConnectRequest_SealStageOutput{
				SealStageOutput: &velav1.SealStageOutputRequest{
					Authority: stageAuthority, LocalReceipt: &velav1.LocalMaterializationReceipt{},
				},
			}),
			authorities: stageAuthorities, wantResult: "materialization",
		},
		{
			name: "commit", operation: stageworkercontrol.OperationCommitStageMaterialization,
			request: controlRequest(requestID, &velav1.StageWorkerControlServiceConnectRequest_CommitStageMaterialization{
				CommitStageMaterialization: &velav1.CommitStageMaterializationRequest{
					MaterializationAuthority: materializationAuthority,
					ObjectVersion:            "version-1", CommittedAt: timestamppb.New(now),
				},
			}),
			authorities: materializationAuthorities, wantResult: "command-materialization",
		},
		{
			name: "fail", operation: stageworkercontrol.OperationFailStage,
			request: controlRequest(requestID, &velav1.StageWorkerControlServiceConnectRequest_FailStage{
				FailStage: &velav1.FailStageRequest{
					Authority: stageAuthority, FailureClass: "RUNTIME",
					FailureFingerprint: bytes.Repeat([]byte{0x42}, sha256.Size), WorkerReusable: true,
					ConsumedResourceUnits: 10, FailedAt: timestamppb.New(now),
					RetryAt: timestamppb.New(now.Add(time.Second)),
				},
			}),
			authorities: stageAuthorities, wantResult: "command-stage",
		},
		{
			name: "reattach", operation: stageworkercontrol.OperationReattachStage,
			request: controlRequest(requestID, &velav1.StageWorkerControlServiceConnectRequest_ReattachStage{
				ReattachStage: &velav1.ReattachStageRequest{
					Authority:            stageAuthority,
					ObservedRuntimeState: velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_RUNNING,
				},
			}),
			authorities: stageAuthorities, wantResult: "command-stage",
		},
		{
			name: "source lost", operation: stageworkercontrol.OperationReportMaterializationSourceLost,
			request: controlRequest(requestID, &velav1.StageWorkerControlServiceConnectRequest_ReportMaterializationSourceLost{
				ReportMaterializationSourceLost: &velav1.ReportMaterializationSourceLostRequest{
					MaterializationAuthority: materializationAuthority,
					FailureFingerprint:       bytes.Repeat([]byte{0x43}, sha256.Size),
					ConsumedResourceUnits:    10, LostAt: timestamppb.New(now),
					RetryAt: timestamppb.New(now.Add(time.Second)),
				},
			}),
			authorities: materializationAuthorities, wantResult: "command-materialization",
		},
		{
			name: "resolve input", operation: stageworkercontrol.OperationResolveInputTransfer,
			request: controlRequest(requestID, &velav1.StageWorkerControlServiceConnectRequest_ResolveInputTransfer{
				ResolveInputTransfer: &velav1.ResolveInputTransferRequest{
					Authority:           stageAuthority,
					TicketId:            "a3000000-0000-0000-0000-000000000001",
					TokenDigest:         bytes.Repeat([]byte{0x52}, sha256.Size),
					WorkerInstanceId:    stageAuthority.GetWorkerInstanceId(),
					WorkerInstanceEpoch: stageAuthority.GetWorkerInstanceEpoch(),
					ModelResidencyId:    stageAuthority.GetModelResidencyId(),
					ModelRuntimeEpoch:   stageAuthority.GetModelRuntimeBarrierGeneration(),
					ConnectorRevisionId: "a3000000-0000-0000-0000-000000000003",
					ResolvedAt:          timestamppb.New(now),
				},
			}),
			authorities: stageAuthorities, wantResult: "transfer",
		},
		{
			name: "consume input", operation: stageworkercontrol.OperationConsumeInputTransfer,
			request: controlRequest(requestID, &velav1.StageWorkerControlServiceConnectRequest_ConsumeInputTransfer{
				ConsumeInputTransfer: &velav1.ConsumeInputTransferRequest{
					Authority:           stageAuthority,
					TicketId:            "a3000000-0000-0000-0000-000000000001",
					TokenDigest:         bytes.Repeat([]byte{0x52}, sha256.Size),
					OutcomeDigest:       bytes.Repeat([]byte{0x53}, sha256.Size),
					ConsumedAt:          timestamppb.New(now),
					WorkerInstanceId:    stageAuthority.GetWorkerInstanceId(),
					WorkerInstanceEpoch: stageAuthority.GetWorkerInstanceEpoch(),
					ModelResidencyId:    stageAuthority.GetModelResidencyId(),
					ModelRuntimeEpoch:   stageAuthority.GetModelRuntimeBarrierGeneration(),
					ConnectorRevisionId: "a3000000-0000-0000-0000-000000000003",
				},
			}),
			authorities: stageAuthorities, wantResult: "command-stage",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			backend.calls = 0
			response, err := executor.Execute(
				context.Background(), identity, 9, test.operation, test.request, test.authorities,
			)
			if err != nil || backend.calls != 1 || response.GetRequestId() != requestID {
				t.Fatalf("Execute = %#v error=%v calls=%d", response, err, backend.calls)
			}
			if backend.last.CommandID.String() != requestID || backend.last.Identity != identity ||
				backend.last.ControlSessionEpoch != 9 || backend.operation != test.operation {
				t.Fatalf("command context = %#v operation=%s", backend.last, backend.operation)
			}
			assertExecutorResult(t, response, test.wantResult, stageDigest, materializationDigest)
		})
	}
}

func TestProductionExecutorRejectsMalformedEvidenceBeforeBackend(t *testing.T) {
	backend := &recordingOperationBackend{}
	executor, err := stageworkercontrol.NewProductionExecutor(backend)
	if err != nil {
		t.Fatalf("NewProductionExecutor: %v", err)
	}
	request := controlRequest(uuid.NewString(), &velav1.StageWorkerControlServiceConnectRequest_StartStage{
		StartStage: &velav1.StartStageRequest{Authority: &velav1.StageAuthority{}},
	})
	response, err := executor.Execute(
		context.Background(),
		stageworkertransport.Identity{SPIFFEID: "spiffe://vela.test/stage-worker"},
		1,
		stageworkercontrol.OperationStartStage,
		request,
		stageworkercontrol.VerifiedAuthorities{Stage: &stageauthority.Verified{}},
	)
	if err != nil || response.GetStageCommandResult().GetDecision() !=
		velav1.StageWorkerCommandDecision_STAGE_WORKER_COMMAND_DECISION_REJECTED || backend.calls != 0 {
		t.Fatalf("malformed Execute = %#v error=%v calls=%d", response, err, backend.calls)
	}
}

func TestProductionExecutorAllowsDatabaseResolvedRegistrationCapacitySequence(t *testing.T) {
	workerID := uuid.New()
	backend := &recordingOperationBackend{readiness: stageworkercontrol.ReadinessResult{
		WorkerInstanceID: workerID, WorkerInstanceEpoch: 1, Ready: false,
		Reason: "database capacity lookup pending",
	}}
	executor, err := stageworkercontrol.NewProductionExecutor(backend)
	if err != nil {
		t.Fatalf("NewProductionExecutor: %v", err)
	}
	request := controlRequest(uuid.NewString(),
		&velav1.StageWorkerControlServiceConnectRequest_RegisterWorkerEvidence{
			RegisterWorkerEvidence: &velav1.RegisterWorkerEvidenceRequest{
				RuntimeIdentity: &velav1.ModelRuntimeIdentity{},
				Devices: []*velav1.StageAuthorityDeviceEpoch{{
					DeviceId: uuid.NewString(), DeviceEpoch: 1,
				}},
				Members: []*velav1.StageAuthorityMemberEpoch{{
					WorkerMemberId: uuid.NewString(), MemberEpoch: 1, ModelRuntimeEpoch: 1,
				}},
				ReadinessEvidence: []byte("ready"),
			},
		})
	response, err := executor.Execute(
		context.Background(),
		stageworkertransport.Identity{SPIFFEID: "spiffe://vela.test/stage-worker"},
		2, stageworkercontrol.OperationRegisterWorkerEvidence, request,
		stageworkercontrol.VerifiedAuthorities{},
	)
	if err != nil || backend.calls != 1 || response.GetWorkerReadinessDecision() == nil {
		t.Fatalf("database-resolved registration = %#v error=%v calls=%d", response, err, backend.calls)
	}
	if backend.last.ControlSessionEpoch != 2 ||
		request.GetRegisterWorkerEvidence().GetCapacityObservationSequence() != 0 {
		t.Fatalf("registration context=%#v request=%#v", backend.last, request)
	}
}

func TestProductionExecutorRejectsInputConsumeForAnotherDestination(t *testing.T) {
	now := time.Now().UTC()
	authority, digest := verifiedExecutorStageAuthority(t, now)
	backend := &recordingOperationBackend{}
	executor, err := stageworkercontrol.NewProductionExecutor(backend)
	if err != nil {
		t.Fatalf("NewProductionExecutor: %v", err)
	}
	request := controlRequest(uuid.NewString(), &velav1.StageWorkerControlServiceConnectRequest_ConsumeInputTransfer{
		ConsumeInputTransfer: &velav1.ConsumeInputTransferRequest{
			Authority: authority, TicketId: "a3000000-0000-0000-0000-000000000001",
			TokenDigest:         bytes.Repeat([]byte{0x52}, sha256.Size),
			OutcomeDigest:       bytes.Repeat([]byte{0x53}, sha256.Size),
			ConsumedAt:          timestamppb.New(now),
			WorkerInstanceId:    authority.GetWorkerInstanceId(),
			WorkerInstanceEpoch: authority.GetWorkerInstanceEpoch() + 1,
			ModelResidencyId:    authority.GetModelResidencyId(),
			ModelRuntimeEpoch:   authority.GetModelRuntimeBarrierGeneration(),
			ConnectorRevisionId: "a3000000-0000-0000-0000-000000000003",
		},
	})
	response, err := executor.Execute(
		context.Background(),
		stageworkertransport.Identity{SPIFFEID: "spiffe://vela.test/stage-worker"},
		1, stageworkercontrol.OperationConsumeInputTransfer, request,
		stageworkercontrol.VerifiedAuthorities{Stage: &stageauthority.Verified{
			Authority: authority, Digest: digest,
		}},
	)
	if err != nil || response.GetStageCommandResult().GetDecision() !=
		velav1.StageWorkerCommandDecision_STAGE_WORKER_COMMAND_DECISION_REJECTED || backend.calls != 0 {
		t.Fatalf("cross-destination consume = %#v error=%v calls=%d", response, err, backend.calls)
	}
}

func TestProductionExecutorRejectsMalformedBackendResults(t *testing.T) {
	backend := &recordingOperationBackend{
		acquire: stageworkercontrol.AcquireResult{},
	}
	executor, err := stageworkercontrol.NewProductionExecutor(backend)
	if err != nil {
		t.Fatalf("NewProductionExecutor: %v", err)
	}
	workerID := uuid.New()
	request := controlRequest(uuid.NewString(), &velav1.StageWorkerControlServiceConnectRequest_AcquireStage{
		AcquireStage: &velav1.AcquireStageRequest{
			WorkerInstanceId: workerID.String(), WorkerInstanceEpoch: 1,
			CapacityObservationSequence: 1, ModelResidencyId: uuid.NewString(),
			ModelRuntimeEpoch: 1, StageProfileRevisionId: uuid.NewString(),
		},
	})
	if _, err := executor.Execute(
		context.Background(), stageworkertransport.Identity{SPIFFEID: "spiffe://vela.test/worker"},
		1, stageworkercontrol.OperationAcquireStage, request,
		stageworkercontrol.VerifiedAuthorities{},
	); err == nil {
		t.Fatal("malformed backend result was accepted")
	}
	backend.err = errors.New("database unavailable")
	if _, err := executor.Execute(
		context.Background(), stageworkertransport.Identity{SPIFFEID: "spiffe://vela.test/worker"},
		1, stageworkercontrol.OperationAcquireStage, request,
		stageworkercontrol.VerifiedAuthorities{},
	); err == nil {
		t.Fatal("backend failure was swallowed")
	}
}

func TestProductionExecutorReturnsTerminalDatabaseRaceDecisions(t *testing.T) {
	now := time.Now().UTC()
	stageAuthority, stageDigest := verifiedExecutorStageAuthority(t, now)
	backend := &recordingOperationBackend{command: stageworkercontrol.CommandResult{
		Decision: velav1.StageWorkerCommandDecision_STAGE_WORKER_COMMAND_DECISION_STALE,
		Detail:   "StageAuthority was revoked before command commit",
	}}
	executor, err := stageworkercontrol.NewProductionExecutor(backend)
	if err != nil {
		t.Fatalf("NewProductionExecutor: %v", err)
	}
	request := controlRequest(uuid.NewString(), &velav1.StageWorkerControlServiceConnectRequest_StartStage{
		StartStage: &velav1.StartStageRequest{
			Authority: stageAuthority, StartedAt: timestamppb.New(now),
		},
	})
	for _, decision := range []velav1.StageWorkerCommandDecision{
		velav1.StageWorkerCommandDecision_STAGE_WORKER_COMMAND_DECISION_STALE,
		velav1.StageWorkerCommandDecision_STAGE_WORKER_COMMAND_DECISION_REJECTED,
	} {
		backend.command.Decision = decision
		response, executeErr := executor.Execute(
			context.Background(),
			stageworkertransport.Identity{SPIFFEID: "spiffe://vela.test/stage-worker"},
			1, stageworkercontrol.OperationStartStage, request,
			stageworkercontrol.VerifiedAuthorities{Stage: &stageauthority.Verified{
				Authority: stageAuthority, Digest: stageDigest,
			}},
		)
		if executeErr != nil || response.GetStageCommandResult().GetDecision() != decision ||
			!bytes.Equal(response.GetStageCommandResult().GetAuthorityDigest(), stageDigest[:]) {
			t.Fatalf("terminal decision %s response=%#v error=%v", decision, response, executeErr)
		}
	}
}

func TestProductionExecutorReturnsResolveDatabaseRaceDecisions(t *testing.T) {
	now := time.Now().UTC()
	stageAuthority, stageDigest := verifiedExecutorStageAuthority(t, now)
	backend := &recordingOperationBackend{}
	executor, err := stageworkercontrol.NewProductionExecutor(backend)
	if err != nil {
		t.Fatalf("NewProductionExecutor: %v", err)
	}
	request := controlRequest(uuid.NewString(), &velav1.StageWorkerControlServiceConnectRequest_ResolveInputTransfer{
		ResolveInputTransfer: &velav1.ResolveInputTransferRequest{
			Authority: stageAuthority, TicketId: uuid.NewString(),
			TokenDigest:         bytes.Repeat([]byte{0x52}, sha256.Size),
			WorkerInstanceId:    stageAuthority.GetWorkerInstanceId(),
			WorkerInstanceEpoch: stageAuthority.GetWorkerInstanceEpoch(),
			ModelResidencyId:    stageAuthority.GetModelResidencyId(),
			ModelRuntimeEpoch:   stageAuthority.GetModelRuntimeBarrierGeneration(),
			ConnectorRevisionId: uuid.NewString(), ResolvedAt: timestamppb.New(now),
		},
	})
	for _, decision := range []velav1.StageWorkerCommandDecision{
		velav1.StageWorkerCommandDecision_STAGE_WORKER_COMMAND_DECISION_STALE,
		velav1.StageWorkerCommandDecision_STAGE_WORKER_COMMAND_DECISION_REJECTED,
	} {
		backend.resolve = stageworkercontrol.ResolveInputTransferResult{
			Command: &stageworkercontrol.CommandResult{
				Decision: decision, Detail: "transfer authority changed during resolve",
			},
		}
		response, executeErr := executor.Execute(
			context.Background(),
			stageworkertransport.Identity{SPIFFEID: "spiffe://vela.test/stage-worker"},
			1, stageworkercontrol.OperationResolveInputTransfer, request,
			stageworkercontrol.VerifiedAuthorities{Stage: &stageauthority.Verified{
				Authority: stageAuthority, Digest: stageDigest,
			}},
		)
		if executeErr != nil || response.GetStageCommandResult().GetDecision() != decision ||
			response.GetStageCommandResult().GetOperation() != stageworkercontrol.OperationResolveInputTransfer ||
			!bytes.Equal(response.GetStageCommandResult().GetAuthorityDigest(), stageDigest[:]) {
			t.Fatalf("resolve terminal decision %s response=%#v error=%v", decision, response, executeErr)
		}
	}
}

func TestProductionExecutorReturnsAcquireDatabaseRaceDecisions(t *testing.T) {
	workerID := uuid.NewString()
	backend := &recordingOperationBackend{}
	executor, err := stageworkercontrol.NewProductionExecutor(backend)
	if err != nil {
		t.Fatalf("NewProductionExecutor: %v", err)
	}
	request := controlRequest(uuid.NewString(), &velav1.StageWorkerControlServiceConnectRequest_AcquireStage{
		AcquireStage: &velav1.AcquireStageRequest{
			WorkerInstanceId: workerID, WorkerInstanceEpoch: 1,
			CapacityObservationSequence: 1, ModelResidencyId: uuid.NewString(),
			ModelRuntimeEpoch: 1, StageProfileRevisionId: uuid.NewString(),
		},
	})
	for _, decision := range []velav1.StageWorkerCommandDecision{
		velav1.StageWorkerCommandDecision_STAGE_WORKER_COMMAND_DECISION_STALE,
		velav1.StageWorkerCommandDecision_STAGE_WORKER_COMMAND_DECISION_REJECTED,
	} {
		backend.acquire = stageworkercontrol.AcquireResult{Command: &stageworkercontrol.CommandResult{
			Decision: decision, Detail: "worker authority changed during stage claim",
		}}
		response, executeErr := executor.Execute(
			context.Background(),
			stageworkertransport.Identity{SPIFFEID: "spiffe://vela.test/stage-worker"},
			1, stageworkercontrol.OperationAcquireStage, request,
			stageworkercontrol.VerifiedAuthorities{},
		)
		if executeErr != nil || response.GetStageCommandResult().GetDecision() != decision ||
			response.GetStageCommandResult().GetOperation() != stageworkercontrol.OperationAcquireStage ||
			len(response.GetStageCommandResult().GetAuthorityDigest()) != 0 {
			t.Fatalf("acquire terminal decision %s response=%#v error=%v", decision, response, executeErr)
		}
	}
}

func TestProductionExecutorValidatesRenewedAuthorityContract(t *testing.T) {
	now := time.Now().UTC()
	stageAuthority, stageDigest := verifiedExecutorStageAuthority(t, now)
	validRenewal := proto.Clone(stageAuthority).(*velav1.StageAuthority)
	validRenewal.StageVersion++
	validRenewal.IssuedAt = timestamppb.New(now.Add(30 * time.Second))
	validRenewal.ExpiresAt = timestamppb.New(now.Add(2 * time.Minute))
	validRenewal.MonotonicValidFor = durationpb.New(90 * time.Second)
	validRenewal.Signature = bytes.Repeat([]byte{0x87}, ed25519.SignatureSize)
	backend := &recordingOperationBackend{command: stageworkercontrol.CommandResult{
		Decision:         velav1.StageWorkerCommandDecision_STAGE_WORKER_COMMAND_DECISION_ACCEPTED,
		RenewedAuthority: validRenewal,
	}}
	executor, err := stageworkercontrol.NewProductionExecutor(backend)
	if err != nil {
		t.Fatalf("NewProductionExecutor: %v", err)
	}
	request := controlRequest(uuid.NewString(), &velav1.StageWorkerControlServiceConnectRequest_StartStage{
		StartStage: &velav1.StartStageRequest{
			Authority: stageAuthority, StartedAt: timestamppb.New(now),
		},
	})
	authorities := stageworkercontrol.VerifiedAuthorities{Stage: &stageauthority.Verified{
		Authority: stageAuthority, Digest: stageDigest,
	}}
	response, executeErr := executor.Execute(
		context.Background(),
		stageworkertransport.Identity{SPIFFEID: "spiffe://vela.test/stage-worker"},
		1, stageworkercontrol.OperationStartStage, request, authorities,
	)
	if executeErr != nil || response.GetStageCommandResult().GetRenewedAuthority() == nil {
		t.Fatalf("valid renewal response=%#v error=%v", response, executeErr)
	}

	invalidCases := map[string]func(*velav1.StageAuthority){
		"execution identity": func(authority *velav1.StageAuthority) {
			authority.JobId = uuid.NewString()
		},
		"stage version": func(authority *velav1.StageAuthority) {
			authority.StageVersion = stageAuthority.GetStageVersion() - 1
		},
		"issued at": func(authority *velav1.StageAuthority) {
			authority.IssuedAt = stageAuthority.GetIssuedAt()
		},
		"expires at": func(authority *velav1.StageAuthority) {
			authority.ExpiresAt = stageAuthority.GetExpiresAt()
			authority.MonotonicValidFor = durationpb.New(30 * time.Second)
		},
	}
	for name, mutate := range invalidCases {
		t.Run(name, func(t *testing.T) {
			renewed := proto.Clone(validRenewal).(*velav1.StageAuthority)
			mutate(renewed)
			backend.command.RenewedAuthority = renewed
			if _, err := executor.Execute(
				context.Background(),
				stageworkertransport.Identity{SPIFFEID: "spiffe://vela.test/stage-worker"},
				1, stageworkercontrol.OperationStartStage, request, authorities,
			); err == nil {
				t.Fatal("invalid StageAuthority renewal was emitted")
			}
		})
	}
}

func TestProductionExecutorReturnsSealNegativeDecision(t *testing.T) {
	now := time.Now().UTC()
	stageAuthority, stageDigest := verifiedExecutorStageAuthority(t, now)
	backend := &recordingOperationBackend{seal: stageworkercontrol.SealResult{
		Command: &stageworkercontrol.CommandResult{
			Decision: velav1.StageWorkerCommandDecision_STAGE_WORKER_COMMAND_DECISION_STALE,
			Detail:   "StageAuthority was revoked before output seal",
		},
	}}
	executor, err := stageworkercontrol.NewProductionExecutor(backend)
	if err != nil {
		t.Fatalf("NewProductionExecutor: %v", err)
	}
	request := controlRequest(uuid.NewString(), &velav1.StageWorkerControlServiceConnectRequest_SealStageOutput{
		SealStageOutput: &velav1.SealStageOutputRequest{
			Authority: stageAuthority, LocalReceipt: &velav1.LocalMaterializationReceipt{},
		},
	})
	response, executeErr := executor.Execute(
		context.Background(),
		stageworkertransport.Identity{SPIFFEID: "spiffe://vela.test/stage-worker"},
		1, stageworkercontrol.OperationSealStageOutput, request,
		stageworkercontrol.VerifiedAuthorities{Stage: &stageauthority.Verified{
			Authority: stageAuthority, Digest: stageDigest,
		}},
	)
	if executeErr != nil || response.GetStageCommandResult().GetDecision() !=
		velav1.StageWorkerCommandDecision_STAGE_WORKER_COMMAND_DECISION_STALE ||
		!bytes.Equal(response.GetStageCommandResult().GetAuthorityDigest(), stageDigest[:]) {
		t.Fatalf("seal stale response=%#v error=%v", response, executeErr)
	}
}

func TestProductionExecutorRejectsMalformedOutboundAuthorities(t *testing.T) {
	now := time.Now().UTC()
	stageAuthority, stageDigest := verifiedExecutorStageAuthority(t, now)
	backend := &recordingOperationBackend{}
	executor, err := stageworkercontrol.NewProductionExecutor(backend)
	if err != nil {
		t.Fatalf("NewProductionExecutor: %v", err)
	}
	workerID := uuid.NewString()
	acquire := controlRequest(uuid.NewString(), &velav1.StageWorkerControlServiceConnectRequest_AcquireStage{
		AcquireStage: &velav1.AcquireStageRequest{
			WorkerInstanceId: workerID, WorkerInstanceEpoch: 1,
			CapacityObservationSequence: 1, ModelResidencyId: uuid.NewString(),
			ModelRuntimeEpoch: 1, StageProfileRevisionId: uuid.NewString(),
		},
	})
	assignmentCases := map[string]func(*velav1.StageAssignment){
		"missing member barrier": func(assignment *velav1.StageAssignment) {
			assignment.MemberStartTimeout = nil
		},
		"mismatched member set": func(assignment *velav1.StageAssignment) {
			assignment.RequiredWorkerMemberIds = []string{uuid.NewString()}
		},
		"mismatched execution spec": func(assignment *velav1.StageAssignment) {
			assignment.ExecutionSpec.ParametersJson = []byte(`{"steps":31}`)
		},
		"missing exact input ticket": func(assignment *velav1.StageAssignment) {
			assignment.ExecutionSpec.Inputs = []*velav1.StageInputArtifact{{
				StageArtifactId: uuid.NewString(), ObjectVersion: "object-version-1",
				Sha256: bytes.Repeat([]byte{0x44}, sha256.Size), SizeBytes: 1,
				StageInterfaceRevisionId: uuid.NewString(),
			}}
			digest, digestErr := stageauthority.ExecutionSpecDigest(assignment.ExecutionSpec)
			if digestErr != nil {
				t.Fatalf("ExecutionSpecDigest: %v", digestErr)
			}
			assignment.Authority.ExecutionSpecDigest = digest[:]
		},
		"missing input digest": func(assignment *velav1.StageAssignment) {
			setExecutorAssignmentInput(t, assignment, nil, uuid.NewString())
		},
		"missing input interface": func(assignment *velav1.StageAssignment) {
			setExecutorAssignmentInput(t, assignment, bytes.Repeat([]byte{0x45}, sha256.Size), "")
		},
		"zero input size": func(assignment *velav1.StageAssignment) {
			setExecutorAssignmentInput(
				t, assignment, bytes.Repeat([]byte{0x46}, sha256.Size), uuid.NewString(),
			)
			assignment.ExecutionSpec.Inputs[0].SizeBytes = 0
			rebindExecutorExecutionSpec(t, assignment)
		},
		"malformed input interface": func(assignment *velav1.StageAssignment) {
			setExecutorAssignmentInput(
				t, assignment, bytes.Repeat([]byte{0x47}, sha256.Size), "not-a-uuid",
			)
		},
		"oversized execution spec": func(assignment *velav1.StageAssignment) {
			assignment.ExecutionSpec.ParametersJson = bytes.Repeat([]byte{'x'}, 64*1024)
			digest, digestErr := stageauthority.ExecutionSpecDigest(assignment.ExecutionSpec)
			if digestErr != nil {
				t.Fatalf("ExecutionSpecDigest: %v", digestErr)
			}
			assignment.Authority.ExecutionSpecDigest = digest[:]
		},
	}
	for name, mutate := range assignmentCases {
		t.Run(name, func(t *testing.T) {
			assignment := proto.Clone(validExecutorAssignment(t, now)).(*velav1.StageAssignment)
			mutate(assignment)
			backend.acquire = stageworkercontrol.AcquireResult{Assignment: assignment}
			if _, executeErr := executor.Execute(
				context.Background(), stageworkertransport.Identity{SPIFFEID: "spiffe://vela.test/worker"},
				1, stageworkercontrol.OperationAcquireStage, acquire, stageworkercontrol.VerifiedAuthorities{},
			); executeErr == nil {
				t.Fatal("malformed Assignment was emitted")
			}
		})
	}

	backend.seal = stageworkercontrol.SealResult{
		Authority: validExecutorMaterializationAuthority(now, sha256.Sum256([]byte("wrong-stage"))),
	}
	seal := controlRequest(uuid.NewString(), &velav1.StageWorkerControlServiceConnectRequest_SealStageOutput{
		SealStageOutput: &velav1.SealStageOutputRequest{
			Authority: stageAuthority, LocalReceipt: &velav1.LocalMaterializationReceipt{},
		},
	})
	if _, executeErr := executor.Execute(
		context.Background(), stageworkertransport.Identity{SPIFFEID: "spiffe://vela.test/worker"},
		1, stageworkercontrol.OperationSealStageOutput, seal,
		stageworkercontrol.VerifiedAuthorities{Stage: &stageauthority.Verified{
			Authority: stageAuthority, Digest: stageDigest,
		}},
	); executeErr == nil {
		t.Fatal("MaterializationAuthority with mismatched StageAuthority digest was emitted")
	}
}

func validExecutorAssignment(t *testing.T, now time.Time) *velav1.StageAssignment {
	t.Helper()
	executionSpec := &velav1.StageExecutionSpec{ParametersJson: []byte(`{}`)}
	executionSpecDigest, err := stageauthority.ExecutionSpecDigest(executionSpec)
	if err != nil {
		t.Fatalf("ExecutionSpecDigest: %v", err)
	}
	authority := controlAuthority(now)
	authority.ExecutionSpecDigest = executionSpecDigest[:]
	authority.Signature = bytes.Repeat([]byte{0x86}, ed25519.SignatureSize)
	return &velav1.StageAssignment{
		Authority: authority, ExecutionSpec: executionSpec,
		RequiredWorkerMemberIds: []string{authority.GetMembers()[0].GetWorkerMemberId()},
		MemberStartTimeout:      durationpb.New(5 * time.Second),
	}
}

func setExecutorAssignmentInput(
	t *testing.T,
	assignment *velav1.StageAssignment,
	digest []byte,
	interfaceRevisionID string,
) {
	t.Helper()
	artifactID := uuid.NewString()
	assignment.ExecutionSpec.Inputs = []*velav1.StageInputArtifact{{
		StageArtifactId: artifactID, ObjectVersion: "object-version-1",
		Sha256: digest, SizeBytes: 1, StageInterfaceRevisionId: interfaceRevisionID,
	}}
	assignment.InputTransferTickets = []*velav1.StageInputTransferTicket{{
		StageArtifactId: artifactID, ObjectVersion: "object-version-1", TransferTicket: []byte("ticket"),
	}}
	rebindExecutorExecutionSpec(t, assignment)
}

func rebindExecutorExecutionSpec(t *testing.T, assignment *velav1.StageAssignment) {
	t.Helper()
	executionSpecDigest, err := stageauthority.ExecutionSpecDigest(assignment.ExecutionSpec)
	if err != nil {
		t.Fatalf("ExecutionSpecDigest: %v", err)
	}
	assignment.Authority.ExecutionSpecDigest = executionSpecDigest[:]
}

func verifiedExecutorStageAuthority(
	t *testing.T,
	now time.Time,
) (*velav1.StageAuthority, [sha256.Size]byte) {
	t.Helper()
	authority := controlAuthority(now)
	authority.Signature = bytes.Repeat([]byte{0x86}, ed25519.SignatureSize)
	digest, err := stageauthority.Digest(authority)
	if err != nil {
		t.Fatalf("StageAuthority digest: %v", err)
	}
	return authority, digest
}

func validExecutorMaterializationAuthority(
	now time.Time,
	stageDigest [sha256.Size]byte,
) *velav1.MaterializationAuthority {
	authority := controlMaterializationAuthority(now)
	authority.StageAuthorityDigest = stageDigest[:]
	authority.Token = bytes.Repeat([]byte{0x94}, sha256.Size)
	return authority
}

func controlRequest(
	requestID string,
	operation any,
) *velav1.StageWorkerControlServiceConnectRequest {
	request := &velav1.StageWorkerControlServiceConnectRequest{RequestId: requestID}
	switch typed := operation.(type) {
	case *velav1.StageWorkerControlServiceConnectRequest_RegisterWorkerEvidence:
		request.Operation = typed
	case *velav1.StageWorkerControlServiceConnectRequest_ReportCapacityObservation:
		request.Operation = typed
	case *velav1.StageWorkerControlServiceConnectRequest_AcquireStage:
		request.Operation = typed
	case *velav1.StageWorkerControlServiceConnectRequest_StartStage:
		request.Operation = typed
	case *velav1.StageWorkerControlServiceConnectRequest_HeartbeatStage:
		request.Operation = typed
	case *velav1.StageWorkerControlServiceConnectRequest_SealStageOutput:
		request.Operation = typed
	case *velav1.StageWorkerControlServiceConnectRequest_CommitStageMaterialization:
		request.Operation = typed
	case *velav1.StageWorkerControlServiceConnectRequest_FailStage:
		request.Operation = typed
	case *velav1.StageWorkerControlServiceConnectRequest_ReattachStage:
		request.Operation = typed
	case *velav1.StageWorkerControlServiceConnectRequest_ReportMaterializationSourceLost:
		request.Operation = typed
	case *velav1.StageWorkerControlServiceConnectRequest_ResolveInputTransfer:
		request.Operation = typed
	case *velav1.StageWorkerControlServiceConnectRequest_ConsumeInputTransfer:
		request.Operation = typed
	}
	return request
}

func assertExecutorResult(
	t *testing.T,
	response *velav1.StageWorkerControlServiceConnectResponse,
	want string,
	stageDigest [sha256.Size]byte,
	materializationDigest [sha256.Size]byte,
) {
	t.Helper()
	switch want {
	case "readiness":
		if !response.GetWorkerReadinessDecision().GetReady() {
			t.Fatalf("readiness response = %#v", response)
		}
	case "assignment":
		if response.GetStageAssignment() == nil {
			t.Fatalf("assignment response = %#v", response)
		}
	case "materialization":
		if response.GetMaterializationAuthority() == nil {
			t.Fatalf("materialization response = %#v", response)
		}
	case "transfer":
		transfer := response.GetResolvedInputTransfer()
		if transfer.GetTicketId() == "" || transfer.GetObjectVersion() == "" ||
			len(transfer.GetSha256()) != sha256.Size {
			t.Fatalf("resolved transfer response = %#v", response)
		}
	case "command-stage":
		if !bytes.Equal(response.GetStageCommandResult().GetAuthorityDigest(), stageDigest[:]) {
			t.Fatalf("stage command response = %#v", response)
		}
	case "command-materialization":
		if !bytes.Equal(response.GetStageCommandResult().GetAuthorityDigest(), materializationDigest[:]) {
			t.Fatalf("materialization command response = %#v", response)
		}
	default:
		t.Fatalf("unknown expected result %q", want)
	}
}

type recordingOperationBackend struct {
	calls     int
	operation stageworkercontrol.Operation
	last      stageworkercontrol.CommandContext
	readiness stageworkercontrol.ReadinessResult
	acquire   stageworkercontrol.AcquireResult
	command   stageworkercontrol.CommandResult
	seal      stageworkercontrol.SealResult
	resolve   stageworkercontrol.ResolveInputTransferResult
	err       error
}

func (backend *recordingOperationBackend) record(
	command stageworkercontrol.CommandContext,
	operation stageworkercontrol.Operation,
) {
	backend.calls++
	backend.last = command
	backend.operation = operation
}

func (backend *recordingOperationBackend) RegisterWorkerEvidence(
	_ context.Context,
	command stageworkercontrol.CommandContext,
	_ *velav1.RegisterWorkerEvidenceRequest,
) (stageworkercontrol.ReadinessResult, error) {
	backend.record(command, stageworkercontrol.OperationRegisterWorkerEvidence)
	return backend.readiness, backend.err
}

func (backend *recordingOperationBackend) ReportCapacityObservation(
	_ context.Context,
	command stageworkercontrol.CommandContext,
	_ *velav1.ReportStageCapacityObservationRequest,
) (stageworkercontrol.ReadinessResult, error) {
	backend.record(command, stageworkercontrol.OperationReportCapacityObservation)
	return backend.readiness, backend.err
}

func (backend *recordingOperationBackend) AcquireStage(
	_ context.Context,
	command stageworkercontrol.CommandContext,
	_ *velav1.AcquireStageRequest,
) (stageworkercontrol.AcquireResult, error) {
	backend.record(command, stageworkercontrol.OperationAcquireStage)
	return backend.acquire, backend.err
}

func (backend *recordingOperationBackend) StartStage(
	_ context.Context,
	command stageworkercontrol.CommandContext,
	_ *velav1.StartStageRequest,
	_ stageworkercontrol.VerifiedAuthorities,
) (stageworkercontrol.CommandResult, error) {
	backend.record(command, stageworkercontrol.OperationStartStage)
	return backend.command, backend.err
}

func (backend *recordingOperationBackend) HeartbeatStage(
	_ context.Context,
	command stageworkercontrol.CommandContext,
	_ *velav1.HeartbeatStageRequest,
	_ stageworkercontrol.VerifiedAuthorities,
) (stageworkercontrol.CommandResult, error) {
	backend.record(command, stageworkercontrol.OperationHeartbeatStage)
	return backend.command, backend.err
}

func (backend *recordingOperationBackend) SealStageOutput(
	_ context.Context,
	command stageworkercontrol.CommandContext,
	_ *velav1.SealStageOutputRequest,
	_ stageworkercontrol.VerifiedAuthorities,
) (stageworkercontrol.SealResult, error) {
	backend.record(command, stageworkercontrol.OperationSealStageOutput)
	return backend.seal, backend.err
}

func (backend *recordingOperationBackend) CommitStageMaterialization(
	_ context.Context,
	command stageworkercontrol.CommandContext,
	_ *velav1.CommitStageMaterializationRequest,
	_ stageworkercontrol.VerifiedAuthorities,
) (stageworkercontrol.CommandResult, error) {
	backend.record(command, stageworkercontrol.OperationCommitStageMaterialization)
	return backend.command, backend.err
}

func (backend *recordingOperationBackend) FailStage(
	_ context.Context,
	command stageworkercontrol.CommandContext,
	_ *velav1.FailStageRequest,
	_ stageworkercontrol.VerifiedAuthorities,
) (stageworkercontrol.CommandResult, error) {
	backend.record(command, stageworkercontrol.OperationFailStage)
	return backend.command, backend.err
}

func (backend *recordingOperationBackend) ReattachStage(
	_ context.Context,
	command stageworkercontrol.CommandContext,
	_ *velav1.ReattachStageRequest,
	_ stageworkercontrol.VerifiedAuthorities,
) (stageworkercontrol.CommandResult, error) {
	backend.record(command, stageworkercontrol.OperationReattachStage)
	return backend.command, backend.err
}

func (backend *recordingOperationBackend) ReportMaterializationSourceLost(
	_ context.Context,
	command stageworkercontrol.CommandContext,
	_ *velav1.ReportMaterializationSourceLostRequest,
	_ stageworkercontrol.VerifiedAuthorities,
) (stageworkercontrol.CommandResult, error) {
	backend.record(command, stageworkercontrol.OperationReportMaterializationSourceLost)
	return backend.command, backend.err
}

func (backend *recordingOperationBackend) ResolveInputTransfer(
	_ context.Context,
	command stageworkercontrol.CommandContext,
	_ *velav1.ResolveInputTransferRequest,
	_ stageworkercontrol.VerifiedAuthorities,
) (stageworkercontrol.ResolveInputTransferResult, error) {
	backend.record(command, stageworkercontrol.OperationResolveInputTransfer)
	return backend.resolve, backend.err
}

func (backend *recordingOperationBackend) ConsumeInputTransfer(
	_ context.Context,
	command stageworkercontrol.CommandContext,
	_ *velav1.ConsumeInputTransferRequest,
	_ stageworkercontrol.VerifiedAuthorities,
) (stageworkercontrol.CommandResult, error) {
	backend.record(command, stageworkercontrol.OperationConsumeInputTransfer)
	return backend.command, backend.err
}

var _ stageworkercontrol.OperationBackend = (*recordingOperationBackend)(nil)
