//go:build integration

package integration_test

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/attemptcoordinator"
	"github.com/vivym/vela/internal/stageauthority"
	"github.com/vivym/vela/internal/stageworkercontrol"
	"github.com/vivym/vela/internal/stageworkertransport"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestStageWorkerControlDurableAuthorizerRejectsStaleExecutionEvidence(t *testing.T) {
	database, _, coordinator, job, attemptID, encoderRunID, _ :=
		newStageGraphCancellationFixture(t, "stage-worker-control-authorizer")
	assignment := assignEncoder(
		t, database, coordinator, attemptID, encoderRunID, time.Now().Add(time.Hour),
	)
	spiffeID := "spiffe://vela/worker/" + assignment.WorkerInstanceID.String()
	verified := signedAssignedStageAuthority(t, database, job, assignment, 2)
	authorizer, err := stageworkercontrol.NewPostgresAuthorizer(newRolePool(
		t, database.DSN,
		"vela_stage_worker_control_login", "vela-stage-worker-control-password",
	))
	if err != nil {
		t.Fatalf("NewPostgresAuthorizer: %v", err)
	}
	identity := stageworkertransport.Identity{SPIFFEID: spiffeID}

	active, err := authorizer.IsActive(
		context.Background(), identity, 1, stageworkercontrol.OperationStartStage, verified,
	)
	if err != nil || !active {
		t.Fatalf("assigned StageAuthority active=%t error=%v", active, err)
	}
	active, err = authorizer.IsActive(
		context.Background(), identity, 1,
		stageworkercontrol.OperationHeartbeatStage, verified,
	)
	if err != nil || active {
		t.Fatalf("ASSIGNED Heartbeat StageAuthority active=%t error=%v", active, err)
	}
	for _, test := range []struct {
		name   string
		mutate func(*velav1.StageAuthority)
	}{
		{
			name: "forged lease token",
			mutate: func(authority *velav1.StageAuthority) {
				authority.LeaseToken = bytes.Repeat([]byte{0xff}, 32)
			},
		},
		{
			name: "stale member epoch",
			mutate: func(authority *velav1.StageAuthority) {
				authority.Members[0].MemberEpoch++
			},
		},
		{
			name: "stale device epoch",
			mutate: func(authority *velav1.StageAuthority) {
				authority.Devices[0].DeviceEpoch++
			},
		},
		{
			name: "stale runtime epoch",
			mutate: func(authority *velav1.StageAuthority) {
				authority.Members[0].ModelRuntimeEpoch++
			},
		},
		{
			name: "stale capacity observation",
			mutate: func(authority *velav1.StageAuthority) {
				authority.CapacityObservationSequence++
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			mutatedEnvelope := proto.Clone(verified.Authority).(*velav1.StageAuthority)
			test.mutate(mutatedEnvelope)
			mutatedEnvelope.Signature = nil
			mutated := signAndVerifyStageAuthority(
				t, mutatedEnvelope, assignment.IssuedAt.Add(time.Millisecond),
			)
			active, activeErr := authorizer.IsActive(
				context.Background(), identity, 1,
				stageworkercontrol.OperationStartStage, mutated,
			)
			if activeErr != nil || active {
				t.Fatalf("stale StageAuthority active=%t error=%v", active, activeErr)
			}
		})
	}
	for _, test := range []struct {
		name         string
		identity     stageworkertransport.Identity
		sessionEpoch int64
	}{
		{name: "forged SPIFFE", identity: stageworkertransport.Identity{SPIFFEID: spiffeID + "/forged"}, sessionEpoch: 1},
		{name: "stale session", identity: identity, sessionEpoch: 2},
	} {
		t.Run(test.name, func(t *testing.T) {
			active, activeErr := authorizer.IsActive(
				context.Background(), test.identity, test.sessionEpoch,
				stageworkercontrol.OperationStartStage, verified,
			)
			if activeErr != nil || active {
				t.Fatalf("stale StageAuthority active=%t error=%v", active, activeErr)
			}
		})
	}

	if _, err := coordinator.Apply(context.Background(), attemptcoordinator.StartStageCommand{
		CommandID: uuid.New(), AttemptID: assignment.AttemptID, StageRunID: assignment.StageRunID,
		StageAttemptID: assignment.StageAttemptID, StageLeaseID: assignment.StageLeaseID,
		ExpectedAttemptFence: 1, ExpectedStageFence: 1, ExpectedStageVersion: 2,
		StartedAt: assignment.IssuedAt.Add(time.Millisecond),
	}); err != nil {
		t.Fatalf("start StageAttempt: %v", err)
	}
	active, err = authorizer.IsActive(
		context.Background(), identity, 1, stageworkercontrol.OperationHeartbeatStage, verified,
	)
	if err != nil || active {
		t.Fatalf("old StageVersion active=%t error=%v", active, err)
	}

	renewedEnvelope := proto.Clone(verified.Authority).(*velav1.StageAuthority)
	renewedEnvelope.StageVersion = 3
	renewedEnvelope.Signature = nil
	renewed := signAndVerifyStageAuthority(t, renewedEnvelope, assignment.IssuedAt.Add(2*time.Millisecond))
	active, err = authorizer.IsActive(
		context.Background(), identity, 1, stageworkercontrol.OperationHeartbeatStage, renewed,
	)
	if err != nil || !active {
		t.Fatalf("renewed StageAuthority active=%t error=%v", active, err)
	}
}

func signedAssignedStageAuthority(
	t *testing.T,
	database testDatabase,
	job jobResponse,
	assignment attemptcoordinator.AssignStageCommand,
	stageVersion int64,
) stageauthority.Verified {
	t.Helper()
	var runtimeIdentity string
	var memberID, deviceID uuid.UUID
	var memberEpoch, deviceEpoch int64
	if err := database.Admin.QueryRow(`
		SELECT residency.runtime_identity, member.id, member.member_epoch,
		       device.id, device.device_epoch
		FROM model_residencies AS residency
		JOIN worker_members AS member
		  ON member.worker_instance_id = residency.worker_instance_id
		JOIN worker_member_devices AS binding
		  ON binding.worker_instance_id = member.worker_instance_id
		 AND binding.worker_member_id = member.id
		JOIN devices AS device ON device.id = binding.device_id
		WHERE residency.id = $1
	`, assignment.ModelResidencyID).Scan(
		&runtimeIdentity, &memberID, &memberEpoch, &deviceID, &deviceEpoch,
	); err != nil {
		t.Fatalf("read assigned Stage runtime evidence: %v", err)
	}
	executionSpecDigest, err := stageauthority.ExecutionSpecDigest(&velav1.StageExecutionSpec{})
	if err != nil {
		t.Fatalf("ExecutionSpecDigest: %v", err)
	}
	envelope := &velav1.StageAuthority{
		SchemaVersion: stageauthority.SchemaVersionV1,
		JobId:         uuid.MustParse(job.JobID).String(), AttemptId: assignment.AttemptID.String(),
		StageRunId: assignment.StageRunID.String(), StageAttemptId: assignment.StageAttemptID.String(),
		StageAllocationId: assignment.StageAllocationID.String(), StageLeaseId: assignment.StageLeaseID.String(),
		AttemptFence: 1, StageFence: 1, StageVersion: stageVersion,
		WorkerInstanceId:    assignment.WorkerInstanceID.String(),
		WorkerInstanceEpoch: assignment.WorkerInstanceEpoch,
		DeviceSetDigest:     append([]byte(nil), assignment.DeviceSetDigest...),
		Devices: []*velav1.StageAuthorityDeviceEpoch{{
			DeviceId: deviceID.String(), DeviceEpoch: deviceEpoch,
		}},
		MembershipDigest: append([]byte(nil), assignment.MembershipDigest...),
		Members: []*velav1.StageAuthorityMemberEpoch{{
			WorkerMemberId: memberID.String(), MemberEpoch: memberEpoch,
			ModelRuntimeEpoch: assignment.ModelRuntimeEpoch,
		}},
		ModelResidencyId:            assignment.ModelResidencyID.String(),
		ModelRuntimeIdentity:        runtimeIdentity,
		StageProfileRevisionId:      assignment.StageProfileRevisionID.String(),
		CapacityObservationSequence: assignment.ObservationSequence,
		CapacityVector:              assignment.CapacityVector,
		LeaseToken:                  bytes.Repeat([]byte{0xb3}, 32),
		ExecutionNonce:              append([]byte(nil), assignment.ExecutionNonce...),
		SigningKeyId:                assignment.SigningKeyID,
		IssuedAt:                    timestamppb.New(assignment.IssuedAt),
		ExpiresAt:                   timestamppb.New(assignment.ExpiresAt),
		MonotonicValidFor:           durationpb.New(assignment.LocalDeadlineAt.Sub(assignment.IssuedAt)),
		ExecutionSpecDigest:         executionSpecDigest[:],
	}
	return signAndVerifyStageAuthority(t, envelope, assignment.IssuedAt.Add(time.Millisecond))
}

func signAndVerifyStageAuthority(
	t *testing.T,
	envelope *velav1.StageAuthority,
	now time.Time,
) stageauthority.Verified {
	t.Helper()
	keys := map[string][]byte{
		"stage-authority-key-v1": bytes.Repeat([]byte{0x9a}, 32),
	}
	signer, err := stageauthority.NewSigner(keys)
	if err != nil {
		t.Fatalf("NewSigner: %v", err)
	}
	signed, err := signer.Sign(envelope)
	if err != nil {
		t.Fatalf("Sign StageAuthority: %v", err)
	}
	validator, err := stageauthority.NewValidator(keys, func() time.Time { return now })
	if err != nil {
		t.Fatalf("NewValidator: %v", err)
	}
	verified, err := validator.ValidateEnvelope(signed)
	if err != nil {
		t.Fatalf("ValidateEnvelope: %v", err)
	}
	return verified
}
