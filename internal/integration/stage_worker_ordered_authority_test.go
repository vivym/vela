//go:build integration

package integration_test

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/stageauthority"
	"github.com/vivym/vela/internal/stageworkercontrol"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestStageWorkerAssignmentOrderIsDurableAcrossReplayAndRenewal(t *testing.T) {
	ctx := context.Background()
	fixture := newStageSchedulerFixture(t, "signed-allocation-order")
	backend := newPostgresAssignmentTestBackend(t, fixture)
	command, request := stageWorkerAcquireCommand(fixture), stageWorkerAcquireRequest(fixture)
	acquired, err := backend.AcquireStage(ctx, command, request)
	if err != nil || acquired.Assignment == nil {
		t.Fatalf("acquire: %v %v", acquired, err)
	}
	authority := acquired.Assignment.GetAuthority()
	if authority.GetSchemaVersion() != stageauthority.SchemaVersionV2 || authority.GetExecutionSequence() <= 0 {
		t.Fatalf("assignment lacks signed database order: schema=%d sequence=%d", authority.GetSchemaVersion(), authority.GetExecutionSequence())
	}
	replayed, err := backend.AcquireStage(ctx, command, request)
	if err != nil || !proto.Equal(replayed.Assignment, acquired.Assignment) {
		t.Fatalf("assignment replay changed: %v", err)
	}
	keys := map[string][]byte{"stage-authority-key-v1": bytes.Repeat([]byte{0x9a}, 32)}
	signer, err := stageauthority.NewSigner(keys)
	if err != nil {
		t.Fatal(err)
	}
	now := authority.GetIssuedAt().AsTime().Add(50 * time.Millisecond)
	validator, err := stageauthority.NewValidator(keys, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	pool := newRolePool(t, fixture.database.DSN, "vela_stage_worker_control_login", "vela-stage-worker-control-password")
	authorizer, err := stageworkercontrol.NewPostgresAuthorizer(pool)
	if err != nil {
		t.Fatal(err)
	}
	verified, err := validator.ValidateEnvelope(authority)
	if err != nil {
		t.Fatal(err)
	}
	active, err := authorizer.IsActive(ctx, command.Identity, command.ControlSessionEpoch, stageworkercontrol.OperationStartStage, verified)
	if err != nil || !active {
		t.Fatalf("original order authorization: %t %v", active, err)
	}
	changed := proto.Clone(authority).(*velav1.StageAuthority)
	changed.ExecutionSequence++
	changed.Signature = nil
	changed, err = signer.Sign(changed)
	if err != nil {
		t.Fatal(err)
	}
	changedVerified, err := validator.ValidateEnvelope(changed)
	if err != nil {
		t.Fatal(err)
	}
	active, err = authorizer.IsActive(ctx, command.Identity, command.ControlSessionEpoch, stageworkercontrol.OperationStartStage, changedVerified)
	if err != nil || active {
		t.Fatalf("signed sequence differing from database was authorized: %t %v", active, err)
	}
	execution, err := stageworkercontrol.NewPostgresExecutionBackend(pool, signer, stageworkercontrol.PostgresExecutionConfig{
		ActiveSigningKeyID: "stage-authority-key-v1", AuthorityTTL: 2 * time.Minute,
		LocalDeadlineTTL: 90 * time.Second, MaxClockSkew: time.Second,
		Now: func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	startCommand := command
	startCommand.CommandID = uuid.New()
	startRequest := &velav1.StartStageRequest{Authority: authority, StartedAt: timestamppb.New(now)}
	started, err := execution.StartStage(ctx, startCommand, startRequest, stageworkercontrol.VerifiedAuthorities{Stage: &verified})
	if err != nil || started.Decision != velav1.StageWorkerCommandDecision_STAGE_WORKER_COMMAND_DECISION_ACCEPTED ||
		started.RenewedAuthority.GetSchemaVersion() != stageauthority.SchemaVersionV2 ||
		started.RenewedAuthority.GetExecutionSequence() != authority.GetExecutionSequence() {
		t.Fatalf("START renewal changed allocation order: %v %v", started, err)
	}
	startReplay, err := execution.StartStage(ctx, startCommand, startRequest, stageworkercontrol.VerifiedAuthorities{Stage: &verified})
	if err != nil || startReplay.Decision != velav1.StageWorkerCommandDecision_STAGE_WORKER_COMMAND_DECISION_REPLAYED ||
		!proto.Equal(started.RenewedAuthority, startReplay.RenewedAuthority) {
		t.Fatalf("START replay changed signed authority: %v %v", startReplay, err)
	}
}
