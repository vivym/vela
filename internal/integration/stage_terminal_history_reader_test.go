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
)

func TestPostgresTerminalHistoryRequiresExactSignedOriginal(t *testing.T) {
	fixture := newStageSchedulerFixture(t, "terminal-history-reader")
	command := stageWorkerAcquireCommand(fixture)
	backend := newPostgresAssignmentTestBackend(t, fixture)
	acquired, err := backend.AcquireStage(context.Background(), command, stageWorkerAcquireRequest(fixture))
	if err != nil || acquired.Assignment == nil {
		t.Fatalf("Acquire: %v %v", acquired, err)
	}
	authority := acquired.Assignment.GetAuthority()
	keys := map[string][]byte{"stage-authority-key-v1": bytes.Repeat([]byte{0x9a}, 32)}
	now := authority.GetIssuedAt().AsTime().Add(time.Millisecond)
	validator, err := stageauthority.NewValidator(keys, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	pool := newRolePool(t, fixture.database.DSN, "vela_stage_worker_control_login", "vela-stage-worker-control-password")
	reader, err := stageworkercontrol.NewPostgresTerminalHistoryReader(pool, validator)
	if err != nil {
		t.Fatal(err)
	}
	if history, err := reader.Read(context.Background(), command, authority, command.CommandID); err != nil || history != nil {
		t.Fatalf("active StageRun history=%v error=%v", history, err)
	}
	failTerminalHistoryAssignment(t, fixture, authority)
	// Historical authentication survives expiry without granting execution time.
	now = authority.GetExpiresAt().AsTime().Add(time.Hour)
	history, err := reader.Read(context.Background(), command, authority, command.CommandID)
	if err != nil || history == nil || history.StageRunID.String() != authority.GetStageRunId() ||
		history.TerminalState != "FAILED" || history.Cutoff != authority.GetExecutionSequence() || len(history.Allocations) != 1 {
		t.Fatalf("expired original history=%v error=%v", history, err)
	}
	digest, err := stageauthority.Digest(authority)
	if err != nil || history.OriginalAuthorityDigest != digest {
		t.Fatalf("history original digest differs: %v", err)
	}
	signer, err := stageauthority.NewSigner(keys)
	if err != nil {
		t.Fatal(err)
	}
	changed := proto.Clone(authority).(*velav1.StageAuthority)
	changed.ExecutionSpecDigest[0] ^= 1
	changed.Signature = nil
	changed, err = signer.Sign(changed)
	if err != nil {
		t.Fatal(err)
	}
	if candidate := readTerminalHistory(t, fixture, terminalHistoryRequest(t, command, changed)); !candidate.Eligible {
		t.Fatal("fixture did not reach original-wire candidate matching")
	}
	if history, err := reader.Read(context.Background(), command, changed, command.CommandID); err != nil || history != nil {
		t.Fatalf("different signed original accepted: history=%v error=%v", history, err)
	}
	server := admissionServerForDatabase(t, fixture.database)
	accepted := submitJob(t, server.URL, "terminal-history-other-acquire", []byte(`{
		"model":"minimax-h3", "generation_preset":"balanced", "service_class":"standard",
		"output_spec":"video-1080p-5s-24fps", "generation_count":1, "prompt":"another original envelope"
	}`))
	if accepted.StatusCode != 202 {
		t.Fatalf("submit second Job status=%d", accepted.StatusCode)
	}
	otherCommand := stageWorkerAcquireCommand(fixture)
	other, err := backend.AcquireStage(context.Background(), otherCommand, stageWorkerAcquireRequest(fixture))
	if err != nil || other.Assignment == nil {
		t.Fatalf("second Acquire: %v %v", other, err)
	}
	request := terminalHistoryRequest(t, command, authority)
	request["acquire_command_id"] = otherCommand.CommandID
	if candidate := readTerminalHistory(t, fixture, request); !candidate.Eligible {
		t.Fatal("fixture did not locate another same-Worker original-wire candidate")
	}
	if history, err := reader.Read(context.Background(), command, authority, otherCommand.CommandID); err != nil || history != nil {
		t.Fatalf("different Acquire original accepted: history=%v error=%v", history, err)
	}
	now = authority.GetIssuedAt().AsTime().Add(-time.Second)
	if history, err := reader.Read(context.Background(), command, authority, command.CommandID); err == nil || history != nil {
		t.Fatalf("future-issued original accepted: history=%v error=%v", history, err)
	}
	now = authority.GetExpiresAt().AsTime().Add(time.Hour)
	changed = proto.Clone(authority).(*velav1.StageAuthority)
	changed.Signature[0] ^= 1
	if history, err := reader.Read(context.Background(), command, changed, uuid.Nil); err == nil || history != nil {
		t.Fatalf("invalid signature accepted: history=%v error=%v", history, err)
	}
}

func newTerminalHistoryReaderForTest(t *testing.T, fixture stageSchedulerFixture, authority *velav1.StageAuthority) *stageworkercontrol.PostgresTerminalHistoryReader {
	t.Helper()
	validator, err := stageauthority.NewValidator(map[string][]byte{
		"stage-authority-key-v1": bytes.Repeat([]byte{0x9a}, 32),
	}, func() time.Time { return authority.GetExpiresAt().AsTime().Add(time.Hour) })
	if err != nil {
		t.Fatal(err)
	}
	pool := newRolePool(t, fixture.database.DSN, "vela_stage_worker_control_login", "vela-stage-worker-control-password")
	reader, err := stageworkercontrol.NewPostgresTerminalHistoryReader(pool, validator)
	if err != nil {
		t.Fatal(err)
	}
	return reader
}
