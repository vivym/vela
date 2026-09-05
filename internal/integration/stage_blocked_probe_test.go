//go:build integration

package integration_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"path/filepath"
	"testing"
	"time"

	"github.com/pressly/goose/v3"
	"github.com/vivym/vela/internal/stageworkercontrol"
	"google.golang.org/protobuf/proto"
)

func TestBlockedStageWorkerProbeConcurrentCapacityChangeOnlyDelaysOnePoll(t *testing.T) {
	fixture := newStageSchedulerFixture(t, "blocked-probe-race")
	if _, err := fixture.database.Admin.Exec(`
		UPDATE projects SET running_count = running_limit WHERE id = $1
	`, testProjectID); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.database.Admin.Exec(`
		DO $test$
		DECLARE definition text;
		BEGIN
			definition := pg_get_functiondef('vela_read_stage_scheduler_snapshot(jsonb)'::regprocedure);
			EXECUTE replace(definition, '    RETURN QUERY SELECT v_snapshot_id, v_snapshot;',
			    E'    PERFORM pg_advisory_xact_lock(84001);\n    RETURN QUERY SELECT v_snapshot_id, v_snapshot;');
		END
		$test$
	`); err != nil {
		t.Fatal(err)
	}
	blocker, err := fixture.database.Admin.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = blocker.Rollback() }()
	if _, err := blocker.Exec(`SELECT pg_advisory_xact_lock(84001)`); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	backend := newPostgresAssignmentTestBackend(t, fixture)
	command := stageWorkerAcquireCommand(fixture)
	request := stageWorkerAcquireRequest(fixture)
	type response struct {
		result stageworkercontrol.AcquireResult
		err    error
	}
	done := make(chan response, 1)
	go func() {
		result, err := backend.AcquireStage(ctx, command, request)
		done <- response{result, err}
	}()
	deadline := time.Now().Add(5 * time.Second)
	for {
		var waiting bool
		if err := fixture.database.Admin.QueryRow(`
			SELECT EXISTS (SELECT 1 FROM pg_stat_activity
			WHERE usename = 'vela_stage_worker_control_login' AND wait_event = 'advisory')
		`).Scan(&waiting); err != nil {
			t.Fatal(err)
		}
		if waiting {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("advisory probe never reached its captured blocked candidate snapshot")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if _, err := fixture.database.Admin.Exec(`UPDATE projects SET running_count = 0 WHERE id = $1`, testProjectID); err != nil {
		t.Fatal(err)
	}
	if err := blocker.Commit(); err != nil {
		t.Fatal(err)
	}
	first := <-done
	if first.err != nil || first.result.Assignment != nil || first.result.RetryAfter != 250*time.Millisecond {
		t.Fatalf("snapshot captured before capacity release: result=%#v error=%v", first.result, first.err)
	}
	var rows int
	if err := fixture.database.Admin.QueryRow(`SELECT count(*) FROM stage_worker_acquire_intents`).Scan(&rows); err != nil {
		t.Fatal(err)
	}
	if rows != 0 {
		t.Fatal("advisory race persisted a stale NoWork command")
	}
	assigned, err := backend.AcquireStage(ctx, command, request)
	if err != nil || assigned.Assignment == nil {
		t.Fatalf("next poll did not observe newly eligible work: result=%#v error=%v", assigned, err)
	}
	replayed, err := backend.AcquireStage(ctx, command, request)
	if err != nil || !proto.Equal(replayed.Assignment, assigned.Assignment) {
		t.Fatalf("assignment acquired after advisory race did not replay: result=%#v error=%v", replayed, err)
	}
}

func TestBlockedStageWorkerProbeUpgradeRollbackPreservesDurableWireReplay(t *testing.T) {
	fixture := newStageSchedulerFixture(t, "blocked-probe-wire-replay")
	migrations := filepath.Join(repositoryRoot(t), "db", "migrations")
	if err := goose.DownTo(fixture.database.Admin, migrations, 83); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.database.Admin.Exec(`UPDATE projects SET running_count = running_limit WHERE id = $1`, testProjectID); err != nil {
		t.Fatal(err)
	}
	capture := &legacyAcquireReplayScheduler{AssignmentScheduler: newStageSchedulerTestService(t, fixture)}
	backend := newPostgresAssignmentTestBackendWithScheduler(t, fixture, capture)
	request := stageWorkerAcquireRequest(fixture)
	negativeCommand := stageWorkerAcquireCommand(fixture)
	negative, err := backend.AcquireStage(context.Background(), negativeCommand, request)
	if err != nil || negative.Assignment != nil || negative.RetryAfter != 250*time.Millisecond {
		t.Fatalf("schema 83 durable negative response: result=%#v error=%v", negative, err)
	}
	if err := goose.UpTo(fixture.database.Admin, migrations, 84); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.database.Admin.Exec(`UPDATE projects SET running_count = 0 WHERE id = $1`, testProjectID); err != nil {
		t.Fatal(err)
	}
	replayedNegative, err := backend.AcquireStage(context.Background(), negativeCommand, request)
	if err != nil || replayedNegative != negative {
		t.Fatalf("schema 84 changed a durable schema 83 negative response: result=%#v error=%v", replayedNegative, err)
	}
	command := stageWorkerAcquireCommand(fixture)
	digest := sha256.Sum256([]byte(command.Identity.SPIFFEID))
	payload, err := json.Marshal(map[string]any{
		"schema_version": 1, "command_id": command.CommandID,
		"worker_instance_id": request.WorkerInstanceId, "worker_instance_epoch": request.WorkerInstanceEpoch,
		"control_session_epoch":         command.ControlSessionEpoch,
		"capacity_observation_sequence": request.CapacityObservationSequence,
		"model_residency_id":            request.ModelResidencyId, "model_runtime_epoch": request.ModelRuntimeEpoch,
		"stage_profile_revision_id": request.StageProfileRevisionId,
		"spiffe_id_digest":          hex.EncodeToString(digest[:]),
	})
	if err != nil {
		t.Fatal(err)
	}
	oldClientBegin := func() []byte {
		t.Helper()
		var wire []byte
		if err := fixture.database.Admin.QueryRow(`SELECT assignment_wire FROM vela_begin_stage_worker_acquire($1::jsonb)`, payload).Scan(&wire); err != nil {
			t.Fatal(err)
		}
		return wire
	}
	if oldClientBegin() != nil {
		t.Fatal("unfinished old-client command already had a wire response")
	}
	rejectLegacyAcquireProduction(t, fixture, backend, capture, command, request)
	assigned := completeLegacyAcquireReplayFixture(t, fixture, command, capture.assignment)
	wire := oldClientBegin()
	for range 2 {
		if err := goose.DownTo(fixture.database.Admin, migrations, 83); err != nil {
			t.Fatal(err)
		}
		replayed, replayErr := backend.AcquireStage(context.Background(), command, request)
		if replayErr != nil || !proto.Equal(replayed.Assignment, assigned) {
			t.Fatalf("schema 83 wire replay after rollback: result=%#v error=%v", replayed, replayErr)
		}
		replayedWire, replayErr := proto.MarshalOptions{Deterministic: true}.Marshal(replayed.Assignment)
		if replayErr != nil || !bytes.Equal(replayedWire, wire) {
			t.Fatalf("schema 83 current Backend changed historical wire bytes: %v", replayErr)
		}
		if got := oldClientBegin(); !bytes.Equal(got, wire) {
			t.Fatal("schema 83 old-client wire response changed")
		}
		if err := goose.UpTo(fixture.database.Admin, migrations, 84); err != nil {
			t.Fatal(err)
		}
		replayed, replayErr = backend.AcquireStage(context.Background(), command, request)
		if replayErr != nil || !proto.Equal(replayed.Assignment, assigned) {
			t.Fatalf("schema 84 wire replay after re-upgrade: result=%#v error=%v", replayed, replayErr)
		}
		replayedWire, replayErr = proto.MarshalOptions{Deterministic: true}.Marshal(replayed.Assignment)
		if replayErr != nil || !bytes.Equal(replayedWire, wire) {
			t.Fatalf("schema 84 current Backend changed historical wire bytes: %v", replayErr)
		}
		if got := oldClientBegin(); !bytes.Equal(got, wire) {
			t.Fatal("schema 84 old-client wire response changed")
		}
	}
}

func TestBlockedStageWorkerProbeMigrationPreservesIdentityRolesAndOriginalCapture(t *testing.T) {
	database := newPostgres(t)
	applyFoundationTo(t, database.Admin, 83)
	migrations := filepath.Join(repositoryRoot(t), "db", "migrations")
	read := func() (string, string) {
		t.Helper()
		var identity, definition string
		if err := database.Admin.QueryRow(`
			SELECT jsonb_agg(jsonb_build_array(oid, proowner, proacl, provolatile) ORDER BY oid)::text,
			       pg_get_functiondef('vela_capture_stage_scheduler_snapshot(jsonb)'::regprocedure)
			FROM pg_proc WHERE oid IN (
				'vela_capture_stage_scheduler_snapshot(jsonb)'::regprocedure,
				'vela_begin_stage_worker_acquire(jsonb)'::regprocedure,
				'vela_stage_worker_acquire_queue_empty(jsonb)'::regprocedure)
		`).Scan(&identity, &definition); err != nil {
			t.Fatal(err)
		}
		return identity, definition
	}
	wantIdentity, wantDefinition := read()
	for range 2 {
		if err := goose.UpTo(database.Admin, migrations, 84); err != nil {
			t.Fatal(err)
		}
		if got, _ := read(); got != wantIdentity {
			t.Fatalf("probe migration changed canonical identity/ACL/volatility: got=%s want=%s", got, wantIdentity)
		}
		var exposed, stable bool
		if err := database.Admin.QueryRow(`
			SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolcanlogin AND NOT rolsuper
			       AND has_function_privilege(oid, 'vela_read_stage_scheduler_snapshot(jsonb)', 'EXECUTE')),
			       (SELECT provolatile = 's' FROM pg_proc
			        WHERE oid = to_regprocedure('vela_read_stage_scheduler_snapshot(jsonb)'))
		`).Scan(&exposed, &stable); err != nil {
			t.Fatal(err)
		}
		if exposed || !stable {
			t.Fatalf("private candidate reader exposed=%t stable=%t", exposed, stable)
		}
		if err := goose.DownTo(database.Admin, migrations, 83); err != nil {
			t.Fatal(err)
		}
		gotIdentity, gotDefinition := read()
		if gotIdentity != wantIdentity || gotDefinition != wantDefinition {
			t.Fatalf("rollback changed canonical identity/ACL/volatility or original capture: identity=%t definition=%t", gotIdentity == wantIdentity, gotDefinition == wantDefinition)
		}
	}
}
