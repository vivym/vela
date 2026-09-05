//go:build integration

package integration_test

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"

	"github.com/pressly/goose/v3"
)

func TestStageAllocationExecutionSequenceMigrationPreservesLegacyAndEntrypoints(t *testing.T) {
	fixture := newStageSchedulerFixture(t, "execution-sequence-migration")
	migrations := filepath.Join(repositoryRoot(t), "db", "migrations")
	if err := goose.DownTo(fixture.database.Admin, migrations, 87); err != nil {
		t.Fatal(err)
	}
	if _, _, err := newStageSchedulerTestService(t, fixture).Acquire(
		context.Background(), fixture.authority, fixture.observation,
	); err != nil {
		t.Fatal(err)
	}
	type functionState struct {
		identity, definition string
	}
	capture := func() map[string]functionState {
		t.Helper()
		rows, err := fixture.database.Admin.Query(`SELECT proname,
			jsonb_build_array(oid, pg_get_userbyid(proowner), proacl::text, prosecdef, proconfig)::text,
			pg_get_functiondef(oid)
			FROM pg_proc WHERE oid IN (
				'public.vela_apply_stage_command(jsonb)'::regprocedure,
				'public.vela_enforce_stage_allocation_authority()'::regprocedure,
				'public.vela_read_stage_assignment_execution_v1(uuid,uuid)'::regprocedure,
				'public.vela_read_stage_authority_snapshot(uuid,bigint)'::regprocedure)
			ORDER BY proname`)
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		states := make(map[string]functionState)
		for rows.Next() {
			var name string
			var state functionState
			if err := rows.Scan(&name, &state.identity, &state.definition); err != nil {
				t.Fatal(err)
			}
			states[name] = state
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		if len(states) != 4 {
			t.Fatalf("migration entrypoint count=%d, want 4", len(states))
		}
		return states
	}
	original := capture()
	var upgraded map[string]functionState
	for range 2 {
		if err := goose.UpTo(fixture.database.Admin, migrations, 88); err != nil {
			t.Fatal(err)
		}
		current := capture()
		for name, before := range original {
			after := current[name]
			if before.identity != after.identity {
				t.Fatalf("%s changed OID, owner, ACL, security or search_path", name)
			}
			changed := name == "vela_enforce_stage_allocation_authority" || name == "vela_read_stage_assignment_execution_v1"
			if (before.definition != after.definition) != changed {
				t.Fatalf("%s body changed=%t, want %t", name, before.definition != after.definition, changed)
			}
			if upgraded != nil && after != upgraded[name] {
				t.Fatalf("%s second Up changed its schema88 definition", name)
			}
		}
		upgraded = current
		var sequence, reader sql.NullInt64
		if err := fixture.database.Admin.QueryRow(`SELECT allocation.execution_sequence,
			vela_read_stage_allocation_execution_sequence(lease.id, allocation.id)
			FROM stage_allocations allocation JOIN stage_leases lease ON lease.stage_allocation_id = allocation.id
			WHERE allocation.stage_run_id = $1`, fixture.stageRunID).Scan(&sequence, &reader); err != nil {
			t.Fatal(err)
		}
		if sequence.Valid || reader.Valid {
			t.Fatalf("legacy V1 allocation acquired fabricated ordering: column=%v reader=%v", sequence, reader)
		}
		var owner, volatility string
		var secure, runtimeCanRead, runtimeCanAllocate, schedulerCanRead bool
		if err := fixture.database.Admin.QueryRow(`SELECT pg_get_userbyid(proowner), provolatile::text, prosecdef,
			has_function_privilege('vela_stage_worker_control_login', oid, 'EXECUTE'),
			has_sequence_privilege('vela_stage_worker_control_login', 'stage_allocation_execution_sequence', 'USAGE'),
			has_function_privilege('vela_stage_scheduler_login', oid, 'EXECUTE')
				FROM pg_proc WHERE pronamespace = 'public'::regnamespace
				AND proname = 'vela_read_stage_allocation_execution_sequence' AND pronargs = 2`).
			Scan(&owner, &volatility, &secure, &runtimeCanRead, &runtimeCanAllocate, &schedulerCanRead); err != nil {
			t.Fatal(err)
		}
		if owner != "vela_attempt_coordinator_owner" || volatility != "s" || !secure ||
			!runtimeCanRead || runtimeCanAllocate || schedulerCanRead {
			t.Fatalf("reader owner=%s volatility=%s definer=%t runtimeRead=%t runtimeSequence=%t schedulerRead=%t",
				owner, volatility, secure, runtimeCanRead, runtimeCanAllocate, schedulerCanRead)
		}
		if err := goose.DownTo(fixture.database.Admin, migrations, 87); err != nil {
			t.Fatal(err)
		}
		for name, restored := range capture() {
			if restored != original[name] {
				t.Fatalf("%s Down did not restore exact identity and definition", name)
			}
		}
	}
}
