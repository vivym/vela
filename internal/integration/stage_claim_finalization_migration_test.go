//go:build integration

package integration_test

import (
	"path/filepath"
	"testing"

	"github.com/pressly/goose/v3"
)

func TestStageClaimFinalizationLockMigrationRoundTripPreservesEntrypoints(t *testing.T) {
	database := newPostgres(t)
	applyFoundationTo(t, database.Admin, 86)
	migrations := filepath.Join(repositoryRoot(t), "db", "migrations")
	type functionState struct {
		identity   string
		definition string
	}
	capture := func() map[string]functionState {
		t.Helper()
		rows, err := database.Admin.Query(`SELECT proname,
			jsonb_build_array(oid, pg_get_userbyid(proowner), proacl::text, prosecdef, proconfig)::text,
			pg_get_functiondef(oid)
			FROM pg_proc WHERE oid IN (
				'public.vela_commit_stage_scheduler_claim(uuid,uuid)'::regprocedure,
				'public.vela_abandon_stage_scheduler_claim(uuid,text)'::regprocedure,
				'public.vela_reconcile_expired_stage_scheduler_claims(integer)'::regprocedure,
				'public.vela_account_stage_scheduler_fairness(uuid,timestamptz)'::regprocedure)
			ORDER BY proname`)
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		result := make(map[string]functionState)
		for rows.Next() {
			var name string
			var state functionState
			if err := rows.Scan(&name, &state.identity, &state.definition); err != nil {
				t.Fatal(err)
			}
			result[name] = state
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		if len(result) != 4 {
			t.Fatalf("claim finalization entrypoint count = %d, want 4", len(result))
		}
		return result
	}
	original := capture()
	var upgraded map[string]functionState
	for range 2 {
		if err := goose.UpTo(database.Admin, migrations, 87); err != nil {
			t.Fatal(err)
		}
		current := capture()
		for name, before := range original {
			after := current[name]
			if after.identity != before.identity || after.definition == before.definition {
				t.Fatalf("%s did not preserve identity/ACL while changing lock order", name)
			}
			if upgraded != nil && after != upgraded[name] {
				t.Fatalf("%s did not restore its schema87 definition exactly", name)
			}
		}
		upgraded = current
		var ownerCanLock, schedulerCanWrite, schedulerCanExecute, schedulerCanAccount bool
		if err := database.Admin.QueryRow(`SELECT
			has_column_privilege('vela_stage_scheduler_owner', 'capacity_pools', 'id', 'UPDATE'),
			has_column_privilege('vela_stage_scheduler_login', 'capacity_pools', 'id', 'UPDATE'),
			has_function_privilege('vela_stage_scheduler_login', 'vela_commit_stage_scheduler_claim(uuid,uuid)', 'EXECUTE'),
			has_function_privilege('vela_stage_scheduler_login', 'vela_account_stage_scheduler_fairness(uuid,timestamptz)', 'EXECUTE')`).
			Scan(&ownerCanLock, &schedulerCanWrite, &schedulerCanExecute, &schedulerCanAccount); err != nil {
			t.Fatal(err)
		}
		if !ownerCanLock || schedulerCanWrite || !schedulerCanExecute || schedulerCanAccount {
			t.Fatalf("claim finalization authority ownerLock=%t runtimeWrite=%t commit=%t fairness=%t",
				ownerCanLock, schedulerCanWrite, schedulerCanExecute, schedulerCanAccount)
		}
		if err := goose.DownTo(database.Admin, migrations, 86); err != nil {
			t.Fatal(err)
		}
		for name, restored := range capture() {
			if restored != original[name] {
				t.Fatalf("%s Down did not restore exact OID, owner, ACL, config and body", name)
			}
		}
	}
}
