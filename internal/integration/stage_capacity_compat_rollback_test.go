//go:build integration

package integration_test

import (
	"path/filepath"
	"testing"

	"github.com/pressly/goose/v3"
)

func TestStageCapacityCompatibilityRollbackRestoresOriginalFunctions(t *testing.T) {
	database := newPostgres(t)
	applyFoundationTo(t, database.Admin, 100)
	migrations := filepath.Join(repositoryRoot(t), "db", "migrations")
	signatures := []string{
		"public.vela_lock_stage_graph_ready_capacity_path(uuid,uuid)",
		"public.vela_capture_stage_scheduler_snapshot(jsonb)",
	}
	capture := func() map[string]string {
		t.Helper()
		result := make(map[string]string)
		for _, signature := range signatures {
			var state string
			if err := database.Admin.QueryRow(`SELECT jsonb_build_array(oid, proowner, proacl::text, prosecdef, proconfig, pg_get_functiondef(oid))::text FROM pg_proc WHERE oid = $1::regprocedure`, signature).Scan(&state); err != nil {
				t.Fatal(err)
			}
			result[signature] = state
		}
		return result
	}
	original := capture()
	// Exercise both the immediate rollback and rollback through later replacements.
	for _, target := range []int64{102, 0} {
		var err error
		if target == 0 {
			err = goose.Up(database.Admin, migrations)
		} else {
			err = goose.UpTo(database.Admin, migrations, target)
		}
		if err != nil {
			t.Fatal(err)
		}
		if err := goose.DownTo(database.Admin, migrations, 100); err != nil {
			t.Fatal(err)
		}
		for signature, state := range capture() {
			if state != original[signature] {
				t.Fatalf("rollback through version %d changed body, identity or privileges of %s", target, signature)
			}
		}
	}
}
