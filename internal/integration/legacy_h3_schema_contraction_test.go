//go:build integration

package integration_test

import (
	"path/filepath"
	"testing"

	"github.com/pressly/goose/v3"
)

func TestLegacyH3SchemaContractionFreshInstallNeedsNoProductionAuthorization(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)

	version, err := goose.GetDBVersion(database.Admin)
	if err != nil || version != 58 {
		t.Fatalf("fresh Stage-only schema version = %d error=%v, want 58", version, err)
	}

	for _, table := range []string{
		"attempt_leases",
		"scheduler_dispatch_intents",
		"scheduler_pool_counters",
		"worker_profile_readiness",
		"worker_epochs",
		"workers",
	} {
		assertTableDoesNotExist(t, database.Admin, table)
	}

	var contracted bool
	if err := database.Admin.QueryRow(`
		SELECT to_regtype('public.execution_authority_kind') IS NULL
			AND NOT EXISTS (
				SELECT 1 FROM information_schema.columns
				WHERE table_schema = 'public' AND table_name = 'attempts'
				  AND column_name IN (
					'execution_authority_kind', 'execution_profile_revision_id',
					'worker_pool_id', 'worker_id', 'worker_epoch', 'assigned_at',
					'profile_certification_id', 'scheduler_dispatch_intent_id'
				  )
			)
			AND NOT EXISTS (
				SELECT 1 FROM information_schema.columns
				WHERE table_schema = 'public' AND table_name = 'jobs'
				  AND column_name IN ('execution_authority_kind', 'worker_pool_id')
			)
	`).Scan(&contracted); err != nil {
		t.Fatalf("inspect fresh Stage-only schema contraction: %v", err)
	}
	if !contracted {
		t.Fatal("fresh install retained Legacy H3 authority columns or discriminator")
	}

	var legacyFunctionCount int
	if err := database.Admin.QueryRow(`
		SELECT count(*)
		FROM (
			VALUES
				('public.vela_cancel_legacy_job(uuid,uuid,uuid,uuid,uuid,uuid,uuid,uuid)'),
				('public.execution_authority_kind(public.jobs)'),
				('public.worker_pool_id(public.jobs)'),
				('public.execution_authority_kind(public.attempts)'),
				('public.execution_profile_revision_id(public.attempts)'),
				('public.worker_pool_id(public.attempts)'),
				('public.worker_id(public.attempts)'),
				('public.worker_epoch(public.attempts)')
		) AS legacy(signature)
		WHERE to_regprocedure(legacy.signature) IS NOT NULL
	`).Scan(&legacyFunctionCount); err != nil {
		t.Fatalf("inspect Legacy H3 compatibility functions: %v", err)
	}
	if legacyFunctionCount != 0 {
		t.Fatalf("fresh install retained %d Legacy H3 compatibility functions", legacyFunctionCount)
	}

	if err := database.Admin.QueryRow(`
		SELECT count(*)
		FROM pg_proc AS function
		JOIN pg_namespace AS namespace ON namespace.oid = function.pronamespace
		WHERE namespace.nspname IN ('public', 'vela_private')
		  AND function.prokind = 'f'
		  AND pg_get_functiondef(function.oid) ~* (
			'execution_authority_kind|worker_pool_id|worker_pools|attempt_leases|'
			|| 'scheduler_dispatch_intent|'
			|| '(^|[^a-z_])(attempt|v_attempt)\\.'
			|| '(execution_profile_revision_id|worker_id|worker_epoch|'
			|| 'profile_certification_id|scheduler_dispatch_intent_id)|'
			|| '(^|[^a-z_])(job|v_job)\\.(execution_authority_kind|worker_pool_id)|'
			|| '(^|[^a-z_])(decision|v_decision)\\.execution_authority_kind'
		  )
	`).Scan(&legacyFunctionCount); err != nil {
		t.Fatalf("scan retained function definitions for Legacy H3 authority: %v", err)
	}
	if legacyFunctionCount != 0 {
		t.Fatalf("fresh install retained %d functions with Legacy H3 authority dependencies", legacyFunctionCount)
	}

	migrations := filepath.Join(repositoryRoot(t), "db", "migrations")
	err = goose.DownTo(database.Admin, migrations, 57)
	assertPostgresConstraint(t, err, "legacy_h3_schema_contraction_is_irreversible")
	version, versionErr := goose.GetDBVersion(database.Admin)
	if versionErr != nil || version != 58 {
		t.Fatalf("schema version after refused contraction Down = %d error=%v", version, versionErr)
	}
}
