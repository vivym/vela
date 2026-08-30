//go:build integration

package integration_test

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"
	"time"

	"github.com/pressly/goose/v3"
	veladb "github.com/vivym/vela/internal/database"
)

const (
	stageGraphID          = "49000000-0000-0000-0000-000000000001"
	requestInterfaceID    = "49000000-0000-0000-0000-000000000010"
	conditioningInterface = "49000000-0000-0000-0000-000000000011"
	latentInterface       = "49000000-0000-0000-0000-000000000012"
	videoInterface        = "49000000-0000-0000-0000-000000000013"
)

func TestStageExecutionCatalogActivationAndImmutability(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	seedAdmissionFixture(t, database.Admin)
	seedStageExecutionCatalog(t, database.Admin)
	activation := newRolePool(
		t,
		database.DSN,
		"vela_stage_catalog_activation_login",
		"vela-stage-catalog-activation-password",
	)
	var graphDigest []byte
	if err := database.Admin.QueryRow(`
		SELECT content_digest FROM execution_graph_revisions WHERE id = $1
	`, stageGraphID).Scan(&graphDigest); err != nil {
		t.Fatalf("read H3 graph digest: %v", err)
	}

	var state string
	var topologicalOrder string
	err := activation.QueryRow(context.Background(), `
		SELECT state::text, topological_order::text
		FROM vela_activate_execution_graph(
			$1, $2
		)
	`, stageGraphID, graphDigest).Scan(&state, &topologicalOrder)
	assertPostgresConstraint(t, err, "execution_graph_connector_fallback_missing")
	if _, err := activation.Exec(context.Background(), `
		UPDATE connector_revisions SET state = 'CERTIFIED'
	`); !isPostgresCode(err, "42501") {
		t.Fatalf("Stage Catalog activation direct mutation error = %v, want SQLSTATE 42501", err)
	}

	if _, err := database.Admin.Exec(`
		UPDATE connector_revisions
		SET state = 'CERTIFIED'
		WHERE stable_id IN ('h3-conditioning-l2', 'h3-latent-l2')
	`); err != nil {
		t.Fatalf("certify H3 connector revisions: %v", err)
	}
	if err := activation.QueryRow(context.Background(), `
		SELECT state::text, topological_order::text
		FROM vela_activate_execution_graph(
			$1, $2
		)
	`, stageGraphID, graphDigest).Scan(&state, &topologicalOrder); err != nil {
		t.Fatalf("activate H3 execution graph: %v", err)
	}
	if state != "ACTIVE" || topologicalOrder != "{encoder,dit,vae}" {
		t.Fatalf("activated H3 graph state/order = %q/%v", state, topologicalOrder)
	}

	_, err = database.Admin.Exec(`
		UPDATE execution_graph_revisions
		SET schema_version = 2
		WHERE id = $1
	`, stageGraphID)
	assertPostgresConstraint(t, err, "stage_catalog_revision_is_immutable")

	_, err = database.Admin.Exec(`
		UPDATE execution_graph_stages
		SET max_fan_out = 2
		WHERE execution_graph_revision_id = $1 AND stage_key = 'encoder'
	`, stageGraphID)
	assertPostgresConstraint(t, err, "active_execution_graph_is_immutable")

	_, err = database.Admin.Exec(`
		INSERT INTO execution_profile_stage_options (
			execution_profile_revision_id, execution_graph_revision_id, stage_key,
			stage_definition_revision_id, stage_profile_revision_id,
			preference, eligibility_metadata
		) VALUES (
			'00000000-0000-0000-0000-000000000014', $1, 'encoder',
			'49000000-0000-0000-0000-000000000030',
			'49000000-0000-0000-0000-000000000040', 0, '{}'
		)
	`, stageGraphID)
	assertPostgresConstraint(t, err, "active_execution_graph_is_immutable")
}

func TestStageExecutionCatalogExecutionProfileAuthorityShape(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	seedAdmissionFixture(t, database.Admin)
	seedStageExecutionCatalog(t, database.Admin)

	if _, err := database.Admin.Exec(`
		INSERT INTO execution_profile_revisions (
			id, model_revision_id, worker_pool_id, execution_graph_revision_id,
			stable_id, revision, state
		) VALUES (
			'49000000-0000-0000-0000-000000000071',
			'00000000-0000-0000-0000-000000000010', NULL, $1,
			'h3-stage-graph-alt', 1, 'CERTIFIED'
		)
	`, stageGraphID); err != nil {
		t.Fatalf("insert graph-only ExecutionProfile: %v", err)
	}

	for _, test := range []struct {
		name       string
		workerPool any
		graph      any
	}{
		{name: "neither authority", workerPool: nil, graph: nil},
		{
			name:       "mixed authority",
			workerPool: "00000000-0000-0000-0000-000000000005",
			graph:      stageGraphID,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := database.Admin.Exec(`
				INSERT INTO execution_profile_revisions (
					id, model_revision_id, worker_pool_id, execution_graph_revision_id,
					stable_id, revision, state
				) VALUES (
					gen_random_uuid(), '00000000-0000-0000-0000-000000000010', $1, $2,
					$3, 1, 'CERTIFIED'
				)
			`, test.workerPool, test.graph, "invalid-"+test.name)
			assertPostgresConstraint(t, err, "execution_profile_revisions_authority_shape")
		})
	}
}

func TestStageExecutionCatalogProfileOptionsCannotCrossGraphs(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	seedAdmissionFixture(t, database.Admin)
	seedStageExecutionCatalog(t, database.Admin)

	if _, err := database.Admin.Exec(`
		INSERT INTO execution_profile_revisions (
			id, model_revision_id, worker_pool_id, execution_graph_revision_id,
			stable_id, revision, state
		) VALUES (
			'49000000-0000-0000-0000-000000000072',
			'00000000-0000-0000-0000-000000000010', NULL, $1,
			'h3-stage-graph-cross-check', 1, 'CERTIFIED'
		)
	`, stageGraphID); err != nil {
		t.Fatalf("seed cross-graph ExecutionProfile: %v", err)
	}
	if _, err := database.Admin.Exec(`
		INSERT INTO execution_graph_revisions (
			id, model_revision_id, stable_id, revision, schema_version, state,
			final_output_contract, public_phase_map, content_digest
		) VALUES (
			'49000000-0000-0000-0000-000000000002',
			'00000000-0000-0000-0000-000000000010', 'minimax-h3-other-graph', 1, 1,
			'CERTIFIED', '{"output_key":"latent"}', '{}', decode(repeat('92', 32), 'hex')
		);
		INSERT INTO execution_graph_stages (
			execution_graph_revision_id, stage_key, stage_definition_revision_id,
			required, max_fan_out
		) VALUES
			('49000000-0000-0000-0000-000000000002', 'encoder-copy',
			 '49000000-0000-0000-0000-000000000030', true, 1),
			('49000000-0000-0000-0000-000000000002', 'dit-copy',
			 '49000000-0000-0000-0000-000000000031', true, 1);
		INSERT INTO execution_graph_edges (
			id, execution_graph_revision_id, source_stage_key, source_port,
			destination_stage_key, destination_port, buffer_class
		) VALUES (
			'49000000-0000-0000-0000-000000000064',
			'49000000-0000-0000-0000-000000000002',
			'encoder-copy', 'conditioning', 'dit-copy', 'conditioning', 'L2_DURABLE'
		)
	`); err != nil {
		t.Fatalf("seed cross-graph option fixture: %v", err)
	}

	t.Run("StageProfile option", func(t *testing.T) {
		_, err := database.Admin.Exec(`
			INSERT INTO execution_profile_stage_options (
				execution_profile_revision_id, execution_graph_revision_id, stage_key,
				stage_definition_revision_id, stage_profile_revision_id,
				preference, eligibility_metadata
			) VALUES (
				'49000000-0000-0000-0000-000000000072',
				'49000000-0000-0000-0000-000000000002', 'encoder-copy',
				'49000000-0000-0000-0000-000000000030',
				'49000000-0000-0000-0000-000000000040', 0, '{}'
			)
		`)
		assertPostgresConstraint(t, err, "execution_profile_stage_options_profile_graph")
	})

	t.Run("Connector option", func(t *testing.T) {
		_, err := database.Admin.Exec(`
			INSERT INTO execution_profile_connector_options (
				execution_profile_revision_id, execution_graph_revision_id,
				execution_graph_edge_id, connector_revision_id,
				required_topology_policy, preference
			) VALUES (
				'49000000-0000-0000-0000-000000000072',
				'49000000-0000-0000-0000-000000000002',
				'49000000-0000-0000-0000-000000000064',
				'49000000-0000-0000-0000-000000000050', '{}', 0
			)
		`)
		assertPostgresConstraint(t, err, "execution_profile_connector_options_profile_graph")
	})
}

func TestStageExecutionCatalogStageOptionDefinitionMustMatch(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	seedAdmissionFixture(t, database.Admin)
	seedStageExecutionCatalog(t, database.Admin)

	if _, err := database.Admin.Exec(`
		INSERT INTO execution_profile_revisions (
			id, model_revision_id, worker_pool_id, execution_graph_revision_id,
			stable_id, revision, state
		) VALUES (
			'49000000-0000-0000-0000-000000000073',
			'00000000-0000-0000-0000-000000000010', NULL, $1,
			'h3-stage-definition-check', 1, 'CERTIFIED'
		)
	`, stageGraphID); err != nil {
		t.Fatalf("insert graph-only ExecutionProfile: %v", err)
	}

	_, err := database.Admin.Exec(`
		INSERT INTO execution_profile_stage_options (
			execution_profile_revision_id, execution_graph_revision_id, stage_key,
			stage_definition_revision_id, stage_profile_revision_id,
			preference, eligibility_metadata
		) VALUES (
			'49000000-0000-0000-0000-000000000073', $1, 'encoder',
			'49000000-0000-0000-0000-000000000030',
			'49000000-0000-0000-0000-000000000041', 0, '{}'
		)
	`, stageGraphID)
	assertPostgresConstraint(t, err, "execution_profile_stage_options_profile_definition")
}

func TestStageExecutionCatalogActivationRequiresCompleteProfileOptions(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate string
	}{
		{
			name: "certified ExecutionProfile",
			mutate: `UPDATE execution_profile_revisions
				SET state = 'REGISTERED'
				WHERE id = '49000000-0000-0000-0000-000000000070'`,
		},
		{
			name: "every Stage option",
			mutate: `DELETE FROM execution_profile_stage_options
				WHERE execution_profile_revision_id = '49000000-0000-0000-0000-000000000070'
				  AND stage_key = 'dit'`,
		},
		{
			name: "every Connector option",
			mutate: `DELETE FROM execution_profile_connector_options
				WHERE execution_profile_revision_id = '49000000-0000-0000-0000-000000000070'
				  AND execution_graph_edge_id = '49000000-0000-0000-0000-000000000060'`,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			database := newPostgres(t)
			applyFoundation(t, database.Admin)
			seedAdmissionFixture(t, database.Admin)
			seedStageExecutionCatalog(t, database.Admin)
			if _, err := database.Admin.Exec(`
				UPDATE connector_revisions SET state = 'CERTIFIED';
			`); err != nil {
				t.Fatalf("certify Connector fixtures: %v", err)
			}
			if _, err := database.Admin.Exec(test.mutate); err != nil {
				t.Fatalf("remove complete ExecutionProfile option: %v", err)
			}
			_, err := database.Admin.Exec(`
				SELECT * FROM vela_activate_execution_graph(
					$1, (SELECT content_digest FROM execution_graph_revisions WHERE id = $1)
				)
			`, stageGraphID)
			assertPostgresConstraint(t, err, "execution_graph_profile_options_incomplete")
		})
	}
}

func TestStageExecutionCatalogActivationRejectsStaleStoredDigest(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	seedAdmissionFixture(t, database.Admin)
	seedStageExecutionCatalog(t, database.Admin)
	if _, err := database.Admin.Exec(`
		UPDATE connector_revisions SET state = 'CERTIFIED'
	`); err != nil {
		t.Fatalf("certify Connector fixtures: %v", err)
	}

	var storedDigest []byte
	if err := database.Admin.QueryRow(`
		SELECT content_digest FROM execution_graph_revisions WHERE id = $1
	`, stageGraphID).Scan(&storedDigest); err != nil {
		t.Fatalf("read stored graph digest: %v", err)
	}
	if _, err := database.Admin.Exec(`
		UPDATE execution_graph_stages
		SET max_fan_out = 2
		WHERE execution_graph_revision_id = $1 AND stage_key = 'encoder'
	`, stageGraphID); err != nil {
		t.Fatalf("mutate graph after stored digest: %v", err)
	}

	_, err := database.Admin.Exec(`
		SELECT * FROM vela_activate_execution_graph($1, $2)
	`, stageGraphID, storedDigest)
	assertPostgresConstraint(t, err, "execution_graph_content_digest_mismatch")
}

func TestStageExecutionCatalogActiveGraphDependenciesCannotBeDemoted(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	seedAdmissionFixture(t, database.Admin)
	seedStageExecutionCatalog(t, database.Admin)
	if _, err := database.Admin.Exec(`
		UPDATE connector_revisions SET state = 'CERTIFIED'
	`); err != nil {
		t.Fatalf("certify Connector fixtures: %v", err)
	}
	if _, err := database.Admin.Exec(`
		SELECT * FROM vela_activate_execution_graph(
			$1, (SELECT content_digest FROM execution_graph_revisions WHERE id = $1)
		)
	`, stageGraphID); err != nil {
		t.Fatalf("activate H3 graph: %v", err)
	}

	for _, test := range []struct {
		name   string
		mutate string
	}{
		{name: "StageInterface", mutate: `UPDATE stage_interface_revisions SET state = 'REGISTERED' WHERE id = '` + latentInterface + `'`},
		{name: "StageDefinition", mutate: `UPDATE stage_definition_revisions SET state = 'REGISTERED' WHERE id = '49000000-0000-0000-0000-000000000031'`},
		{name: "StageProfile", mutate: `UPDATE stage_profile_revisions SET state = 'REGISTERED' WHERE id = '49000000-0000-0000-0000-000000000041'`},
		{name: "WorkerProfile", mutate: `UPDATE worker_profile_revisions SET state = 'REGISTERED' WHERE id = '49000000-0000-0000-0000-000000000022'`},
		{name: "ResultEquivalence", mutate: `UPDATE stage_result_equivalence_revisions SET state = 'REGISTERED' WHERE id = '49000000-0000-0000-0000-000000000024'`},
		{name: "CachePolicy", mutate: `UPDATE stage_cache_policy_revisions SET state = 'REGISTERED' WHERE id = '49000000-0000-0000-0000-000000000020'`},
		{name: "CheckpointPolicy", mutate: `UPDATE checkpoint_policy_revisions SET state = 'REGISTERED' WHERE id = '49000000-0000-0000-0000-000000000021'`},
		{name: "Connector", mutate: `UPDATE connector_revisions SET state = 'REGISTERED' WHERE id = '49000000-0000-0000-0000-000000000051'`},
		{name: "ExecutionProfile", mutate: `UPDATE execution_profile_revisions SET state = 'REGISTERED' WHERE id = '49000000-0000-0000-0000-000000000070'`},
	} {
		t.Run(test.name, func(t *testing.T) {
			tx, err := database.Admin.Begin()
			if err != nil {
				t.Fatalf("begin dependency demotion: %v", err)
			}
			defer func() { _ = tx.Rollback() }()
			_, err = tx.Exec(test.mutate)
			assertPostgresConstraint(t, err, "active_execution_graph_dependency_in_use")
		})
	}
}

func TestStageExecutionCatalogActiveGraphOptionsAreImmutable(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	seedAdmissionFixture(t, database.Admin)
	seedStageExecutionCatalog(t, database.Admin)
	if _, err := database.Admin.Exec(`
		UPDATE connector_revisions SET state = 'CERTIFIED'
	`); err != nil {
		t.Fatalf("certify Connector fixtures: %v", err)
	}
	if _, err := database.Admin.Exec(`
		SELECT * FROM vela_activate_execution_graph(
			$1, (SELECT content_digest FROM execution_graph_revisions WHERE id = $1)
		)
	`, stageGraphID); err != nil {
		t.Fatalf("activate H3 graph: %v", err)
	}

	for _, test := range []struct {
		name   string
		mutate string
	}{
		{
			name: "StageProfile option",
			mutate: `DELETE FROM execution_profile_stage_options
				WHERE execution_profile_revision_id = '49000000-0000-0000-0000-000000000070'
				  AND stage_key = 'dit'`,
		},
		{
			name: "Connector option",
			mutate: `DELETE FROM execution_profile_connector_options
				WHERE execution_profile_revision_id = '49000000-0000-0000-0000-000000000070'
				  AND execution_graph_edge_id = '49000000-0000-0000-0000-000000000060'`,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := database.Admin.Exec(test.mutate)
			assertPostgresConstraint(t, err, "active_execution_graph_is_immutable")
		})
	}
}

func TestStageExecutionCatalogActivationWaitsForConcurrentCatalogMutation(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	seedAdmissionFixture(t, database.Admin)
	seedStageExecutionCatalog(t, database.Admin)
	if _, err := database.Admin.Exec(`
		UPDATE connector_revisions SET state = 'CERTIFIED'
	`); err != nil {
		t.Fatalf("certify Connector fixtures: %v", err)
	}
	var graphDigest []byte
	if err := database.Admin.QueryRow(`
		SELECT content_digest FROM execution_graph_revisions WHERE id = $1
	`, stageGraphID).Scan(&graphDigest); err != nil {
		t.Fatalf("read H3 graph digest: %v", err)
	}

	mutation, err := database.Admin.Begin()
	if err != nil {
		t.Fatalf("begin Catalog mutation: %v", err)
	}
	defer func() { _ = mutation.Rollback() }()
	if _, err := mutation.Exec(`
		UPDATE execution_graph_stages
		SET max_fan_out = 2
		WHERE execution_graph_revision_id = $1 AND stage_key = 'encoder'
	`, stageGraphID); err != nil {
		t.Fatalf("hold concurrent graph mutation: %v", err)
	}

	activationConnection, err := database.Admin.Conn(context.Background())
	if err != nil {
		t.Fatalf("reserve activation connection: %v", err)
	}
	defer activationConnection.Close()
	var activationPID int
	if err := activationConnection.QueryRowContext(
		context.Background(), `SELECT pg_backend_pid()`,
	).Scan(&activationPID); err != nil {
		t.Fatalf("read activation backend PID: %v", err)
	}
	activationResult := make(chan error, 1)
	go func() {
		_, activationErr := activationConnection.ExecContext(context.Background(), `
			SELECT * FROM vela_activate_execution_graph($1, $2)
		`, stageGraphID, graphDigest)
		activationResult <- activationErr
	}()

	deadline := time.Now().Add(5 * time.Second)
	observedLockWait := false
	for time.Now().Before(deadline) {
		select {
		case activationErr := <-activationResult:
			t.Fatalf("activation completed before concurrent mutation ended: %v", activationErr)
		default:
		}
		var waitEventType sql.NullString
		if err := database.Admin.QueryRow(`
			SELECT wait_event_type FROM pg_stat_activity WHERE pid = $1
		`, activationPID).Scan(&waitEventType); err != nil {
			t.Fatalf("inspect activation lock wait: %v", err)
		}
		if waitEventType.Valid && waitEventType.String == "Lock" {
			observedLockWait = true
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !observedLockWait {
		t.Fatal("activation did not wait on a Catalog lock")
	}
	if err := mutation.Rollback(); err != nil {
		t.Fatalf("release concurrent graph mutation: %v", err)
	}
	if err := <-activationResult; err != nil {
		t.Fatalf("activate after concurrent mutation rolled back: %v", err)
	}
}

func TestStageExecutionCatalogActivationRejectsMalformedGraphs(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	seedAdmissionFixture(t, database.Admin)
	seedStageExecutionCatalog(t, database.Admin)
	if _, err := database.Admin.Exec(`
		UPDATE connector_revisions
		SET state = 'CERTIFIED'
	`); err != nil {
		t.Fatalf("certify connector fixtures: %v", err)
	}

	for _, test := range []struct {
		name       string
		constraint string
		mutate     string
		restore    string
	}{
		{
			name:       "required input",
			constraint: "execution_graph_required_input_missing",
			mutate: `DELETE FROM execution_graph_inputs
				WHERE execution_graph_revision_id = '49000000-0000-0000-0000-000000000001'`,
			restore: `INSERT INTO execution_graph_inputs (
				execution_graph_revision_id, input_key, interface_revision_id,
				destination_stage_key, destination_port
			) VALUES (
				'49000000-0000-0000-0000-000000000001', 'request',
				'49000000-0000-0000-0000-000000000010', 'encoder', 'request'
			)`,
		},
		{
			name:       "boundary interface",
			constraint: "execution_graph_boundary_interface_incompatible",
			mutate: `UPDATE execution_graph_outputs
				SET interface_revision_id = '49000000-0000-0000-0000-000000000012'
				WHERE execution_graph_revision_id = '49000000-0000-0000-0000-000000000001'`,
			restore: `UPDATE execution_graph_outputs
				SET interface_revision_id = '49000000-0000-0000-0000-000000000013'
				WHERE execution_graph_revision_id = '49000000-0000-0000-0000-000000000001'`,
		},
		{
			name:       "profile certification",
			constraint: "execution_graph_stage_certification_incomplete",
			mutate: `UPDATE stage_profile_revisions SET state = 'REGISTERED'
				WHERE stable_id = 'h3-dit-single-gpu'`,
			restore: `UPDATE stage_profile_revisions SET state = 'CERTIFIED'
				WHERE stable_id = 'h3-dit-single-gpu'`,
		},
		{
			name:       "definition certification",
			constraint: "execution_graph_stage_certification_incomplete",
			mutate: `UPDATE stage_definition_revisions SET state = 'REGISTERED'
				WHERE stable_id = 'h3-dit'`,
			restore: `UPDATE stage_definition_revisions SET state = 'CERTIFIED'
				WHERE stable_id = 'h3-dit'`,
		},
		{
			name:       "interface certification",
			constraint: "execution_graph_interface_certification_incomplete",
			mutate: `UPDATE stage_interface_revisions SET state = 'REGISTERED'
				WHERE stable_id = 'h3-latent'`,
			restore: `UPDATE stage_interface_revisions SET state = 'CERTIFIED'
				WHERE stable_id = 'h3-latent'`,
		},
		{
			name:       "fan-out",
			constraint: "execution_graph_fan_out_exceeded",
			mutate: `
				INSERT INTO execution_graph_stages (
					execution_graph_revision_id, stage_key, stage_definition_revision_id,
					required, max_fan_out
				) VALUES (
					'49000000-0000-0000-0000-000000000001', 'dit-shadow',
					'49000000-0000-0000-0000-000000000031', false, 1
				);
				INSERT INTO execution_graph_edges (
					id, execution_graph_revision_id, source_stage_key, source_port,
					destination_stage_key, destination_port, buffer_class
				) VALUES (
					'49000000-0000-0000-0000-000000000062',
					'49000000-0000-0000-0000-000000000001',
					'encoder', 'conditioning', 'dit-shadow', 'conditioning', 'L2_DURABLE'
				);
				INSERT INTO execution_graph_outputs (
					execution_graph_revision_id, output_key, interface_revision_id,
					source_stage_key, source_port, required
				) VALUES (
					'49000000-0000-0000-0000-000000000001', 'shadow-latent',
					'49000000-0000-0000-0000-000000000012', 'dit-shadow', 'latent', false
				)`,
			restore: `
				DELETE FROM execution_graph_outputs WHERE output_key = 'shadow-latent';
				DELETE FROM execution_graph_edges WHERE id = '49000000-0000-0000-0000-000000000062';
				DELETE FROM execution_graph_stages WHERE stage_key = 'dit-shadow'`,
		},
		{
			name:       "cycle",
			constraint: "execution_graph_cycle",
			mutate: `
				INSERT INTO execution_graph_edges (
					id, execution_graph_revision_id, source_stage_key, source_port,
					destination_stage_key, destination_port, buffer_class
				) VALUES (
					'49000000-0000-0000-0000-000000000063',
					'49000000-0000-0000-0000-000000000001',
					'vae', 'cycle', 'encoder', 'cycle', 'L2_DURABLE'
				);
				INSERT INTO execution_profile_connector_options (
					execution_profile_revision_id, execution_graph_revision_id,
					execution_graph_edge_id, connector_revision_id,
					required_topology_policy, preference
				) VALUES (
					'49000000-0000-0000-0000-000000000070',
					'49000000-0000-0000-0000-000000000001',
					'49000000-0000-0000-0000-000000000063',
					'49000000-0000-0000-0000-000000000052', '{}', 0
				)`,
			restore: `
				DELETE FROM execution_profile_connector_options
				WHERE execution_graph_edge_id = '49000000-0000-0000-0000-000000000063';
				DELETE FROM execution_graph_edges
				WHERE id = '49000000-0000-0000-0000-000000000063'`,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := database.Admin.Exec(test.mutate); err != nil {
				t.Fatalf("mutate graph fixture: %v", err)
			}
			if _, err := database.Admin.Exec(`
				UPDATE execution_graph_revisions
				SET content_digest = vela_execution_graph_content_digest(id)
				WHERE id = $1
			`, stageGraphID); err != nil {
				t.Fatalf("seal mutated graph digest: %v", err)
			}
			_, err := database.Admin.Exec(`
				SELECT * FROM vela_activate_execution_graph(
					$1, (SELECT content_digest FROM execution_graph_revisions WHERE id = $1)
				)
			`, stageGraphID)
			assertPostgresConstraint(t, err, test.constraint)
			if _, err := database.Admin.Exec(test.restore); err != nil {
				t.Fatalf("restore graph fixture: %v", err)
			}
			if _, err := database.Admin.Exec(`
				UPDATE execution_graph_revisions
				SET content_digest = vela_execution_graph_content_digest(id)
				WHERE id = $1
			`, stageGraphID); err != nil {
				t.Fatalf("seal restored graph digest: %v", err)
			}
		})
	}
}

func TestStageExecutionCatalogMigrationEmptyDownUpAndAuthorityRefusal(t *testing.T) {
	migrations := filepath.Join(repositoryRoot(t), "db", "migrations")
	t.Run("empty Down Up", func(t *testing.T) {
		database := newPostgres(t)
		applyFoundation(t, database.Admin)
		if err := goose.DownTo(database.Admin, migrations, 33); err != nil {
			t.Fatalf("migrate empty Stage Execution Catalog down: %v", err)
		}
		for _, table := range []string{
			"execution_graph_revisions",
			"execution_graph_stages",
			"stage_interface_revisions",
			"stage_definition_revisions",
			"stage_profile_revisions",
			"connector_revisions",
			"worker_profile_revisions",
			"cost_model_revisions",
		} {
			assertTableDoesNotExist(t, database.Admin, table)
		}
		var graphColumnExists bool
		if err := database.Admin.QueryRow(`
			SELECT EXISTS (
				SELECT 1 FROM information_schema.columns
				WHERE table_schema = 'public'
				  AND table_name = 'execution_profile_revisions'
				  AND column_name = 'execution_graph_revision_id'
			)
		`).Scan(&graphColumnExists); err != nil || graphColumnExists {
			t.Fatalf("stage graph profile column after Down exists=%t error=%v", graphColumnExists, err)
		}
		if err := goose.UpTo(database.Admin, migrations, 34); err != nil {
			t.Fatalf("migrate Stage Execution Catalog up: %v", err)
		}
		version, err := goose.GetDBVersion(database.Admin)
		if err != nil || version != 34 {
			t.Fatalf("Stage Execution Catalog version after Down Up = %d error=%v", version, err)
		}
		activation := newRolePool(
			t,
			database.DSN,
			"vela_stage_catalog_activation_login",
			"vela-stage-catalog-activation-password",
		)
		if err := veladb.VerifyRole(
			context.Background(), activation, veladb.RoleStageCatalogActivation,
		); err != nil {
			t.Fatalf("verify Stage Catalog activation role after Down Up: %v", err)
		}
	})

	t.Run("Catalog authority refuses Down", func(t *testing.T) {
		database := newPostgres(t)
		applyFoundation(t, database.Admin)
		seedAdmissionFixture(t, database.Admin)
		seedStageExecutionCatalog(t, database.Admin)
		err := goose.DownTo(database.Admin, migrations, 33)
		assertPostgresConstraint(t, err, "stage_execution_catalog_rollback_is_unsafe")
		version, versionErr := goose.GetDBVersion(database.Admin)
		if versionErr != nil || version != 34 {
			t.Fatalf("Stage Execution Catalog version after refused Down = %d error=%v", version, versionErr)
		}
	})
}

func TestStageExecutionCatalogDownWaitsForConcurrentCatalogInsert(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	migrations := filepath.Join(repositoryRoot(t), "db", "migrations")

	mutation, err := database.Admin.Begin()
	if err != nil {
		t.Fatalf("begin concurrent Catalog insert: %v", err)
	}
	defer func() { _ = mutation.Rollback() }()
	if _, err := mutation.Exec(`
		INSERT INTO stage_interface_revisions (
			id, stable_id, revision, state, payload_kind, dtype, layout,
			shape_contract, serialization, max_bytes, digest_algorithm,
			schema_digest, content_digest
		) VALUES (
			'49000000-0000-0000-0000-000000000099', 'concurrent-interface', 1,
			'REGISTERED', 'tensor', 'bf16', 'test', '{}', 'safetensors', 1024,
			'sha256', decode(repeat('98', 32), 'hex'), decode(repeat('99', 32), 'hex')
		)
	`); err != nil {
		t.Fatalf("hold concurrent Catalog insert: %v", err)
	}

	downResult := make(chan error, 1)
	go func() {
		downResult <- goose.DownTo(database.Admin, migrations, 33)
	}()
	select {
	case downErr := <-downResult:
		t.Fatalf("Stage Catalog Down completed before concurrent insert ended: %v", downErr)
	case <-time.After(200 * time.Millisecond):
	}
	if err := mutation.Commit(); err != nil {
		t.Fatalf("commit concurrent Catalog insert: %v", err)
	}
	downErr := <-downResult
	assertPostgresConstraint(t, downErr, "stage_execution_catalog_rollback_is_unsafe")
	version, versionErr := goose.GetDBVersion(database.Admin)
	if versionErr != nil || version != 34 {
		t.Fatalf("Stage Catalog version after concurrent refused Down = %d error=%v", version, versionErr)
	}
}

func seedStageExecutionCatalog(t *testing.T, database *sql.DB) {
	t.Helper()
	if _, err := database.Exec(`
		INSERT INTO stage_interface_revisions (
			id, stable_id, revision, state, payload_kind, dtype, layout,
			shape_contract, serialization, max_bytes, digest_algorithm,
			schema_digest, content_digest
		) VALUES
			('49000000-0000-0000-0000-000000000010', 'h3-request', 1, 'CERTIFIED',
			 'request', '', '', '{}', 'json', 1048576, 'sha256',
			 decode(repeat('10', 32), 'hex'), decode(repeat('20', 32), 'hex')),
			('49000000-0000-0000-0000-000000000011', 'h3-conditioning', 1, 'CERTIFIED',
			 'tensor', 'bf16', 'h3-conditioning', '{}', 'safetensors', 67108864, 'sha256',
			 decode(repeat('11', 32), 'hex'), decode(repeat('21', 32), 'hex')),
			('49000000-0000-0000-0000-000000000012', 'h3-latent', 1, 'CERTIFIED',
			 'tensor', 'bf16', 'h3-latent', '{}', 'safetensors', 1073741824, 'sha256',
			 decode(repeat('12', 32), 'hex'), decode(repeat('22', 32), 'hex')),
			('49000000-0000-0000-0000-000000000013', 'h3-video', 1, 'CERTIFIED',
			 'video', '', 'frames-rgb', '{}', 'frame-bundle', 8589934592, 'sha256',
			 decode(repeat('13', 32), 'hex'), decode(repeat('23', 32), 'hex'));

		INSERT INTO stage_cache_policy_revisions (
			id, stable_id, revision, state, allowed_stage_keys, scope_ceiling,
			ttl_seconds, quota_policy, encryption_policy, deletion_policy, content_digest
		) VALUES (
			'49000000-0000-0000-0000-000000000020', 'h3-exact-cache', 1, 'CERTIFIED',
			ARRAY['encoder', 'dit', 'vae'], 'PROJECT', 86400, '{}', '{}', '{}',
			decode(repeat('30', 32), 'hex')
		);
		INSERT INTO checkpoint_policy_revisions (
			id, stable_id, revision, state, resume_format, compatibility_contract,
			interval_policy, max_overhead_ppm, evidence_digest, content_digest
		) VALUES (
			'49000000-0000-0000-0000-000000000021', 'h3-no-checkpoint', 1, 'CERTIFIED',
			'none', '{}', '{}', 0, decode(repeat('31', 32), 'hex'),
			decode(repeat('32', 32), 'hex')
		);
		INSERT INTO worker_profile_revisions (
			id, stable_id, revision, state, device_count, member_count,
			device_set_shape, resident_model_revisions, capacity_limits,
			readiness_checks, content_digest
		) VALUES (
			'49000000-0000-0000-0000-000000000022', 'h3-single-gpu', 1, 'CERTIFIED',
			1, 1, '{"kind":"single-gpu"}', '["h3-component-v1"]',
			'{"concurrency":1}', '{"warmup":true}', decode(repeat('33', 32), 'hex')
		);
		INSERT INTO stage_result_equivalence_revisions (
			id, stable_id, revision, state, exact_contract, evidence_receipt_ref,
			evidence_digest, content_digest
		) VALUES
			('49000000-0000-0000-0000-000000000023', 'h3-encoder-exact', 1, 'CERTIFIED',
			 '{"precision":"bf16","rng":"exact"}', 'receipt://h3/encoder',
			 decode(repeat('34', 32), 'hex'), decode(repeat('35', 32), 'hex')),
			('49000000-0000-0000-0000-000000000024', 'h3-dit-exact', 1, 'CERTIFIED',
			 '{"precision":"bf16","rng":"exact"}', 'receipt://h3/dit',
			 decode(repeat('36', 32), 'hex'), decode(repeat('37', 32), 'hex')),
			('49000000-0000-0000-0000-000000000025', 'h3-vae-exact', 1, 'CERTIFIED',
			 '{"precision":"bf16","rng":"exact"}', 'receipt://h3/vae',
			 decode(repeat('38', 32), 'hex'), decode(repeat('39', 32), 'hex'));

		INSERT INTO stage_definition_revisions (
			id, stable_id, revision, state, stage_kind, input_ports, output_ports,
			required_input_ports, required_output_ports, resource_class, retry_class,
			cache_policy_revision_id, checkpoint_policy_revision_id, public_phase,
			content_digest
		) VALUES
			('49000000-0000-0000-0000-000000000030', 'h3-encoder', 1, 'CERTIFIED',
			 'ENCODER', jsonb_build_object(
				'request', '49000000-0000-0000-0000-000000000010',
				'cycle', '49000000-0000-0000-0000-000000000010'
			 ),
			 jsonb_build_object('conditioning', '49000000-0000-0000-0000-000000000011'),
			 ARRAY['request'], ARRAY['conditioning'], 'GPU', 'STAGE_RETRY',
			 '49000000-0000-0000-0000-000000000020', '49000000-0000-0000-0000-000000000021',
			 'PREPARING', decode(repeat('40', 32), 'hex')),
			('49000000-0000-0000-0000-000000000031', 'h3-dit', 1, 'CERTIFIED',
			 'DIT', jsonb_build_object('conditioning', '49000000-0000-0000-0000-000000000011'),
			 jsonb_build_object('latent', '49000000-0000-0000-0000-000000000012'),
			 ARRAY['conditioning'], ARRAY['latent'], 'GPU', 'STAGE_RETRY',
			 '49000000-0000-0000-0000-000000000020', '49000000-0000-0000-0000-000000000021',
			 'GENERATING', decode(repeat('41', 32), 'hex')),
			('49000000-0000-0000-0000-000000000032', 'h3-vae', 1, 'CERTIFIED',
			 'VAE', jsonb_build_object('latent', '49000000-0000-0000-0000-000000000012'),
			 jsonb_build_object(
				'video', '49000000-0000-0000-0000-000000000013',
				'cycle', '49000000-0000-0000-0000-000000000010'
			 ),
			 ARRAY['latent'], ARRAY['video'], 'GPU', 'STAGE_RETRY',
			 '49000000-0000-0000-0000-000000000020', '49000000-0000-0000-0000-000000000021',
			 'GENERATING', decode(repeat('42', 32), 'hex'));

		INSERT INTO stage_profile_revisions (
			id, stable_id, revision, state, stage_definition_revision_id,
			model_component_revision, runtime_image_digest, worker_profile_revision_id,
			result_equivalence_revision_id, certified_capacity_vector, content_digest
		) VALUES
			('49000000-0000-0000-0000-000000000040', 'h3-encoder-single-gpu', 1, 'CERTIFIED',
			 '49000000-0000-0000-0000-000000000030', 'h3-encoder-v1', 'sha256:encoder',
			 '49000000-0000-0000-0000-000000000022', '49000000-0000-0000-0000-000000000023',
			 '{"concurrency":1}', decode(repeat('50', 32), 'hex')),
			('49000000-0000-0000-0000-000000000041', 'h3-dit-single-gpu', 1, 'CERTIFIED',
			 '49000000-0000-0000-0000-000000000031', 'h3-dit-v1', 'sha256:dit',
			 '49000000-0000-0000-0000-000000000022', '49000000-0000-0000-0000-000000000024',
			 '{"concurrency":1}', decode(repeat('51', 32), 'hex')),
			('49000000-0000-0000-0000-000000000042', 'h3-vae-single-gpu', 1, 'CERTIFIED',
			 '49000000-0000-0000-0000-000000000032', 'h3-vae-v1', 'sha256:vae',
			 '49000000-0000-0000-0000-000000000022', '49000000-0000-0000-0000-000000000025',
			 '{"concurrency":1}', decode(repeat('52', 32), 'hex'));

		INSERT INTO connector_revisions (
			id, stable_id, revision, state, source_interface_revision_id,
			destination_interface_revision_id, transport, durable_fallback,
			topology_policy, integrity_policy, security_policy, limits, content_digest
		) VALUES
			('49000000-0000-0000-0000-000000000050', 'h3-conditioning-l2', 1, 'REGISTERED',
			 '49000000-0000-0000-0000-000000000011', '49000000-0000-0000-0000-000000000011',
			 'OBJECT_STORE', true, '{}', '{}', '{}', '{}', decode(repeat('60', 32), 'hex')),
			('49000000-0000-0000-0000-000000000051', 'h3-latent-l2', 1, 'REGISTERED',
			 '49000000-0000-0000-0000-000000000012', '49000000-0000-0000-0000-000000000012',
			 'OBJECT_STORE', true, '{}', '{}', '{}', '{}', decode(repeat('61', 32), 'hex')),
			('49000000-0000-0000-0000-000000000052', 'h3-cycle-l2', 1, 'CERTIFIED',
			 '49000000-0000-0000-0000-000000000010', '49000000-0000-0000-0000-000000000010',
			 'OBJECT_STORE', true, '{}', '{}', '{}', '{}', decode(repeat('62', 32), 'hex'));

		INSERT INTO execution_graph_revisions (
			id, model_revision_id, stable_id, revision, schema_version, state,
			final_output_contract, public_phase_map, content_digest
		) VALUES (
			'49000000-0000-0000-0000-000000000001',
			'00000000-0000-0000-0000-000000000010', 'minimax-h3-stage-graph', 1, 1,
			'CERTIFIED', '{"output_key":"video"}',
			'{"encoder":"PREPARING","dit":"GENERATING","vae":"GENERATING"}',
			decode(repeat('91', 32), 'hex')
		);
		INSERT INTO execution_graph_stages (
			execution_graph_revision_id, stage_key, stage_definition_revision_id,
			required, max_fan_out
		) VALUES
			('49000000-0000-0000-0000-000000000001', 'encoder', '49000000-0000-0000-0000-000000000030', true, 1),
			('49000000-0000-0000-0000-000000000001', 'dit', '49000000-0000-0000-0000-000000000031', true, 1),
			('49000000-0000-0000-0000-000000000001', 'vae', '49000000-0000-0000-0000-000000000032', true, 1);
		INSERT INTO execution_graph_edges (
			id, execution_graph_revision_id, source_stage_key, source_port,
			destination_stage_key, destination_port, buffer_class
		) VALUES
			('49000000-0000-0000-0000-000000000060', '49000000-0000-0000-0000-000000000001',
			 'encoder', 'conditioning', 'dit', 'conditioning', 'L2_DURABLE'),
			('49000000-0000-0000-0000-000000000061', '49000000-0000-0000-0000-000000000001',
			 'dit', 'latent', 'vae', 'latent', 'L2_DURABLE');
		INSERT INTO execution_graph_inputs (
			execution_graph_revision_id, input_key, interface_revision_id,
			destination_stage_key, destination_port
		) VALUES (
			'49000000-0000-0000-0000-000000000001', 'request',
			'49000000-0000-0000-0000-000000000010', 'encoder', 'request'
		);
		INSERT INTO execution_graph_outputs (
			execution_graph_revision_id, output_key, interface_revision_id,
			source_stage_key, source_port, required
		) VALUES (
			'49000000-0000-0000-0000-000000000001', 'video',
			'49000000-0000-0000-0000-000000000013', 'vae', 'video', true
		);

		INSERT INTO execution_profile_revisions (
			id, model_revision_id, worker_pool_id, execution_graph_revision_id,
			stable_id, revision, state
		) VALUES (
			'49000000-0000-0000-0000-000000000070',
			'00000000-0000-0000-0000-000000000010', NULL,
			'49000000-0000-0000-0000-000000000001',
			'h3-stage-graph', 1, 'CERTIFIED'
		);
		INSERT INTO execution_profile_stage_options (
			execution_profile_revision_id, execution_graph_revision_id, stage_key,
			stage_definition_revision_id, stage_profile_revision_id,
			preference, eligibility_metadata
		) VALUES
			('49000000-0000-0000-0000-000000000070',
			 '49000000-0000-0000-0000-000000000001', 'encoder',
			 '49000000-0000-0000-0000-000000000030',
			 '49000000-0000-0000-0000-000000000040', 0, '{}'),
			('49000000-0000-0000-0000-000000000070',
			 '49000000-0000-0000-0000-000000000001', 'dit',
			 '49000000-0000-0000-0000-000000000031',
			 '49000000-0000-0000-0000-000000000041', 0, '{}'),
			('49000000-0000-0000-0000-000000000070',
			 '49000000-0000-0000-0000-000000000001', 'vae',
			 '49000000-0000-0000-0000-000000000032',
			 '49000000-0000-0000-0000-000000000042', 0, '{}');
		INSERT INTO execution_profile_connector_options (
			execution_profile_revision_id, execution_graph_revision_id,
			execution_graph_edge_id, connector_revision_id,
			required_topology_policy, preference
		) VALUES
			('49000000-0000-0000-0000-000000000070',
			 '49000000-0000-0000-0000-000000000001',
			 '49000000-0000-0000-0000-000000000060',
			 '49000000-0000-0000-0000-000000000050', '{}', 0),
			('49000000-0000-0000-0000-000000000070',
			 '49000000-0000-0000-0000-000000000001',
			 '49000000-0000-0000-0000-000000000061',
			 '49000000-0000-0000-0000-000000000051', '{}', 0);

		UPDATE execution_graph_revisions
		SET content_digest = vela_execution_graph_content_digest(id)
		WHERE id = '49000000-0000-0000-0000-000000000001';
	`); err != nil {
		t.Fatalf("seed H3 Stage Execution Catalog: %v", err)
	}
}
