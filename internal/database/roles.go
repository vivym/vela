package database

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
)

type Role string

const (
	RoleAuth            Role = "vela_auth"
	RoleRequest         Role = "vela_request"
	RoleInternal        Role = "vela_internal"
	RoleCancel          Role = "vela_cancel"
	RoleArtifactRequest Role = "vela_artifact_request"
)

type rowQuerier interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}

func VerifyRole(ctx context.Context, database rowQuerier, expected Role) error {
	if database == nil {
		return errors.New("database connection is required for role verification")
	}
	if expected != RoleAuth && expected != RoleRequest && expected != RoleInternal &&
		expected != RoleCancel && expected != RoleArtifactRequest {
		return fmt.Errorf("unsupported database role %q", expected)
	}

	var (
		currentUser           string
		superuser             bool
		createDatabase        bool
		createRole            bool
		replication           bool
		bypassRLS             bool
		memberAuth            bool
		memberRequest         bool
		memberInternal        bool
		memberCancel          bool
		memberArtifactRequest bool
		unexpectedMembership  bool
	)
	err := database.QueryRow(ctx, `
        SELECT
            current_user,
            role.rolsuper,
            role.rolcreatedb,
            role.rolcreaterole,
            role.rolreplication,
            role.rolbypassrls,
            pg_has_role(current_user, 'vela_auth', 'MEMBER'),
            pg_has_role(current_user, 'vela_request', 'MEMBER'),
			pg_has_role(current_user, 'vela_internal', 'MEMBER'),
			pg_has_role(current_user, 'vela_cancel', 'MEMBER'),
			pg_has_role(current_user, 'vela_artifact_request', 'MEMBER'),
            EXISTS (
                SELECT 1
                FROM pg_catalog.pg_roles AS inherited
                WHERE inherited.rolname <> current_user
                  AND inherited.rolname <> $1
                  AND pg_has_role(current_user, inherited.oid, 'SET')
            )
        FROM pg_catalog.pg_roles AS role
        WHERE role.rolname = current_user
    `, string(expected)).Scan(
		&currentUser,
		&superuser,
		&createDatabase,
		&createRole,
		&replication,
		&bypassRLS,
		&memberAuth,
		&memberRequest,
		&memberInternal,
		&memberCancel,
		&memberArtifactRequest,
		&unexpectedMembership,
	)
	if err != nil {
		return fmt.Errorf("inspect effective database role: %w", err)
	}
	if superuser || createDatabase || createRole || replication || unexpectedMembership {
		return fmt.Errorf("database login %q has forbidden cluster privileges or role inheritance", currentUser)
	}

	memberships := map[Role]bool{
		RoleAuth: memberAuth, RoleRequest: memberRequest, RoleInternal: memberInternal,
		RoleCancel: memberCancel, RoleArtifactRequest: memberArtifactRequest,
	}
	if !memberships[expected] {
		return fmt.Errorf("database login %q is not a member of required role %q", currentUser, expected)
	}
	for role, member := range memberships {
		if role != expected && member {
			return fmt.Errorf("database login %q also belongs to forbidden role %q", currentUser, role)
		}
	}
	if expected == RoleInternal && !bypassRLS {
		return fmt.Errorf("internal database login %q must have BYPASSRLS", currentUser)
	}
	if expected != RoleInternal && bypassRLS {
		return fmt.Errorf("database login %q must not have BYPASSRLS for role %q", currentUser, expected)
	}
	if expected == RoleAuth {
		if err := verifyAuthPrivileges(ctx, database, currentUser); err != nil {
			return err
		}
	}
	if expected == RoleRequest {
		if err := verifyRequestPrivileges(ctx, database, currentUser); err != nil {
			return err
		}
	}
	if expected == RoleCancel {
		if err := verifyCancelPrivileges(ctx, database, currentUser); err != nil {
			return err
		}
	}
	if expected == RoleArtifactRequest {
		if err := verifyArtifactRequestPrivileges(ctx, database, currentUser); err != nil {
			return err
		}
	}
	return nil
}

func verifyArtifactRequestPrivileges(ctx context.Context, database rowQuerier, currentUser string) error {
	var boundaryViolation bool
	err := database.QueryRow(ctx, `
		WITH expected_table_privileges (relation_name, privilege) AS (
			VALUES
				('jobs', 'SELECT'),
				('artifact_sets', 'SELECT'),
				('artifact_set_items', 'SELECT'),
				('artifact_access_grants', 'SELECT')
		),
		expected_functions (function_oid) AS (
			VALUES
				('vela_current_organization_id()'::regprocedure::oid),
				('vela_current_project_id()'::regprocedure::oid),
				('vela_current_principal_id()'::regprocedure::oid),
				('vela_current_request_scope()'::regprocedure::oid),
				('vela_set_artifact_request_context(uuid,bytea)'::regprocedure::oid)
		),
		public_relations AS (
			SELECT relation.oid, relation.relname
			FROM pg_catalog.pg_class AS relation
			JOIN pg_catalog.pg_namespace AS namespace ON namespace.oid = relation.relnamespace
			WHERE namespace.nspname = 'public'
			  AND relation.relkind IN ('r', 'p', 'v', 'm')
		),
		table_privilege_names (privilege) AS (
			VALUES ('SELECT'), ('INSERT'), ('UPDATE'), ('DELETE'), ('TRUNCATE'), ('REFERENCES'), ('TRIGGER')
		),
		column_privilege_names (privilege) AS (
			VALUES ('SELECT'), ('INSERT'), ('UPDATE'), ('REFERENCES')
		)
		SELECT
			NOT has_schema_privilege(current_user, 'public', 'USAGE')
			OR has_schema_privilege(current_user, 'public', 'CREATE')
			OR has_schema_privilege(current_user, 'vela_private', 'USAGE')
			OR has_schema_privilege(current_user, 'vela_private', 'CREATE')
			OR EXISTS (
				SELECT 1
				FROM pg_catalog.pg_class AS relation
				JOIN pg_catalog.pg_namespace AS namespace ON namespace.oid = relation.relnamespace
				CROSS JOIN table_privilege_names AS candidate
				WHERE namespace.nspname = 'vela_private'
				  AND has_table_privilege(current_user, relation.oid, candidate.privilege)
			)
			OR EXISTS (
				SELECT 1
				FROM pg_catalog.pg_proc AS function
				JOIN pg_catalog.pg_namespace AS namespace ON namespace.oid = function.pronamespace
				WHERE namespace.nspname = 'vela_private'
				  AND has_function_privilege(current_user, function.oid, 'EXECUTE')
			)
			OR EXISTS (
				SELECT 1
				FROM public_relations AS relation
				CROSS JOIN table_privilege_names AS candidate
				LEFT JOIN expected_table_privileges AS expected
				  ON expected.relation_name = relation.relname
				 AND expected.privilege = candidate.privilege
				WHERE has_table_privilege(current_user, relation.oid, candidate.privilege)
				  AND expected.relation_name IS NULL
			)
			OR EXISTS (
				SELECT 1
				FROM expected_table_privileges AS expected
				WHERE NOT has_table_privilege(
					current_user,
					format('public.%I', expected.relation_name),
					expected.privilege
				)
			)
			OR EXISTS (
				SELECT 1
				FROM public_relations AS relation
				JOIN pg_catalog.pg_attribute AS attribute
				  ON attribute.attrelid = relation.oid
				 AND attribute.attnum > 0
				 AND NOT attribute.attisdropped
				CROSS JOIN column_privilege_names AS candidate
				WHERE has_column_privilege(
					current_user,
					relation.oid,
					attribute.attnum,
					candidate.privilege
				)
				  AND NOT has_table_privilege(current_user, relation.oid, candidate.privilege)
			)
			OR EXISTS (
				SELECT 1
				FROM pg_catalog.pg_proc AS function
				JOIN pg_catalog.pg_namespace AS namespace ON namespace.oid = function.pronamespace
				LEFT JOIN expected_functions AS expected ON expected.function_oid = function.oid
				WHERE namespace.nspname = 'public'
				  AND has_function_privilege(current_user, function.oid, 'EXECUTE')
				  AND expected.function_oid IS NULL
			)
			OR EXISTS (
				SELECT 1 FROM expected_functions AS expected
				WHERE NOT has_function_privilege(current_user, expected.function_oid, 'EXECUTE')
			)
	`).Scan(&boundaryViolation)
	if err != nil {
		return fmt.Errorf("inspect Artifact request database privileges: %w", err)
	}
	if boundaryViolation {
		return fmt.Errorf("database login %q exceeds the Artifact read transaction privilege boundary", currentUser)
	}
	return nil
}

func verifyCancelPrivileges(ctx context.Context, database rowQuerier, currentUser string) error {
	var boundaryViolation bool
	err := database.QueryRow(ctx, `
		WITH expected_functions (function_oid) AS (
			VALUES
				('vela_set_cancellation_request_context(uuid,bytea)'::regprocedure::oid),
				('vela_cancel_job(uuid,uuid,uuid,uuid,uuid,uuid,uuid,uuid)'::regprocedure::oid)
		)
		SELECT
			NOT has_schema_privilege(current_user, 'public', 'USAGE')
			OR has_schema_privilege(current_user, 'public', 'CREATE')
			OR has_schema_privilege(current_user, 'vela_private', 'USAGE')
			OR has_schema_privilege(current_user, 'vela_private', 'CREATE')
			OR EXISTS (
				SELECT 1
				FROM pg_catalog.pg_class AS relation
				JOIN pg_catalog.pg_namespace AS namespace ON namespace.oid = relation.relnamespace
				WHERE namespace.nspname IN ('public', 'vela_private')
				  AND relation.relkind IN ('r', 'p', 'v', 'm')
				  AND (
					has_table_privilege(current_user, relation.oid, 'SELECT')
					OR has_table_privilege(current_user, relation.oid, 'INSERT')
					OR has_table_privilege(current_user, relation.oid, 'UPDATE')
					OR has_table_privilege(current_user, relation.oid, 'DELETE')
					OR has_table_privilege(current_user, relation.oid, 'TRUNCATE')
					OR has_table_privilege(current_user, relation.oid, 'REFERENCES')
					OR has_table_privilege(current_user, relation.oid, 'TRIGGER')
				  )
			)
			OR EXISTS (
				SELECT 1
				FROM pg_catalog.pg_proc AS function
				JOIN pg_catalog.pg_namespace AS namespace ON namespace.oid = function.pronamespace
				LEFT JOIN expected_functions AS expected ON expected.function_oid = function.oid
				WHERE namespace.nspname = 'public'
				  AND has_function_privilege(current_user, function.oid, 'EXECUTE')
				  AND expected.function_oid IS NULL
			)
			OR EXISTS (
				SELECT 1 FROM expected_functions AS expected
				WHERE NOT has_function_privilege(current_user, expected.function_oid, 'EXECUTE')
			)
	`).Scan(&boundaryViolation)
	if err != nil {
		return fmt.Errorf("inspect cancel database privileges: %w", err)
	}
	if boundaryViolation {
		return fmt.Errorf("database login %q exceeds the cancellation command privilege boundary", currentUser)
	}
	return nil
}

func verifyAuthPrivileges(ctx context.Context, database rowQuerier, currentUser string) error {
	var missingSchemaUsage, hasSchemaCreate, hasTablePrivilege, hasCredentialLookup, hasUnexpectedFunction bool
	var hasPrivateSchemaPrivilege, hasPrivateObjectPrivilege bool
	err := database.QueryRow(ctx, `
        SELECT
			NOT has_schema_privilege(current_user, 'public', 'USAGE'),
			has_schema_privilege(current_user, 'public', 'CREATE'),
            EXISTS (
                SELECT 1
                FROM pg_catalog.pg_class AS relation
                JOIN pg_catalog.pg_namespace AS namespace ON namespace.oid = relation.relnamespace
                WHERE namespace.nspname = 'public'
                  AND relation.relkind IN ('r', 'p')
                  AND (
                      has_table_privilege(current_user, relation.oid, 'SELECT')
                      OR has_table_privilege(current_user, relation.oid, 'INSERT')
                      OR has_table_privilege(current_user, relation.oid, 'UPDATE')
                      OR has_table_privilege(current_user, relation.oid, 'DELETE')
                      OR has_table_privilege(current_user, relation.oid, 'TRUNCATE')
                      OR has_table_privilege(current_user, relation.oid, 'REFERENCES')
                      OR has_table_privilege(current_user, relation.oid, 'TRIGGER')
                  )
            ),
            has_function_privilege(
                current_user,
                'vela_authenticate_service_credential(uuid)'::regprocedure,
                'EXECUTE'
            ),
            EXISTS (
                SELECT 1
                FROM pg_catalog.pg_proc AS function
                JOIN pg_catalog.pg_namespace AS namespace ON namespace.oid = function.pronamespace
                WHERE namespace.nspname = 'public'
                  AND function.oid <> 'vela_authenticate_service_credential(uuid)'::regprocedure
                  AND has_function_privilege(current_user, function.oid, 'EXECUTE')
			),
			has_schema_privilege(current_user, 'vela_private', 'USAGE')
				OR has_schema_privilege(current_user, 'vela_private', 'CREATE'),
			EXISTS (
				SELECT 1
				FROM pg_catalog.pg_class AS relation
				JOIN pg_catalog.pg_namespace AS namespace ON namespace.oid = relation.relnamespace
				WHERE namespace.nspname = 'vela_private'
				  AND (
					has_table_privilege(current_user, relation.oid, 'SELECT')
					OR has_table_privilege(current_user, relation.oid, 'INSERT')
					OR has_table_privilege(current_user, relation.oid, 'UPDATE')
					OR has_table_privilege(current_user, relation.oid, 'DELETE')
					OR has_table_privilege(current_user, relation.oid, 'TRUNCATE')
					OR has_table_privilege(current_user, relation.oid, 'REFERENCES')
					OR has_table_privilege(current_user, relation.oid, 'TRIGGER')
				  )
			)
	`).Scan(
		&missingSchemaUsage,
		&hasSchemaCreate,
		&hasTablePrivilege,
		&hasCredentialLookup,
		&hasUnexpectedFunction,
		&hasPrivateSchemaPrivilege,
		&hasPrivateObjectPrivilege,
	)
	if err != nil {
		return fmt.Errorf("inspect auth database privileges: %w", err)
	}
	if missingSchemaUsage || hasSchemaCreate || hasTablePrivilege || !hasCredentialLookup || hasUnexpectedFunction ||
		hasPrivateSchemaPrivilege || hasPrivateObjectPrivilege {
		return fmt.Errorf("database login %q exceeds the credential lookup privilege boundary", currentUser)
	}
	return nil
}

func verifyRequestPrivileges(ctx context.Context, database rowQuerier, currentUser string) error {
	var boundaryViolation bool
	err := database.QueryRow(ctx, `
		WITH expected_table_privileges (relation_name, privilege) AS (
			VALUES
				('worker_pools', 'SELECT'),
				('customer_organizations', 'SELECT'),
				('projects', 'SELECT'),
				('principals', 'SELECT'),
				('service_principals', 'SELECT'),
				('organization_credit_accounts', 'SELECT'),
				('jobs', 'SELECT'),
				('credit_reservations', 'SELECT'),
				('idempotency_results', 'SELECT'),
				('outbox_events', 'SELECT'),
				('vela_request_execution_lease_renewal_protocol', 'SELECT'),
				('vela_request_job_runtime', 'SELECT'),
				('vela_request_job_progress', 'SELECT'),
				('jobs', 'INSERT'),
				('credit_reservations', 'INSERT'),
				('retry_runtime_states', 'INSERT'),
				('idempotency_results', 'INSERT'),
				('outbox_events', 'INSERT')
			UNION ALL
			SELECT 'retry_runtime_states', 'SELECT'
			FROM vela_request_execution_lease_renewal_protocol
			WHERE NOT enabled
		),
		expected_column_privileges (relation_name, column_name, privilege) AS (
			VALUES
				('worker_pools', 'queued_count', 'UPDATE'),
				('projects', 'queued_count', 'UPDATE'),
				('projects', 'running_count', 'UPDATE'),
				('organization_credit_accounts', 'reserved_minor', 'UPDATE'),
				('organization_credit_accounts', 'version', 'UPDATE'),
				('organization_credit_accounts', 'updated_at', 'UPDATE')
		),
		expected_functions (function_oid) AS (
			VALUES
				('vela_current_organization_id()'::regprocedure::oid),
				('vela_current_project_id()'::regprocedure::oid),
				('vela_current_principal_id()'::regprocedure::oid),
				('vela_current_request_scope()'::regprocedure::oid),
				('vela_set_request_context(uuid,bytea,text)'::regprocedure::oid),
				('vela_resolve_active_sku(text,text,text,text)'::regprocedure::oid),
				('vela_lock_compatible_pool(uuid,uuid,uuid)'::regprocedure::oid)
		),
		public_relations AS (
			SELECT relation.oid, relation.relname
			FROM pg_catalog.pg_class AS relation
			JOIN pg_catalog.pg_namespace AS namespace ON namespace.oid = relation.relnamespace
			WHERE namespace.nspname = 'public'
			  AND relation.relkind IN ('r', 'p', 'v', 'm')
		),
			table_privilege_names (privilege) AS (
				VALUES ('SELECT'), ('INSERT'), ('UPDATE'), ('DELETE'), ('TRUNCATE'), ('REFERENCES'), ('TRIGGER')
			),
			column_privilege_names (privilege) AS (
				VALUES ('SELECT'), ('INSERT'), ('UPDATE'), ('REFERENCES')
		)
		SELECT
			NOT has_schema_privilege(current_user, 'public', 'USAGE')
			OR has_schema_privilege(current_user, 'public', 'CREATE')
			OR has_schema_privilege(current_user, 'vela_private', 'USAGE')
			OR has_schema_privilege(current_user, 'vela_private', 'CREATE')
			OR EXISTS (
				SELECT 1
				FROM pg_catalog.pg_class AS relation
				JOIN pg_catalog.pg_namespace AS namespace ON namespace.oid = relation.relnamespace
				CROSS JOIN table_privilege_names AS candidate
				WHERE namespace.nspname = 'vela_private'
				  AND has_table_privilege(current_user, relation.oid, candidate.privilege)
			)
			OR EXISTS (
				SELECT 1
				FROM pg_catalog.pg_proc AS function
				JOIN pg_catalog.pg_namespace AS namespace ON namespace.oid = function.pronamespace
				WHERE namespace.nspname = 'vela_private'
				  AND has_function_privilege(current_user, function.oid, 'EXECUTE')
			)
			OR EXISTS (
				SELECT 1
				FROM public_relations AS relation
				CROSS JOIN table_privilege_names AS candidate
				LEFT JOIN expected_table_privileges AS expected
				  ON expected.relation_name = relation.relname
				 AND expected.privilege = candidate.privilege
				WHERE has_table_privilege(current_user, relation.oid, candidate.privilege)
				  AND expected.relation_name IS NULL
			)
			OR EXISTS (
				SELECT 1
				FROM expected_table_privileges AS expected
				WHERE NOT has_table_privilege(
					current_user,
					format('public.%I', expected.relation_name),
					expected.privilege
				)
			)
			OR EXISTS (
				SELECT 1
				FROM public_relations AS relation
				JOIN pg_catalog.pg_attribute AS attribute
				  ON attribute.attrelid = relation.oid
				 AND attribute.attnum > 0
				 AND NOT attribute.attisdropped
				CROSS JOIN column_privilege_names AS candidate
				LEFT JOIN expected_column_privileges AS expected
				  ON expected.relation_name = relation.relname
				 AND expected.column_name = attribute.attname
				 AND expected.privilege = candidate.privilege
				WHERE has_column_privilege(
					current_user,
					relation.oid,
					attribute.attnum,
					candidate.privilege
				)
				  AND NOT has_table_privilege(current_user, relation.oid, candidate.privilege)
				  AND expected.relation_name IS NULL
			)
			OR EXISTS (
				SELECT 1
				FROM expected_column_privileges AS expected
				WHERE NOT has_column_privilege(
					current_user,
					format('public.%I', expected.relation_name),
					expected.column_name,
					expected.privilege
				)
			)
			OR EXISTS (
				SELECT 1
				FROM pg_catalog.pg_proc AS function
				JOIN pg_catalog.pg_namespace AS namespace ON namespace.oid = function.pronamespace
				LEFT JOIN expected_functions AS expected ON expected.function_oid = function.oid
				WHERE namespace.nspname = 'public'
				  AND has_function_privilege(current_user, function.oid, 'EXECUTE')
				  AND expected.function_oid IS NULL
			)
			OR EXISTS (
				SELECT 1
				FROM expected_functions AS expected
				WHERE NOT has_function_privilege(current_user, expected.function_oid, 'EXECUTE')
			)
	`).Scan(&boundaryViolation)
	if err != nil {
		return fmt.Errorf("inspect request database privileges: %w", err)
	}
	if boundaryViolation {
		return fmt.Errorf("database login %q exceeds the request transaction privilege boundary", currentUser)
	}
	return nil
}
