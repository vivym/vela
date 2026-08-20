//go:build integration

package integration_test

import (
	"context"
	"errors"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	veladb "github.com/vivym/vela/internal/database"
)

func TestDatabasePoolsFailClosedOnRoleConfusion(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	authPool := newRolePool(t, database.DSN, "vela_auth_login", "vela-auth-password")
	requestPool := newRolePool(t, database.DSN, "vela_request_login", "vela-request-password")
	internalPool := newRolePool(t, database.DSN, "vela_internal_login", "vela-internal-password")

	for _, test := range []struct {
		name string
		pool *pgxpool.Pool
		role veladb.Role
	}{
		{name: "auth", pool: authPool, role: veladb.RoleAuth},
		{name: "request", pool: requestPool, role: veladb.RoleRequest},
		{name: "internal", pool: internalPool, role: veladb.RoleInternal},
	} {
		t.Run(test.name, func(t *testing.T) {
			if err := veladb.VerifyRole(context.Background(), test.pool, test.role); err != nil {
				t.Fatalf("verify correct %s role: %v", test.name, err)
			}
		})
	}

	var privateContextCount int
	err := requestPool.QueryRow(
		context.Background(),
		"SELECT count(*) FROM vela_private.request_contexts",
	).Scan(&privateContextCount)
	var postgresError *pgconn.PgError
	if !errors.As(err, &postgresError) || postgresError.Code != "42501" {
		t.Fatalf("request login private context read error = %v, want SQLSTATE 42501", err)
	}

	for _, test := range []struct {
		name string
		pool *pgxpool.Pool
		role veladb.Role
	}{
		{name: "request as internal", pool: requestPool, role: veladb.RoleInternal},
		{name: "internal as request", pool: internalPool, role: veladb.RoleRequest},
		{name: "auth as request", pool: authPool, role: veladb.RoleRequest},
	} {
		t.Run(test.name, func(t *testing.T) {
			if err := veladb.VerifyRole(context.Background(), test.pool, test.role); err == nil {
				t.Fatalf("role confusion %s was accepted", test.name)
			}
		})
	}

	if _, err := database.Admin.Exec("GRANT SELECT ON jobs TO vela_auth_login"); err != nil {
		t.Fatalf("grant unexpected auth table privilege: %v", err)
	}
	if err := veladb.VerifyRole(context.Background(), authPool, veladb.RoleAuth); err == nil {
		t.Fatal("auth login with direct table access was accepted")
	}
	if _, err := database.Admin.Exec("REVOKE SELECT ON jobs FROM vela_auth_login"); err != nil {
		t.Fatalf("revoke unexpected auth table privilege: %v", err)
	}

	if _, err := database.Admin.Exec(`
        CREATE ROLE vela_rogue_privileged NOLOGIN BYPASSRLS;
        GRANT vela_rogue_privileged TO vela_auth_login;
    `); err != nil {
		t.Fatalf("grant rogue privileged role: %v", err)
	}
	if err := veladb.VerifyRole(context.Background(), authPool, veladb.RoleAuth); err == nil {
		t.Fatal("auth login that can SET ROLE to BYPASSRLS was accepted")
	}

	if _, err := database.Admin.Exec(`
		CREATE ROLE vela_rogue_reader NOLOGIN;
		GRANT SELECT ON credentials TO vela_rogue_reader;
		GRANT vela_rogue_reader TO vela_request_login;
	`); err != nil {
		t.Fatalf("grant rogue data-reading role: %v", err)
	}
	if err := veladb.VerifyRole(context.Background(), requestPool, veladb.RoleRequest); err == nil {
		t.Fatal("request login that can SET ROLE to an unexpected data-reading role was accepted")
	}
	if _, err := database.Admin.Exec(`
		REVOKE vela_rogue_reader FROM vela_request_login;
		REVOKE SELECT ON credentials FROM vela_rogue_reader;
		DROP ROLE vela_rogue_reader;
	`); err != nil {
		t.Fatalf("remove rogue data-reading role: %v", err)
	}

	if _, err := database.Admin.Exec("GRANT DELETE ON jobs TO vela_request_login"); err != nil {
		t.Fatalf("grant unexpected request table privilege: %v", err)
	}
	if err := veladb.VerifyRole(context.Background(), requestPool, veladb.RoleRequest); err == nil {
		t.Fatal("request login with direct Job deletion privilege was accepted")
	}
	if _, err := database.Admin.Exec("REVOKE DELETE ON jobs FROM vela_request_login"); err != nil {
		t.Fatalf("revoke unexpected request Job deletion privilege: %v", err)
	}

	if _, err := database.Admin.Exec(`
		GRANT USAGE ON SCHEMA vela_private TO vela_request_login;
		GRANT SELECT ON vela_private.request_contexts TO vela_request_login;
	`); err != nil {
		t.Fatalf("grant unexpected private request-context access: %v", err)
	}
	if err := veladb.VerifyRole(context.Background(), requestPool, veladb.RoleRequest); err == nil {
		t.Fatal("request login with private request-context access was accepted")
	}

	if _, err := database.Admin.Exec(`
		GRANT USAGE ON SCHEMA vela_private TO vela_auth_login;
		GRANT SELECT ON vela_private.request_contexts TO vela_auth_login;
	`); err != nil {
		t.Fatalf("grant unexpected private auth request-context access: %v", err)
	}
	if err := veladb.VerifyRole(context.Background(), authPool, veladb.RoleAuth); err == nil {
		t.Fatal("auth login with private request-context access was accepted")
	}
}
