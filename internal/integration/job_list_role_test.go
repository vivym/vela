//go:build integration

package integration_test

import (
	veladb "github.com/vivym/vela/internal/database"
	"testing"
)

func TestProjectJobListPreservesRequestRoleBoundary(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	pool := newRolePool(t, database.DSN, "vela_request_login", "vela-request-password")
	if err := veladb.VerifyRole(t.Context(), pool, veladb.RoleRequest); err != nil {
		t.Fatal(err)
	}
	var exposed bool
	if err := database.Admin.QueryRow("SELECT has_any_column_privilege('vela_request_login','model_revisions','SELECT')").Scan(&exposed); err != nil {
		t.Fatal(err)
	}
	if exposed {
		t.Fatal("Job listing must not grant direct access to the model catalog")
	}
}
