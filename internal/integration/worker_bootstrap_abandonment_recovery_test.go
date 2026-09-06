//go:build integration

package integration_test

import (
	"context"
	"io"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/vivym/vela/internal/fleet"
	"github.com/vivym/vela/internal/recovery"
)

func TestWorkerBootstrapAbandonmentSurvivesIndependentDatabaseRestore(t *testing.T) {
	database, service, request := newWorkerBootstrapFixture(t)
	claim, err := service.ClaimWorkerBootstrap(t.Context(), request)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.AbandonWorkerBootstrap(t.Context(), fleet.WorkerBootstrapLookup{
		RequestID: request.RequestID, NodeIdentity: claim.NodeIdentity, ActorIdentity: request.ActorIdentity}); err != nil {
		t.Fatal(err)
	}
	connection := recoveryConnection(t, database)
	operation := uuid.New()
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()
	if _, err := recovery.Quiesce(ctx, connection, operation, time.Millisecond); err != nil {
		t.Fatal(err)
	}
	backup, err := pgx.Connect(ctx, database.DSN)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = backup.Close(context.Background()) }()
	directory := filepath.Join(t.TempDir(), "snapshot")
	rolesPath := filepath.Join(repositoryRoot(t), "db", "bootstrap", "roles.sql")
	manifest, err := recovery.Capture(ctx, backup, operation, directory, rolesPath, func(ctx context.Context, snapshot string, writer io.Writer) error {
		command := exec.CommandContext(ctx, "docker", "exec", database.Container.GetContainerID(),
			"pg_dump", "--username=postgres", "--format=custom", "--snapshot="+snapshot, "vela")
		command.Stdout = writer
		return command.Run()
	})
	if err != nil {
		t.Fatal(err)
	}
	if manifest.Fingerprints["worker_bootstrap_claims"].Rows != 1 || manifest.Fingerprints["worker_bootstrap_abandonments"].Rows != 1 ||
		manifest.Fingerprints["worker_bootstrap_receipts"].Rows != 0 {
		t.Fatal("snapshot did not retain the exact terminal bootstrap inventory")
	}
	restored, err := recovery.RestoreDrill(ctx, directory, rolesPath, "postgres:17-alpine")
	if err != nil {
		t.Fatal(err)
	}
	if restored.ProductionGate || restored.Scope != "DATABASE_ONLY" || !restored.CatalogVerified || !restored.AdmissionClosed ||
		restored.TablesVerified != len(manifest.Tables) || restored.TargetSystemIdentifier == restored.SourceSystemIdentifier {
		t.Fatalf("restore did not preserve terminal rows and rejecting authority functions: %+v", restored)
	}
}
