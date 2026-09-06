//go:build integration

package integration_test

import (
	"bytes"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/pressly/goose/v3"
	"github.com/vivym/vela/internal/fleet"
)

func TestWorkerBootstrapReceiptRejectsZeroScopesBeforeCommit(t *testing.T) {
	for _, surface := range []string{"service", "database"} {
		for _, field := range []string{"worker", "runtime"} {
			t.Run(surface+"/"+field, func(t *testing.T) {
				database, service, request := newWorkerBootstrapFixture(t)
				if _, err := service.ClaimWorkerBootstrap(t.Context(), request); err != nil {
					t.Fatal(err)
				}
				receipt := fleet.WorkerBootstrapReceipt{RequestID: request.RequestID, ActorIdentity: request.ActorIdentity,
					WorkerJournalID: uuid.New(), RuntimeJournalID: uuid.New(), WorkerScope: bytes.Repeat([]byte{1}, 32), RuntimeScope: bytes.Repeat([]byte{2}, 32)}
				invalid := receipt
				if field == "worker" {
					invalid.WorkerScope = make([]byte, 32)
				} else {
					invalid.RuntimeScope = make([]byte, 32)
				}
				if surface == "service" {
					when, err := service.RecordWorkerBootstrapReceipt(t.Context(), invalid)
					if err == nil || !when.IsZero() {
						t.Fatalf("zero scope permanently consumed receipt identity: %v %v", when, err)
					}
					assertFleetFailure(t, err, fleet.FailureInvalid)
				} else {
					var when time.Time
					err := database.Admin.QueryRow(`SELECT vela_record_worker_bootstrap_receipt($1,$2,$3,$4,$5,$6)`,
						invalid.RequestID, invalid.WorkerJournalID, invalid.WorkerScope, invalid.RuntimeJournalID, invalid.RuntimeScope, invalid.ActorIdentity).Scan(&when)
					var postgresError *pgconn.PgError
					if !errors.As(err, &postgresError) || postgresError.Code != "23514" {
						t.Fatalf("database accepted zero scope or failed outside identity constraint: %v", err)
					}
				}
				var count int
				if err := database.Admin.QueryRow("SELECT count(*) FROM worker_bootstrap_receipts").Scan(&count); err != nil || count != 0 {
					t.Fatalf("rejected receipt left permanent history: %d %v", count, err)
				}
				first, err := service.RecordWorkerBootstrapReceipt(t.Context(), receipt)
				if err != nil || first.IsZero() {
					t.Fatalf("valid scope could not complete original claim: %v", err)
				}
				replay, err := service.RecordWorkerBootstrapReceipt(t.Context(), receipt)
				if err != nil || !first.Equal(replay) {
					t.Fatalf("valid replay changed original receipt: %v", err)
				}
			})
		}
	}
}

func TestWorkerBootstrapScopeMigrationPreservesAndRejectsInvalidHistory(t *testing.T) {
	for _, invalid := range []bool{false, true} {
		t.Run(map[bool]string{false: "valid", true: "zero-scope"}[invalid], func(t *testing.T) {
			database, service, request := newWorkerBootstrapFixture(t)
			if _, err := service.ClaimWorkerBootstrap(t.Context(), request); err != nil {
				t.Fatal(err)
			}
			migrations := filepath.Join(repositoryRoot(t), "db", "migrations")
			if err := goose.DownTo(database.Admin, migrations, 92); err != nil {
				t.Fatal(err)
			}
			scope := bytes.Repeat([]byte{1}, 32)
			if invalid {
				clear(scope)
			}
			var recordedAt time.Time
			if err := database.Admin.QueryRow(`SELECT vela_record_worker_bootstrap_receipt($1,$2,$3,$4,$5,$6)`,
				request.RequestID, uuid.New(), scope, uuid.New(), bytes.Repeat([]byte{2}, 32), request.ActorIdentity).Scan(&recordedAt); err != nil {
				t.Fatal(err)
			}
			read := func() string {
				t.Helper()
				var wire string
				if err := database.Admin.QueryRow("SELECT row_to_json(t)::text FROM worker_bootstrap_receipts t WHERE request_id=$1", request.RequestID).Scan(&wire); err != nil {
					t.Fatal(err)
				}
				return wire
			}
			before := read()
			err := goose.UpTo(database.Admin, migrations, 93)
			version, versionErr := goose.GetDBVersion(database.Admin)
			if versionErr != nil || invalid && (err == nil || version != 92) || !invalid && (err != nil || version != 93) || before != read() {
				t.Fatalf("scope migration changed history or accepted invalid metadata: version=%d err=%v versionErr=%v", version, err, versionErr)
			}
			if invalid {
				var postgresError *pgconn.PgError
				if !errors.As(err, &postgresError) || postgresError.Code != "23514" {
					t.Fatalf("migration failed outside its scope constraint: %v", err)
				}
			}
		})
	}
}
