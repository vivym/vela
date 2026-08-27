//go:build integration

package integration_test

import (
	"database/sql"
	"testing"
	"time"

	"github.com/google/uuid"
)

type terminalJobFixtureExecer interface {
	Exec(string, ...any) (sql.Result, error)
}

func terminalizeJobWithCanonicalEvent(
	t *testing.T,
	database terminalJobFixtureExecer,
	jobID uuid.UUID,
	state string,
	occurredAt *time.Time,
) {
	t.Helper()
	eventType := canonicalTerminalEventType(t, state)
	if _, err := database.Exec(`
		WITH terminal AS (
			UPDATE jobs
			SET state = $2, version = version + 1,
				updated_at = COALESCE($4::timestamptz, clock_timestamp())
			WHERE id = $1
			RETURNING organization_id, project_id, id, version, updated_at
		)
		INSERT INTO outbox_events (
			event_id, organization_id, project_id, aggregate_type, aggregate_id,
			aggregate_version, event_type, schema_version, payload,
			occurred_at, available_at
		)
		SELECT gen_random_uuid(), organization_id, project_id, 'Job', id,
			version, $3, 1, decode('00', 'hex'), updated_at, updated_at
		FROM terminal
	`, jobID, state, eventType, occurredAt); err != nil {
		t.Fatalf("terminalize %s Job fixture with canonical event: %v", state, err)
	}
}

func insertCanonicalTerminalJobEvent(
	t *testing.T,
	database terminalJobFixtureExecer,
	jobID uuid.UUID,
	state string,
	occurredAt *time.Time,
) {
	t.Helper()
	eventType := canonicalTerminalEventType(t, state)
	if _, err := database.Exec(`
		INSERT INTO outbox_events (
			event_id, organization_id, project_id, aggregate_type, aggregate_id,
			aggregate_version, event_type, schema_version, payload,
			occurred_at, available_at
		)
		SELECT gen_random_uuid(), organization_id, project_id, 'Job', id,
			version, $2, 1, decode('00', 'hex'),
			COALESCE($3::timestamptz, updated_at),
			COALESCE($3::timestamptz, updated_at)
		FROM jobs
		WHERE id = $1
	`, jobID, eventType, occurredAt); err != nil {
		t.Fatalf("insert %s Job canonical terminal event fixture: %v", state, err)
	}
}

func canonicalTerminalEventType(t *testing.T, state string) string {
	t.Helper()
	switch state {
	case "SUCCEEDED":
		return "job.succeeded"
	case "FAILED":
		return "job.failed"
	case "CANCELED":
		return "job.canceled"
	default:
		t.Fatalf("unsupported terminal Job fixture state %q", state)
		return ""
	}
}
