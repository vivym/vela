package recovery

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

type Operation struct {
	ID               uuid.UUID `json:"id"`
	DatabaseIdentity uuid.UUID `json:"database_identity"`
	Generation       int64     `json:"generation"`
	SystemIdentifier string    `json:"system_identifier"`
	DatabaseName     string    `json:"database_name"`
	DatabaseOID      uint32    `json:"database_oid,string"`
	Actor            string    `json:"actor"`
	ClosedAt         time.Time `json:"closed_at"`
}

type Receipt struct {
	Operation
	Schema         string           `json:"schema"`
	ProductionGate bool             `json:"production_gate"`
	Scope          string           `json:"scope"`
	SchemaVersion  int64            `json:"schema_version"`
	Inventory      map[string]int64 `json:"inventory"`
	SealedAt       time.Time        `json:"sealed_at"`
}

// Quiesce retains its database operation across interruption. Cancellation can
// close the caller-owned pgx connection; retries must use a usable connection
// and the same operation ID, as separate operator command invocations do.
func Quiesce(ctx context.Context, connection *pgx.Conn, id uuid.UUID, pollInterval time.Duration) (Receipt, error) {
	if ctx == nil || connection == nil || id == uuid.Nil || pollInterval <= 0 {
		return Receipt{}, errors.New("recovery operation and polling configuration are required")
	}
	var operation json.RawMessage
	if err := connection.QueryRow(ctx, `SELECT vela_close_recovery_admission($1)`, id).Scan(&operation); err != nil {
		return Receipt{}, fmt.Errorf("close recovery Admission: %w", err)
	}
	for {
		var encoded json.RawMessage
		err := connection.QueryRow(ctx, `SELECT vela_seal_recovery_quiescence($1)`, id).Scan(&encoded)
		if err == nil {
			var receipt Receipt
			if err := json.Unmarshal(encoded, &receipt); err != nil {
				return Receipt{}, fmt.Errorf("decode quiescence receipt: %w", err)
			}
			return receipt, receipt.Validate()
		}
		var postgresError *pgconn.PgError
		if !errors.As(err, &postgresError) || postgresError.ConstraintName != "recovery_authority_not_quiescent" {
			return Receipt{}, fmt.Errorf("seal recovery quiescence: %w", err)
		}
		timer := time.NewTimer(pollInterval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return Receipt{}, fmt.Errorf("wait for execution drain; Admission remains closed: %w", ctx.Err())
		case <-timer.C:
		}
	}
}

func Reopen(ctx context.Context, connection *pgx.Conn, id uuid.UUID, generation int64) error {
	if connection == nil || id == uuid.Nil || generation <= 1 {
		return errors.New("exact recovery operation and gate generation are required")
	}
	_, err := connection.Exec(ctx, `SELECT vela_reopen_recovery_admission($1,$2)`, id, generation)
	return err
}

func Status(ctx context.Context, connection *pgx.Conn) (json.RawMessage, error) {
	if connection == nil {
		return nil, errors.New("source recovery connection is required")
	}
	var status json.RawMessage
	err := connection.QueryRow(ctx, `SELECT vela_recovery_status()`).Scan(&status)
	return status, err
}

func (receipt Receipt) Validate() error {
	if receipt.ID == uuid.Nil || receipt.DatabaseIdentity == uuid.Nil || receipt.Generation <= 1 ||
		receipt.SystemIdentifier == "" || receipt.DatabaseName == "" || receipt.DatabaseOID == 0 ||
		receipt.Actor == "" || receipt.ClosedAt.IsZero() || receipt.SealedAt.Before(receipt.ClosedAt) ||
		receipt.Schema != "vela-db-quiescence-v1" || receipt.Scope != "DATABASE_ONLY" ||
		receipt.ProductionGate || receipt.SchemaVersion < 72 {
		return errors.New("quiescence receipt identity or evidence scope is invalid")
	}
	keys := []string{"jobs", "attempts", "stage_runs", "stage_attempts", "stage_leases", "stage_allocations",
		"materialization_leases", "transfer_tickets", "finalization_claims", "execution_pins", "edge_buffer_credits", "storage_reservations"}
	if receipt.SchemaVersion >= 91 {
		keys = append(keys, "worker_bootstrap_claims")
	}
	if len(receipt.Inventory) != len(keys) {
		return errors.New("quiescence inventory is incomplete")
	}
	for _, key := range keys {
		if value, ok := receipt.Inventory[key]; !ok || value != 0 {
			return fmt.Errorf("quiescence inventory %s is missing or nonzero", key)
		}
	}
	return nil
}
