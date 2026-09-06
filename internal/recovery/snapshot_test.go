package recovery

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
)

func validTestReceipt() Receipt {
	now := time.Now().UTC()
	return Receipt{
		Operation: Operation{ID: uuid.New(), DatabaseIdentity: uuid.New(), Generation: 2,
			SystemIdentifier: "123456789", DatabaseName: "vela", DatabaseOID: 1, Actor: "recovery-test", ClosedAt: now},
		Schema: "vela-db-quiescence-v1", Scope: "DATABASE_ONLY", SchemaVersion: 72, SealedAt: now,
		Inventory: map[string]int64{"jobs": 0, "attempts": 0, "stage_runs": 0, "stage_attempts": 0, "stage_leases": 0,
			"stage_allocations": 0, "materialization_leases": 0, "transfer_tickets": 0, "finalization_claims": 0,
			"execution_pins": 0, "edge_buffer_credits": 0, "storage_reservations": 0},
	}
}

func TestReceiptRejectsMissingAndActiveAuthority(t *testing.T) {
	for key := range validTestReceipt().Inventory {
		t.Run(key, func(t *testing.T) {
			receipt := validTestReceipt()
			delete(receipt.Inventory, key)
			if err := receipt.Validate(); err == nil {
				t.Fatal("missing authority accepted")
			}
			receipt.Inventory[key] = 1
			if err := receipt.Validate(); err == nil {
				t.Fatal("active authority accepted")
			}
		})
	}
}

func TestReceiptRequiresBootstrapInventoryAtSchema91(t *testing.T) {
	receipt := validTestReceipt()
	receipt.SchemaVersion = 90
	if err := receipt.Validate(); err != nil {
		t.Fatal(err)
	}
	receipt.SchemaVersion = 91
	if err := receipt.Validate(); err == nil {
		t.Fatal("schema 91 receipt omitted bootstrap authority")
	}
	receipt.Inventory["worker_bootstrap_claims"] = 1
	if err := receipt.Validate(); err == nil {
		t.Fatal("pending bootstrap accepted as quiescent")
	}
	receipt.Inventory["worker_bootstrap_claims"] = 0
	if err := receipt.Validate(); err != nil {
		t.Fatal(err)
	}
}

func TestRestoreRejectsCorruptDumpBeforeDocker(t *testing.T) {
	directory := t.TempDir()
	dumpPath := filepath.Join(directory, "database.dump")
	if err := os.WriteFile(dumpPath, []byte("original"), 0o600); err != nil {
		t.Fatal(err)
	}
	digest, err := hashFile(dumpPath)
	if err != nil {
		t.Fatal(err)
	}
	manifest := Manifest{Schema: "vela-db-snapshot-v1", Scope: "DATABASE_ONLY", Receipt: validTestReceipt(),
		SnapshotID: "snapshot", DumpSHA256: digest, Tables: []string{"jobs"}, Fingerprints: map[string]TableFingerprint{"jobs": {Rows: 1}}}
	if err := writeJSONExclusive(filepath.Join(directory, "snapshot.json"), manifest); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(dumpPath, []byte("corrupt"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", t.TempDir())
	if _, err := RestoreDrill(context.Background(), directory, "unused", "postgres:17-alpine"); err == nil || err.Error() != "database dump does not match the captured SHA256" {
		t.Fatalf("corrupted evidence reached Docker: %v", err)
	}
}

func TestReceiptNeverClaimsProductionOrObjectRecovery(t *testing.T) {
	for _, field := range []string{"production", "objects", "empty identity"} {
		t.Run(field, func(t *testing.T) {
			receipt := validTestReceipt()
			switch field {
			case "production":
				receipt.ProductionGate = true
			case "objects":
				receipt.Scope = "DATABASE_AND_OBJECTS"
			default:
				receipt.SystemIdentifier = ""
			}
			if err := receipt.Validate(); err == nil {
				t.Fatal("unsupported recovery claim accepted")
			}
		})
	}
	receipt := validTestReceipt()
	encoded, err := json.Marshal(receipt)
	if err != nil {
		t.Fatal(err)
	}
	var decoded Receipt
	if err := json.Unmarshal(encoded, &decoded); err != nil || decoded.Validate() != nil {
		t.Fatalf("receipt roundtrip invalid: %v", err)
	}
}
