package nodeagent

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/modelruntime"
)

func TestRuntimeStartupGrantConsumption(t *testing.T) {
	f, config := newRemoteReservationFixture(t, false)
	ledger, directory := startupTestLedger(t)
	reservation, err := ledger.ReserveRemote(t.Context(), config)
	if err != nil {
		t.Fatal(err)
	}
	before, err := modelruntime.ReadJournalDocument(t.Context(), ledger.starts[f.request.JournalID].Remote.JournalIdentity, config.Journal.Read)
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256([]byte("fixture external authorization evidence; not a permit"))
	record, err := ledger.ConsumeJournalGrantAttempt(t.Context(), reservation.JournalID, reservation.OperationID, digest)
	if err != nil {
		t.Fatal(err)
	}
	after, err := modelruntime.ReadJournalDocument(t.Context(), ledger.starts[f.request.JournalID].Remote.JournalIdentity, config.Journal.Read)
	if err != nil || !bytes.Equal(before.Document, after.Document) || !bytes.Equal(before.LockDocument, after.LockDocument) {
		t.Fatalf("negative fence mutated Runtime journal: %v", err)
	}
	wire, err := os.ReadFile(filepath.Join(directory, runtimeStartupLedgerName))
	if err != nil {
		t.Fatal(err)
	}
	for _, evidence := range [][32]byte{digest, sha256.Sum256([]byte("replacement evidence"))} {
		if result, err := ledger.ConsumeJournalGrantAttempt(t.Context(), record.JournalID, record.OperationID, evidence); !errors.Is(err, ErrRuntimeStartupGrantConsumed) || result != (RuntimeStartupGrantAttempt{}) {
			t.Fatalf("duplicate consumption: %+v %v", result, err)
		}
	}
	current, err := os.ReadFile(filepath.Join(directory, runtimeStartupLedgerName))
	if err != nil || !bytes.Equal(current, wire) {
		t.Fatal("retry appended another fence")
	}
	if err := ledger.Close(); err != nil {
		t.Fatal(err)
	}
	recovered, err := OpenRuntimeStartupLedger(t.Context(), directory, "cpu-node", false)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = recovered.Close() }()
	if got, err := recovered.InspectJournalGrantAttempt(t.Context(), record.JournalID); err != nil || got != record {
		t.Fatalf("lost grant fence: %+v %v", got, err)
	}
	if _, err := recovered.ConsumeJournalGrantAttempt(t.Context(), record.JournalID, record.OperationID, digest); !errors.Is(err, ErrRuntimeStartupGrantConsumed) {
		t.Fatalf("recovery consumed again: %v", err)
	}
	if len(recovered.owners) != 0 {
		t.Fatal("recovery reconstructed original owner")
	}
}

func TestRuntimeStartupGrantRejectsUnboundConsumption(t *testing.T) {
	_, config := newRemoteReservationFixture(t, false)
	ledger, directory := startupTestLedger(t)
	reservation, err := ledger.ReserveRemote(t.Context(), config)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(directory, runtimeStartupLedgerName)
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	cancelled, cancel := context.WithCancel(t.Context())
	cancel()
	for _, tc := range []struct {
		name               string
		ctx                context.Context
		journal, operation uuid.UUID
		digest             [32]byte
	}{
		{"wrong-operation", t.Context(), reservation.JournalID, uuid.New(), [32]byte{1}},
		{"wrong-journal", t.Context(), uuid.New(), reservation.OperationID, [32]byte{1}},
		{"no-evidence", t.Context(), reservation.JournalID, reservation.OperationID, [32]byte{}},
		{"cancelled", cancelled, reservation.JournalID, reservation.OperationID, [32]byte{1}},
		{"nil-context", nil, reservation.JournalID, reservation.OperationID, [32]byte{1}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got, err := ledger.ConsumeJournalGrantAttempt(tc.ctx, tc.journal, tc.operation, tc.digest); err == nil || got != (RuntimeStartupGrantAttempt{}) {
				t.Fatal("invalid request accepted")
			}
		})
	}
	after, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(before, after) {
		t.Fatal("invalid consumption changed state")
	}
	if _, err := ledger.ConsumeJournalGrantAttempt(t.Context(), reservation.JournalID, reservation.OperationID, [32]byte{1}); err != nil {
		t.Fatalf("invalid request poisoned legitimate consumption: %v", err)
	}
}

func TestRuntimeStartupGrantConcurrentConsumption(t *testing.T) {
	_, config := newRemoteReservationFixture(t, false)
	ledger, _ := startupTestLedger(t)
	reservation, err := ledger.ReserveRemote(t.Context(), config)
	if err != nil {
		t.Fatal(err)
	}
	var group sync.WaitGroup
	results := make(chan error, 8)
	for range 8 {
		group.Go(func() {
			_, err := ledger.ConsumeJournalGrantAttempt(t.Context(), reservation.JournalID, reservation.OperationID, [32]byte{1})
			results <- err
		})
	}
	group.Wait()
	close(results)
	successes := 0
	for err := range results {
		if err == nil {
			successes++
		} else if !errors.Is(err, ErrRuntimeStartupGrantConsumed) {
			t.Fatalf("unexpected refusal: %v", err)
		}
	}
	if successes != 1 {
		t.Fatalf("successful consumptions=%d", successes)
	}
}

func TestRuntimeStartupGrantUncertainAppend(t *testing.T) {
	for _, phase := range []string{"before-append", "after-append", "after-sync"} {
		t.Run(phase, func(t *testing.T) {
			_, config := newRemoteReservationFixture(t, false)
			ledger, directory := startupTestLedger(t)
			reservation, err := ledger.ReserveRemote(t.Context(), config)
			if err != nil {
				t.Fatal(err)
			}
			ledger.boundary = func(at string) error {
				if at == phase {
					return errors.New("injected grant append failure")
				}
				return nil
			}
			if record, err := ledger.ConsumeJournalGrantAttempt(t.Context(), reservation.JournalID, reservation.OperationID, [32]byte{1}); err == nil || record != (RuntimeStartupGrantAttempt{}) {
				t.Fatal("uncertain append returned usable result")
			}
			ledger.boundary = nil
			if _, err := ledger.ConsumeJournalGrantAttempt(t.Context(), reservation.JournalID, reservation.OperationID, [32]byte{1}); err == nil {
				t.Fatal("uncertain live handle retried")
			}
			if err := ledger.Close(); err != nil {
				t.Fatal(err)
			}
			recovered, err := OpenRuntimeStartupLedger(t.Context(), directory, "cpu-node", false)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = recovered.Close() }()
			_, err = recovered.InspectJournalGrantAttempt(t.Context(), reservation.JournalID)
			if phase == "before-append" && !errors.Is(err, os.ErrNotExist) || phase != "before-append" && err != nil {
				t.Fatalf("unexpected recovered history: %v", err)
			}
			if _, err := recovered.ConsumeJournalGrantAttempt(t.Context(), reservation.JournalID, reservation.OperationID, [32]byte{1}); err == nil {
				t.Fatal("recovery reused consumed startup")
			}
		})
	}
}

func TestRuntimeStartupGrantRecoveryRejectsTampering(t *testing.T) {
	for _, mode := range []string{"operation", "startup-digest", "reservation-digest", "empty-evidence", "time", "duplicate", "before-reservation", "schema2"} {
		t.Run(mode, func(t *testing.T) {
			_, config := newRemoteReservationFixture(t, false)
			ledger, directory := startupTestLedger(t)
			reservation, err := ledger.ReserveRemote(t.Context(), config)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := ledger.ConsumeJournalGrantAttempt(t.Context(), reservation.JournalID, reservation.OperationID, [32]byte{1}); err != nil {
				t.Fatal(err)
			}
			if err := ledger.Close(); err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(directory, runtimeStartupLedgerName)
			wire, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			lines := bytes.Split(bytes.TrimSuffix(wire, []byte{'\n'}), []byte{'\n'})
			var entry runtimeStartupEntry
			if err := json.Unmarshal(lines[3], &entry); err != nil {
				t.Fatal(err)
			}
			switch mode {
			case "operation":
				entry.GrantAttempt.OperationID = uuid.New()
			case "startup-digest":
				entry.GrantAttempt.StartupDigest[0] ^= 1
			case "reservation-digest":
				entry.GrantAttempt.ReservationDigest[0] ^= 1
			case "empty-evidence":
				entry.GrantAttempt.AuthorizationDigest = [32]byte{}
			case "time":
				entry.GrantAttempt.RecordedAt = time.Time{}
			}
			lines[3], err = json.Marshal(entry)
			if err != nil {
				t.Fatal(err)
			}
			switch mode {
			case "duplicate":
				lines = append(lines, lines[3])
			case "before-reservation":
				lines[2], lines[3] = lines[3], lines[2]
			case "schema2":
				lines[0] = bytes.Replace(lines[0], []byte(`"schema_version":3`), []byte(`"schema_version":2`), 1)
			}
			if err := os.WriteFile(path, append(bytes.Join(lines, []byte{'\n'}), '\n'), 0o600); err != nil {
				t.Fatal(err)
			}
			if recovered, err := OpenRuntimeStartupLedger(t.Context(), directory, "cpu-node", false); err == nil {
				_ = recovered.Close()
				t.Fatal("tampered history accepted")
			}
		})
	}
}

func TestRuntimeStartupGrantRequiresFreshLiveOwner(t *testing.T) {
	for _, mode := range []string{"schema2", "reopened", "exited", "missing-reservation"} {
		t.Run(mode, func(t *testing.T) {
			f, config := newRemoteReservationFixture(t, false)
			ledger, directory := startupTestLedger(t)
			reservation, err := ledger.ReserveRemote(t.Context(), config)
			if err != nil {
				t.Fatal(err)
			}
			switch mode {
			case "reopened", "schema2", "missing-reservation":
				if err := ledger.Close(); err != nil {
					t.Fatal(err)
				}
				if mode != "reopened" {
					path := filepath.Join(directory, runtimeStartupLedgerName)
					wire, err := os.ReadFile(path)
					if err != nil {
						t.Fatal(err)
					}
					if mode == "schema2" {
						wire = bytes.Replace(wire, []byte(`"schema_version":3`), []byte(`"schema_version":2`), 1)
					} else {
						lines := bytes.SplitAfter(wire, []byte{'\n'})
						wire = bytes.Join(lines[:2], nil)
					}
					if err := os.WriteFile(path, wire, 0o600); err != nil {
						t.Fatal(err)
					}
				}
				ledger, err = OpenRuntimeStartupLedger(t.Context(), directory, "cpu-node", false)
				if err != nil {
					t.Fatal(err)
				}
				defer func() { _ = ledger.Close() }()
			case "exited":
				if err := f.process.Kill(); err != nil {
					t.Fatal(err)
				}
				ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
				defer cancel()
				for {
					_, err := ledger.RecordExit(ctx, reservation.JournalID)
					if err == nil {
						break
					}
					if !errors.Is(err, ErrRuntimeNamespaceOwnerLive) {
						t.Fatal(err)
					}
					select {
					case <-ctx.Done():
						t.Fatal(ctx.Err())
					case <-time.After(time.Millisecond):
					}
				}
			}
			if _, err := ledger.ConsumeJournalGrantAttempt(t.Context(), reservation.JournalID, reservation.OperationID, [32]byte{1}); err == nil {
				t.Fatal("non-fresh or non-live attempt consumed")
			}
		})
	}
}
