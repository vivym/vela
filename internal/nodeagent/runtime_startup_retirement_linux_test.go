package nodeagent

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/vivym/vela/internal/modelruntime"
)

func TestRuntimeStartupRetirementRequiresMatchingOriginalOwner(t *testing.T) {
	for _, fault := range []string{"exact", "live", "lost-owner", "other-journal", "later-incarnation", "durable-exit"} {
		t.Run(fault, func(t *testing.T) {
			f, config := newRemoteReservationFixture(t, false)
			ledger, directory := startupTestLedger(t)
			if _, err := ledger.Record(t.Context(), f.plan, f.pods, f.observer, f.caller); err != nil {
				t.Fatal(err)
			}
			journal := config.Journal
			if fault == "other-journal" {
				_, other := newRemoteReservationFixture(t, false)
				journal = other.Journal
			}
			if fault == "lost-owner" {
				if err := ledger.Close(); err != nil {
					t.Fatal(err)
				}
				var err error
				ledger, err = OpenRuntimeStartupLedger(t.Context(), directory, "cpu-node", false)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = ledger.Close() })
			}
			if fault != "live" {
				if err := f.process.Kill(); err != nil {
					t.Fatal(err)
				}
			}
			if fault == "later-incarnation" || fault == "durable-exit" {
				exit, err := ledger.recordExitWithRetry(t.Context(), f.request.JournalID)
				if err != nil {
					t.Fatal(err)
				}
				if fault == "later-incarnation" {
					wire, err := json.Marshal(exit.Observation)
					if err != nil {
						t.Fatal(err)
					}
					_, err = journal.RetireBackendIncarnation(t.Context(), modelruntime.BackendRetirementProof{
						IncarnationID: f.request.IncarnationID, LaunchDigest: f.request.LaunchDigest,
						ExitDigest: sha256.Sum256(wire), RetiredAt: exit.Observation.ObservedAt,
					})
					if err != nil {
						t.Fatal(err)
					}
					if _, err := journal.RecordBackendStartupIntent(t.Context()); err != nil {
						t.Fatal(err)
					}
				} else {
					if err := ledger.Close(); err != nil {
						t.Fatal(err)
					}
					ledger, err = OpenRuntimeStartupLedger(t.Context(), directory, "cpu-node", false)
					if err != nil {
						t.Fatal(err)
					}
					t.Cleanup(func() { _ = ledger.Close() })
				}
			}
			ctx, cancel := context.WithTimeout(t.Context(), 300*time.Millisecond)
			defer cancel()
			err := ledger.RetireBackendIncarnation(ctx, f.request.JournalID, journal)
			status, statusErr := journal.Status(t.Context())
			if statusErr != nil {
				t.Fatal(statusErr)
			}
			if fault == "exact" || fault == "durable-exit" {
				if err != nil || status.BackendLifecycle.State != modelruntime.BackendLifecycleRetired {
					t.Fatalf("retirement: %v state=%s", err, status.BackendLifecycle.State)
				}
				if err := ledger.RetireBackendIncarnation(t.Context(), f.request.JournalID, journal); err != nil {
					t.Fatalf("exact retirement replay: %v", err)
				}
				if _, err := ledger.Record(t.Context(), f.plan, f.pods, f.observer, f.caller); !errors.Is(err, ErrRuntimeStartupRecorded) {
					t.Fatalf("retirement reused Fleet startup authority: %v", err)
				}
			} else if err == nil || status.BackendLifecycle.State != modelruntime.BackendLifecycleUnresolved {
				t.Fatalf("%s manufactured retirement: err=%v state=%s", fault, err, status.BackendLifecycle.State)
			}
		})
	}
}
