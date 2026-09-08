package workerbootstrap

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"syscall"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/fleet"
)

func TestInspectProvisionedJournalsRetainsLocksAndOriginalEvidence(t *testing.T) {
	config, directory, registry := provisionFixture(t)
	want, err := Provision(t.Context(), config, directory, registry)
	mustDo(t, err)
	before := snapshotFiles(t, directory)
	queries := 0
	reader := historyReaderFunc(func(_ context.Context, id uuid.UUID) (fleet.WorkerBootstrapHistory, error) {
		queries++
		if id != want.RequestID {
			t.Fatal("inspection changed original Registry request")
		}
		for _, name := range []string{provisionIntentName, "scratch/bootstrap/" + operationName,
			"scratch/worker-admission/assignment-admission.lock", "scratch/runtime-admission/execution-admission.lock"} {
			file, err := os.Open(filepath.Join(directory, name))
			mustDo(t, err)
			lockErr := syscall.Flock(int(file.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
			mustDo(t, file.Close())
			if !errors.Is(lockErr, syscall.EWOULDBLOCK) {
				t.Fatalf("inspection did not retain %s: %v", name, lockErr)
			}
		}
		return recordedHistory(config, registry), nil
	})
	for range 2 {
		got, err := InspectProvisionedJournals(t.Context(), config, directory, reader)
		if err != nil || got != want || !reflect.DeepEqual(before, snapshotFiles(t, directory)) || registry.claimCalls != 1 || registry.receiptCalls != 1 {
			t.Fatalf("inspection altered authority or storage: %+v %v", got, err)
		}
	}
	if queries != 2 {
		t.Fatal("inspection did not independently read Registry each time")
	}
}

func TestInspectProvisionedJournalsRejectsInterruptedHandover(t *testing.T) {
	for _, stop := range []string{"intent-durable", "roots-created", "journals-prepared", "origin-durable",
		"after-handover:runtime-admission/execution-admission.lock", "after-handover:.", "handover-durable"} {
		t.Run(stop, func(t *testing.T) {
			config, directory, registry := provisionFixture(t)
			_, err := provision(t.Context(), config, directory, registry, func(phase string) error {
				if phase == stop {
					return errors.New("lost completion")
				}
				return nil
			})
			if err == nil {
				t.Fatal("interruption boundary not reached")
			}
			before := snapshotFiles(t, directory)
			got, err := InspectProvisionedJournals(t.Context(), config, directory, constantHistory(recordedHistory(config, registry)))
			if stop == "handover-durable" {
				if err != nil || got.RequestID != registry.claim.RequestID || got.ProvisionID == uuid.Nil {
					t.Fatalf("lost completion could not be inspected: %+v %v", got, err)
				}
			} else if err == nil || got != (ProvisionedJournals{}) {
				t.Fatalf("partial handover was adopted: %+v %v", got, err)
			}
			if !reflect.DeepEqual(before, snapshotFiles(t, directory)) {
				t.Fatal("inspection repaired interrupted state")
			}
		})
	}
}

func TestInspectProvisionedJournalsRejectsLiveOwnersWithoutLeakingLocks(t *testing.T) {
	config, directory, registry := provisionFixture(t)
	want, err := Provision(t.Context(), config, directory, registry)
	mustDo(t, err)
	queries := 0
	reader := historyReaderFunc(func(context.Context, uuid.UUID) (fleet.WorkerBootstrapHistory, error) {
		queries++
		return recordedHistory(config, registry), nil
	})
	for _, name := range []string{provisionIntentName, "scratch/bootstrap/" + operationName,
		"scratch/worker-admission/assignment-admission.lock", "scratch/runtime-admission/execution-admission.lock"} {
		t.Run(name, func(t *testing.T) {
			file, err := os.Open(filepath.Join(directory, name))
			mustDo(t, err)
			mustDo(t, syscall.Flock(int(file.Fd()), syscall.LOCK_EX|syscall.LOCK_NB))
			before := queries
			got, err := InspectProvisionedJournals(t.Context(), config, directory, reader)
			mustDo(t, file.Close())
			if err == nil || got != (ProvisionedJournals{}) || queries != before {
				t.Fatalf("inspection accepted a live owner: %+v %v", got, err)
			}
			got, err = InspectProvisionedJournals(t.Context(), config, directory, reader)
			if err != nil || got != want || queries != before+1 {
				t.Fatalf("failed inspection leaked ownership: %+v %v", got, err)
			}
		})
	}
}

func TestInspectProvisionedJournalsRejectsChangedStateAndHistory(t *testing.T) {
	for _, fault := range []string{"intent-scope", "duplicate-json", "unknown-json", "root-mode", "record-owner", "record-link",
		"record-replaced", "journal-replaced", "journal-bytes", "journal-symlink", "journal-mode", "journal-owner", "extra-file",
		"missing-receipt", "abandoned", "pair", "timestamp", "node", "actor", "max-records", "lookup-error", "canceled"} {
		t.Run(fault, func(t *testing.T) {
			config, directory, registry := provisionFixture(t)
			_, err := Provision(t.Context(), config, directory, registry)
			mustDo(t, err)
			history := recordedHistory(config, registry)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			journal := filepath.Join(directory, "scratch/runtime-admission/execution-admission.json")
			intent := filepath.Join(directory, provisionIntentName)
			switch fault {
			case "intent-scope":
				wire, err := os.ReadFile(intent)
				mustDo(t, err)
				var value provisionIntent
				mustDo(t, json.Unmarshal(wire, &value))
				value.Node = "other-node"
				wire, err = json.Marshal(value)
				mustDo(t, err)
				mustDo(t, os.WriteFile(intent, wire, 0o600))
			case "duplicate-json", "unknown-json":
				wire, err := os.ReadFile(intent)
				mustDo(t, err)
				key := "schema_version"
				if fault == "unknown-json" {
					key = "unrecognized"
				}
				wire = append([]byte("{\""+key+"\":1,"), wire[1:]...)
				mustDo(t, os.WriteFile(intent, wire, 0o600))
			case "root-mode":
				mustDo(t, os.Chmod(directory, 0o755))
			case "record-owner":
				mustDo(t, os.Chown(intent, provisionOwner, provisionOwner))
			case "record-link":
				mustDo(t, os.Link(intent, filepath.Join(t.TempDir(), "intent-link")))
			case "record-replaced", "journal-replaced":
				path := intent
				if fault == "journal-replaced" {
					path = journal
				}
				wire, err := os.ReadFile(path)
				mustDo(t, err)
				// Hold the old inode so this tests replacement, not inode reuse.
				old, err := os.Open(path)
				mustDo(t, err)
				t.Cleanup(func() { mustDo(t, old.Close()) })
				mustDo(t, os.Remove(path))
				mustDo(t, os.WriteFile(path, wire, 0o600))
				if fault == "journal-replaced" {
					mustDo(t, os.Chown(path, provisionOwner, provisionOwner))
				}
			case "journal-bytes":
				mustDo(t, os.WriteFile(journal, []byte("{}"), 0o600))
			case "journal-symlink":
				mustDo(t, os.Remove(journal))
				mustDo(t, os.Symlink(intent, journal))
			case "journal-mode":
				mustDo(t, os.Chmod(journal, 0o640))
			case "journal-owner":
				mustDo(t, os.Chown(journal, 0, 0))
			case "extra-file":
				mustDo(t, os.WriteFile(filepath.Join(directory, "scratch/inputs/extra"), []byte("x"), 0o600))
			case "missing-receipt":
				history.Receipt = nil
			case "abandoned":
				history.Abandonment = &fleet.WorkerBootstrapAbandonment{FencedInstanceEpoch: 2, AbandonedAt: time.Now()}
			case "pair":
				history.Receipt.RuntimeJournalID = uuid.New()
			case "timestamp":
				history.RecordedAt = history.RecordedAt.Add(time.Second)
			case "node":
				history.Claim.NodeIdentity = "other-node"
			case "actor":
				history.ActorIdentity = "other-actor"
			case "max-records":
				config.MaxRecords++
			case "canceled":
				cancel()
			}
			reader := constantHistory(history)
			if fault == "lookup-error" {
				reader = historyReaderFunc(func(context.Context, uuid.UUID) (fleet.WorkerBootstrapHistory, error) {
					return fleet.WorkerBootstrapHistory{}, errors.New("unavailable")
				})
			}
			before := snapshotFiles(t, directory)
			got, err := InspectProvisionedJournals(ctx, config, directory, reader)
			if err == nil || got != (ProvisionedJournals{}) || !reflect.DeepEqual(before, snapshotFiles(t, directory)) {
				t.Fatalf("inspection accepted or repaired changed state: %+v %v", got, err)
			}
		})
	}
}

func TestInspectProvisionedJournalsRechecksAfterLookup(t *testing.T) {
	for _, fault := range []string{"journal", "record", "extra", "root", "cancel"} {
		t.Run(fault, func(t *testing.T) {
			config, directory, registry := provisionFixture(t)
			_, err := Provision(t.Context(), config, directory, registry)
			mustDo(t, err)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			var afterFault map[string]string
			reader := historyReaderFunc(func(context.Context, uuid.UUID) (fleet.WorkerBootstrapHistory, error) {
				switch fault {
				case "journal":
					mustDo(t, os.WriteFile(filepath.Join(directory, "scratch/runtime-admission/execution-admission.json"), []byte("{}"), 0o600))
				case "record":
					mustDo(t, os.WriteFile(filepath.Join(directory, provisionDoneName), []byte("{}"), 0o600))
				case "extra":
					mustDo(t, os.WriteFile(filepath.Join(directory, "scratch/outputs/extra"), []byte("x"), 0o600))
				case "root":
					mustDo(t, os.Chmod(directory, 0o755))
				case "cancel":
					cancel()
				}
				afterFault = snapshotFiles(t, directory)
				return recordedHistory(config, registry), nil
			})
			got, err := InspectProvisionedJournals(ctx, config, directory, reader)
			if afterFault == nil || err == nil || got != (ProvisionedJournals{}) || !reflect.DeepEqual(afterFault, snapshotFiles(t, directory)) {
				t.Fatalf("inspection did not reject mutation at lookup: %+v %v", got, err)
			}
		})
	}
}
