package workerbootstrap

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/vivym/vela/internal/fleet"
	"github.com/vivym/vela/internal/modelruntime"
	"github.com/vivym/vela/internal/stageworkeragent"
)

type receiptAuthority struct {
	Authority
	record func(context.Context, fleet.WorkerBootstrapReceipt) (time.Time, error)
}

func (authority receiptAuthority) RecordWorkerBootstrapReceipt(ctx context.Context, receipt fleet.WorkerBootstrapReceipt) (time.Time, error) {
	return authority.record(ctx, receipt)
}

func TestPrepareRetainsBothJournalLocksThroughReceipt(t *testing.T) {
	config, registry := bootstrapFixture(t)
	p, err := bind(config)
	mustDo(t, err)
	checkLocked := func(ctx context.Context) {
		t.Helper()
		if _, err := stageworkeragent.PrepareAssignmentJournal(ctx, p.worker); err == nil {
			t.Error("Worker journal ownership was released before receipt completion")
		}
		if _, err := modelruntime.PrepareExecutionJournal(ctx, p.launch, config.Validator,
			modelruntime.ExecutionFloorStateConfig{Directory: filepath.Join(config.ScratchDirectory, "runtime-admission")}); err == nil {
			t.Error("Runtime journal ownership was released before receipt completion")
		}
	}
	authority := receiptAuthority{Authority: registry, record: func(ctx context.Context, receipt fleet.WorkerBootstrapReceipt) (time.Time, error) {
		checkLocked(ctx)
		return registry.RecordWorkerBootstrapReceipt(ctx, receipt)
	}}
	var first Result
	var before map[string]string
	for _, attempt := range []string{"fresh", "replay"} {
		t.Run(attempt, func(t *testing.T) {
			result, err := prepare(t.Context(), config, authority, func(phase string) error {
				if phase == "receipt-committed" {
					checkLocked(t.Context())
				}
				return nil
			})
			mustDo(t, err)
			if attempt == "fresh" {
				first = result
				before = snapshotFiles(t, config.ScratchDirectory)
			} else if result != first || !reflect.DeepEqual(before, snapshotFiles(t, config.ScratchDirectory)) {
				t.Fatal("receipt replay changed journal history or the original result")
			}
			worker, err := stageworkeragent.PrepareAssignmentJournal(t.Context(), p.worker)
			mustDo(t, err)
			runtime, err := modelruntime.PrepareExecutionJournal(t.Context(), p.launch, config.Validator,
				modelruntime.ExecutionFloorStateConfig{Directory: filepath.Join(config.ScratchDirectory, "runtime-admission")})
			mustDo(t, err)
			if worker != result.Worker || runtime != result.Runtime {
				t.Fatal("receipt did not describe the original recoverable journal pair")
			}
		})
	}
	if registry.claimCalls != 1 || registry.receiptCalls != 2 {
		t.Fatalf("receipt replay repeated first use: claims=%d receipts=%d", registry.claimCalls, registry.receiptCalls)
	}
}

func TestPrepareRejectsJournalReplacementDuringReceipt(t *testing.T) {
	for _, journal := range []string{"worker-admission/assignment-admission.json", "runtime-admission/execution-admission.json"} {
		for _, phase := range []string{"record-call", "receipt-committed"} {
			t.Run(journal+"/"+phase, func(t *testing.T) {
				config, registry := bootstrapFixture(t)
				path := filepath.Join(config.ScratchDirectory, journal)
				replace := func() {
					wire, err := os.ReadFile(path)
					mustDo(t, err)
					mustDo(t, os.Rename(path, path+".retained"))
					mustDo(t, os.WriteFile(path, wire, 0o600))
				}
				authority := receiptAuthority{Authority: registry, record: func(ctx context.Context, receipt fleet.WorkerBootstrapReceipt) (time.Time, error) {
					if phase == "record-call" {
						replace()
					}
					return registry.RecordWorkerBootstrapReceipt(ctx, receipt)
				}}
				result, err := prepare(t.Context(), config, authority, func(current string) error {
					if phase == "receipt-committed" && current == phase {
						replace()
					}
					return nil
				})
				if err == nil || result != (Result{}) {
					t.Fatalf("replaced journal produced successful preparation: %+v %v", result, err)
				}
				if registry.receiptCalls != 1 {
					t.Fatal("replacement test did not cross the committed receipt boundary")
				}
				mustDo(t, os.Remove(path))
				mustDo(t, os.Rename(path+".retained", path))
				before := snapshotFiles(t, config.ScratchDirectory)
				result, err = Prepare(t.Context(), config, registry)
				if err != nil || result.Worker.JournalID != registry.receipt.WorkerJournalID || result.Runtime.JournalID != registry.receipt.RuntimeJournalID ||
					registry.claimCalls != 1 || registry.receiptCalls != 2 || !reflect.DeepEqual(before, snapshotFiles(t, config.ScratchDirectory)) {
					t.Fatalf("original pair could not recover the committed receipt: %+v %v", result, err)
				}
			})
		}
	}
}
