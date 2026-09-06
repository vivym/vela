package workerbootstrap

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/fleet"
	"github.com/vivym/vela/internal/modelruntime"
	"github.com/vivym/vela/internal/stageworkeragent"
)

type historyReaderFunc func(context.Context, uuid.UUID) (fleet.WorkerBootstrapHistory, error)

func (read historyReaderFunc) LookupWorkerBootstrap(ctx context.Context, id uuid.UUID) (fleet.WorkerBootstrapHistory, error) {
	return read(ctx, id)
}

func recordedHistory(config Config, registry *fakeAuthority) fleet.WorkerBootstrapHistory {
	claim := registry.claim
	claim.Fresh = false
	claim.BundleDigest = bytes.Clone(claim.BundleDigest)
	history := fleet.WorkerBootstrapHistory{Claim: claim, ActorIdentity: config.ActorIdentity}
	if registry.receipt.RequestID != uuid.Nil {
		receipt := registry.receipt
		receipt.WorkerScope, receipt.RuntimeScope = bytes.Clone(receipt.WorkerScope), bytes.Clone(receipt.RuntimeScope)
		history.Receipt, history.RecordedAt = &receipt, claim.ClaimedAt
	}
	return history
}

func constantHistory(history fleet.WorkerBootstrapHistory) HistoryReader {
	return historyReaderFunc(func(_ context.Context, id uuid.UUID) (fleet.WorkerBootstrapHistory, error) {
		if id != history.Claim.RequestID {
			return fleet.WorkerBootstrapHistory{}, errors.New("unexpected request")
		}
		return history, nil
	})
}

func TestReconcileRecordedPairRestoresOnlyRegistryMetadataWithBothLocksHeld(t *testing.T) {
	config, registry := bootstrapFixture(t)
	first, err := Prepare(t.Context(), config, registry)
	mustDo(t, err)
	pairPath := filepath.Join(config.ScratchDirectory, "bootstrap", pairName)
	originalPair, err := os.ReadFile(pairPath)
	mustDo(t, err)
	mustDo(t, os.Remove(pairPath))
	before := snapshotFiles(t, config.ScratchDirectory)
	p, err := bind(config)
	mustDo(t, err)
	queries := 0
	reader := historyReaderFunc(func(ctx context.Context, id uuid.UUID) (fleet.WorkerBootstrapHistory, error) {
		queries++
		if id != first.RequestID {
			t.Fatal("reconciliation changed operation identity")
		}
		if _, err := stageworkeragent.PrepareAssignmentJournal(ctx, p.worker); err == nil {
			t.Fatal("Worker lifetime lock was released during history lookup")
		}
		if _, err := modelruntime.PrepareExecutionJournal(ctx, p.launch, config.Validator,
			modelruntime.ExecutionFloorStateConfig{Directory: filepath.Join(config.ScratchDirectory, "runtime-admission")}); err == nil {
			t.Fatal("Runtime lifetime lock was released during history lookup")
		}
		return recordedHistory(config, registry), nil
	})
	result, err := ReconcileRecordedPair(t.Context(), config, reader)
	if err != nil || result != first || queries != 1 || registry.claimCalls != 1 || registry.receiptCalls != 1 {
		t.Fatalf("recorded pair reconciliation: %+v %v", result, err)
	}
	after := snapshotFiles(t, config.ScratchDirectory)
	delete(after, pairPath)
	if !reflect.DeepEqual(before, after) {
		t.Fatal("reconciliation modified journal identities, content or operation")
	}
	if restored, err := os.ReadFile(pairPath); err != nil || !bytes.Equal(originalPair, restored) {
		t.Fatalf("reconciliation did not restore original pair bytes: %v", err)
	}
	before = snapshotFiles(t, config.ScratchDirectory)
	result, err = ReconcileRecordedPair(t.Context(), config, reader)
	if err != nil || result != first || !reflect.DeepEqual(before, snapshotFiles(t, config.ScratchDirectory)) {
		t.Fatalf("idempotent reconciliation changed retained state: %+v %v", result, err)
	}
}

func TestReconcileCannotCreateAuthorityOrCompleteUnrecordedInitialization(t *testing.T) {
	for _, stop := range []string{"absent", "operation-durable", "claim-committed", "worker-prepared", "runtime-prepared", "pair-durable"} {
		t.Run(stop, func(t *testing.T) {
			config, registry := bootstrapFixture(t)
			if stop != "absent" {
				_, err := prepare(t.Context(), config, registry, func(phase string) error {
					if phase == stop {
						return errors.New("interrupted")
					}
					return nil
				})
				if err == nil {
					t.Fatal("preparation did not stop")
				}
			}
			before := snapshotFiles(t, config.ScratchDirectory)
			result, err := ReconcileRecordedPair(t.Context(), config, constantHistory(recordedHistory(config, registry)))
			if err == nil || result != (Result{}) || registry.receiptCalls != 0 || !reflect.DeepEqual(before, snapshotFiles(t, config.ScratchDirectory)) {
				t.Fatalf("reconciliation invented missing authority/state: %+v %v", result, err)
			}
		})
	}
}

func TestReconcileRejectsMismatchedOrUnrecordedRegistryHistory(t *testing.T) {
	for _, fault := range []string{"permission", "request", "worker", "member", "epoch", "node", "actor", "digest", "unrecorded", "abandoned", "receipt-actor", "receipt-request", "worker-journal", "runtime-scope", "empty-scope", "timestamp"} {
		t.Run(fault, func(t *testing.T) {
			config, registry := bootstrapFixture(t)
			_, err := Prepare(t.Context(), config, registry)
			mustDo(t, err)
			mustDo(t, os.Remove(filepath.Join(config.ScratchDirectory, "bootstrap", pairName)))
			history := recordedHistory(config, registry)
			switch fault {
			case "permission":
				history.Claim.Fresh = true
			case "request":
				history.Claim.RequestID = uuid.New()
			case "worker":
				history.Claim.WorkerInstanceID = uuid.New()
			case "member":
				history.Claim.WorkerMemberID = uuid.New()
			case "epoch":
				history.Claim.WorkerInstanceEpoch++
			case "node":
				history.Claim.NodeIdentity = "other-node"
			case "actor":
				history.ActorIdentity = "other-agent"
			case "digest":
				history.Claim.BundleDigest[0] ^= 0xff
			case "unrecorded":
				history.Receipt = nil
			case "abandoned":
				history.Abandonment = &fleet.WorkerBootstrapAbandonment{FencedInstanceEpoch: 2, AbandonedAt: time.Now()}
			case "receipt-actor":
				history.Receipt.ActorIdentity = "other-agent"
			case "receipt-request":
				history.Receipt.RequestID = uuid.New()
			case "worker-journal":
				history.Receipt.WorkerJournalID = uuid.New()
			case "runtime-scope":
				history.Receipt.RuntimeScope[0] ^= 0xff
			case "empty-scope":
				history.Receipt.WorkerScope = nil
			case "timestamp":
				history.RecordedAt = time.Time{}
			}
			before := snapshotFiles(t, config.ScratchDirectory)
			reader := historyReaderFunc(func(context.Context, uuid.UUID) (fleet.WorkerBootstrapHistory, error) { return history, nil })
			result, err := ReconcileRecordedPair(t.Context(), config, reader)
			if err == nil || result != (Result{}) || !reflect.DeepEqual(before, snapshotFiles(t, config.ScratchDirectory)) {
				t.Fatalf("mismatched history recreated a pair: %+v %v", result, err)
			}
		})
	}
}

func TestReconcilePreservesMalformedOrConflictingLocalState(t *testing.T) {
	for _, fault := range []string{"operation", "pair-empty", "pair-conflicting", "worker", "runtime"} {
		t.Run(fault, func(t *testing.T) {
			config, registry := bootstrapFixture(t)
			_, err := Prepare(t.Context(), config, registry)
			mustDo(t, err)
			path := filepath.Join(config.ScratchDirectory, "bootstrap", pairName)
			data := []byte(nil)
			switch fault {
			case "operation":
				path = filepath.Join(config.ScratchDirectory, "bootstrap", operationName)
			case "pair-conflicting":
				encoded, err := os.ReadFile(path)
				mustDo(t, err)
				var pair journalPair
				mustDo(t, json.Unmarshal(encoded, &pair))
				pair.WorkerID = uuid.New()
				data, err = json.Marshal(pair)
				mustDo(t, err)
			case "worker":
				path = filepath.Join(config.ScratchDirectory, "worker-admission", "assignment-admission.json")
			case "runtime":
				path = filepath.Join(config.ScratchDirectory, "runtime-admission", "execution-admission.json")
			}
			mustDo(t, os.WriteFile(path, data, 0o600))
			before := snapshotFiles(t, config.ScratchDirectory)
			result, err := ReconcileRecordedPair(t.Context(), config, constantHistory(recordedHistory(config, registry)))
			if err == nil || result != (Result{}) || !reflect.DeepEqual(before, snapshotFiles(t, config.ScratchDirectory)) {
				t.Fatalf("reconciliation replaced uncertain local state: %+v %v", result, err)
			}
		})
	}
}

func TestReconcileInterruptionReleasesLocksAndRetainsOriginalJournals(t *testing.T) {
	for _, stop := range []string{"journals-locked", "history-validated", "pair-durable"} {
		t.Run(stop, func(t *testing.T) {
			config, registry := bootstrapFixture(t)
			first, err := Prepare(t.Context(), config, registry)
			mustDo(t, err)
			mustDo(t, os.Remove(filepath.Join(config.ScratchDirectory, "bootstrap", pairName)))
			reader := constantHistory(recordedHistory(config, registry))
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			result, err := reconcileRecordedPair(ctx, config, reader, func(phase string) error {
				if phase == stop {
					cancel()
					return ctx.Err()
				}
				return nil
			})
			if !errors.Is(err, context.Canceled) || result != (Result{}) {
				t.Fatalf("interrupted reconciliation: %+v %v", result, err)
			}
			result, err = ReconcileRecordedPair(t.Context(), config, reader)
			if err != nil || result != first {
				t.Fatalf("reconciliation after interruption: %+v %v", result, err)
			}
		})
	}
}

func TestReconcileRecordedPairSurvivesAbruptProcessExit(t *testing.T) {
	for _, stop := range []string{"history-validated", "pair-durable"} {
		t.Run(stop, func(t *testing.T) {
			config, registry := bootstrapFixture(t)
			first, err := Prepare(t.Context(), config, registry)
			mustDo(t, err)
			mustDo(t, os.Remove(filepath.Join(config.ScratchDirectory, "bootstrap", pairName)))
			wireConfig := config
			wireConfig.Validator = nil
			document, err := json.Marshal(reconcileProcessInput{Config: wireConfig, History: recordedHistory(config, registry)})
			mustDo(t, err)
			command := exec.CommandContext(t.Context(), os.Args[0], "-test.run=^TestReconcileProcessExitHelper$")
			command.Env = append(os.Environ(), "VELA_RECONCILE_EXIT_HELPER="+stop)
			command.Stdin = bytes.NewReader(document)
			output, err := command.CombinedOutput()
			var exit *exec.ExitError
			if !errors.As(err, &exit) || exit.ExitCode() != 38 {
				t.Fatalf("reconciliation helper did not exit: %s %v", output, err)
			}
			result, err := ReconcileRecordedPair(t.Context(), config, constantHistory(recordedHistory(config, registry)))
			if err != nil || result != first || registry.claimCalls != 1 || registry.receiptCalls != 1 {
				t.Fatalf("process death changed operation or pair: %+v %v", result, err)
			}
		})
	}
}

type reconcileProcessInput struct {
	Config  Config
	History fleet.WorkerBootstrapHistory
}

func TestReconcileProcessExitHelper(t *testing.T) {
	stop := os.Getenv("VELA_RECONCILE_EXIT_HELPER")
	if stop == "" {
		return
	}
	var input reconcileProcessInput
	mustDo(t, json.NewDecoder(os.Stdin).Decode(&input))
	input.Config.Validator = testValidator(t)
	_, err := reconcileRecordedPair(t.Context(), input.Config, constantHistory(input.History), func(phase string) error {
		if phase == stop {
			os.Exit(38)
		}
		return nil
	})
	t.Fatalf("reconciliation exit boundary not reached: %v", err)
}
