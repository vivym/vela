package workerbootstrap

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/fleet"
	"github.com/vivym/vela/internal/modelruntime"
	"github.com/vivym/vela/internal/stageworkeragent"
)

// HistoryReader cannot acquire initialization permission or report a new pair.
type HistoryReader interface {
	LookupWorkerBootstrap(context.Context, uuid.UUID) (fleet.WorkerBootstrapHistory, error)
}

// ReconcileRecordedPair can recreate missing local pair metadata only from an
// existing Registry receipt and the original independently recovered journals.
// It holds both journal lifetime locks through lookup, comparison and publication.
// It never creates an operation, initializes a journal or repairs malformed data.
func ReconcileRecordedPair(ctx context.Context, config Config, reader HistoryReader) (Result, error) {
	return reconcileRecordedPair(ctx, config, reader, nil)
}

func reconcileRecordedPair(ctx context.Context, config Config, reader HistoryReader, boundary func(string) error) (result Result, err error) {
	if ctx == nil || reader == nil {
		return Result{}, errors.New("worker bootstrap reconciliation requires context and Registry history")
	}
	p, err := bind(config)
	if err != nil {
		return Result{}, err
	}
	checkpoint := func(name string) error {
		if err := context.Cause(ctx); err != nil {
			return err
		}
		if boundary != nil {
			return boundary(name)
		}
		return nil
	}
	if err := checkpoint("preflight"); err != nil {
		return Result{}, err
	}
	local, err := openOperation(p, false)
	if err != nil {
		return Result{}, err
	}
	defer func() {
		err = errors.Join(err, local.close())
		if err != nil {
			result = Result{}
		}
	}()
	p.request.RequestID = local.operation.RequestID
	pair, err := local.readPair()
	missing := errors.Is(err, os.ErrNotExist)
	if err != nil && !missing {
		return Result{}, fmt.Errorf("inspect retained Worker bootstrap pair: %w", err)
	}
	if err := local.validate(); err != nil {
		return Result{}, err
	}
	err = stageworkeragent.WithPreparedAssignmentJournal(ctx, p.worker, func(worker stageworkeragent.AssignmentJournalStatus) error {
		return modelruntime.WithPreparedExecutionJournal(ctx, p.launch, config.Validator,
			modelruntime.ExecutionFloorStateConfig{Directory: local.paths[runtimeRoot]}, func(runtime modelruntime.ExecutionJournalStatus) error {
				if err := checkpoint("journals-locked"); err != nil {
					return err
				}
				history, err := reader.LookupWorkerBootstrap(ctx, p.request.RequestID)
				if err != nil {
					return fmt.Errorf("lookup recorded Worker bootstrap pair: %w", err)
				}
				expected, err := p.recordedPair(history)
				if err != nil {
					return err
				}
				if worker.JournalID != expected.WorkerID || worker.Scope != expected.WorkerScope ||
					runtime.JournalID != expected.RuntimeID || runtime.Scope != expected.RuntimeScope || !missing && pair != expected {
					return errors.New("worker bootstrap journals or local pair differ from Registry history")
				}
				if err := checkpoint("history-validated"); err != nil {
					return err
				}
				if err := local.validate(); err != nil {
					return err
				}
				if missing {
					if err := local.writePair(expected); err != nil {
						return err
					}
				}
				if err := checkpoint("pair-durable"); err != nil {
					return err
				}
				if err := local.validate(); err != nil {
					return err
				}
				result = Result{RequestID: expected.RequestID, Worker: worker, Runtime: runtime, RecordedAt: history.RecordedAt}
				return nil
			})
	})
	return result, err
}

func (p preparation) recordedPair(history fleet.WorkerBootstrapHistory) (journalPair, error) {
	if history.Claim.Fresh || !p.matchesClaim(history.Claim) || history.ActorIdentity != p.config.ActorIdentity || history.Abandonment != nil {
		return journalPair{}, errors.New("worker bootstrap history differs from retained operation")
	}
	receipt := history.Receipt
	if receipt == nil {
		return journalPair{}, fmt.Errorf("%w: Registry has no recorded journal pair", ErrIncomplete)
	}
	if receipt.RequestID != p.request.RequestID || receipt.ActorIdentity != p.config.ActorIdentity ||
		receipt.WorkerJournalID == uuid.Nil || receipt.RuntimeJournalID == uuid.Nil || receipt.WorkerJournalID == receipt.RuntimeJournalID ||
		len(receipt.WorkerScope) != sha256.Size || len(receipt.RuntimeScope) != sha256.Size || history.RecordedAt.IsZero() {
		return journalPair{}, errors.New("worker bootstrap Registry pair is incomplete or invalid")
	}
	pair := journalPair{RequestID: receipt.RequestID, WorkerID: receipt.WorkerJournalID, WorkerScope: [sha256.Size]byte(receipt.WorkerScope),
		RuntimeID: receipt.RuntimeJournalID, RuntimeScope: [sha256.Size]byte(receipt.RuntimeScope)}
	if pair.WorkerScope == ([sha256.Size]byte{}) || pair.RuntimeScope == ([sha256.Size]byte{}) {
		return journalPair{}, errors.New("worker bootstrap Registry pair has no journal scope")
	}
	return pair, nil
}
