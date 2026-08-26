package inbox

import (
	"context"
	"errors"
	"fmt"
	"regexp"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	store "github.com/vivym/vela/internal/store/sqlc"
)

var namePattern = regexp.MustCompile(`^[A-Za-z0-9._:-]+$`)

type Event struct {
	ID               uuid.UUID
	OrganizationID   uuid.UUID
	ProjectID        uuid.UUID
	AggregateType    string
	AggregateID      uuid.UUID
	AggregateVersion int64
	Type             string
}

type Handler func(context.Context, pgx.Tx) error

type Processor struct {
	pool           *pgxpool.Pool
	prepareReceipt func(context.Context, *store.Queries, Event) (bool, error)
	recordReceipt  func(context.Context, *store.Queries, Event) (bool, error)
}

func NewProcessor(pool *pgxpool.Pool, consumerName string) (*Processor, error) {
	if pool == nil {
		return nil, errors.New("inbox Processor database pool is required")
	}
	if len(consumerName) == 0 || len(consumerName) > 100 || !namePattern.MatchString(consumerName) {
		return nil, errors.New("inbox Processor consumer name is invalid")
	}
	return &Processor{
		pool: pool,
		recordReceipt: func(ctx context.Context, queries *store.Queries, event Event) (bool, error) {
			return recordGenericReceipt(ctx, queries, consumerName, event)
		},
	}, nil
}

func NewSchedulerProcessor(pool *pgxpool.Pool) (*Processor, error) {
	if pool == nil {
		return nil, errors.New("scheduler Inbox database pool is required")
	}
	return &Processor{
		pool:           pool,
		prepareReceipt: prepareSchedulerReceipt,
		recordReceipt:  recordSchedulerReceipt,
	}, nil
}

func (p *Processor) ProcessOnce(ctx context.Context, event Event, handler Handler) (bool, error) {
	if p == nil || p.pool == nil {
		return false, errors.New("inbox Processor is not configured")
	}
	if handler == nil {
		return false, errors.New("inbox aggregate transition handler is required")
	}
	if err := validateEvent(event); err != nil {
		return false, err
	}

	tx, err := p.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return false, fmt.Errorf("begin Inbox transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	queries := store.New(tx)
	prepared := p.prepareReceipt != nil
	if prepared {
		pending, prepareErr := p.prepareReceipt(ctx, queries, event)
		if prepareErr != nil {
			return false, fmt.Errorf("prepare Inbox receipt: %w", prepareErr)
		}
		if !pending {
			return false, nil
		}
	} else {
		applied, recordErr := p.recordReceipt(ctx, queries, event)
		if recordErr != nil {
			return false, fmt.Errorf("record Inbox receipt: %w", recordErr)
		}
		if !applied {
			return false, nil
		}
	}
	if err := handler(ctx, tx); err != nil {
		return false, fmt.Errorf("apply Inbox aggregate transition: %w", err)
	}
	if prepared {
		applied, recordErr := p.recordReceipt(ctx, queries, event)
		if recordErr != nil {
			return false, fmt.Errorf("record Inbox receipt: %w", recordErr)
		}
		if !applied {
			return false, errors.New("prepared Inbox receipt was not recorded")
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return false, fmt.Errorf("commit Inbox aggregate transition: %w", err)
	}
	return true, nil
}

func prepareSchedulerReceipt(ctx context.Context, queries *store.Queries, event Event) (bool, error) {
	if event.AggregateType != "Job" || event.Type != "job.ready" {
		return false, errors.New("scheduler Inbox event contract is invalid")
	}
	return queries.PrepareSchedulerInboxReceipt(ctx, store.PrepareSchedulerInboxReceiptParams{
		EventID:          event.ID,
		OrganizationID:   event.OrganizationID,
		ProjectID:        event.ProjectID,
		AggregateID:      event.AggregateID,
		AggregateVersion: event.AggregateVersion,
	})
}

func recordGenericReceipt(
	ctx context.Context,
	queries *store.Queries,
	consumerName string,
	event Event,
) (bool, error) {
	_, err := queries.RecordInboxReceipt(ctx, store.RecordInboxReceiptParams{
		ConsumerName:     consumerName,
		EventID:          event.ID,
		OrganizationID:   event.OrganizationID,
		ProjectID:        event.ProjectID,
		AggregateType:    event.AggregateType,
		AggregateID:      event.AggregateID,
		AggregateVersion: event.AggregateVersion,
		EventType:        event.Type,
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	return err == nil, err
}

func recordSchedulerReceipt(ctx context.Context, queries *store.Queries, event Event) (bool, error) {
	if event.AggregateType != "Job" || event.Type != "job.ready" {
		return false, errors.New("scheduler Inbox event contract is invalid")
	}
	return queries.RecordSchedulerInboxReceipt(ctx, store.RecordSchedulerInboxReceiptParams{
		EventID:          event.ID,
		OrganizationID:   event.OrganizationID,
		ProjectID:        event.ProjectID,
		AggregateID:      event.AggregateID,
		AggregateVersion: event.AggregateVersion,
	})
}

func validateEvent(event Event) error {
	if event.ID == uuid.Nil || event.OrganizationID == uuid.Nil || event.ProjectID == uuid.Nil ||
		event.AggregateID == uuid.Nil || event.AggregateVersion < 1 ||
		!namePattern.MatchString(event.AggregateType) || !namePattern.MatchString(event.Type) {
		return errors.New("inbox event identity is invalid")
	}
	return nil
}
