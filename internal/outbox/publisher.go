package outbox

import (
	"context"
	"errors"
	"fmt"
	"math"
	"regexp"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	store "github.com/vivym/vela/internal/store/sqlc"
	"github.com/vivym/vela/internal/tracing"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

var eventTypePattern = regexp.MustCompile(`^[A-Za-z0-9_-]+(?:\.[A-Za-z0-9_-]+)*$`)

type Receipt struct {
	Stream   string
	Sequence int64
}

type Broker interface {
	Publish(ctx context.Context, subject, messageID string, payload []byte) (Receipt, error)
}

type Config struct {
	InstanceID string
	BatchSize  int32
	ClaimTTL   time.Duration
	RetryDelay time.Duration
}

type Publisher struct {
	pool              *pgxpool.Pool
	broker            Broker
	instanceID        string
	batchSize         int32
	claimSeconds      int32
	retryAfterSeconds int32
}

func NewPublisher(pool *pgxpool.Pool, broker Broker, config Config) (*Publisher, error) {
	if pool == nil {
		return nil, errors.New("outbox Publisher database pool is required")
	}
	if broker == nil {
		return nil, errors.New("outbox Publisher Broker is required")
	}
	if config.InstanceID == "" {
		return nil, errors.New("outbox Publisher instance id is required")
	}
	if config.BatchSize < 1 || config.BatchSize > 1000 {
		return nil, errors.New("outbox Publisher batch size must be between 1 and 1000")
	}
	claimSeconds, ok := durationSeconds(config.ClaimTTL, false)
	if !ok {
		return nil, errors.New("outbox Publisher claim TTL must be a positive supported duration")
	}
	retryAfterSeconds, ok := durationSeconds(config.RetryDelay, true)
	if !ok {
		return nil, errors.New("outbox Publisher retry delay is outside the supported range")
	}
	return &Publisher{
		pool:              pool,
		broker:            broker,
		instanceID:        config.InstanceID,
		batchSize:         config.BatchSize,
		claimSeconds:      claimSeconds,
		retryAfterSeconds: retryAfterSeconds,
	}, nil
}

func (p *Publisher) PublishBatch(ctx context.Context) (int, error) {
	tx, err := p.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return 0, fmt.Errorf("begin Outbox claim transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	claimToken := uuid.New()
	claimedBy := p.instanceID
	events, err := store.New(tx).ClaimOutboxEvents(ctx, store.ClaimOutboxEventsParams{
		ClaimedBy:    &claimedBy,
		ClaimToken:   uuid.NullUUID{UUID: claimToken, Valid: true},
		ClaimSeconds: p.claimSeconds,
		BatchSize:    p.batchSize,
	})
	if err != nil {
		return 0, fmt.Errorf("claim Outbox events: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("commit Outbox claims: %w", err)
	}

	queries := store.New(p.pool)
	published := 0
	var publishErrors []error
	for _, event := range events {
		if err := p.publishEvent(ctx, queries, event); err != nil {
			publishErrors = append(publishErrors, err)
			continue
		}
		published++
	}
	return published, errors.Join(publishErrors...)
}

func (p *Publisher) publishEvent(ctx context.Context, queries *store.Queries, event store.ClaimOutboxEventsRow) (err error) {
	ctx, span := otel.Tracer("vela/outbox").Start(tracing.WithDurableParent(ctx, event.OriginTraceParent),
		"VELA_EVENTS publish", trace.WithSpanKind(trace.SpanKindProducer), trace.WithAttributes(
			attribute.String("messaging.system", "nats"),
			attribute.String("messaging.destination.name", "VELA_EVENTS"),
			attribute.String("messaging.operation.name", "publish"),
			attribute.String("messaging.message.id", event.EventID.String()),
			attribute.String("vela.job.id", event.AggregateID.String())))
	defer func() {
		if value := recover(); value != nil {
			span.SetStatus(codes.Error, "publish panic")
			span.End()
			panic(value)
		}
		if err != nil {
			span.SetStatus(codes.Error, "publish failed")
		}
		span.End()
	}()
	if !event.ClaimToken.Valid {
		return fmt.Errorf("outbox event %s has no claim token", event.EventID)
	}
	subject, subjectErr := eventSubject(event.EventType)
	if subjectErr != nil {
		return p.markFailed(ctx, queries, event.EventID, event.ClaimToken, subjectErr)
	}
	receipt, publishErr := p.broker.Publish(ctx, subject, event.EventID.String(), event.Payload)
	if publishErr != nil {
		return p.markFailed(ctx, queries, event.EventID, event.ClaimToken, publishErr)
	}
	if receipt.Stream == "" || receipt.Sequence < 1 {
		return p.markFailed(ctx, queries, event.EventID, event.ClaimToken,
			errors.New("broker acknowledgement has no durable stream receipt"))
	}
	rows, markErr := queries.MarkOutboxPublished(ctx, store.MarkOutboxPublishedParams{
		BrokerStream: &receipt.Stream, BrokerSequence: &receipt.Sequence,
		EventID: event.EventID, ClaimToken: event.ClaimToken,
	})
	if markErr != nil {
		return fmt.Errorf("record Outbox PubAck for %s: %w", event.EventID, markErr)
	}
	if rows != 1 {
		return fmt.Errorf("outbox claim for %s is stale after PubAck", event.EventID)
	}
	span.SetAttributes(attribute.Int64("messaging.nats.stream.sequence", receipt.Sequence))
	return nil
}

func (p *Publisher) markFailed(
	ctx context.Context,
	queries *store.Queries,
	eventID uuid.UUID,
	claimToken uuid.NullUUID,
	publishErr error,
) error {
	rows, err := queries.MarkOutboxFailed(ctx, store.MarkOutboxFailedParams{
		RetryAfterSeconds: p.retryAfterSeconds,
		LastError:         publishErr.Error(),
		EventID:           eventID,
		ClaimToken:        claimToken,
	})
	if err != nil {
		return fmt.Errorf("publish Outbox event %s: %v; release claim: %w", eventID, publishErr, err)
	}
	if rows != 1 {
		return fmt.Errorf("publish Outbox event %s: %v; claim is stale", eventID, publishErr)
	}
	return fmt.Errorf("publish Outbox event %s: %w", eventID, publishErr)
}

func eventSubject(eventType string) (string, error) {
	if !eventTypePattern.MatchString(eventType) {
		return "", fmt.Errorf("outbox event type %q cannot form a NATS subject", eventType)
	}
	return "vela.events." + eventType, nil
}

func durationSeconds(duration time.Duration, allowZero bool) (int32, bool) {
	if duration < 0 || (!allowZero && duration == 0) {
		return 0, false
	}
	seconds := duration / time.Second
	if duration%time.Second != 0 {
		seconds++
	}
	if seconds > time.Duration(math.MaxInt32) {
		return 0, false
	}
	return int32(seconds), true
}
