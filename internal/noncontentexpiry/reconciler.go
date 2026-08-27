package noncontentexpiry

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const maxBatchSize = 1000

type expiryKind string

const (
	kindJobMetadata           expiryKind = "JOB_METADATA"
	kindJobFinancial          expiryKind = "JOB_FINANCIAL"
	kindOrganizationFinancial expiryKind = "ORGANIZATION_FINANCIAL"
)

type completionOutcome string

const (
	outcomeExpired completionOutcome = "EXPIRED"
	outcomeHeld    completionOutcome = "HELD"
	outcomeStale   completionOutcome = "STALE"
)

type Config struct {
	InstanceID string
	BatchSize  int
	ClaimTTL   time.Duration
	HeldRetry  time.Duration
}

type Result struct {
	Claimed int
	Expired int
	Held    int
	Stale   int
}

type rowQuerier interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}

type Reconciler struct {
	database         rowQuerier
	instanceID       string
	batchSize        int
	claimTTLSeconds  int32
	heldRetrySeconds int32
}

func New(pool *pgxpool.Pool, config Config) (*Reconciler, error) {
	if pool == nil {
		return nil, errors.New("non-content expiry database pool is required")
	}
	return newReconciler(pool, config)
}

func newReconciler(database rowQuerier, config Config) (*Reconciler, error) {
	if database == nil {
		return nil, errors.New("non-content expiry database is required")
	}
	if config.InstanceID == "" || len(config.InstanceID) > 200 ||
		strings.TrimSpace(config.InstanceID) != config.InstanceID ||
		strings.IndexFunc(config.InstanceID, func(character rune) bool {
			return character < 0x20 || character == 0x7f
		}) >= 0 {
		return nil, errors.New("non-content expiry instance id is invalid")
	}
	if config.BatchSize < 1 || config.BatchSize > maxBatchSize {
		return nil, errors.New("non-content expiry batch size must be between 1 and 1000")
	}
	claimTTLSeconds, ok := exactDurationSeconds(config.ClaimTTL, time.Hour)
	if !ok {
		return nil, errors.New("non-content expiry claim TTL must be between one second and one hour")
	}
	heldRetrySeconds, ok := exactDurationSeconds(config.HeldRetry, 24*time.Hour)
	if !ok {
		return nil, errors.New("non-content expiry held retry must be between one second and 24 hours")
	}
	return &Reconciler{
		database: database, instanceID: config.InstanceID, batchSize: config.BatchSize,
		claimTTLSeconds: claimTTLSeconds, heldRetrySeconds: heldRetrySeconds,
	}, nil
}

func (reconciler *Reconciler) ReconcileBatch(ctx context.Context) (Result, error) {
	if reconciler == nil || reconciler.database == nil {
		return Result{}, errors.New("non-content expiry Reconciler is not configured")
	}
	if ctx == nil {
		return Result{}, errors.New("non-content expiry context is required")
	}
	var result Result
	for range reconciler.batchSize {
		claimID := uuid.New()
		var persistedKind string
		var sourceID uuid.UUID
		var returnedClaimID uuid.UUID
		err := reconciler.database.QueryRow(ctx, `
			SELECT kind::text, source_id, claim_id
			FROM vela_claim_non_content_expiry($1, $2, $3)
		`, reconciler.instanceID, claimID, reconciler.claimTTLSeconds).Scan(
			&persistedKind, &sourceID, &returnedClaimID,
		)
		if errors.Is(err, pgx.ErrNoRows) {
			break
		}
		if err != nil {
			return result, fmt.Errorf("claim non-content expiry: %w", err)
		}
		kind := expiryKind(persistedKind)
		if sourceID == uuid.Nil || returnedClaimID != claimID || !validKind(kind) {
			return result, errors.New("claimed non-content expiry identity is invalid")
		}
		result.Claimed++

		var persistedOutcome string
		var receiptID *uuid.UUID
		var deletedSourceCount int32
		err = reconciler.database.QueryRow(ctx, `
			SELECT outcome, receipt_id, deleted_source_count
			FROM vela_complete_non_content_expiry($1, $2, $3, $4)
		`, string(kind), sourceID, claimID, reconciler.heldRetrySeconds).Scan(
			&persistedOutcome, &receiptID, &deletedSourceCount,
		)
		if err != nil {
			return result, fmt.Errorf("complete non-content expiry %s %s: %w", kind, sourceID, err)
		}
		outcome := completionOutcome(persistedOutcome)
		switch outcome {
		case outcomeExpired:
			if receiptID == nil || *receiptID == uuid.Nil || deletedSourceCount < 1 {
				return result, errors.New("completed non-content expiry result is invalid")
			}
			result.Expired++
		case outcomeHeld:
			if receiptID != nil || deletedSourceCount != 0 {
				return result, errors.New("held non-content expiry result is invalid")
			}
			result.Held++
		case outcomeStale:
			if receiptID != nil || deletedSourceCount != 0 {
				return result, errors.New("stale non-content expiry result is invalid")
			}
			result.Stale++
		default:
			return result, fmt.Errorf("unsupported non-content expiry outcome %q", outcome)
		}
	}
	return result, nil
}

func exactDurationSeconds(value time.Duration, maximum time.Duration) (int32, bool) {
	if value < time.Second || value > maximum || value%time.Second != 0 {
		return 0, false
	}
	return int32(value / time.Second), true
}

func validKind(kind expiryKind) bool {
	switch kind {
	case kindJobMetadata, kindJobFinancial, kindOrganizationFinancial:
		return true
	default:
		return false
	}
}
