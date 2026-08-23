package webhook

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/vivym/vela/internal/identity"
	store "github.com/vivym/vela/internal/store/sqlc"
)

const signingSecretBytes = 32

type EventType string

const (
	EventJobSucceeded EventType = "job.succeeded"
	EventJobFailed    EventType = "job.failed"
	EventJobCanceled  EventType = "job.canceled"
)

type SubscriptionState string

const (
	SubscriptionActive   SubscriptionState = "ACTIVE"
	SubscriptionDisabled SubscriptionState = "DISABLED"
)

type DeliveryState string

const (
	DeliveryPending    DeliveryState = "PENDING"
	DeliveryInFlight   DeliveryState = "IN_FLIGHT"
	DeliveryDelivered  DeliveryState = "DELIVERED"
	DeliveryDeadLetter DeliveryState = "DEAD_LETTER"
)

type Subscription struct {
	ID             uuid.UUID
	OrganizationID uuid.UUID
	ProjectID      uuid.UUID
	Endpoint       string
	EventTypes     []EventType
	State          SubscriptionState
	SecretRevision int32
	CreatedAt      time.Time
	DisabledAt     *time.Time
}

type CreatedSubscription struct {
	Subscription  Subscription
	SigningSecret string
}

type RotatedSubscription struct {
	Subscription             Subscription
	SigningSecret            string
	PreviousSecretValidUntil time.Time
}

type CreateRequest struct {
	Endpoint   string
	EventTypes []EventType
}

type Delivery struct {
	ID              uuid.UUID
	EventID         uuid.UUID
	EventType       EventType
	JobID           uuid.UUID
	JobVersion      int64
	State           DeliveryState
	Generation      int32
	Attempts        int32
	RetryDeadlineAt time.Time
	LastAttemptAt   *time.Time
	DeliveredAt     *time.Time
	DeadLetteredAt  *time.Time
	LastHTTPStatus  *int32
	LastError       *string
	CreatedAt       time.Time
	UpdatedAt       time.Time
}

type FailureCode string

const (
	FailureUnauthorized   FailureCode = "unauthorized"
	FailureForbidden      FailureCode = "forbidden"
	FailureInvalidRequest FailureCode = "invalid_request"
	FailureConflict       FailureCode = "conflict"
	FailureNotFound       FailureCode = "not_found"
)

type Failure struct {
	Code    FailureCode
	Message string
}

func (f *Failure) Error() string {
	return f.Message
}

type Service struct {
	pool     *pgxpool.Pool
	sealer   SecretSealer
	resolver AddressResolver
}

func NewService(
	pool *pgxpool.Pool,
	sealer SecretSealer,
	resolver AddressResolver,
) (*Service, error) {
	if pool == nil {
		return nil, errors.New("webhook Service database pool is required")
	}
	if sealer == nil {
		return nil, errors.New("webhook Service secret sealer is required")
	}
	if resolver == nil {
		return nil, errors.New("webhook Service DNS resolver is required")
	}
	return &Service{pool: pool, sealer: sealer, resolver: resolver}, nil
}

func (s *Service) Create(
	ctx context.Context,
	principal identity.Principal,
	projectID uuid.UUID,
	request CreateRequest,
) (CreatedSubscription, error) {
	if principal.CredentialID == uuid.Nil {
		return CreatedSubscription{}, failure(FailureUnauthorized, "valid Service Principal credential is required")
	}
	if !principal.HasScope(identity.ScopeWebhooksManage) || principal.ProjectID != projectID {
		return CreatedSubscription{}, failure(FailureForbidden, "credential cannot manage webhooks for this Project")
	}
	if err := validateEndpointForRegistration(ctx, request.Endpoint, s.resolver); err != nil {
		return CreatedSubscription{}, failure(FailureInvalidRequest, err.Error())
	}
	eventTypes, err := validateEventTypes(request.EventTypes)
	if err != nil {
		return CreatedSubscription{}, failure(FailureInvalidRequest, err.Error())
	}

	subscriptionID := uuid.New()
	secretID := uuid.New()
	secretBytes := make([]byte, signingSecretBytes)
	if _, err := rand.Read(secretBytes); err != nil {
		return CreatedSubscription{}, fmt.Errorf("generate webhook signing secret: %w", err)
	}
	signingSecret := "vwhsec_" + base64.RawURLEncoding.EncodeToString(secretBytes)
	clear(secretBytes)
	plaintext := []byte(signingSecret)
	sealed, err := s.sealer.Seal(
		plaintext,
		secretAssociatedData(principal.OrganizationID, projectID, subscriptionID, secretID, 1),
	)
	clear(plaintext)
	if err != nil {
		return CreatedSubscription{}, fmt.Errorf("encrypt webhook signing secret: %w", err)
	}

	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return CreatedSubscription{}, fmt.Errorf("begin Webhook Subscription transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	queries := store.New(tx)
	requestContext, err := queries.SetRequestContext(ctx, store.SetRequestContextParams{
		CredentialID:    principal.CredentialID,
		CredentialProof: principal.RequestContextProof(),
		RequiredScope:   identity.ScopeWebhooksManage,
	})
	if err != nil {
		return CreatedSubscription{}, mapDatabaseFailure(err)
	}
	if requestContext.OrganizationID != principal.OrganizationID ||
		requestContext.ProjectID != principal.ProjectID ||
		requestContext.PrincipalID != principal.PrincipalID {
		return CreatedSubscription{}, errors.New("webhook request identity context does not match authenticated Principal")
	}

	eventValues := make([]string, len(eventTypes))
	for index, eventType := range eventTypes {
		eventValues[index] = string(eventType)
	}
	var subscription Subscription
	var returnedEventTypes []string
	var returnedState string
	err = tx.QueryRow(ctx, `
		SELECT id, organization_id, project_id, endpoint_url, event_types,
			state, secret_revision, created_at
		FROM vela_create_webhook_subscription(
			$1, $2, $3, $4::webhook_event_type[], $5, $6, $7, $8
		)
	`,
		subscriptionID,
		projectID,
		request.Endpoint,
		eventValues,
		secretID,
		sealed.KeyID,
		sealed.Nonce,
		sealed.Ciphertext,
	).Scan(
		&subscription.ID,
		&subscription.OrganizationID,
		&subscription.ProjectID,
		&subscription.Endpoint,
		&returnedEventTypes,
		&returnedState,
		&subscription.SecretRevision,
		&subscription.CreatedAt,
	)
	if err != nil {
		return CreatedSubscription{}, mapDatabaseFailure(err)
	}
	subscription.EventTypes = make([]EventType, len(returnedEventTypes))
	for index, eventType := range returnedEventTypes {
		subscription.EventTypes[index] = EventType(eventType)
	}
	subscription.State = SubscriptionState(returnedState)
	if err := tx.Commit(ctx); err != nil {
		return CreatedSubscription{}, fmt.Errorf("commit Webhook Subscription: %w", err)
	}
	return CreatedSubscription{Subscription: subscription, SigningSecret: signingSecret}, nil
}

func (s *Service) RotateSecret(
	ctx context.Context,
	principal identity.Principal,
	projectID, subscriptionID uuid.UUID,
) (RotatedSubscription, error) {
	if principal.CredentialID == uuid.Nil {
		return RotatedSubscription{}, failure(FailureUnauthorized, "valid Service Principal credential is required")
	}
	if !principal.HasScope(identity.ScopeWebhooksManage) || principal.ProjectID != projectID {
		return RotatedSubscription{}, failure(FailureForbidden, "credential cannot manage webhooks for this Project")
	}
	if subscriptionID == uuid.Nil {
		return RotatedSubscription{}, failure(FailureInvalidRequest, "Webhook Subscription id is required")
	}

	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return RotatedSubscription{}, fmt.Errorf("begin webhook secret rotation transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	queries := store.New(tx)
	requestContext, err := queries.SetRequestContext(ctx, store.SetRequestContextParams{
		CredentialID:    principal.CredentialID,
		CredentialProof: principal.RequestContextProof(),
		RequiredScope:   identity.ScopeWebhooksManage,
	})
	if err != nil {
		return RotatedSubscription{}, mapDatabaseFailure(err)
	}
	if requestContext.OrganizationID != principal.OrganizationID ||
		requestContext.ProjectID != principal.ProjectID ||
		requestContext.PrincipalID != principal.PrincipalID {
		return RotatedSubscription{}, errors.New("webhook request identity context does not match authenticated Principal")
	}

	var nextRevision int32
	if err := tx.QueryRow(ctx, `
		SELECT vela_lock_webhook_secret_rotation($1, $2)
	`, projectID, subscriptionID).Scan(&nextRevision); err != nil {
		return RotatedSubscription{}, mapDatabaseFailure(err)
	}
	secretID := uuid.New()
	secretBytes := make([]byte, signingSecretBytes)
	if _, err := rand.Read(secretBytes); err != nil {
		return RotatedSubscription{}, fmt.Errorf("generate webhook signing secret: %w", err)
	}
	signingSecret := "vwhsec_" + base64.RawURLEncoding.EncodeToString(secretBytes)
	clear(secretBytes)
	plaintext := []byte(signingSecret)
	sealed, err := s.sealer.Seal(
		plaintext,
		secretAssociatedData(
			principal.OrganizationID,
			projectID,
			subscriptionID,
			secretID,
			nextRevision,
		),
	)
	clear(plaintext)
	if err != nil {
		return RotatedSubscription{}, fmt.Errorf("encrypt webhook signing secret: %w", err)
	}

	var result RotatedSubscription
	var returnedEventTypes []string
	var returnedState string
	err = tx.QueryRow(ctx, `
		SELECT id, organization_id, project_id, endpoint_url, event_types,
			state, secret_revision, created_at, previous_secret_valid_until
		FROM vela_rotate_webhook_secret($1, $2, $3, $4, $5, $6, $7)
	`,
		projectID,
		subscriptionID,
		secretID,
		nextRevision,
		sealed.KeyID,
		sealed.Nonce,
		sealed.Ciphertext,
	).Scan(
		&result.Subscription.ID,
		&result.Subscription.OrganizationID,
		&result.Subscription.ProjectID,
		&result.Subscription.Endpoint,
		&returnedEventTypes,
		&returnedState,
		&result.Subscription.SecretRevision,
		&result.Subscription.CreatedAt,
		&result.PreviousSecretValidUntil,
	)
	if err != nil {
		return RotatedSubscription{}, mapDatabaseFailure(err)
	}
	result.Subscription.EventTypes = make([]EventType, len(returnedEventTypes))
	for index, eventType := range returnedEventTypes {
		result.Subscription.EventTypes[index] = EventType(eventType)
	}
	result.Subscription.State = SubscriptionState(returnedState)
	result.SigningSecret = signingSecret
	if err := tx.Commit(ctx); err != nil {
		return RotatedSubscription{}, fmt.Errorf("commit webhook secret rotation: %w", err)
	}
	return result, nil
}

func (s *Service) Replay(
	ctx context.Context,
	principal identity.Principal,
	projectID, subscriptionID, deliveryID uuid.UUID,
) (Delivery, error) {
	if principal.CredentialID == uuid.Nil {
		return Delivery{}, failure(FailureUnauthorized, "valid Service Principal credential is required")
	}
	if !principal.HasScope(identity.ScopeWebhooksManage) || principal.ProjectID != projectID {
		return Delivery{}, failure(FailureForbidden, "credential cannot manage webhooks for this Project")
	}
	if subscriptionID == uuid.Nil || deliveryID == uuid.Nil {
		return Delivery{}, failure(FailureInvalidRequest, "Webhook Subscription and Delivery ids are required")
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return Delivery{}, fmt.Errorf("begin Webhook Delivery replay transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	queries := store.New(tx)
	requestContext, err := queries.SetRequestContext(ctx, store.SetRequestContextParams{
		CredentialID:    principal.CredentialID,
		CredentialProof: principal.RequestContextProof(),
		RequiredScope:   identity.ScopeWebhooksManage,
	})
	if err != nil {
		return Delivery{}, mapDatabaseFailure(err)
	}
	if requestContext.OrganizationID != principal.OrganizationID ||
		requestContext.ProjectID != principal.ProjectID ||
		requestContext.PrincipalID != principal.PrincipalID {
		return Delivery{}, errors.New("webhook request identity context does not match authenticated Principal")
	}

	var delivery Delivery
	var eventType, state string
	err = tx.QueryRow(ctx, `
		SELECT id, event_id, event_type, job_id, job_version, state, generation,
			attempts, retry_deadline_at, created_at, updated_at
		FROM vela_replay_webhook_delivery($1, $2, $3, $4)
	`, projectID, subscriptionID, deliveryID, uuid.New()).Scan(
		&delivery.ID,
		&delivery.EventID,
		&eventType,
		&delivery.JobID,
		&delivery.JobVersion,
		&state,
		&delivery.Generation,
		&delivery.Attempts,
		&delivery.RetryDeadlineAt,
		&delivery.CreatedAt,
		&delivery.UpdatedAt,
	)
	if err != nil {
		return Delivery{}, mapDatabaseFailure(err)
	}
	delivery.EventType = EventType(eventType)
	delivery.State = DeliveryState(state)
	if err := tx.Commit(ctx); err != nil {
		return Delivery{}, fmt.Errorf("commit Webhook Delivery replay: %w", err)
	}
	return delivery, nil
}

func (s *Service) Disable(
	ctx context.Context,
	principal identity.Principal,
	projectID, subscriptionID uuid.UUID,
) (Subscription, error) {
	if principal.CredentialID == uuid.Nil {
		return Subscription{}, failure(FailureUnauthorized, "valid Service Principal credential is required")
	}
	if !principal.HasScope(identity.ScopeWebhooksManage) || principal.ProjectID != projectID {
		return Subscription{}, failure(FailureForbidden, "credential cannot manage webhooks for this Project")
	}
	if subscriptionID == uuid.Nil {
		return Subscription{}, failure(FailureInvalidRequest, "Webhook Subscription id is required")
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return Subscription{}, fmt.Errorf("begin Webhook Subscription disable transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	queries := store.New(tx)
	requestContext, err := queries.SetRequestContext(ctx, store.SetRequestContextParams{
		CredentialID:    principal.CredentialID,
		CredentialProof: principal.RequestContextProof(),
		RequiredScope:   identity.ScopeWebhooksManage,
	})
	if err != nil {
		return Subscription{}, mapDatabaseFailure(err)
	}
	if requestContext.OrganizationID != principal.OrganizationID ||
		requestContext.ProjectID != principal.ProjectID ||
		requestContext.PrincipalID != principal.PrincipalID {
		return Subscription{}, errors.New("webhook request identity context does not match authenticated Principal")
	}

	var subscription Subscription
	var eventTypes []string
	var state string
	var disabledAt pgtype.Timestamptz
	err = tx.QueryRow(ctx, `
		SELECT id, organization_id, project_id, endpoint_url, event_types, state,
			secret_revision, created_at, disabled_at
		FROM vela_disable_webhook_subscription($1, $2)
	`, projectID, subscriptionID).Scan(
		&subscription.ID,
		&subscription.OrganizationID,
		&subscription.ProjectID,
		&subscription.Endpoint,
		&eventTypes,
		&state,
		&subscription.SecretRevision,
		&subscription.CreatedAt,
		&disabledAt,
	)
	if err != nil {
		return Subscription{}, mapDatabaseFailure(err)
	}
	subscription.EventTypes = make([]EventType, len(eventTypes))
	for index, eventType := range eventTypes {
		subscription.EventTypes[index] = EventType(eventType)
	}
	subscription.State = SubscriptionState(state)
	if disabledAt.Valid {
		disabledTime := disabledAt.Time
		subscription.DisabledAt = &disabledTime
	}
	if err := tx.Commit(ctx); err != nil {
		return Subscription{}, fmt.Errorf("commit Webhook Subscription disable: %w", err)
	}
	return subscription, nil
}

func (s *Service) List(
	ctx context.Context,
	principal identity.Principal,
	projectID uuid.UUID,
	limit int32,
) ([]Subscription, error) {
	if principal.CredentialID == uuid.Nil {
		return nil, failure(FailureUnauthorized, "valid Service Principal credential is required")
	}
	if !principal.HasScope(identity.ScopeWebhooksRead) || principal.ProjectID != projectID {
		return nil, failure(FailureForbidden, "credential cannot read webhooks for this Project")
	}
	if limit < 1 || limit > 100 {
		return nil, failure(FailureInvalidRequest, "Webhook Subscription list limit must be between 1 and 100")
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return nil, fmt.Errorf("begin Webhook Subscription list transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	queries := store.New(tx)
	requestContext, err := queries.SetRequestContext(ctx, store.SetRequestContextParams{
		CredentialID:    principal.CredentialID,
		CredentialProof: principal.RequestContextProof(),
		RequiredScope:   identity.ScopeWebhooksRead,
	})
	if err != nil {
		return nil, mapDatabaseFailure(err)
	}
	if requestContext.OrganizationID != principal.OrganizationID ||
		requestContext.ProjectID != principal.ProjectID ||
		requestContext.PrincipalID != principal.PrincipalID {
		return nil, errors.New("webhook request identity context does not match authenticated Principal")
	}
	rows, err := tx.Query(ctx, `
		SELECT id, organization_id, project_id, endpoint_url, event_types, state,
			secret_revision, created_at, disabled_at
		FROM vela_list_webhook_subscriptions($1, $2)
	`, projectID, limit)
	if err != nil {
		return nil, mapDatabaseFailure(err)
	}
	defer rows.Close()
	subscriptions := make([]Subscription, 0)
	for rows.Next() {
		var subscription Subscription
		var eventTypes []string
		var state string
		var disabledAt pgtype.Timestamptz
		if err := rows.Scan(
			&subscription.ID,
			&subscription.OrganizationID,
			&subscription.ProjectID,
			&subscription.Endpoint,
			&eventTypes,
			&state,
			&subscription.SecretRevision,
			&subscription.CreatedAt,
			&disabledAt,
		); err != nil {
			return nil, fmt.Errorf("scan Webhook Subscription: %w", err)
		}
		subscription.EventTypes = make([]EventType, len(eventTypes))
		for index, eventType := range eventTypes {
			subscription.EventTypes[index] = EventType(eventType)
		}
		subscription.State = SubscriptionState(state)
		if disabledAt.Valid {
			disabledTime := disabledAt.Time
			subscription.DisabledAt = &disabledTime
		}
		subscriptions = append(subscriptions, subscription)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read Webhook Subscriptions: %w", err)
	}
	rows.Close()
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit Webhook Subscription list: %w", err)
	}
	return subscriptions, nil
}

func (s *Service) ListDeliveries(
	ctx context.Context,
	principal identity.Principal,
	projectID, subscriptionID uuid.UUID,
	limit int32,
) ([]Delivery, error) {
	if principal.CredentialID == uuid.Nil {
		return nil, failure(FailureUnauthorized, "valid Service Principal credential is required")
	}
	if !principal.HasScope(identity.ScopeWebhooksRead) || principal.ProjectID != projectID {
		return nil, failure(FailureForbidden, "credential cannot read webhooks for this Project")
	}
	if subscriptionID == uuid.Nil || limit < 1 || limit > 100 {
		return nil, failure(FailureInvalidRequest, "valid Webhook Subscription id and list limit are required")
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return nil, fmt.Errorf("begin Webhook Delivery list transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	queries := store.New(tx)
	requestContext, err := queries.SetRequestContext(ctx, store.SetRequestContextParams{
		CredentialID:    principal.CredentialID,
		CredentialProof: principal.RequestContextProof(),
		RequiredScope:   identity.ScopeWebhooksRead,
	})
	if err != nil {
		return nil, mapDatabaseFailure(err)
	}
	if requestContext.OrganizationID != principal.OrganizationID ||
		requestContext.ProjectID != principal.ProjectID ||
		requestContext.PrincipalID != principal.PrincipalID {
		return nil, errors.New("webhook request identity context does not match authenticated Principal")
	}
	rows, err := tx.Query(ctx, `
		SELECT id, event_id, event_type, job_id, job_version, state, generation,
			attempts, retry_deadline_at, last_attempt_at, delivered_at,
			dead_lettered_at, last_http_status, created_at, updated_at
		FROM vela_list_webhook_deliveries($1, $2, $3)
	`, projectID, subscriptionID, limit)
	if err != nil {
		return nil, mapDatabaseFailure(err)
	}
	defer rows.Close()
	deliveries := make([]Delivery, 0)
	for rows.Next() {
		var delivery Delivery
		var eventType, state string
		var lastAttemptAt, deliveredAt, deadLetteredAt pgtype.Timestamptz
		var lastHTTPStatus pgtype.Int4
		if err := rows.Scan(
			&delivery.ID,
			&delivery.EventID,
			&eventType,
			&delivery.JobID,
			&delivery.JobVersion,
			&state,
			&delivery.Generation,
			&delivery.Attempts,
			&delivery.RetryDeadlineAt,
			&lastAttemptAt,
			&deliveredAt,
			&deadLetteredAt,
			&lastHTTPStatus,
			&delivery.CreatedAt,
			&delivery.UpdatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan Webhook Delivery: %w", err)
		}
		delivery.EventType = EventType(eventType)
		delivery.State = DeliveryState(state)
		delivery.LastAttemptAt = timestamptzPointer(lastAttemptAt)
		delivery.DeliveredAt = timestamptzPointer(deliveredAt)
		delivery.DeadLetteredAt = timestamptzPointer(deadLetteredAt)
		if lastHTTPStatus.Valid {
			value := lastHTTPStatus.Int32
			delivery.LastHTTPStatus = &value
		}
		deliveries = append(deliveries, delivery)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read Webhook Deliveries: %w", err)
	}
	rows.Close()
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit Webhook Delivery list: %w", err)
	}
	return deliveries, nil
}

func timestamptzPointer(value pgtype.Timestamptz) *time.Time {
	if !value.Valid {
		return nil
	}
	timestamp := value.Time
	return &timestamp
}

func validateEventTypes(eventTypes []EventType) ([]EventType, error) {
	if len(eventTypes) < 1 || len(eventTypes) > 3 {
		return nil, errors.New("webhook Subscription requires between one and three event types")
	}
	seen := make(map[EventType]struct{}, len(eventTypes))
	for _, eventType := range eventTypes {
		switch eventType {
		case EventJobSucceeded, EventJobFailed, EventJobCanceled:
		default:
			return nil, fmt.Errorf("unsupported webhook event type %q", eventType)
		}
		if _, duplicate := seen[eventType]; duplicate {
			return nil, fmt.Errorf("duplicate webhook event type %q", eventType)
		}
		seen[eventType] = struct{}{}
	}
	normalized := append([]EventType(nil), eventTypes...)
	sort.Slice(normalized, func(left, right int) bool { return normalized[left] < normalized[right] })
	return normalized, nil
}

func secretAssociatedData(
	organizationID, projectID, subscriptionID, secretID uuid.UUID,
	revision int32,
) []byte {
	return []byte(strings.Join([]string{
		organizationID.String(),
		projectID.String(),
		subscriptionID.String(),
		secretID.String(),
		fmt.Sprint(revision),
	}, "|"))
}

func mapDatabaseFailure(err error) error {
	var postgresError *pgconn.PgError
	if !errors.As(err, &postgresError) {
		return err
	}
	switch postgresError.Code {
	case "28000":
		return failure(FailureUnauthorized, "webhook request credential is invalid or inactive")
	case "42501":
		return failure(FailureForbidden, "webhook resource is outside the authenticated Project")
	case "23514", "22P02":
		return failure(FailureInvalidRequest, "webhook request violates the service contract")
	case "23505":
		return failure(FailureConflict, "webhook request conflicts with current state")
	case "40001", "55000":
		return failure(FailureConflict, "webhook resource has a conflicting active transition")
	case "P0002":
		switch postgresError.ConstraintName {
		case "webhook_subscription_not_found":
			return failure(FailureNotFound, "Webhook Subscription was not found")
		case "webhook_delivery_not_found":
			return failure(FailureNotFound, "Webhook Delivery was not found")
		default:
			return err
		}
	default:
		return err
	}
}

func failure(code FailureCode, message string) *Failure {
	return &Failure{Code: code, Message: message}
}
