package webhook

import (
	"context"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
)

const webhookReceiptTimeout = 5 * time.Second

type DispatcherConfig struct {
	InstanceID      string
	BatchSize       int32
	ClaimTTL        time.Duration
	DeliveryTimeout time.Duration
}

type BatchResult struct {
	Claimed   int
	Delivered int
	Failed    int
}

type Dispatcher struct {
	pool            *pgxpool.Pool
	sealer          SecretSealer
	adapter         DeliveryAdapter
	instanceID      string
	batchSize       int32
	claimSeconds    int32
	deliveryTimeout time.Duration
}

func NewDispatcher(
	pool *pgxpool.Pool,
	sealer SecretSealer,
	adapter DeliveryAdapter,
	config DispatcherConfig,
) (*Dispatcher, error) {
	if pool == nil {
		return nil, errors.New("webhook Dispatcher database pool is required")
	}
	if sealer == nil {
		return nil, errors.New("webhook Dispatcher secret sealer is required")
	}
	if adapter == nil {
		return nil, errors.New("webhook Dispatcher adapter is required")
	}
	if len(config.InstanceID) < 1 || len(config.InstanceID) > 200 {
		return nil, errors.New("webhook Dispatcher instance id must contain between 1 and 200 bytes")
	}
	if config.BatchSize < 1 || config.BatchSize > 1000 {
		return nil, errors.New("webhook Dispatcher batch size must be between 1 and 1000")
	}
	claimSeconds, ok := webhookDurationSeconds(config.ClaimTTL)
	if !ok || claimSeconds > 3600 {
		return nil, errors.New("webhook Dispatcher claim TTL must be between one second and one hour")
	}
	deliverySeconds, ok := webhookDurationSeconds(config.DeliveryTimeout)
	if !ok || deliverySeconds > 3600 {
		return nil, errors.New("webhook Dispatcher delivery timeout must be between one second and one hour")
	}
	receiptSeconds, _ := webhookDurationSeconds(webhookReceiptTimeout)
	if claimSeconds <= deliverySeconds+receiptSeconds {
		return nil, errors.New("webhook Dispatcher claim TTL must exceed delivery timeout plus receipt timeout")
	}
	return &Dispatcher{
		pool:            pool,
		sealer:          sealer,
		adapter:         adapter,
		instanceID:      config.InstanceID,
		batchSize:       config.BatchSize,
		claimSeconds:    claimSeconds,
		deliveryTimeout: config.DeliveryTimeout,
	}, nil
}

func (d *Dispatcher) DispatchBatch(ctx context.Context) (BatchResult, error) {
	var result BatchResult
	var dispatchErrors []error
	for range d.batchSize {
		current, err := d.dispatchOne(ctx)
		result.Claimed += current.Claimed
		result.Delivered += current.Delivered
		result.Failed += current.Failed
		if err != nil {
			dispatchErrors = append(dispatchErrors, err)
		}
		if current.Claimed == 0 {
			break
		}
	}
	return result, errors.Join(dispatchErrors...)
}

func (d *Dispatcher) dispatchOne(ctx context.Context) (BatchResult, error) {
	rows, err := d.pool.Query(ctx, `
		SELECT delivery_id, organization_id, project_id, subscription_id,
			event_id, endpoint_url, payload, claimed_at, claim_token,
			current_secret_id, current_secret_revision, current_encryption_key_id,
			current_encryption_nonce, current_encrypted_secret,
			previous_secret_id, previous_secret_revision, previous_encryption_key_id,
			previous_encryption_nonce, previous_encrypted_secret
		FROM vela_claim_webhook_deliveries($1, $2, $3)
	`, d.instanceID, d.claimSeconds, 1)
	if err != nil {
		return BatchResult{}, fmt.Errorf("claim Webhook Deliveries: %w", err)
	}
	claims := make([]deliveryClaim, 0, 1)
	for rows.Next() {
		var claim deliveryClaim
		if err := rows.Scan(
			&claim.deliveryID,
			&claim.organizationID,
			&claim.projectID,
			&claim.subscriptionID,
			&claim.eventID,
			&claim.endpoint,
			&claim.payload,
			&claim.claimedAt,
			&claim.claimToken,
			&claim.current.id,
			&claim.current.revision,
			&claim.current.keyID,
			&claim.current.nonce,
			&claim.current.ciphertext,
			&claim.previousID,
			&claim.previousRevision,
			&claim.previousKeyID,
			&claim.previousNonce,
			&claim.previousCiphertext,
		); err != nil {
			rows.Close()
			return BatchResult{}, fmt.Errorf("scan Webhook Delivery claim: %w", err)
		}
		claims = append(claims, claim)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return BatchResult{}, fmt.Errorf("read Webhook Delivery claims: %w", err)
	}
	rows.Close()

	result := BatchResult{Claimed: len(claims)}
	var dispatchErrors []error
	for _, claim := range claims {
		secrets, openErr := d.openSecrets(claim)
		if openErr != nil {
			result.Failed++
			markErr := d.markFailed(ctx, claim, 0, openErr)
			dispatchErrors = append(dispatchErrors, errors.Join(
				fmt.Errorf("open signing secrets for Delivery %s: %w", claim.deliveryID, openErr),
				markErr,
			))
			continue
		}
		deliveryContext, cancelDelivery := context.WithTimeout(ctx, d.deliveryTimeout)
		statusCode, deliveryErr := d.adapter.Deliver(deliveryContext, DeliveryRequest{
			Endpoint:       claim.endpoint,
			SubscriptionID: claim.subscriptionID,
			DeliveryID:     claim.deliveryID,
			EventID:        claim.eventID,
			ClaimedAt:      claim.claimedAt,
			Payload:        claim.payload,
			Secrets:        secrets,
		})
		cancelDelivery()
		for _, secret := range secrets {
			clear(secret)
		}
		if deliveryErr != nil {
			result.Failed++
			markErr := d.markFailed(ctx, claim, statusCode, deliveryErr)
			dispatchErrors = append(dispatchErrors, errors.Join(
				fmt.Errorf("deliver Webhook event %s: %w", claim.eventID, deliveryErr),
				markErr,
			))
			continue
		}
		marked, markErr := d.markDelivered(ctx, claim, statusCode)
		if markErr != nil {
			result.Failed++
			dispatchErrors = append(dispatchErrors, markErr)
			continue
		}
		if !marked {
			result.Failed++
			dispatchErrors = append(dispatchErrors, fmt.Errorf("webhook Delivery %s claim is stale after remote success", claim.deliveryID))
			continue
		}
		result.Delivered++
	}
	return result, errors.Join(dispatchErrors...)
}

func (d *Dispatcher) markFailed(
	ctx context.Context,
	claim deliveryClaim,
	statusCode int,
	deliveryErr error,
) error {
	receiptContext, cancel := context.WithTimeout(context.WithoutCancel(ctx), webhookReceiptTimeout)
	defer cancel()
	var status any
	if statusCode > 0 {
		status = statusCode
	}
	var marked bool
	if err := d.pool.QueryRow(receiptContext, `
		SELECT vela_mark_webhook_failed($1, $2, $3, $4)
	`, claim.deliveryID, claim.claimToken, status, deliveryErr.Error()).Scan(&marked); err != nil {
		return fmt.Errorf("record failed Webhook Delivery %s: %w", claim.deliveryID, err)
	}
	if !marked {
		return fmt.Errorf("webhook Delivery %s claim is stale after remote failure", claim.deliveryID)
	}
	return nil
}

func (d *Dispatcher) markDelivered(
	ctx context.Context,
	claim deliveryClaim,
	statusCode int,
) (bool, error) {
	receiptContext, cancel := context.WithTimeout(context.WithoutCancel(ctx), webhookReceiptTimeout)
	defer cancel()
	var marked bool
	if err := d.pool.QueryRow(receiptContext, `
		SELECT vela_mark_webhook_delivered($1, $2, $3)
	`, claim.deliveryID, claim.claimToken, statusCode).Scan(&marked); err != nil {
		return false, fmt.Errorf("record Webhook Delivery %s: %w", claim.deliveryID, err)
	}
	return marked, nil
}

type encryptedClaimSecret struct {
	id         uuid.UUID
	revision   int32
	keyID      string
	nonce      []byte
	ciphertext []byte
}

type deliveryClaim struct {
	deliveryID         uuid.UUID
	organizationID     uuid.UUID
	projectID          uuid.UUID
	subscriptionID     uuid.UUID
	eventID            uuid.UUID
	endpoint           string
	payload            []byte
	claimedAt          time.Time
	claimToken         uuid.UUID
	current            encryptedClaimSecret
	previousID         pgtype.UUID
	previousRevision   pgtype.Int4
	previousKeyID      pgtype.Text
	previousNonce      []byte
	previousCiphertext []byte
}

func (d *Dispatcher) openSecrets(claim deliveryClaim) ([][]byte, error) {
	current, err := d.sealer.Open(SealedSecret{
		KeyID:      claim.current.keyID,
		Nonce:      claim.current.nonce,
		Ciphertext: claim.current.ciphertext,
	}, secretAssociatedData(
		claim.organizationID,
		claim.projectID,
		claim.subscriptionID,
		claim.current.id,
		claim.current.revision,
	))
	if err != nil {
		return nil, err
	}
	secrets := [][]byte{current}
	if !claim.previousID.Valid {
		return secrets, nil
	}
	previous, err := d.sealer.Open(SealedSecret{
		KeyID:      claim.previousKeyID.String,
		Nonce:      claim.previousNonce,
		Ciphertext: claim.previousCiphertext,
	}, secretAssociatedData(
		claim.organizationID,
		claim.projectID,
		claim.subscriptionID,
		claim.previousID.Bytes,
		claim.previousRevision.Int32,
	))
	if err != nil {
		clear(current)
		return nil, err
	}
	return append(secrets, previous), nil
}

func webhookDurationSeconds(duration time.Duration) (int32, bool) {
	if duration <= 0 {
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
