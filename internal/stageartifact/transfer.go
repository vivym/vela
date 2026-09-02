package stageartifact

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/artifactstore"
)

const maxTransferTicketTTL = 15 * time.Minute

var ErrTransferTicketDestinationMismatch = errors.New("TransferTicket destination mismatch")

type TransferDestination struct {
	WorkerInstanceID    uuid.UUID `json:"worker_instance_id"`
	WorkerInstanceEpoch int64     `json:"worker_instance_epoch"`
	ModelResidencyID    uuid.UUID `json:"model_residency_id"`
	ModelRuntimeEpoch   int64     `json:"model_runtime_epoch"`
	ConnectorRevisionID uuid.UUID `json:"connector_revision_id"`
}

type TransferTicketClaims struct {
	TicketID    uuid.UUID           `json:"ticket_id"`
	Destination TransferDestination `json:"destination"`
	IssuedAt    time.Time           `json:"issued_at"`
	ExpiresAt   time.Time           `json:"expires_at"`
}

type SignedTransferTicket struct {
	Token []byte
}

type IssueTransferRequest struct {
	CommandID    uuid.UUID
	TicketID     uuid.UUID
	ArtifactID   uuid.UUID
	PinID        uuid.UUID
	SigningKeyID string
	Destination  TransferDestination
	IssuedAt     time.Time
	ExpiresAt    time.Time
}

type IssueTransferCommand struct {
	IssueTransferRequest
	TokenDigest [sha256.Size]byte
}

type IssuedTransferTicket struct {
	TicketID  uuid.UUID
	ExpiresAt time.Time
	Replayed  bool
}

type TransferTicketIssuerRepository interface {
	Issue(context.Context, IssueTransferCommand) (IssuedTransferTicket, error)
}

type TransferTicketIssuer struct {
	repository TransferTicketIssuerRepository
	signer     *TransferTicketSigner
}

func NewTransferTicketIssuer(
	repository TransferTicketIssuerRepository,
	signer *TransferTicketSigner,
) (*TransferTicketIssuer, error) {
	if repository == nil || signer == nil {
		return nil, errors.New("TransferTicket issuer configuration is incomplete")
	}
	return &TransferTicketIssuer{repository: repository, signer: signer}, nil
}

func (issuer *TransferTicketIssuer) Issue(
	ctx context.Context,
	request IssueTransferRequest,
) (SignedTransferTicket, error) {
	if issuer == nil || issuer.repository == nil || issuer.signer == nil || ctx == nil {
		return SignedTransferTicket{}, errors.New("TransferTicket issuer is not configured")
	}
	if request.CommandID == uuid.Nil || request.ArtifactID == uuid.Nil ||
		request.PinID == uuid.Nil {
		return SignedTransferTicket{}, errors.New("TransferTicket issue identities are required")
	}
	claims := TransferTicketClaims{
		TicketID: request.TicketID, Destination: request.Destination,
		IssuedAt: request.IssuedAt, ExpiresAt: request.ExpiresAt,
	}
	var ticket SignedTransferTicket
	var err error
	if request.SigningKeyID == "" {
		ticket, err = issuer.signer.Sign(claims)
	} else {
		ticket, err = issuer.signer.SignWithKeyID(request.SigningKeyID, claims)
	}
	if err != nil {
		return SignedTransferTicket{}, err
	}
	issued, err := issuer.repository.Issue(ctx, IssueTransferCommand{
		IssueTransferRequest: request,
		TokenDigest:          sha256.Sum256(ticket.Token),
	})
	if err != nil {
		return SignedTransferTicket{}, fmt.Errorf("persist TransferTicket authority: %w", err)
	}
	if issued.TicketID != request.TicketID || !issued.ExpiresAt.Equal(request.ExpiresAt) {
		return SignedTransferTicket{}, errors.New("persisted TransferTicket identity is mismatched")
	}
	return ticket, nil
}

type transferTicketEnvelope struct {
	SchemaVersion int                  `json:"schema_version"`
	KeyID         string               `json:"key_id"`
	Claims        TransferTicketClaims `json:"claims"`
}

type TransferTicketSigner struct {
	defaultKeyID string
	keys         map[string][]byte
}

func NewTransferTicketSigner(keyID string, key []byte) (*TransferTicketSigner, error) {
	return NewTransferTicketKeyringSigner(keyID, map[string][]byte{keyID: key})
}

func NewTransferTicketKeyringSigner(
	defaultKeyID string,
	keys map[string][]byte,
) (*TransferTicketSigner, error) {
	originalDefaultKeyID := defaultKeyID
	defaultKeyID = strings.TrimSpace(defaultKeyID)
	if defaultKeyID == "" || len(defaultKeyID) > 100 ||
		defaultKeyID != originalDefaultKeyID || len(keys) == 0 {
		return nil, errors.New("TransferTicket signing configuration is invalid")
	}
	validated := make(map[string][]byte, len(keys))
	for keyID, key := range keys {
		if strings.TrimSpace(keyID) != keyID || keyID == "" || len(keyID) > 100 ||
			len(key) < sha256.Size {
			return nil, errors.New("TransferTicket signing configuration is invalid")
		}
		validated[keyID] = bytes.Clone(key)
	}
	if _, ok := validated[defaultKeyID]; !ok {
		return nil, errors.New("TransferTicket default signing key is absent from keyring")
	}
	return &TransferTicketSigner{defaultKeyID: defaultKeyID, keys: validated}, nil
}

func (signer *TransferTicketSigner) Sign(
	claims TransferTicketClaims,
) (SignedTransferTicket, error) {
	if signer == nil {
		return SignedTransferTicket{}, errors.New("TransferTicket signer is not configured")
	}
	return signer.SignWithKeyID(signer.defaultKeyID, claims)
}

func (signer *TransferTicketSigner) SignWithKeyID(
	keyID string,
	claims TransferTicketClaims,
) (SignedTransferTicket, error) {
	originalKeyID := keyID
	keyID = strings.TrimSpace(keyID)
	if signer == nil || keyID == "" || len(keyID) > 100 || keyID != originalKeyID {
		return SignedTransferTicket{}, errors.New("TransferTicket signer is not configured")
	}
	key, ok := signer.keys[keyID]
	if !ok {
		return SignedTransferTicket{}, fmt.Errorf("TransferTicket signing key is unknown: %s", keyID)
	}
	if err := validateTransferTicketClaims(claims); err != nil {
		return SignedTransferTicket{}, err
	}
	payload, err := json.Marshal(transferTicketEnvelope{
		SchemaVersion: 1,
		KeyID:         keyID,
		Claims:        claims,
	})
	if err != nil {
		return SignedTransferTicket{}, fmt.Errorf("encode TransferTicket: %w", err)
	}
	mac := hmac.New(sha256.New, key)
	_, _ = mac.Write(payload)
	token := base64.RawURLEncoding.EncodeToString(payload) + "." +
		base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
	return SignedTransferTicket{Token: []byte(token)}, nil
}

func (signer *TransferTicketSigner) Verify(
	ticket SignedTransferTicket,
	now time.Time,
) (TransferTicketClaims, error) {
	if signer == nil || len(signer.keys) == 0 || len(ticket.Token) == 0 {
		return TransferTicketClaims{}, errors.New("TransferTicket verifier is not configured")
	}
	parts := strings.Split(string(ticket.Token), ".")
	if len(parts) != 2 {
		return TransferTicketClaims{}, errors.New("TransferTicket envelope is malformed")
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil {
		return TransferTicketClaims{}, errors.New("TransferTicket payload is malformed")
	}
	signature, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil || len(signature) != sha256.Size {
		return TransferTicketClaims{}, errors.New("TransferTicket signature is malformed")
	}
	var envelope transferTicketEnvelope
	if err := json.Unmarshal(payload, &envelope); err != nil || envelope.SchemaVersion != 1 {
		return TransferTicketClaims{}, errors.New("TransferTicket envelope is unsupported")
	}
	key, ok := signer.keys[envelope.KeyID]
	if !ok {
		return TransferTicketClaims{}, errors.New("TransferTicket signing key is unknown")
	}
	mac := hmac.New(sha256.New, key)
	_, _ = mac.Write(payload)
	if !hmac.Equal(signature, mac.Sum(nil)) {
		return TransferTicketClaims{}, errors.New("TransferTicket signature is invalid")
	}
	if err := validateTransferTicketClaims(envelope.Claims); err != nil {
		return TransferTicketClaims{}, err
	}
	now = now.UTC()
	if now.Before(envelope.Claims.IssuedAt) || !now.Before(envelope.Claims.ExpiresAt) {
		return TransferTicketClaims{}, errors.New("TransferTicket is not currently valid")
	}
	return envelope.Claims, nil
}

type TransferDescriptor struct {
	TicketID      uuid.UUID
	ArtifactID    uuid.UUID
	ObjectKey     string
	ObjectVersion string
	SHA256        [sha256.Size]byte
	SizeBytes     int64
	ContentType   string
}

type ResolveTransferCommand struct {
	TicketID    uuid.UUID
	TokenDigest [sha256.Size]byte
	Destination TransferDestination
	ResolvedAt  time.Time
}

type ConsumeTransferCommand struct {
	CommandID     uuid.UUID
	TicketID      uuid.UUID
	TokenDigest   [sha256.Size]byte
	Destination   TransferDestination
	OutcomeDigest [sha256.Size]byte
	ConsumedAt    time.Time
}

type ConsumedTransferTicket struct {
	TicketID   uuid.UUID
	ConsumedAt time.Time
	Replayed   bool
}

type TransferAuthority interface {
	Resolve(context.Context, ResolveTransferCommand) (TransferDescriptor, error)
	Consume(context.Context, ConsumeTransferCommand) error
}

type PullReceipt struct {
	TicketID    uuid.UUID
	ArtifactID  uuid.UUID
	SHA256      [sha256.Size]byte
	SizeBytes   int64
	CompletedAt time.Time
}

type TransferTarget interface {
	Begin(context.Context, TransferDescriptor) (io.WriteCloser, error)
	Commit(context.Context, PullReceipt) error
	Abort(context.Context) error
}

type ObjectStorePullConnector struct {
	store     artifactstore.VersionedStore
	authority TransferAuthority
	signer    *TransferTicketSigner
	now       func() time.Time
}

func NewObjectStorePullConnector(
	store artifactstore.VersionedStore,
	authority TransferAuthority,
	signer *TransferTicketSigner,
	now func() time.Time,
) (*ObjectStorePullConnector, error) {
	if store == nil || authority == nil || signer == nil || now == nil {
		return nil, errors.New("object-store pull Connector configuration is incomplete")
	}
	return &ObjectStorePullConnector{
		store: store, authority: authority, signer: signer, now: now,
	}, nil
}

func (connector *ObjectStorePullConnector) Pull(
	ctx context.Context,
	ticket SignedTransferTicket,
	destination TransferDestination,
	target TransferTarget,
) (PullReceipt, error) {
	if connector == nil || connector.store == nil || connector.authority == nil ||
		connector.signer == nil || connector.now == nil || target == nil || ctx == nil {
		return PullReceipt{}, errors.New("object-store pull Connector is not configured")
	}
	now := connector.now().UTC()
	claims, err := connector.signer.Verify(ticket, now)
	if err != nil {
		return PullReceipt{}, err
	}
	if claims.Destination != destination {
		return PullReceipt{}, ErrTransferTicketDestinationMismatch
	}
	tokenDigest := sha256.Sum256(ticket.Token)
	descriptor, err := connector.authority.Resolve(ctx, ResolveTransferCommand{
		TicketID: claims.TicketID, TokenDigest: tokenDigest,
		Destination: destination, ResolvedAt: now,
	})
	if err != nil {
		return PullReceipt{}, fmt.Errorf("resolve TransferTicket: %w", err)
	}
	if descriptor.TicketID != claims.TicketID || descriptor.ArtifactID == uuid.Nil ||
		descriptor.ObjectKey == "" || descriptor.ObjectVersion == "" ||
		descriptor.SHA256 == [sha256.Size]byte{} || descriptor.SizeBytes <= 0 {
		return PullReceipt{}, errors.New("resolved TransferTicket descriptor is malformed")
	}
	reader, err := connector.store.ReadExactVersion(
		ctx, descriptor.ObjectKey, descriptor.ObjectVersion,
	)
	if err != nil {
		return PullReceipt{}, fmt.Errorf("read TransferTicket exact object version: %w", err)
	}
	defer func() { _ = reader.Close() }()
	writer, err := target.Begin(ctx, descriptor)
	if err != nil {
		return PullReceipt{}, fmt.Errorf("begin StageArtifact transfer target: %w", err)
	}
	abort := true
	defer func() {
		if abort {
			_ = target.Abort(context.WithoutCancel(ctx))
		}
	}()
	digest := sha256.New()
	written, copyErr := io.Copy(
		io.MultiWriter(writer, digest),
		io.LimitReader(reader, descriptor.SizeBytes+1),
	)
	closeErr := writer.Close()
	if copyErr != nil || closeErr != nil {
		return PullReceipt{}, errors.Join(copyErr, closeErr)
	}
	if written != descriptor.SizeBytes || !bytes.Equal(digest.Sum(nil), descriptor.SHA256[:]) {
		return PullReceipt{}, errors.New("transferred StageArtifact failed exact integrity verification")
	}
	receipt := PullReceipt{
		TicketID: claims.TicketID, ArtifactID: descriptor.ArtifactID,
		SHA256: descriptor.SHA256, SizeBytes: written, CompletedAt: now,
	}
	if err := target.Commit(ctx, receipt); err != nil {
		return PullReceipt{}, fmt.Errorf("commit StageArtifact transfer target: %w", err)
	}
	abort = false
	if err := connector.authority.Consume(ctx, ConsumeTransferCommand{
		CommandID: deterministicCommandID("transfer-consume", claims.TicketID),
		TicketID:  claims.TicketID, TokenDigest: tokenDigest,
		Destination: destination, OutcomeDigest: descriptor.SHA256, ConsumedAt: now,
	}); err != nil {
		return PullReceipt{}, fmt.Errorf("record TransferTicket outcome: %w", err)
	}
	return receipt, nil
}

func validateTransferTicketClaims(claims TransferTicketClaims) error {
	if claims.TicketID == uuid.Nil || claims.Destination.WorkerInstanceID == uuid.Nil ||
		claims.Destination.WorkerInstanceEpoch <= 0 ||
		claims.Destination.ModelResidencyID == uuid.Nil ||
		claims.Destination.ModelRuntimeEpoch <= 0 ||
		claims.Destination.ConnectorRevisionID == uuid.Nil || claims.IssuedAt.IsZero() ||
		!claims.ExpiresAt.After(claims.IssuedAt) ||
		claims.ExpiresAt.Sub(claims.IssuedAt) > maxTransferTicketTTL {
		return errors.New("TransferTicket claims are invalid")
	}
	return nil
}

type MemoryTransferTarget struct {
	mu        sync.Mutex
	pending   *bytes.Buffer
	committed []byte
}

func NewMemoryTransferTarget() *MemoryTransferTarget {
	return &MemoryTransferTarget{}
}

func (target *MemoryTransferTarget) Begin(
	_ context.Context,
	_ TransferDescriptor,
) (io.WriteCloser, error) {
	target.mu.Lock()
	defer target.mu.Unlock()
	if target.pending != nil {
		return nil, errors.New("memory transfer target already has a pending write")
	}
	target.pending = &bytes.Buffer{}
	return nopWriteCloser{Writer: target.pending}, nil
}

func (target *MemoryTransferTarget) Commit(
	_ context.Context,
	_ PullReceipt,
) error {
	target.mu.Lock()
	defer target.mu.Unlock()
	if target.pending == nil {
		return errors.New("memory transfer target has no pending write")
	}
	target.committed = bytes.Clone(target.pending.Bytes())
	target.pending = nil
	return nil
}

func (target *MemoryTransferTarget) Abort(context.Context) error {
	target.mu.Lock()
	defer target.mu.Unlock()
	target.pending = nil
	return nil
}

func (target *MemoryTransferTarget) Bytes() []byte {
	target.mu.Lock()
	defer target.mu.Unlock()
	return bytes.Clone(target.committed)
}

type nopWriteCloser struct {
	io.Writer
}

func (nopWriteCloser) Close() error { return nil }
