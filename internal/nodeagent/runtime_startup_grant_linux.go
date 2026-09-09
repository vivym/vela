package nodeagent

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"os"
	"time"

	"github.com/google/uuid"
)

var ErrRuntimeStartupGrantConsumed = errors.New("runtime startup grant attempt already consumed")

// RuntimeStartupGrantAttempt is a durable negative fence, not a permit or proof
// of activation. AuthorizationDigest identifies independently verified evidence;
// storing a digest does not verify that evidence. Nothing in this API issues or
// activates a JournalWriteGrant, replies to a caller or starts a backend.
type RuntimeStartupGrantAttempt struct {
	OperationID         uuid.UUID         `json:"operation_id"`
	JournalID           uuid.UUID         `json:"journal_id"`
	StartupDigest       [sha256.Size]byte `json:"startup_digest"`
	ReservationDigest   [sha256.Size]byte `json:"reservation_digest"`
	AuthorizationDigest [sha256.Size]byte `json:"authorization_digest"`
	RecordedAt          time.Time         `json:"recorded_at"`
}

// ConsumeJournalGrantAttempt durably consumes the opportunity before trusted
// orchestration may attempt activation. It requires the live ledger's retained
// original Runtime and exact reservation. Every retry fails, even with identical
// arguments. Recovery exposes history only and never reconstructs a live owner.
// A failed append poisons this handle; do not issue a grant on any error.
// Schema 1/2 ledgers remain readable but cannot enter this new transition.
func (ledger *RuntimeStartupLedger) ConsumeJournalGrantAttempt(ctx context.Context, journalID, operationID uuid.UUID, authorizationDigest [sha256.Size]byte) (RuntimeStartupGrantAttempt, error) {
	if err := contextError(ctx); err != nil {
		return RuntimeStartupGrantAttempt{}, err
	}
	if ledger == nil || journalID == uuid.Nil || operationID == uuid.Nil || authorizationDigest == ([sha256.Size]byte{}) {
		return RuntimeStartupGrantAttempt{}, ErrRuntimeStartupLedger
	}
	ledger.mu.Lock()
	defer ledger.mu.Unlock()
	if err := errors.Join(ledger.check(), context.Cause(ctx)); err != nil {
		return RuntimeStartupGrantAttempt{}, err
	}
	if _, exists := ledger.grantAttempts[journalID]; exists {
		return RuntimeStartupGrantAttempt{}, ErrRuntimeStartupGrantConsumed
	}
	startup, started := ledger.starts[journalID]
	reservation, reserved := ledger.reservations[journalID]
	_, exited := ledger.exits[journalID]
	if ledger.header.SchemaVersion != 3 || !started || !reserved || exited || startup.OperationID != operationID || reservation.OperationID != operationID {
		return RuntimeStartupGrantAttempt{}, ErrRuntimeStartupLedger
	}
	original, err := retainJournalProcess(ctx, ledger.owners[journalID])
	if err != nil {
		return RuntimeStartupGrantAttempt{}, err
	}
	defer func() { _ = original.Close() }()
	startupDigest, reservationDigest, err := runtimeStartupGrantDigests(startup, reservation)
	if err != nil {
		return RuntimeStartupGrantAttempt{}, err
	}
	record := RuntimeStartupGrantAttempt{OperationID: operationID, JournalID: journalID,
		StartupDigest: startupDigest, ReservationDigest: reservationDigest,
		AuthorizationDigest: authorizationDigest, RecordedAt: time.Now().UTC()}
	if err := ledger.validateGrantAttempt(record); err != nil {
		return RuntimeStartupGrantAttempt{}, err
	}
	if err := ledger.append(ctx, runtimeStartupEntry{GrantAttempt: &record}); err != nil {
		return RuntimeStartupGrantAttempt{}, err
	}
	return record, nil
}

// InspectJournalGrantAttempt returns history, including after an uncertain
// append. Finding a record means the opportunity was consumed, never that a
// backend was started or that another activation is safe.
func (ledger *RuntimeStartupLedger) InspectJournalGrantAttempt(ctx context.Context, journalID uuid.UUID) (RuntimeStartupGrantAttempt, error) {
	if err := contextError(ctx); err != nil {
		return RuntimeStartupGrantAttempt{}, err
	}
	if ledger == nil {
		return RuntimeStartupGrantAttempt{}, ErrRuntimeStartupLedger
	}
	ledger.mu.Lock()
	defer ledger.mu.Unlock()
	if err := errors.Join(ledger.check(), context.Cause(ctx)); err != nil {
		return RuntimeStartupGrantAttempt{}, err
	}
	record, ok := ledger.grantAttempts[journalID]
	if !ok {
		return RuntimeStartupGrantAttempt{}, os.ErrNotExist
	}
	return record, nil
}

func runtimeStartupGrantDigests(startup RuntimeStartupRecord, reservation RuntimeStartupReservationRecord) ([sha256.Size]byte, [sha256.Size]byte, error) {
	first, err := json.Marshal(startup)
	if err != nil {
		return [sha256.Size]byte{}, [sha256.Size]byte{}, err
	}
	second, err := json.Marshal(reservation)
	return sha256.Sum256(first), sha256.Sum256(second), err
}

func (ledger *RuntimeStartupLedger) validateGrantAttempt(record RuntimeStartupGrantAttempt) error {
	startup, started := ledger.starts[record.JournalID]
	reservation, reserved := ledger.reservations[record.JournalID]
	_, duplicate := ledger.grantAttempts[record.JournalID]
	_, exited := ledger.exits[record.JournalID]
	if ledger.header.SchemaVersion != 3 || !started || !reserved || duplicate || exited ||
		record.OperationID != startup.OperationID || record.OperationID != reservation.OperationID ||
		record.AuthorizationDigest == ([sha256.Size]byte{}) || record.RecordedAt.IsZero() ||
		record.RecordedAt.Location() != time.UTC || record.RecordedAt.Before(reservation.RecordedAt) {
		return ErrRuntimeStartupLedger
	}
	startupDigest, reservationDigest, err := runtimeStartupGrantDigests(startup, reservation)
	if err != nil || record.StartupDigest != startupDigest || record.ReservationDigest != reservationDigest {
		return errors.Join(ErrRuntimeStartupLedger, err)
	}
	return nil
}

func (ledger *RuntimeStartupLedger) applyGrantAttempt(record RuntimeStartupGrantAttempt) error {
	if err := ledger.validateGrantAttempt(record); err != nil {
		return err
	}
	ledger.grantAttempts[record.JournalID] = record
	return nil
}
