package runtimepolicy

import (
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"time"

	"github.com/google/uuid"
)

const ProtocolVersion = 1

type Request struct {
	Version           int               `json:"version"`
	OperationID       uuid.UUID         `json:"operation_id"`
	JournalID         uuid.UUID         `json:"journal_id"`
	RequestDigest     [sha256.Size]byte `json:"request_digest"`
	ReservationDigest [sha256.Size]byte `json:"reservation_digest"`
}

type Reply struct {
	Version           int               `json:"version"`
	OperationID       uuid.UUID         `json:"operation_id"`
	JournalID         uuid.UUID         `json:"journal_id"`
	RequestDigest     [sha256.Size]byte `json:"request_digest"`
	ReservationDigest [sha256.Size]byte `json:"reservation_digest"`
	EvidenceDigest    [sha256.Size]byte `json:"evidence_digest"`
	IssuedAt          time.Time         `json:"issued_at"`
	ExpiresAt         time.Time         `json:"expires_at"`
	Signature         []byte            `json:"signature"`
}

// Authorization is the Fleet-side reservation attestation consumed by the
// policy issuer. It is deliberately a different signed object from Reply:
// the issuer may sign a Reply only after an independent authority has signed
// this exact request and reservation binding.
type Authorization struct {
	Version           int               `json:"version"`
	OperationID       uuid.UUID         `json:"operation_id"`
	JournalID         uuid.UUID         `json:"journal_id"`
	RequestDigest     [sha256.Size]byte `json:"request_digest"`
	ReservationDigest [sha256.Size]byte `json:"reservation_digest"`
	IssuedAt          time.Time         `json:"issued_at"`
	ExpiresAt         time.Time         `json:"expires_at"`
	Signature         []byte            `json:"signature"`
}

// ReservationBindingDigest is the canonical digest shared by Fleet and Node.
// It intentionally excludes Node-local receipt timestamps so Fleet can sign
// the authorization immediately after committing the reservation while Node
// can verify the same binding after persisting its local ledger entry.
func ReservationBindingDigest(operationID, journalID uuid.UUID, requestDigest [sha256.Size]byte, reservedAt time.Time) ([sha256.Size]byte, error) {
	if operationID == uuid.Nil || journalID == uuid.Nil || requestDigest == ([sha256.Size]byte{}) ||
		reservedAt.IsZero() || reservedAt.Location() != time.UTC {
		return [sha256.Size]byte{}, errors.New("runtime policy reservation binding is invalid")
	}
	wire, err := json.Marshal(struct {
		Version     int               `json:"version"`
		OperationID uuid.UUID         `json:"operation_id"`
		JournalID   uuid.UUID         `json:"journal_id"`
		Request     [sha256.Size]byte `json:"request_digest"`
		ReservedAt  time.Time         `json:"reserved_at"`
	}{
		Version: ProtocolVersion, OperationID: operationID, JournalID: journalID,
		Request: requestDigest, ReservedAt: reservedAt,
	})
	if err != nil {
		return [sha256.Size]byte{}, err
	}
	return sha256.Sum256(wire), nil
}

func (r Request) Valid() bool {
	return r.Version == ProtocolVersion && r.OperationID != uuid.Nil && r.JournalID != uuid.Nil &&
		r.RequestDigest != ([sha256.Size]byte{}) && r.ReservationDigest != ([sha256.Size]byte{})
}

func (r Reply) ValidFor(request Request, now time.Time) bool {
	return request.Valid() && r.Version == ProtocolVersion && r.OperationID == request.OperationID && r.JournalID == request.JournalID &&
		r.RequestDigest == request.RequestDigest && r.ReservationDigest == request.ReservationDigest &&
		r.EvidenceDigest != ([sha256.Size]byte{}) && !r.IssuedAt.IsZero() && !r.ExpiresAt.IsZero() &&
		r.IssuedAt.Location() == time.UTC && r.ExpiresAt.Location() == time.UTC && r.ExpiresAt.After(r.IssuedAt) &&
		r.ExpiresAt.After(now) && r.ExpiresAt.Sub(r.IssuedAt) <= 5*time.Minute && !r.IssuedAt.After(now.Add(time.Second))
}

func SigningBytes(r Reply) ([]byte, error) {
	r.Signature = nil
	return json.Marshal(r)
}

func VerifyReply(r Reply, request Request, publicKey ed25519.PublicKey, now time.Time) error {
	if !r.ValidFor(request, now) || len(publicKey) != ed25519.PublicKeySize || len(r.Signature) != ed25519.SignatureSize {
		return errors.New("runtime policy reply is invalid")
	}
	wire, err := SigningBytes(r)
	if err != nil || !ed25519.Verify(publicKey, wire, r.Signature) {
		return errors.New("runtime policy reply signature is invalid")
	}
	return nil
}

func (a Authorization) Request() Request {
	return Request{Version: ProtocolVersion, OperationID: a.OperationID, JournalID: a.JournalID, RequestDigest: a.RequestDigest, ReservationDigest: a.ReservationDigest}
}

func (a Authorization) ValidFor(request Request, now time.Time) bool {
	return request.Valid() && a.Version == ProtocolVersion && a.OperationID == request.OperationID && a.JournalID == request.JournalID && a.RequestDigest == request.RequestDigest && a.ReservationDigest == request.ReservationDigest && !a.IssuedAt.IsZero() && !a.ExpiresAt.IsZero() && a.IssuedAt.Location() == time.UTC && a.ExpiresAt.Location() == time.UTC && a.ExpiresAt.After(a.IssuedAt) && a.ExpiresAt.After(now) && a.ExpiresAt.Sub(a.IssuedAt) <= 5*time.Minute && !a.IssuedAt.After(now.Add(time.Second))
}

func AuthorizationSigningBytes(a Authorization) ([]byte, error) {
	a.Signature = nil
	return json.Marshal(a)
}

func VerifyAuthorization(a Authorization, request Request, publicKey ed25519.PublicKey, now time.Time) error {
	if !a.ValidFor(request, now) || len(publicKey) != ed25519.PublicKeySize || len(a.Signature) != ed25519.SignatureSize {
		return errors.New("runtime policy authorization is invalid")
	}
	wire, err := AuthorizationSigningBytes(a)
	if err != nil || !ed25519.Verify(publicKey, wire, a.Signature) {
		return errors.New("runtime policy authorization signature is invalid")
	}
	return nil
}
