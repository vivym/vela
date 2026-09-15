// Package journalbinding authenticates immutable Registry journal identity.
// Its keys are separate from the execution seeds distributed to Workers.
package journalbinding

import (
	"bytes"
	"crypto/ed25519"
	"crypto/sha256"
	"errors"
	"slices"
	"strings"
	"unicode/utf8"

	"github.com/google/uuid"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/protobuf/proto"
)

const (
	SchemaVersion = 1
	MaximumBytes  = 16 << 10
	signingDomain = "vela-worker-journal-binding-v1\x00"
)

var ErrInvalid = errors.New("worker journal binding is invalid")

type Signer struct {
	keyID string
	key   ed25519.PrivateKey
}

type Verifier struct{ keys map[string]ed25519.PublicKey }

// PublicKeys returns a defensive copy suitable for embedding in a Runtime
// bootstrap snapshot. The private verifier state remains immutable.
func (verifier *Verifier) PublicKeys() map[string][]byte {
	if verifier == nil {
		return nil
	}
	keys := make(map[string][]byte, len(verifier.keys))
	for id, key := range verifier.keys {
		keys[id] = slices.Clone(key)
	}
	return keys
}

type JournalKind int

const (
	WorkerJournal JournalKind = iota + 1
	RuntimeJournal
)

// Journal is the identity recovered while holding the actual journal lock.
type Journal struct {
	WorkerInstanceID    string
	WorkerInstanceEpoch int64
	WorkerMemberID      string
	WorkerMemberEpoch   int64
	JournalID           uuid.UUID
	Scope               [sha256.Size]byte
}

func NewSigner(keyID string, seed []byte) (*Signer, error) {
	if !validText(keyID, 100) || len(seed) != ed25519.SeedSize {
		return nil, errors.New("journal binding signer requires a key id and dedicated Ed25519 seed")
	}
	return &Signer{keyID: keyID, key: ed25519.NewKeyFromSeed(seed)}, nil
}

func NewVerifier(keys map[string][]byte) (*Verifier, error) {
	if len(keys) == 0 || len(keys) > 32 {
		return nil, errors.New("journal binding verifier requires trusted public keys")
	}
	verifier := &Verifier{keys: make(map[string]ed25519.PublicKey, len(keys))}
	for id, key := range keys {
		if !validText(id, 100) || len(key) != ed25519.PublicKeySize {
			return nil, errors.New("journal binding verifier key is invalid")
		}
		verifier.keys[id] = slices.Clone(key)
	}
	return verifier, nil
}

// Sign records historical identity only. Callers must first read the committed
// Registry receipt through their authenticated, principal-scoped authority.
func (signer *Signer) Sign(value *velav1.WorkerBootstrapBinding) (*velav1.WorkerBootstrapBinding, error) {
	if signer == nil || !validText(signer.keyID, 100) || len(signer.key) != ed25519.PrivateKeySize ||
		value == nil || value.GetSchemaVersion() != SchemaVersion || value.GetSigningKeyId() != "" || len(value.GetSignature()) != 0 {
		return nil, ErrInvalid
	}
	value = proto.Clone(value).(*velav1.WorkerBootstrapBinding)
	value.SchemaVersion, value.SigningKeyId, value.Signature = SchemaVersion, signer.keyID, nil
	if err := validate(value, false); err != nil {
		return nil, err
	}
	wire, err := signingBytes(value)
	if err != nil {
		return nil, err
	}
	value.Signature = ed25519.Sign(signer.key, wire)
	if err := validate(value, true); err != nil {
		return nil, err
	}
	return value, nil
}

// Verify validates immutable facts without an expiry or a readiness claim.
// Current execution authorization and local journal ownership remain mandatory.
func (verifier *Verifier) Verify(value *velav1.WorkerBootstrapBinding) (*velav1.WorkerBootstrapBinding, error) {
	if verifier == nil || validate(value, true) != nil {
		return nil, ErrInvalid
	}
	value = proto.Clone(value).(*velav1.WorkerBootstrapBinding)
	key, ok := verifier.keys[value.GetSigningKeyId()]
	if !ok || len(key) != ed25519.PublicKeySize {
		return nil, ErrInvalid
	}
	signature := value.Signature
	value.Signature = nil
	wire, err := signingBytes(value)
	if err != nil || !ed25519.Verify(key, wire, signature) {
		return nil, ErrInvalid
	}
	value.Signature = signature
	return value, nil
}

func (verifier *Verifier) VerifyJournal(value *velav1.WorkerBootstrapBinding, kind JournalKind, journal Journal) error {
	verified, err := verifier.Verify(value)
	if err != nil {
		return err
	}
	claim, pair := verified.GetClaim(), verified.GetPair()
	if claim.GetWorkerInstanceId() != journal.WorkerInstanceID || claim.GetWorkerInstanceEpoch() != journal.WorkerInstanceEpoch ||
		claim.GetWorkerMemberId() != journal.WorkerMemberID || claim.GetWorkerMemberEpoch() != journal.WorkerMemberEpoch {
		return ErrInvalid
	}
	var id string
	var scope []byte
	switch kind {
	case WorkerJournal:
		id, scope = pair.GetWorkerJournalId(), pair.GetWorkerScope()
	case RuntimeJournal:
		id, scope = pair.GetRuntimeJournalId(), pair.GetRuntimeScope()
	default:
		return ErrInvalid
	}
	if id != journal.JournalID.String() || !bytes.Equal(scope, journal.Scope[:]) {
		return ErrInvalid
	}
	return nil
}

func validate(value *velav1.WorkerBootstrapBinding, signed bool) error {
	claim, pair := value.GetClaim(), value.GetPair()
	for _, message := range []proto.Message{value, claim, pair, claim.GetClaimedAt(), pair.GetRecordedAt()} {
		if message == nil || !message.ProtoReflect().IsValid() || len(message.ProtoReflect().GetUnknown()) != 0 {
			return ErrInvalid
		}
	}
	if value.GetSchemaVersion() != SchemaVersion || proto.Size(value) > MaximumBytes || !validText(value.GetSigningKeyId(), 100) ||
		signed && len(value.GetSignature()) != ed25519.SignatureSize ||
		!validUUID(claim.GetRequestId()) || !validUUID(claim.GetWorkerInstanceId()) || !validUUID(claim.GetWorkerMemberId()) ||
		claim.GetWorkerInstanceEpoch() <= 0 || claim.GetWorkerMemberEpoch() <= 0 || !validText(claim.GetNodeIdentity(), 253) ||
		!validText(claim.GetActorIdentity(), 500) || !validDigest(claim.GetBundleDigest()) ||
		claim.GetClaimedAt().CheckValid() != nil || claim.GetClaimedAt().AsTime().IsZero() ||
		pair.GetRecordedAt().CheckValid() != nil || pair.GetRecordedAt().AsTime().IsZero() ||
		pair.GetRequestId() != claim.GetRequestId() || pair.GetActorIdentity() != claim.GetActorIdentity() ||
		!validUUID(pair.GetWorkerJournalId()) || !validUUID(pair.GetRuntimeJournalId()) || pair.GetWorkerJournalId() == pair.GetRuntimeJournalId() ||
		!validDigest(pair.GetWorkerScope()) || !validDigest(pair.GetRuntimeScope()) {
		return ErrInvalid
	}
	return nil
}

func signingBytes(value *velav1.WorkerBootstrapBinding) ([]byte, error) {
	wire, err := proto.MarshalOptions{Deterministic: true}.Marshal(value)
	return append([]byte(signingDomain), wire...), err
}

func validUUID(value string) bool {
	id, err := uuid.Parse(value)
	return err == nil && id != uuid.Nil && id.String() == value
}

func validDigest(value []byte) bool {
	return len(value) == sha256.Size && !bytes.Equal(value, make([]byte, sha256.Size))
}

func validText(value string, maximum int) bool {
	return len(value) > 0 && len(value) <= maximum && utf8.ValidString(value) && strings.TrimSpace(value) == value && !strings.ContainsAny(value, "\x00\r\n\t")
}
