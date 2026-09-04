package stageauthority

import (
	"bytes"
	"crypto/ed25519"
	"crypto/sha256"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
	"time"

	"github.com/google/uuid"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/protobuf/proto"
)

const (
	SchemaVersionV1      = 1
	minSigningKeyBytes   = 32
	maxAuthorityValidity = 7 * 24 * time.Hour
	maxValidationSkew    = time.Minute
)

var (
	ErrInvalid          = errors.New("StageAuthority is invalid")
	ErrInvalidSignature = errors.New("StageAuthority signature is invalid")
	ErrUnknownKey       = errors.New("StageAuthority signing key is unknown")
	ErrStale            = errors.New("StageAuthority is stale")
	ErrRuntimeMismatch  = errors.New("StageAuthority does not match resident runtime")
	ErrRenewalMismatch  = errors.New("StageAuthority renewal does not match active execution")
)

type DeviceEpoch struct {
	ID    string
	Epoch int64
}

type MemberEpoch struct {
	ID    string
	Epoch int64
}

type RuntimeBinding struct {
	WorkerInstanceID       string
	WorkerInstanceEpoch    int64
	WorkerMemberID         string
	WorkerMemberEpoch      int64
	DeviceSetDigest        []byte
	Devices                []DeviceEpoch
	MembershipDigest       []byte
	Members                []MemberEpoch
	ModelResidencyID       string
	ModelRuntimeIdentity   string
	ModelRuntimeEpoch      int64
	StageProfileRevisionID string
}

type Verified struct {
	Authority         *velav1.StageAuthority
	Digest            [32]byte
	MonotonicValidFor time.Duration
}

type Signer struct {
	keys map[string]ed25519.PrivateKey
}

type Validator struct {
	keys map[string]ed25519.PublicKey
	now  func() time.Time
}

func NewSigner(keys map[string][]byte) (*Signer, error) {
	validated, err := validateKeyring(keys)
	if err != nil {
		return nil, err
	}
	defer ClearKeyring(validated)
	privateKeys := make(map[string]ed25519.PrivateKey, len(validated))
	for id, key := range validated {
		seed := stageAuthoritySigningSeed(key)
		privateKeys[id] = ed25519.NewKeyFromSeed(seed[:])
	}
	return &Signer{keys: privateKeys}, nil
}

func NewValidator(keys map[string][]byte, now func() time.Time) (*Validator, error) {
	verifierKeys, err := DeriveVerifierKeyring(keys)
	if err != nil {
		return nil, err
	}
	defer ClearKeyring(verifierKeys)
	return NewVerifier(verifierKeys, now)
}

func NewVerifier(keys map[string][]byte, now func() time.Time) (*Validator, error) {
	validated, err := validateVerifierKeyring(keys)
	if err != nil {
		return nil, err
	}
	if now == nil {
		now = time.Now
	}
	return &Validator{keys: validated, now: now}, nil
}

func DeriveVerifierKeyring(keys map[string][]byte) (map[string][]byte, error) {
	validated, err := validateKeyring(keys)
	if err != nil {
		return nil, err
	}
	defer ClearKeyring(validated)
	verifierKeys := make(map[string][]byte, len(validated))
	for id, key := range validated {
		seed := stageAuthoritySigningSeed(key)
		privateKey := ed25519.NewKeyFromSeed(seed[:])
		verifierKeys[id] = slices.Clone(privateKey.Public().(ed25519.PublicKey))
		clear(privateKey)
	}
	return verifierKeys, nil
}

func (signer *Signer) Sign(authority *velav1.StageAuthority) (*velav1.StageAuthority, error) {
	if signer == nil {
		return nil, errors.New("StageAuthority signer is not configured")
	}
	canonical, err := canonicalize(authority)
	if err != nil {
		return nil, err
	}
	canonical.Signature = nil
	if err := validateShape(canonical, false); err != nil {
		return nil, err
	}
	key, ok := signer.keys[canonical.GetSigningKeyId()]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrUnknownKey, canonical.GetSigningKeyId())
	}
	payload, err := proto.MarshalOptions{Deterministic: true}.Marshal(canonical)
	if err != nil {
		return nil, fmt.Errorf("%w: encode signature payload: %v", ErrInvalid, err)
	}
	canonical.Signature = ed25519.Sign(key, payload)
	return canonical, nil
}

func (validator *Validator) Validate(
	authority *velav1.StageAuthority,
	binding RuntimeBinding,
) (Verified, error) {
	return validator.ValidateWithClockSkew(authority, binding, 0)
}

func (validator *Validator) ValidateWithClockSkew(
	authority *velav1.StageAuthority,
	binding RuntimeBinding,
	maxFutureSkew time.Duration,
) (Verified, error) {
	verified, err := validator.ValidateEnvelopeWithClockSkew(authority, maxFutureSkew)
	if err != nil {
		return Verified{}, err
	}
	if err := matchRuntime(verified.Authority, binding); err != nil {
		return Verified{}, err
	}
	return verified, nil
}

func (validator *Validator) ValidateEnvelope(
	authority *velav1.StageAuthority,
) (Verified, error) {
	return validator.ValidateEnvelopeWithClockSkew(authority, 0)
}

func (validator *Validator) ValidateEnvelopeWithClockSkew(
	authority *velav1.StageAuthority,
	maxFutureSkew time.Duration,
) (Verified, error) {
	if validator == nil {
		return Verified{}, errors.New("StageAuthority validator is not configured")
	}
	if maxFutureSkew < 0 || maxFutureSkew > maxValidationSkew {
		return Verified{}, fmt.Errorf("%w: StageAuthority clock skew is invalid", ErrInvalid)
	}
	verified, err := validator.ValidateEnvelopeSignature(authority)
	if err != nil {
		return Verified{}, err
	}
	canonical := verified.Authority

	now := validator.now().UTC()
	issuedAt := canonical.GetIssuedAt().AsTime().UTC()
	expiresAt := canonical.GetExpiresAt().AsTime().UTC()
	if now.Add(maxFutureSkew).Before(issuedAt) || !now.Before(expiresAt) {
		return Verified{}, ErrStale
	}
	remainingWall := expiresAt.Sub(now)
	elapsed := now.Sub(issuedAt)
	if elapsed < 0 {
		elapsed = 0
	}
	remainingMonotonic := canonical.GetMonotonicValidFor().AsDuration() - elapsed
	if remainingMonotonic <= 0 {
		return Verified{}, ErrStale
	}
	verified.MonotonicValidFor = min(remainingWall, remainingMonotonic)
	return verified, nil
}

// ValidateEnvelopeSignature verifies shape, key, signature, and digest without
// evaluating temporal validity. Callers must separately enforce a replay bound.
func (validator *Validator) ValidateEnvelopeSignature(
	authority *velav1.StageAuthority,
) (Verified, error) {
	if validator == nil {
		return Verified{}, errors.New("StageAuthority validator is not configured")
	}
	canonical, err := canonicalize(authority)
	if err != nil {
		return Verified{}, err
	}
	if err := validateShape(canonical, true); err != nil {
		return Verified{}, err
	}
	key, ok := validator.keys[canonical.GetSigningKeyId()]
	if !ok {
		return Verified{}, fmt.Errorf("%w: %s", ErrUnknownKey, canonical.GetSigningKeyId())
	}
	signature := slices.Clone(canonical.GetSignature())
	canonical.Signature = nil
	payload, err := proto.MarshalOptions{Deterministic: true}.Marshal(canonical)
	if err != nil {
		return Verified{}, fmt.Errorf("%w: encode signature payload: %v", ErrInvalid, err)
	}
	if !ed25519.Verify(key, payload, signature) {
		return Verified{}, ErrInvalidSignature
	}
	canonical.Signature = signature
	digest, err := Digest(canonical)
	if err != nil {
		return Verified{}, err
	}
	return Verified{
		Authority: proto.Clone(canonical).(*velav1.StageAuthority),
		Digest:    digest,
	}, nil
}

func Digest(authority *velav1.StageAuthority) ([32]byte, error) {
	canonical, err := canonicalize(authority)
	if err != nil {
		return [32]byte{}, err
	}
	if err := validateShape(canonical, true); err != nil {
		return [32]byte{}, err
	}
	payload, err := proto.MarshalOptions{Deterministic: true}.Marshal(canonical)
	if err != nil {
		return [32]byte{}, fmt.Errorf("%w: encode authority digest: %v", ErrInvalid, err)
	}
	return sha256.Sum256(payload), nil
}

func ExecutionSpecDigest(spec *velav1.StageExecutionSpec) ([32]byte, error) {
	if spec == nil {
		spec = &velav1.StageExecutionSpec{}
	}
	payload, err := proto.MarshalOptions{Deterministic: true}.Marshal(spec)
	if err != nil {
		return [32]byte{}, fmt.Errorf("encode StageExecutionSpec digest: %w", err)
	}
	return sha256.Sum256(payload), nil
}

func ValidateRenewal(current, renewed *velav1.StageAuthority) error {
	currentCanonical, err := canonicalize(current)
	if err != nil {
		return err
	}
	if err := validateShape(currentCanonical, true); err != nil {
		return err
	}
	renewedCanonical, err := canonicalize(renewed)
	if err != nil {
		return err
	}
	if err := validateShape(renewedCanonical, true); err != nil {
		return err
	}
	if renewedCanonical.GetStageVersion() < currentCanonical.GetStageVersion() ||
		!renewedCanonical.GetIssuedAt().AsTime().After(currentCanonical.GetIssuedAt().AsTime()) ||
		!renewedCanonical.GetExpiresAt().AsTime().After(currentCanonical.GetExpiresAt().AsTime()) {
		return ErrRenewalMismatch
	}
	for _, authority := range []*velav1.StageAuthority{currentCanonical, renewedCanonical} {
		authority.StageVersion = 0
		authority.SigningKeyId = ""
		authority.IssuedAt = nil
		authority.ExpiresAt = nil
		authority.MonotonicValidFor = nil
		authority.Signature = nil
	}
	if !proto.Equal(currentCanonical, renewedCanonical) {
		return ErrRenewalMismatch
	}
	return nil
}

func validateKeyring(keys map[string][]byte) (map[string][]byte, error) {
	if len(keys) == 0 {
		return nil, errors.New("StageAuthority keyring is required")
	}
	validated := make(map[string][]byte, len(keys))
	for id, key := range keys {
		if strings.TrimSpace(id) != id || id == "" || len(id) > 100 {
			ClearKeyring(validated)
			return nil, errors.New("StageAuthority signing key identity is invalid")
		}
		if len(key) < minSigningKeyBytes || len(key) > maxSigningKeyBytes {
			ClearKeyring(validated)
			return nil, fmt.Errorf("StageAuthority signing key %s has invalid length", id)
		}
		validated[id] = slices.Clone(key)
	}
	return validated, nil
}

func validateVerifierKeyring(keys map[string][]byte) (map[string]ed25519.PublicKey, error) {
	if len(keys) == 0 {
		return nil, errors.New("StageAuthority verifier keyring is required")
	}
	validated := make(map[string]ed25519.PublicKey, len(keys))
	for id, key := range keys {
		if strings.TrimSpace(id) != id || id == "" || len(id) > 100 {
			for keyID := range validated {
				clear(validated[keyID])
				delete(validated, keyID)
			}
			return nil, errors.New("StageAuthority verifier key identity is invalid")
		}
		if len(key) != ed25519.PublicKeySize {
			for keyID := range validated {
				clear(validated[keyID])
				delete(validated, keyID)
			}
			return nil, fmt.Errorf("StageAuthority verifier key %s has invalid length", id)
		}
		validated[id] = slices.Clone(key)
	}
	return validated, nil
}

func stageAuthoritySigningSeed(key []byte) [sha256.Size]byte {
	payload := make([]byte, 0, len("vela-stage-authority-ed25519-v1\x00")+len(key))
	payload = append(payload, "vela-stage-authority-ed25519-v1\x00"...)
	payload = append(payload, key...)
	seed := sha256.Sum256(payload)
	clear(payload)
	return seed
}

func canonicalize(authority *velav1.StageAuthority) (*velav1.StageAuthority, error) {
	if authority == nil {
		return nil, fmt.Errorf("%w: authority is required", ErrInvalid)
	}
	canonical := proto.Clone(authority).(*velav1.StageAuthority)
	slices.SortFunc(canonical.Devices, func(left, right *velav1.StageAuthorityDeviceEpoch) int {
		if left == nil || right == nil {
			if left == nil && right == nil {
				return 0
			}
			if left == nil {
				return -1
			}
			return 1
		}
		if compared := strings.Compare(left.GetDeviceId(), right.GetDeviceId()); compared != 0 {
			return compared
		}
		return intCompare(left.GetDeviceEpoch(), right.GetDeviceEpoch())
	})
	slices.SortFunc(canonical.Members, func(left, right *velav1.StageAuthorityMemberEpoch) int {
		if left == nil || right == nil {
			if left == nil && right == nil {
				return 0
			}
			if left == nil {
				return -1
			}
			return 1
		}
		if compared := strings.Compare(left.GetWorkerMemberId(), right.GetWorkerMemberId()); compared != 0 {
			return compared
		}
		return intCompare(left.GetMemberEpoch(), right.GetMemberEpoch())
	})
	canonical.CapacityVector = maps.Clone(canonical.GetCapacityVector())
	return canonical, nil
}

func validateShape(authority *velav1.StageAuthority, requireSignature bool) error {
	if authority.GetSchemaVersion() != SchemaVersionV1 {
		return fmt.Errorf("%w: unsupported schema version", ErrInvalid)
	}
	for name, value := range map[string]string{
		"Job":                  authority.GetJobId(),
		"Attempt":              authority.GetAttemptId(),
		"StageRun":             authority.GetStageRunId(),
		"StageAttempt":         authority.GetStageAttemptId(),
		"StageAllocation":      authority.GetStageAllocationId(),
		"StageLease":           authority.GetStageLeaseId(),
		"WorkerInstance":       authority.GetWorkerInstanceId(),
		"ModelResidency":       authority.GetModelResidencyId(),
		"StageProfileRevision": authority.GetStageProfileRevisionId(),
	} {
		if _, err := uuid.Parse(value); err != nil {
			return fmt.Errorf("%w: %s identity is invalid", ErrInvalid, name)
		}
	}
	if authority.GetAttemptFence() <= 0 || authority.GetStageFence() <= 0 ||
		authority.GetStageVersion() <= 0 || authority.GetWorkerInstanceEpoch() <= 0 ||
		authority.GetModelRuntimeBarrierGeneration() <= 0 ||
		authority.GetCapacityObservationSequence() <= 0 {
		return fmt.Errorf("%w: fence, version, or epoch is invalid", ErrInvalid)
	}
	if len(authority.GetDeviceSetDigest()) != sha256.Size ||
		len(authority.GetMembershipDigest()) != sha256.Size ||
		len(authority.GetLeaseToken()) != sha256.Size ||
		len(authority.GetExecutionNonce()) != sha256.Size ||
		len(authority.GetExecutionSpecDigest()) != sha256.Size {
		return fmt.Errorf("%w: digest, token, or nonce length is invalid", ErrInvalid)
	}
	if authority.GetModelRuntimeIdentity() == "" || len(authority.GetModelRuntimeIdentity()) > 200 ||
		authority.GetSigningKeyId() == "" || len(authority.GetSigningKeyId()) > 100 {
		return fmt.Errorf("%w: runtime or signing key identity is invalid", ErrInvalid)
	}
	if err := validateAuthorityDevices(authority.GetDevices()); err != nil {
		return err
	}
	if err := validateAuthorityMembers(authority.GetMembers()); err != nil {
		return err
	}
	if len(authority.GetCapacityVector()) == 0 {
		return fmt.Errorf("%w: capacity vector is empty", ErrInvalid)
	}
	for key, value := range authority.GetCapacityVector() {
		if key == "" || len(key) > 100 || value <= 0 {
			return fmt.Errorf("%w: capacity vector is invalid", ErrInvalid)
		}
	}
	if authority.GetIssuedAt() == nil || authority.GetExpiresAt() == nil ||
		authority.GetMonotonicValidFor() == nil {
		return fmt.Errorf("%w: authority deadlines are required", ErrInvalid)
	}
	if err := authority.GetIssuedAt().CheckValid(); err != nil {
		return fmt.Errorf("%w: issued_at: %v", ErrInvalid, err)
	}
	if err := authority.GetExpiresAt().CheckValid(); err != nil {
		return fmt.Errorf("%w: expires_at: %v", ErrInvalid, err)
	}
	if err := authority.GetMonotonicValidFor().CheckValid(); err != nil {
		return fmt.Errorf("%w: monotonic_valid_for: %v", ErrInvalid, err)
	}
	issuedAt := authority.GetIssuedAt().AsTime()
	expiresAt := authority.GetExpiresAt().AsTime()
	validFor := authority.GetMonotonicValidFor().AsDuration()
	if !expiresAt.After(issuedAt) || validFor <= 0 || validFor > maxAuthorityValidity ||
		validFor > expiresAt.Sub(issuedAt) {
		return fmt.Errorf("%w: authority deadline interval is invalid", ErrInvalid)
	}
	if requireSignature && len(authority.GetSignature()) != ed25519.SignatureSize {
		return fmt.Errorf("%w: signature length is invalid", ErrInvalidSignature)
	}
	return nil
}

func validateAuthorityDevices(devices []*velav1.StageAuthorityDeviceEpoch) error {
	if len(devices) == 0 {
		return fmt.Errorf("%w: device epochs are required", ErrInvalid)
	}
	lastID := ""
	for _, device := range devices {
		if device == nil || device.GetDeviceEpoch() <= 0 {
			return fmt.Errorf("%w: device epoch is invalid", ErrInvalid)
		}
		if _, err := uuid.Parse(device.GetDeviceId()); err != nil || device.GetDeviceId() == lastID {
			return fmt.Errorf("%w: device identity is invalid or duplicated", ErrInvalid)
		}
		lastID = device.GetDeviceId()
	}
	return nil
}

func validateAuthorityMembers(members []*velav1.StageAuthorityMemberEpoch) error {
	if len(members) == 0 {
		return fmt.Errorf("%w: member epochs are required", ErrInvalid)
	}
	lastID := ""
	for _, member := range members {
		if member == nil || member.GetMemberEpoch() <= 0 || member.GetModelRuntimeEpoch() <= 0 ||
			len(member.GetIdentityDigest()) != sha256.Size {
			return fmt.Errorf("%w: member epoch is invalid", ErrInvalid)
		}
		if _, err := uuid.Parse(member.GetWorkerMemberId()); err != nil ||
			member.GetWorkerMemberId() == lastID {
			return fmt.Errorf("%w: member identity is invalid or duplicated", ErrInvalid)
		}
		lastID = member.GetWorkerMemberId()
	}
	return nil
}

func matchRuntime(authority *velav1.StageAuthority, binding RuntimeBinding) error {
	if authority.GetWorkerInstanceId() != binding.WorkerInstanceID ||
		authority.GetWorkerInstanceEpoch() != binding.WorkerInstanceEpoch ||
		!bytes.Equal(authority.GetDeviceSetDigest(), binding.DeviceSetDigest) ||
		!bytes.Equal(authority.GetMembershipDigest(), binding.MembershipDigest) ||
		authority.GetModelResidencyId() != binding.ModelResidencyID ||
		authority.GetModelRuntimeIdentity() != binding.ModelRuntimeIdentity ||
		authority.GetStageProfileRevisionId() != binding.StageProfileRevisionID {
		return ErrRuntimeMismatch
	}
	devices := slices.Clone(binding.Devices)
	slices.SortFunc(devices, func(left, right DeviceEpoch) int {
		if compared := strings.Compare(left.ID, right.ID); compared != 0 {
			return compared
		}
		return intCompare(left.Epoch, right.Epoch)
	})
	if len(devices) != len(authority.GetDevices()) {
		return ErrRuntimeMismatch
	}
	for index, device := range authority.GetDevices() {
		if device.GetDeviceId() != devices[index].ID || device.GetDeviceEpoch() != devices[index].Epoch {
			return ErrRuntimeMismatch
		}
	}
	members := slices.Clone(binding.Members)
	slices.SortFunc(members, func(left, right MemberEpoch) int {
		if compared := strings.Compare(left.ID, right.ID); compared != 0 {
			return compared
		}
		return intCompare(left.Epoch, right.Epoch)
	})
	if len(members) != len(authority.GetMembers()) {
		return ErrRuntimeMismatch
	}
	memberBindingMatched := false
	for index, member := range authority.GetMembers() {
		if member.GetWorkerMemberId() != members[index].ID || member.GetMemberEpoch() != members[index].Epoch {
			return ErrRuntimeMismatch
		}
		if member.GetWorkerMemberId() == binding.WorkerMemberID &&
			member.GetMemberEpoch() == binding.WorkerMemberEpoch &&
			member.GetModelRuntimeEpoch() == binding.ModelRuntimeEpoch {
			memberBindingMatched = true
		}
	}
	if !memberBindingMatched {
		return ErrRuntimeMismatch
	}
	return nil
}

func intCompare(left, right int64) int {
	if left < right {
		return -1
	}
	if left > right {
		return 1
	}
	return 0
}
