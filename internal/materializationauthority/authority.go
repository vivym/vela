package materializationauthority

import (
	"crypto/hmac"
	"crypto/sha256"
	"errors"
	"fmt"
	"mime"
	"slices"
	"strings"
	"time"

	"github.com/google/uuid"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/protobuf/proto"
)

const maxAuthorityValidity = 24 * time.Hour

var (
	ErrInvalid          = errors.New("MaterializationAuthority is invalid")
	ErrInvalidSignature = errors.New("MaterializationAuthority token is invalid")
	ErrUnknownKey       = errors.New("MaterializationAuthority signing key is unknown")
	ErrStale            = errors.New("MaterializationAuthority is stale")
)

type Verified struct {
	Authority *velav1.MaterializationAuthority
	Digest    [sha256.Size]byte
}

type Signer struct {
	keys map[string][]byte
}

type Validator struct {
	keys map[string][]byte
	now  func() time.Time
}

func NewSigner(keys map[string][]byte) (*Signer, error) {
	validated, err := validateKeyring(keys)
	if err != nil {
		return nil, err
	}
	return &Signer{keys: validated}, nil
}

func NewValidator(keys map[string][]byte, now func() time.Time) (*Validator, error) {
	validated, err := validateKeyring(keys)
	if err != nil {
		return nil, err
	}
	if now == nil {
		now = time.Now
	}
	return &Validator{keys: validated, now: now}, nil
}

func (signer *Signer) Sign(
	authority *velav1.MaterializationAuthority,
) (*velav1.MaterializationAuthority, error) {
	if signer == nil {
		return nil, errors.New("MaterializationAuthority signer is not configured")
	}
	canonical := clone(authority)
	if canonical == nil {
		return nil, fmt.Errorf("%w: authority is required", ErrInvalid)
	}
	canonical.Token = nil
	if err := validateShape(canonical, false); err != nil {
		return nil, err
	}
	key, ok := signer.keys[canonical.GetSigningKeyId()]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrUnknownKey, canonical.GetSigningKeyId())
	}
	payload, err := proto.MarshalOptions{Deterministic: true}.Marshal(canonical)
	if err != nil {
		return nil, fmt.Errorf("%w: encode token payload: %v", ErrInvalid, err)
	}
	mac := hmac.New(sha256.New, key)
	_, _ = mac.Write(payload)
	canonical.Token = mac.Sum(nil)
	return canonical, nil
}

func (validator *Validator) Validate(
	authority *velav1.MaterializationAuthority,
) (Verified, error) {
	if validator == nil {
		return Verified{}, errors.New("MaterializationAuthority validator is not configured")
	}
	canonical := clone(authority)
	if canonical == nil {
		return Verified{}, fmt.Errorf("%w: authority is required", ErrInvalid)
	}
	if err := validateShape(canonical, true); err != nil {
		return Verified{}, err
	}
	key, ok := validator.keys[canonical.GetSigningKeyId()]
	if !ok {
		return Verified{}, fmt.Errorf("%w: %s", ErrUnknownKey, canonical.GetSigningKeyId())
	}
	token := slices.Clone(canonical.GetToken())
	canonical.Token = nil
	payload, err := proto.MarshalOptions{Deterministic: true}.Marshal(canonical)
	if err != nil {
		return Verified{}, fmt.Errorf("%w: encode token payload: %v", ErrInvalid, err)
	}
	mac := hmac.New(sha256.New, key)
	_, _ = mac.Write(payload)
	if !hmac.Equal(token, mac.Sum(nil)) {
		return Verified{}, ErrInvalidSignature
	}
	canonical.Token = token
	now := validator.now().UTC()
	if now.Before(canonical.GetIssuedAt().AsTime().UTC()) ||
		!now.Before(canonical.GetExpiresAt().AsTime().UTC()) {
		return Verified{}, ErrStale
	}
	digest, err := Digest(canonical)
	if err != nil {
		return Verified{}, err
	}
	return Verified{Authority: clone(canonical), Digest: digest}, nil
}

func Digest(authority *velav1.MaterializationAuthority) ([sha256.Size]byte, error) {
	canonical := clone(authority)
	if canonical == nil {
		return [sha256.Size]byte{}, fmt.Errorf("%w: authority is required", ErrInvalid)
	}
	if err := validateShape(canonical, true); err != nil {
		return [sha256.Size]byte{}, err
	}
	payload, err := proto.MarshalOptions{Deterministic: true}.Marshal(canonical)
	if err != nil {
		return [sha256.Size]byte{}, fmt.Errorf("%w: encode digest payload: %v", ErrInvalid, err)
	}
	return sha256.Sum256(payload), nil
}

func validateShape(authority *velav1.MaterializationAuthority, requireToken bool) error {
	if authority.GetSchemaVersion() != 1 {
		return fmt.Errorf("%w: unsupported schema version", ErrInvalid)
	}
	if len(authority.GetStageAuthorityDigest()) != sha256.Size ||
		len(authority.GetSha256()) != sha256.Size ||
		len(authority.GetLocalReceiptDigest()) != sha256.Size {
		return fmt.Errorf("%w: digest is malformed", ErrInvalid)
	}
	for name, value := range map[string]string{
		"StageMaterializationLease": authority.GetStageMaterializationLeaseId(),
		"StageArtifact":             authority.GetStageArtifactId(),
		"SourceWorkerInstance":      authority.GetSourceWorkerInstanceId(),
		"SourceWorkerMember":        authority.GetSourceWorkerMemberId(),
	} {
		if _, err := uuid.Parse(value); err != nil {
			return fmt.Errorf("%w: %s identity is invalid", ErrInvalid, name)
		}
	}
	if !validObjectKey(authority.GetObjectKey()) || authority.GetSizeBytes() <= 0 ||
		authority.GetSourceWorkerInstanceEpoch() <= 0 ||
		authority.GetSourceWorkerMemberEpoch() <= 0 ||
		len(authority.GetSourceSpiffeIdDigest()) != sha256.Size ||
		authority.GetLocalReceiptId() == "" || len(authority.GetLocalReceiptId()) > 1000 ||
		authority.GetSigningKeyId() == "" || len(authority.GetSigningKeyId()) > 100 ||
		strings.TrimSpace(authority.GetSigningKeyId()) != authority.GetSigningKeyId() {
		return fmt.Errorf("%w: publication identity is incomplete", ErrInvalid)
	}
	contentType := strings.TrimSpace(authority.GetContentType())
	mediaType, _, err := mime.ParseMediaType(contentType)
	if err != nil || mediaType == "" || contentType != authority.GetContentType() ||
		len(contentType) > 200 {
		return fmt.Errorf("%w: content type is invalid", ErrInvalid)
	}
	if authority.GetIssuedAt() == nil || authority.GetExpiresAt() == nil ||
		authority.GetIssuedAt().CheckValid() != nil || authority.GetExpiresAt().CheckValid() != nil {
		return fmt.Errorf("%w: deadlines are invalid", ErrInvalid)
	}
	issuedAt := authority.GetIssuedAt().AsTime().UTC()
	expiresAt := authority.GetExpiresAt().AsTime().UTC()
	if !expiresAt.After(issuedAt) || expiresAt.Sub(issuedAt) > maxAuthorityValidity {
		return fmt.Errorf("%w: deadline interval is invalid", ErrInvalid)
	}
	if requireToken && len(authority.GetToken()) != sha256.Size {
		return ErrInvalidSignature
	}
	return nil
}

func validObjectKey(value string) bool {
	return len(value) > len("artifacts/stage/") && len(value) <= 1024 &&
		strings.HasPrefix(value, "artifacts/stage/") && !strings.Contains(value, "//") &&
		!strings.ContainsRune(value, '\x00')
}

func validateKeyring(keys map[string][]byte) (map[string][]byte, error) {
	if len(keys) == 0 {
		return nil, errors.New("MaterializationAuthority keyring is required")
	}
	validated := make(map[string][]byte, len(keys))
	for keyID, key := range keys {
		if keyID == "" || len(keyID) > 100 || strings.TrimSpace(keyID) != keyID ||
			len(key) < sha256.Size {
			return nil, errors.New("MaterializationAuthority signing key is invalid")
		}
		validated[keyID] = slices.Clone(key)
	}
	return validated, nil
}

func clone(authority *velav1.MaterializationAuthority) *velav1.MaterializationAuthority {
	if authority == nil {
		return nil
	}
	return proto.Clone(authority).(*velav1.MaterializationAuthority)
}
