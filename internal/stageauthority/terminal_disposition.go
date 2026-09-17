package stageauthority

import (
	"bytes"
	"cmp"
	"crypto/ed25519"
	"crypto/sha256"
	"errors"
	"slices"
	"strings"
	"time"

	"github.com/google/uuid"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

const (
	TerminalDispositionSchemaVersion = 1
	MaxTerminalDispositionValidity   = 5 * time.Minute
	// Terminal observations cross the database, Control and Worker clocks.
	// This narrow future bound applies only to signed restrictive history. It
	// never extends an execution lease or the disposition's expiry, and cannot
	// establish writer drain. Larger clock errors remain fail-closed.
	MaxTerminalObservationSkew  = time.Second
	maxTerminalDispositionBytes = 3 << 20
	terminalDispositionDomain   = "vela-stage-terminal-disposition-v1\x00"
)

var ErrInvalidTerminalDisposition = errors.New("stage terminal disposition is invalid")

// VerifiedTerminalDisposition contains no execution time or writer-drain proof.
type VerifiedTerminalDisposition struct {
	Disposition *velav1.StageTerminalDisposition
	Digest      [sha256.Size]byte
}

// SignTerminalDisposition reuses execution verifier keys with a separate message
// domain. It does not establish Control-exclusive signing against seed holders.
func (signer *Signer) SignTerminalDisposition(value *velav1.StageTerminalDisposition) (*velav1.StageTerminalDisposition, error) {
	if signer == nil {
		return nil, ErrInvalidTerminalDisposition
	}
	canonical, err := canonicalTerminalDisposition(value, false)
	if err != nil {
		return nil, err
	}
	key, ok := signer.keys[canonical.GetSigningKeyId()]
	if !ok {
		return nil, ErrUnknownKey
	}
	canonical.Signature = nil
	payload, err := terminalDispositionPayload(canonical)
	if err != nil {
		return nil, err
	}
	canonical.Signature = ed25519.Sign(key, payload)
	return canonical, nil
}

// ValidateTerminalDispositionEnvelope authenticates facts before floor admission.
// Callers must additionally match their Worker/member/runtime or original request.
func (validator *Validator) ValidateTerminalDispositionEnvelope(value *velav1.StageTerminalDisposition) (VerifiedTerminalDisposition, error) {
	if validator == nil || validator.now == nil {
		return VerifiedTerminalDisposition{}, ErrInvalidTerminalDisposition
	}
	verified, err := validator.ValidateTerminalDispositionSignature(value)
	if err != nil {
		return VerifiedTerminalDisposition{}, err
	}
	now := validator.now().UTC()
	if now.Add(MaxTerminalObservationSkew).Before(verified.Disposition.GetObservedAt().AsTime()) || !now.Before(verified.Disposition.GetExpiresAt().AsTime()) {
		return VerifiedTerminalDisposition{}, ErrStale
	}
	return verified, nil
}

// ValidateTerminalDispositionForReplay accepts expired historical facts but
// rejects observations beyond the bounded clock skew. It grants no new floor,
// execution or drain.
func (validator *Validator) ValidateTerminalDispositionForReplay(value *velav1.StageTerminalDisposition) (VerifiedTerminalDisposition, error) {
	verified, err := validator.ValidateTerminalDispositionSignature(value)
	if err != nil {
		return verified, err
	}
	if validator.now().UTC().Add(MaxTerminalObservationSkew).Before(verified.Disposition.GetObservedAt().AsTime()) {
		return VerifiedTerminalDisposition{}, ErrStale
	}
	return verified, nil
}

// FindTerminalAllocation selects an identity from already verified history.
func FindTerminalAllocation(disposition *velav1.StageTerminalDisposition, id string) *velav1.StageTerminalAllocation {
	for _, allocation := range disposition.GetAllocations() {
		if allocation.GetStageAllocationId() == id {
			return allocation
		}
	}
	return nil
}

// ValidateSameTerminalAllocation compares already verified histories. Refreshed
// query anchors/sessions/times may differ; the selected terminal identity may not.
func ValidateSameTerminalAllocation(a, b *velav1.StageTerminalDisposition, id string) error {
	allocation := FindTerminalAllocation(b, id)
	if a == nil || b == nil || allocation == nil || a.GetOrganizationId() != b.GetOrganizationId() || a.GetProjectId() != b.GetProjectId() || a.GetJobId() != b.GetJobId() ||
		a.GetAttemptId() != b.GetAttemptId() || a.GetStageRunId() != b.GetStageRunId() || a.GetTerminalState() != b.GetTerminalState() ||
		a.GetStageFence() != b.GetStageFence() || a.GetStageVersion() != b.GetStageVersion() ||
		a.GetWorkerInstanceId() != b.GetWorkerInstanceId() || a.GetWorkerInstanceEpoch() != b.GetWorkerInstanceEpoch() ||
		!bytes.Equal(a.GetDeviceSetDigest(), b.GetDeviceSetDigest()) || !bytes.Equal(a.GetMembershipDigest(), b.GetMembershipDigest()) ||
		len(a.GetDevices()) != len(b.GetDevices()) || !proto.Equal(FindTerminalAllocation(a, id), allocation) {
		return ErrInvalidTerminalDisposition
	}
	for i, device := range a.GetDevices() {
		if !proto.Equal(device, b.GetDevices()[i]) {
			return ErrInvalidTerminalDisposition
		}
	}
	return nil
}

// ValidateTerminalDispositionSignature authenticates retained restrictive facts
// without granting freshness, execution, or drain. Recovery must still bind the
// stored fact to its trusted Worker/member topology before restoring a floor.
func (validator *Validator) ValidateTerminalDispositionSignature(value *velav1.StageTerminalDisposition) (VerifiedTerminalDisposition, error) {
	if validator == nil {
		return VerifiedTerminalDisposition{}, ErrInvalidTerminalDisposition
	}
	canonical, err := canonicalTerminalDisposition(value, true)
	if err != nil {
		return VerifiedTerminalDisposition{}, err
	}
	key, ok := validator.keys[canonical.GetSigningKeyId()]
	if !ok {
		return VerifiedTerminalDisposition{}, ErrUnknownKey
	}
	signature := canonical.Signature
	canonical.Signature = nil
	payload, err := terminalDispositionPayload(canonical)
	if err != nil {
		return VerifiedTerminalDisposition{}, err
	}
	if !ed25519.Verify(key, payload, signature) {
		return VerifiedTerminalDisposition{}, ErrInvalidSignature
	}
	canonical.Signature = signature
	wire, err := terminalDispositionPayload(canonical)
	if err != nil {
		return VerifiedTerminalDisposition{}, err
	}
	return VerifiedTerminalDisposition{Disposition: canonical, Digest: sha256.Sum256(wire)}, nil
}

// ValidateTerminalDisposition binds a response to an exact historical envelope
// and authenticated Worker session. Expiry is permitted only for that envelope.
func (validator *Validator) ValidateTerminalDisposition(
	value *velav1.StageTerminalDisposition, authority *velav1.StageAuthority,
	workerMemberID string, sessionEpoch int64,
) (VerifiedTerminalDisposition, error) {
	original, err := validator.ValidateEnvelopeForReplay(authority, 0)
	if err != nil {
		return VerifiedTerminalDisposition{}, err
	}
	verified, err := validator.ValidateTerminalDispositionEnvelope(value)
	if err != nil {
		return VerifiedTerminalDisposition{}, err
	}
	d, a := verified.Disposition, original.Authority
	if a.GetSchemaVersion() != SchemaVersionV2 || !bytes.Equal(d.GetOriginalAuthorityDigest(), original.Digest[:]) ||
		d.GetStageAttemptId() != a.GetStageAttemptId() || d.GetStageAllocationId() != a.GetStageAllocationId() || d.GetStageLeaseId() != a.GetStageLeaseId() ||
		d.GetWorkerMemberId() != workerMemberID || d.GetControlSessionEpoch() != sessionEpoch {
		return VerifiedTerminalDisposition{}, ErrInvalidTerminalDisposition
	}
	for _, allocation := range d.GetAllocations() {
		if allocation.GetStageAllocationId() != a.GetStageAllocationId() {
			continue
		}
		if err := ValidateTerminalAllocation(d, allocation, a); err != nil {
			return VerifiedTerminalDisposition{}, err
		}
		return verified, nil
	}
	return VerifiedTerminalDisposition{}, ErrInvalidTerminalDisposition
}

// ValidateTerminalAllocation binds an allocation from a verified terminal history
// to a separately verified execution envelope. It performs no signature/freshness
// validation and grants no execution, drain or deletion permission.
func ValidateTerminalAllocation(d *velav1.StageTerminalDisposition, allocation *velav1.StageTerminalAllocation, a *velav1.StageAuthority) error {
	if d == nil || allocation == nil || a == nil || a.GetSchemaVersion() != SchemaVersionV2 ||
		d.GetJobId() != a.GetJobId() || d.GetAttemptId() != a.GetAttemptId() || d.GetStageRunId() != a.GetStageRunId() ||
		d.GetWorkerInstanceId() != a.GetWorkerInstanceId() || d.GetWorkerInstanceEpoch() != a.GetWorkerInstanceEpoch() ||
		d.GetStageFence() < a.GetStageFence() || d.GetStageVersion() <= a.GetStageVersion() ||
		d.GetObservedAt().AsTime().Before(a.GetIssuedAt().AsTime()) ||
		!bytes.Equal(d.GetDeviceSetDigest(), a.GetDeviceSetDigest()) || !bytes.Equal(d.GetMembershipDigest(), a.GetMembershipDigest()) ||
		len(d.GetDevices()) != len(a.GetDevices()) || len(allocation.GetMembers()) != len(a.GetMembers()) ||
		allocation.GetStageAttemptId() != a.GetStageAttemptId() || allocation.GetStageAllocationId() != a.GetStageAllocationId() ||
		allocation.GetStageLeaseId() != a.GetStageLeaseId() || allocation.GetExecutionSequence() != a.GetExecutionSequence() ||
		!bytes.Equal(allocation.GetExecutionNonce(), a.GetExecutionNonce()) || allocation.GetModelResidencyId() != a.GetModelResidencyId() ||
		allocation.GetModelRuntimeIdentity() != a.GetModelRuntimeIdentity() || allocation.GetStageProfileRevisionId() != a.GetStageProfileRevisionId() ||
		allocation.GetBarrierGeneration() != a.GetModelRuntimeBarrierGeneration() {
		return ErrInvalidTerminalDisposition
	}
	for i, device := range d.GetDevices() {
		if !proto.Equal(device, a.GetDevices()[i]) {
			return ErrInvalidTerminalDisposition
		}
	}
	for i, member := range allocation.GetMembers() {
		expected := a.GetMembers()[i]
		if member.GetWorkerMemberId() != expected.GetWorkerMemberId() || member.GetMemberEpoch() != expected.GetMemberEpoch() ||
			member.GetModelRuntimeEpoch() != expected.GetModelRuntimeEpoch() || !bytes.Equal(member.GetIdentityDigest(), expected.GetIdentityDigest()) {
			return ErrInvalidTerminalDisposition
		}
	}
	return nil
}

func terminalDispositionPayload(value *velav1.StageTerminalDisposition) ([]byte, error) {
	wire, err := proto.MarshalOptions{Deterministic: true}.Marshal(value)
	if err != nil {
		return nil, ErrInvalidTerminalDisposition
	}
	return append([]byte(terminalDispositionDomain), wire...), nil
}

func canonicalTerminalDisposition(value *velav1.StageTerminalDisposition, signed bool) (*velav1.StageTerminalDisposition, error) {
	if value == nil || proto.Size(value) > maxTerminalDispositionBytes || unknownTerminalFields(value.ProtoReflect()) {
		return nil, ErrInvalidTerminalDisposition
	}
	d := proto.Clone(value).(*velav1.StageTerminalDisposition)
	slices.SortFunc(d.Devices, func(a, b *velav1.StageAuthorityDeviceEpoch) int {
		return strings.Compare(a.GetDeviceId(), b.GetDeviceId())
	})
	slices.SortFunc(d.Allocations, func(a, b *velav1.StageTerminalAllocation) int {
		return cmp.Compare(a.GetExecutionSequence(), b.GetExecutionSequence())
	})
	for _, allocation := range d.Allocations {
		if allocation != nil {
			slices.SortFunc(allocation.Members, func(a, b *velav1.StageTerminalMember) int {
				return strings.Compare(a.GetWorkerMemberId(), b.GetWorkerMemberId())
			})
		}
	}
	if err := validateTerminalDispositionShape(d, signed); err != nil {
		return nil, err
	}
	return d, nil
}

func validateTerminalDispositionShape(d *velav1.StageTerminalDisposition, signed bool) error {
	if d.GetSchemaVersion() != TerminalDispositionSchemaVersion || d.GetInputDisposition() != velav1.StageInputDisposition_STAGE_INPUT_DISPOSITION_INPUTS_UNUSED ||
		!terminalUUIDs(d.GetOrganizationId(), d.GetProjectId(), d.GetJobId(), d.GetAttemptId(), d.GetStageRunId(), d.GetStageAttemptId(), d.GetStageAllocationId(), d.GetStageLeaseId(), d.GetWorkerInstanceId(), d.GetWorkerMemberId()) ||
		d.GetWorkerInstanceEpoch() <= 0 || d.GetControlSessionEpoch() <= 0 || d.GetStageFence() <= 0 || d.GetStageVersion() <= 0 || d.GetCutoff() <= 0 ||
		!terminalDigests(d.GetOriginalAuthorityDigest(), d.GetDeviceSetDigest(), d.GetMembershipDigest()) ||
		d.GetSigningKeyId() == "" || strings.TrimSpace(d.GetSigningKeyId()) != d.GetSigningKeyId() || len(d.GetSigningKeyId()) > 100 ||
		(signed && len(d.GetSignature()) != ed25519.SignatureSize) {
		return ErrInvalidTerminalDisposition
	}
	switch d.GetTerminalState() {
	case velav1.StageTerminalState_STAGE_TERMINAL_STATE_SUCCEEDED, velav1.StageTerminalState_STAGE_TERMINAL_STATE_FAILED, velav1.StageTerminalState_STAGE_TERMINAL_STATE_CANCELED:
	default:
		return ErrInvalidTerminalDisposition
	}
	if d.GetObservedAt() == nil || d.GetExpiresAt() == nil || d.GetObservedAt().CheckValid() != nil || d.GetExpiresAt().CheckValid() != nil {
		return ErrInvalidTerminalDisposition
	}
	validity := d.GetExpiresAt().AsTime().Sub(d.GetObservedAt().AsTime())
	if validity <= 0 || validity > MaxTerminalDispositionValidity || len(d.GetDevices()) == 0 || len(d.GetDevices()) > 64 || len(d.GetAllocations()) == 0 || len(d.GetAllocations()) > 256 {
		return ErrInvalidTerminalDisposition
	}
	var previousDevice string
	for _, device := range d.GetDevices() {
		if !terminalUUIDs(device.GetDeviceId()) || device.GetDeviceId() <= previousDevice || device.GetDeviceEpoch() <= 0 {
			return ErrInvalidTerminalDisposition
		}
		previousDevice = device.GetDeviceId()
	}
	seenAttempts, seenAllocations, seenLeases := map[string]bool{}, map[string]bool{}, map[string]bool{}
	var previous int64
	originalFound := false
	for _, allocation := range d.GetAllocations() {
		if !terminalUUIDs(allocation.GetStageAttemptId(), allocation.GetStageAllocationId(), allocation.GetStageLeaseId(), allocation.GetModelResidencyId(), allocation.GetStageProfileRevisionId()) ||
			seenAttempts[allocation.GetStageAttemptId()] || seenAllocations[allocation.GetStageAllocationId()] || seenLeases[allocation.GetStageLeaseId()] ||
			allocation.GetExecutionSequence() <= previous || allocation.GetBarrierGeneration() <= 0 || !terminalDigests(allocation.GetExecutionNonce()) ||
			strings.TrimSpace(allocation.GetModelRuntimeIdentity()) == "" || len(allocation.GetModelRuntimeIdentity()) > 200 {
			return ErrInvalidTerminalDisposition
		}
		seenAttempts[allocation.GetStageAttemptId()], seenAllocations[allocation.GetStageAllocationId()], seenLeases[allocation.GetStageLeaseId()] = true, true, true
		previous = allocation.GetExecutionSequence()
		if allocation.GetStageAllocationId() == d.GetStageAllocationId() {
			if allocation.GetStageAttemptId() != d.GetStageAttemptId() || allocation.GetStageLeaseId() != d.GetStageLeaseId() {
				return ErrInvalidTerminalDisposition
			}
			originalFound = true
		}
		if err := validateTerminalMembers(allocation.GetMembers(), d.GetAllocations()[0].GetMembers(), d.GetWorkerMemberId()); err != nil {
			return err
		}
	}
	if !originalFound || previous != d.GetCutoff() {
		return ErrInvalidTerminalDisposition
	}
	return nil
}

func validateTerminalMembers(members, first []*velav1.StageTerminalMember, caller string) error {
	if len(members) == 0 || len(members) > 64 || len(members) != len(first) {
		return ErrInvalidTerminalDisposition
	}
	var previous string
	found := false
	for i, member := range members {
		if !terminalUUIDs(member.GetWorkerMemberId()) || member.GetWorkerMemberId() <= previous || member.GetMemberEpoch() <= 0 || member.GetModelRuntimeEpoch() <= 0 ||
			!terminalDigests(member.GetIdentityDigest(), member.GetDeviceSubsetDigest()) ||
			member.GetWorkerMemberId() != first[i].GetWorkerMemberId() || member.GetMemberEpoch() != first[i].GetMemberEpoch() ||
			!bytes.Equal(member.GetIdentityDigest(), first[i].GetIdentityDigest()) || !bytes.Equal(member.GetDeviceSubsetDigest(), first[i].GetDeviceSubsetDigest()) {
			return ErrInvalidTerminalDisposition
		}
		previous = member.GetWorkerMemberId()
		found = found || previous == caller
	}
	if !found {
		return ErrInvalidTerminalDisposition
	}
	return nil
}

func terminalUUIDs(values ...string) bool {
	for _, value := range values {
		parsed, err := uuid.Parse(value)
		if err != nil || parsed == uuid.Nil || value != parsed.String() {
			return false
		}
	}
	return true
}

func terminalDigests(values ...[]byte) bool {
	for _, value := range values {
		if len(value) != sha256.Size {
			return false
		}
	}
	return true
}

// Unknown fields cannot acquire meaning in a newer verifier under a v1 signature.
func unknownTerminalFields(message protoreflect.Message) bool {
	if len(message.GetUnknown()) != 0 {
		return true
	}
	unknown := false
	message.Range(func(field protoreflect.FieldDescriptor, value protoreflect.Value) bool {
		if field.Kind() == protoreflect.MessageKind {
			if field.IsList() {
				for i := 0; i < value.List().Len() && !unknown; i++ {
					unknown = unknownTerminalFields(value.List().Get(i).Message())
				}
			} else {
				unknown = unknownTerminalFields(value.Message())
			}
		}
		return !unknown
	})
	return unknown
}
