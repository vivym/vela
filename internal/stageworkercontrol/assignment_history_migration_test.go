package stageworkercontrol

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/vivym/vela/internal/stageauthority"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestStageAssignmentAuthorityEvidencePreservesSignedAuthorityAndOriginalWire(t *testing.T) {
	authority, _, validator := assignmentHistoryAuthorityFixture(t)
	const customerContent = "customer-prompt-and-private-url"
	assignment := &velav1.StageAssignment{
		Authority: authority,
		ExecutionSpec: &velav1.StageExecutionSpec{
			ParametersJson: []byte(`{"prompt":"` + customerContent + `"}`),
		},
		RootInputFetches: []*velav1.StageRootInputFetch{{DownloadUrl: "https://private.invalid/" + customerContent}},
	}
	baseWire, err := proto.MarshalOptions{Deterministic: true}.Marshal(assignment)
	if err != nil {
		t.Fatal(err)
	}
	wire := append(bytes.Clone(baseWire), assignmentHistoryUnknownField(customerContent)...)
	beforeWire := bytes.Clone(wire)
	beforeAuthority := proto.Clone(authority)
	evidence, err := stageAssignmentAuthorityEvidence(authority, wire)
	if err != nil {
		t.Fatalf("construct authority evidence: %v", err)
	}
	wantAssignmentDigest := sha256.Sum256(wire)
	if evidence["assignment_digest"] != hex.EncodeToString(wantAssignmentDigest[:]) {
		t.Fatal("assignment digest does not bind the original wire including outer unknown fields")
	}
	baseEvidence, err := stageAssignmentAuthorityEvidence(authority, baseWire)
	if err != nil {
		t.Fatal(err)
	}
	if baseEvidence["assignment_digest"] == evidence["assignment_digest"] ||
		baseEvidence["authority_wire"] != evidence["authority_wire"] ||
		baseEvidence["authority_digest"] != evidence["authority_digest"] {
		t.Fatal("delivery-only bytes changed signed authority evidence or were omitted from the delivery digest")
	}
	encodedAuthority, ok := evidence["authority_wire"].(string)
	if !ok {
		t.Fatal("authority wire is not hexadecimal text")
	}
	authorityWire, err := hex.DecodeString(encodedAuthority)
	if err != nil {
		t.Fatal(err)
	}
	var stored velav1.StageAuthority
	if err := proto.Unmarshal(authorityWire, &stored); err != nil {
		t.Fatal(err)
	}
	verified, err := validator.ValidateEnvelopeForReplay(&stored, 0)
	if err != nil {
		t.Fatalf("stored original signature no longer validates: %v", err)
	}
	if !proto.Equal(authority, &stored) || !bytes.Equal(authority.GetSignature(), stored.GetSignature()) ||
		evidence["authority_digest"] != hex.EncodeToString(verified.Digest[:]) {
		t.Fatal("stored authority changed the original signed envelope")
	}
	if !bytes.Equal(wire, beforeWire) || !proto.Equal(authority, beforeAuthority) {
		t.Fatal("evidence construction mutated its inputs")
	}
	evidenceJSON, err := json.Marshal(evidence)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(evidenceJSON, []byte(customerContent)) ||
		bytes.Contains(evidenceJSON, []byte(hex.EncodeToString([]byte(customerContent)))) {
		t.Fatal("authority evidence retained delivery Customer Content")
	}
}

func TestStageAssignmentAuthorityEvidenceRejectsUnknownAuthorityFields(t *testing.T) {
	for _, test := range []struct {
		name   string
		target func(*velav1.StageAuthority) proto.Message
	}{
		{name: "authority", target: func(a *velav1.StageAuthority) proto.Message { return a }},
		{name: "device", target: func(a *velav1.StageAuthority) proto.Message { return a.Devices[0] }},
		{name: "member", target: func(a *velav1.StageAuthority) proto.Message { return a.Members[0] }},
		{name: "issued timestamp", target: func(a *velav1.StageAuthority) proto.Message { return a.IssuedAt }},
		{name: "expiry timestamp", target: func(a *velav1.StageAuthority) proto.Message { return a.ExpiresAt }},
		{name: "monotonic duration", target: func(a *velav1.StageAuthority) proto.Message { return a.MonotonicValidFor }},
	} {
		t.Run(test.name, func(t *testing.T) {
			authority, signer, validator := assignmentHistoryAuthorityFixture(t)
			test.target(authority).ProtoReflect().SetUnknown(assignmentHistoryUnknownField("unknown-customer-content"))
			signed, err := signer.Sign(authority)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := validator.ValidateEnvelopeForReplay(signed, 0); err != nil {
				t.Fatalf("fixture is not a validly signed authority: %v", err)
			}
			wire, err := proto.Marshal(&velav1.StageAssignment{Authority: signed})
			if err != nil {
				t.Fatal(err)
			}
			evidence, err := stageAssignmentAuthorityEvidence(signed, wire)
			if err == nil || evidence != nil || !strings.Contains(err.Error(), "unknown protobuf fields") {
				t.Fatalf("unknown fields = evidence %v, error %v", evidence != nil, err)
			}
		})
	}
}

func TestAssignmentHistoryErrorsRedactDatabaseAndContextDetails(t *testing.T) {
	const secret = "assignment-customer-content-secret"
	for _, test := range []struct {
		name  string
		err   error
		cause error
	}{
		{name: "database", err: &pgconn.PgError{Code: "55000", Message: secret, Detail: secret,
			Hint: secret, Where: secret, ConstraintName: secret}},
		{name: "ordinary", err: errors.New(secret)},
		{name: "canceled", err: fmt.Errorf("%s: %w", secret, context.Canceled), cause: context.Canceled},
		{name: "deadline", err: fmt.Errorf("%s: %w", secret, context.DeadlineExceeded), cause: context.DeadlineExceeded},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := redactedAssignmentHistoryError("record history", test.err)
			if strings.Contains(err.Error(), secret) || !strings.Contains(err.Error(), "record history") {
				t.Fatal("redacted error leaked details or lost the operation name")
			}
			if test.cause != nil && !errors.Is(err, test.cause) {
				t.Fatalf("redacted error lost cancellation identity: %v", err)
			}
			if test.name == "database" && !strings.Contains(err.Error(), "SQLSTATE 55000") {
				t.Fatal("redacted database error lost SQLSTATE")
			}
		})
	}
}

func assignmentHistoryUnknownField(content string) []byte {
	return protowire.AppendString(protowire.AppendTag(nil, 2047, protowire.BytesType), content)
}

func assignmentHistoryAuthorityFixture(t *testing.T) (*velav1.StageAuthority, *stageauthority.Signer, *stageauthority.Validator) {
	t.Helper()
	const id = "58000000-0000-0000-0000-000000000001"
	now := time.Date(2026, 9, 5, 0, 0, 0, 0, time.UTC)
	digest := bytes.Repeat([]byte{0x51}, sha256.Size)
	keys := map[string][]byte{"history-test": bytes.Repeat([]byte{0x61}, sha256.Size)}
	signer, err := stageauthority.NewSigner(keys)
	if err != nil {
		t.Fatal(err)
	}
	validator, err := stageauthority.NewValidator(keys, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	signed, err := signer.Sign(&velav1.StageAuthority{
		SchemaVersion: 2, ExecutionSequence: 1, JobId: id, AttemptId: id, StageRunId: id,
		StageAttemptId: id, StageAllocationId: id, StageLeaseId: id,
		AttemptFence: 1, StageFence: 1, StageVersion: 1, WorkerInstanceId: id, WorkerInstanceEpoch: 1,
		DeviceSetDigest: digest, MembershipDigest: digest, ModelResidencyId: id, ModelRuntimeIdentity: "test-runtime",
		ModelRuntimeBarrierGeneration: 1, StageProfileRevisionId: id, CapacityObservationSequence: 1,
		CapacityVector: map[string]int64{"active_stage_slots": 1}, LeaseToken: digest, ExecutionNonce: digest,
		ExecutionSpecDigest: digest, SigningKeyId: "history-test", IssuedAt: timestamppb.New(now),
		ExpiresAt: timestamppb.New(now.Add(time.Minute)), MonotonicValidFor: durationpb.New(time.Minute),
		Devices: []*velav1.StageAuthorityDeviceEpoch{{DeviceId: id, DeviceEpoch: 1}},
		Members: []*velav1.StageAuthorityMemberEpoch{{
			WorkerMemberId: id, MemberEpoch: 1, ModelRuntimeEpoch: 1, IdentityDigest: digest,
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	return signed, signer, validator
}
