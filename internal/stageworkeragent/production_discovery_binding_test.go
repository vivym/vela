package stageworkeragent_test

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/journalbinding"
	"github.com/vivym/vela/internal/stageauthority"
	"github.com/vivym/vela/internal/stageworkeragent"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

func TestDurableRuntimeDiscoveryVerifiesRegistryAndMemberPair(t *testing.T) {
	for _, fault := range []string{"valid-local", "valid-peer", "missing", "signature", "unknown-key", "envelope-unknown", "claim-unknown", "pair-unknown", "timestamp-unknown", "response-unknown", "identity-unknown", "oversize", "worker", "worker-epoch", "member", "member-epoch", "other-pair", "other-claim", "invalid-local", "missing-verifier"} {
		t.Run(fault, func(t *testing.T) {
			identity := productionRuntimeIdentity()
			binding, verifier := admissionRegistryBinding(t, stageworkeragent.AssignmentAdmissionConfig{
				WorkerInstanceID: uuid.MustParse(identity.GetWorkerInstanceId()), WorkerInstanceEpoch: identity.GetWorkerInstanceEpoch(),
				WorkerMemberID: uuid.MustParse(identity.GetWorkerMemberId()),
				Bindings:       []stageworkeragent.AdmissionRuntimeBinding{{Runtime: stageauthority.RuntimeBinding{WorkerMemberEpoch: identity.GetWorkerMemberEpoch()}}},
			}, stageworkeragent.AssignmentJournalStatus{JournalID: uuid.New(), Scope: [32]byte{1}}, nil)
			expected := stageworkeragent.RuntimeIdentityExpectation{
				WorkerInstanceID: identity.GetWorkerInstanceId(), WorkerInstanceEpoch: identity.GetWorkerInstanceEpoch(),
				WorkerMemberID: identity.GetWorkerMemberId(), WorkerMemberEpoch: identity.GetWorkerMemberEpoch(),
				RegistryBinding: proto.Clone(binding).(*velav1.WorkerBootstrapBinding), RegistryVerifier: verifier,
			}
			response := &velav1.ModelRuntimeServiceDiscoverRuntimeIdentitiesResponse{Identities: []*velav1.ModelRuntimeIdentity{identity}, JournalBinding: binding}
			resign := false
			switch fault {
			case "valid-peer":
				expected.RegistryBinding = nil
			case "missing":
				response.JournalBinding = nil
			case "signature":
				binding.Signature[0] ^= 1
			case "unknown-key":
				binding.SigningKeyId = "untrusted"
			case "envelope-unknown":
				binding.ProtoReflect().SetUnknown([]byte{0x78, 1})
			case "claim-unknown":
				binding.Claim.ProtoReflect().SetUnknown([]byte{0x78, 1})
			case "pair-unknown":
				binding.Pair.ProtoReflect().SetUnknown([]byte{0x78, 1})
			case "timestamp-unknown":
				binding.Pair.RecordedAt.ProtoReflect().SetUnknown([]byte{0x78, 1})
			case "response-unknown":
				response.ProtoReflect().SetUnknown([]byte{0x78, 1})
			case "identity-unknown":
				identity.ProtoReflect().SetUnknown([]byte{0x78, 1})
			case "oversize":
				response.Detail = string(bytes.Repeat([]byte{'a'}, 64<<10))
			case "worker":
				binding.Claim.WorkerInstanceId, resign = uuid.NewString(), true
			case "worker-epoch":
				binding.Claim.WorkerInstanceEpoch++
				resign = true
			case "member":
				binding.Claim.WorkerMemberId, resign = uuid.NewString(), true
			case "member-epoch":
				binding.Claim.WorkerMemberEpoch++
				resign = true
			case "other-pair":
				binding.Pair.RuntimeJournalId, resign = uuid.NewString(), true
			case "other-claim":
				binding.Claim.BundleDigest[0] ^= 1
				resign = true
			case "invalid-local":
				expected.RegistryBinding.Signature[0] ^= 1
			case "missing-verifier":
				expected.RegistryVerifier = nil
			}
			if resign {
				binding.Signature, binding.SigningKeyId = nil, ""
				signer, err := journalbinding.NewSigner("registry", bytes.Repeat([]byte{31}, ed25519.SeedSize))
				if err != nil {
					t.Fatal(err)
				}
				response.JournalBinding, err = signer.Sign(binding)
				if err != nil {
					t.Fatal(err)
				}
				if fault != "other-pair" && fault != "other-claim" {
					expected.RegistryBinding = nil
				}
			}
			runtime := &journalDiscoveryReply{response: response}
			identities, err := stageworkeragent.DiscoverRuntimeIdentities(t.Context(), runtime, expected)
			if fault == "valid-local" || fault == "valid-peer" {
				if err != nil || len(identities) != 1 || !proto.Equal(identities[0], identity) {
					t.Fatalf("valid journal binding: %v %v", identities, err)
				}
				identities[0].ModelRuntimeEpoch++
				if identity.GetModelRuntimeEpoch() == identities[0].GetModelRuntimeEpoch() {
					t.Fatal("discovery leaked mutable response identities")
				}
			} else if err == nil || identities != nil {
				t.Fatalf("invalid ownership produced Runtime routes: %v %v", identities, err)
			}
		})
	}
}

func TestRuntimeDiscoveryDoesNotReturnCanceledReply(t *testing.T) {
	for _, before := range []bool{true, false} {
		identity := productionRuntimeIdentity()
		ctx, cancel := context.WithCancel(t.Context())
		runtime := &journalDiscoveryReply{response: &velav1.ModelRuntimeServiceDiscoverRuntimeIdentitiesResponse{Identities: []*velav1.ModelRuntimeIdentity{identity}}, call: cancel}
		if before {
			cancel()
		}
		identities, err := stageworkeragent.DiscoverRuntimeIdentities(ctx, runtime, stageworkeragent.RuntimeIdentityExpectation{
			WorkerInstanceID: identity.GetWorkerInstanceId(), WorkerInstanceEpoch: identity.GetWorkerInstanceEpoch(),
			WorkerMemberID: identity.GetWorkerMemberId(), WorkerMemberEpoch: identity.GetWorkerMemberEpoch(),
		})
		cancel()
		if identities != nil || !errors.Is(err, context.Canceled) || before && runtime.calls != 0 || !before && runtime.calls != 1 {
			t.Fatalf("canceled discovery returned routes: %v %v calls=%d", identities, err, runtime.calls)
		}
	}
}

type journalDiscoveryReply struct {
	response *velav1.ModelRuntimeServiceDiscoverRuntimeIdentitiesResponse
	call     func()
	calls    int
}

func (runtime *journalDiscoveryReply) DiscoverRuntimeIdentities(context.Context, *velav1.ModelRuntimeServiceDiscoverRuntimeIdentitiesRequest, ...grpc.CallOption) (*velav1.ModelRuntimeServiceDiscoverRuntimeIdentitiesResponse, error) {
	runtime.calls++
	if runtime.call != nil {
		runtime.call()
	}
	return runtime.response, nil
}
