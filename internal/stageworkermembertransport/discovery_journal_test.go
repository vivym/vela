package stageworkermembertransport

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/journalbinding"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestMemberDiscoveryChecksWorkerJournalBeforeAndAfterRuntime(t *testing.T) {
	for _, fault := range []string{"valid", "closed-before", "closed-during", "changed-during", "wrong-scope", "runtime-pair", "runtime-missing", "canceled-during", "unauthorized"} {
		t.Run(fault, func(t *testing.T) {
			f, runtime, config := newDiscoveryFixture(t)
			binding, _ := memberDiscoveryBinding(t, f.runtime.identity)
			observer := &discoveryJournalObserver{binding: binding}
			config.WorkerJournal = observer
			runtime.response.JournalBinding = proto.Clone(binding).(*velav1.WorkerBootstrapBinding)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			wantCode, wantRuntimeCalls := codes.FailedPrecondition, 1
			switch fault {
			case "valid":
				wantCode = codes.OK
			case "closed-before":
				observer.err, wantRuntimeCalls = errors.New("closed journal"), 0
			case "closed-during":
				runtime.call = func(context.Context, *velav1.ModelRuntimeServiceDiscoverRuntimeIdentitiesRequest) {
					observer.err = errors.New("closed during discovery")
				}
			case "changed-during":
				runtime.call = func(context.Context, *velav1.ModelRuntimeServiceDiscoverRuntimeIdentitiesRequest) {
					observer.binding.Pair.WorkerJournalId = uuid.NewString()
				}
			case "wrong-scope":
				observer.binding.Claim.WorkerMemberId, wantRuntimeCalls = uuid.NewString(), 0
			case "runtime-pair":
				runtime.response.JournalBinding.Pair.RuntimeJournalId = uuid.NewString()
			case "runtime-missing":
				runtime.response.JournalBinding = nil
			case "canceled-during":
				runtime.call = func(context.Context, *velav1.ModelRuntimeServiceDiscoverRuntimeIdentitiesRequest) { cancel() }
				wantCode = codes.Canceled
			case "unauthorized":
				f.auth.identity.SPIFFEID = f.localSPIFFE
				wantCode, wantRuntimeCalls = codes.PermissionDenied, 0
			}
			server, err := NewServer(config)
			if err != nil {
				t.Fatal(err)
			}
			response, err := server.DiscoverRuntimeIdentities(ctx, &velav1.StageWorkerMemberServiceDiscoverRuntimeIdentitiesRequest{
				TargetWorkerMemberId: f.local.ID, Command: discoveryRequestFor(f.runtime.identity),
			})
			if status.Code(err) != wantCode || runtime.calls != wantRuntimeCalls {
				t.Fatalf("journal observation: %v %v runtime calls=%d", response, err, runtime.calls)
			}
			if fault == "valid" {
				if observer.calls != 2 || !proto.Equal(response.GetWorkerJournalBinding(), binding) {
					t.Fatal("discovery did not check both sides of the Runtime RPC")
				}
				response.WorkerJournalBinding.Signature[0] ^= 1
				if !proto.Equal(observer.binding, runtime.response.JournalBinding) {
					t.Fatal("discovery leaked mutable Worker journal evidence")
				}
			} else if response != nil {
				t.Fatal("failed ownership returned evidence")
			}
			if fault == "unauthorized" && observer.calls != 0 {
				t.Fatal("unauthorized caller inspected the Worker journal")
			}
		})
	}
}

func TestMemberDiscoveryClientRequiresBothRegistryBoundJournals(t *testing.T) {
	for _, fault := range []string{"valid", "missing-worker", "missing-runtime", "worker-signature", "runtime-signature", "unknown-worker", "unknown-pair", "different-pair", "different-worker", "different-member", "different-epoch", "different-member-epoch"} {
		t.Run(fault, func(t *testing.T) {
			f, runtime, _ := newDiscoveryFixture(t)
			binding, verifier := memberDiscoveryBinding(t, f.runtime.identity)
			runtime.response.JournalBinding = proto.Clone(binding).(*velav1.WorkerBootstrapBinding)
			service := &discoveryReplyClient{result: runtime.response, workerBinding: binding}
			resign := false
			switch fault {
			case "missing-worker":
				service.workerBinding = nil
			case "missing-runtime":
				runtime.response.JournalBinding = nil
			case "worker-signature":
				binding.Signature[0] ^= 1
			case "runtime-signature":
				runtime.response.JournalBinding.Signature[0] ^= 1
			case "unknown-worker":
				binding.ProtoReflect().SetUnknown([]byte{0x78, 1})
			case "unknown-pair":
				binding.Pair.ProtoReflect().SetUnknown([]byte{0x78, 1})
			case "different-pair":
				binding.Pair.WorkerJournalId, resign = uuid.NewString(), true
			case "different-worker":
				binding.Claim.WorkerInstanceId, resign = uuid.NewString(), true
			case "different-member":
				binding.Claim.WorkerMemberId, resign = uuid.NewString(), true
			case "different-epoch":
				binding.Claim.WorkerInstanceEpoch++
				resign = true
			case "different-member-epoch":
				binding.Claim.WorkerMemberEpoch++
				resign = true
			}
			if resign {
				service.workerBinding = signMemberDiscoveryBinding(t, binding)
				if fault != "different-pair" {
					runtime.response.JournalBinding = proto.Clone(service.workerBinding).(*velav1.WorkerBootstrapBinding)
				}
			}
			client := &Client{targetID: f.local.ID, service: service, registryVerifier: verifier}
			response, err := client.DiscoverRuntimeIdentities(t.Context(), discoveryRequestFor(f.runtime.identity))
			if fault == "valid" {
				if err != nil || !proto.Equal(response.GetJournalBinding(), binding) {
					t.Fatalf("matching journals rejected: %v %v", response, err)
				}
			} else if response != nil || status.Code(err) != codes.FailedPrecondition {
				t.Fatalf("invalid journal pair exposed routes: %v %v", response, err)
			}
		})
	}
}

type discoveryJournalObserver struct {
	binding *velav1.WorkerBootstrapBinding
	err     error
	calls   int
}

func (observer *discoveryJournalObserver) InspectJournalBinding(context.Context) (*velav1.WorkerBootstrapBinding, error) {
	observer.calls++
	return observer.binding, observer.err
}

func memberDiscoveryBinding(t *testing.T, identity *velav1.ModelRuntimeIdentity) (*velav1.WorkerBootstrapBinding, *journalbinding.Verifier) {
	t.Helper()
	id := uuid.NewString()
	value := &velav1.WorkerBootstrapBinding{SchemaVersion: journalbinding.SchemaVersion,
		Claim: &velav1.WorkerBootstrapClaim{RequestId: id, WorkerInstanceId: identity.GetWorkerInstanceId(), WorkerInstanceEpoch: identity.GetWorkerInstanceEpoch(),
			WorkerMemberId: identity.GetWorkerMemberId(), WorkerMemberEpoch: identity.GetWorkerMemberEpoch(), NodeIdentity: "cpu-node", ActorIdentity: "cpu-agent",
			BundleDigest: bytes.Repeat([]byte{1}, 32), ClaimedAt: timestamppb.Now()},
		Pair: &velav1.WorkerBootstrapJournalPair{RequestId: id, ActorIdentity: "cpu-agent", WorkerJournalId: uuid.NewString(), RuntimeJournalId: uuid.NewString(),
			WorkerScope: bytes.Repeat([]byte{2}, 32), RuntimeScope: bytes.Repeat([]byte{3}, 32), RecordedAt: timestamppb.Now()}}
	verifier, err := journalbinding.NewVerifier(map[string][]byte{"registry": ed25519.NewKeyFromSeed(bytes.Repeat([]byte{43}, 32)).Public().(ed25519.PublicKey)})
	if err != nil {
		t.Fatal(err)
	}
	return signMemberDiscoveryBinding(t, value), verifier
}

func signMemberDiscoveryBinding(t *testing.T, value *velav1.WorkerBootstrapBinding) *velav1.WorkerBootstrapBinding {
	t.Helper()
	value = proto.Clone(value).(*velav1.WorkerBootstrapBinding)
	value.Signature, value.SigningKeyId = nil, ""
	signer, err := journalbinding.NewSigner("registry", bytes.Repeat([]byte{43}, 32))
	if err != nil {
		t.Fatal(err)
	}
	signed, err := signer.Sign(value)
	if err != nil {
		t.Fatal(err)
	}
	return signed
}
