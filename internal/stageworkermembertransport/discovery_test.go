package stageworkermembertransport

import (
	"context"
	"crypto/sha256"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func TestMemberDiscoveryRequiresConfiguredLeaderBeforeRuntime(t *testing.T) {
	for _, fault := range []string{"valid", "disabled", "nonleader", "nil", "wrapper-unknown", "command-unknown", "wrong-target", "worker", "worker-epoch", "member", "member-epoch", "canceled"} {
		t.Run(fault, func(t *testing.T) {
			f, runtime, _ := newDiscoveryFixture(t)
			request := &velav1.StageWorkerMemberServiceDiscoverRuntimeIdentitiesRequest{
				TargetWorkerMemberId: f.local.ID, Command: discoveryRequestFor(f.runtime.identity),
			}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			want := codes.OK
			switch fault {
			case "disabled":
				f.server.discoveryLeaderDigest = [sha256.Size]byte{}
				want = codes.FailedPrecondition
			case "nonleader":
				f.auth.identity.SPIFFEID = f.localSPIFFE
				want = codes.PermissionDenied
			case "nil":
				request, want = nil, codes.InvalidArgument
			case "wrapper-unknown":
				request.ProtoReflect().SetUnknown([]byte{0x78, 0x01})
				want = codes.InvalidArgument
			case "command-unknown":
				request.Command.ProtoReflect().SetUnknown([]byte{0x78, 0x01})
				want = codes.InvalidArgument
			case "wrong-target":
				request.TargetWorkerMemberId, want = f.leader.ID, codes.InvalidArgument
			case "worker":
				request.Command.WorkerInstanceId, want = uuid.NewString(), codes.FailedPrecondition
			case "worker-epoch":
				request.Command.WorkerInstanceEpoch++
				want = codes.FailedPrecondition
			case "member":
				request.Command.WorkerMemberId, want = f.leader.ID, codes.FailedPrecondition
			case "member-epoch":
				request.Command.WorkerMemberEpoch++
				want = codes.FailedPrecondition
			case "canceled":
				cancel()
				want = codes.Canceled
			}
			response, err := f.server.DiscoverRuntimeIdentities(ctx, request)
			if status.Code(err) != want {
				t.Fatalf("discovery response=%v error=%v, want %s", response, err, want)
			}
			if fault == "valid" {
				if runtime.calls != 1 || !proto.Equal(response.GetResult(), runtime.response) {
					t.Fatal("discovery did not return the independently configured local identity set")
				}
				response.Result.Identities[0].ModelRuntimeEpoch++
				if runtime.response.Identities[0].ModelRuntimeEpoch != f.runtime.identity.ModelRuntimeEpoch {
					t.Fatal("discovery leaked mutable Runtime response")
				}
			} else if response != nil || runtime.calls != 0 {
				t.Fatalf("invalid discovery reached Runtime: calls=%d response=%v", runtime.calls, response)
			}
		})
	}
}

func TestMemberDiscoveryPreservesOpaqueRegistryEvidenceAtBothHops(t *testing.T) {
	f, runtime, _ := newDiscoveryFixture(t)
	// Signature validation belongs to the independently configured Worker caller.
	// Neither transport hop may synthesize, drop or retain mutable evidence.
	evidence := &velav1.WorkerBootstrapBinding{SchemaVersion: 1, SigningKeyId: "registry", Signature: []byte{1, 2, 3},
		Claim: &velav1.WorkerBootstrapClaim{WorkerMemberId: f.local.ID}}
	runtime.response.JournalBinding = proto.Clone(evidence).(*velav1.WorkerBootstrapBinding)
	client := &Client{targetID: f.local.ID, service: &discoveryReplyClient{result: runtime.response}}
	request := discoveryRequestFor(f.runtime.identity)
	response, err := f.server.DiscoverRuntimeIdentities(t.Context(), &velav1.StageWorkerMemberServiceDiscoverRuntimeIdentitiesRequest{
		TargetWorkerMemberId: f.local.ID, Command: request,
	})
	if err != nil || !proto.Equal(response.GetResult().GetJournalBinding(), evidence) {
		t.Fatalf("member server changed Registry evidence: %v %v", response, err)
	}
	response.Result.JournalBinding.Signature[0] = 9
	if !proto.Equal(runtime.response.JournalBinding, evidence) {
		t.Fatal("member server leaked mutable evidence")
	}
	forwarded, err := client.DiscoverRuntimeIdentities(t.Context(), request)
	if err != nil || !proto.Equal(forwarded.GetJournalBinding(), evidence) {
		t.Fatalf("member client changed Registry evidence: %v %v", forwarded, err)
	}
	forwarded.JournalBinding.Signature[0] = 9
	if !proto.Equal(runtime.response.JournalBinding, evidence) {
		t.Fatal("member client leaked mutable evidence")
	}
}

func TestMemberDiscoveryRejectsMalformedResultsAtBothHops(t *testing.T) {
	for _, hop := range []string{"runtime", "member"} {
		t.Run(hop, func(t *testing.T) {
			for _, fault := range []string{"nil", "empty", "unknown", "identity-unknown", "worker", "member", "epoch", "profile", "residency", "zero-uuid", "runtime-name", "digest", "zero-digest", "duplicate", "conflicting-route", "mixed-topology", "too-many", "too-large"} {
				t.Run(fault, func(t *testing.T) {
					f, runtime, _ := newDiscoveryFixture(t)
					request := discoveryRequestFor(f.runtime.identity)
					identity := runtime.response.Identities[0]
					switch fault {
					case "nil":
						runtime.response = nil
					case "empty":
						runtime.response.Identities = nil
					case "unknown":
						runtime.response.ProtoReflect().SetUnknown([]byte{0x78, 0x01})
					case "identity-unknown":
						identity.ProtoReflect().SetUnknown([]byte{0x78, 0x01})
					case "worker":
						identity.WorkerInstanceEpoch++
					case "member":
						identity.WorkerMemberEpoch++
					case "epoch":
						identity.ModelRuntimeEpoch = 0
					case "profile":
						identity.StageProfileRevisionId = "bad-profile"
					case "residency":
						identity.ModelResidencyId = "bad-residency"
					case "zero-uuid":
						identity.ModelResidencyId = uuid.Nil.String()
					case "runtime-name":
						identity.RuntimeIdentity = " "
					case "digest":
						identity.DeviceSetDigest = nil
					case "zero-digest":
						identity.MembershipDigest = make([]byte, sha256.Size)
					case "duplicate":
						runtime.response.Identities = append(runtime.response.Identities, proto.Clone(identity).(*velav1.ModelRuntimeIdentity))
					case "conflicting-route":
						other := proto.Clone(identity).(*velav1.ModelRuntimeIdentity)
						other.ModelRuntimeEpoch++
						runtime.response.Identities = append(runtime.response.Identities, other)
					case "mixed-topology":
						other := proto.Clone(identity).(*velav1.ModelRuntimeIdentity)
						other.ModelResidencyId = uuid.NewString()
						other.MembershipDigest[0] ^= 1
						runtime.response.Identities = append(runtime.response.Identities, other)
					case "too-many":
						for range 16 {
							other := proto.Clone(identity).(*velav1.ModelRuntimeIdentity)
							other.ModelResidencyId = uuid.NewString()
							runtime.response.Identities = append(runtime.response.Identities, other)
						}
					case "too-large":
						runtime.response.Detail = strings.Repeat("x", 64<<10)
					}
					var err error
					if hop == "runtime" {
						_, err = f.server.DiscoverRuntimeIdentities(t.Context(), &velav1.StageWorkerMemberServiceDiscoverRuntimeIdentitiesRequest{TargetWorkerMemberId: f.local.ID, Command: request})
					} else {
						client := &Client{targetID: f.local.ID, service: &discoveryReplyClient{result: runtime.response}}
						_, err = client.DiscoverRuntimeIdentities(t.Context(), request)
					}
					if status.Code(err) != codes.DataLoss {
						t.Fatalf("invalid %s result accepted: %v", hop, err)
					}
				})
			}
		})
	}
}

func TestMemberDiscoveryRejectsChangedPinnedRuntimeSet(t *testing.T) {
	for _, fault := range []string{"epoch", "profile", "device-set", "subset", "extra-route"} {
		t.Run(fault, func(t *testing.T) {
			f, runtime, _ := newDiscoveryFixture(t)
			identity := runtime.response.Identities[0]
			switch fault {
			case "epoch":
				identity.ModelRuntimeEpoch++
			case "profile":
				identity.StageProfileRevisionId = uuid.NewString()
			case "device-set":
				identity.DeviceSetDigest[0] ^= 1
			case "subset":
				second := proto.Clone(identity).(*velav1.ModelRuntimeIdentity)
				second.ModelResidencyId = uuid.NewString()
				f.server.localIdentities = append(f.server.localIdentities, second)
			case "extra-route":
				second := proto.Clone(identity).(*velav1.ModelRuntimeIdentity)
				second.ModelResidencyId = uuid.NewString()
				runtime.response.Identities = append(runtime.response.Identities, second)
			}
			response, err := f.server.DiscoverRuntimeIdentities(t.Context(), &velav1.StageWorkerMemberServiceDiscoverRuntimeIdentitiesRequest{TargetWorkerMemberId: f.local.ID, Command: discoveryRequestFor(f.runtime.identity)})
			if response != nil || err == nil || runtime.calls != 1 {
				t.Fatalf("changed configured Runtime set was advertised: %v %v calls=%d", response, err, runtime.calls)
			}
		})
	}
}

func TestMemberDiscoveryConfigurationAndCopyIsolation(t *testing.T) {
	f, runtime, config := newDiscoveryFixture(t)
	config.Members[0].IdentityDigest[0] ^= 1
	config.LocalIdentities[0].ModelRuntimeEpoch++
	request := discoveryRequestFor(f.runtime.identity)
	client := &Client{targetID: f.local.ID, service: &discoveryReplyClient{result: runtime.response}}
	response, err := client.DiscoverRuntimeIdentities(t.Context(), request)
	if err != nil || !proto.Equal(response, runtime.response) {
		t.Fatalf("valid client discovery: %v %v", response, err)
	}
	response.Identities[0].ModelRuntimeEpoch++
	if runtime.response.Identities[0].ModelRuntimeEpoch != f.runtime.identity.ModelRuntimeEpoch {
		t.Fatal("client leaked mutable member response")
	}
	if _, err := f.server.DiscoverRuntimeIdentities(t.Context(), &velav1.StageWorkerMemberServiceDiscoverRuntimeIdentitiesRequest{TargetWorkerMemberId: f.local.ID, Command: request}); err != nil {
		t.Fatalf("configuration mutation changed trusted server discovery: %v", err)
	}
	for _, fault := range []string{"partial", "short-digest", "zero-digest", "bad-local-identity"} {
		t.Run(fault, func(t *testing.T) {
			_, _, config := newDiscoveryFixture(t)
			switch fault {
			case "partial":
				config.Members[0].IdentityDigest = nil
			case "short-digest":
				config.Members[0].IdentityDigest = []byte{1}
			case "zero-digest":
				config.Members[0].IdentityDigest = make([]byte, sha256.Size)
			case "bad-local-identity":
				config.LocalIdentities[0].ModelResidencyId = "bad"
			}
			if server, err := NewServer(config); err == nil || server != nil {
				t.Fatalf("invalid discovery configuration accepted: %v %v", server, err)
			}
		})
	}
}

func TestMemberDiscoveryRejectsLateCancellationAndRequestMutation(t *testing.T) {
	for _, hop := range []string{"runtime", "member"} {
		t.Run(hop, func(t *testing.T) {
			f, runtime, _ := newDiscoveryFixture(t)
			request := discoveryRequestFor(f.runtime.identity)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			var err error
			if hop == "runtime" {
				runtime.call = func(_ context.Context, forwarded *velav1.ModelRuntimeServiceDiscoverRuntimeIdentitiesRequest) {
					forwarded.WorkerInstanceEpoch++
					cancel()
				}
				_, err = f.server.DiscoverRuntimeIdentities(ctx, &velav1.StageWorkerMemberServiceDiscoverRuntimeIdentitiesRequest{TargetWorkerMemberId: f.local.ID, Command: request})
			} else {
				service := &discoveryReplyClient{result: runtime.response, call: func(forwarded *velav1.StageWorkerMemberServiceDiscoverRuntimeIdentitiesRequest) {
					forwarded.Command.WorkerInstanceEpoch++
					cancel()
				}}
				client := &Client{targetID: f.local.ID, service: service}
				_, err = client.DiscoverRuntimeIdentities(ctx, request)
			}
			if status.Code(err) != codes.Canceled || request.WorkerInstanceEpoch != f.runtime.identity.WorkerInstanceEpoch {
				t.Fatalf("late result or request mutation escaped %s: request=%v error=%v", hop, request, err)
			}
		})
	}
}

func TestMemberDiscoveryReturnsCompleteMultipleProfileSet(t *testing.T) {
	f, runtime, config := newDiscoveryFixture(t)
	second := proto.Clone(f.runtime.identity).(*velav1.ModelRuntimeIdentity)
	second.ModelResidencyId, second.StageProfileRevisionId = uuid.NewString(), uuid.NewString()
	second.RuntimeIdentity = "second-profile-runtime"
	second.ModelRuntimeEpoch++
	config.LocalIdentities = append(config.LocalIdentities, second)
	var err error
	f.server, err = NewServer(config)
	if err != nil {
		t.Fatal(err)
	}
	runtime.response.Identities = append([]*velav1.ModelRuntimeIdentity{proto.Clone(second).(*velav1.ModelRuntimeIdentity)}, runtime.response.Identities...)
	response, err := f.server.DiscoverRuntimeIdentities(t.Context(), &velav1.StageWorkerMemberServiceDiscoverRuntimeIdentitiesRequest{
		TargetWorkerMemberId: f.local.ID, Command: discoveryRequestFor(f.runtime.identity),
	})
	if err != nil || !proto.Equal(response.GetResult(), runtime.response) || len(response.GetResult().GetIdentities()) != 2 {
		t.Fatalf("complete current profiles rejected: %v %v", response, err)
	}
}

func TestMemberDiscoveryClientRejectsInvalidScopeBeforeTransport(t *testing.T) {
	for _, fault := range []string{"nil", "target", "zero-worker", "zero-epoch", "unknown", "canceled", "nil-context"} {
		t.Run(fault, func(t *testing.T) {
			f, runtime, _ := newDiscoveryFixture(t)
			request := discoveryRequestFor(f.runtime.identity)
			service := &discoveryReplyClient{result: runtime.response, call: func(*velav1.StageWorkerMemberServiceDiscoverRuntimeIdentitiesRequest) {
				t.Fatal("invalid discovery scope reached member transport")
			}}
			client := &Client{targetID: f.local.ID, service: service}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			switch fault {
			case "nil":
				request = nil
			case "target":
				request.WorkerMemberId = f.leader.ID
			case "zero-worker":
				request.WorkerInstanceId = uuid.Nil.String()
			case "zero-epoch":
				request.WorkerMemberEpoch = 0
			case "unknown":
				request.ProtoReflect().SetUnknown([]byte{0x78, 0x01})
			case "canceled":
				cancel()
			case "nil-context":
				ctx = nil
			}
			if response, err := client.DiscoverRuntimeIdentities(ctx, request); response != nil || err == nil {
				t.Fatalf("invalid discovery accepted: %v %v", response, err)
			}
		})
	}
}

func newDiscoveryFixture(t *testing.T) (*serverFixture, *discoveryRuntimeClient, ServerConfig) {
	t.Helper()
	f := newServerFixture(t, time.Time{})
	leaderDigest, localDigest := sha256.Sum256([]byte(f.leaderSPIFFE)), sha256.Sum256([]byte(f.localSPIFFE))
	runtime := &discoveryRuntimeClient{response: &velav1.ModelRuntimeServiceDiscoverRuntimeIdentitiesResponse{Identities: []*velav1.ModelRuntimeIdentity{proto.Clone(f.runtime.identity).(*velav1.ModelRuntimeIdentity)}}}
	config := ServerConfig{
		Authenticator: f.auth, Validator: f.server.validator, Runtime: runtime,
		LocalIdentities: []*velav1.ModelRuntimeIdentity{proto.Clone(f.runtime.identity).(*velav1.ModelRuntimeIdentity)},
		Members: []MemberBinding{
			{ID: f.local.ID, Epoch: f.local.Epoch, IdentityDigest: localDigest[:]},
			{ID: f.leader.ID, Epoch: f.leader.Epoch, IdentityDigest: leaderDigest[:]},
		},
	}
	var err error
	f.server, err = NewServer(config)
	if err != nil {
		t.Fatal(err)
	}
	return f, runtime, config
}

type discoveryRuntimeClient struct {
	velav1.ModelRuntimeServiceClient
	response *velav1.ModelRuntimeServiceDiscoverRuntimeIdentitiesResponse
	call     func(context.Context, *velav1.ModelRuntimeServiceDiscoverRuntimeIdentitiesRequest)
	calls    int
}

func (client *discoveryRuntimeClient) DiscoverRuntimeIdentities(ctx context.Context, request *velav1.ModelRuntimeServiceDiscoverRuntimeIdentitiesRequest, _ ...grpc.CallOption) (*velav1.ModelRuntimeServiceDiscoverRuntimeIdentitiesResponse, error) {
	client.calls++
	if client.call != nil {
		client.call(ctx, request)
	}
	return client.response, nil
}

type discoveryReplyClient struct {
	velav1.StageWorkerMemberServiceClient
	result        *velav1.ModelRuntimeServiceDiscoverRuntimeIdentitiesResponse
	call          func(*velav1.StageWorkerMemberServiceDiscoverRuntimeIdentitiesRequest)
	workerBinding *velav1.WorkerBootstrapBinding
}

func (client *discoveryReplyClient) DiscoverRuntimeIdentities(_ context.Context, request *velav1.StageWorkerMemberServiceDiscoverRuntimeIdentitiesRequest, _ ...grpc.CallOption) (*velav1.StageWorkerMemberServiceDiscoverRuntimeIdentitiesResponse, error) {
	if client.call != nil {
		client.call(request)
	}
	return &velav1.StageWorkerMemberServiceDiscoverRuntimeIdentitiesResponse{Result: client.result, WorkerJournalBinding: client.workerBinding}, nil
}
