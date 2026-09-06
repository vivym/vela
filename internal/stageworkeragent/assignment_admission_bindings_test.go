package stageworkeragent_test

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/journalbinding"
	"github.com/vivym/vela/internal/stageworkeragent"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func admissionRegistryBinding(t *testing.T, config stageworkeragent.AssignmentAdmissionConfig, journal stageworkeragent.AssignmentJournalStatus, mutate func(*velav1.WorkerBootstrapBinding)) (*velav1.WorkerBootstrapBinding, *journalbinding.Verifier) {
	t.Helper()
	seed := bytes.Repeat([]byte{31}, ed25519.SeedSize)
	signer, err := journalbinding.NewSigner("registry", seed)
	if err != nil {
		t.Fatal(err)
	}
	verifier, err := journalbinding.NewVerifier(map[string][]byte{"registry": ed25519.NewKeyFromSeed(seed).Public().(ed25519.PublicKey)})
	if err != nil {
		t.Fatal(err)
	}
	requestID := uuid.NewString()
	value := &velav1.WorkerBootstrapBinding{SchemaVersion: journalbinding.SchemaVersion,
		Claim: &velav1.WorkerBootstrapClaim{RequestId: requestID, WorkerInstanceId: config.WorkerInstanceID.String(), WorkerInstanceEpoch: config.WorkerInstanceEpoch,
			WorkerMemberId: config.WorkerMemberID.String(), WorkerMemberEpoch: config.Bindings[0].Runtime.WorkerMemberEpoch,
			NodeIdentity: "cpu-node", ActorIdentity: "node-agent/cpu-node", BundleDigest: bytes.Repeat([]byte{1}, 32), ClaimedAt: timestamppb.Now()},
		Pair: &velav1.WorkerBootstrapJournalPair{RequestId: requestID, ActorIdentity: "node-agent/cpu-node", WorkerJournalId: journal.JournalID.String(), WorkerScope: journal.Scope[:],
			RuntimeJournalId: uuid.NewString(), RuntimeScope: bytes.Repeat([]byte{2}, 32), RecordedAt: timestamppb.Now()}}
	if mutate != nil {
		mutate(value)
	}
	signed, err := signer.Sign(value)
	if err != nil {
		t.Fatal(err)
	}
	return signed, verifier
}

func TestAssignmentAdmissionRejectsDifferentRegistryIdentity(t *testing.T) {
	for _, fault := range []string{"journal", "scope", "worker", "worker-epoch", "member", "member-epoch", "signature", "verifier", "binding", "initialize", "upgrade"} {
		t.Run(fault, func(t *testing.T) {
			fixture := newAdmissionFixture(t)
			journal, err := stageworkeragent.PrepareAssignmentJournal(t.Context(), fixture.config)
			if err != nil {
				t.Fatal(err)
			}
			fixture.config.Initialize = false
			configuration := fixture.config
			configuration.RegistryBinding, configuration.RegistryVerifier = admissionRegistryBinding(t, fixture.config, journal, func(v *velav1.WorkerBootstrapBinding) {
				switch fault {
				case "journal":
					v.Pair.WorkerJournalId = uuid.NewString()
				case "scope":
					v.Pair.WorkerScope[0] ^= 1
				case "worker":
					v.Claim.WorkerInstanceId = uuid.NewString()
				case "worker-epoch":
					v.Claim.WorkerInstanceEpoch++
				case "member":
					v.Claim.WorkerMemberId = uuid.NewString()
				case "member-epoch":
					v.Claim.WorkerMemberEpoch++
				}
			})
			switch fault {
			case "signature":
				configuration.RegistryBinding.Signature[0] ^= 1
			case "verifier":
				configuration.RegistryVerifier = nil
			case "binding":
				configuration.RegistryBinding = nil
			case "initialize":
				configuration.Initialize = true
			case "upgrade":
				configuration.UpgradeV4 = true
			}
			gate, err := stageworkeragent.NewFileAssignmentAdmission(configuration)
			if gate != nil {
				_ = gate.Close()
			}
			if err == nil || gate != nil {
				t.Fatal("invalid Registry binding exposed admission")
			}
			if recovered, err := stageworkeragent.PrepareAssignmentJournal(t.Context(), fixture.config); err != nil || recovered != journal {
				t.Fatalf("binding failure changed history or retained journal ownership: %+v %v", recovered, err)
			}
		})
	}
}

func TestDeferredAssignmentRoutesRetainJournalAndBindOnce(t *testing.T) {
	fixture := newAdmissionFixture(t)
	journal, err := stageworkeragent.PrepareAssignmentJournal(t.Context(), fixture.config)
	if err != nil {
		t.Fatal(err)
	}
	fixture.config.Initialize = false
	fixture.config.RegistryBinding, fixture.config.RegistryVerifier = admissionRegistryBinding(t, fixture.config, journal, nil)
	fixture.config.DeferRuntimeRoutes = true
	gate := fixture.open(t)
	before := admissionSnapshot(t, gate)
	if handle, err := gate.Begin(t.Context(), fixture.assignment, fixture.acquireID); !errors.Is(err, stageworkeragent.ErrAdmissionClosed) || handle != nil {
		t.Fatal("deferred discovery allowed execution admission")
	}
	if err := gate.ObserveRuntimeAuthority(t.Context(), fixture.assignment.Authority); !errors.Is(err, stageworkeragent.ErrAdmissionClosed) {
		t.Fatalf("deferred discovery observed current execution: %v", err)
	}
	if _, err := stageworkeragent.PrepareAssignmentJournal(t.Context(), fixture.config); err == nil {
		t.Fatal("deferred routes released journal lock")
	}
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	if err := gate.BindRuntimeRoutes(canceled, fixture.config.Bindings); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled discovery bound routes: %v", err)
	}
	if err := gate.BindRuntimeRoutes(t.Context(), fixture.config.Bindings); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(before, admissionSnapshot(t, gate)) {
		t.Fatal("discovery changed historical execution state")
	}
	if err := gate.BindRuntimeRoutes(t.Context(), fixture.config.Bindings); err == nil {
		t.Fatal("serving route binding was replaceable")
	}
	fixture.config.Bindings[0].Runtime.ModelRuntimeEpoch++
	fixture.config.Bindings[0].Runtime.Devices[0].Epoch++
	fixture.config.Bindings[0].Runtime.DeviceSetDigest[0] ^= 1
	handle := beginAdmission(t, gate, fixture.assignment, fixture.acquireID)
	handle.Release()
}

func TestDeferredAssignmentRouteBindingRejectsTopologyDrift(t *testing.T) {
	for _, fault := range []string{"empty", "worker", "worker-epoch", "member-epoch", "missing-member", "device", "device-digest", "identity", "subset", "incomplete-route", "topology-only", "replaced-state", "closed"} {
		t.Run(fault, func(t *testing.T) {
			fixture := newAdmissionFixture(t)
			fixture.config.DeferRuntimeRoutes = true
			gate := fixture.open(t)
			bindings := slices.Clone(fixture.config.Bindings)
			for index := range bindings {
				bindings[index].Runtime.Devices = slices.Clone(bindings[index].Runtime.Devices)
				bindings[index].Runtime.DeviceSetDigest = bytes.Clone(bindings[index].Runtime.DeviceSetDigest)
			}
			switch fault {
			case "empty":
				bindings = nil
			case "worker":
				bindings[0].Runtime.WorkerInstanceID = uuid.NewString()
			case "worker-epoch":
				bindings[0].Runtime.WorkerInstanceEpoch++
			case "member-epoch":
				bindings[0].Runtime.WorkerMemberEpoch++
			case "missing-member":
				bindings = bindings[:1]
			case "device":
				bindings[0].Runtime.Devices[0].Epoch++
			case "device-digest":
				bindings[0].Runtime.DeviceSetDigest[0] ^= 1
			case "identity":
				bindings[0].IdentityDigest[0] ^= 1
			case "subset":
				bindings[0].DeviceSubsetDigest[0] ^= 1
			case "incomplete-route":
				bindings[0].Runtime.ModelRuntimeEpoch = 0
			case "topology-only":
				bindings[0].Runtime.ModelRuntimeEpoch = 0
				bindings[0].Runtime.ModelRuntimeIdentity, bindings[0].Runtime.ModelResidencyID, bindings[0].Runtime.StageProfileRevisionID = "", "", ""
			case "replaced-state":
				path := filepath.Join(fixture.config.Directory, admissionTestState)
				wire, err := os.ReadFile(path)
				if err != nil {
					t.Fatal(err)
				}
				if err := os.Rename(path, path+".retained"); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(path, wire, 0o600); err != nil {
					t.Fatal(err)
				}
			case "closed":
				if err := gate.Close(); err != nil {
					t.Fatal(err)
				}
			}
			if err := gate.BindRuntimeRoutes(t.Context(), bindings); err == nil {
				t.Fatal("invalid discovery or journal ownership completed binding")
			}
			if handle, err := gate.Begin(t.Context(), fixture.assignment, fixture.acquireID); err == nil || handle != nil {
				t.Fatal("failed discovery opened admission")
			}
			if fault != "replaced-state" && fault != "closed" {
				if err := gate.BindRuntimeRoutes(t.Context(), fixture.config.Bindings); err != nil {
					t.Fatalf("invalid candidate prevented valid initial binding: %v", err)
				}
				beginAdmission(t, gate, fixture.assignment, fixture.acquireID).Release()
			}
		})
	}
}

func TestDeferredAssignmentRouteBindingIsExclusive(t *testing.T) {
	fixture := newAdmissionFixture(t)
	fixture.config.DeferRuntimeRoutes = true
	gate := fixture.open(t)
	var bound atomic.Int32
	var callers sync.WaitGroup
	for range 8 {
		callers.Go(func() {
			if gate.BindRuntimeRoutes(t.Context(), fixture.config.Bindings) == nil {
				bound.Add(1)
			}
		})
	}
	callers.Wait()
	if bound.Load() != 1 {
		t.Fatalf("discovery completed %d times", bound.Load())
	}
	beginAdmission(t, gate, fixture.assignment, fixture.acquireID).Release()
}
