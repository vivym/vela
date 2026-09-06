package stageworkeragent_test

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/stageauthority"
	"github.com/vivym/vela/internal/stageworkeragent"
)

func TestAssignmentAdmissionRecoveryRejectsChangedTopologyWithoutFloor(t *testing.T) {
	for _, history := range []string{"empty", "pending"} {
		for _, change := range []string{"device-id", "device-epoch", "device-set", "member-epoch", "membership", "member-identity", "member-subset"} {
			t.Run(history+"/"+change, func(t *testing.T) {
				fixture := newAdmissionFixture(t)
				gate := fixture.open(t)
				if history == "pending" {
					beginAdmission(t, gate, fixture.assignment, fixture.acquireID).Release()
				}
				if err := gate.Close(); err != nil {
					t.Fatal(err)
				}
				path := filepath.Join(fixture.config.Directory, admissionTestState)
				before, err := os.ReadFile(path)
				if err != nil {
					t.Fatal(err)
				}
				for i := range fixture.config.Bindings {
					binding := &fixture.config.Bindings[i]
					switch change {
					case "device-id":
						binding.Runtime.Devices[0].ID = "32000000-0000-0000-0000-000000000003"
					case "device-epoch":
						binding.Runtime.Devices[0].Epoch++
					case "device-set":
						binding.Runtime.DeviceSetDigest[0] ^= 1
					case "member-epoch":
						binding.Runtime.WorkerMemberEpoch++
						for j := range binding.Runtime.Members {
							binding.Runtime.Members[j].Epoch++
						}
					case "membership":
						binding.Runtime.MembershipDigest[0] ^= 1
					case "member-identity":
						binding.IdentityDigest[0] ^= 1
					case "member-subset":
						binding.DeviceSubsetDigest[0] ^= 1
					}
				}
				if reopened, err := stageworkeragent.NewFileAssignmentAdmission(fixture.config); err == nil {
					_ = reopened.Close()
					t.Fatal("changed topology recovered under unchanged Worker epoch")
				}
				after, err := os.ReadFile(path)
				if err != nil || !bytes.Equal(before, after) {
					t.Fatalf("rejected recovery changed journal: %v", err)
				}
			})
		}
	}
}

func TestAssignmentAdmissionScopeAllowsReorderingAndRuntimeReplacement(t *testing.T) {
	fixture := newAdmissionFixture(t)
	gate := fixture.open(t)
	beginAdmission(t, gate, fixture.assignment, fixture.acquireID).Release()
	if err := gate.Close(); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(fixture.config.Directory, admissionTestState)
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	slices.Reverse(fixture.config.Bindings)
	for i := range fixture.config.Bindings {
		binding := &fixture.config.Bindings[i].Runtime
		slices.Reverse(binding.Devices)
		slices.Reverse(binding.Members)
		binding.ModelRuntimeEpoch++
		binding.ModelRuntimeIdentity = "replacement-runtime"
		binding.ModelResidencyID = uuid.NewString()
		binding.StageProfileRevisionID = uuid.NewString()
	}
	gate = fixture.open(t)
	if snapshot := admissionSnapshot(t, gate); snapshot.Latest == nil || snapshot.Latest.InputDrain != nil || snapshot.Watermark != 1 {
		t.Fatalf("recovery changed pending history: %+v", snapshot)
	}
	if handle, err := gate.Begin(t.Context(), fixture.assignment, fixture.acquireID); err == nil {
		handle.Release()
		t.Fatal("recovery granted old Runtime execution")
	}
	if after, err := os.ReadFile(path); err != nil || !bytes.Equal(before, after) {
		t.Fatalf("Runtime replacement rewrote journal: %v", err)
	}
}

func TestAssignmentAdmissionScopeRejectsInvalidConfigurationBeforeWriting(t *testing.T) {
	for _, fault := range []string{"missing-binding", "local-member", "duplicate-device", "duplicate-member", "device-id", "device-epoch", "device-set", "membership", "member-epoch", "identity", "subset", "inconsistent-devices", "inconsistent-members", "conflicting-route"} {
		t.Run(fault, func(t *testing.T) {
			fixture := newAdmissionFixture(t)
			binding := &fixture.config.Bindings[0]
			switch fault {
			case "missing-binding":
				fixture.config.Bindings = fixture.config.Bindings[:1]
			case "local-member":
				fixture.config.WorkerMemberID = uuid.New()
			case "duplicate-device":
				binding.Runtime.Devices[1] = binding.Runtime.Devices[0]
			case "duplicate-member":
				binding.Runtime.Members[1] = binding.Runtime.Members[0]
			case "device-id":
				binding.Runtime.Devices[0].ID = "invalid"
			case "device-epoch":
				binding.Runtime.Devices[0].Epoch = 0
			case "device-set":
				binding.Runtime.DeviceSetDigest = make([]byte, sha256.Size)
			case "membership":
				binding.Runtime.MembershipDigest = binding.Runtime.MembershipDigest[:31]
			case "member-epoch":
				binding.Runtime.WorkerMemberEpoch++
			case "identity":
				binding.IdentityDigest = [sha256.Size]byte{}
			case "subset":
				binding.DeviceSubsetDigest = [sha256.Size]byte{}
			case "inconsistent-devices":
				binding.Runtime.Devices[0].Epoch++
			case "inconsistent-members":
				binding.Runtime.Members[1].Epoch++
			case "conflicting-route":
				other := *binding
				other.Runtime.ModelRuntimeEpoch++
				other.IdentityDigest[0] ^= 1
				fixture.config.Bindings = append(fixture.config.Bindings, other)
			}
			if gate, err := stageworkeragent.NewFileAssignmentAdmission(fixture.config); err == nil {
				_ = gate.Close()
				t.Fatal("invalid topology was initialized")
			}
			for _, root := range []string{fixture.config.Directory, fixture.config.InputRoot, fixture.config.OutputRoot} {
				if entries, err := os.ReadDir(root); err != nil || len(entries) != 0 {
					t.Fatalf("rejection wrote journal or ownership marker: %v %v", entries, err)
				}
			}
		})
	}
}

func TestAssignmentAdmissionLegacyScopeUpgradeRequiresSignedFloor(t *testing.T) {
	for _, schema := range []int{2, 3, 4} {
		for _, history := range []string{"empty", "pending", "floor", "changed-subset"} {
			t.Run(fmt.Sprintf("v%d/%s", schema, history), func(t *testing.T) {
				fixture := newAssignmentFloorFixture(t)
				gate := fixture.open(t)
				if history != "empty" {
					beginAdmission(t, gate, fixture.assignment, fixture.acquireID).Release()
				}
				if history == "floor" || history == "changed-subset" {
					if _, err := gate.InstallExecutionFloor(t.Context(), fixture.disposition); err != nil {
						t.Fatal(err)
					}
				}
				if err := gate.Close(); err != nil {
					t.Fatal(err)
				}
				config := fixture.admissionFixture.config
				path := filepath.Join(config.Directory, admissionTestState)
				original, err := os.ReadFile(path)
				if err != nil {
					t.Fatal(err)
				}
				legacy := legacyAdmissionDocument(t, original, schema)
				if err := os.WriteFile(path, legacy, 0o600); err != nil {
					t.Fatal(err)
				}
				if reopened, err := stageworkeragent.NewFileAssignmentAdmission(config); err == nil {
					_ = reopened.Close()
					t.Fatal("legacy topology migrated implicitly")
				}
				config.UpgradeV2, config.UpgradeV3, config.UpgradeV4 = schema == 2, schema == 3, schema == 4
				if history == "changed-subset" {
					config.Bindings[0].DeviceSubsetDigest[0] ^= 1
				}
				upgraded, err := stageworkeragent.NewFileAssignmentAdmission(config)
				want := legacy
				if history == "floor" {
					if err != nil {
						t.Fatal(err)
					}
					if err := upgraded.Close(); err != nil {
						t.Fatal(err)
					}
					want = original
				} else if err == nil {
					_ = upgraded.Close()
					t.Fatal("upgrade inferred original topology from current configuration")
				}
				if after, err := os.ReadFile(path); err != nil || !bytes.Equal(want, after) {
					t.Fatalf("upgrade failed to preserve evidence: %v", err)
				}
			})
		}
	}
}

func legacyAdmissionDocument(t *testing.T, document []byte, schema int) []byte {
	t.Helper()
	legacy, err := stageworkeragent.LegacyAssignmentAdmissionStateForTest(document, schema)
	if err != nil {
		t.Fatal(err)
	}
	return legacy
}

func TestAssignmentAdmissionScopeRevalidatesSignedHistory(t *testing.T) {
	fixture := newAdmissionFixture(t)
	gate := fixture.open(t)
	beginAdmission(t, gate, fixture.assignment, fixture.acquireID).Release()
	if err := gate.Close(); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(fixture.config.Directory, admissionTestState)
	document, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	other := newAdmissionFixture(t)
	for i := range fixture.config.Bindings {
		fixture.config.Bindings[i].IdentityDigest[0] ^= 1
		other.config.Bindings[i].IdentityDigest[0] ^= 1
	}
	otherGate := other.open(t)
	if err := otherGate.Close(); err != nil {
		t.Fatal(err)
	}
	source, err := os.ReadFile(filepath.Join(other.config.Directory, admissionTestState))
	if err != nil {
		t.Fatal(err)
	}
	changed, err := stageworkeragent.CopyAssignmentAdmissionScopeForTest(document, source)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, changed, 0o600); err != nil {
		t.Fatal(err)
	}
	if reopened, err := stageworkeragent.NewFileAssignmentAdmission(fixture.config); !errors.Is(err, stageauthority.ErrRuntimeMismatch) {
		if reopened != nil {
			_ = reopened.Close()
		}
		t.Fatalf("recovery failed to cross-check signed historical topology: %v", err)
	}
	if after, err := os.ReadFile(path); err != nil || !bytes.Equal(after, changed) {
		t.Fatalf("rejected history was rewritten: %v", err)
	}
}

func TestAssignmentAdmissionScopeUpgradePreservesRetirement(t *testing.T) {
	fixture := newAssignmentFloorFixture(t)
	gate := fixture.open(t)
	startFloorCollectorRuntimes(t, fixture, t.TempDir(), true, false)
	if result, err := terminalRetirer(t, gate, fixture).Retire(t.Context(), fixture.disposition, nil, drainCollectorTargets(fixture)); err != nil || result.Phase != stageworkeragent.TerminalRetirementRetired {
		t.Fatalf("retire: %+v %v", result, err)
	}
	if err := gate.Close(); err != nil {
		t.Fatal(err)
	}
	config := fixture.admissionFixture.config
	path := filepath.Join(config.Directory, admissionTestState)
	original, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, legacyAdmissionDocument(t, original, 4), 0o600); err != nil {
		t.Fatal(err)
	}
	config.UpgradeV4 = true
	gate, err = stageworkeragent.NewFileAssignmentAdmission(config)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = gate.Close() }()
	if snapshot := admissionSnapshot(t, gate); len(snapshot.Retirements) != 1 || snapshot.Retirements[0].Phase != stageworkeragent.TerminalRetirementRetired {
		t.Fatalf("upgrade changed retirement: %+v", snapshot)
	}
	if after, err := os.ReadFile(path); err != nil || !bytes.Equal(original, after) {
		t.Fatalf("upgrade changed retained proof: %v", err)
	}
}
