package modelruntime_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/vivym/vela/internal/modelruntime"
	"github.com/vivym/vela/internal/stageauthority"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/protobuf/proto"
)

type renewalJournalBackend struct {
	*renewalRecoveryBackend
	beforeStatus func()
}

func (backend *renewalJournalBackend) Status(ctx context.Context, authority stageauthority.Verified) (modelruntime.BackendStatus, error) {
	if backend.beforeStatus != nil {
		backend.beforeStatus()
	}
	return backend.renewalRecoveryBackend.Status(ctx, authority)
}

func TestRuntimeRenewalPersistsCandidatesBeforeBackendDispatch(t *testing.T) {
	for _, applied := range []bool{false, true} {
		t.Run(map[bool]string{false: "not-applied", true: "applied-response-lost"}[applied], func(t *testing.T) {
			directory := privateExecutionStateDirectory(t)
			backend := &renewalJournalBackend{renewalRecoveryBackend: &renewalRecoveryBackend{
				FakeRuntime: modelruntime.NewFakeDiTRuntime(), failStatus: true, applyRenewal: applied,
				calls: make(chan cancellationAuthorityCall, 2),
			}}
			f := newExecutionDrainFixture(t, directory, backend)
			first := f.authorities[0]
			prepareFloorRuntime(t, f.supervisor, first)
			f.clock.Advance(time.Second)
			latest := renewWatchdogAuthority(t, f.signer, first, f.clock.Now())
			backend.beforeStatus = func() { assertDurableRenewalCandidates(t, directory, first, latest, first) }
			if response, err := f.supervisor.Status(t.Context(), &velav1.ModelRuntimeServiceStatusRequest{Authority: latest}); err != nil || response.GetDecision() != velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_REJECTED {
				t.Fatalf("renewal fault: %v %v", response, err)
			}
			assertDurableRenewalCandidates(t, directory, first, latest, first)
			backend.failStatus = false
			if response, err := f.supervisor.Status(t.Context(), &velav1.ModelRuntimeServiceStatusRequest{Authority: latest}); err != nil || response.GetDecision() != velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_ACCEPTED {
				t.Fatalf("confirmed renewal: %v %v", response, err)
			}
			assertDurableRenewalCandidates(t, directory, first, latest, latest)
		})
	}
}

func assertDurableRenewalCandidates(t *testing.T, directory string, original, accepted, confirmed *velav1.StageAuthority) {
	t.Helper()
	state := readDurableExecutionState(t, directory)
	var records []retainedExecutionDocument
	if err := json.Unmarshal(state.Executions, &records); err != nil || len(records) != 1 || records[0].Candidates == nil {
		t.Fatalf("renewal entered backend without durable authority candidates: records=%d error=%v", len(records), err)
	}
	for index, wire := range [][]byte{records[0].Authority, records[0].Candidates.Accepted, records[0].Candidates.Confirmed} {
		authority := &velav1.StageAuthority{}
		if err := proto.Unmarshal(wire, authority); err != nil || !proto.Equal(authority, []*velav1.StageAuthority{original, accepted, confirmed}[index]) {
			t.Fatalf("durable candidate %d changed its signed identity: %v %v", index, authority, err)
		}
	}
}

func withoutRetainedCandidates(t *testing.T, wire json.RawMessage) json.RawMessage {
	t.Helper()
	var records []retainedExecutionDocument
	if err := json.Unmarshal(wire, &records); err != nil {
		t.Fatal(err)
	}
	for index := range records {
		records[index].Candidates = nil
		records[index].Seal = nil // Simulate history from before sealed receipts existed.
	}
	result, err := json.Marshal(records)
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func TestRuntimeRenewalCandidatesRecoverWithoutReattachingBackend(t *testing.T) {
	for _, confirmed := range []bool{false, true} {
		t.Run(map[bool]string{false: "uncertain", true: "confirmed"}[confirmed], func(t *testing.T) {
			directory := privateExecutionStateDirectory(t)
			backend := &renewalRecoveryBackend{FakeRuntime: modelruntime.NewFakeDiTRuntime(), failStatus: !confirmed,
				applyRenewal: true, calls: make(chan cancellationAuthorityCall, 2)}
			f := newExecutionDrainFixture(t, directory, backend)
			first := f.authorities[0]
			prepareFloorRuntime(t, f.supervisor, first)
			f.clock.Advance(time.Second)
			latest := renewWatchdogAuthority(t, f.signer, first, f.clock.Now())
			if response, err := f.supervisor.Status(t.Context(), &velav1.ModelRuntimeServiceStatusRequest{Authority: latest}); err != nil || (response.GetDecision() == velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_ACCEPTED) != confirmed {
				t.Fatalf("renewal outcome: %v %v", response, err)
			}
			f.supervisor.Close()
			recovered := durableExecutionFixture(t, directory, false, "", 10, f.clock.Now().Add(2*time.Minute))
			expected := first
			if confirmed {
				expected = latest
			}
			for range 2 {
				read, err := recovered.supervisor.InspectRetainedAllocationAuthorities(t.Context(), latest)
				if err != nil || read == nil || !proto.Equal(read.Original, first) || !proto.Equal(read.Accepted, latest) || !proto.Equal(read.Confirmed, expected) {
					t.Fatalf("restart lost exact historical candidates: %+v %v", read, err)
				}
				read.Original.Signature[0] ^= 1
				read.Accepted.Signature[0] ^= 1
				read.Confirmed.Signature[0] ^= 1
			}
			assertRecoveryDrainBlocks(t, recovered, recovered.authority(t, 1, 12))
			if checkpoint, err := recovered.supervisor.DrainExecution(t.Context(), latest); err == nil || checkpoint != nil {
				t.Fatalf("retained candidates granted backend entry across epochs: %+v %v", checkpoint, err)
			}
			if recovered.backend.calls.Load() != 0 {
				t.Fatal("historical recovery entered the replacement backend")
			}
		})
	}
}

func TestRuntimeRenewalJournalFailureRetainsUncertainAuthority(t *testing.T) {
	for _, failure := range []string{"dispatch-sync", "confirmation-sync", "dispatch-expiry", "dispatch-cancellation", "confirmation-cancellation"} {
		t.Run(failure, func(t *testing.T) {
			directory := privateExecutionStateDirectory(t)
			backend := &renewalRecoveryBackend{FakeRuntime: modelruntime.NewFakeDiTRuntime(), calls: make(chan cancellationAuthorityCall, 2)}
			f := newExecutionDrainFixture(t, directory, backend)
			first := f.authorities[0]
			prepareFloorRuntime(t, f.supervisor, first)
			f.clock.Advance(time.Second)
			latest := renewWatchdogAuthority(t, f.signer, first, f.clock.Now())
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			writes := 0
			restore := modelruntime.SetExecutionStateSyncHookForTest(f.supervisor, func(syncDirectory func() error) error {
				writes++
				switch failure {
				case "dispatch-sync":
					return errors.New("injected sync failure before renewal dispatch")
				case "confirmation-sync":
					if writes == 2 {
						return errors.New("injected sync failure after backend confirmation")
					}
				case "dispatch-expiry":
					f.clock.Advance(time.Minute)
				case "dispatch-cancellation":
					cancel()
				case "confirmation-cancellation":
					if writes == 2 {
						cancel()
					}
				}
				return syncDirectory()
			})
			response, err := f.supervisor.Status(ctx, &velav1.ModelRuntimeServiceStatusRequest{Authority: latest})
			restore()
			if err != nil || response.GetDecision() == velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_ACCEPTED {
				t.Fatalf("uncertain or expired journal write acknowledged execution: %v %v", response, err)
			}
			expected, calls := first, 0
			if failure == "confirmation-sync" || failure == "confirmation-cancellation" {
				expected, calls = latest, 1
			}
			if backend.statusCalls != calls {
				t.Fatalf("renewal reached backend across failed persistence: calls=%d", backend.statusCalls)
			}
			assertDurableRenewalCandidates(t, directory, first, latest, expected)
			f.supervisor.Close()
			recovered := durableExecutionFixture(t, directory, false, "", 10, f.clock.Now())
			read, err := recovered.supervisor.InspectRetainedAllocationAuthorities(t.Context(), latest)
			if err != nil || read == nil || !proto.Equal(read.Accepted, latest) || !proto.Equal(read.Confirmed, expected) {
				t.Fatalf("restart discarded visible candidates after sync failure: %+v %v", read, err)
			}
			assertRecoveryDrainBlocks(t, recovered, recovered.authority(t, 1, 12))
		})
	}
}

func TestRuntimeRenewalSchema4UpgradePreservesUnknownCandidates(t *testing.T) {
	for _, drained := range []bool{false, true} {
		t.Run(map[bool]string{false: "pending", true: "drained"}[drained], func(t *testing.T) {
			directory := privateExecutionStateDirectory(t)
			f := durableExecutionFixture(t, directory, true, "", 9, time.Time{})
			if drained {
				readyDrainOutput(t, f, f.backend.FakeRuntime, f.authorities[0])
				if response, err := f.supervisor.SealOutput(t.Context(), &velav1.ModelRuntimeServiceSealOutputRequest{Authority: f.authorities[0]}); err != nil || response.GetReceipt() == nil {
					t.Fatalf("seal: %v %v", response, err)
				}
			} else {
				prepareFloorRuntime(t, f.supervisor, f.authorities[0])
			}
			f.supervisor.Close()
			legacy := readDurableExecutionState(t, directory)
			legacy.SchemaVersion = 4
			legacy.Executions = withoutRetainedCandidates(t, legacy.Executions)
			if err := os.WriteFile(filepath.Join(directory, durableStateFileName), encodeDurableExecutionState(t, legacy), 0o600); err != nil {
				t.Fatal(err)
			}
			if _, err := tryDurableExecutionFixture(t, directory, false); err == nil {
				t.Fatal("ordinary recovery implicitly upgraded schema 4")
			}
			upgraded, err := executionFloorFixtureWithState(t, "", &modelruntime.ExecutionFloorStateConfig{Directory: directory, UpgradeV4: true}, 10, f.clock.Now())
			if err != nil {
				t.Fatal(err)
			}
			read, err := upgraded.supervisor.InspectRetainedAllocationAuthorities(t.Context(), f.authorities[0])
			if err != nil || read == nil || !proto.Equal(read.Original, f.authorities[0]) || read.Accepted != nil || read.Confirmed != nil {
				t.Fatalf("upgrade inferred lost renewal history: %+v %v", read, err)
			}
			assertExecutionDrainCheckpoint(t, upgraded.supervisor, f.authorities[0], drained)
			if !drained {
				assertRecoveryDrainBlocks(t, upgraded, upgraded.authority(t, 1, 12))
			}
		})
	}
}

func TestRuntimeRenewalRecoveryRejectsInvalidCandidates(t *testing.T) {
	for _, fault := range []string{"accepted-signature", "confirmed-signature", "accepted-empty", "accepted-unrelated", "confirmed-unrelated", "confirmed-future", "accepted-regressed", "unconfirmed-renewal", "legacy-version", "drain-outside-candidates"} {
		t.Run(fault, func(t *testing.T) {
			directory := privateExecutionStateDirectory(t)
			f := durableExecutionFixture(t, directory, true, "", 9, time.Time{})
			first := f.authorities[0]
			prepareFloorRuntime(t, f.supervisor, first)
			f.clock.Advance(time.Second)
			latest := renewWatchdogAuthority(t, f.signer, first, f.clock.Now())
			if response, err := f.supervisor.Status(t.Context(), &velav1.ModelRuntimeServiceStatusRequest{Authority: latest}); err != nil || response.GetDecision() != velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_ACCEPTED {
				t.Fatalf("renewal: %v %v", response, err)
			}
			if fault == "drain-outside-candidates" {
				if response, err := f.supervisor.StartStage(t.Context(), &velav1.ModelRuntimeServiceStartStageRequest{Authority: latest}); err != nil || response.GetDecision() != velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_ACCEPTED {
					t.Fatalf("start: %v %v", response, err)
				}
				f.backend.MarkOutputReady([]byte(`{"kind":"LATENT","path":"/local/dit.bin"}`))
				if response, err := f.supervisor.SealOutput(t.Context(), &velav1.ModelRuntimeServiceSealOutputRequest{Authority: latest}); err != nil || response.GetReceipt() == nil {
					t.Fatalf("seal: %v %v", response, err)
				}
			}
			f.supervisor.Close()
			state := readDurableExecutionState(t, directory)
			var records []retainedExecutionDocument
			if err := json.Unmarshal(state.Executions, &records); err != nil {
				t.Fatal(err)
			}
			encode := func(authority *velav1.StageAuthority) []byte {
				t.Helper()
				wire, err := proto.MarshalOptions{Deterministic: true}.Marshal(authority)
				if err != nil {
					t.Fatal(err)
				}
				return wire
			}
			switch fault {
			case "accepted-signature", "confirmed-signature":
				bad := proto.Clone(latest).(*velav1.StageAuthority)
				bad.Signature[0] ^= 1
				if fault == "accepted-signature" {
					records[0].Candidates.Accepted = encode(bad)
				} else {
					records[0].Candidates.Confirmed = encode(bad)
				}
			case "accepted-empty":
				records[0].Candidates.Accepted = nil
			case "accepted-unrelated":
				records[0].Candidates.Accepted = encode(f.authority(t, 1, 12))
			case "confirmed-unrelated":
				records[0].Candidates.Confirmed = encode(f.authority(t, 1, 12))
			case "confirmed-future":
				records[0].Candidates.Confirmed = encode(renewWatchdogAuthority(t, f.signer, latest, f.clock.Now().Add(time.Second)))
			case "accepted-regressed":
				records[0].Candidates.Accepted = encode(first)
			case "unconfirmed-renewal":
				records[0].Candidates.Confirmed = nil
			case "legacy-version":
				state.SchemaVersion = 4
			case "drain-outside-candidates":
				records[0].Candidates.Accepted = encode(first)
				records[0].Candidates.Confirmed = encode(first)
			}
			var err error
			state.Executions, err = json.Marshal(records)
			if err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(directory, durableStateFileName)
			before := encodeDurableExecutionState(t, state)
			if err := os.WriteFile(path, before, 0o600); err != nil {
				t.Fatal(err)
			}
			if _, err := executionFloorFixtureWithState(t, "", &modelruntime.ExecutionFloorStateConfig{Directory: directory, UpgradeV4: fault == "legacy-version"}, 10, f.clock.Now()); err == nil {
				t.Fatal("damaged candidate history reopened")
			}
			after, err := os.ReadFile(path)
			if err != nil || !bytes.Equal(before, after) {
				t.Fatalf("rejected recovery rewrote evidence: %v", err)
			}
		})
	}
}
