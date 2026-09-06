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

func TestExecutionNonAdmissionRequiresDurableFloorAndNeverClaimsPersistedIntent(t *testing.T) {
	directory := privateExecutionStateDirectory(t)
	f := durableExecutionFixture(t, directory, true, "", 9, time.Time{})
	first, unseen := f.authorities[0], f.authorities[1]
	if proof, err := f.supervisor.CheckpointNonAdmission(t.Context(), unseen); proof != nil || !errors.Is(err, modelruntime.ErrExecutionNonAdmissionUnproven) {
		t.Fatalf("absence without floor: %+v %v", proof, err)
	}
	prepareFloorRuntime(t, f.supervisor, first)
	if _, err := f.supervisor.InstallExecutionFloor(t.Context(), f.disposition(t)); err != nil {
		t.Fatal(err)
	}
	if proof, err := f.supervisor.CheckpointNonAdmission(t.Context(), first); proof != nil || !errors.Is(err, modelruntime.ErrExecutionNonAdmissionUnproven) {
		t.Fatalf("persisted intent claimed never admitted: %+v %v", proof, err)
	}
	before := f.backend.calls.Load()
	proof, err := f.supervisor.CheckpointNonAdmission(t.Context(), unseen)
	if err != nil || proof == nil || proof.InstalledCutoff != 11 || proof.ExecutionSequence != 11 || proof.Contract != modelruntime.ExecutionNonAdmissionContract ||
		!proto.Equal(proof.Authority, unseen) || f.backend.calls.Load() != before || f.backend.closed.Load() {
		t.Fatalf("current unseen allocation checkpoint: %+v %v", proof, err)
	}
	state := readDurableExecutionState(t, directory)
	if state.SchemaVersion != 5 || state.Highest != 10 || len(state.NonAdmissions) == 0 {
		t.Fatalf("proof returned before persistence or changed admission watermark: %+v", state)
	}
	assertFloorCommandsRejected(t, f.supervisor, unseen)
	if drained, err := f.supervisor.InspectAllocationDrain(t.Context(), unseen); err != nil || drained != nil {
		t.Fatalf("non-admission became backend drain: %+v %v", drained, err)
	}
	again, err := f.supervisor.CheckpointNonAdmission(t.Context(), unseen)
	if err != nil || !sameNonAdmission(proof, again) {
		t.Fatalf("non-admission retry replaced proof: %+v %v", again, err)
	}
	f.clock.Advance(time.Second)
	renewal := renewWatchdogAuthority(t, f.signer, unseen, f.clock.Now())
	again, err = f.supervisor.InspectNonAdmission(t.Context(), renewal)
	if err != nil || !sameNonAdmission(proof, again) {
		t.Fatalf("renewal did not retain actual proof authority: %+v %v", again, err)
	}
	f.supervisor.Close()
	recovered := durableExecutionFixture(t, directory, false, "", 10, f.clock.Now().Add(2*time.Minute))
	again, err = recovered.supervisor.InspectNonAdmission(t.Context(), unseen)
	if err != nil || !sameNonAdmission(proof, again) {
		t.Fatalf("proof recovery: %+v %v", again, err)
	}
	assertRecoveryDrainBlocks(t, recovered, recovered.authority(t, 1, 12))
}

func TestExecutionNonAdmissionDoesNotInferHistoricalCoverageAfterRestart(t *testing.T) {
	directory := privateExecutionStateDirectory(t)
	f := durableExecutionFixture(t, directory, true, "", 9, time.Time{})
	if _, err := f.supervisor.InstallExecutionFloor(t.Context(), f.disposition(t)); err != nil {
		t.Fatal(err)
	}
	original := f.authorities[1]
	f.supervisor.Close()
	recovered := durableExecutionFixture(t, directory, false, "", 10, f.clock.Now())
	if proof, err := recovered.supervisor.InspectNonAdmission(t.Context(), original); err != nil || proof != nil {
		t.Fatalf("missing historical proof: %+v %v", proof, err)
	}
	if proof, err := recovered.supervisor.CheckpointNonAdmission(t.Context(), original); err == nil || proof != nil {
		t.Fatalf("old Runtime epoch inferred never admitted: %+v %v", proof, err)
	}
}

func TestExecutionNonAdmissionPersistenceFailureAndLostResponseRecover(t *testing.T) {
	for _, fault := range []string{"sync", "reply"} {
		t.Run(fault, func(t *testing.T) {
			directory := privateExecutionStateDirectory(t)
			f := durableExecutionFixture(t, directory, true, "", 9, time.Time{})
			if _, err := f.supervisor.InstallExecutionFloor(t.Context(), f.disposition(t)); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			restore := modelruntime.SetExecutionStateSyncHookForTest(f.supervisor, func(sync func() error) error {
				if fault == "sync" {
					return errors.New("injected non-admission directory sync failure")
				}
				err := sync()
				cancel()
				return err
			})
			proof, err := f.supervisor.CheckpointNonAdmission(ctx, f.authorities[1])
			restore()
			if proof != nil || err == nil {
				t.Fatalf("uncertain persistence/reply returned proof: %+v %v", proof, err)
			}
			if fault == "sync" {
				if proof, err := f.supervisor.InspectNonAdmission(t.Context(), f.authorities[1]); proof != nil || !errors.Is(err, modelruntime.ErrExecutionStateRecovery) {
					t.Fatalf("failed store leaked proof: %+v %v", proof, err)
				}
			}
			f.supervisor.Close()
			recovered := durableExecutionFixture(t, directory, false, "", 10, f.clock.Now())
			proof, err = recovered.supervisor.InspectNonAdmission(t.Context(), f.authorities[1])
			if err != nil || proof == nil {
				t.Fatalf("recovered durable non-admission: %+v %v", proof, err)
			}
		})
	}
}

func TestExecutionNonAdmissionRejectsConcurrentAcceptedPrepare(t *testing.T) {
	f := durableExecutionFixture(t, privateExecutionStateDirectory(t), true, "prepare", 9, time.Time{})
	finished := make(chan error, 1)
	go func() { finished <- runFloorOperation(f, "prepare") }()
	select {
	case <-f.backend.entered:
	case <-time.After(time.Second):
		t.Fatal("Prepare did not enter")
	}
	if _, err := f.supervisor.InstallExecutionFloor(t.Context(), f.disposition(t)); err != nil {
		t.Fatal(err)
	}
	proof, err := f.supervisor.CheckpointNonAdmission(t.Context(), f.authorities[0])
	f.backend.unblock()
	if prepareErr := <-finished; prepareErr != nil {
		t.Fatal(prepareErr)
	}
	if proof != nil || !errors.Is(err, modelruntime.ErrExecutionNonAdmissionUnproven) {
		t.Fatalf("in-flight backend claimed absent: %+v %v", proof, err)
	}
}

func TestExecutionNonAdmissionHistoryRejectsCorruption(t *testing.T) {
	for _, fault := range []string{"signature", "digest", "sequence", "floor", "contract", "timestamp", "duplicate", "admitted"} {
		t.Run(fault, func(t *testing.T) {
			directory := privateExecutionStateDirectory(t)
			f := durableExecutionFixture(t, directory, true, "", 9, time.Time{})
			prepareFloorRuntime(t, f.supervisor, f.authorities[0])
			if _, err := f.supervisor.InstallExecutionFloor(t.Context(), f.disposition(t)); err != nil {
				t.Fatal(err)
			}
			if _, err := f.supervisor.CheckpointNonAdmission(t.Context(), f.authorities[1]); err != nil {
				t.Fatal(err)
			}
			f.supervisor.Close()
			state := readDurableExecutionState(t, directory)
			var entries []nonAdmissionDocument
			if err := json.Unmarshal(state.NonAdmissions, &entries); err != nil || len(entries) != 1 {
				t.Fatalf("decode proof: %v", err)
			}
			switch fault {
			case "signature":
				entries[0].Authority[len(entries[0].Authority)-1] ^= 1
			case "digest":
				entries[0].AuthorityDigest[0] ^= 1
			case "sequence":
				entries[0].ExecutionSequence++
			case "floor":
				entries[0].InstalledCutoff++
			case "contract":
				entries[0].Contract = "STOPPED"
			case "timestamp":
				entries[0].ObservedAt = time.Time{}
			case "duplicate":
				entries = append(entries, entries[0])
			case "admitted":
				entries[0].Authority = bytes.Clone(state.Authority)
				entries[0].ExecutionSequence = 10
				digest, err := stageauthority.Digest(f.authorities[0])
				if err != nil {
					t.Fatal(err)
				}
				entries[0].AuthorityDigest = digest
			}
			var err error
			state.NonAdmissions, err = json.Marshal(entries)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(directory, durableStateFileName), encodeDurableExecutionState(t, state), 0o600); err != nil {
				t.Fatal(err)
			}
			if _, err := tryDurableExecutionFixture(t, directory, false); err == nil {
				t.Fatal("corrupt non-admission proof recovered")
			}
		})
	}
}

type nonAdmissionDocument struct {
	Authority         []byte    `json:"authority"`
	AuthorityDigest   [32]byte  `json:"authority_digest"`
	ExecutionSequence int64     `json:"execution_sequence"`
	InstalledCutoff   int64     `json:"installed_cutoff"`
	Contract          string    `json:"contract"`
	ObservedAt        time.Time `json:"observed_at"`
}

func TestExecutionNonAdmissionHistoryBoundsWithoutEvictingProof(t *testing.T) {
	f := durableExecutionFixture(t, privateExecutionStateDirectory(t), true, "", 9, time.Time{})
	disposition := f.disposition(t)
	disposition.Cutoff, disposition.Allocations[1].ExecutionSequence = 100, 100
	disposition, err := f.signer.SignTerminalDisposition(disposition)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.supervisor.InstallExecutionFloor(t.Context(), disposition); err != nil {
		t.Fatal(err)
	}
	var oldest *velav1.StageAuthority
	for sequence := int64(32); sequence >= 1; sequence-- {
		authority := f.authority(t, 1, sequence)
		oldest = authority
		if proof, err := f.supervisor.CheckpointNonAdmission(t.Context(), authority); err != nil || proof == nil {
			t.Fatalf("proof %d: %+v %v", sequence, proof, err)
		}
	}
	if proof, err := f.supervisor.CheckpointNonAdmission(t.Context(), f.authority(t, 1, 33)); proof != nil || !errors.Is(err, modelruntime.ErrExecutionNonAdmissionHistoryFull) {
		t.Fatalf("unbounded non-admission history: %+v %v", proof, err)
	}
	if proof, err := f.supervisor.InspectNonAdmission(t.Context(), oldest); err != nil || proof == nil {
		t.Fatalf("old proof evicted: %+v %v", proof, err)
	}
}

func sameNonAdmission(a, b *modelruntime.ExecutionNonAdmissionCheckpoint) bool {
	return a != nil && b != nil && proto.Equal(a.Authority, b.Authority) && a.WorkerMemberID == b.WorkerMemberID &&
		a.AuthorityDigest == b.AuthorityDigest && a.ExecutionSequence == b.ExecutionSequence &&
		a.InstalledCutoff == b.InstalledCutoff && a.Contract == b.Contract && a.ObservedAt.Equal(b.ObservedAt)
}

func TestExecutionNonAdmissionSchemaUpgradeRejectsUnverifiableSource(t *testing.T) {
	for _, fault := range []string{"schema-1", "signature", "new-proof-in-v2", "initialize"} {
		t.Run(fault, func(t *testing.T) {
			directory := privateExecutionStateDirectory(t)
			f := durableExecutionFixture(t, directory, true, "", 9, time.Time{})
			if _, err := f.supervisor.InstallExecutionFloor(t.Context(), f.disposition(t)); err != nil {
				t.Fatal(err)
			}
			if fault == "new-proof-in-v2" {
				if _, err := f.supervisor.CheckpointNonAdmission(t.Context(), f.authorities[1]); err != nil {
					t.Fatal(err)
				}
			}
			f.supervisor.Close()
			legacy := readDurableExecutionState(t, directory)
			legacy.SchemaVersion = 2
			legacy.Executions = withoutRetainedCandidates(t, legacy.Executions)
			if fault == "schema-1" {
				legacy.SchemaVersion = 1
			}
			if fault == "signature" {
				legacy.Disposition[len(legacy.Disposition)-1] ^= 1
			}
			wire := encodeDurableExecutionState(t, legacy)
			path := filepath.Join(directory, durableStateFileName)
			if err := os.WriteFile(path, wire, 0o600); err != nil {
				t.Fatal(err)
			}
			_, err := executionFloorFixtureWithState(t, "", &modelruntime.ExecutionFloorStateConfig{Directory: directory, UpgradeV2: true, Initialize: fault == "initialize"}, 10, f.clock.Now())
			if err == nil {
				t.Fatal("unverifiable journal upgraded")
			}
			after, err := os.ReadFile(path)
			if err != nil || !bytes.Equal(wire, after) {
				t.Fatal("rejected upgrade changed original journal")
			}
		})
	}
}

func TestExecutionNonAdmissionSchemaUpgradePreservesHistoryAndRequiresOptIn(t *testing.T) {
	for _, drained := range []bool{false, true} {
		t.Run(map[bool]string{false: "pending", true: "drained"}[drained], func(t *testing.T) {
			directory := privateExecutionStateDirectory(t)
			f := durableExecutionFixture(t, directory, true, "", 9, time.Time{})
			if drained {
				readyDrainOutput(t, f, f.backend.FakeRuntime, f.authorities[0])
				sealed, err := f.supervisor.SealOutput(t.Context(), &velav1.ModelRuntimeServiceSealOutputRequest{Authority: f.authorities[0]})
				if err != nil || sealed.GetReceipt() == nil {
					t.Fatalf("seal: %v %v", sealed, err)
				}
			} else {
				prepareFloorRuntime(t, f.supervisor, f.authorities[0])
			}
			if _, err := f.supervisor.InstallExecutionFloor(t.Context(), f.disposition(t)); err != nil {
				t.Fatal(err)
			}
			f.supervisor.Close()
			legacy := readDurableExecutionState(t, directory)
			legacy.SchemaVersion = 2
			legacy.Executions = withoutRetainedCandidates(t, legacy.Executions)
			path := filepath.Join(directory, durableStateFileName)
			wire := encodeDurableExecutionState(t, legacy)
			if err := os.WriteFile(path, wire, 0o600); err != nil {
				t.Fatal(err)
			}
			if _, err := tryDurableExecutionFixture(t, directory, false); err == nil {
				t.Fatal("schema-2 upgraded without explicit opt-in")
			}
			unchanged, err := os.ReadFile(path)
			if err != nil || !bytes.Equal(wire, unchanged) {
				t.Fatal("rejected upgrade changed legacy state")
			}
			upgraded, err := executionFloorFixtureWithState(t, "", &modelruntime.ExecutionFloorStateConfig{Directory: directory, UpgradeV2: true}, 10, f.clock.Now())
			if err != nil {
				t.Fatal(err)
			}
			after := readDurableExecutionState(t, directory)
			if after.SchemaVersion != 5 || after.ID != legacy.ID || after.Highest != legacy.Highest || after.Floor != legacy.Floor ||
				!bytes.Equal(after.Executions, legacy.Executions) || !bytes.Equal(after.Authority, legacy.Authority) || !bytes.Equal(after.Disposition, legacy.Disposition) || len(after.NonAdmissions) != 0 {
				t.Fatal("upgrade changed retained evidence or invented absence proof")
			}
			assertExecutionDrainCheckpoint(t, upgraded.supervisor, f.authorities[0], drained)
			if proof, err := upgraded.supervisor.InspectNonAdmission(t.Context(), f.authorities[1]); err != nil || proof != nil {
				t.Fatalf("upgrade invented historical absence: %+v %v", proof, err)
			}
			if !drained {
				assertRecoveryDrainBlocks(t, upgraded, upgraded.authority(t, 1, 12))
			}
			upgraded.supervisor.Close()
			retry, err := executionFloorFixtureWithState(t, "", &modelruntime.ExecutionFloorStateConfig{Directory: directory, UpgradeV2: true}, 11, f.clock.Now())
			if err != nil {
				t.Fatalf("upgrade retry after published state: %v", err)
			}
			retry.supervisor.Close()
		})
	}
}
