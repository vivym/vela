package modelruntime_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/modelruntime"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestTerminalNonAdmissionCoversUnsignedAllocationWithoutEnteringBackend(t *testing.T) {
	directory := privateExecutionStateDirectory(t)
	f := durableExecutionFixture(t, directory, true, "", 9, time.Time{})
	disposition := unsignedTerminalAllocation(t, f)
	id := disposition.GetAllocations()[1].GetStageAllocationId()
	if proof, err := f.supervisor.CheckpointTerminalNonAdmission(t.Context(), disposition, id); proof != nil || !errors.Is(err, modelruntime.ErrExecutionNonAdmissionUnproven) {
		t.Fatalf("checkpoint without durable floor: %+v %v", proof, err)
	}
	prepareFloorRuntime(t, f.supervisor, f.authorities[0])
	if _, err := f.supervisor.InstallExecutionFloor(t.Context(), disposition); err != nil {
		t.Fatal(err)
	}
	if proof, err := f.supervisor.CheckpointTerminalNonAdmission(t.Context(), disposition, f.authorities[0].GetStageAllocationId()); proof != nil || !errors.Is(err, modelruntime.ErrExecutionNonAdmissionUnproven) {
		t.Fatalf("persisted execution intent claimed absent: %+v %v", proof, err)
	}
	before := f.backend.calls.Load()
	proof, err := f.supervisor.CheckpointTerminalNonAdmission(t.Context(), disposition, id)
	if err != nil || proof == nil || proof.Contract != modelruntime.TerminalNonAdmissionContract || proof.StageAllocationID != id ||
		proof.ExecutionSequence != 11 || proof.InstalledCutoff != 11 || !proto.Equal(proof.Disposition, disposition) ||
		proof.WorkerMemberID != f.bindings[0].WorkerMemberID || f.backend.calls.Load() != before || f.backend.closed.Load() {
		t.Fatalf("unsigned allocation checkpoint: %+v %v", proof, err)
	}
	state := readDurableExecutionState(t, directory)
	if state.SchemaVersion != 7 || state.Highest != 10 || len(state.TerminalNonAdmissions) == 0 || len(state.NonAdmissions) != 0 {
		t.Fatal("checkpoint omitted durable proof or invented execution authority")
	}
	if next, err := f.supervisor.CheckpointTerminalNonAdmission(t.Context(), disposition, id); err != nil || !sameTerminalNonAdmission(proof, next) {
		t.Fatalf("checkpoint retry: %+v %v", next, err)
	}
	// Existing execution-envelope queries must not convert the new proof format.
	if next, err := f.supervisor.InspectNonAdmission(t.Context(), f.authorities[1]); err != nil || next != nil {
		t.Fatalf("terminal proof became execution-envelope proof: %+v %v", next, err)
	}
	if next, err := f.supervisor.CheckpointNonAdmission(t.Context(), f.authorities[1]); err == nil || next != nil {
		t.Fatalf("same sequence with different allocation acquired a second proof: %+v %v", next, err)
	}
	assertFloorCommandsRejected(t, f.supervisor, f.authorities[1])
	f.clock.Advance(2 * time.Minute)
	if next, err := f.supervisor.InspectTerminalNonAdmission(t.Context(), disposition, id); err != nil || !sameTerminalNonAdmission(proof, next) {
		t.Fatalf("expired historical checkpoint: %+v %v", next, err)
	}
	if next, err := f.supervisor.CheckpointTerminalNonAdmission(t.Context(), disposition, id); err == nil || next != nil {
		t.Fatalf("expired checkpoint request: %+v %v", next, err)
	}
	refreshed := proto.Clone(disposition).(*velav1.StageTerminalDisposition)
	refreshed.ObservedAt, refreshed.ExpiresAt = timestamppb.New(f.clock.Now()), timestamppb.New(f.clock.Now().Add(time.Minute))
	refreshed.ControlSessionEpoch++
	refreshed = signTerminalNonAdmission(t, f, refreshed)
	if next, err := f.supervisor.InspectTerminalNonAdmission(t.Context(), refreshed, id); err != nil || !sameTerminalNonAdmission(proof, next) {
		t.Fatalf("refreshed query replaced original signed proof: %+v %v", next, err)
	}
	proof.Disposition.Signature[0] ^= 1
	if next, err := f.supervisor.InspectTerminalNonAdmission(t.Context(), disposition, id); err != nil || next == nil || !proto.Equal(next.Disposition, disposition) {
		t.Fatal("caller mutated durable checkpoint")
	}
}

func TestTerminalNonAdmissionRecoveryNeverInfersMissingOldEpochProof(t *testing.T) {
	for _, checkpoint := range []bool{false, true} {
		t.Run(map[bool]string{false: "unknown", true: "checkpointed"}[checkpoint], func(t *testing.T) {
			directory := privateExecutionStateDirectory(t)
			f := durableExecutionFixture(t, directory, true, "", 9, time.Time{})
			disposition := unsignedTerminalAllocation(t, f)
			id := disposition.GetAllocations()[1].GetStageAllocationId()
			if _, err := f.supervisor.InstallExecutionFloor(t.Context(), disposition); err != nil {
				t.Fatal(err)
			}
			var saved *modelruntime.TerminalNonAdmissionCheckpoint
			if checkpoint {
				var err error
				saved, err = f.supervisor.CheckpointTerminalNonAdmission(t.Context(), disposition, id)
				if err != nil || saved == nil {
					t.Fatal(err)
				}
			}
			f.supervisor.Close()
			recovered := durableExecutionFixture(t, directory, false, "", 10, f.clock.Now().Add(2*time.Minute))
			proof, err := recovered.supervisor.InspectTerminalNonAdmission(t.Context(), disposition, id)
			if err != nil || !sameTerminalNonAdmission(saved, proof) {
				t.Fatalf("recovered checkpoint=%t: %+v %v", checkpoint, proof, err)
			}
			fresh := proto.Clone(disposition).(*velav1.StageTerminalDisposition)
			fresh.ObservedAt, fresh.ExpiresAt = timestamppb.New(recovered.clock.Now()), timestamppb.New(recovered.clock.Now().Add(time.Minute))
			fresh = signTerminalNonAdmission(t, recovered, fresh)
			if proof, err := recovered.supervisor.CheckpointTerminalNonAdmission(t.Context(), fresh, id); err == nil || proof != nil {
				t.Fatalf("new epoch checkpointed historical absence: %+v %v", proof, err)
			}
		})
	}
}

func TestTerminalNonAdmissionReplayBindsExactHistoricalScope(t *testing.T) {
	f := durableExecutionFixture(t, privateExecutionStateDirectory(t), true, "", 9, time.Time{})
	disposition := unsignedTerminalAllocation(t, f)
	id := disposition.GetAllocations()[1].GetStageAllocationId()
	if _, err := f.supervisor.InstallExecutionFloor(t.Context(), disposition); err != nil {
		t.Fatal(err)
	}
	if proof, err := f.supervisor.CheckpointTerminalNonAdmission(t.Context(), disposition, id); err != nil || proof == nil {
		t.Fatal(err)
	}
	mutations := map[string]func(*velav1.StageTerminalDisposition){
		"organization": func(d *velav1.StageTerminalDisposition) { d.OrganizationId = uuid.NewString() },
		"project":      func(d *velav1.StageTerminalDisposition) { d.ProjectId = uuid.NewString() },
		"job":          func(d *velav1.StageTerminalDisposition) { d.JobId = uuid.NewString() },
		"attempt":      func(d *velav1.StageTerminalDisposition) { d.AttemptId = uuid.NewString() },
		"stage run":    func(d *velav1.StageTerminalDisposition) { d.StageRunId = uuid.NewString() },
		"terminal state": func(d *velav1.StageTerminalDisposition) {
			d.TerminalState = velav1.StageTerminalState_STAGE_TERMINAL_STATE_CANCELED
		},
		"fence":         func(d *velav1.StageTerminalDisposition) { d.StageFence++ },
		"version":       func(d *velav1.StageTerminalDisposition) { d.StageVersion++ },
		"allocation":    func(d *velav1.StageTerminalDisposition) { d.Allocations[1].StageAllocationId = uuid.NewString() },
		"stage attempt": func(d *velav1.StageTerminalDisposition) { d.Allocations[1].StageAttemptId = uuid.NewString() },
		"lease":         func(d *velav1.StageTerminalDisposition) { d.Allocations[1].StageLeaseId = uuid.NewString() },
		"nonce":         func(d *velav1.StageTerminalDisposition) { d.Allocations[1].ExecutionNonce[0] ^= 1 },
		"barrier":       func(d *velav1.StageTerminalDisposition) { d.Allocations[1].BarrierGeneration++ },
	}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			changed := proto.Clone(disposition).(*velav1.StageTerminalDisposition)
			mutate(changed)
			changed = signTerminalNonAdmission(t, f, changed)
			id := changed.GetAllocations()[1].GetStageAllocationId()
			if proof, err := f.supervisor.InspectTerminalNonAdmission(t.Context(), changed, id); err == nil || proof != nil {
				t.Fatalf("conflicting replay: %+v %v", proof, err)
			}
			if proof, err := f.supervisor.CheckpointTerminalNonAdmission(t.Context(), changed, id); err == nil || proof != nil {
				t.Fatalf("conflicting checkpoint replaced proof: %+v %v", proof, err)
			}
		})
	}
	if proof, err := f.supervisor.InspectTerminalNonAdmission(t.Context(), disposition, id); err != nil || proof == nil {
		t.Fatal("rejected query damaged original proof")
	}
}

func TestTerminalNonAdmissionSerializesWithAcceptedPrepare(t *testing.T) {
	f := durableExecutionFixture(t, privateExecutionStateDirectory(t), true, "prepare", 9, time.Time{})
	finished := make(chan error, 1)
	go func() { finished <- runFloorOperation(f, "prepare") }()
	select {
	case <-f.backend.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("Prepare did not enter backend")
	}
	disposition := f.disposition(t)
	if _, err := f.supervisor.InstallExecutionFloor(t.Context(), disposition); err != nil {
		t.Fatal(err)
	}
	if proof, err := f.supervisor.CheckpointTerminalNonAdmission(t.Context(), disposition, f.authorities[0].GetStageAllocationId()); err == nil || proof != nil {
		t.Fatalf("active Prepare was missed: %+v %v", proof, err)
	}
	f.backend.unblock()
	if err := <-finished; err != nil {
		t.Fatal(err)
	}
	if proof, err := f.supervisor.CheckpointTerminalNonAdmission(t.Context(), disposition, f.authorities[0].GetStageAllocationId()); err == nil || proof != nil {
		t.Fatalf("returned Prepare became non-admission: %+v %v", proof, err)
	}
}

func TestTerminalNonAdmissionRejectsInvalidOrMismatchedEvidence(t *testing.T) {
	mutations := map[string]func(*velav1.StageTerminalDisposition){
		"worker":       func(d *velav1.StageTerminalDisposition) { d.WorkerInstanceId = uuid.NewString() },
		"worker epoch": func(d *velav1.StageTerminalDisposition) { d.WorkerInstanceEpoch++ },
		"device":       func(d *velav1.StageTerminalDisposition) { d.Devices[0].DeviceEpoch++ },
		"device set":   func(d *velav1.StageTerminalDisposition) { d.DeviceSetDigest[0] ^= 1 },
		"membership":   func(d *velav1.StageTerminalDisposition) { d.MembershipDigest[0] ^= 1 },
		"member epoch": func(d *velav1.StageTerminalDisposition) {
			for _, allocation := range d.Allocations {
				allocation.Members[0].MemberEpoch++
			}
		},
		"identity": func(d *velav1.StageTerminalDisposition) {
			for _, allocation := range d.Allocations {
				allocation.Members[0].IdentityDigest[0] ^= 1
			}
		},
		"subset": func(d *velav1.StageTerminalDisposition) {
			for _, allocation := range d.Allocations {
				allocation.Members[0].DeviceSubsetDigest[0] ^= 1
			}
		},
		"runtime epoch": func(d *velav1.StageTerminalDisposition) { d.Allocations[1].Members[0].ModelRuntimeEpoch++ },
		"runtime":       func(d *velav1.StageTerminalDisposition) { d.Allocations[1].ModelRuntimeIdentity = "unknown-runtime" },
		"residency":     func(d *velav1.StageTerminalDisposition) { d.Allocations[1].ModelResidencyId = uuid.NewString() },
		"profile":       func(d *velav1.StageTerminalDisposition) { d.Allocations[1].StageProfileRevisionId = uuid.NewString() },
		"future": func(d *velav1.StageTerminalDisposition) {
			d.ObservedAt = timestamppb.New(d.GetObservedAt().AsTime().Add(time.Second))
		},
	}
	f := durableExecutionFixture(t, privateExecutionStateDirectory(t), true, "", 9, time.Time{})
	disposition := f.disposition(t)
	if _, err := f.supervisor.InstallExecutionFloor(t.Context(), disposition); err != nil {
		t.Fatal(err)
	}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			changed := proto.Clone(disposition).(*velav1.StageTerminalDisposition)
			mutate(changed)
			changed = signTerminalNonAdmission(t, f, changed)
			if proof, err := f.supervisor.CheckpointTerminalNonAdmission(t.Context(), changed, changed.GetAllocations()[1].GetStageAllocationId()); err == nil || proof != nil {
				t.Fatalf("mismatched evidence: %+v %v", proof, err)
			}
		})
	}
	tampered := proto.Clone(disposition).(*velav1.StageTerminalDisposition)
	tampered.Signature[0] ^= 1
	if proof, err := f.supervisor.CheckpointTerminalNonAdmission(t.Context(), tampered, tampered.GetStageAllocationId()); err == nil || proof != nil {
		t.Fatalf("invalid signature: %+v %v", proof, err)
	}
	if proof, err := f.supervisor.CheckpointTerminalNonAdmission(t.Context(), disposition, uuid.NewString()); err == nil || proof != nil {
		t.Fatalf("allocation outside history: %+v %v", proof, err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if proof, err := f.supervisor.CheckpointTerminalNonAdmission(ctx, disposition, disposition.GetStageAllocationId()); !errors.Is(err, context.Canceled) || proof != nil {
		t.Fatalf("canceled checkpoint: %+v %v", proof, err)
	}
	memory := newExecutionFloorFixture(t, "")
	disposition = memory.disposition(t)
	if _, err := memory.supervisor.InstallExecutionFloor(t.Context(), disposition); err != nil {
		t.Fatal(err)
	}
	if proof, err := memory.supervisor.CheckpointTerminalNonAdmission(t.Context(), disposition, disposition.GetStageAllocationId()); err == nil || proof != nil {
		t.Fatalf("memory-only floor claimed durable non-admission: %+v %v", proof, err)
	}
}

func TestTerminalNonAdmissionPersistenceFailureAndLostReplyRecover(t *testing.T) {
	for _, fault := range []string{"sync", "reply"} {
		t.Run(fault, func(t *testing.T) {
			directory := privateExecutionStateDirectory(t)
			f := durableExecutionFixture(t, directory, true, "", 9, time.Time{})
			disposition := unsignedTerminalAllocation(t, f)
			id := disposition.GetAllocations()[1].GetStageAllocationId()
			if _, err := f.supervisor.InstallExecutionFloor(t.Context(), disposition); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			restore := modelruntime.SetExecutionStateSyncHookForTest(f.supervisor, func(sync func() error) error {
				if fault == "sync" {
					return errors.New("injected terminal non-admission sync failure")
				}
				err := sync()
				cancel()
				return err
			})
			proof, err := f.supervisor.CheckpointTerminalNonAdmission(ctx, disposition, id)
			restore()
			if err == nil || proof != nil {
				t.Fatalf("uncertain persistence/reply: %+v %v", proof, err)
			}
			if fault == "sync" {
				if proof, err := f.supervisor.InspectTerminalNonAdmission(t.Context(), disposition, id); !errors.Is(err, modelruntime.ErrExecutionStateRecovery) || proof != nil {
					t.Fatalf("unhealthy instance leaked checkpoint: %+v %v", proof, err)
				}
			}
			f.supervisor.Close()
			recovered := durableExecutionFixture(t, directory, false, "", 10, f.clock.Now())
			if proof, err := recovered.supervisor.InspectTerminalNonAdmission(t.Context(), disposition, id); err != nil || proof == nil {
				t.Fatalf("lost checkpoint recovery: %+v %v", proof, err)
			}
		})
	}
}

func TestTerminalNonAdmissionRejectsConflictingProofsInBothOrders(t *testing.T) {
	for _, terminalFirst := range []bool{false, true} {
		t.Run(map[bool]string{false: "execution first", true: "terminal first"}[terminalFirst], func(t *testing.T) {
			f := durableExecutionFixture(t, privateExecutionStateDirectory(t), true, "", 9, time.Time{})
			disposition := f.disposition(t)
			id := disposition.GetAllocations()[1].GetStageAllocationId()
			if _, err := f.supervisor.InstallExecutionFloor(t.Context(), disposition); err != nil {
				t.Fatal(err)
			}
			if terminalFirst {
				if proof, err := f.supervisor.CheckpointTerminalNonAdmission(t.Context(), disposition, id); err != nil || proof == nil {
					t.Fatal(err)
				}
			} else if proof, err := f.supervisor.CheckpointNonAdmission(t.Context(), f.authorities[1]); err != nil || proof == nil {
				t.Fatal(err)
			}
			if terminalFirst {
				if proof, err := f.supervisor.CheckpointNonAdmission(t.Context(), f.authority(t, 1, 11)); err == nil || proof != nil {
					t.Fatal("conflicting execution identity accepted")
				}
			} else {
				conflict := proto.Clone(disposition).(*velav1.StageTerminalDisposition)
				conflict.Allocations[1].ExecutionNonce[0] ^= 1
				conflict = signTerminalNonAdmission(t, f, conflict)
				if proof, err := f.supervisor.CheckpointTerminalNonAdmission(t.Context(), conflict, id); err == nil || proof != nil {
					t.Fatal("conflicting terminal identity accepted")
				}
			}
			if proof, err := f.supervisor.CheckpointTerminalNonAdmission(t.Context(), disposition, id); err != nil || proof == nil {
				t.Fatalf("matching terminal proof rejected: %+v %v", proof, err)
			}
			if proof, err := f.supervisor.CheckpointNonAdmission(t.Context(), f.authorities[1]); err != nil || proof == nil {
				t.Fatalf("matching execution proof rejected: %+v %v", proof, err)
			}
		})
	}
}

func TestTerminalNonAdmissionBoundNeverEvictsOldProof(t *testing.T) {
	f := durableExecutionFixture(t, privateExecutionStateDirectory(t), true, "", 9, time.Time{})
	base := unsignedTerminalAllocation(t, f)
	var oldest *velav1.StageTerminalDisposition
	for sequence := int64(11); sequence <= 43; sequence++ {
		disposition := proto.Clone(base).(*velav1.StageTerminalDisposition)
		disposition.Cutoff, disposition.Allocations[1].ExecutionSequence = sequence, sequence
		disposition.Allocations[1].StageAllocationId = uuid.NewString()
		disposition = signTerminalNonAdmission(t, f, disposition)
		if _, err := f.supervisor.InstallExecutionFloor(t.Context(), disposition); err != nil {
			t.Fatal(err)
		}
		proof, err := f.supervisor.CheckpointTerminalNonAdmission(t.Context(), disposition, disposition.GetAllocations()[1].GetStageAllocationId())
		if sequence < 43 && (err != nil || proof == nil) {
			t.Fatalf("checkpoint sequence %d: %+v %v", sequence, proof, err)
		}
		if sequence == 43 && (!errors.Is(err, modelruntime.ErrExecutionNonAdmissionHistoryFull) || proof != nil) {
			t.Fatalf("unbounded history: %+v %v", proof, err)
		}
		if sequence == 11 {
			oldest = disposition
		}
	}
	if proof, err := f.supervisor.InspectTerminalNonAdmission(t.Context(), oldest, oldest.GetAllocations()[1].GetStageAllocationId()); err != nil || proof == nil || proof.InstalledCutoff != 11 {
		t.Fatalf("history overflow evicted proof or changed its floor: %+v %v", proof, err)
	}
}

type terminalNonAdmissionDocument struct {
	Disposition       []byte            `json:"disposition"`
	DispositionDigest [sha256.Size]byte `json:"disposition_digest"`
	StageAllocationID string            `json:"stage_allocation_id"`
	ExecutionSequence int64             `json:"execution_sequence"`
	InstalledCutoff   int64             `json:"installed_cutoff"`
	Contract          string            `json:"contract"`
	ObservedAt        time.Time         `json:"observed_at"`
}

func TestTerminalNonAdmissionRecoveryRejectsCorruptProofs(t *testing.T) {
	for _, fault := range []string{"digest", "signature", "allocation", "sequence", "floor low", "floor high", "contract", "time", "duplicate", "schema3", "scope", "execution collision"} {
		t.Run(fault, func(t *testing.T) {
			directory := privateExecutionStateDirectory(t)
			f := durableExecutionFixture(t, directory, true, "", 9, time.Time{})
			disposition := unsignedTerminalAllocation(t, f)
			id := disposition.GetAllocations()[1].GetStageAllocationId()
			if _, err := f.supervisor.InstallExecutionFloor(t.Context(), disposition); err != nil {
				t.Fatal(err)
			}
			if proof, err := f.supervisor.CheckpointTerminalNonAdmission(t.Context(), disposition, id); err != nil || proof == nil {
				t.Fatal(err)
			}
			f.supervisor.Close()
			state := readDurableExecutionState(t, directory)
			var records []terminalNonAdmissionDocument
			if err := json.Unmarshal(state.TerminalNonAdmissions, &records); err != nil || len(records) != 1 {
				t.Fatal("missing stored checkpoint")
			}
			switch fault {
			case "digest":
				records[0].DispositionDigest[0] ^= 1
			case "signature":
				disposition.Signature[0] ^= 1
				records[0].Disposition, _ = proto.MarshalOptions{Deterministic: true}.Marshal(disposition)
			case "allocation":
				records[0].StageAllocationID = uuid.NewString()
			case "sequence":
				records[0].ExecutionSequence++
			case "floor low":
				records[0].InstalledCutoff--
			case "floor high":
				records[0].InstalledCutoff++
			case "contract":
				records[0].Contract = modelruntime.ExecutionDrainContract
			case "time":
				records[0].ObservedAt = disposition.GetObservedAt().AsTime().Add(-time.Second)
			case "duplicate":
				records = append(records, records[0])
			case "schema3":
				state.SchemaVersion = 3
			case "scope":
				disposition.WorkerInstanceId = uuid.NewString()
				disposition = signTerminalNonAdmission(t, f, disposition)
				verified, err := f.validator.ValidateTerminalDispositionSignature(disposition)
				if err != nil {
					t.Fatal(err)
				}
				records[0].Disposition, _ = proto.MarshalOptions{Deterministic: true}.Marshal(disposition)
				records[0].DispositionDigest = verified.Digest
			case "execution collision":
				wire, err := proto.MarshalOptions{Deterministic: true}.Marshal(f.authorities[1])
				if err != nil {
					t.Fatal(err)
				}
				state.Highest, state.Authority = 11, wire
				state.Executions, err = json.Marshal([]retainedExecutionDocument{{Authority: wire}})
				if err != nil {
					t.Fatal(err)
				}
			}
			var err error
			state.TerminalNonAdmissions, err = json.Marshal(records)
			if err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(directory, durableStateFileName)
			wire := encodeDurableExecutionState(t, state)
			if err := os.WriteFile(path, wire, 0o600); err != nil {
				t.Fatal(err)
			}
			if _, err := executionFloorFixtureWithState(t, "", &modelruntime.ExecutionFloorStateConfig{Directory: directory, UpgradeV3: true}, 10, f.clock.Now()); err == nil {
				t.Fatal("corrupt or mislabeled proof accepted")
			}
			unchanged, err := os.ReadFile(path)
			if err != nil || !bytes.Equal(unchanged, wire) {
				t.Fatal("rejected recovery changed journal")
			}
		})
	}
}

func TestTerminalNonAdmissionSchema3UpgradePreservesEvidence(t *testing.T) {
	for _, drained := range []bool{false, true} {
		t.Run(map[bool]string{false: "pending", true: "drained"}[drained], func(t *testing.T) {
			directory := privateExecutionStateDirectory(t)
			f := durableExecutionFixture(t, directory, true, "", 9, time.Time{})
			if drained {
				readyDrainOutput(t, f, f.backend.FakeRuntime, f.authorities[0])
				response, err := f.supervisor.SealOutput(t.Context(), &velav1.ModelRuntimeServiceSealOutputRequest{Authority: f.authorities[0]})
				if err != nil || response.GetReceipt() == nil {
					t.Fatal("seal failed")
				}
			} else {
				prepareFloorRuntime(t, f.supervisor, f.authorities[0])
			}
			disposition := f.disposition(t)
			if _, err := f.supervisor.InstallExecutionFloor(t.Context(), disposition); err != nil {
				t.Fatal(err)
			}
			if proof, err := f.supervisor.CheckpointNonAdmission(t.Context(), f.authorities[1]); err != nil || proof == nil {
				t.Fatal(err)
			}
			f.supervisor.Close()
			legacy := readDurableExecutionState(t, directory)
			legacy.SchemaVersion = 3
			legacy.Executions = withoutRetainedCandidates(t, legacy.Executions)
			path := filepath.Join(directory, durableStateFileName)
			wire := encodeDurableExecutionState(t, legacy)
			if err := os.WriteFile(path, wire, 0o600); err != nil {
				t.Fatal(err)
			}
			for _, config := range []modelruntime.ExecutionFloorStateConfig{
				{Directory: directory}, {Directory: directory, UpgradeV2: true},
				{Directory: directory, Initialize: true, UpgradeV3: true}, {Directory: directory, UpgradeV2: true, UpgradeV3: true},
			} {
				if _, err := executionFloorFixtureWithState(t, "", &config, 10, f.clock.Now()); err == nil {
					t.Fatal("invalid migration mode accepted")
				}
				unchanged, err := os.ReadFile(path)
				if err != nil || !bytes.Equal(unchanged, wire) {
					t.Fatal("rejected upgrade modified legacy evidence")
				}
			}
			upgraded, err := executionFloorFixtureWithState(t, "", &modelruntime.ExecutionFloorStateConfig{Directory: directory, UpgradeV3: true}, 10, f.clock.Now())
			if err != nil {
				t.Fatal(err)
			}
			after := readDurableExecutionState(t, directory)
			legacy.SchemaVersion = 7
			legacy.BackendLifecycle = &modelruntime.BackendLifecycleStatus{State: modelruntime.BackendLifecycleLegacyUnknown}
			if !bytes.Equal(encodeDurableExecutionState(t, after), encodeDurableExecutionState(t, legacy)) {
				t.Fatal("migration modified prior proof or invented new proof")
			}
			assertExecutionDrainCheckpoint(t, upgraded.supervisor, f.authorities[0], drained)
			if proof, err := upgraded.supervisor.InspectNonAdmission(t.Context(), f.authorities[1]); err != nil || proof == nil {
				t.Fatal("lost existing non-admission proof")
			}
			if proof, err := upgraded.supervisor.InspectTerminalNonAdmission(t.Context(), disposition, disposition.GetAllocations()[1].GetStageAllocationId()); err != nil || proof != nil {
				t.Fatal("migration invented terminal proof")
			}
			if !drained {
				assertRecoveryDrainBlocks(t, upgraded, upgraded.authority(t, 1, 12))
			}
			upgraded.supervisor.Close()
			retry, err := executionFloorFixtureWithState(t, "", &modelruntime.ExecutionFloorStateConfig{Directory: directory, UpgradeV3: true}, 11, f.clock.Now())
			if err != nil {
				t.Fatal(err)
			}
			retry.supervisor.Close()
		})
	}
}

func TestTerminalNonAdmissionSurvivesAbruptProcessExit(t *testing.T) {
	directory := privateExecutionStateDirectory(t)
	command := exec.CommandContext(t.Context(), os.Args[0], "-test.run=^TestTerminalNonAdmissionProcessHelper$")
	command.Env = append(os.Environ(), "VELA_TERMINAL_NON_ADMISSION_TEST_ROOT="+directory)
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("process helper: %s %v", output, err)
	}
	state := readDurableExecutionState(t, directory)
	var records []terminalNonAdmissionDocument
	if err := json.Unmarshal(state.TerminalNonAdmissions, &records); err != nil || len(records) != 1 {
		t.Fatal("process left no proof")
	}
	var disposition velav1.StageTerminalDisposition
	if err := proto.Unmarshal(records[0].Disposition, &disposition); err != nil {
		t.Fatal(err)
	}
	f := durableExecutionFixture(t, directory, false, "", 10, disposition.GetObservedAt().AsTime().Add(2*time.Minute))
	proof, err := f.supervisor.InspectTerminalNonAdmission(t.Context(), &disposition, records[0].StageAllocationID)
	if err != nil || proof == nil || proof.DispositionDigest != records[0].DispositionDigest {
		t.Fatalf("process-exit recovery: %+v %v", proof, err)
	}
}

func TestTerminalNonAdmissionProcessHelper(t *testing.T) {
	directory := os.Getenv("VELA_TERMINAL_NON_ADMISSION_TEST_ROOT")
	if directory == "" {
		return
	}
	f := durableExecutionFixture(t, directory, true, "", 9, time.Time{})
	disposition := unsignedTerminalAllocation(t, f)
	if _, err := f.supervisor.InstallExecutionFloor(t.Context(), disposition); err != nil {
		t.Fatal(err)
	}
	if proof, err := f.supervisor.CheckpointTerminalNonAdmission(t.Context(), disposition, disposition.GetAllocations()[1].GetStageAllocationId()); err != nil || proof == nil {
		t.Fatal(err)
	}
	os.Exit(0)
}

func unsignedTerminalAllocation(t *testing.T, f *executionFloorFixture) *velav1.StageTerminalDisposition {
	t.Helper()
	disposition := f.disposition(t)
	// This identity exists only in terminal history, never in a signed StageAuthority.
	allocation := disposition.Allocations[1]
	allocation.StageAttemptId, allocation.StageAllocationId, allocation.StageLeaseId = uuid.NewString(), uuid.NewString(), uuid.NewString()
	allocation.ExecutionNonce = bytes.Repeat([]byte{0xa7}, 32)
	return signTerminalNonAdmission(t, f, disposition)
}

func signTerminalNonAdmission(t *testing.T, f *executionFloorFixture, disposition *velav1.StageTerminalDisposition) *velav1.StageTerminalDisposition {
	t.Helper()
	signed, err := f.signer.SignTerminalDisposition(disposition)
	if err != nil {
		t.Fatal(err)
	}
	return signed
}

func sameTerminalNonAdmission(a, b *modelruntime.TerminalNonAdmissionCheckpoint) bool {
	if a == nil || b == nil {
		return a == b
	}
	return a.WorkerMemberID == b.WorkerMemberID && a.StageAllocationID == b.StageAllocationID && a.DispositionDigest == b.DispositionDigest &&
		a.ExecutionSequence == b.ExecutionSequence && a.InstalledCutoff == b.InstalledCutoff && a.Contract == b.Contract &&
		a.ObservedAt.Equal(b.ObservedAt) && proto.Equal(a.Disposition, b.Disposition)
}
