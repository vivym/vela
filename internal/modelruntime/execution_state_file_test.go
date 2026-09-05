package modelruntime_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/modelruntime"
	"github.com/vivym/vela/internal/stageauthority"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/protobuf/proto"
)

const durableStateFileName = "execution-admission.json"
const durableStateLockName = "execution-admission.lock"

func TestDurableExecutionStatePersistsBeforeBackendAndRestoresRestrictions(t *testing.T) {
	directory := privateExecutionStateDirectory(t)
	f := durableExecutionFixture(t, directory, true, "prepare", 9, time.Time{})
	prepared := make(chan error, 1)
	go func() { prepared <- runFloorOperation(f, "prepare") }()
	select {
	case <-f.backend.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("Prepare did not enter backend")
	}
	state := readDurableExecutionState(t, directory)
	if state.Highest != 10 || state.Floor != 0 || len(state.Authority) == 0 {
		t.Fatalf("backend entered before durable watermark: %+v", state)
	}
	var retained []retainedExecutionDocument
	if err := json.Unmarshal(state.Executions, &retained); err != nil || len(retained) != 1 || retained[0].Drain != nil ||
		!bytes.Equal(retained[0].Authority, state.Authority) {
		t.Fatalf("backend entered without a retained pending execution: %+v %v", retained, err)
	}
	checkpoint, err := f.supervisor.InstallExecutionFloor(context.Background(), f.disposition(t))
	if err != nil || !checkpoint.Durable || checkpoint.Cutoff != 11 {
		t.Fatalf("durable floor: %+v %v", checkpoint, err)
	}
	state = readDurableExecutionState(t, directory)
	if state.Highest != 10 || state.Floor != 11 || len(state.Disposition) == 0 {
		t.Fatalf("floor returned before persistence: %+v", state)
	}
	f.backend.unblock()
	if err := <-prepared; err != nil {
		t.Fatal(err)
	}
	if err := f.supervisor.Shutdown(); err != nil {
		t.Fatal(err)
	}

	// Historical signatures have expired, and the replacement Runtime has a new
	// epoch. Recovery restores only restrictions; it does not reattach old work.
	recovered := durableExecutionFixture(t, directory, false, "", 10, f.clock.Now().Add(2*time.Minute))
	assertFloorCommandsRejected(t, recovered.supervisor, recovered.authorities[0])
	assertFloorCommandsRejected(t, recovered.services[1], recovered.authorities[1])
	assertRecoveryDrainBlocks(t, recovered, recovered.authority(t, 0, 12))
	if err := recovered.supervisor.Shutdown(); err != nil {
		t.Fatal(err)
	}

	// Even an erroneously reused Runtime epoch cannot reset a consumed sequence.
	again := durableExecutionFixture(t, directory, false, "", 10, recovered.clock.Now())
	old, err := again.supervisor.PrepareStage(context.Background(), &velav1.ModelRuntimeServicePrepareStageRequest{
		Authority: again.authority(t, 1, 10), ExecutionSpec: runtimeExecutionSpec(),
	})
	if err != nil || old.GetDecision() != velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_STALE {
		t.Fatalf("recovered watermark reset across profiles: %v %v", old, err)
	}
	assertRecoveryDrainBlocks(t, again, again.authority(t, 1, 13))
}

func TestDurableExecutionStateRequiresExplicitInitializationAndExclusiveOwnership(t *testing.T) {
	directory := privateExecutionStateDirectory(t)
	if _, err := tryDurableExecutionFixture(t, directory, false); err == nil {
		t.Fatal("recovery initialized an empty root")
	}
	f := durableExecutionFixture(t, directory, true, "", 9, time.Time{})
	if _, err := tryDurableExecutionFixture(t, directory, true); err == nil {
		t.Fatal("initialization reused existing state")
	}
	if _, err := tryDurableExecutionFixture(t, directory, false); err == nil {
		t.Fatal("a second supervisor acquired the same state")
	}
	if err := f.supervisor.Shutdown(); err != nil {
		t.Fatal(err)
	}
	if _, err := tryDurableExecutionFixture(t, directory, false); err != nil {
		t.Fatalf("released state did not reopen: %v", err)
	}
	for _, name := range []string{durableStateFileName, durableStateLockName} {
		t.Run(name, func(t *testing.T) {
			root := privateExecutionStateDirectory(t)
			original := durableExecutionFixture(t, root, true, "", 9, time.Time{})
			original.supervisor.Close()
			if err := os.Remove(filepath.Join(root, name)); err != nil {
				t.Fatal(err)
			}
			if _, err := tryDurableExecutionFixture(t, root, false); err == nil {
				t.Fatal("missing state was reinitialized during recovery")
			}
		})
	}
	reused := privateExecutionStateDirectory(t)
	if err := os.WriteFile(filepath.Join(reused, "existing-data"), []byte("retained"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := tryDurableExecutionFixture(t, reused, true); err == nil {
		t.Fatal("initialized a reused directory")
	}
}

func TestDurableExecutionStateRejectsDamagedWitnessesAndOwnership(t *testing.T) {
	for _, corruption := range []string{"truncate", "duplicate field", "unknown field", "trailing value", "schema", "journal id", "lock id", "highest", "floor", "authority signature", "floor signature", "state symlink", "state hardlink", "state fifo", "lock symlink", "lock hardlink", "lock fifo", "copied root"} {
		t.Run(corruption, func(t *testing.T) {
			directory := privateExecutionStateDirectory(t)
			f := durableExecutionFixture(t, directory, true, "", 9, time.Time{})
			prepareFloorRuntime(t, f.supervisor, f.authorities[0])
			if _, err := f.supervisor.InstallExecutionFloor(context.Background(), f.disposition(t)); err != nil {
				t.Fatal(err)
			}
			f.supervisor.Close()
			state := readDurableExecutionState(t, directory)
			statePath := filepath.Join(directory, durableStateFileName)
			document, err := os.ReadFile(statePath)
			if err != nil {
				t.Fatal(err)
			}
			switch corruption {
			case "truncate":
				document = document[:len(document)/2]
			case "duplicate field":
				document = append([]byte(`{"schema_version":1,`), document[1:]...)
			case "unknown field":
				document = append([]byte(`{"unrecognized":true,`), document[1:]...)
			case "trailing value":
				document = append(document, []byte(`{}`)...)
			case "schema":
				state.SchemaVersion++
				document = encodeDurableExecutionState(t, state)
			case "journal id":
				state.ID = uuid.NewString()
				document = encodeDurableExecutionState(t, state)
			case "lock id":
				if err := os.WriteFile(filepath.Join(directory, durableStateLockName), []byte(uuid.NewString()), 0o600); err != nil {
					t.Fatal(err)
				}
			case "highest":
				state.Highest++
				document = encodeDurableExecutionState(t, state)
			case "floor":
				state.Floor++
				document = encodeDurableExecutionState(t, state)
			case "authority signature":
				state.Authority[len(state.Authority)-1] ^= 1
				document = encodeDurableExecutionState(t, state)
			case "floor signature":
				state.Disposition[len(state.Disposition)-1] ^= 1
				document = encodeDurableExecutionState(t, state)
			}
			if err := os.WriteFile(statePath, document, 0o600); err != nil {
				t.Fatal(err)
			}
			switch corruption {
			case "state symlink", "lock symlink":
				path := statePath
				if corruption == "lock symlink" {
					path = filepath.Join(directory, durableStateLockName)
				}
				if err := os.Rename(path, path+".retained"); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(path+".retained", path); err != nil {
					t.Fatal(err)
				}
			case "state hardlink", "lock hardlink":
				path := statePath
				if corruption == "lock hardlink" {
					path = filepath.Join(directory, durableStateLockName)
				}
				if err := os.Link(path, path+".alias"); err != nil {
					t.Fatal(err)
				}
			case "state fifo", "lock fifo":
				path := statePath
				if corruption == "lock fifo" {
					path = filepath.Join(directory, durableStateLockName)
				}
				if err := os.Remove(path); err != nil {
					t.Fatal(err)
				}
				if err := syscall.Mkfifo(path, 0o600); err != nil {
					t.Fatal(err)
				}
			case "copied root":
				copyRoot := privateExecutionStateDirectory(t)
				for _, name := range []string{durableStateFileName, durableStateLockName} {
					payload, err := os.ReadFile(filepath.Join(directory, name))
					if err != nil {
						t.Fatal(err)
					}
					if err := os.WriteFile(filepath.Join(copyRoot, name), payload, 0o600); err != nil {
						t.Fatal(err)
					}
				}
				directory = copyRoot
			}
			if _, err := tryDurableExecutionFixture(t, directory, false); err == nil {
				t.Fatalf("accepted damaged state: %s", corruption)
			}
		})
	}
}

func TestDurableExecutionStateFailsClosedOnLiveReplacementAndKeepsFailureSticky(t *testing.T) {
	for _, mutation := range []string{"state", "lock", "root", "in-place content"} {
		t.Run(mutation, func(t *testing.T) {
			directory := privateExecutionStateDirectory(t)
			f := durableExecutionFixture(t, directory, true, "", 9, time.Time{})
			path := filepath.Join(directory, durableStateFileName)
			if mutation == "lock" {
				path = filepath.Join(directory, durableStateLockName)
			}
			if mutation == "root" {
				path = directory
			}
			original, err := os.ReadFile(filepath.Join(directory, durableStateFileName))
			if err != nil {
				t.Fatal(err)
			}
			if mutation == "in-place content" {
				if err := os.WriteFile(path, []byte("damaged"), 0o600); err != nil {
					t.Fatal(err)
				}
			} else {
				if err := os.Rename(path, path+".retained"); err != nil {
					t.Fatal(err)
				}
				if mutation == "root" {
					if err := os.Mkdir(path, 0o700); err != nil {
						t.Fatal(err)
					}
				} else if err := os.WriteFile(path, nil, 0o600); err != nil {
					t.Fatal(err)
				}
			}
			assertFloorCommandsRejected(t, f.services[0], f.authorities[0])
			if mutation == "in-place content" {
				if err := os.WriteFile(path, original, 0o600); err != nil {
					t.Fatal(err)
				}
			} else {
				if err := os.Remove(path); err != nil {
					t.Fatal(err)
				}
				if err := os.Rename(path+".retained", path); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := f.supervisor.InstallExecutionFloor(context.Background(), f.disposition(t)); !errors.Is(err, modelruntime.ErrExecutionStateRecovery) {
				t.Fatalf("restoring a path cleared sticky recovery state: %v", err)
			}
			if f.backend.calls.Load() != 0 {
				t.Fatal("damaged state permitted backend execution")
			}
			f.supervisor.Close()
			recovered := durableExecutionFixture(t, directory, false, "", 9, time.Time{})
			prepareFloorRuntime(t, recovered.supervisor, recovered.authorities[0])
		})
	}
}

func TestDurableExecutionStateWriteFailurePreventsPrepareAndRenewal(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("requires a non-root identity for filesystem permission failures")
	}
	for _, operation := range []string{"prepare", "floor"} {
		t.Run(operation, func(t *testing.T) {
			directory := privateExecutionStateDirectory(t)
			f := durableExecutionFixture(t, directory, true, "", 9, time.Time{})
			if operation == "floor" {
				prepareFloorRuntime(t, f.supervisor, f.authorities[0])
			}
			if err := os.Chmod(directory, 0o500); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = os.Chmod(directory, 0o700) })
			if operation == "prepare" {
				assertFloorCommandsRejected(t, f.supervisor, f.authorities[0])
			} else {
				if checkpoint, err := f.supervisor.InstallExecutionFloor(context.Background(), f.disposition(t)); checkpoint != nil || !errors.Is(err, modelruntime.ErrExecutionStateRecovery) {
					t.Fatalf("failed persistence returned a floor checkpoint: %+v %v", checkpoint, err)
				}
				f.clock.Advance(time.Second)
				renewal := renewWatchdogAuthority(t, f.signer, f.authorities[0], f.clock.Now())
				assertFloorCommandsRejected(t, f.supervisor, renewal)
				cancel, err := f.supervisor.CancelStage(context.Background(), &velav1.ModelRuntimeServiceCancelStageRequest{
					Authority: f.authorities[0], Reason: velav1.ModelRuntimeCancelReason_MODEL_RUNTIME_CANCEL_REASON_CONTROL_PLANE_STOP,
				})
				if err != nil || !cancel.GetCancellationAcknowledged() {
					t.Fatalf("state failure prevented exact stop signaling: %v %v", cancel, err)
				}
			}
			if err := os.Chmod(directory, 0o700); err != nil {
				t.Fatal(err)
			}
			assertFloorCommandsRejected(t, f.supervisor, f.authority(t, 1, 100))
			expectedCalls := int64(0)
			if operation == "floor" {
				expectedCalls = 2
			}
			if f.backend.calls.Load() != expectedCalls {
				t.Fatalf("unexpected backend calls after state failure: %d", f.backend.calls.Load())
			}
			f.supervisor.Close()
			recovered := durableExecutionFixture(t, directory, false, "", 9, f.clock.Now())
			if state := readDurableExecutionState(t, directory); state.Floor != 0 {
				t.Fatal("failed floor was committed before temporary-file creation")
			}
			if _, err := recovered.supervisor.InstallExecutionFloor(context.Background(), recovered.disposition(t)); err != nil {
				t.Fatal(err)
			}
			if operation == "floor" {
				assertRecoveryDrainBlocks(t, recovered, recovered.authority(t, 1, 12))
			} else {
				prepareFloorRuntime(t, recovered.supervisor, recovered.authority(t, 1, 12))
			}
		})
	}
}

func TestDurableExecutionStateRejectsUnrecoverableAuthorityWithoutPoisoningGate(t *testing.T) {
	f := durableExecutionFixture(t, privateExecutionStateDirectory(t), true, "", 9, time.Time{})
	changed := f.authority(t, 0, 100)
	changed.Members[0].IdentityDigest = bytes.Repeat([]byte{0x99}, 32)
	changed, err := f.signer.Sign(changed)
	if err != nil {
		t.Fatal(err)
	}
	response, err := f.supervisor.PrepareStage(context.Background(), &velav1.ModelRuntimeServicePrepareStageRequest{Authority: changed, ExecutionSpec: runtimeExecutionSpec()})
	if err != nil || response.GetDecision() != velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_STALE {
		t.Fatalf("unrecoverable authority was consumed: %v %v", response, err)
	}
	prepareFloorRuntime(t, f.supervisor, f.authorities[0])
}

func TestDurableExecutionStateFaultStopsReadinessAdvertisement(t *testing.T) {
	directory := privateExecutionStateDirectory(t)
	f := durableExecutionFixture(t, directory, true, "", 9, time.Time{})
	identities, err := f.supervisor.DiscoverRuntimeIdentities(context.Background(), &velav1.ModelRuntimeServiceDiscoverRuntimeIdentitiesRequest{
		WorkerInstanceId: f.bindings[0].WorkerInstanceID, WorkerInstanceEpoch: f.bindings[0].WorkerInstanceEpoch,
		WorkerMemberId: f.bindings[0].WorkerMemberID, WorkerMemberEpoch: f.bindings[0].WorkerMemberEpoch,
	})
	if err != nil || len(identities.GetIdentities()) != 2 {
		t.Fatalf("discovery: %v %v", identities, err)
	}
	probe := func() bool {
		response, err := f.supervisor.ProbeReadiness(context.Background(), &velav1.ModelRuntimeServiceProbeReadinessRequest{
			Identity: identities.GetIdentities()[0], Check: velav1.ModelRuntimeReadinessCheck_MODEL_RUNTIME_READINESS_CHECK_MODEL_WARMUP,
		})
		if err != nil {
			t.Fatal(err)
		}
		return response.GetReady()
	}
	if _, err := f.supervisor.InstallExecutionFloor(context.Background(), f.disposition(t)); err != nil {
		t.Fatal(err)
	}
	if !probe() {
		t.Fatal("healthy retired Runtime stopped advertising readiness for newer allocations")
	}
	path := filepath.Join(directory, durableStateFileName)
	if err := os.Rename(path, path+".retained"); err != nil {
		t.Fatal(err)
	}
	if probe() {
		t.Fatal("missing execution state still advertised readiness")
	}
	if err := os.Rename(path+".retained", path); err != nil {
		t.Fatal(err)
	}
	if probe() {
		t.Fatal("path restoration cleared failed readiness without recovery")
	}
	f.supervisor.Close()
	if probe() {
		t.Fatal("closed Runtime advertised readiness")
	}
}

func TestDurableExecutionStateRejectsOversizedAuthorityWithoutPoisoningGate(t *testing.T) {
	directory := privateExecutionStateDirectory(t)
	f := durableExecutionFixture(t, directory, true, "", 9, time.Time{})
	large := f.authority(t, 0, 100)
	for i := range 1000 {
		large.CapacityVector[fmt.Sprintf("capacity-%05d-%070d", i, i)] = 1
	}
	large, err := f.signer.Sign(large)
	if err != nil {
		t.Fatal(err)
	}
	if proto.Size(large) <= 64<<10 {
		t.Fatal("authority fixture does not exceed the persistence bound")
	}
	response, err := f.supervisor.PrepareStage(context.Background(), &velav1.ModelRuntimeServicePrepareStageRequest{Authority: large, ExecutionSpec: runtimeExecutionSpec()})
	if err != nil || response.GetDecision() != velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_STALE {
		t.Fatalf("oversized authority: %v %v", response, err)
	}
	if state := readDurableExecutionState(t, directory); state.Highest != 0 {
		t.Fatal("oversized authority consumed a persistent sequence")
	}
	prepareFloorRuntime(t, f.supervisor, f.authorities[0])
}

func TestDurableExecutionStateBindsWorkerTopologyWhilePreservingRetiredProfiles(t *testing.T) {
	directory := privateExecutionStateDirectory(t)
	f := durableExecutionFixture(t, directory, true, "", 9, time.Time{})
	if _, err := f.supervisor.InstallExecutionFloor(context.Background(), f.disposition(t)); err != nil {
		t.Fatal(err)
	}
	f.supervisor.Close()
	for _, change := range []string{"worker", "worker epoch", "member", "member epoch", "device", "device set", "membership", "identity digest", "device subset", "new profile"} {
		t.Run(change, func(t *testing.T) {
			binding := runtimeBinding()
			identity, subset := bytes.Repeat([]byte{0x66}, 32), bytes.Repeat([]byte{0x67}, 32)
			switch change {
			case "worker":
				binding.WorkerInstanceID = uuid.NewString()
			case "worker epoch":
				binding.WorkerInstanceEpoch++
			case "member":
				binding.WorkerMemberID = uuid.NewString()
				binding.Members[0].ID = binding.WorkerMemberID
			case "member epoch":
				binding.WorkerMemberEpoch++
				binding.Members[0].Epoch++
			case "device":
				binding.Devices[0].Epoch++
			case "device set":
				binding.DeviceSetDigest[0] ^= 1
			case "membership":
				binding.MembershipDigest[0] ^= 1
			case "identity digest":
				identity[0] ^= 1
			case "device subset":
				subset[0] ^= 1
			case "new profile":
				binding.ModelResidencyID, binding.ModelRuntimeIdentity, binding.StageProfileRevisionID = uuid.NewString(), "replacement-runtime", uuid.NewString()
			}
			service := newRuntimeService(t, f.clock, f.validator, binding, modelruntime.NewFakeDiTRuntime())
			supervisor, err := modelruntime.NewSupervisorWithExecutionFloor(modelruntime.ExecutionFloorConfig{
				Validator: f.validator, State: &modelruntime.ExecutionFloorStateConfig{Directory: directory},
				Members: []modelruntime.ExecutionFloorMember{{WorkerMemberID: binding.WorkerMemberID, MemberEpoch: binding.WorkerMemberEpoch, IdentityDigest: identity, DeviceSubsetDigest: subset}},
			}, service)
			if change != "new profile" {
				if err == nil {
					supervisor.Close()
					t.Fatal("recovery changed trusted Worker topology")
				}
				return
			}
			if err != nil {
				t.Fatalf("historical profile prevented restoring a restriction: %v", err)
			}
			t.Cleanup(supervisor.Close)
			if _, err := supervisor.InstallExecutionFloor(context.Background(), f.disposition(t)); !errors.Is(err, stageauthority.ErrRuntimeMismatch) {
				t.Fatalf("fresh install claimed coverage of absent historical runtimes: %v", err)
			}
			authority := signSupervisorAuthority(t, f.signer, f.clock.Now(), binding, "31")
			authority.ExecutionSequence = 11
			authority, err = f.signer.Sign(authority)
			if err != nil {
				t.Fatal(err)
			}
			assertFloorCommandsRejected(t, supervisor, authority)
			authority.ExecutionSequence = 12
			authority, err = f.signer.Sign(authority)
			if err != nil {
				t.Fatal(err)
			}
			prepareFloorRuntime(t, supervisor, authority)
		})
	}
}

func TestDurableExecutionStateKeepsLifetimeLockUntilAdmittedCallsReturn(t *testing.T) {
	directory := privateExecutionStateDirectory(t)
	f := newExecutionFloorFixture(t, "")
	backend := &floorBlockingBackend{FakeRuntime: modelruntime.NewFakeDiTRuntime(), blocked: "prepare", entered: make(chan struct{}), resume: make(chan struct{})}
	// This backend deliberately has no Close contract, so Shutdown cannot join it.
	service := newRuntimeService(t, f.clock, f.validator, f.bindings[0], struct{ modelruntime.Backend }{backend})
	supervisor, err := modelruntime.NewSupervisorWithExecutionFloor(modelruntime.ExecutionFloorConfig{
		Validator: f.validator, State: &modelruntime.ExecutionFloorStateConfig{Directory: directory, Initialize: true},
		Members: []modelruntime.ExecutionFloorMember{{WorkerMemberID: f.bindings[0].WorkerMemberID, MemberEpoch: f.bindings[0].WorkerMemberEpoch,
			IdentityDigest: bytes.Repeat([]byte{0x66}, 32), DeviceSubsetDigest: bytes.Repeat([]byte{0x67}, 32)}},
	}, service)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(supervisor.Close)
	t.Cleanup(backend.unblock)
	done := make(chan velav1.ModelRuntimeCommandDecision, 1)
	go func() { done <- queuedAuthorityCommand(service, "prepare", f.authorities[0]) }()
	select {
	case <-backend.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("backend did not enter")
	}
	if err := supervisor.Shutdown(); err == nil {
		t.Fatal("Shutdown hid an admitted operation still in flight")
	}
	if _, err := tryDurableExecutionFixture(t, directory, false); err == nil {
		t.Fatal("Shutdown released the journal while an admitted operation remained")
	}
	backend.unblock()
	if decision := <-done; decision != velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_ACCEPTED {
		t.Fatalf("previously admitted operation did not return: %s", decision)
	}
	if err := supervisor.Shutdown(); err != nil {
		t.Fatal(err)
	}
	if _, err := tryDurableExecutionFixture(t, directory, false); err != nil {
		t.Fatalf("journal did not release after the final admitted return: %v", err)
	}
}

func TestDurableExecutionStateRecoversUncertainRenameWithoutReopeningAdmission(t *testing.T) {
	for _, operation := range []string{"prepare", "floor"} {
		t.Run(operation, func(t *testing.T) {
			directory := privateExecutionStateDirectory(t)
			f := durableExecutionFixture(t, directory, true, "", 9, time.Time{})
			restore := modelruntime.SetExecutionStateSyncHookForTest(f.supervisor, func(func() error) error {
				return errors.New("injected directory sync failure after rename")
			})
			if operation == "prepare" {
				assertFloorCommandsRejected(t, f.supervisor, f.authorities[0])
			} else if checkpoint, err := f.supervisor.InstallExecutionFloor(context.Background(), f.disposition(t)); checkpoint != nil || !errors.Is(err, modelruntime.ErrExecutionStateRecovery) {
				t.Fatalf("uncertain floor persistence returned success: %+v %v", checkpoint, err)
			}
			restore()
			if f.backend.calls.Load() != 0 {
				t.Fatal("uncertain persistence reached backend")
			}
			assertFloorCommandsRejected(t, f.supervisor, f.authority(t, 1, 100))
			f.supervisor.Close()
			state := readDurableExecutionState(t, directory)
			if operation == "prepare" && state.Highest != 10 || operation == "floor" && state.Floor != 11 {
				t.Fatalf("fault did not occur after rename: %+v", state)
			}
			// Recovery verifies and synchronizes the visible file and directory.
			recovered := durableExecutionFixture(t, directory, false, "", 10, time.Time{})
			if operation == "floor" {
				assertFloorCommandsRejected(t, recovered.supervisor, recovered.authorities[1])
			} else {
				response, err := recovered.supervisor.PrepareStage(context.Background(), &velav1.ModelRuntimeServicePrepareStageRequest{Authority: recovered.authorities[0], ExecutionSpec: runtimeExecutionSpec()})
				if err != nil || response.GetDecision() != velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_STALE {
					t.Fatalf("uncertain allocation reopened: %v %v", response, err)
				}
			}
			if operation == "prepare" {
				assertRecoveryDrainBlocks(t, recovered, recovered.authority(t, 1, 12))
			} else {
				prepareFloorRuntime(t, recovered.supervisor, recovered.authority(t, 1, 12))
			}
		})
	}
}

func TestDurableExecutionStateChecksLifetimeAfterPersistence(t *testing.T) {
	directory := privateExecutionStateDirectory(t)
	f := durableExecutionFixture(t, directory, true, "", 9, time.Time{})
	restore := modelruntime.SetExecutionStateSyncHookForTest(f.supervisor, func(syncDirectory func() error) error {
		f.clock.Advance(time.Minute)
		return syncDirectory()
	})
	response, err := f.supervisor.PrepareStage(context.Background(), &velav1.ModelRuntimeServicePrepareStageRequest{Authority: f.authorities[0], ExecutionSpec: runtimeExecutionSpec()})
	restore()
	if err != nil || response.GetDecision() != velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_STALE || f.backend.calls.Load() != 0 {
		t.Fatalf("persistence extended execution lifetime: %v %v", response, err)
	}
	if state := readDurableExecutionState(t, directory); state.Highest != 10 {
		t.Fatal("expired persisted allocation lost its consumed sequence")
	}
	f.supervisor.Close()
	recovered := durableExecutionFixture(t, directory, false, "", 9, f.clock.Now())
	response, err = recovered.supervisor.PrepareStage(context.Background(), &velav1.ModelRuntimeServicePrepareStageRequest{Authority: recovered.authorities[0], ExecutionSpec: runtimeExecutionSpec()})
	if err != nil || response.GetDecision() != velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_STALE {
		t.Fatalf("expired entry reopened on recovery: %v %v", response, err)
	}
	assertRecoveryDrainBlocks(t, recovered, recovered.authority(t, 1, 11))
}

func TestDurableExecutionStateRejectsReplacementAfterRename(t *testing.T) {
	directory := privateExecutionStateDirectory(t)
	f := durableExecutionFixture(t, directory, true, "", 9, time.Time{})
	restore := modelruntime.SetExecutionStateSyncHookForTest(f.supervisor, func(syncDirectory func() error) error {
		path := filepath.Join(directory, durableStateFileName)
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if err := os.Rename(path, path+".retained"); err != nil {
			return err
		}
		if err := os.WriteFile(path, data, 0o600); err != nil {
			return err
		}
		return syncDirectory()
	})
	response, err := f.supervisor.PrepareStage(context.Background(), &velav1.ModelRuntimeServicePrepareStageRequest{Authority: f.authorities[0], ExecutionSpec: runtimeExecutionSpec()})
	restore()
	if err != nil || response.GetDecision() != velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_STALE || f.backend.calls.Load() != 0 {
		t.Fatalf("replacement inode was adopted after rename: %v %v", response, err)
	}
	if _, err := f.supervisor.InstallExecutionFloor(context.Background(), f.disposition(t)); !errors.Is(err, modelruntime.ErrExecutionStateRecovery) {
		t.Fatalf("replacement did not close admission: %v", err)
	}
}

func TestDurableExecutionStateDetectsLiveLockIdentityChange(t *testing.T) {
	directory := privateExecutionStateDirectory(t)
	f := durableExecutionFixture(t, directory, true, "", 9, time.Time{})
	path := filepath.Join(directory, durableStateLockName)
	original, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(uuid.NewString()), 0o600); err != nil {
		t.Fatal(err)
	}
	assertFloorCommandsRejected(t, f.supervisor, f.authorities[0])
	if err := os.WriteFile(path, original, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := f.supervisor.InstallExecutionFloor(context.Background(), f.disposition(t)); !errors.Is(err, modelruntime.ErrExecutionStateRecovery) {
		t.Fatalf("restoring the lock ID cleared recovery state: %v", err)
	}
}

func TestDurableExecutionStateReclaimsOnlyOwnedUnpublishedFiles(t *testing.T) {
	directory := privateExecutionStateDirectory(t)
	f := durableExecutionFixture(t, directory, true, "", 9, time.Time{})
	f.supervisor.Close()
	state := readDurableExecutionState(t, directory)
	prefix := ".execution-admission-" + state.ID + "-"
	var orphans []string
	for range 80 {
		path := filepath.Join(directory, prefix+uuid.NewString()+".tmp")
		if err := os.WriteFile(path, []byte("partial write"), 0o600); err != nil {
			t.Fatal(err)
		}
		orphans = append(orphans, path)
	}
	preserved := filepath.Join(directory, ".execution-admission-"+uuid.NewString()+"-"+uuid.NewString()+".tmp")
	if err := os.WriteFile(preserved, []byte("another owner"), 0o600); err != nil {
		t.Fatal(err)
	}
	recovered := durableExecutionFixture(t, directory, false, "", 9, time.Time{})
	for _, path := range orphans {
		if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("unpublished journal file survived recovery: %v", err)
		}
	}
	if data, err := os.ReadFile(preserved); err != nil || string(data) != "another owner" {
		t.Fatalf("recovery removed an unrelated file: %s %v", data, err)
	}
	prepareFloorRuntime(t, recovered.supervisor, recovered.authorities[0])
}

func TestDurableExecutionStateRejectsLinkedUnpublishedFiles(t *testing.T) {
	for _, hardlink := range []bool{false, true} {
		t.Run(map[bool]string{false: "symlink", true: "hardlink"}[hardlink], func(t *testing.T) {
			directory := privateExecutionStateDirectory(t)
			f := durableExecutionFixture(t, directory, true, "", 9, time.Time{})
			f.supervisor.Close()
			state := readDurableExecutionState(t, directory)
			target := filepath.Join(t.TempDir(), "preserved")
			if err := os.WriteFile(target, []byte("retained"), 0o600); err != nil {
				t.Fatal(err)
			}
			name := filepath.Join(directory, ".execution-admission-"+state.ID+"-"+uuid.NewString()+".tmp")
			link := os.Symlink
			if hardlink {
				link = os.Link
			}
			if err := link(target, name); err != nil {
				t.Fatal(err)
			}
			if _, err := tryDurableExecutionFixture(t, directory, false); err == nil {
				t.Fatal("recovery accepted a linked unpublished file")
			}
			if data, err := os.ReadFile(target); err != nil || string(data) != "retained" {
				t.Fatalf("recovery changed a linked target: %s %v", data, err)
			}
			if _, err := os.Lstat(name); err != nil {
				t.Fatalf("recovery removed the rejected link: %v", err)
			}
		})
	}
}

func TestDurableExecutionStateSurvivesProcessExitBeforeRuntimeReturn(t *testing.T) {
	for _, mode := range []string{"prepare", "floor"} {
		t.Run(mode, func(t *testing.T) {
			directory := privateExecutionStateDirectory(t)
			executable, err := os.Executable()
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			command := exec.CommandContext(ctx, executable, "-test.run=^TestDurableExecutionStateProcessHelper$")
			command.Env = append(os.Environ(), "VELA_EXECUTION_STATE_TEST_ROOT="+directory, "VELA_EXECUTION_STATE_TEST_MODE="+mode)
			if output, err := command.CombinedOutput(); err != nil {
				t.Fatalf("process helper: %v %s", err, output)
			}
			recovered := durableExecutionFixture(t, directory, false, "", 9, time.Time{})
			if mode == "prepare" {
				response, err := recovered.supervisor.PrepareStage(context.Background(), &velav1.ModelRuntimeServicePrepareStageRequest{Authority: recovered.authorities[0], ExecutionSpec: runtimeExecutionSpec()})
				if err != nil || response.GetDecision() != velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_STALE {
					t.Fatalf("process exit reopened consumed Prepare: %v %v", response, err)
				}
			} else {
				assertFloorCommandsRejected(t, recovered.supervisor, recovered.authorities[1])
			}
			if mode == "prepare" {
				assertRecoveryDrainBlocks(t, recovered, recovered.authority(t, 1, 12))
			} else {
				prepareFloorRuntime(t, recovered.supervisor, recovered.authority(t, 1, 12))
			}
		})
	}
}

func TestDurableExecutionStateProcessHelper(t *testing.T) {
	directory := os.Getenv("VELA_EXECUTION_STATE_TEST_ROOT")
	if directory == "" {
		return
	}
	mode := os.Getenv("VELA_EXECUTION_STATE_TEST_MODE")
	f := durableExecutionFixture(t, directory, true, mode, 9, time.Time{})
	switch mode {
	case "prepare":
		go func() { _ = runFloorOperation(f, "prepare") }()
		select {
		case <-f.backend.entered:
		case <-time.After(5 * time.Second):
			t.Fatal("helper did not enter backend after persistence")
		}
	case "floor":
		checkpoint, err := f.supervisor.InstallExecutionFloor(context.Background(), f.disposition(t))
		if err != nil || !checkpoint.Durable {
			t.Fatalf("helper floor: %+v %v", checkpoint, err)
		}
	default:
		t.Fatal("invalid process helper mode")
	}
	// Abrupt process exit skips Release, Supervisor.Close and test cleanups.
	os.Exit(0)
}

func durableExecutionFixture(t *testing.T, directory string, initialize bool, blocked string, epoch int64, now time.Time) *executionFloorFixture {
	t.Helper()
	if now.IsZero() {
		now = time.Date(2026, 9, 5, 8, 0, 0, 0, time.UTC)
	}
	f, err := executionFloorFixtureWithState(t, blocked, &modelruntime.ExecutionFloorStateConfig{Directory: directory, Initialize: initialize}, epoch, now)
	if err != nil {
		t.Fatal(err)
	}
	return f
}

func privateExecutionStateDirectory(t *testing.T) string {
	t.Helper()
	directory := t.TempDir()
	if err := os.Chmod(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	return directory
}

func tryDurableExecutionFixture(t *testing.T, directory string, initialize bool) (*executionFloorFixture, error) {
	t.Helper()
	return executionFloorFixtureWithState(t, "", &modelruntime.ExecutionFloorStateConfig{Directory: directory, Initialize: initialize}, 9, time.Date(2026, 9, 5, 8, 0, 0, 0, time.UTC))
}

type durableExecutionStateDocument struct {
	SchemaVersion int             `json:"schema_version"`
	ID            string          `json:"journal_id"`
	Scope         json.RawMessage `json:"scope"`
	Root          json.RawMessage `json:"root"`
	Lock          json.RawMessage `json:"lock"`
	Highest       int64           `json:"highest"`
	Authority     []byte          `json:"highest_authority"`
	Floor         int64           `json:"floor"`
	Disposition   []byte          `json:"floor_disposition"`
	Executions    json.RawMessage `json:"executions"`
	NonAdmissions json.RawMessage `json:"non_admissions,omitempty"`
}

func readDurableExecutionState(t *testing.T, directory string) durableExecutionStateDocument {
	t.Helper()
	wire, err := os.ReadFile(filepath.Join(directory, durableStateFileName))
	if err != nil {
		t.Fatal(err)
	}
	var state durableExecutionStateDocument
	if err := json.Unmarshal(wire, &state); err != nil {
		t.Fatal(err)
	}
	return state
}

func encodeDurableExecutionState(t *testing.T, state durableExecutionStateDocument) []byte {
	t.Helper()
	wire, err := json.Marshal(state)
	if err != nil {
		t.Fatal(err)
	}
	return wire
}
