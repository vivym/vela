package modelruntime_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/vivym/vela/internal/modelruntime"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/protobuf/proto"
)

type directJournalTransport struct {
	owner       *modelruntime.ExecutionJournalOwner
	identity    modelruntime.ExecutionJournalIdentity
	role        modelruntime.JournalCallerRole
	mu          sync.Mutex
	fault       string
	mutations   int
	frozen      *modelruntime.JournalDocument
	readChanges int
	beforeRead  func(context.Context) error
	afterApply  func(context.Context) error
}

func (transport *directJournalTransport) Apply(ctx context.Context, command modelruntime.JournalCommand) (modelruntime.JournalMutationReceipt, error) {
	transport.mu.Lock()
	defer transport.mu.Unlock()
	transport.mutations++
	if transport.fault == "unavailable" {
		return modelruntime.JournalMutationReceipt{}, errors.New("owner unavailable")
	}
	wire, err := modelruntime.EncodeJournalCommand(command)
	if err != nil {
		return modelruntime.JournalMutationReceipt{}, err
	}
	receipt, err := transport.owner.Apply(ctx, transport.role, wire)
	if err == nil && transport.afterApply != nil {
		err = transport.afterApply(ctx)
	}
	if err == nil && transport.fault == "lost-reply" {
		return modelruntime.JournalMutationReceipt{}, errors.New("lost durable reply")
	}
	if transport.fault == "replayed" {
		receipt.Replayed = true
	}
	if transport.fault == "foreign-receipt" {
		receipt.JournalID[0]++
	}
	return receipt, err
}
func (transport *directJournalTransport) Read(ctx context.Context) (modelruntime.JournalDocument, error) {
	if transport.beforeRead != nil {
		if err := transport.beforeRead(ctx); err != nil {
			return modelruntime.JournalDocument{}, err
		}
	}
	transport.mu.Lock()
	if transport.readChanges > 0 {
		transport.readChanges--
		transport.mu.Unlock()
		return modelruntime.JournalDocument{}, modelruntime.ErrJournalChanged
	}
	frozen := transport.frozen
	transport.mu.Unlock()
	if frozen != nil {
		return *frozen, nil
	}
	return modelruntime.ReadJournalDocument(ctx, transport.identity, transport.owner.Read)
}

func TestJournalRemoteReadContentionDoesNotPoisonAdmission(t *testing.T) {
	for _, changes := range []int{2, 3} {
		t.Run(fmt.Sprint(changes), func(t *testing.T) {
			f, _, transport, _ := remoteSupervisorFixture(t)
			transport.readChanges = changes
			request := &velav1.ModelRuntimeServicePrepareStageRequest{Authority: f.authorities[0], ExecutionSpec: runtimeExecutionSpec()}
			response, err := f.supervisor.PrepareStage(t.Context(), request)
			if err != nil {
				t.Fatal(err)
			}
			if changes == 3 {
				if response.GetDecision() == velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_ACCEPTED || transport.mutations != 0 || f.backend.calls.Load() != 0 {
					t.Fatal("exhausted reads entered backend or mutated journal")
				}
				response, err = f.supervisor.PrepareStage(t.Context(), request)
			}
			if err != nil || response.GetDecision() != velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_ACCEPTED || f.backend.calls.Load() != 1 {
				t.Fatalf("read-only contention poisoned legal admission: %v %v", response, err)
			}
		})
	}
}

func TestJournalRemoteReadUnavailableDoesNotWeakenMutationFence(t *testing.T) {
	for _, phase := range []string{"read", "write-reply", "readback"} {
		t.Run(phase, func(t *testing.T) {
			f, owner, transport, _ := remoteSupervisorFixture(t)
			unavailable := errors.Join(modelruntime.ErrJournalReadUnavailable, io.ErrUnexpectedEOF)
			reads := 0
			transport.beforeRead = func(context.Context) error {
				reads++
				if phase == "read" || phase == "readback" && transport.mutations != 0 {
					return unavailable
				}
				return nil
			}
			if phase == "write-reply" {
				transport.afterApply = func(context.Context) error { return unavailable }
			}
			request := &velav1.ModelRuntimeServicePrepareStageRequest{Authority: f.authorities[0], ExecutionSpec: runtimeExecutionSpec()}
			response, err := f.supervisor.PrepareStage(t.Context(), request)
			if err != nil || response.GetDecision() == velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_ACCEPTED || f.backend.calls.Load() != 0 {
				t.Fatalf("unavailable exchange entered backend: %v %v", response, err)
			}
			status, err := owner.Status(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			if phase == "read" {
				if reads != 1 || transport.mutations != 0 || status.Highest != 0 {
					t.Fatalf("pure read retried internally or mutated: reads=%d writes=%d highest=%d", reads, transport.mutations, status.Highest)
				}
			} else if transport.mutations != 1 || status.Highest != 10 {
				t.Fatalf("uncertain write lost durable sequence: %+v writes=%d", status, transport.mutations)
			}
			transport.beforeRead, transport.afterApply = nil, nil
			response, err = f.supervisor.PrepareStage(t.Context(), request)
			if phase == "read" {
				if err != nil || response.GetDecision() != velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_ACCEPTED || f.backend.calls.Load() != 1 {
					t.Fatalf("pure read failure poisoned legal retry: %v %v", response, err)
				}
			} else if err != nil || !strings.Contains(response.GetDetail(), modelruntime.ErrExecutionStateRecovery.Error()) || transport.mutations != 1 || f.backend.calls.Load() != 0 {
				t.Fatalf("read-unavailable marker bypassed mutation fence: %v %v", response, err)
			}
		})
	}
}

func TestJournalRemoteReadIntegrityFailuresRemainFenced(t *testing.T) {
	for _, fault := range []struct {
		name   string
		err    error
		cancel bool
	}{
		{"invalid", modelruntime.ErrJournalCommand, false},
		{"invalid-and-canceled", modelruntime.ErrJournalCommand, true},
		{"uncertain", modelruntime.ErrExecutionStateRecovery, false},
		{"uncertain-and-canceled", modelruntime.ErrExecutionStateRecovery, true},
		{"unavailable-and-invalid", errors.Join(modelruntime.ErrJournalReadUnavailable, modelruntime.ErrJournalCommand), false},
		{"unavailable-and-uncertain", errors.Join(modelruntime.ErrJournalReadUnavailable, modelruntime.ErrExecutionStateRecovery), false},
		{"changed-and-uncertain", errors.Join(modelruntime.ErrJournalChanged, modelruntime.ErrExecutionStateRecovery), false},
	} {
		t.Run(fault.name, func(t *testing.T) {
			f, _, transport, _ := remoteSupervisorFixture(t)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			reads := 0
			transport.beforeRead = func(context.Context) error {
				reads++
				if fault.cancel {
					cancel()
				}
				return fault.err
			}
			request := &velav1.ModelRuntimeServicePrepareStageRequest{Authority: f.authorities[0], ExecutionSpec: runtimeExecutionSpec()}
			response, err := f.supervisor.PrepareStage(ctx, request)
			if err != nil || response.GetDecision() == velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_ACCEPTED || reads != 1 {
				t.Fatalf("fatal read was accepted or retried: %v %v reads=%d", response, err, reads)
			}
			transport.beforeRead = nil
			response, err = f.supervisor.PrepareStage(t.Context(), request)
			if err != nil || !strings.Contains(response.GetDetail(), modelruntime.ErrExecutionStateRecovery.Error()) || transport.mutations != 0 || f.backend.calls.Load() != 0 {
				t.Fatalf("integrity/recovery error was downgraded: %v %v", response, err)
			}
		})
	}
}

func TestJournalRemoteReadRetryObservesNewWorkerFloor(t *testing.T) {
	f, owner, transport, _ := remoteSupervisorFixture(t)
	prepareFloorRuntime(t, f.supervisor, f.authorities[0])
	beforeCalls, beforeWrites := f.backend.calls.Load(), transport.mutations
	transport.beforeRead = func(context.Context) error {
		return errors.Join(modelruntime.ErrJournalReadUnavailable, io.ErrUnexpectedEOF)
	}
	request := &velav1.ModelRuntimeServiceStartStageRequest{Authority: f.authorities[0]}
	failed, err := f.supervisor.StartStage(t.Context(), request)
	if err != nil || failed.GetDecision() == velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_ACCEPTED {
		t.Fatalf("unavailable read started execution: %v %v", failed, err)
	}
	applyOwnerCommand(t, owner, modelruntime.JournalWorkerRole, modelruntime.JournalCommand{Floor: &modelruntime.JournalFloorCommand{Disposition: journalProto(t, f.disposition(t))}})
	transport.beforeRead = nil
	retry, err := f.supervisor.StartStage(t.Context(), request)
	if err != nil || retry.GetDecision() != velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_STALE || !strings.Contains(retry.GetDetail(), "admission floor") ||
		f.backend.calls.Load() != beforeCalls || transport.mutations != beforeWrites {
		t.Fatalf("retry used cached state or retained a transport fence instead of reading the new floor: %v %v", retry, err)
	}
}

func remoteSupervisorFixture(t *testing.T) (*executionFloorFixture, *modelruntime.ExecutionJournalOwner, *directJournalTransport, string) {
	t.Helper()
	return remoteSupervisorContextFixture(t, t.Context(), time.Second)
}

func remoteSupervisorContextFixture(t *testing.T, lifetime context.Context, timeout time.Duration) (*executionFloorFixture, *modelruntime.ExecutionJournalOwner, *directJournalTransport, string) {
	t.Helper()
	f, owner, config := journalOwnerFixture(t)
	startup, err := owner.RecordBackendStartupIntent(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	status, err := owner.Status(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	transport := &directJournalTransport{owner: owner, identity: modelruntime.ExecutionJournalIdentity{JournalID: status.JournalID, Scope: status.Scope, Storage: status.Storage}, role: modelruntime.JournalRuntimeRole}
	f.backend = &floorBlockingBackend{FakeRuntime: modelruntime.NewFakeDiTRuntime(), entered: make(chan struct{}), resume: make(chan struct{})}
	f.otherBackend = modelruntime.NewFakeVAERuntime()
	f.services = []*modelruntime.Service{newRuntimeService(t, f.clock, f.validator, f.bindings[0], f.backend), newRuntimeService(t, f.clock, f.validator, f.bindings[1], f.otherBackend)}
	f.supervisor, err = modelruntime.NewSupervisorWithRemoteExecutionJournal(lifetime, modelruntime.RemoteExecutionJournalConfig{
		Manifest: config.Manifest, Validator: f.validator, Identity: transport.identity, Startup: startup, Transport: transport, Timeout: timeout}, f.services...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(f.supervisor.Close)
	return f, owner, transport, config.State.Directory
}

func TestJournalRemoteCancellationBoundsAdmission(t *testing.T) {
	for _, phase := range []string{"read", "write-reply", "readback", "lifetime"} {
		t.Run(phase, func(t *testing.T) {
			lifetime, stop := context.WithCancel(t.Context())
			defer stop()
			f, owner, transport, _ := remoteSupervisorContextFixture(t, lifetime, 30*time.Second)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			entered := make(chan struct{})
			block := func(ctx context.Context) error {
				close(entered)
				<-ctx.Done()
				return ctx.Err()
			}
			switch phase {
			case "read", "lifetime":
				transport.beforeRead = block
			case "write-reply":
				transport.afterApply = block
			case "readback":
				transport.beforeRead = func(ctx context.Context) error {
					if transport.mutations != 0 {
						return block(ctx)
					}
					return nil
				}
			}
			request := &velav1.ModelRuntimeServicePrepareStageRequest{Authority: f.authorities[0], ExecutionSpec: runtimeExecutionSpec()}
			type outcome struct {
				response *velav1.ModelRuntimeServicePrepareStageResponse
				err      error
			}
			done := make(chan outcome, 1)
			go func() {
				response, err := f.supervisor.PrepareStage(ctx, request)
				done <- outcome{response, err}
			}()
			select {
			case <-entered:
			case <-time.After(5 * time.Second):
				t.Fatal("journal phase not entered")
			}
			if phase == "lifetime" {
				stop()
			} else {
				cancel()
			}
			select {
			case result := <-done:
				if result.err != nil || result.response.GetDecision() == velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_ACCEPTED || !strings.Contains(result.response.GetDetail(), context.Canceled.Error()) {
					t.Fatalf("cancellation lost: %v %v", result.response, result.err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("cancellation waited for the 30-second journal timeout")
			}
			transport.beforeRead, transport.afterApply = nil, nil
			status, err := owner.Status(t.Context())
			written := phase == "write-reply" || phase == "readback"
			if err != nil || written && status.Highest != 10 || !written && status.Highest != 0 || f.backend.calls.Load() != 0 {
				t.Fatalf("wrong cancellation boundary: %+v %v backend=%d", status, err, f.backend.calls.Load())
			}
			before := transport.mutations
			response, err := f.supervisor.PrepareStage(t.Context(), request)
			if phase == "read" {
				if err != nil || response.GetDecision() != velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_ACCEPTED || f.backend.calls.Load() != 1 {
					t.Fatalf("pure read cancellation poisoned admission: %v %v", response, err)
				}
			} else if err != nil || !strings.Contains(response.GetDetail(), modelruntime.ErrExecutionStateRecovery.Error()) || transport.mutations != before || f.backend.calls.Load() != 0 {
				t.Fatalf("uncertainty or closed lifetime allowed retry: %v %v", response, err)
			}
		})
	}
}

func TestJournalRemoteWriteReadbackSharesDeadline(t *testing.T) {
	f, _, transport, _ := remoteSupervisorContextFixture(t, t.Context(), 20*time.Second)
	var writeDeadline time.Time
	transport.afterApply = func(ctx context.Context) error {
		writeDeadline, _ = ctx.Deadline()
		return nil
	}
	transport.beforeRead = func(ctx context.Context) error {
		if !writeDeadline.IsZero() {
			deadline, ok := ctx.Deadline()
			if !ok || !deadline.Equal(writeDeadline) {
				t.Errorf("readback reset write budget: %v -> %v", writeDeadline, deadline)
			}
			writeDeadline = time.Time{}
		}
		return nil
	}
	prepareFloorRuntime(t, f.supervisor, f.authorities[0])
	if transport.mutations == 0 || !writeDeadline.IsZero() {
		t.Fatal("writes did not complete readback")
	}
}

func TestJournalRemoteReadTimeoutCanRetry(t *testing.T) {
	// The timeout is intentional here. Other cancellation tests signal entry
	// before canceling and have a much longer owner budget than their hang guard.
	f, _, transport, _ := remoteSupervisorContextFixture(t, t.Context(), time.Second)
	transport.beforeRead = func(ctx context.Context) error {
		<-ctx.Done()
		return ctx.Err()
	}
	request := &velav1.ModelRuntimeServicePrepareStageRequest{Authority: f.authorities[0], ExecutionSpec: runtimeExecutionSpec()}
	response, err := f.supervisor.PrepareStage(t.Context(), request)
	if err != nil || !strings.Contains(response.GetDetail(), context.DeadlineExceeded.Error()) || transport.mutations != 0 || f.backend.calls.Load() != 0 {
		t.Fatalf("pure read timeout crossed admission: %v %v", response, err)
	}
	transport.beforeRead = nil
	prepareFloorRuntime(t, f.supervisor, f.authorities[0])
}

func TestJournalRemoteSupervisorExecutesAndReplaysSealedHistory(t *testing.T) {
	f, owner, transport, directory := remoteSupervisorFixture(t)
	readyDrainOutput(t, f, f.backend.FakeRuntime, f.authorities[0])
	response, err := f.supervisor.SealOutput(t.Context(), &velav1.ModelRuntimeServiceSealOutputRequest{Authority: f.authorities[0]})
	if err != nil || response.GetReceipt() == nil {
		t.Fatalf("remote-backed seal: %v %v", response, err)
	}
	if records := readSealedHistory(t, directory); len(records) != 1 || records[0].Seal == nil || records[0].Drain == nil {
		t.Fatal("backend result acknowledged before durable seal/drain")
	}
	before, err := os.ReadFile(filepath.Join(directory, durableStateFileName))
	if err != nil {
		t.Fatal(err)
	}
	client, _ := serveRuntimeServer(t, f.supervisor)
	again, err := client.SealOutput(t.Context(), &velav1.ModelRuntimeServiceSealOutputRequest{Authority: f.authorities[0]})
	if err != nil || !proto.Equal(response.GetReceipt(), again.GetReceipt()) {
		t.Fatalf("remote RPC receipt replay: %v %v", again, err)
	}
	after, err := os.ReadFile(filepath.Join(directory, durableStateFileName))
	if err != nil || !bytes.Equal(before, after) || transport.mutations < 5 {
		t.Fatal("receipt replay changed journal or bypassed owner")
	}
	if err := f.supervisor.Shutdown(); err != nil {
		t.Fatal(err)
	}
	if _, err := owner.Status(t.Context()); err != nil {
		t.Fatalf("Runtime shutdown closed Node journal: %v", err)
	}
}

func TestJournalRemoteUncertainAdmissionNeverEntersBackend(t *testing.T) {
	for _, fault := range []string{"unavailable", "lost-reply", "replayed", "foreign-receipt", "stale-read"} {
		t.Run(fault, func(t *testing.T) {
			f, owner, transport, _ := remoteSupervisorFixture(t)
			transport.fault = fault
			if fault == "stale-read" {
				frozen, err := transport.Read(t.Context())
				if err != nil {
					t.Fatal(err)
				}
				transport.frozen = &frozen
			}
			request := &velav1.ModelRuntimeServicePrepareStageRequest{Authority: f.authorities[0], ExecutionSpec: runtimeExecutionSpec()}
			response, err := f.supervisor.PrepareStage(t.Context(), request)
			if err != nil || response.GetDecision() == velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_ACCEPTED || f.backend.calls.Load() != 0 {
				t.Fatalf("uncertain admission entered backend: %v %v", response, err)
			}
			transport.fault = ""
			response, err = f.supervisor.PrepareStage(t.Context(), request)
			if err != nil || response.GetDecision() == velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_ACCEPTED || f.backend.calls.Load() != 0 || transport.mutations != 1 {
				t.Fatalf("uncertainty automatically redispatched: %v %v mutations=%d", response, err, transport.mutations)
			}
			status, err := owner.Status(t.Context())
			if err != nil || (fault != "unavailable" && status.Highest != 10) || (fault == "unavailable" && status.Highest != 0) {
				t.Fatalf("wrong durable outcome: %+v %v", status, err)
			}
		})
	}
}

func TestJournalRemoteFloorWaitsForAlreadyAdmittedOperation(t *testing.T) {
	f, owner, _, _ := remoteSupervisorFixture(t)
	f.backend.blocked = "prepare"
	t.Cleanup(f.backend.unblock)
	done := make(chan error, 1)
	go func() {
		_, err := f.supervisor.PrepareStage(t.Context(), &velav1.ModelRuntimeServicePrepareStageRequest{Authority: f.authorities[0], ExecutionSpec: runtimeExecutionSpec()})
		done <- err
	}()
	select {
	case <-f.backend.entered:
	case <-time.After(3 * time.Second):
		t.Fatal("Prepare did not enter backend after remote persistence")
	}
	disposition := f.disposition(t)
	applyOwnerCommand(t, owner, modelruntime.JournalWorkerRole, modelruntime.JournalCommand{Floor: &modelruntime.JournalFloorCommand{Disposition: journalProto(t, disposition)}})
	installation, err := f.supervisor.InstallExecutionFloor(t.Context(), disposition)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	if err := installation.WaitAcceptedOperations(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("remote floor forgot in-flight operation: %v", err)
	}
	f.backend.unblock()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if err := installation.WaitAcceptedOperations(t.Context()); err != nil {
		t.Fatal(err)
	}
	started, err := f.supervisor.StartStage(t.Context(), &velav1.ModelRuntimeServiceStartStageRequest{Authority: f.authorities[0]})
	if err != nil || started.GetDecision() != velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_STALE {
		t.Fatalf("new call crossed installed floor: %v %v", started, err)
	}
}

func TestJournalRemoteWorkerFloorAndNonAdmissionUseSeparateRole(t *testing.T) {
	f, owner, transport, _ := remoteSupervisorFixture(t)
	client, _ := serveRuntimeServer(t, f.supervisor)
	worker := &directJournalTransport{owner: owner, identity: transport.identity, role: modelruntime.JournalWorkerRole}
	wrapped, err := modelruntime.NewJournalWorkerClient(client, worker)
	if err != nil {
		t.Fatal(err)
	}
	disposition := unsignedTerminalAllocation(t, f)
	request := &velav1.ModelRuntimeServiceInstallStageExecutionFloorRequest{SchemaVersion: 1, Identity: inspectionIdentity(f.authorities[0]), Disposition: disposition}
	// A direct Runtime request cannot write as Worker, and refusal does not poison it.
	if result, err := client.InstallStageExecutionFloor(t.Context(), request); err != nil || result.GetDurable() {
		t.Fatalf("Runtime borrowed Worker role: %v %v", result, err)
	}
	installed, err := wrapped.InstallStageExecutionFloor(t.Context(), request)
	if err != nil || !installed.GetDurable() || installed.GetInstalledCutoff() != 11 {
		t.Fatalf("Worker durable floor: %v %v", installed, err)
	}
	proof, err := wrapped.CheckpointStageNonAdmission(t.Context(), &velav1.ModelRuntimeServiceCheckpointStageNonAdmissionRequest{
		Scope: &velav1.ModelRuntimeExecutionDrainScope{SchemaVersion: 1, Identity: inspectionIdentity(f.authorities[0]), Authority: f.authorities[0]}})
	if err != nil || proof.GetResult().GetCheckpoint() == nil {
		t.Fatalf("Runtime could not inspect Worker proof: %+v %v", proof, err)
	}
	terminal, err := wrapped.CheckpointStageTerminalNonAdmission(t.Context(), &velav1.ModelRuntimeServiceCheckpointStageTerminalNonAdmissionRequest{
		Scope: &velav1.ModelRuntimeTerminalAllocationScope{SchemaVersion: 1, Identity: inspectionIdentity(f.authorities[1]), Disposition: disposition, StageAllocationId: disposition.Allocations[1].StageAllocationId}})
	if err != nil || terminal.GetResult().GetCheckpoint() == nil {
		t.Fatalf("Runtime could not inspect terminal Worker proof: %+v %v", terminal, err)
	}
	assertFloorCommandsRejected(t, f.supervisor, f.authorities[0])
	if transport.mutations != 0 || worker.mutations != 3 || f.backend.calls.Load() != 0 {
		t.Fatal("role separation or floor fencing failed")
	}
}

func TestJournalReadRejectsMixedPagesAndPreservesSnapshot(t *testing.T) {
	f, owner, config := journalOwnerFixture(t)
	if _, err := owner.RecordBackendStartupIntent(t.Context()); err != nil {
		t.Fatal(err)
	}
	for i := range 8 {
		applyOwnerCommand(t, owner, modelruntime.JournalRuntimeRole, modelruntime.JournalCommand{Admit: &modelruntime.JournalAuthorityCommand{Authority: journalProto(t, f.authority(t, 0, int64(i+10)))}})
	}
	status, err := owner.Status(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	identity := modelruntime.ExecutionJournalIdentity{JournalID: status.JournalID, Scope: status.Scope, Storage: status.Storage}
	first, err := owner.Read(t.Context(), modelruntime.JournalReadCommand{})
	if err != nil || first.TotalBytes <= modelruntime.JournalPageBytes {
		t.Fatalf("fixture must span multiple pages: %+v %v", first, err)
	}
	document, err := modelruntime.ReadJournalDocument(t.Context(), identity, owner.Read)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := modelruntime.VerifyExecutionJournalSnapshot(document.Document, document.LockDocument, config.Manifest, f.validator, identity); err != nil {
		t.Fatal(err)
	}
	calls := 0
	_, err = modelruntime.ReadJournalDocument(t.Context(), identity, func(ctx context.Context, request modelruntime.JournalReadCommand) (modelruntime.JournalPage, error) {
		calls++
		if calls == 2 {
			applyOwnerCommand(t, owner, modelruntime.JournalRuntimeRole, modelruntime.JournalCommand{Admit: &modelruntime.JournalAuthorityCommand{Authority: journalProto(t, f.authority(t, 0, 30))}})
		}
		return owner.Read(ctx, request)
	})
	if !errors.Is(err, modelruntime.ErrJournalChanged) {
		t.Fatalf("mixed versions formed snapshot: %v", err)
	}
	for _, fault := range []string{"offset", "digest", "size", "lock", "journal", "bytes"} {
		t.Run(fault, func(t *testing.T) {
			result, err := modelruntime.ReadJournalDocument(t.Context(), identity, func(ctx context.Context, request modelruntime.JournalReadCommand) (modelruntime.JournalPage, error) {
				page, err := owner.Read(ctx, request)
				if err != nil {
					return page, err
				}
				switch fault {
				case "offset":
					page.Offset++
				case "digest":
					page.StateDigest[0]++
				case "size":
					page.TotalBytes = 1 << 30
				case "lock":
					page.LockDocument = []byte("wrong")
				case "journal":
					page.JournalID[0]++
				case "bytes":
					page.Document[0] ^= 1
				}
				return page, nil
			})
			if err == nil || len(result.Document) != 0 {
				t.Fatalf("invalid page produced usable partial result: %v", err)
			}
		})
	}
}
