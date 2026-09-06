package stageworkeragent_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/stageauthority"
	"github.com/vivym/vela/internal/stageworkeragent"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/protobuf/proto"
)

func newAssignmentFloorFixture(t *testing.T) *floorCollectorFixture {
	t.Helper()
	f := newFloorCollectorFixture(t)
	f.admissionFixture.config.Bindings = nil
	for _, binding := range f.config.ExecutionFloor.Bindings {
		f.admissionFixture.config.Bindings = append(f.admissionFixture.config.Bindings, stageworkeragent.AdmissionRuntimeBinding(binding))
	}
	return f
}

func TestAssignmentFloorPersistsBeforeInputReleaseAndReplays(t *testing.T) {
	f := newAssignmentFloorFixture(t)
	gate := f.open(t)
	handle := beginAdmission(t, gate, f.assignment, f.acquireID)
	installation, err := gate.InstallExecutionFloor(t.Context(), f.disposition)
	if err != nil || installation.Cutoff != 7 {
		t.Fatalf("install floor: %+v %v", installation, err)
	}
	if err := handle.EnterRuntime(t.Context()); !errors.Is(err, stageworkeragent.ErrAdmissionClosed) {
		t.Fatalf("late resolver crossed floor: %v", err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	if err := installation.WaitInputWriters(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("floor installation forgot live input writer: %v", err)
	}
	if snapshot := admissionSnapshot(t, gate); snapshot.Watermark != 1 || snapshot.Floor != 7 ||
		snapshot.Latest.Phase != stageworkeragent.AssignmentInputsPending || !proto.Equal(snapshot.Disposition, f.disposition) {
		t.Fatalf("floor lost input history: %+v", snapshot)
	}
	completeAdmissionInputs(t, handle)
	if err := installation.WaitInputWriters(t.Context()); err != nil {
		t.Fatal(err)
	}
	lower := proto.Clone(f.disposition).(*velav1.StageTerminalDisposition)
	lower.Allocations, lower.Cutoff = lower.Allocations[:1], 1
	lower, err = f.signer.SignTerminalDisposition(lower)
	if err != nil {
		t.Fatal(err)
	}
	verified, err := f.admissionFixture.config.Validator.ValidateTerminalDispositionEnvelope(lower)
	if err != nil {
		t.Fatal(err)
	}
	if replay, err := gate.InstallExecutionFloor(t.Context(), lower); err != nil || replay.Cutoff != 7 || replay.DispositionDigest != verified.Digest {
		t.Fatalf("lower retry reduced floor or lost request binding: %+v %v", replay, err)
	}
	if err := gate.Close(); err != nil {
		t.Fatal(err)
	}
	f.clock.Add(int64(2 * time.Minute))
	gate = f.open(t)
	if snapshot := admissionSnapshot(t, gate); snapshot.Floor != 7 || !proto.Equal(snapshot.Disposition, f.disposition) {
		t.Fatalf("expired witness lost recovery restriction: %+v", snapshot)
	}
	for _, sequence := range []int64{1, 6, 7} {
		if handle, err := gate.Begin(t.Context(), f.next(t, sequence), uuid.New()); !errors.Is(err, stageworkeragent.ErrAdmissionClosed) {
			if handle != nil {
				handle.Release()
			}
			t.Fatalf("recovery admitted sequence %d through cutoff: %v", sequence, err)
		}
	}
	beginAdmission(t, gate, f.next(t, 8), uuid.New()).Release()
}

func TestAssignmentFloorBlocksRenewalWithoutClaimingRuntimeDrain(t *testing.T) {
	f := newAssignmentFloorFixture(t)
	gate := f.open(t)
	handle := beginAdmission(t, gate, f.assignment, f.acquireID)
	if err := handle.CompleteInputs(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := handle.EnterRuntime(t.Context()); err != nil {
		t.Fatal(err)
	}
	handle.Release()
	if _, err := gate.InstallExecutionFloor(t.Context(), f.disposition); err != nil {
		t.Fatal(err)
	}
	if err := gate.ObserveRuntimeAuthority(t.Context(), f.assignment.Authority); !errors.Is(err, stageworkeragent.ErrAdmissionClosed) {
		t.Fatalf("floor renewed an accepted Runtime: %v", err)
	}
	if snapshot := admissionSnapshot(t, gate); snapshot.Latest.Phase != stageworkeragent.AssignmentRuntimeEntered {
		t.Fatal("floor forgot the Runtime requiring recovery")
	}
	if handle, err := gate.Begin(t.Context(), f.next(t, 8), uuid.New()); !errors.Is(err, stageworkeragent.ErrAdmissionRecoveryRequired) {
		if handle != nil {
			handle.Release()
		}
		t.Fatalf("floor bypassed existing Runtime recovery: %v", err)
	}
}

func TestAssignmentFloorRejectsUntrustedHistoryWithoutChangingAdmission(t *testing.T) {
	for _, mutation := range []string{"signature", "expired", "future", "unknown", "worker", "target", "device", "missing member", "profile", "runtime epoch", "identity", "subset"} {
		t.Run(mutation, func(t *testing.T) {
			f := newAssignmentFloorFixture(t)
			gate := f.open(t)
			sign := true
			switch mutation {
			case "signature":
				f.disposition.Signature[0] ^= 1
				sign = false
			case "expired":
				f.clock.Add(int64(2 * time.Minute))
			case "future":
				f.clock.Add(-int64(time.Second))
			case "unknown":
				f.disposition.ProtoReflect().SetUnknown([]byte{0x78, 1})
				sign = false
			case "worker":
				f.disposition.WorkerInstanceEpoch++
			case "target":
				f.disposition.WorkerMemberId = f.assignment.Authority.Members[1].WorkerMemberId
			case "device":
				f.disposition.Devices[0].DeviceEpoch++
			case "missing member":
				for _, allocation := range f.disposition.Allocations {
					allocation.Members = allocation.Members[:1]
				}
			case "profile":
				f.disposition.Allocations[1].StageProfileRevisionId = uuid.NewString()
			case "runtime epoch":
				f.disposition.Allocations[1].Members[1].ModelRuntimeEpoch++
			case "identity", "subset":
				for _, allocation := range f.disposition.Allocations {
					if mutation == "identity" {
						allocation.Members[1].IdentityDigest[0] ^= 1
					} else {
						allocation.Members[1].DeviceSubsetDigest[0] ^= 1
					}
				}
			}
			if sign {
				f.signDisposition(t)
			}
			if installation, err := gate.InstallExecutionFloor(t.Context(), f.disposition); err == nil || installation != nil {
				t.Fatalf("invalid history installed floor: %+v %v", installation, err)
			}
			if snapshot := admissionSnapshot(t, gate); snapshot.Floor != 0 || snapshot.Watermark != 0 || snapshot.Disposition != nil {
				t.Fatal("rejected history changed admission")
			}
		})
	}
}

func TestAssignmentFloorRecoveryBindsWitnessAndPreservesRestrictionAcrossRuntimeChange(t *testing.T) {
	for _, mutation := range []string{"none", "expired", "runtime", "profile", "subset", "floor", "missing witness", "signature", "old schema"} {
		t.Run(mutation, func(t *testing.T) {
			f := newAssignmentFloorFixture(t)
			gate := f.open(t)
			if _, err := gate.InstallExecutionFloor(t.Context(), f.disposition); err != nil {
				t.Fatal(err)
			}
			if err := gate.Close(); err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(f.admissionFixture.config.Directory, admissionTestState)
			document, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			switch mutation {
			case "expired":
				f.clock.Add(int64(6 * time.Minute))
			case "runtime", "profile", "subset":
				for i := range f.admissionFixture.config.Bindings {
					binding := &f.admissionFixture.config.Bindings[i]
					switch mutation {
					case "runtime":
						binding.Runtime.ModelRuntimeEpoch += 100
					case "profile":
						binding.Runtime.StageProfileRevisionID = uuid.NewString()
					case "subset":
						binding.DeviceSubsetDigest[0] ^= 1
					}
				}
			case "floor":
				document = bytes.Replace(document, []byte(`"floor":7`), []byte(`"floor":6`), 1)
			case "old schema":
				document = legacyAdmissionDocument(t, document, 1)
			case "missing witness", "signature":
				encoded, err := proto.MarshalOptions{Deterministic: true}.Marshal(f.disposition)
				if err != nil {
					t.Fatal(err)
				}
				old, _ := json.Marshal(encoded)
				var replacement []byte
				if mutation == "signature" {
					f.disposition.Signature[0] ^= 1
					replacement, err = proto.MarshalOptions{Deterministic: true}.Marshal(f.disposition)
					if err != nil {
						t.Fatal(err)
					}
				}
				wire, _ := json.Marshal(replacement)
				document = bytes.Replace(document, old, wire, 1)
			}
			if err := os.WriteFile(path, document, 0o600); err != nil {
				t.Fatal(err)
			}
			recovered, err := stageworkeragent.NewFileAssignmentAdmission(f.admissionFixture.config)
			valid := mutation == "none" || mutation == "expired" || mutation == "runtime" || mutation == "profile"
			if !valid {
				if err == nil {
					_ = recovered.Close()
					t.Fatal("changed authority restored admission")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = recovered.Close() }()
			if snapshot := admissionSnapshot(t, recovered); snapshot.Floor != 7 {
				t.Fatal("recovery lowered restrictive floor")
			}
		})
	}
}

func TestAssignmentFloorFailureLeavesNoReceiptAndRequiresRecovery(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root bypasses directory write permissions")
	}
	f := newAssignmentFloorFixture(t)
	gate := f.open(t)
	path := f.admissionFixture.config.Directory
	if err := os.Chmod(path, 0o500); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(path, 0o700) })
	if installation, err := gate.InstallExecutionFloor(t.Context(), f.disposition); err == nil || installation != nil {
		t.Fatalf("failed persistence produced a receipt: %+v %v", installation, err)
	}
	if err := os.Chmod(path, 0o700); err != nil {
		t.Fatal(err)
	}
	if _, err := gate.InstallExecutionFloor(t.Context(), f.disposition); err == nil {
		t.Fatal("persistence failure was not sticky")
	}
	if err := gate.Close(); err != nil {
		t.Fatal(err)
	}
	gate = f.open(t)
	if snapshot := admissionSnapshot(t, gate); snapshot.Floor != 0 {
		t.Fatal("failed persistence advanced recovered floor")
	}
	if _, err := gate.InstallExecutionFloor(t.Context(), f.disposition); err != nil {
		t.Fatal(err)
	}
}

func TestWorkerFloorClosesInputBeforePartialRuntimeDispatch(t *testing.T) {
	f := newAssignmentFloorFixture(t)
	gate := f.open(t)
	var mu sync.Mutex
	for index, client := range f.clients {
		client.reply = func(_ context.Context, request *velav1.ModelRuntimeServiceInstallStageExecutionFloorRequest) (*velav1.ModelRuntimeServiceInstallStageExecutionFloorResponse, error) {
			mu.Lock()
			defer mu.Unlock()
			if snapshot := admissionSnapshot(t, gate); snapshot.Floor != request.Disposition.Cutoff {
				t.Error("Runtime installation preceded durable input floor")
			}
			if index == 1 && client.calls.Load() == 1 {
				return nil, errors.New("lost Runtime installation response")
			}
			return f.acknowledge(t.Context(), request)
		}
	}
	stream, err := stageworkeragent.NewDurableStreamAgent(stageworkeragent.DurableStreamConfig{
		Runtime: f.agent(t), Admission: gate, Control: &recordingStreamControl{},
	})
	if err != nil {
		t.Fatal(err)
	}
	first, err := stream.InstallExecutionFloor(t.Context(), f.disposition)
	if err == nil || first.Input == nil || first.Runtimes.AllInstalled {
		t.Fatalf("partial Runtime installation claimed completion: %+v %v", first, err)
	}
	if handle, err := gate.Begin(t.Context(), f.assignment, f.acquireID); !errors.Is(err, stageworkeragent.ErrAdmissionClosed) {
		if handle != nil {
			handle.Release()
		}
		t.Fatalf("partial Runtime failure reopened input: %v", err)
	}
	second, err := stream.InstallExecutionFloor(t.Context(), f.disposition)
	if err != nil || second.Input == nil || !second.Runtimes.AllInstalled {
		t.Fatalf("retry did not reconfirm both entry points: %+v %v", second, err)
	}
	for _, client := range f.clients {
		if client.calls.Load() != 2 {
			t.Fatal("retry reused an old Runtime acknowledgement")
		}
	}
}

func TestAssignmentFloorRechecksCancellationAfterSignatureValidation(t *testing.T) {
	f := newAssignmentFloorFixture(t)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	keys := map[string][]byte{"barrier-key": bytes.Repeat([]byte{0x6b}, 32)}
	validator, err := stageauthority.NewValidator(keys, func() time.Time {
		cancel()
		return time.Unix(0, f.clock.Load())
	})
	if err != nil {
		t.Fatal(err)
	}
	f.admissionFixture.config.Validator = validator
	gate := f.open(t)
	if installation, err := gate.InstallExecutionFloor(ctx, f.disposition); !errors.Is(err, context.Canceled) || installation != nil {
		t.Fatalf("canceled validation installed floor: %+v %v", installation, err)
	}
	if snapshot := admissionSnapshot(t, gate); snapshot.Floor != 0 {
		t.Fatal("canceled validation persisted floor")
	}
}

func TestWorkerFloorCopiesDispositionAcrossBothAdmissionSteps(t *testing.T) {
	f := newAssignmentFloorFixture(t)
	expected, err := f.admissionFixture.config.Validator.ValidateTerminalDispositionEnvelope(f.disposition)
	if err != nil {
		t.Fatal(err)
	}
	var once sync.Once
	validator, err := stageauthority.NewValidator(map[string][]byte{"barrier-key": bytes.Repeat([]byte{0x6b}, 32)}, func() time.Time {
		once.Do(func() { f.disposition.Signature[0] ^= 1 })
		return time.Unix(0, f.clock.Load())
	})
	if err != nil {
		t.Fatal(err)
	}
	f.admissionFixture.config.Validator = validator
	gate := f.open(t)
	stream, err := stageworkeragent.NewDurableStreamAgent(stageworkeragent.DurableStreamConfig{
		Runtime: f.agent(t), Admission: gate, Control: &recordingStreamControl{},
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := stream.InstallExecutionFloor(t.Context(), f.disposition)
	if err != nil || result.Input == nil || result.Input.DispositionDigest != expected.Digest ||
		!result.Runtimes.AllInstalled || result.Runtimes.DispositionDigest != expected.Digest {
		t.Fatalf("caller mutation split input and Runtime checkpoints: %+v %v", result, err)
	}
}

func TestWorkerFloorRejectsLateInputResolutionBeforeRuntimeCall(t *testing.T) {
	f := newAssignmentFloorFixture(t)
	gate := f.open(t)
	assignment := proto.Clone(f.assignment).(*velav1.StageAssignment)
	digest := sha256.Sum256([]byte("input"))
	assignment.ExecutionSpec.RootInputs = []*velav1.StageRootInputMaterial{{
		ConditionIndex: 0, Uri: "vela://uploads/reference", Sha256: digest[:], SizeBytes: 5,
	}}
	assignment.RootInputFetches = []*velav1.StageRootInputFetch{{ConditionIndex: 0, Sha256: digest[:], DownloadUrl: "https://example.test/input"}}
	executionDigest, err := stageauthority.ExecutionSpecDigest(assignment.ExecutionSpec)
	if err != nil {
		t.Fatal(err)
	}
	assignment.Authority.ExecutionSpecDigest = executionDigest[:]
	f.sign(t, assignment)
	originalDigest, err := stageauthority.Digest(assignment.Authority)
	if err != nil {
		t.Fatal(err)
	}
	f.disposition.OriginalAuthorityDigest = originalDigest[:]
	f.signDisposition(t)
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	t.Cleanup(unblock)
	resolver := inputResolverFunc(func(context.Context, *velav1.StageAssignment) error {
		close(entered)
		<-release
		return nil
	})
	var prepares atomic.Int32
	for _, client := range f.clients {
		client.ModelRuntimeServiceClient = &admissionRuntimeProbe{beforePrepare: func(*velav1.StageAuthority) error {
			prepares.Add(1)
			return errors.New("late resolver reached Runtime")
		}}
	}
	stream, err := stageworkeragent.NewDurableStreamAgent(stageworkeragent.DurableStreamConfig{
		Runtime: f.agent(t), Admission: gate, Control: &recordingStreamControl{}, InputResolver: resolver,
	})
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		_, err := stream.ExecuteAcquiredAssignment(t.Context(), assignment, f.acquireID)
		done <- err
	}()
	select {
	case <-entered:
	case err := <-done:
		t.Fatalf("assignment stopped before resolving input: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("input resolver did not start")
	}
	installed, err := stream.InstallExecutionFloor(t.Context(), f.disposition)
	if err != nil || installed.Input == nil || !installed.Runtimes.AllInstalled {
		unblock()
		<-done
		t.Fatalf("install while resolving: %+v %v", installed, err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	if err := installed.Input.WaitInputWriters(ctx); !errors.Is(err, context.DeadlineExceeded) {
		unblock()
		<-done
		t.Fatalf("reported released while resolver was still running: %v", err)
	}
	unblock()
	if err := <-done; !errors.Is(err, stageworkeragent.ErrAdmissionClosed) || prepares.Load() != 0 {
		t.Fatalf("late input resolution reopened Runtime admission: %v, calls=%d", err, prepares.Load())
	}
	if err := installed.Input.WaitInputWriters(t.Context()); err != nil {
		t.Fatal(err)
	}
}
