package stageworkeragent_test

import (
	"bytes"
	"context"
	"crypto/sha256"
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
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const admissionTestState = "assignment-admission.json"

type admissionFixture struct {
	config     stageworkeragent.AssignmentAdmissionConfig
	signer     *stageauthority.Signer
	clock      *atomic.Int64
	assignment *velav1.StageAssignment
	acquireID  uuid.UUID
}

func newAdmissionFixture(t *testing.T) admissionFixture {
	t.Helper()
	base := t.TempDir()
	for _, name := range []string{"state", "inputs", "outputs"} {
		if err := os.Mkdir(filepath.Join(base, name), 0o700); err != nil {
			t.Fatal(err)
		}
	}
	fixture := admissionFixtureAt(t, base)
	fixture.config.Initialize = true
	return fixture
}

func admissionFixtureAt(t *testing.T, base string) admissionFixture {
	t.Helper()
	now := time.Date(2026, 9, 5, 12, 0, 0, 0, time.UTC)
	clock := &atomic.Int64{}
	clock.Store(now.UnixNano())
	keys := map[string][]byte{"barrier-key": bytes.Repeat([]byte{0x6b}, 32)}
	signer, err := stageauthority.NewSigner(keys)
	if err != nil {
		t.Fatal(err)
	}
	validator, err := stageauthority.NewValidator(keys, func() time.Time { return time.Unix(0, clock.Load()) })
	if err != nil {
		t.Fatal(err)
	}
	members := []string{"42000000-0000-0000-0000-000000000001", "42000000-0000-0000-0000-000000000002"}
	spec := &velav1.StageExecutionSpec{ParametersJson: []byte(`{"prompt":"admission-customer-content-sentinel"}`)}
	authority, err := signer.Sign(barrierAuthority(now, members, spec))
	if err != nil {
		t.Fatal(err)
	}
	config := stageworkeragent.AssignmentAdmissionConfig{
		Directory: filepath.Join(base, "state"), InputRoot: filepath.Join(base, "inputs"), OutputRoot: filepath.Join(base, "outputs"),
		WorkerInstanceID: uuid.MustParse(authority.GetWorkerInstanceId()), WorkerInstanceEpoch: authority.GetWorkerInstanceEpoch(),
		WorkerMemberID: uuid.MustParse(members[0]), Validator: validator, MaxRecords: 4,
	}
	for index, member := range members {
		binding := barrierBinding(members, member)
		binding.ModelRuntimeEpoch = 9
		config.Bindings = append(config.Bindings, stageworkeragent.AdmissionRuntimeBinding{
			Runtime: binding, IdentityDigest: [sha256.Size]byte(bytes.Repeat([]byte{byte(0x75 + index)}, 32)),
			DeviceSubsetDigest: [sha256.Size]byte(bytes.Repeat([]byte{byte(0x30 + index)}, 32)),
		})
	}
	return admissionFixture{
		config: config, signer: signer, clock: clock,
		assignment: &velav1.StageAssignment{Authority: authority, ExecutionSpec: spec, RequiredWorkerMemberIds: members, MemberStartTimeout: durationpb.New(time.Second)},
		acquireID:  uuid.MustParse("72000000-0000-0000-0000-000000000001"),
	}
}

func (fixture *admissionFixture) open(t *testing.T) *stageworkeragent.FileAssignmentAdmission {
	t.Helper()
	gate, err := stageworkeragent.NewFileAssignmentAdmission(fixture.config)
	if err != nil {
		t.Fatal(err)
	}
	fixture.config.Initialize = false
	t.Cleanup(func() {
		if err := gate.Close(); err != nil {
			t.Error(err)
		}
	})
	return gate
}

func (fixture admissionFixture) sign(t *testing.T, assignment *velav1.StageAssignment) {
	t.Helper()
	authority, err := fixture.signer.Sign(assignment.Authority)
	if err != nil {
		t.Fatal(err)
	}
	assignment.Authority = authority
}

func (fixture admissionFixture) next(t *testing.T, sequence int64) *velav1.StageAssignment {
	t.Helper()
	assignment := proto.Clone(fixture.assignment).(*velav1.StageAssignment)
	assignment.Authority.ExecutionSequence = sequence
	assignment.Authority.StageAllocationId = uuid.NewString()
	assignment.Authority.StageAttemptId = uuid.NewString()
	assignment.Authority.StageLeaseId = uuid.NewString()
	fixture.sign(t, assignment)
	return assignment
}

func beginAdmission(t *testing.T, gate *stageworkeragent.FileAssignmentAdmission, assignment *velav1.StageAssignment, acquireID uuid.UUID) *stageworkeragent.AssignmentAdmission {
	t.Helper()
	handle, err := gate.Begin(t.Context(), assignment, acquireID)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(handle.Release)
	return handle
}

func admissionSnapshot(t *testing.T, gate *stageworkeragent.FileAssignmentAdmission) stageworkeragent.AssignmentAdmissionSnapshot {
	t.Helper()
	snapshot, err := gate.Snapshot(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	return snapshot
}

func TestAssignmentAdmissionRejectsInvalidWithoutConsumingWatermark(t *testing.T) {
	for _, name := range []string{"signature", "spec", "member-set", "identity-digest", "runtime-epoch", "profile", "worker", "expired", "future", "v1", "acquire-id"} {
		t.Run(name, func(t *testing.T) {
			fixture := newAdmissionFixture(t)
			gate := fixture.open(t)
			assignment := proto.Clone(fixture.assignment).(*velav1.StageAssignment)
			acquireID := fixture.acquireID
			sign := false
			switch name {
			case "signature":
				assignment.Authority.Signature[0] ^= 1
			case "spec":
				assignment.ExecutionSpec.ParametersJson = []byte(`{"changed":true}`)
			case "member-set":
				assignment.RequiredWorkerMemberIds = assignment.RequiredWorkerMemberIds[:1]
			case "identity-digest":
				assignment.Authority.Members[1].IdentityDigest[0] ^= 1
				sign = true
			case "runtime-epoch":
				assignment.Authority.Members[1].ModelRuntimeEpoch++
				sign = true
			case "profile":
				assignment.Authority.StageProfileRevisionId = uuid.NewString()
				sign = true
			case "worker":
				assignment.Authority.WorkerInstanceId = uuid.NewString()
				sign = true
			case "expired":
				fixture.clock.Add(int64(6 * time.Minute))
			case "future":
				fixture.clock.Add(-int64(time.Second))
			case "v1":
				assignment.Authority.SchemaVersion = 1
				assignment.Authority.ExecutionSequence = 0
				sign = true
			case "acquire-id":
				acquireID = uuid.Nil
			}
			if sign {
				fixture.sign(t, assignment)
			}
			if handle, err := gate.Begin(t.Context(), assignment, acquireID); err == nil {
				handle.Release()
				t.Fatal("invalid assignment was admitted")
			}
			fixture.clock.Store(fixture.assignment.Authority.IssuedAt.AsTime().UnixNano())
			if snapshot := admissionSnapshot(t, gate); snapshot.Watermark != 0 || snapshot.Latest != nil || len(snapshot.Pending) != 0 {
				t.Fatalf("invalid assignment consumed history: %#v", snapshot)
			}
			beginAdmission(t, gate, fixture.assignment, fixture.acquireID).Release()
		})
	}
}

func TestAssignmentAdmissionInputRetryAndRenewalSurviveReopen(t *testing.T) {
	fixture := newAdmissionFixture(t)
	gate := fixture.open(t)
	completeAdmissionInputs(t, beginAdmission(t, gate, fixture.assignment, fixture.acquireID))
	if err := gate.Close(); err != nil {
		t.Fatal(err)
	}
	gate = fixture.open(t)
	if handle, err := gate.Begin(t.Context(), fixture.assignment, uuid.New()); !errors.Is(err, stageworkeragent.ErrAdmissionClosed) {
		if handle != nil {
			handle.Release()
		}
		t.Fatalf("different Acquire identity accepted: %v", err)
	}
	completeAdmissionInputs(t, beginAdmission(t, gate, fixture.assignment, fixture.acquireID))
	renewal := proto.Clone(fixture.assignment).(*velav1.StageAssignment)
	renewal.Authority.StageVersion++
	renewal.Authority.IssuedAt = timestamppb.New(renewal.Authority.IssuedAt.AsTime().Add(time.Second))
	renewal.Authority.ExpiresAt = timestamppb.New(renewal.Authority.ExpiresAt.AsTime().Add(time.Second))
	fixture.clock.Add(int64(time.Second))
	fixture.sign(t, renewal)
	completeAdmissionInputs(t, beginAdmission(t, gate, renewal, fixture.acquireID))
	if handle, err := gate.Begin(t.Context(), fixture.assignment, fixture.acquireID); !errors.Is(err, stageauthority.ErrRenewalMismatch) {
		if handle != nil {
			handle.Release()
		}
		t.Fatalf("older envelope replaced renewal: %v", err)
	}
	if err := gate.Close(); err != nil {
		t.Fatal(err)
	}
	gate = fixture.open(t)
	snapshot := admissionSnapshot(t, gate)
	if snapshot.Latest == nil || snapshot.Latest.Phase != stageworkeragent.AssignmentInputsPending ||
		snapshot.Latest.AcquireCommandID != fixture.acquireID || !proto.Equal(snapshot.Latest.Original, fixture.assignment.Authority) ||
		!proto.Equal(snapshot.Latest.Latest, renewal.Authority) {
		t.Fatalf("lost original/renewal history: %#v", snapshot)
	}
	snapshot.Latest.Original.Signature[0] ^= 1
	if !proto.Equal(admissionSnapshot(t, gate).Latest.Original, fixture.assignment.Authority) {
		t.Fatal("snapshot mutation changed durable history")
	}
	document, err := os.ReadFile(filepath.Join(fixture.config.Directory, admissionTestState))
	if err != nil || bytes.Contains(document, []byte("admission-customer-content-sentinel")) {
		t.Fatalf("journal contains customer content or is unreadable: %v", err)
	}
	if err := gate.CloseExecution(t.Context(), fixture.assignment.Authority); err != nil {
		t.Fatal(err)
	}
	if handle, err := gate.Begin(t.Context(), renewal, fixture.acquireID); !errors.Is(err, stageworkeragent.ErrAdmissionClosed) {
		if handle != nil {
			handle.Release()
		}
		t.Fatalf("renewal reopened closed execution: %v", err)
	}
}

func TestAssignmentAdmissionCloseDoesNotReleaseLateInputWriter(t *testing.T) {
	fixture := newAdmissionFixture(t)
	gate := fixture.open(t)
	handle := beginAdmission(t, gate, fixture.assignment, fixture.acquireID)
	inputPath := filepath.Join(fixture.config.InputRoot, "late-input")
	file, err := os.Create(inputPath)
	if err != nil {
		t.Fatal(err)
	}
	finish := make(chan struct{})
	writerDone := make(chan struct{})
	var writerErr error
	var finishOnce sync.Once
	go func() {
		<-finish
		_, writeErr := file.WriteString("late resolver result")
		closeErr := file.Close()
		var checkpointErr error
		if closeErr == nil {
			checkpointErr = handle.CompleteInputs(t.Context())
		}
		handle.Release()
		writerErr = errors.Join(writeErr, closeErr, checkpointErr)
		close(writerDone)
	}()
	finishWriter := func() {
		finishOnce.Do(func() { close(finish) })
		<-writerDone
		if writerErr != nil {
			t.Error(writerErr)
		}
	}
	t.Cleanup(finishWriter)
	if err := gate.CloseExecution(t.Context(), fixture.assignment.Authority); err != nil {
		t.Fatal(err)
	}
	if err := handle.EnterRuntime(t.Context()); !errors.Is(err, stageworkeragent.ErrAdmissionClosed) {
		t.Fatalf("late resolver can enter Runtime: %v", err)
	}
	if next, err := gate.Begin(t.Context(), fixture.next(t, 2), uuid.New()); !errors.Is(err, stageworkeragent.ErrStageWorkerBusy) {
		if next != nil {
			next.Release()
		}
		t.Fatalf("writer slot released before task exited: %v", err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := handle.WaitReleased(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("closed admission falsely proves writer release: %v", err)
	}
	if err := gate.Close(); !errors.Is(err, stageworkeragent.ErrStageWorkerBusy) {
		t.Fatalf("closed process lock while writer active: %v", err)
	}
	if _, err := os.Stat(inputPath); err != nil {
		t.Fatalf("CloseExecution removed input: %v", err)
	}
	finishWriter()
	if err := handle.WaitReleased(t.Context()); err != nil {
		t.Fatal(err)
	}
	beginAdmission(t, gate, fixture.next(t, 2), uuid.New()).Release()
	if data, err := os.ReadFile(inputPath); err != nil || string(data) != "late resolver result" {
		t.Fatalf("late writer did not finish before release: %q, %v", data, err)
	}
}

func TestAssignmentAdmissionRuntimeEntryRequiresRecoveryAcrossRestart(t *testing.T) {
	fixture := newAdmissionFixture(t)
	gate := fixture.open(t)
	handle := beginAdmission(t, gate, fixture.assignment, fixture.acquireID)
	if err := handle.CompleteInputs(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := handle.EnterRuntime(t.Context()); err != nil {
		t.Fatal(err)
	}
	handle.Release()
	if err := gate.Close(); err != nil {
		t.Fatal(err)
	}
	gate = fixture.open(t)
	for _, assignment := range []*velav1.StageAssignment{fixture.assignment, fixture.next(t, 2)} {
		if next, err := gate.Begin(t.Context(), assignment, fixture.acquireID); !errors.Is(err, stageworkeragent.ErrAdmissionRecoveryRequired) {
			if next != nil {
				next.Release()
			}
			t.Fatalf("unrecovered Runtime execution admitted: %v", err)
		}
	}
	fixture.clock.Add(int64(6 * time.Minute))
	if err := gate.CloseExecution(t.Context(), fixture.assignment.Authority); err != nil {
		t.Fatalf("expired historical identity cannot close local admission: %v", err)
	}
	if snapshot := admissionSnapshot(t, gate); snapshot.Watermark != 1 || snapshot.Latest.Phase != stageworkeragent.AssignmentClosed {
		t.Fatalf("closed historical execution lost: %#v", snapshot)
	}
}

func TestAssignmentAdmissionRevalidatesBeforeRuntimeEntry(t *testing.T) {
	fixture := newAdmissionFixture(t)
	gate := fixture.open(t)
	handle := beginAdmission(t, gate, fixture.assignment, fixture.acquireID)
	fixture.clock.Add(int64(6 * time.Minute))
	if err := handle.EnterRuntime(t.Context()); !errors.Is(err, stageauthority.ErrStale) {
		t.Fatalf("expired input resolution entered Runtime: %v", err)
	}
	if snapshot := admissionSnapshot(t, gate); snapshot.Latest.Phase != stageworkeragent.AssignmentInputsPending {
		t.Fatalf("expired entry changed phase: %#v", snapshot)
	}
}

func TestAssignmentAdmissionClosesWithinAcceptedClockSkew(t *testing.T) {
	fixture := newAdmissionFixture(t)
	fixture.config.MaxClockSkew = time.Second
	fixture.clock.Add(-int64(500 * time.Millisecond))
	gate := fixture.open(t)
	beginAdmission(t, gate, fixture.assignment, fixture.acquireID).Release()
	if err := gate.CloseExecution(t.Context(), fixture.assignment.Authority); err != nil {
		t.Fatalf("cannot close authority admitted under the same skew policy: %v", err)
	}
}

func TestAssignmentAdmissionCrossProfileWatermarkAndBoundedBacklog(t *testing.T) {
	fixture := newAdmissionFixture(t)
	fixture.config.MaxRecords = 3
	otherProfile := uuid.NewString()
	for _, binding := range fixture.config.Bindings[:2] {
		binding.Runtime.StageProfileRevisionID = otherProfile
		fixture.config.Bindings = append(fixture.config.Bindings, binding)
	}
	gate := fixture.open(t)
	for sequence := int64(1); sequence <= 3; sequence++ {
		assignment := fixture.next(t, sequence)
		if sequence == 2 {
			assignment.Authority.StageProfileRevisionId = otherProfile
			fixture.sign(t, assignment)
		}
		completeAdmissionInputs(t, beginAdmission(t, gate, assignment, uuid.New()))
	}
	if handle, err := gate.Begin(t.Context(), fixture.next(t, 4), uuid.New()); !errors.Is(err, stageworkeragent.ErrAdmissionCapacity) {
		if handle != nil {
			handle.Release()
		}
		t.Fatalf("history at capacity silently evicted records: %v", err)
	}
	if err := gate.Close(); err != nil {
		t.Fatal(err)
	}
	gate = fixture.open(t)
	snapshot := admissionSnapshot(t, gate)
	if snapshot.Watermark != 3 || len(snapshot.Pending) != 2 || snapshot.Pending[1].Original.StageProfileRevisionId != otherProfile {
		t.Fatalf("bounded cross-profile history lost: %#v", snapshot)
	}
	for _, record := range snapshot.Pending {
		if record.Phase != stageworkeragent.AssignmentClosed {
			t.Fatalf("superseded record is not closed: %#v", record)
		}
	}
	if handle, err := gate.Begin(t.Context(), fixture.assignment, fixture.acquireID); !errors.Is(err, stageworkeragent.ErrAdmissionClosed) {
		if handle != nil {
			handle.Release()
		}
		t.Fatalf("old profile bypassed Worker watermark: %v", err)
	}
}

func TestAssignmentAdmissionConcurrentBeginHasOneWriter(t *testing.T) {
	fixture := newAdmissionFixture(t)
	gate := fixture.open(t)
	type result struct {
		handle *stageworkeragent.AssignmentAdmission
		err    error
	}
	results := make(chan result, 16)
	for range cap(results) {
		go func() {
			handle, err := gate.Begin(t.Context(), fixture.assignment, fixture.acquireID)
			results <- result{handle, err}
		}()
	}
	var admitted []*stageworkeragent.AssignmentAdmission
	for range cap(results) {
		result := <-results
		if result.err == nil {
			admitted = append(admitted, result.handle)
		} else if !errors.Is(result.err, stageworkeragent.ErrStageWorkerBusy) {
			t.Error(result.err)
		}
	}
	for _, handle := range admitted {
		handle.Release()
		handle.Release()
		if err := handle.WaitReleased(t.Context()); err != nil {
			t.Error(err)
		}
	}
	if len(admitted) != 1 {
		t.Fatalf("concurrent writer count = %d", len(admitted))
	}
	if snapshot := admissionSnapshot(t, gate); snapshot.Watermark != 1 || len(snapshot.Pending) != 0 {
		t.Fatalf("busy requests consumed history: %#v", snapshot)
	}
}
