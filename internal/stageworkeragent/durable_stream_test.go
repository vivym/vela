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
	"github.com/vivym/vela/internal/stageartifact"
	"github.com/vivym/vela/internal/stageauthority"
	"github.com/vivym/vela/internal/stageworkeragent"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func durableFixtureAdmission(t *testing.T, fixture singleMemberMaterializationFixture) admissionFixture {
	t.Helper()
	journal := newAdmissionFixture(t)
	keys := map[string][]byte{"single-stage-key": bytes.Repeat([]byte{0x7b}, 32)}
	validator, err := stageauthority.NewValidator(keys, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	signer, err := stageauthority.NewSigner(keys)
	if err != nil {
		t.Fatal(err)
	}
	// These are trusted fixture identities, fixed before any tested assignment mutation.
	authority := fixture.authority
	member := authority.Members[0]
	binding := stageauthority.RuntimeBinding{
		WorkerInstanceID: authority.WorkerInstanceId, WorkerInstanceEpoch: authority.WorkerInstanceEpoch,
		WorkerMemberID: member.WorkerMemberId, WorkerMemberEpoch: member.MemberEpoch, ModelRuntimeEpoch: member.ModelRuntimeEpoch,
		DeviceSetDigest: authority.DeviceSetDigest, MembershipDigest: authority.MembershipDigest,
		ModelResidencyID: authority.ModelResidencyId, ModelRuntimeIdentity: authority.ModelRuntimeIdentity,
		StageProfileRevisionID: authority.StageProfileRevisionId,
		Members:                []stageauthority.MemberEpoch{{ID: member.WorkerMemberId, Epoch: member.MemberEpoch}},
	}
	for _, device := range authority.Devices {
		binding.Devices = append(binding.Devices, stageauthority.DeviceEpoch{ID: device.DeviceId, Epoch: device.DeviceEpoch})
	}
	journal.config.WorkerInstanceID = uuid.MustParse(authority.WorkerInstanceId)
	journal.config.WorkerInstanceEpoch = authority.WorkerInstanceEpoch
	journal.config.WorkerMemberID = uuid.MustParse(member.WorkerMemberId)
	journal.config.Validator = validator
	journal.config.Bindings = []stageworkeragent.AdmissionRuntimeBinding{{Runtime: binding, IdentityDigest: [32]byte(member.IdentityDigest)}}
	journal.signer, journal.assignment = signer, fixture.assignment
	return journal
}

func durableFixtureStream(t *testing.T, fixture singleMemberMaterializationFixture, gate *stageworkeragent.FileAssignmentAdmission, control stageworkeragent.ControlClient, resolver stageworkeragent.InputResolver, client velav1.ModelRuntimeServiceClient, materialization *stageworkeragent.MaterializationConfig) *stageworkeragent.StreamAgent {
	t.Helper()
	if client == nil {
		client = fixture.client
	}
	runtime, err := stageworkeragent.New(stageworkeragent.Config{Members: []stageworkeragent.RuntimeMember{{ID: fixture.memberID, Client: client}}})
	if err != nil {
		t.Fatal(err)
	}
	stream, err := stageworkeragent.NewDurableStreamAgent(stageworkeragent.DurableStreamConfig{
		Runtime: runtime, Control: control, Admission: gate, InputResolver: resolver, Materialization: materialization,
	})
	if err != nil {
		t.Fatal(err)
	}
	return stream
}

type admissionRuntimeProbe struct {
	velav1.ModelRuntimeServiceClient
	prepares      atomic.Int32
	statuses      atomic.Int32
	beforePrepare func(*velav1.StageAuthority) error
	beforeStatus  func(*velav1.StageAuthority) error
	losePrepare   bool
}

func (probe *admissionRuntimeProbe) PrepareStage(ctx context.Context, request *velav1.ModelRuntimeServicePrepareStageRequest, options ...grpc.CallOption) (*velav1.ModelRuntimeServicePrepareStageResponse, error) {
	probe.prepares.Add(1)
	if probe.beforePrepare != nil {
		if err := probe.beforePrepare(request.Authority); err != nil {
			return nil, err
		}
	}
	response, err := probe.ModelRuntimeServiceClient.PrepareStage(ctx, request, options...)
	if err == nil && probe.losePrepare {
		return nil, errors.New("lost accepted Prepare response")
	}
	return response, err
}

func (probe *admissionRuntimeProbe) Status(ctx context.Context, request *velav1.ModelRuntimeServiceStatusRequest, options ...grpc.CallOption) (*velav1.ModelRuntimeServiceStatusResponse, error) {
	probe.statuses.Add(1)
	if probe.beforeStatus != nil {
		if err := probe.beforeStatus(request.Authority); err != nil {
			return nil, err
		}
	}
	return probe.ModelRuntimeServiceClient.Status(ctx, request, options...)
}

func TestDurableStreamRejectsBeforeInputResolution(t *testing.T) {
	for _, invalid := range []string{"missing-acquire", "signature", "runtime", "closed"} {
		t.Run(invalid, func(t *testing.T) {
			fixture := newSingleMemberMaterializationFixture(t)
			journal := durableFixtureAdmission(t, fixture)
			gate := journal.open(t)
			assignment := rootInputAssignment(t, fixture.assignment, sha256.Sum256([]byte("input")), 5, "https://example.test/input")
			if invalid == "closed" {
				beginAdmission(t, gate, assignment, journal.acquireID).Release()
				if err := gate.CloseExecution(t.Context(), assignment.Authority); err != nil {
					t.Fatal(err)
				}
			}
			if invalid == "signature" {
				assignment.Authority.Signature[0] ^= 1
			}
			if invalid == "runtime" {
				assignment.Authority.Members[0].ModelRuntimeEpoch++
				journal.sign(t, assignment)
			}
			var resolves atomic.Int32
			probe := &admissionRuntimeProbe{ModelRuntimeServiceClient: fixture.client}
			control := &recordingStreamControl{decision: velav1.StageWorkerCommandDecision_STAGE_WORKER_COMMAND_DECISION_ACCEPTED}
			stream := durableFixtureStream(t, fixture, gate, control, inputResolverFunc(func(context.Context, *velav1.StageAssignment) error { resolves.Add(1); return nil }), probe, nil)
			var result stageworkeragent.AssignmentExecutionResult
			var err error
			if invalid == "missing-acquire" {
				result, err = stream.ExecuteAssignment(t.Context(), assignment)
			} else {
				result, err = stream.ExecuteAcquiredAssignment(t.Context(), assignment, journal.acquireID)
			}
			if err == nil || result.ControlStartAccepted || resolves.Load() != 0 || probe.prepares.Load() != 0 || control.calls != 0 {
				t.Fatalf("invalid assignment crossed input gate: %+v, %v, resolves=%d prepares=%d", result, err, resolves.Load(), probe.prepares.Load())
			}
		})
	}
}

func TestDurableStreamReattachRequiresRecordedRuntimeIntent(t *testing.T) {
	for _, phase := range []string{"empty", "inputs-pending", "closed"} {
		t.Run(phase, func(t *testing.T) {
			fixture := newSingleMemberMaterializationFixture(t)
			journal := durableFixtureAdmission(t, fixture)
			gate := journal.open(t)
			if phase != "empty" {
				beginAdmission(t, gate, fixture.assignment, journal.acquireID).Release()
			}
			if phase == "closed" {
				if err := gate.CloseExecution(t.Context(), fixture.authority); err != nil {
					t.Fatal(err)
				}
			}
			probe := &admissionRuntimeProbe{ModelRuntimeServiceClient: fixture.client}
			control := &recordingStreamControl{decision: velav1.StageWorkerCommandDecision_STAGE_WORKER_COMMAND_DECISION_ACCEPTED}
			stream := durableFixtureStream(t, fixture, gate, control, nil, probe, nil)
			if _, err := stream.Reattach(t.Context(), fixture.authority, "", nil); !errors.Is(err, stageworkeragent.ErrAdmissionClosed) {
				t.Fatalf("reattach invented execution history: %v", err)
			}
			if probe.statuses.Load() != 0 || control.calls != 0 {
				t.Fatal("reattach without Runtime intent reached RPCs")
			}
		})
	}
}

func TestDurableStreamPersistenceFailureDoesNotReachPrepareOrRenewal(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root bypasses directory write permissions")
	}
	for _, operation := range []string{"prepare", "renewal"} {
		t.Run(operation, func(t *testing.T) {
			fixture := newSingleMemberMaterializationFixture(t)
			journal := durableFixtureAdmission(t, fixture)
			gate := journal.open(t)
			assignment := rootInputAssignment(t, fixture.assignment, sha256.Sum256([]byte("input")), 5, "https://example.test/input")
			resolver := inputResolverFunc(func(context.Context, *velav1.StageAssignment) error {
				if operation == "prepare" {
					return os.Chmod(journal.config.Directory, 0o500)
				}
				return nil
			})
			t.Cleanup(func() { _ = os.Chmod(journal.config.Directory, 0o700) })
			probe := &admissionRuntimeProbe{ModelRuntimeServiceClient: fixture.client}
			control := &recordingStreamControl{decision: velav1.StageWorkerCommandDecision_STAGE_WORKER_COMMAND_DECISION_ACCEPTED}
			stream := durableFixtureStream(t, fixture, gate, control, resolver, probe, nil)
			_, executeErr := stream.ExecuteAcquiredAssignment(t.Context(), assignment, journal.acquireID)
			if operation == "prepare" {
				if executeErr == nil || probe.prepares.Load() != 0 || control.calls != 0 {
					t.Fatalf("Prepare crossed failed intent: %v", executeErr)
				}
			} else {
				if executeErr != nil {
					t.Fatal(executeErr)
				}
				renewal := proto.Clone(assignment).(*velav1.StageAssignment)
				renewal.Authority.StageVersion++
				renewal.Authority.IssuedAt = timestamppb.Now()
				renewal.Authority.ExpiresAt = timestamppb.New(renewal.Authority.ExpiresAt.AsTime().Add(time.Minute))
				journal.sign(t, renewal)
				control.renewedAuthority = renewal.Authority
				probe.beforeStatus = func(authority *velav1.StageAuthority) error {
					if proto.Equal(authority, renewal.Authority) {
						t.Error("unpersisted renewal reached Runtime")
					}
					return nil
				}
				if err := os.Chmod(journal.config.Directory, 0o500); err != nil {
					t.Fatal(err)
				}
				if _, err := stream.Heartbeat(t.Context(), 1); err == nil {
					t.Fatal("renewal ignored failed journal")
				}
			}
			if err := os.Chmod(journal.config.Directory, 0o700); err != nil {
				t.Fatal(err)
			}
			if _, err := gate.Snapshot(t.Context()); err == nil {
				t.Fatal("persistence failure was not sticky")
			}
			if err := gate.Close(); err != nil {
				t.Fatal(err)
			}
			gate = journal.open(t)
			snapshot := admissionSnapshot(t, gate)
			if !proto.Equal(snapshot.Latest.Latest, assignment.Authority) {
				t.Fatal("failed checkpoint changed recovered authority")
			}
			if operation == "prepare" && snapshot.Latest.Phase != stageworkeragent.AssignmentInputsPending {
				t.Fatal("failed Prepare checkpoint advanced recovered phase")
			}
		})
	}
}

func TestDurableStreamInputRetryCheckpointsBeforePrepare(t *testing.T) {
	fixture := newSingleMemberMaterializationFixture(t)
	journal := durableFixtureAdmission(t, fixture)
	gate := journal.open(t)
	assignment := rootInputAssignment(t, fixture.assignment, sha256.Sum256([]byte("input")), 5, "https://example.test/input")
	var resolves int
	inputErr := errors.New("retryable input failure")
	resolver := inputResolverFunc(func(ctx context.Context, _ *velav1.StageAssignment) error {
		resolves++
		snapshot, err := gate.Snapshot(ctx)
		if err != nil || snapshot.Latest == nil || snapshot.Latest.Phase != stageworkeragent.AssignmentInputsPending || snapshot.Latest.AcquireCommandID != journal.acquireID {
			return errors.New("Resolve called without durable input admission")
		}
		if resolves == 1 {
			return inputErr
		}
		return nil
	})
	probe := &admissionRuntimeProbe{ModelRuntimeServiceClient: fixture.client, beforePrepare: func(authority *velav1.StageAuthority) error {
		snapshot, err := gate.Snapshot(t.Context())
		if err != nil || snapshot.Latest == nil || snapshot.Latest.Phase != stageworkeragent.AssignmentRuntimeEntered || !proto.Equal(snapshot.Latest.Latest, authority) {
			return errors.New("Prepare called before durable Runtime intent")
		}
		return nil
	}}
	control := &recordingStreamControl{decision: velav1.StageWorkerCommandDecision_STAGE_WORKER_COMMAND_DECISION_ACCEPTED}
	stream := durableFixtureStream(t, fixture, gate, control, resolver, probe, nil)
	if _, err := stream.ExecuteAcquiredAssignment(t.Context(), assignment, journal.acquireID); !errors.Is(err, inputErr) {
		t.Fatalf("first input error: %v", err)
	}
	if probe.prepares.Load() != 0 {
		t.Fatal("input failure reached Prepare")
	}
	if err := gate.Close(); err != nil {
		t.Fatal(err)
	}
	gate = journal.open(t)
	stream = durableFixtureStream(t, fixture, gate, control, resolver, probe, nil)
	result, err := stream.ExecuteAcquiredAssignment(t.Context(), assignment, journal.acquireID)
	if err != nil || !result.ControlStartAccepted || probe.prepares.Load() != 1 || resolves != 2 {
		t.Fatalf("retry failed: %+v, %v, resolves=%d prepares=%d", result, err, resolves, probe.prepares.Load())
	}
}

func TestDurableStreamStopPersistsBeforeLateResolverReturns(t *testing.T) {
	for _, unsolicited := range []bool{false, true} {
		t.Run(map[bool]string{false: "direct", true: "unsolicited"}[unsolicited], func(t *testing.T) {
			fixture := newSingleMemberMaterializationFixture(t)
			journal := durableFixtureAdmission(t, fixture)
			gate := journal.open(t)
			assignment := rootInputAssignment(t, fixture.assignment, sha256.Sum256([]byte("input")), 5, "https://example.test/input")
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			started := make(chan context.Context, 1)
			release := make(chan struct{})
			var releaseOnce sync.Once
			finishResolver := func() { releaseOnce.Do(func() { close(release) }) }
			defer finishResolver()
			resolver := inputResolverFunc(func(ctx context.Context, _ *velav1.StageAssignment) error {
				file, err := os.Create(filepath.Join(journal.config.InputRoot, "late-input"))
				if err != nil {
					return err
				}
				defer func() { _ = file.Close() }()
				started <- ctx
				<-release
				_, err = file.WriteString("late input result")
				return err
			})
			commands := make(chan *velav1.StageWorkerControlServiceConnectResponse, 1)
			control := &recordingStreamControl{decision: velav1.StageWorkerCommandDecision_STAGE_WORKER_COMMAND_DECISION_ACCEPTED, commands: commands}
			probe := &admissionRuntimeProbe{ModelRuntimeServiceClient: fixture.client}
			stream := durableFixtureStream(t, fixture, gate, control, resolver, probe, nil)
			finished := make(chan error, 1)
			go func() {
				_, err := stream.ExecuteAcquiredAssignment(ctx, assignment, journal.acquireID)
				finished <- err
			}()
			var inputCtx context.Context
			select {
			case inputCtx = <-started:
			case <-ctx.Done():
				t.Fatal("input resolver did not start")
			}
			stop := &velav1.StopStage{Authority: assignment.Authority, Reason: velav1.StageWorkerStopReason_STAGE_WORKER_STOP_REASON_PARENT_CANCELED}
			if unsolicited {
				commands <- &velav1.StageWorkerControlServiceConnectResponse{Result: &velav1.StageWorkerControlServiceConnectResponse_StopStage{StopStage: stop}}
				close(commands)
				if err := stream.RunControlCommands(ctx); err != nil {
					t.Fatal(err)
				}
			} else {
				result, err := stream.HandleStop(ctx, stop)
				if err != nil || result.AllStopped {
					t.Fatalf("Stop asserted drain: %+v %v", result, err)
				}
			}
			if inputCtx.Err() == nil || admissionSnapshot(t, gate).Latest.Phase != stageworkeragent.AssignmentClosed {
				t.Fatal("Stop returned without cancel and durable closure")
			}
			if err := gate.Close(); !errors.Is(err, stageworkeragent.ErrStageWorkerBusy) {
				t.Fatalf("writer handle released before resolver exit: %v", err)
			}
			finishResolver()
			select {
			case err := <-finished:
				if !errors.Is(err, context.Canceled) {
					t.Fatalf("late resolver admitted: %v", err)
				}
			case <-ctx.Done():
				t.Fatal("resolver did not exit")
			}
			if probe.prepares.Load() != 0 || control.calls != 0 {
				t.Fatal("canceled input reached execution")
			}
			if err := gate.Close(); err != nil {
				t.Fatal(err)
			}
			gate = journal.open(t)
			stream = durableFixtureStream(t, fixture, gate, control, resolver, probe, nil)
			if _, err := stream.ExecuteAcquiredAssignment(ctx, assignment, journal.acquireID); !errors.Is(err, stageworkeragent.ErrAdmissionClosed) {
				t.Fatalf("restart reopened stopped input: %v", err)
			}
		})
	}
}

func TestDurableStreamLostPrepareRetainsRecoveryFence(t *testing.T) {
	fixture := newSingleMemberMaterializationFixture(t)
	journal := durableFixtureAdmission(t, fixture)
	gate := journal.open(t)
	probe := &admissionRuntimeProbe{ModelRuntimeServiceClient: fixture.client, losePrepare: true}
	control := &recordingStreamControl{decision: velav1.StageWorkerCommandDecision_STAGE_WORKER_COMMAND_DECISION_ACCEPTED}
	stream := durableFixtureStream(t, fixture, gate, control, nil, probe, nil)
	if _, err := stream.ExecuteAcquiredAssignment(t.Context(), fixture.assignment, journal.acquireID); err == nil {
		t.Fatal("lost Prepare response succeeded")
	}
	if admissionSnapshot(t, gate).Latest.Phase != stageworkeragent.AssignmentRuntimeEntered {
		t.Fatal("uncertain Prepare lost its recovery fence")
	}
	if err := gate.Close(); err != nil {
		t.Fatal(err)
	}
	gate = journal.open(t)
	stream = durableFixtureStream(t, fixture, gate, control, nil, probe, nil)
	if _, err := stream.ExecuteAcquiredAssignment(t.Context(), fixture.assignment, journal.acquireID); !errors.Is(err, stageworkeragent.ErrAdmissionRecoveryRequired) {
		t.Fatalf("uncertain Prepare replayed: %v", err)
	}
	if probe.prepares.Load() != 1 {
		t.Fatal("Prepare replay crossed recovery fence")
	}
}

func TestDurableStreamRenewalAndReattachUseRecordedRuntimeIntent(t *testing.T) {
	fixture := newSingleMemberMaterializationFixture(t)
	journal := durableFixtureAdmission(t, fixture)
	gate := journal.open(t)
	probe := &admissionRuntimeProbe{ModelRuntimeServiceClient: fixture.client, beforeStatus: func(authority *velav1.StageAuthority) error {
		snapshot, err := gate.Snapshot(t.Context())
		if err != nil || snapshot.Latest == nil || !proto.Equal(snapshot.Latest.Latest, authority) {
			return errors.New("Runtime observed unrecorded authority")
		}
		return nil
	}}
	control := &recordingStreamControl{decision: velav1.StageWorkerCommandDecision_STAGE_WORKER_COMMAND_DECISION_ACCEPTED}
	stream := durableFixtureStream(t, fixture, gate, control, nil, probe, nil)
	if _, err := stream.ExecuteAcquiredAssignment(t.Context(), fixture.assignment, journal.acquireID); err != nil {
		t.Fatal(err)
	}
	renewal := proto.Clone(fixture.assignment).(*velav1.StageAssignment)
	renewal.Authority.StageVersion++
	renewal.Authority.IssuedAt = timestamppb.Now()
	renewal.Authority.ExpiresAt = timestamppb.New(renewal.Authority.ExpiresAt.AsTime().Add(time.Minute))
	journal.sign(t, renewal)
	control.renewedAuthority = renewal.Authority
	if _, err := stream.Heartbeat(t.Context(), 1); err != nil {
		t.Fatal(err)
	}
	if snapshot := admissionSnapshot(t, gate); !proto.Equal(snapshot.Latest.Original, fixture.authority) || !proto.Equal(snapshot.Latest.Latest, renewal.Authority) {
		t.Fatal("renewal replaced original history")
	}
	if err := gate.Close(); err != nil {
		t.Fatal(err)
	}
	gate = journal.open(t)
	control.renewedAuthority = nil
	stream = durableFixtureStream(t, fixture, gate, control, nil, probe, nil)
	if result, err := stream.Reattach(t.Context(), renewal.Authority, "", nil); err != nil || !result.Accepted {
		t.Fatalf("reattach recorded Runtime intent: %+v %v", result, err)
	}
	if probe.prepares.Load() != 1 {
		t.Fatal("reattach prepared Runtime again")
	}
	if _, err := stream.HandleStop(t.Context(), &velav1.StopStage{Authority: renewal.Authority, Reason: velav1.StageWorkerStopReason_STAGE_WORKER_STOP_REASON_PARENT_CANCELED}); err != nil {
		t.Fatal(err)
	}
	beforeStatus, beforeControl := probe.statuses.Load(), control.calls
	if _, err := stream.Reattach(t.Context(), renewal.Authority, "", nil); !errors.Is(err, stageworkeragent.ErrAdmissionClosed) {
		t.Fatalf("closed Runtime reattached: %v", err)
	}
	if _, err := stream.Heartbeat(t.Context(), 2); !errors.Is(err, stageworkeragent.ErrAdmissionClosed) {
		t.Fatalf("closed Runtime heartbeat renewed: %v", err)
	}
	if probe.statuses.Load() != beforeStatus || control.calls != beforeControl {
		t.Fatal("closed Runtime entry performed RPCs")
	}
}

func TestDurableStreamMaterializationClosesAdmissionBeforeRecovery(t *testing.T) {
	fixture := newSingleMemberMaterializationFixture(t)
	journal := durableFixtureAdmission(t, fixture)
	gate := journal.open(t)
	control := newMaterializingStreamControl(t, fixture.authority)
	source, err := stageartifact.NewFilesystemLocalOutputSource(fixture.localRoot)
	if err != nil {
		t.Fatal(err)
	}
	materializationJournal, err := stageworkeragent.NewMemoryMaterializationJournal(4)
	if err != nil {
		t.Fatal(err)
	}
	materialization := &stageworkeragent.MaterializationConfig{
		Validator: control.validator, Source: source, Publisher: &outageOncePublisher{failures: 1, objectVersion: "durable-stream-l2-version"}, Journal: materializationJournal,
		SourceLossEvidence: stageworkeragent.MaterializationSourceLossEvidenceFunc(func(context.Context, stageworkeragent.PendingMaterialization) (stageworkeragent.MaterializationSourceLossEvidence, error) {
			return stageworkeragent.MaterializationSourceLossEvidence{}, errors.New("unexpected source loss")
		}),
	}
	stream := durableFixtureStream(t, fixture, gate, control, nil, nil, materialization)
	if _, err := stream.ExecuteAcquiredAssignment(t.Context(), fixture.assignment, journal.acquireID); err != nil {
		t.Fatal(err)
	}
	fixture.backend.MarkOutputReadyWithSize(fixture.manifest, int64(len(fixture.payload)))
	result, err := stream.SealAndMaterialize(t.Context())
	if err == nil || !result.LocalSealed || !result.GPUReleased {
		t.Fatalf("expected recoverable publication outage: %+v %v", result, err)
	}
	if admissionSnapshot(t, gate).Latest.Phase != stageworkeragent.AssignmentClosed {
		t.Fatal("sealed output released authority before closing admission")
	}
	if err := gate.Close(); err != nil {
		t.Fatal(err)
	}
	gate = journal.open(t)
	beginAdmission(t, gate, journal.next(t, 2), uuid.New()).Release()
	stream = durableFixtureStream(t, fixture, gate, control, nil, nil, materialization)
	result, err = stream.ResumeMaterializations(t.Context())
	if err != nil || !result.Committed {
		t.Fatalf("materialization recovery: %+v %v", result, err)
	}
	snapshot := admissionSnapshot(t, gate)
	if snapshot.Watermark != 2 || snapshot.Latest.Phase != stageworkeragent.AssignmentInputsPending || len(snapshot.Pending) != 1 || snapshot.Pending[0].Phase != stageworkeragent.AssignmentClosed {
		t.Fatal("old materialization changed newer admission")
	}
}

func TestDurableProductionRunCarriesAcquireIdentityThroughMaterialization(t *testing.T) {
	fixture := newSingleMemberMaterializationFixture(t)
	journal := durableFixtureAdmission(t, fixture)
	gate := journal.open(t)
	control := &productionExecutionControl{
		materializingStreamControl: newMaterializingStreamControl(t, fixture.authority),
		identity:                   runtimeIdentityFromAuthority(fixture.authority), assignment: fixture.assignment,
	}
	source, err := stageartifact.NewFilesystemLocalOutputSource(fixture.localRoot)
	if err != nil {
		t.Fatal(err)
	}
	materializationJournal, err := stageworkeragent.NewMemoryMaterializationJournal(4)
	if err != nil {
		t.Fatal(err)
	}
	materialization := &stageworkeragent.MaterializationConfig{
		Validator: control.validator, Source: source, Publisher: &outageOncePublisher{objectVersion: "durable-production-l2-version"}, Journal: materializationJournal,
		SourceLossEvidence: stageworkeragent.MaterializationSourceLossEvidenceFunc(func(context.Context, stageworkeragent.PendingMaterialization) (stageworkeragent.MaterializationSourceLossEvidence, error) {
			return stageworkeragent.MaterializationSourceLossEvidence{}, errors.New("unexpected source loss")
		}),
	}
	stream := durableFixtureStream(t, fixture, gate, control, nil, nil, materialization)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	agent, err := stageworkeragent.NewProductionAgent(stageworkeragent.ProductionConfig{
		Control: control, Runtime: fixture.client, Stream: stream, RuntimeIdentity: control.identity,
		Devices: fixture.authority.Devices, Members: fixture.authority.Members, CapacityVector: fixture.authority.CapacityVector,
		CapacityTTL: 2 * time.Minute, HeartbeatInterval: 10 * time.Second,
		ObservationSequenceSource: &capacitySequenceSource{values: []int64{17, 18}}, Now: time.Now,
		Wait: func(_ context.Context, interval time.Duration) error {
			switch interval {
			case 10 * time.Second:
				fixture.backend.MarkOutputReadyWithSize(fixture.manifest, int64(len(fixture.payload)))
			case 250 * time.Millisecond:
				cancel()
			default:
				return errors.New("unexpected production backoff")
			}
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := agent.RunAssignment(ctx, fixture.assignment); err == nil {
		t.Fatal("durable direct API invented missing Acquire ID")
	}
	if err := agent.Run(ctx); err != nil {
		t.Fatal(err)
	}
	snapshot := admissionSnapshot(t, gate)
	if control.acquireCalls != 2 || control.startCalls != 1 || control.commitCalls != 1 || snapshot.Latest == nil ||
		snapshot.Latest.Phase != stageworkeragent.AssignmentClosed || snapshot.Latest.AcquireCommandID.String() != control.acquireIDs[0] {
		t.Fatalf("durable production lifecycle: acquires=%d starts=%d commits=%d snapshot=%+v", control.acquireCalls, control.startCalls, control.commitCalls, snapshot)
	}
}

func TestDurableStreamFailureClosesConcurrentRenewal(t *testing.T) {
	fixture := newSingleMemberMaterializationFixture(t)
	journal := durableFixtureAdmission(t, fixture)
	gate := journal.open(t)
	renewal := proto.Clone(fixture.assignment).(*velav1.StageAssignment)
	renewal.Authority.StageVersion++
	renewal.Authority.IssuedAt = timestamppb.Now()
	renewal.Authority.ExpiresAt = timestamppb.New(renewal.Authority.ExpiresAt.AsTime().Add(time.Minute))
	journal.sign(t, renewal)
	release := make(chan struct{})
	var once sync.Once
	finishFailure := func() { once.Do(func() { close(release) }) }
	defer finishFailure()
	control := &failureRenewalControl{
		recordingStreamControl: &recordingStreamControl{decision: velav1.StageWorkerCommandDecision_STAGE_WORKER_COMMAND_DECISION_ACCEPTED},
		blocked:                make(chan struct{}), release: release, renewal: renewal.Authority,
	}
	stream := durableFixtureStream(t, fixture, gate, control, nil, nil, nil)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	if _, err := stream.ExecuteAcquiredAssignment(ctx, fixture.assignment, journal.acquireID); err != nil {
		t.Fatal(err)
	}
	failedAt := time.Now()
	status := stageworkeragent.AggregateStatus{
		ReportingMembers: 1,
		States:           map[string]velav1.ModelRuntimeExecutionState{fixture.memberID: velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_FAILED},
		Failures: map[string]*velav1.ModelRuntimeFailureEvidence{fixture.memberID: {
			FailureClass: "backend_oom", FailureFingerprint: bytes.Repeat([]byte{0xa1}, 32), Detail: "injected failure", WorkerReusable: true,
			ConsumedResourceUnits: 1, FailedAt: timestamppb.New(failedAt), RetryAt: timestamppb.New(failedAt.Add(time.Second)),
		}},
	}
	finished := make(chan error, 1)
	go func() { _, err := stream.Fail(ctx, status); finished <- err }()
	select {
	case <-control.blocked:
	case <-ctx.Done():
		t.Fatal("failure did not reach control")
	}
	if _, err := stream.Heartbeat(ctx, 1); err != nil {
		t.Fatal(err)
	}
	finishFailure()
	select {
	case err := <-finished:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("failure did not finish")
	}
	if snapshot := admissionSnapshot(t, gate); snapshot.Latest.Phase != stageworkeragent.AssignmentClosed || !proto.Equal(snapshot.Latest.Latest, renewal.Authority) {
		t.Fatal("accepted failure did not close the renewed execution")
	}
	if _, err := stream.ExecuteAcquiredAssignment(ctx, fixture.assignment, journal.acquireID); !errors.Is(err, stageworkeragent.ErrAdmissionClosed) {
		t.Fatalf("accepted failure left stale active authority: %v", err)
	}
}

type failureRenewalControl struct {
	*recordingStreamControl
	blocked chan struct{}
	release <-chan struct{}
	renewal *velav1.StageAuthority
}

func (control *failureRenewalControl) Exchange(ctx context.Context, request *velav1.StageWorkerControlServiceConnectRequest) (*velav1.StageWorkerControlServiceConnectResponse, error) {
	if request.GetFailStage() != nil {
		close(control.blocked)
		select {
		case <-control.release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		return commandResultResponse(velav1.StageWorkerOperation_STAGE_WORKER_OPERATION_FAIL_STAGE, velav1.StageWorkerCommandDecision_STAGE_WORKER_COMMAND_DECISION_ACCEPTED), nil
	}
	response, err := control.recordingStreamControl.Exchange(ctx, request)
	if err == nil && request.GetHeartbeatStage() != nil {
		response.GetStageCommandResult().RenewedAuthority = control.renewal
	}
	return response, err
}
