package stagescheduler

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/attemptcoordinator"
)

func TestAcquireClaimsAppliesAndCommitsWinner(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.August, 30, 8, 0, 0, 0, time.UTC)
	candidate := schedulingCandidate(
		"52000000-0000-0000-0000-000000000001",
		"52000000-0000-0000-0000-000000000002",
		"52000000-0000-0000-0000-000000000003",
		now.Add(-time.Minute),
	)
	candidate.AttemptID = uuid.MustParse("52000000-0000-0000-0000-000000000004")
	candidate.StageProfileRevisionID = uuid.MustParse("52000000-0000-0000-0000-000000000005")
	snapshot := schedulingSnapshot(now, []Candidate{candidate})
	authority := testWorkerAuthority(snapshot, candidate.StageProfileRevisionID)
	observation := CapacityObservation{Sequence: snapshot.ObservationSequence}
	repository := &recordingStageRepository{captured: CapturedSnapshot{
		ID:       uuid.MustParse("52000000-0000-0000-0000-000000000006"),
		Snapshot: snapshot,
	}}
	coordinator := &recordingStageCoordinator{}
	service := testStageScheduler(t, repository, coordinator, now)

	assignment, ok, err := service.Acquire(context.Background(), authority, observation)
	if err != nil || !ok {
		t.Fatalf("Acquire() ok=%t error=%v", ok, err)
	}
	if repository.claimed == nil || repository.committedClaimID != repository.claimed.ClaimID {
		t.Fatalf("claim lifecycle = claimed %#v committed %s", repository.claimed, repository.committedClaimID)
	}
	if coordinator.command == nil {
		t.Fatal("AttemptCoordinator.Apply was not called")
	}
	command := *coordinator.command
	if command.StageRunID != candidate.StageRunID || command.AttemptID != candidate.AttemptID ||
		command.StageProfileRevisionID != candidate.StageProfileRevisionID ||
		command.WorkerInstanceID != authority.WorkerInstanceID ||
		command.WorkerInstanceEpoch != authority.WorkerInstanceEpoch ||
		command.ModelResidencyID != authority.ModelResidencyID ||
		command.ModelRuntimeEpoch != authority.ModelRuntimeEpoch {
		t.Fatalf("assignment command = %#v", command)
	}
	if !bytes.Equal(command.DeviceSetDigest, authority.DeviceSetDigest) ||
		!bytes.Equal(command.MembershipDigest, authority.MembershipDigest) ||
		len(command.TokenDigest) != 32 || len(command.ExecutionNonce) != 32 {
		t.Fatalf("assignment command evidence = %#v", command)
	}
	if assignment.StageRunID != candidate.StageRunID ||
		assignment.StageAttemptID != command.StageAttemptID ||
		assignment.StageLeaseID != command.StageLeaseID || len(assignment.LeaseToken) != 32 {
		t.Fatalf("Assignment = %#v", assignment)
	}
	if !bytes.Equal(command.TokenDigest, digestBytes(assignment.LeaseToken)) {
		t.Fatal("Assignment Lease token does not match persisted token digest")
	}
}

func TestAcquireReturnsNoWorkWithoutClaiming(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.August, 30, 8, 0, 0, 0, time.UTC)
	snapshot := schedulingSnapshot(now, nil)
	repository := &recordingStageRepository{captured: CapturedSnapshot{
		ID:       uuid.MustParse("52000000-0000-0000-0000-000000000010"),
		Snapshot: snapshot,
	}}
	coordinator := &recordingStageCoordinator{}
	service := testStageScheduler(t, repository, coordinator, now)

	assignment, ok, err := service.Acquire(
		context.Background(),
		testWorkerAuthority(snapshot, uuid.MustParse("52000000-0000-0000-0000-000000000011")),
		CapacityObservation{Sequence: snapshot.ObservationSequence},
	)
	if err != nil || ok || assignment != (Assignment{}) {
		t.Fatalf("Acquire(no work) = %#v ok=%t error=%v", assignment, ok, err)
	}
	if repository.claimed != nil || coordinator.command != nil {
		t.Fatal("NoWork path claimed or applied an assignment")
	}
}

func TestAcquireAbandonsClaimWhenAttemptCoordinatorRejectsAssignment(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.August, 30, 8, 0, 0, 0, time.UTC)
	candidate := schedulingCandidate(
		"52000000-0000-0000-0000-000000000020",
		"52000000-0000-0000-0000-000000000021",
		"52000000-0000-0000-0000-000000000022",
		now.Add(-time.Minute),
	)
	snapshot := schedulingSnapshot(now, []Candidate{candidate})
	repository := &recordingStageRepository{captured: CapturedSnapshot{
		ID:       uuid.MustParse("52000000-0000-0000-0000-000000000023"),
		Snapshot: snapshot,
	}}
	applyErr := errors.New("stale StageRun authority")
	coordinator := &recordingStageCoordinator{err: applyErr}
	service := testStageScheduler(t, repository, coordinator, now)

	_, ok, err := service.Acquire(
		context.Background(),
		testWorkerAuthority(snapshot, candidate.StageProfileRevisionID),
		CapacityObservation{Sequence: snapshot.ObservationSequence},
	)
	if ok || !errors.Is(err, applyErr) {
		t.Fatalf("Acquire(rejected) ok=%t error=%v", ok, err)
	}
	if repository.claimed == nil || repository.abandonedClaimID != repository.claimed.ClaimID ||
		repository.abandonReason != AbandonAssignmentRejected {
		t.Fatalf(
			"abandon = claim %s reason %q, claimed %#v",
			repository.abandonedClaimID,
			repository.abandonReason,
			repository.claimed,
		)
	}
}

func testStageScheduler(
	t *testing.T,
	repository stageRepository,
	coordinator stageAttemptCoordinator,
	now time.Time,
) *Service {
	t.Helper()
	service, err := NewService(repository, coordinator, Config{
		SchedulerID:      "stage-scheduler/test",
		ClaimTTL:         30 * time.Second,
		LeaseTTL:         time.Minute,
		LocalDeadlineTTL: 50 * time.Second,
		SigningKeyID:     "stage-authority-key-v1",
		Now:              func() time.Time { return now },
		Random:           bytes.NewReader(bytes.Repeat([]byte{0x71}, 512)),
	})
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}
	return service
}

func testWorkerAuthority(snapshot Snapshot, profileID uuid.UUID) WorkerAuthority {
	return WorkerAuthority{
		CapacityPoolID:         snapshot.CapacityPoolID,
		StageProfileRevisionID: profileID,
		WorkerInstanceID:       snapshot.WorkerInstanceID,
		WorkerInstanceEpoch:    snapshot.WorkerInstanceEpoch,
		DeviceSetDigest:        bytes.Repeat([]byte{0x51}, 32),
		MembershipDigest:       bytes.Repeat([]byte{0x52}, 32),
		ModelResidencyID:       uuid.MustParse("52000000-0000-0000-0000-000000000030"),
		ModelRuntimeEpoch:      7,
		CapacityVector:         map[string]int64{"concurrency": 1},
	}
}

type recordingStageRepository struct {
	captured         CapturedSnapshot
	claimed          *ClaimRequest
	committedClaimID uuid.UUID
	abandonedClaimID uuid.UUID
	abandonReason    string
}

func (repository *recordingStageRepository) Capture(
	context.Context,
	WorkerAuthority,
	CapacityObservation,
) (CapturedSnapshot, error) {
	return repository.captured, nil
}

func (repository *recordingStageRepository) Claim(
	_ context.Context,
	request ClaimRequest,
) (ClaimResult, error) {
	repository.claimed = &request
	return ClaimResult{ClaimID: request.ClaimID}, nil
}

func (repository *recordingStageRepository) Commit(
	_ context.Context,
	claimID uuid.UUID,
	_ uuid.UUID,
) error {
	repository.committedClaimID = claimID
	return nil
}

func (repository *recordingStageRepository) Abandon(
	_ context.Context,
	claimID uuid.UUID,
	reason string,
) error {
	repository.abandonedClaimID = claimID
	repository.abandonReason = reason
	return nil
}

type recordingStageCoordinator struct {
	command *attemptcoordinator.AssignStageCommand
	err     error
}

func (coordinator *recordingStageCoordinator) Apply(
	_ context.Context,
	command attemptcoordinator.StageCommand,
) (attemptcoordinator.StageDecision, error) {
	assignment, ok := command.(attemptcoordinator.AssignStageCommand)
	if !ok {
		return attemptcoordinator.StageDecision{}, errors.New("unexpected Stage command")
	}
	coordinator.command = &assignment
	if coordinator.err != nil {
		return attemptcoordinator.StageDecision{}, coordinator.err
	}
	return attemptcoordinator.StageDecision{
		StageRunID:     assignment.StageRunID,
		StageAttemptID: assignment.StageAttemptID,
		State:          "ASSIGNED",
		StageFence:     assignment.ExpectedStageFence,
		StageVersion:   assignment.ExpectedStageVersion + 1,
	}, nil
}
