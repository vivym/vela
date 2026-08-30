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

func TestAcquireIdentifiedUsesStableAssignmentAuthority(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.August, 30, 8, 0, 0, 0, time.UTC)
	candidate := schedulingCandidate(
		"52000000-0000-0000-0000-000000000060",
		"52000000-0000-0000-0000-000000000061",
		"52000000-0000-0000-0000-000000000062",
		now.Add(-time.Minute),
	)
	candidate.AttemptID = uuid.MustParse("52000000-0000-0000-0000-000000000063")
	candidate.StageProfileRevisionID = uuid.MustParse("52000000-0000-0000-0000-000000000064")
	snapshot := schedulingSnapshot(now, []Candidate{candidate})
	authority := testWorkerAuthority(snapshot, candidate.StageProfileRevisionID)
	repository := &recordingStageRepository{captured: CapturedSnapshot{
		ID:       uuid.MustParse("52000000-0000-0000-0000-000000000065"),
		Snapshot: snapshot,
	}}
	coordinator := &recordingStageCoordinator{}
	service := testStageScheduler(t, repository, coordinator, now.Add(time.Hour))
	identity := testAssignmentIdentity(now)

	assignment, ok, err := service.AcquireIdentified(
		context.Background(), authority,
		CapacityObservation{Sequence: snapshot.ObservationSequence}, identity,
	)
	if err != nil || !ok {
		t.Fatalf("AcquireIdentified() ok=%t error=%v", ok, err)
	}
	if repository.recoveredClaimID != identity.ClaimID || repository.claimed == nil ||
		repository.captureCalls != 1 || coordinator.command == nil {
		t.Fatalf(
			"identified lifecycle = recovered %s captures %d claimed %#v command %#v",
			repository.recoveredClaimID, repository.captureCalls,
			repository.claimed, coordinator.command,
		)
	}
	command := *coordinator.command
	if assignment.ClaimID != identity.ClaimID || assignment.DecisionID != identity.DecisionID ||
		assignment.StageAttemptID != identity.StageAttemptID ||
		assignment.StageAllocationID != identity.StageAllocationID ||
		assignment.StageLeaseID != identity.StageLeaseID ||
		assignment.LeaseToken != identity.LeaseToken ||
		command.CommandID != identity.CommandID ||
		!bytes.Equal(command.ExecutionNonce, identity.ExecutionNonce[:]) ||
		!command.IssuedAt.Equal(identity.IssuedAt) ||
		!assignment.LeaseExpiresAt.Equal(identity.IssuedAt.Add(time.Minute)) ||
		!assignment.LocalDeadlineAt.Equal(identity.IssuedAt.Add(50*time.Second)) {
		t.Fatalf("identified Assignment = %#v command = %#v", assignment, command)
	}
}

func TestAcquireIdentifiedResumesClaimedAssignment(t *testing.T) {
	t.Parallel()

	now, authority, observation, identity, recovered := testRecoveredClaim(t, "CLAIMED")
	repository := &recordingStageRepository{recovered: recovered, recoveredExists: true}
	coordinator := &recordingStageCoordinator{}
	service := testStageScheduler(t, repository, coordinator, now.Add(time.Hour))

	assignment, ok, err := service.AcquireIdentified(
		context.Background(), authority, observation, identity,
	)
	if err != nil || !ok {
		t.Fatalf("AcquireIdentified(CLAIMED) ok=%t error=%v", ok, err)
	}
	if repository.captureCalls != 0 || repository.claimed != nil ||
		repository.committedClaimID != identity.ClaimID ||
		repository.committedStageAttemptID != identity.StageAttemptID ||
		coordinator.command == nil || coordinator.command.CommandID != identity.CommandID {
		t.Fatalf(
			"resumed lifecycle = captures %d claimed %#v committed %s/%s command %#v",
			repository.captureCalls, repository.claimed,
			repository.committedClaimID, repository.committedStageAttemptID,
			coordinator.command,
		)
	}
	expected := recovered.Assignment
	expected.LeaseToken = identity.LeaseToken
	if assignment != expected {
		t.Fatalf("resumed Assignment = %#v, want %#v", assignment, expected)
	}
}

func TestAcquireIdentifiedReplaysCommittedAssignmentWithoutApplying(t *testing.T) {
	t.Parallel()

	now, authority, observation, identity, recovered := testRecoveredClaim(t, "COMMITTED")
	repository := &recordingStageRepository{recovered: recovered, recoveredExists: true}
	coordinator := &recordingStageCoordinator{}
	service := testStageScheduler(t, repository, coordinator, now.Add(time.Hour))

	assignment, ok, err := service.AcquireIdentified(
		context.Background(), authority, observation, identity,
	)
	if err != nil || !ok {
		t.Fatalf("AcquireIdentified(COMMITTED) ok=%t error=%v", ok, err)
	}
	if repository.captureCalls != 0 || repository.claimed != nil ||
		repository.committedClaimID != uuid.Nil || coordinator.command != nil {
		t.Fatalf(
			"committed replay performed work: captures %d claimed %#v committed %s command %#v",
			repository.captureCalls, repository.claimed,
			repository.committedClaimID, coordinator.command,
		)
	}
	expected := recovered.Assignment
	expected.LeaseToken = identity.LeaseToken
	if assignment != expected {
		t.Fatalf("replayed Assignment = %#v, want %#v", assignment, expected)
	}
}

func TestAcquireIdentifiedRejectsChangedRecoveryAuthority(t *testing.T) {
	t.Parallel()

	now, authority, observation, identity, recovered := testRecoveredClaim(t, "COMMITTED")
	tests := []struct {
		name      string
		authority WorkerAuthority
		identity  AssignmentIdentity
	}{
		{name: "lease token", authority: authority, identity: func() AssignmentIdentity {
			changed := identity
			changed.LeaseToken[0]++
			return changed
		}()},
		{name: "worker epoch", authority: func() WorkerAuthority {
			changed := authority
			changed.WorkerInstanceEpoch++
			return changed
		}(), identity: identity},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			repository := &recordingStageRepository{recovered: recovered, recoveredExists: true}
			coordinator := &recordingStageCoordinator{}
			service := testStageScheduler(t, repository, coordinator, now.Add(time.Hour))

			assignment, ok, err := service.AcquireIdentified(
				context.Background(), test.authority, observation, test.identity,
			)
			if err == nil || ok || assignment != (Assignment{}) {
				t.Fatalf(
					"AcquireIdentified(changed %s) = %#v ok=%t error=%v",
					test.name, assignment, ok, err,
				)
			}
			if repository.captureCalls != 0 || repository.committedClaimID != uuid.Nil ||
				coordinator.command != nil {
				t.Fatal("changed recovery authority performed assignment work")
			}
		})
	}
}

func TestReplayShadowPersistsDeterministicReceipt(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.August, 30, 8, 0, 0, 0, time.UTC)
	candidate := schedulingCandidate(
		"52000000-0000-0000-0000-000000000040",
		"52000000-0000-0000-0000-000000000041",
		"52000000-0000-0000-0000-000000000042",
		now.Add(-time.Minute),
	)
	snapshot := schedulingSnapshot(now, []Candidate{candidate})
	expected, selected, err := Decide(snapshot)
	if err != nil || !selected {
		t.Fatalf("prepare shadow evidence selected=%t error=%v", selected, err)
	}
	repository := &recordingStageRepository{shadow: []ShadowSnapshot{{
		ID:                     uuid.MustParse("52000000-0000-0000-0000-000000000043"),
		Snapshot:               snapshot,
		ExpectedEvidenceDigest: expected.EvidenceDigest,
	}}}
	service := testStageScheduler(t, repository, &recordingStageCoordinator{}, now)

	summary, err := service.ReplayShadow(context.Background(), 10)
	if err != nil {
		t.Fatalf("ReplayShadow: %v", err)
	}
	if summary.Processed != 1 || summary.Matched != 1 || summary.Diverged != 0 {
		t.Fatalf("ReplayShadow summary = %#v", summary)
	}
	if len(repository.shadowReceipts) != 1 {
		t.Fatalf("shadow receipts = %#v", repository.shadowReceipts)
	}
	receipt := repository.shadowReceipts[0]
	if receipt.ID == uuid.Nil || receipt.SnapshotID != repository.shadow[0].ID ||
		receipt.AlgorithmRevision != AlgorithmRevisionV1 ||
		receipt.ExpectedEvidenceDigest != expected.EvidenceDigest ||
		receipt.ReplayedEvidenceDigest != expected.EvidenceDigest || !receipt.Matched ||
		receipt.ReplayedAt != now || receipt.ReplayedBy != "stage-scheduler/test" {
		t.Fatalf("shadow receipt = %#v", receipt)
	}

	firstReceiptID := receipt.ID
	repository.shadowReceipts = nil
	if _, err := service.ReplayShadow(context.Background(), 10); err != nil {
		t.Fatalf("ReplayShadow retry: %v", err)
	}
	if len(repository.shadowReceipts) != 1 ||
		repository.shadowReceipts[0].ID != firstReceiptID {
		t.Fatalf("shadow retry receipts = %#v, want receipt %s", repository.shadowReceipts, firstReceiptID)
	}
}

func TestReconcileExpiredClaimsUsesBoundedRepositoryOperation(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.August, 30, 8, 0, 0, 0, time.UTC)
	repository := &recordingStageRepository{reconciled: 3}
	service := testStageScheduler(t, repository, &recordingStageCoordinator{}, now)

	processed, err := service.ReconcileExpired(context.Background(), 25)
	if err != nil || processed != 3 || repository.reconcileLimit != 25 {
		t.Fatalf(
			"ReconcileExpired processed=%d limit=%d error=%v",
			processed,
			repository.reconcileLimit,
			err,
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

func testAssignmentIdentity(now time.Time) AssignmentIdentity {
	return AssignmentIdentity{
		ClaimID:           uuid.MustParse("52000000-0000-0000-0000-000000000070"),
		DecisionID:        uuid.MustParse("52000000-0000-0000-0000-000000000071"),
		CommandID:         uuid.MustParse("52000000-0000-0000-0000-000000000072"),
		StageAttemptID:    uuid.MustParse("52000000-0000-0000-0000-000000000073"),
		StageAllocationID: uuid.MustParse("52000000-0000-0000-0000-000000000074"),
		StageLeaseID:      uuid.MustParse("52000000-0000-0000-0000-000000000075"),
		LeaseToken:        [32]byte{0x76, 0x76},
		ExecutionNonce:    [32]byte{0x77, 0x77},
		IssuedAt:          now,
	}
}

func testRecoveredClaim(
	t *testing.T,
	state string,
) (time.Time, WorkerAuthority, CapacityObservation, AssignmentIdentity, RecoveredClaim) {
	t.Helper()
	now := time.Date(2026, time.August, 30, 9, 0, 0, 0, time.UTC)
	candidate := schedulingCandidate(
		"52000000-0000-0000-0000-000000000080",
		"52000000-0000-0000-0000-000000000081",
		"52000000-0000-0000-0000-000000000082",
		now.Add(-time.Minute),
	)
	candidate.AttemptID = uuid.MustParse("52000000-0000-0000-0000-000000000083")
	candidate.StageProfileRevisionID = uuid.MustParse("52000000-0000-0000-0000-000000000084")
	snapshot := schedulingSnapshot(now, []Candidate{candidate})
	authority := testWorkerAuthority(snapshot, candidate.StageProfileRevisionID)
	evidence, selected, err := Decide(snapshot)
	if err != nil || !selected {
		t.Fatalf("prepare recovered claim selected=%t error=%v", selected, err)
	}
	identity := testAssignmentIdentity(now)
	service := testStageScheduler(
		t, &recordingStageRepository{}, &recordingStageCoordinator{}, now,
	)
	assignment, command, err := service.newAssignment(
		authority, snapshot.ObservationSequence, evidence, &identity,
	)
	if err != nil {
		t.Fatalf("prepare recovered assignment: %v", err)
	}
	assignment.LeaseToken = [32]byte{}
	return now, authority, CapacityObservation{Sequence: snapshot.ObservationSequence}, identity,
		RecoveredClaim{State: state, Assignment: assignment, Command: command}
}

type recordingStageRepository struct {
	captured                CapturedSnapshot
	captureCalls            int
	recovered               RecoveredClaim
	recoveredExists         bool
	recoveredClaimID        uuid.UUID
	claimed                 *ClaimRequest
	committedClaimID        uuid.UUID
	committedStageAttemptID uuid.UUID
	abandonedClaimID        uuid.UUID
	abandonReason           string
	shadow                  []ShadowSnapshot
	shadowReceipts          []ShadowReplayReceipt
	reconciled              int64
	reconcileLimit          int
}

func (repository *recordingStageRepository) Recover(
	_ context.Context,
	claimID uuid.UUID,
) (RecoveredClaim, bool, error) {
	repository.recoveredClaimID = claimID
	return repository.recovered, repository.recoveredExists, nil
}

func (repository *recordingStageRepository) Capture(
	context.Context,
	WorkerAuthority,
	CapacityObservation,
) (CapturedSnapshot, error) {
	repository.captureCalls++
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
	stageAttemptID uuid.UUID,
) error {
	repository.committedClaimID = claimID
	repository.committedStageAttemptID = stageAttemptID
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

func (repository *recordingStageRepository) ListShadowSnapshots(
	_ context.Context,
	_ int,
) ([]ShadowSnapshot, error) {
	return append([]ShadowSnapshot(nil), repository.shadow...), nil
}

func (repository *recordingStageRepository) RecordShadowReplay(
	_ context.Context,
	receipt ShadowReplayReceipt,
) error {
	repository.shadowReceipts = append(repository.shadowReceipts, receipt)
	return nil
}

func (repository *recordingStageRepository) ReconcileExpired(
	_ context.Context,
	limit int,
) (int64, error) {
	repository.reconcileLimit = limit
	return repository.reconciled, nil
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
