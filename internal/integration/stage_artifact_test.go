//go:build integration

package integration_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/pressly/goose/v3"
	veladb "github.com/vivym/vela/internal/database"
	"github.com/vivym/vela/internal/materializationauthority"
	"github.com/vivym/vela/internal/stageartifact"
	"github.com/vivym/vela/internal/stageworkertransport"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestStageArtifactSealAndCommitReleaseGPUThenUnblockDownstream(t *testing.T) {
	database, _, coordinator, _, attemptID, encoderRunID, ditRunID :=
		newStageGraphCancellationFixture(t, "stage-artifact-commit")
	assignment := assignAndStartEncoder(
		t, database, coordinator, attemptID, encoderRunID, time.Now().Add(time.Hour),
	)
	repository, err := stageartifact.NewPostgresRepository(newRolePool(
		t, database.DSN, "vela_stage_artifact_login", "vela-stage-artifact-password",
	))
	if err != nil {
		t.Fatalf("construct StageArtifact repository: %v", err)
	}
	now := time.Now().UTC().Truncate(time.Millisecond)
	output := []byte("sealed encoder output")
	digest := sha256.Sum256(output)
	lineageDigest := sha256.Sum256([]byte("root request lineage"))
	artifactID := uuid.New()
	materializationLeaseID := uuid.New()
	objectKey := "artifacts/stage/org/project/" + attemptID.String() +
		"/encoder/output.bin"
	var workerMemberID uuid.UUID
	var workerMemberEpoch, controlSessionEpoch int64
	if err := database.Admin.QueryRow(`
		SELECT member.id, member.member_epoch, worker.control_session_epoch
		FROM worker_members AS member
		JOIN worker_instances AS worker ON worker.id = member.worker_instance_id
		WHERE member.worker_instance_id = $1
	`, assignment.WorkerInstanceID).Scan(
		&workerMemberID, &workerMemberEpoch, &controlSessionEpoch,
	); err != nil {
		t.Fatalf("read materialization source WorkerMember: %v", err)
	}
	materializationKeys := map[string][]byte{
		"materialization-key-v1": bytes.Repeat([]byte{0xa4}, 32),
	}
	materializationSigner, err := materializationauthority.NewSigner(materializationKeys)
	if err != nil {
		t.Fatalf("construct MaterializationAuthority signer: %v", err)
	}
	spiffeID := "spiffe://vela/worker/member-1"
	spiffeDigest := sha256.Sum256([]byte(spiffeID))
	materializationAuthority, err := materializationSigner.Sign(&velav1.MaterializationAuthority{
		SchemaVersion: 1, StageAuthorityDigest: bytes.Repeat([]byte{0xb1}, 32),
		StageMaterializationLeaseId: materializationLeaseID.String(),
		StageArtifactId:             artifactID.String(), ObjectKey: objectKey,
		ContentType: "application/octet-stream", Sha256: digest[:],
		SizeBytes: int64(len(output)), LocalReceiptId: "encoder-local-receipt-v1",
		LocalReceiptDigest: digest[:], SigningKeyId: "materialization-key-v1",
		IssuedAt: timestamppb.New(now), ExpiresAt: timestamppb.New(now.Add(30 * time.Minute)),
		SourceWorkerInstanceId:    assignment.WorkerInstanceID.String(),
		SourceWorkerInstanceEpoch: assignment.WorkerInstanceEpoch,
		SourceWorkerMemberId:      workerMemberID.String(), SourceWorkerMemberEpoch: workerMemberEpoch,
		SourceSpiffeIdDigest: spiffeDigest[:],
	})
	if err != nil {
		t.Fatalf("sign MaterializationAuthority: %v", err)
	}
	materializationTokenDigest, err := materializationauthority.Digest(materializationAuthority)
	if err != nil {
		t.Fatalf("digest MaterializationAuthority: %v", err)
	}
	sealed, err := repository.Seal(context.Background(), stageartifact.SealCommand{
		CommandID:              uuid.New(),
		AttemptID:              attemptID,
		StageRunID:             encoderRunID,
		StageAttemptID:         assignment.StageAttemptID,
		StageAllocationID:      assignment.StageAllocationID,
		StageLeaseID:           assignment.StageLeaseID,
		ExpectedAttemptFence:   1,
		ExpectedStageFence:     1,
		ExpectedStageVersion:   3,
		OutputPort:             "conditioning",
		LocalReceiptID:         "encoder-local-receipt-v1",
		LocalReceiptDigest:     digest,
		ManifestSHA256:         digest,
		SHA256:                 digest,
		LineageDigest:          lineageDigest,
		TokenDigest:            materializationTokenDigest,
		SizeBytes:              int64(len(output)),
		ArtifactID:             artifactID,
		MaterializationLeaseID: materializationLeaseID,
		ObjectKey:              objectKey,
		ContentType:            "application/octet-stream",
		SealedAt:               now,
		LeaseExpiresAt:         now.Add(30 * time.Minute),
	})
	if err != nil {
		t.Fatalf("seal StageArtifact output: %v", err)
	}
	if sealed.ID != materializationLeaseID || sealed.ArtifactID != artifactID ||
		sealed.SHA256 != digest || sealed.SizeBytes != int64(len(output)) {
		t.Fatalf("sealed materialization lease = %#v", sealed)
	}
	materializationValidator, err := materializationauthority.NewValidator(
		materializationKeys, func() time.Time { return now.Add(time.Second) },
	)
	if err != nil {
		t.Fatalf("construct MaterializationAuthority validator: %v", err)
	}
	verifiedMaterialization, err := materializationValidator.Validate(materializationAuthority)
	if err != nil {
		t.Fatalf("validate MaterializationAuthority: %v", err)
	}
	active, err := repository.IsActive(
		context.Background(), stageworkertransport.Identity{SPIFFEID: spiffeID},
		controlSessionEpoch, verifiedMaterialization,
	)
	if err != nil || !active {
		t.Fatalf("active MaterializationAuthority = %t error=%v", active, err)
	}
	for _, testCase := range []struct {
		name         string
		identity     stageworkertransport.Identity
		sessionEpoch int64
	}{
		{"wrong identity", stageworkertransport.Identity{SPIFFEID: spiffeID + "/forged"}, controlSessionEpoch},
		{"stale session", stageworkertransport.Identity{SPIFFEID: spiffeID}, controlSessionEpoch + 1},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			active, activeErr := repository.IsActive(
				context.Background(), testCase.identity, testCase.sessionEpoch,
				verifiedMaterialization,
			)
			if activeErr != nil || active {
				t.Fatalf("IsActive = %t error=%v", active, activeErr)
			}
		})
	}

	var runState, attemptState, allocationState, leaseState, downstreamState string
	var materializationLeases int
	if err := database.Admin.QueryRow(`
		SELECT run.state::text, physical.state::text, allocation.state::text,
		       lease.state::text, downstream.state::text,
		       (SELECT count(*) FROM stage_materialization_leases WHERE id = $6)
		FROM stage_runs AS run
		JOIN stage_attempts AS physical ON physical.id = $2
		JOIN stage_allocations AS allocation ON allocation.id = $3
		JOIN stage_leases AS lease ON lease.id = $4
		JOIN stage_runs AS downstream ON downstream.id = $5
		WHERE run.id = $1
	`, encoderRunID, assignment.StageAttemptID, assignment.StageAllocationID,
		assignment.StageLeaseID, ditRunID, materializationLeaseID).Scan(
		&runState, &attemptState, &allocationState, &leaseState,
		&downstreamState, &materializationLeases,
	); err != nil {
		t.Fatalf("inspect sealed StageArtifact authority: %v", err)
	}
	if runState != "MATERIALIZING" || attemptState != "OUTPUT_SEALED" ||
		allocationState != "RELEASED" || leaseState != "REVOKED" ||
		downstreamState != "BLOCKED" || materializationLeases != 1 {
		t.Fatalf(
			"sealed states run=%s attempt=%s allocation=%s lease=%s downstream=%s materializations=%d",
			runState, attemptState, allocationState, leaseState,
			downstreamState, materializationLeases,
		)
	}

	committedAt := now.Add(time.Second)
	forgedTokenDigest := sha256.Sum256([]byte("forged materialization authority"))
	if _, err := repository.Commit(context.Background(), stageartifact.CommitCommand{
		CommandID: uuid.New(), ProgressReceiptID: uuid.New(),
		MaterializationLeaseID: materializationLeaseID,
		ArtifactID:             artifactID, ObjectKey: sealed.ObjectKey,
		ObjectVersion: "l2-forged-version", SHA256: digest,
		SizeBytes: int64(len(output)), TokenDigest: forgedTokenDigest,
		CommittedAt: committedAt,
	}); err == nil {
		t.Fatal("commit accepted a forged MaterializationAuthority token digest")
	}
	commitCommand := stageartifact.CommitCommand{
		CommandID:              uuid.New(),
		ProgressReceiptID:      uuid.New(),
		MaterializationLeaseID: materializationLeaseID,
		ArtifactID:             artifactID,
		ObjectKey:              sealed.ObjectKey,
		ObjectVersion:          "l2-exact-version-1",
		SHA256:                 digest,
		SizeBytes:              int64(len(output)),
		TokenDigest:            materializationTokenDigest,
		CommittedAt:            committedAt,
	}
	artifact, err := repository.Commit(context.Background(), commitCommand)
	if err != nil {
		t.Fatalf("commit StageArtifact: %v", err)
	}
	if artifact.ID != artifactID || artifact.ObjectVersion != "l2-exact-version-1" {
		t.Fatalf("committed StageArtifact = %#v", artifact)
	}
	replayedArtifact, err := repository.Commit(context.Background(), commitCommand)
	if err != nil || replayedArtifact != artifact {
		t.Fatalf("replay exact StageArtifact commit = %#v error=%v", replayedArtifact, err)
	}

	var sourceState, committedAttemptState, committedDownstreamState string
	var pins, credits int
	var consumedBytes int64
	if err := database.Admin.QueryRow(`
		SELECT source.state::text, physical.state::text, downstream.state::text,
		       (SELECT count(*) FROM stage_artifact_pins
		        WHERE stage_artifact_id = $4 AND owner_stage_run_id = $3 AND state = 'ACTIVE'),
		       (SELECT count(*) FROM edge_buffer_credits
		        WHERE stage_artifact_id = $4 AND state = 'HELD'),
		       reservation.consumed_bytes
		FROM stage_runs AS source
		JOIN stage_attempts AS physical ON physical.id = $2
		JOIN stage_runs AS downstream ON downstream.id = $3
		JOIN stage_storage_reservations AS reservation ON reservation.attempt_id = source.attempt_id
		WHERE source.id = $1
	`, encoderRunID, assignment.StageAttemptID, ditRunID, artifactID).Scan(
		&sourceState, &committedAttemptState, &committedDownstreamState,
		&pins, &credits, &consumedBytes,
	); err != nil {
		t.Fatalf("inspect committed StageArtifact authority: %v", err)
	}
	if sourceState != "SUCCEEDED" || committedAttemptState != "SUCCEEDED" ||
		committedDownstreamState != "READY" || pins != 1 || credits != 1 ||
		consumedBytes != int64(len(output)) {
		t.Fatalf(
			"committed states source=%s attempt=%s downstream=%s pins=%d credits=%d bytes=%d",
			sourceState, committedAttemptState, committedDownstreamState,
			pins, credits, consumedBytes,
		)
	}

	var pinID uuid.UUID
	if err := database.Admin.QueryRow(`
		SELECT id FROM stage_artifact_pins
		WHERE stage_artifact_id = $1 AND owner_stage_run_id = $2 AND state = 'ACTIVE'
	`, artifactID, ditRunID).Scan(&pinID); err != nil {
		t.Fatalf("read downstream ExecutionPin: %v", err)
	}
	signer, err := stageartifact.NewTransferTicketSigner(
		"stage-transfer-key-v1", []byte("0123456789abcdef0123456789abcdef"),
	)
	if err != nil {
		t.Fatalf("construct TransferTicket signer: %v", err)
	}
	issuer, err := stageartifact.NewTransferTicketIssuer(repository, signer)
	if err != nil {
		t.Fatalf("construct TransferTicket issuer: %v", err)
	}
	ticketID := uuid.New()
	destination := stageartifact.TransferDestination{
		WorkerInstanceID:    assignment.WorkerInstanceID,
		WorkerInstanceEpoch: assignment.WorkerInstanceEpoch,
		ModelResidencyID:    assignment.ModelResidencyID,
		ModelRuntimeEpoch:   assignment.ModelRuntimeEpoch,
		ConnectorRevisionID: uuid.MustParse("49000000-0000-0000-0000-000000000050"),
	}
	ticketExpiresAt := committedAt.Add(5 * time.Minute)
	signed, err := issuer.Issue(context.Background(), stageartifact.IssueTransferRequest{
		CommandID: uuid.New(), TicketID: ticketID, ArtifactID: artifactID,
		PinID: pinID, Destination: destination, IssuedAt: committedAt,
		ExpiresAt: ticketExpiresAt,
	})
	if err != nil {
		t.Fatalf("issue downstream TransferTicket: %v", err)
	}
	tokenDigest := sha256.Sum256(signed.Token)
	descriptor, err := repository.Resolve(context.Background(), stageartifact.ResolveTransferCommand{
		TicketID: ticketID, TokenDigest: tokenDigest, Destination: destination,
		ResolvedAt: committedAt.Add(time.Second),
	})
	if err != nil {
		t.Fatalf("resolve downstream TransferTicket: %v", err)
	}
	if descriptor.ArtifactID != artifactID || descriptor.ObjectKey != sealed.ObjectKey ||
		descriptor.ObjectVersion != "l2-exact-version-1" || descriptor.SHA256 != digest {
		t.Fatalf("resolved downstream TransferTicket = %#v", descriptor)
	}
	if err := repository.Consume(context.Background(), stageartifact.ConsumeTransferCommand{
		CommandID: uuid.New(), TicketID: ticketID, TokenDigest: tokenDigest,
		OutcomeDigest: digest, ConsumedAt: committedAt.Add(2 * time.Second),
	}); err != nil {
		t.Fatalf("consume downstream TransferTicket: %v", err)
	}
	var ticketState, bufferCreditState string
	var bufferCreditReleased bool
	if err := database.Admin.QueryRow(`
		SELECT ticket.state::text, credit.state::text, credit.released_at IS NOT NULL
		FROM transfer_tickets AS ticket
		JOIN stage_artifact_pins AS pin ON pin.id = ticket.stage_artifact_pin_id
		JOIN edge_buffer_credits AS credit
		  ON credit.stage_artifact_id = ticket.stage_artifact_id
		 AND credit.destination_stage_run_id = pin.owner_stage_run_id
		WHERE ticket.id = $1
	`, ticketID).Scan(&ticketState, &bufferCreditState, &bufferCreditReleased); err != nil {
		t.Fatalf("read consumed TransferTicket: %v", err)
	}
	if ticketState != "CONSUMED" || bufferCreditState != "RELEASED" ||
		!bufferCreditReleased {
		t.Fatalf(
			"consumed TransferTicket state=%s buffer_credit=%s released=%t",
			ticketState, bufferCreditState, bufferCreditReleased,
		)
	}
}

func TestStageArtifactSealRefusesStorageReservationOverflowBeforeGPURelease(t *testing.T) {
	database, _, coordinator, _, attemptID, encoderRunID, _ :=
		newStageGraphCancellationFixture(t, "stage-artifact-storage-bound")
	assignment := assignAndStartEncoder(
		t, database, coordinator, attemptID, encoderRunID, time.Now().Add(time.Hour),
	)
	repository, err := stageartifact.NewPostgresRepository(newRolePool(
		t, database.DSN, "vela_stage_artifact_login", "vela-stage-artifact-password",
	))
	if err != nil {
		t.Fatalf("construct storage-bound StageArtifact repository: %v", err)
	}
	tx, err := database.Admin.Begin()
	if err != nil {
		t.Fatalf("begin storage reservation bound: %v", err)
	}
	if _, err := tx.Exec(`SET LOCAL ROLE vela_attempt_coordinator_owner`); err != nil {
		_ = tx.Rollback()
		t.Fatalf("assume AttemptCoordinator owner role: %v", err)
	}
	if _, err := tx.Exec(`
		UPDATE stage_storage_reservations SET reserved_bytes = 1 WHERE attempt_id = $1
	`, attemptID); err != nil {
		_ = tx.Rollback()
		t.Fatalf("shrink storage reservation: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit storage reservation bound: %v", err)
	}

	now := time.Now().UTC().Truncate(time.Millisecond)
	digest := sha256.Sum256([]byte("two-byte sealed output"))
	lineage := sha256.Sum256([]byte("storage-bound lineage"))
	tokenDigest := sha256.Sum256([]byte("storage-bound materialization authority"))
	leaseID := uuid.New()
	if _, err := repository.Seal(context.Background(), stageartifact.SealCommand{
		CommandID: uuid.New(), AttemptID: attemptID, StageRunID: encoderRunID,
		StageAttemptID: assignment.StageAttemptID, StageAllocationID: assignment.StageAllocationID,
		StageLeaseID: assignment.StageLeaseID, ExpectedAttemptFence: 1,
		ExpectedStageFence: 1, ExpectedStageVersion: 3, OutputPort: "conditioning",
		LocalReceiptID: "storage-bound-local-receipt", LocalReceiptDigest: digest,
		ManifestSHA256: digest, SHA256: digest, LineageDigest: lineage,
		TokenDigest: tokenDigest, SizeBytes: 2, ArtifactID: uuid.New(),
		MaterializationLeaseID: leaseID,
		ObjectKey: "artifacts/stage/org/project/" + attemptID.String() +
			"/encoder/storage-bound-output.bin",
		ContentType: "application/octet-stream", SealedAt: now,
		LeaseExpiresAt: now.Add(30 * time.Minute),
	}); err == nil {
		t.Fatal("seal issued MaterializationAuthority beyond reserved storage capacity")
	}

	var runState, attemptState, allocationState, leaseState string
	var materializationCount int
	if err := database.Admin.QueryRow(`
		SELECT run.state::text, physical.state::text, allocation.state::text,
		       lease.state::text,
		       (SELECT count(*) FROM stage_materialization_leases WHERE id = $5)
		FROM stage_runs AS run
		JOIN stage_attempts AS physical ON physical.id = $2
		JOIN stage_allocations AS allocation ON allocation.id = $3
		JOIN stage_leases AS lease ON lease.id = $4
		WHERE run.id = $1
	`, encoderRunID, assignment.StageAttemptID, assignment.StageAllocationID,
		assignment.StageLeaseID, leaseID).Scan(
		&runState, &attemptState, &allocationState, &leaseState, &materializationCount,
	); err != nil {
		t.Fatalf("inspect storage-bound seal rejection: %v", err)
	}
	if runState != "RUNNING" || attemptState != "RUNNING" || allocationState != "ALLOCATED" ||
		leaseState != "ACTIVE" || materializationCount != 0 {
		t.Fatalf(
			"storage-bound seal mutated authority run=%s attempt=%s allocation=%s lease=%s materializations=%d",
			runState, attemptState, allocationState, leaseState, materializationCount,
		)
	}
}

func TestStageArtifactLocalSourceLossRetriesComputeWithoutHoldingGPU(t *testing.T) {
	database, _, coordinator, _, attemptID, encoderRunID, _ :=
		newStageGraphCancellationFixture(t, "stage-artifact-source-lost")
	assignment := assignAndStartEncoder(
		t, database, coordinator, attemptID, encoderRunID, time.Now().Add(time.Hour),
	)
	repository, err := stageartifact.NewPostgresRepository(newRolePool(
		t, database.DSN, "vela_stage_artifact_login", "vela-stage-artifact-password",
	))
	if err != nil {
		t.Fatalf("construct source-loss StageArtifact repository: %v", err)
	}
	now := time.Now().UTC().Truncate(time.Millisecond)
	digest := sha256.Sum256([]byte("sealed output lost with node"))
	lineage := sha256.Sum256([]byte("source-loss root lineage"))
	materializationTokenDigest := sha256.Sum256([]byte("source-loss materialization authority"))
	leaseID := uuid.New()
	if _, err := repository.Seal(context.Background(), stageartifact.SealCommand{
		CommandID: uuid.New(), AttemptID: attemptID, StageRunID: encoderRunID,
		StageAttemptID:    assignment.StageAttemptID,
		StageAllocationID: assignment.StageAllocationID,
		StageLeaseID:      assignment.StageLeaseID, ExpectedAttemptFence: 1,
		ExpectedStageFence: 1, ExpectedStageVersion: 3, OutputPort: "conditioning",
		LocalReceiptID: "lost-local-receipt", LocalReceiptDigest: digest,
		ManifestSHA256: digest, SHA256: digest, LineageDigest: lineage,
		TokenDigest: materializationTokenDigest,
		SizeBytes:   64, ArtifactID: uuid.New(), MaterializationLeaseID: leaseID,
		ObjectKey: "artifacts/stage/org/project/" + attemptID.String() +
			"/encoder/lost-output.bin",
		ContentType: "application/octet-stream", SealedAt: now,
		LeaseExpiresAt: now.Add(30 * time.Minute),
	}); err != nil {
		t.Fatalf("seal source-loss StageArtifact: %v", err)
	}
	fingerprint := sha256.Sum256([]byte("node epoch disappeared before durable commit"))
	forgedTokenDigest := sha256.Sum256([]byte("forged materialization authority"))
	if _, err := repository.FailSourceLost(
		context.Background(),
		stageartifact.SourceLostCommand{
			CommandID: uuid.New(), MaterializationLeaseID: leaseID,
			TokenDigest:        forgedTokenDigest,
			FailureFingerprint: fingerprint, ConsumedResourceUnits: 100,
			LostAt: now.Add(time.Second), RetryAt: now.Add(2 * time.Second),
		},
	); err == nil {
		t.Fatal("forged materialization token authorized source loss")
	}
	var preflightRunState, preflightMaterializationState string
	var preflightAttemptsConsumed int
	if err := database.Admin.QueryRow(`
		SELECT run.state::text, materialization.state::text, budget.attempts_consumed
		FROM stage_runs AS run
		JOIN stage_materialization_leases AS materialization ON materialization.id = $2
		JOIN stage_retry_budgets AS budget ON budget.stage_run_id = run.id
		WHERE run.id = $1
	`, encoderRunID, leaseID).Scan(
		&preflightRunState, &preflightMaterializationState, &preflightAttemptsConsumed,
	); err != nil {
		t.Fatalf("inspect forged source-loss rejection: %v", err)
	}
	if preflightRunState != "MATERIALIZING" || preflightMaterializationState != "ACTIVE" ||
		preflightAttemptsConsumed != 1 {
		t.Fatalf(
			"forged source-loss mutated authority run=%s materialization=%s attempts=%d",
			preflightRunState, preflightMaterializationState, preflightAttemptsConsumed,
		)
	}
	decision, err := repository.FailSourceLost(
		context.Background(),
		stageartifact.SourceLostCommand{
			CommandID: uuid.New(), MaterializationLeaseID: leaseID,
			TokenDigest:        materializationTokenDigest,
			FailureFingerprint: fingerprint, ConsumedResourceUnits: 100,
			LostAt: now.Add(time.Second), RetryAt: now.Add(2 * time.Second),
		},
	)
	if err != nil {
		t.Fatalf("fail lost local StageArtifact source: %v", err)
	}
	if decision.State != "RETRY_WAIT" || decision.StageFence != 2 ||
		decision.StageVersion != 5 {
		t.Fatalf("source-loss decision = %#v", decision)
	}
	var runState, attemptState, allocationState, materializationState string
	var retryCount, artifactCount int
	if err := database.Admin.QueryRow(`
		SELECT run.state::text, physical.state::text, allocation.state::text,
		       materialization.state::text, run.retry_count,
		       (SELECT count(*) FROM stage_artifacts WHERE producer_stage_run_id = run.id)
		FROM stage_runs AS run
		JOIN stage_attempts AS physical ON physical.id = $2
		JOIN stage_allocations AS allocation ON allocation.id = $3
		JOIN stage_materialization_leases AS materialization ON materialization.id = $4
		WHERE run.id = $1
	`, encoderRunID, assignment.StageAttemptID, assignment.StageAllocationID, leaseID).Scan(
		&runState, &attemptState, &allocationState, &materializationState,
		&retryCount, &artifactCount,
	); err != nil {
		t.Fatalf("inspect source-loss retry authority: %v", err)
	}
	if runState != "RETRY_WAIT" || attemptState != "LOST" ||
		allocationState != "RELEASED" || materializationState != "REVOKED" ||
		retryCount != 1 || artifactCount != 0 {
		t.Fatalf(
			"source-loss states run=%s attempt=%s allocation=%s materialization=%s retry=%d artifacts=%d",
			runState, attemptState, allocationState, materializationState,
			retryCount, artifactCount,
		)
	}
}

func TestStageArtifactLocalSourceLossExhaustedBudgetFailsParentGraph(t *testing.T) {
	database, _, coordinator, _, attemptID, encoderRunID, _ :=
		newStageGraphCancellationFixture(t, "stage-artifact-source-lost-terminal")
	assignment := assignAndStartEncoder(
		t, database, coordinator, attemptID, encoderRunID, time.Now().Add(time.Hour),
	)
	repository, err := stageartifact.NewPostgresRepository(newRolePool(
		t, database.DSN, "vela_stage_artifact_login", "vela-stage-artifact-password",
	))
	if err != nil {
		t.Fatalf("construct terminal source-loss repository: %v", err)
	}
	now := time.Now().UTC().Truncate(time.Millisecond)
	digest := sha256.Sum256([]byte("last-attempt output lost before L2"))
	lineage := sha256.Sum256([]byte("terminal source-loss lineage"))
	tokenDigest := sha256.Sum256([]byte("terminal source-loss authority"))
	leaseID := uuid.New()
	if _, err := repository.Seal(context.Background(), stageartifact.SealCommand{
		CommandID: uuid.New(), AttemptID: attemptID, StageRunID: encoderRunID,
		StageAttemptID: assignment.StageAttemptID, StageAllocationID: assignment.StageAllocationID,
		StageLeaseID: assignment.StageLeaseID, ExpectedAttemptFence: 1,
		ExpectedStageFence: 1, ExpectedStageVersion: 3, OutputPort: "conditioning",
		LocalReceiptID: "terminal-lost-local-receipt", LocalReceiptDigest: digest,
		ManifestSHA256: digest, SHA256: digest, LineageDigest: lineage,
		TokenDigest: tokenDigest, SizeBytes: 64, ArtifactID: uuid.New(),
		MaterializationLeaseID: leaseID,
		ObjectKey: "artifacts/stage/org/project/" + attemptID.String() +
			"/encoder/terminal-lost-output.bin",
		ContentType: "application/octet-stream", SealedAt: now,
		LeaseExpiresAt: now.Add(30 * time.Minute),
	}); err != nil {
		t.Fatalf("seal terminal source-loss StageArtifact: %v", err)
	}
	tx, err := database.Admin.Begin()
	if err != nil {
		t.Fatalf("begin stage retry budget exhaustion: %v", err)
	}
	if _, err := tx.Exec(`SET LOCAL ROLE vela_attempt_coordinator_owner`); err != nil {
		_ = tx.Rollback()
		t.Fatalf("assume AttemptCoordinator owner role: %v", err)
	}
	if _, err := tx.Exec(`
		UPDATE stage_retry_budgets
		SET attempts_consumed = max_attempts, state = 'EXHAUSTED'
		WHERE stage_run_id = $1
	`, encoderRunID); err != nil {
		_ = tx.Rollback()
		t.Fatalf("exhaust stage retry budget: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit stage retry budget exhaustion: %v", err)
	}
	fingerprint := sha256.Sum256([]byte("last local source vanished"))
	decision, err := repository.FailSourceLost(
		context.Background(),
		stageartifact.SourceLostCommand{
			CommandID: uuid.New(), MaterializationLeaseID: leaseID, TokenDigest: tokenDigest,
			FailureFingerprint: fingerprint, ConsumedResourceUnits: 100,
			LostAt: now.Add(time.Second), RetryAt: now.Add(2 * time.Second),
		},
	)
	if err != nil {
		t.Fatalf("terminal source loss: %v", err)
	}
	if decision.State != "FAILED" || decision.StageFence != 2 || decision.StageVersion != 5 {
		t.Fatalf("terminal source-loss decision = %#v", decision)
	}
	var jobState, attemptState, graphState, runState, materializationState string
	if err := database.Admin.QueryRow(`
		SELECT job.state::text, attempt.state::text, attempt.graph_state::text,
		       run.state::text, materialization.state::text
		FROM attempts AS attempt
		JOIN jobs AS job ON job.id = attempt.job_id
		JOIN stage_runs AS run ON run.attempt_id = attempt.id AND run.id = $2
		JOIN stage_materialization_leases AS materialization
		  ON materialization.stage_run_id = run.id AND materialization.id = $3
		WHERE attempt.id = $1
	`, attemptID, encoderRunID, leaseID).Scan(
		&jobState, &attemptState, &graphState, &runState, &materializationState,
	); err != nil {
		t.Fatalf("inspect terminal source-loss graph: %v", err)
	}
	if jobState != "FAILED" || attemptState != "FAILED" || graphState != "FAILED" ||
		runState != "FAILED" || materializationState != "REVOKED" {
		t.Fatalf(
			"terminal source-loss states job=%s attempt=%s graph=%s run=%s materialization=%s",
			jobState, attemptState, graphState, runState, materializationState,
		)
	}
}

func TestStageArtifactMigrationRoundTripAndDurableAuthorityRefusal(t *testing.T) {
	migrations := filepath.Join(repositoryRoot(t), "db", "migrations")
	t.Run("empty Down Up restores exact role surface", func(t *testing.T) {
		database := newPostgres(t)
		applyFoundation(t, database.Admin)
		if err := goose.DownTo(database.Admin, migrations, 37); err != nil {
			t.Fatalf("migrate empty StageArtifact down: %v", err)
		}
		version, err := goose.GetDBVersion(database.Admin)
		if err != nil || version != 37 {
			t.Fatalf("StageArtifact version after Down = %d error=%v", version, err)
		}
		var runsRemain, artifactsRemoved, functionRemoved, winnerColumnRemoved bool
		if err := database.Admin.QueryRow(`
			SELECT to_regclass('stage_runs') IS NOT NULL,
			       to_regclass('stage_artifacts') IS NULL,
			       to_regprocedure('vela_commit_stage_artifact(jsonb)') IS NULL,
			       NOT EXISTS (
			           SELECT 1 FROM information_schema.columns
			           WHERE table_schema = 'public' AND table_name = 'stage_runs'
			             AND column_name = 'winner_stage_artifact_id'
			       )
		`).Scan(
			&runsRemain, &artifactsRemoved, &functionRemoved, &winnerColumnRemoved,
		); err != nil {
			t.Fatalf("inspect schema 37 StageArtifact rollback surface: %v", err)
		}
		if !runsRemain || !artifactsRemoved || !functionRemoved || !winnerColumnRemoved {
			t.Fatalf(
				"schema 37 StageArtifact surface runs/artifacts/function/column = %t/%t/%t/%t",
				runsRemain, artifactsRemoved, functionRemoved, winnerColumnRemoved,
			)
		}
		if err := goose.UpTo(database.Admin, migrations, 38); err != nil {
			t.Fatalf("migrate StageArtifact up again: %v", err)
		}
		stageArtifactPool := newRolePool(
			t, database.DSN, "vela_stage_artifact_login", "vela-stage-artifact-password",
		)
		if err := veladb.VerifyRole(
			context.Background(), stageArtifactPool, veladb.RoleStageArtifact,
		); err != nil {
			t.Fatalf("verify StageArtifact role after Down Up: %v", err)
		}
	})

	t.Run("durable materialization authority refuses Down", func(t *testing.T) {
		database, _, coordinator, _, attemptID, encoderRunID, _ :=
			newStageGraphCancellationFixture(t, "stage-artifact-migration-refusal")
		assignment := assignAndStartEncoder(
			t, database, coordinator, attemptID, encoderRunID, time.Now().Add(time.Hour),
		)
		repository, err := stageartifact.NewPostgresRepository(newRolePool(
			t, database.DSN, "vela_stage_artifact_login", "vela-stage-artifact-password",
		))
		if err != nil {
			t.Fatalf("construct migration-refusal StageArtifact repository: %v", err)
		}
		now := time.Now().UTC().Truncate(time.Millisecond)
		digest := sha256.Sum256([]byte("durable local materialization authority"))
		lineage := sha256.Sum256([]byte("migration refusal lineage"))
		tokenDigest := sha256.Sum256([]byte("durable materialization token"))
		if _, err := repository.Seal(context.Background(), stageartifact.SealCommand{
			CommandID: uuid.New(), AttemptID: attemptID, StageRunID: encoderRunID,
			StageAttemptID:    assignment.StageAttemptID,
			StageAllocationID: assignment.StageAllocationID,
			StageLeaseID:      assignment.StageLeaseID, ExpectedAttemptFence: 1,
			ExpectedStageFence: 1, ExpectedStageVersion: 3,
			OutputPort: "conditioning", LocalReceiptID: "migration-refusal-receipt",
			LocalReceiptDigest: digest, ManifestSHA256: digest, SHA256: digest,
			LineageDigest: lineage, TokenDigest: tokenDigest,
			SizeBytes: 128, ArtifactID: uuid.New(),
			MaterializationLeaseID: uuid.New(),
			ObjectKey: "artifacts/stage/org/project/" + attemptID.String() +
				"/encoder/migration-refusal.bin",
			ContentType: "application/octet-stream", SealedAt: now,
			LeaseExpiresAt: now.Add(30 * time.Minute),
		}); err != nil {
			t.Fatalf("seed migration-refusal materialization authority: %v", err)
		}

		err = goose.DownTo(database.Admin, migrations, 37)
		assertPostgresConstraint(t, err, "stage_artifact_rollback_is_unsafe")
		version, versionErr := goose.GetDBVersion(database.Admin)
		if versionErr != nil || version != 38 {
			t.Fatalf(
				"StageArtifact version after refused Down = %d error=%v", version, versionErr,
			)
		}
		var leases, commands int
		if err := database.Admin.QueryRow(`
			SELECT (SELECT count(*) FROM stage_materialization_leases),
			       (SELECT count(*) FROM stage_artifact_commands)
		`).Scan(&leases, &commands); err != nil {
			t.Fatalf("read StageArtifact authority after refused Down: %v", err)
		}
		if leases != 1 || commands != 1 {
			t.Fatalf("StageArtifact authority after refused Down = leases %d commands %d", leases, commands)
		}
	})
}
