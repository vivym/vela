//go:build integration

package integration_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/pressly/goose/v3"
	"github.com/vivym/vela/internal/artifactstore"
	"github.com/vivym/vela/internal/attemptcoordinator"
	"github.com/vivym/vela/internal/stageartifact"
	"github.com/vivym/vela/internal/stageassignment"
	"github.com/vivym/vela/internal/stageauthority"
	"github.com/vivym/vela/internal/stagescheduler"
	"github.com/vivym/vela/internal/stageworkercontrol"
	"github.com/vivym/vela/internal/stageworkertransport"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/protobuf/proto"
)

func TestEncoderAssignmentBindsFrozenH3ParametersAndRootMaterial(t *testing.T) {
	const digestHex = "d7a8fbb307d7809469ca9abcb0082e4f8d5651e46d3cdb762d02d0bf37c9e592"
	fixture := newStageSchedulerFixtureWithRequest(
		t,
		"h3-root-material",
		[]byte(`{
			"model":"minimax-h3",
			"generation_preset":"balanced",
			"service_class":"standard",
			"output_spec":"video-1080p-5s-24fps",
			"generation_count":1,
			"prompt":"animate the reference",
			"h3":{
				"task":"ref2va",
				"seed":17,
				"conditions":[{
					"role":"reference",
					"type":"image",
					"uri":"vela://uploads/reference-frame",
					"download_url":"https://objects.example.test/reference?signature=worker-only",
					"sha256":"`+digestHex+`",
					"size_bytes":4096
				}],
				"target":{"short_edge":768,"aspect_ratio":"16:9","duration_seconds":5},
				"sampling":{"num_inference_steps":30,"quality":"lossless"}
			}
		}`),
	)
	result, err := newPostgresAssignmentTestBackend(t, fixture).AcquireStage(
		context.Background(),
		stageWorkerAcquireCommand(fixture),
		stageWorkerAcquireRequest(fixture),
	)
	if err != nil || result.Assignment == nil {
		t.Fatalf("Acquire Encoder = %#v error=%v", result, err)
	}
	assignment := result.Assignment
	if _, err := stageassignment.Validate(assignment); err != nil {
		t.Fatalf("validate Encoder Assignment: %v", err)
	}
	var parameters struct {
		SchemaRevision   int `json:"schema_revision"`
		CanonicalRequest struct {
			Task       string `json:"task"`
			Prompt     string `json:"prompt"`
			Seed       int64  `json:"seed"`
			Conditions []struct {
				URI string `json:"uri"`
			} `json:"conditions"`
		} `json:"canonical_request"`
		Sampling struct {
			NumInferenceSteps int    `json:"num_inference_steps"`
			Quality           string `json:"quality"`
		} `json:"sampling"`
	}
	if err := json.Unmarshal(assignment.GetExecutionSpec().GetParametersJson(), &parameters); err != nil {
		t.Fatalf("decode fast-h3 parameters: %v", err)
	}
	if parameters.SchemaRevision != 1 || parameters.CanonicalRequest.Task != "ref2va" ||
		parameters.CanonicalRequest.Prompt != "animate the reference" ||
		parameters.CanonicalRequest.Seed != 17 || len(parameters.CanonicalRequest.Conditions) != 1 ||
		parameters.CanonicalRequest.Conditions[0].URI != "vela://uploads/reference-frame" ||
		parameters.Sampling.NumInferenceSteps != 30 || parameters.Sampling.Quality != "lossless" {
		t.Fatalf("fast-h3 parameters = %#v", parameters)
	}
	if bytes.Contains(assignment.GetExecutionSpec().GetParametersJson(), []byte("download_url")) ||
		bytes.Contains(assignment.GetExecutionSpec().GetParametersJson(), []byte("generation_preset")) {
		t.Fatalf("backend parameters expose non-fast-h3 fields: %s", assignment.GetExecutionSpec().GetParametersJson())
	}
	digest, _ := hex.DecodeString(digestHex)
	rootInputs := assignment.GetExecutionSpec().GetRootInputs()
	fetches := assignment.GetRootInputFetches()
	if len(rootInputs) != 1 || rootInputs[0].GetConditionIndex() != 0 ||
		rootInputs[0].GetUri() != "vela://uploads/reference-frame" ||
		!bytes.Equal(rootInputs[0].GetSha256(), digest) || rootInputs[0].GetSizeBytes() != 4096 ||
		len(fetches) != 1 || fetches[0].GetDownloadUrl() !=
		"https://objects.example.test/reference?signature=worker-only" ||
		!bytes.Equal(fetches[0].GetSha256(), digest) {
		t.Fatalf("root inputs/fetches = %#v / %#v", rootInputs, fetches)
	}
}

func TestPostgresAssignmentBackendReplaysExactAssignment(t *testing.T) {
	fixture := newStageSchedulerFixture(t, "stage-worker-assignment-replay")
	backend := newPostgresAssignmentTestBackend(t, fixture)
	command := stageWorkerAcquireCommand(fixture)
	request := stageWorkerAcquireRequest(fixture)

	first, err := backend.AcquireStage(context.Background(), command, request)
	if err != nil || first.Assignment == nil || first.Command != nil || first.RetryAfter != 0 {
		t.Fatalf("first AcquireStage = %#v error=%v", first, err)
	}
	if _, err := stageassignment.Validate(first.Assignment); err != nil {
		t.Fatalf("validate first StageAssignment: %v", err)
	}
	if first.Assignment.GetAuthority().GetModelRuntimeBarrierGeneration() !=
		request.GetModelRuntimeEpoch() {
		t.Fatalf(
			"StageAssignment barrier generation = %d, want %d",
			first.Assignment.GetAuthority().GetModelRuntimeBarrierGeneration(),
			request.GetModelRuntimeEpoch(),
		)
	}
	members := first.Assignment.GetAuthority().GetMembers()
	identityDigest := sha256.Sum256([]byte(command.Identity.SPIFFEID))
	if len(members) != 1 || !bytes.Equal(members[0].GetIdentityDigest(), identityDigest[:]) {
		t.Fatalf("StageAssignment signed member identity digest = %#v", members)
	}
	validator, err := stageauthority.NewValidator(
		map[string][]byte{"stage-authority-key-v1": bytes.Repeat([]byte{0x9a}, 32)},
		func() time.Time { return first.Assignment.GetAuthority().GetIssuedAt().AsTime().Add(time.Millisecond) },
	)
	if err != nil {
		t.Fatalf("construct StageAuthority validator: %v", err)
	}
	verified, err := validator.ValidateEnvelope(first.Assignment.GetAuthority())
	if err != nil {
		t.Fatalf("verify StageAssignment authority: %v", err)
	}
	authorizer, err := stageworkercontrol.NewPostgresAuthorizer(newRolePool(
		t, fixture.database.DSN,
		"vela_stage_worker_control_login", "vela-stage-worker-control-password",
	))
	if err != nil {
		t.Fatalf("construct StageAuthority durable authorizer: %v", err)
	}
	active, err := authorizer.IsActive(
		context.Background(), command.Identity, command.ControlSessionEpoch,
		stageworkercontrol.OperationStartStage, verified,
	)
	if err != nil || !active {
		t.Fatalf("authorize StageAssignment barrier = %t error=%v", active, err)
	}
	firstWire, err := proto.MarshalOptions{Deterministic: true}.Marshal(first.Assignment)
	if err != nil {
		t.Fatalf("marshal first StageAssignment: %v", err)
	}

	second, err := backend.AcquireStage(context.Background(), command, request)
	if err != nil || second.Assignment == nil || second.Command != nil || second.RetryAfter != 0 {
		t.Fatalf("replayed AcquireStage = %#v error=%v", second, err)
	}
	secondWire, err := proto.MarshalOptions{Deterministic: true}.Marshal(second.Assignment)
	if err != nil {
		t.Fatalf("marshal replayed StageAssignment: %v", err)
	}
	if !bytes.Equal(firstWire, secondWire) {
		t.Fatal("replayed StageAssignment wire changed")
	}

	var attempts, leases, intents, results int
	if err := fixture.database.Admin.QueryRow(`
		SELECT
			(SELECT count(*) FROM stage_attempts WHERE stage_run_id = $1),
			(SELECT count(*) FROM stage_leases WHERE stage_run_id = $1),
			(SELECT count(*) FROM stage_worker_acquire_intents WHERE command_id = $2),
			(SELECT count(*) FROM stage_worker_acquire_results WHERE command_id = $2)
	`, fixture.stageRunID, command.CommandID).Scan(
		&attempts, &leases, &intents, &results,
	); err != nil {
		t.Fatalf("read durable StageAssignment replay evidence: %v", err)
	}
	if attempts != 1 || leases != 1 || intents != 1 || results != 1 {
		t.Fatalf(
			"durable Assignment rows attempts=%d leases=%d intents=%d results=%d",
			attempts, leases, intents, results,
		)
	}
}

func TestPostgresAssignmentBackendAssignsCertifiedConnectorInput(t *testing.T) {
	database, coordinator, _, attemptID := newH3IntegrationGraphFixture(
		t, "stage-worker-certified-connector-input",
	)
	seedWorkerRegistryPlan(t, database.Admin)
	stages := h3IntegrationStages(
		[]string{"encoder-node-01", "dit-node-09", "vae-node-03"}, nil,
	)
	var encoderRunID, ditRunID uuid.UUID
	if err := database.Admin.QueryRow(`
		SELECT
			(SELECT id FROM stage_runs WHERE attempt_id = $1 AND stage_key = 'encoder'),
			(SELECT id FROM stage_runs WHERE attempt_id = $1 AND stage_key = 'dit')
	`, attemptID).Scan(&encoderRunID, &ditRunID); err != nil {
		t.Fatalf("read Encoder/DiT StageRuns: %v", err)
	}

	encoderFixture := newH3AssignmentWorkerFixture(
		t, database, coordinator, encoderRunID, stages[0], 0xd1,
	)
	encoderResult, err := newPostgresAssignmentTestBackend(t, encoderFixture).AcquireStage(
		context.Background(),
		stageWorkerAcquireCommand(encoderFixture),
		stageWorkerAcquireRequest(encoderFixture),
	)
	if err != nil || encoderResult.Assignment == nil {
		t.Fatalf("Acquire Encoder = %#v error=%v", encoderResult, err)
	}
	validator, err := stageauthority.NewValidator(
		map[string][]byte{"stage-authority-key-v1": bytes.Repeat([]byte{0x9a}, 32)},
		func() time.Time {
			return encoderResult.Assignment.GetAuthority().GetIssuedAt().AsTime().Add(time.Millisecond)
		},
	)
	if err != nil {
		t.Fatalf("construct Encoder StageAuthority validator: %v", err)
	}
	verified, err := validator.ValidateEnvelope(encoderResult.Assignment.GetAuthority())
	if err != nil {
		t.Fatalf("verify Encoder StageAssignment authority: %v", err)
	}
	authority := verified.Authority
	encoderAssignment := attemptcoordinator.AssignStageCommand{
		AttemptID:              uuid.MustParse(authority.GetAttemptId()),
		StageRunID:             uuid.MustParse(authority.GetStageRunId()),
		ExpectedStageVersion:   authority.GetStageVersion() - 1,
		StageAttemptID:         uuid.MustParse(authority.GetStageAttemptId()),
		StageAllocationID:      uuid.MustParse(authority.GetStageAllocationId()),
		StageLeaseID:           uuid.MustParse(authority.GetStageLeaseId()),
		WorkerInstanceID:       uuid.MustParse(authority.GetWorkerInstanceId()),
		WorkerInstanceEpoch:    authority.GetWorkerInstanceEpoch(),
		ModelResidencyID:       uuid.MustParse(authority.GetModelResidencyId()),
		ModelRuntimeEpoch:      authority.GetModelRuntimeBarrierGeneration(),
		StageProfileRevisionID: uuid.MustParse(authority.GetStageProfileRevisionId()),
		IssuedAt:               authority.GetIssuedAt().AsTime(),
	}
	started := startH3IntegrationStage(t, database, encoderAssignment, verified)
	if started.Authority.GetStageVersion() != authority.GetStageVersion()+1 {
		t.Fatalf("started Encoder StageVersion = %d", started.Authority.GetStageVersion())
	}
	artifactRepository, err := stageartifact.NewPostgresRepository(newRolePool(
		t, database.DSN, "vela_stage_artifact_login", "vela-stage-artifact-password",
	))
	if err != nil {
		t.Fatalf("construct Encoder StageArtifact repository: %v", err)
	}
	objectStore := artifactstore.NewLocal()
	encoderArtifact := materializeH3IntegrationStage(
		t, artifactRepository, objectStore, attemptID, encoderRunID,
		encoderAssignment, stages[0], []byte("certified connector input"),
		[]byte(`{"kind":"conditioning","schema_version":1}`),
	)

	ditFixture := newH3AssignmentWorkerFixture(
		t, database, coordinator, ditRunID, stages[1], 0xd2,
	)
	ditResult, err := newPostgresAssignmentTestBackend(t, ditFixture).AcquireStage(
		context.Background(),
		stageWorkerAcquireCommand(ditFixture),
		stageWorkerAcquireRequest(ditFixture),
	)
	if err != nil || ditResult.Assignment == nil {
		t.Fatalf("Acquire DiT with CERTIFIED Connector = %#v error=%v", ditResult, err)
	}
	inputs := ditResult.Assignment.GetExecutionSpec().GetInputs()
	tickets := ditResult.Assignment.GetInputTransferTickets()
	if len(inputs) != 1 || len(tickets) != 1 ||
		inputs[0].GetStageArtifactId() != encoderArtifact.ID.String() ||
		inputs[0].GetObjectVersion() != encoderArtifact.ObjectVersion ||
		!bytes.Equal(inputs[0].GetSha256(), encoderArtifact.SHA256[:]) ||
		inputs[0].GetSizeBytes() != encoderArtifact.SizeBytes ||
		inputs[0].GetStageInterfaceRevisionId() != stages[0].outputInterface ||
		tickets[0].GetStageArtifactId() != encoderArtifact.ID.String() ||
		tickets[0].GetObjectVersion() != encoderArtifact.ObjectVersion ||
		len(tickets[0].GetTransferTicket()) == 0 {
		t.Fatalf("DiT certified Connector inputs/tickets = %#v / %#v", inputs, tickets)
	}
	ticketSigner, err := stageartifact.NewTransferTicketKeyringSigner(
		"stage-authority-key-v1",
		map[string][]byte{"stage-authority-key-v1": bytes.Repeat([]byte{0x9a}, 32)},
	)
	if err != nil {
		t.Fatalf("construct TransferTicket verifier: %v", err)
	}
	issuedAt := ditResult.Assignment.GetAuthority().GetIssuedAt().AsTime()
	claims, err := ticketSigner.Verify(
		stageartifact.SignedTransferTicket{Token: tickets[0].GetTransferTicket()},
		issuedAt.Add(time.Millisecond),
	)
	if err != nil {
		t.Fatalf("verify certified Connector TransferTicket: %v", err)
	}
	connectorID := uuid.MustParse("49000000-0000-0000-0000-000000000050")
	if claims.TicketID == uuid.Nil ||
		claims.Destination.WorkerInstanceID.String() != ditResult.Assignment.GetAuthority().GetWorkerInstanceId() ||
		claims.Destination.WorkerInstanceEpoch != ditResult.Assignment.GetAuthority().GetWorkerInstanceEpoch() ||
		claims.Destination.ModelResidencyID.String() != ditResult.Assignment.GetAuthority().GetModelResidencyId() ||
		claims.Destination.ModelRuntimeEpoch != ditResult.Assignment.GetAuthority().GetModelRuntimeBarrierGeneration() ||
		claims.Destination.ConnectorRevisionID != connectorID ||
		!claims.IssuedAt.Equal(issuedAt) ||
		!claims.ExpiresAt.Equal(issuedAt.Add(30*time.Second)) {
		t.Fatalf("certified Connector TransferTicket claims = %#v", claims)
	}
	descriptor, err := artifactRepository.Resolve(
		context.Background(),
		stageartifact.ResolveTransferCommand{
			TicketID:    claims.TicketID,
			TokenDigest: sha256.Sum256(tickets[0].GetTransferTicket()),
			Destination: claims.Destination,
			ResolvedAt:  issuedAt.Add(time.Millisecond),
		},
	)
	if err != nil || descriptor.TicketID != claims.TicketID ||
		descriptor.ArtifactID != encoderArtifact.ID ||
		descriptor.ObjectVersion != encoderArtifact.ObjectVersion ||
		descriptor.SHA256 != encoderArtifact.SHA256 ||
		descriptor.SizeBytes != encoderArtifact.SizeBytes {
		t.Fatalf("resolve certified Connector TransferTicket = %#v error=%v", descriptor, err)
	}
	var pinID uuid.UUID
	if err := database.Admin.QueryRow(`
		SELECT id
		FROM stage_artifact_pins
		WHERE stage_artifact_id = $1
		  AND owner_stage_run_id = $2
		  AND state = 'ACTIVE'
	`, encoderArtifact.ID, ditRunID).Scan(&pinID); err != nil {
		t.Fatalf("read certified Connector StageArtifact pin: %v", err)
	}
	ticketIssuer, err := stageartifact.NewTransferTicketIssuer(artifactRepository, ticketSigner)
	if err != nil {
		t.Fatalf("construct server-clock TransferTicket issuer: %v", err)
	}
	issueTicket := func(label string, ticketIssuedAt, ticketExpiresAt time.Time) (
		stageartifact.SignedTransferTicket,
		stageartifact.TransferTicketClaims,
	) {
		t.Helper()
		ticket, issueErr := ticketIssuer.Issue(
			context.Background(),
			stageartifact.IssueTransferRequest{
				CommandID: uuid.New(), TicketID: uuid.New(), ArtifactID: encoderArtifact.ID,
				PinID: pinID, SigningKeyID: "stage-authority-key-v1",
				Destination: claims.Destination, IssuedAt: ticketIssuedAt,
				ExpiresAt: ticketExpiresAt,
			},
		)
		if issueErr != nil {
			t.Fatalf("issue %s TransferTicket fixture: %v", label, issueErr)
		}
		verifiedClaims, verifyErr := ticketSigner.Verify(
			ticket, ticketIssuedAt.Add(time.Millisecond),
		)
		if verifyErr != nil {
			t.Fatalf("verify %s TransferTicket fixture at claimed time: %v", label, verifyErr)
		}
		return ticket, verifiedClaims
	}
	precisionIssuedAt := time.Now().UTC().Add(-time.Second).Truncate(time.Millisecond)
	precisionTicket, precisionClaims := issueTicket(
		"sub-microsecond consume", precisionIssuedAt, precisionIssuedAt.Add(time.Minute),
	)
	precisionConsumedAt := time.Now().UTC().Truncate(time.Microsecond).Add(789 * time.Nanosecond)
	precisionCommand := stageartifact.ConsumeTransferCommand{
		CommandID: uuid.New(), TicketID: precisionClaims.TicketID,
		TokenDigest:   sha256.Sum256(precisionTicket.Token),
		Destination:   precisionClaims.Destination,
		OutcomeDigest: encoderArtifact.SHA256,
		ConsumedAt:    precisionConsumedAt,
	}
	consumed, err := artifactRepository.ConsumeWithResult(context.Background(), precisionCommand)
	if err != nil || consumed.TicketID != precisionClaims.TicketID || consumed.Replayed ||
		!consumed.ConsumedAt.Equal(precisionConsumedAt.Round(time.Microsecond)) {
		t.Fatalf("consume sub-microsecond TransferTicket = %#v error=%v", consumed, err)
	}
	replayedConsume, err := artifactRepository.ConsumeWithResult(
		context.Background(), precisionCommand,
	)
	if err != nil || replayedConsume.TicketID != consumed.TicketID ||
		!replayedConsume.Replayed || !replayedConsume.ConsumedAt.Equal(consumed.ConsumedAt) {
		t.Fatalf("replay sub-microsecond TransferTicket = %#v error=%v", replayedConsume, err)
	}
	assertServerClockRejects := func(label string, ticketIssuedAt, ticketExpiresAt time.Time) {
		t.Helper()
		ticket, ticketClaims := issueTicket(label, ticketIssuedAt, ticketExpiresAt)
		tokenDigest := sha256.Sum256(ticket.Token)
		_, resolveErr := artifactRepository.Resolve(
			context.Background(),
			stageartifact.ResolveTransferCommand{
				TicketID: ticketClaims.TicketID, TokenDigest: tokenDigest,
				Destination: ticketClaims.Destination,
				ResolvedAt:  ticketIssuedAt.Add(time.Millisecond),
			},
		)
		assertPostgresConstraint(t, resolveErr, "stage_transfer_ticket_resolve_stale")
		consumeErr := artifactRepository.Consume(
			context.Background(),
			stageartifact.ConsumeTransferCommand{
				CommandID: uuid.New(), TicketID: ticketClaims.TicketID,
				TokenDigest: tokenDigest, Destination: ticketClaims.Destination,
				OutcomeDigest: encoderArtifact.SHA256,
				ConsumedAt:    ticketIssuedAt.Add(time.Millisecond),
			},
		)
		assertPostgresConstraint(t, consumeErr, "stage_transfer_ticket_consume_stale")
	}
	expiredIssuedAt := time.Now().UTC().Add(-time.Minute).Truncate(time.Millisecond)
	assertServerClockRejects(
		"expired", expiredIssuedAt, expiredIssuedAt.Add(30*time.Second),
	)
	futureIssuedAt := time.Now().UTC().Add(time.Minute).Truncate(time.Millisecond)
	assertServerClockRejects(
		"future", futureIssuedAt, futureIssuedAt.Add(30*time.Second),
	)

	capacityResult, err := database.Admin.Exec(`
		UPDATE capacity_observations
		SET expires_at = clock_timestamp() + interval '1 minute'
		WHERE worker_instance_id = $1 AND worker_instance_epoch = $2
	`, claims.Destination.WorkerInstanceID, claims.Destination.WorkerInstanceEpoch)
	if err != nil {
		t.Fatalf("extend destination CapacityObservation: %v", err)
	}
	capacityRows, err := capacityResult.RowsAffected()
	if err != nil || capacityRows != 1 {
		t.Fatalf("extended CapacityObservations = %d error=%v, want 1", capacityRows, err)
	}
	assertLockWaitRejects := func(label string, expireCapacity bool) {
		t.Helper()
		lockIssuedAt := time.Now().UTC().Add(-time.Second).Truncate(time.Millisecond)
		ticketExpiresAt := lockIssuedAt.Add(time.Minute)
		resolveTicket, resolveClaims := issueTicket(label+" resolve", lockIssuedAt, ticketExpiresAt)
		consumeTicket, consumeClaims := issueTicket(label+" consume", lockIssuedAt, ticketExpiresAt)
		blocker, beginErr := database.Admin.Begin()
		if beginErr != nil {
			t.Fatalf("begin %s lock blocker: %v", label, beginErr)
		}
		defer func() { _ = blocker.Rollback() }()
		if _, lockErr := blocker.Exec(`
			SELECT id FROM transfer_tickets WHERE id IN ($1, $2) FOR UPDATE
		`, resolveClaims.TicketID, consumeClaims.TicketID); lockErr != nil {
			t.Fatalf("lock %s TransferTickets: %v", label, lockErr)
		}
		resolveErrors := make(chan error, 1)
		go func() {
			_, resolveErr := artifactRepository.Resolve(
				context.Background(),
				stageartifact.ResolveTransferCommand{
					TicketID:    resolveClaims.TicketID,
					TokenDigest: sha256.Sum256(resolveTicket.Token),
					Destination: resolveClaims.Destination,
					ResolvedAt:  lockIssuedAt.Add(time.Millisecond),
				},
			)
			resolveErrors <- resolveErr
		}()
		consumeErrors := make(chan error, 1)
		go func() {
			consumeErrors <- artifactRepository.Consume(
				context.Background(),
				stageartifact.ConsumeTransferCommand{
					CommandID: uuid.New(), TicketID: consumeClaims.TicketID,
					TokenDigest:   sha256.Sum256(consumeTicket.Token),
					Destination:   consumeClaims.Destination,
					OutcomeDigest: encoderArtifact.SHA256,
					ConsumedAt:    lockIssuedAt.Add(time.Millisecond),
				},
			)
		}()
		deadline := time.Now().Add(6 * time.Second)
		for {
			var waiters int
			if queryErr := database.Admin.QueryRow(`
				SELECT count(*) FROM pg_stat_activity
				WHERE usename = 'vela_stage_artifact_login' AND wait_event_type = 'Lock'
			`).Scan(&waiters); queryErr != nil {
				t.Fatalf("inspect %s lock waiters: %v", label, queryErr)
			}
			if waiters >= 2 {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("%s lock waiters = %d, want 2", label, waiters)
			}
			time.Sleep(10 * time.Millisecond)
		}
		if expireCapacity {
			result, expireErr := blocker.Exec(`
				UPDATE capacity_observations
				SET expires_at = clock_timestamp() + interval '250 milliseconds'
				WHERE worker_instance_id = $1 AND worker_instance_epoch = $2
			`, claims.Destination.WorkerInstanceID, claims.Destination.WorkerInstanceEpoch)
			if expireErr != nil {
				t.Fatalf("expire %s CapacityObservation: %v", label, expireErr)
			}
			rows, rowsErr := result.RowsAffected()
			if rowsErr != nil || rows != 1 {
				t.Fatalf("expired %s CapacityObservations = %d error=%v, want 1", label, rows, rowsErr)
			}
		} else {
			result, expireErr := blocker.Exec(`
				UPDATE transfer_tickets
				SET expires_at = clock_timestamp() + interval '250 milliseconds'
				WHERE id IN ($1, $2)
			`, resolveClaims.TicketID, consumeClaims.TicketID)
			if expireErr != nil {
				t.Fatalf("expire %s TransferTickets: %v", label, expireErr)
			}
			rows, rowsErr := result.RowsAffected()
			if rowsErr != nil || rows != 2 {
				t.Fatalf("expired %s TransferTickets = %d error=%v, want 2", label, rows, rowsErr)
			}
		}
		if _, sleepErr := blocker.Exec(`SELECT pg_sleep(0.4)`); sleepErr != nil {
			t.Fatalf("wait across %s rejection boundary: %v", label, sleepErr)
		}
		if commitErr := blocker.Commit(); commitErr != nil {
			t.Fatalf("release %s lock blocker: %v", label, commitErr)
		}
		select {
		case resolveErr := <-resolveErrors:
			assertPostgresConstraint(t, resolveErr, "stage_transfer_ticket_resolve_stale")
		case <-time.After(5 * time.Second):
			t.Fatalf("%s Resolve did not finish", label)
		}
		select {
		case consumeErr := <-consumeErrors:
			assertPostgresConstraint(t, consumeErr, "stage_transfer_ticket_consume_stale")
		case <-time.After(5 * time.Second):
			t.Fatalf("%s Consume did not finish", label)
		}
	}
	clockSkewIssuedAt := time.Now().UTC().Add(-time.Second).Truncate(time.Millisecond)
	clockSkewTicket, clockSkewClaims := issueTicket(
		"worker-behind clock skew",
		clockSkewIssuedAt,
		clockSkewIssuedAt.Add(30*time.Second),
	)
	workerNow := clockSkewIssuedAt.Add(-300 * time.Millisecond)
	connector, err := stageartifact.NewObjectStorePullConnectorWithClockSkew(
		objectStore,
		artifactRepository,
		ticketSigner,
		func() time.Time { return workerNow },
		30*time.Second,
	)
	if err != nil {
		t.Fatalf("construct skew-aware PostgreSQL Connector: %v", err)
	}
	target := stageartifact.NewMemoryTransferTarget()
	receipt, err := connector.Pull(
		context.Background(), clockSkewTicket, clockSkewClaims.Destination, target,
	)
	if err != nil || receipt.TicketID != clockSkewClaims.TicketID ||
		!receipt.CompletedAt.Equal(clockSkewIssuedAt) ||
		!bytes.Equal(target.Bytes(), []byte("certified connector input")) {
		t.Fatalf(
			"pull through PostgreSQL with worker clock skew receipt=%#v bytes=%q error=%v",
			receipt, target.Bytes(), err,
		)
	}
	var clockSkewTicketState string
	var consumedAt time.Time
	if err := database.Admin.QueryRow(`
		SELECT state::text, consumed_at
		FROM transfer_tickets
		WHERE id = $1
	`, clockSkewClaims.TicketID).Scan(&clockSkewTicketState, &consumedAt); err != nil {
		t.Fatalf("read clock-skew TransferTicket outcome: %v", err)
	}
	if clockSkewTicketState != "CONSUMED" || !consumedAt.Equal(clockSkewIssuedAt) {
		t.Fatalf(
			"clock-skew TransferTicket outcome = %s at %s, want CONSUMED at %s",
			clockSkewTicketState, consumedAt, clockSkewIssuedAt,
		)
	}

	assertLockWaitRejects("ticket expiry", false)
	assertLockWaitRejects("capacity expiry", true)
	var connectorState string
	if err := database.Admin.QueryRow(`
		SELECT connector.state::text
		FROM transfer_tickets AS ticket
		JOIN connector_revisions AS connector ON connector.id = ticket.connector_revision_id
		WHERE ticket.id = $1
	`, claims.TicketID).Scan(&connectorState); err != nil {
		t.Fatalf("read DiT TransferTicket Connector: %v", err)
	}
	if connectorState != "CERTIFIED" {
		t.Fatalf("DiT TransferTicket Connector state = %s, want CERTIFIED", connectorState)
	}

	writer, err := database.Admin.Begin()
	if err != nil {
		t.Fatalf("begin concurrent TransferTicket writer: %v", err)
	}
	defer func() { _ = writer.Rollback() }()
	if _, err := writer.Exec(`
		INSERT INTO transfer_tickets (
			id, stage_artifact_id, stage_artifact_pin_id, exact_object_version,
			sha256, size_bytes, destination_worker_instance_id,
			destination_worker_instance_epoch, destination_model_residency_id,
			destination_model_runtime_epoch, connector_revision_id, token_digest,
			issued_at, expires_at
		)
		SELECT $1, stage_artifact_id, stage_artifact_pin_id, exact_object_version,
		       sha256, size_bytes, destination_worker_instance_id,
		       destination_worker_instance_epoch, destination_model_residency_id,
		       destination_model_runtime_epoch, connector_revision_id,
		       decode(repeat('ef', 32), 'hex'), issued_at, expires_at
		FROM transfer_tickets WHERE id = $2
	`, uuid.New(), claims.TicketID); err != nil {
		t.Fatalf("write concurrent TransferTicket evidence: %v", err)
	}
	migrations := filepath.Join(repositoryRoot(t), "db", "migrations")
	downErrors := make(chan error, 1)
	go func() {
		downErrors <- goose.DownTo(database.Admin, migrations, 68)
	}()
	waitForRoleDatabaseLock(t, database.Admin, "postgres")
	if err := writer.Commit(); err != nil {
		t.Fatalf("commit concurrent TransferTicket evidence: %v", err)
	}
	select {
	case downErr := <-downErrors:
		assertPostgresConstraint(t, downErr, "stage_transfer_server_clock_rollback_is_unsafe")
	case <-time.After(10 * time.Second):
		t.Fatal("concurrent Stage TransferTicket migration Down did not finish")
	}
	err = goose.DownTo(database.Admin, migrations, 68)
	assertPostgresConstraint(t, err, "stage_transfer_server_clock_rollback_is_unsafe")
	version, err := goose.GetDBVersion(database.Admin)
	if err != nil || version != 69 {
		t.Fatalf("Stage TransferTicket version after refused Down = %d error=%v", version, err)
	}
	for _, signature := range []string{
		"public.vela_resolve_stage_transfer_ticket(jsonb)",
		"public.vela_consume_stage_transfer_ticket(jsonb)",
	} {
		var definition string
		if err := database.Admin.QueryRow(`
			SELECT pg_get_functiondef($1::regprocedure)
		`, signature).Scan(&definition); err != nil {
			t.Fatalf("read %s after refused Down: %v", signature, err)
		}
		if !strings.Contains(definition, "v_server_now >= v_ticket.expires_at") ||
			!strings.Contains(definition, "observation.expires_at > v_server_now") {
			t.Fatalf("%s lost server clock after refused Down", signature)
		}
	}
}

func TestPostgresAssignmentBackendReplaysNoWorkAndRejectsChangedRequest(t *testing.T) {
	fixture := newStageSchedulerFixture(t, "stage-worker-no-work-replay")
	backend := newPostgresAssignmentTestBackend(t, fixture)
	request := stageWorkerAcquireRequest(fixture)
	if result, err := backend.AcquireStage(
		context.Background(), stageWorkerAcquireCommand(fixture), request,
	); err != nil || result.Assignment == nil {
		t.Fatalf("consume READY StageRun = %#v error=%v", result, err)
	}
	command := stageWorkerAcquireCommand(fixture)
	first, err := backend.AcquireStage(context.Background(), command, request)
	if err != nil || first.Assignment != nil || first.Command != nil ||
		first.RetryAfter != 250*time.Millisecond {
		t.Fatalf("first NoWork = %#v error=%v", first, err)
	}
	second, err := backend.AcquireStage(context.Background(), command, request)
	if err != nil || second != first {
		t.Fatalf("replayed NoWork = %#v want %#v error=%v", second, first, err)
	}
	changed := proto.Clone(request).(*velav1.AcquireStageRequest)
	changed.ModelRuntimeEpoch++
	_, err = backend.AcquireStage(context.Background(), command, changed)
	assertPostgresConstraint(t, err, "stage_worker_acquire_replay_mismatch")

	var kind string
	var retry int64
	if err := fixture.database.Admin.QueryRow(`
		SELECT result_kind::text, retry_after_ms
		FROM stage_worker_acquire_results WHERE command_id = $1
	`, command.CommandID).Scan(&kind, &retry); err != nil {
		t.Fatalf("read durable NoWork result: %v", err)
	}
	if kind != "NO_WORK" || retry != 250 {
		t.Fatalf("durable NoWork kind=%s retry=%d", kind, retry)
	}
}

func TestPostgresAssignmentBackendDoesNotAssignFirstStageAtProjectRunningLimit(t *testing.T) {
	fixture := newStageSchedulerFixture(t, "stage-worker-project-capacity")
	if _, err := fixture.database.Admin.Exec(`
		UPDATE projects
		SET running_count = running_limit
		WHERE id = $1
	`, testProjectID); err != nil {
		t.Fatalf("fill Project running capacity: %v", err)
	}

	result, err := newPostgresAssignmentTestBackend(t, fixture).AcquireStage(
		context.Background(),
		stageWorkerAcquireCommand(fixture),
		stageWorkerAcquireRequest(fixture),
	)
	if err != nil || result.Assignment != nil || result.Command != nil ||
		result.RetryAfter != 250*time.Millisecond {
		t.Fatalf("AcquireStage at Project limit = %#v error=%v", result, err)
	}

	var attempts, allocations int
	var filterReason string
	if err := fixture.database.Admin.QueryRow(`
		SELECT
			(SELECT count(*) FROM stage_attempts WHERE stage_run_id = $1),
			(SELECT count(*) FROM stage_allocations WHERE stage_run_id = $1),
			COALESCE((
				SELECT candidate #>> '{filter_reasons,0}'
				FROM stage_scheduler_snapshot_traces AS trace,
				     jsonb_array_elements(trace.snapshot -> 'candidates') AS candidate
				WHERE (candidate ->> 'stage_run_id')::uuid = $1
				ORDER BY trace.evaluated_at DESC
				LIMIT 1
			), '')
	`, fixture.stageRunID).Scan(&attempts, &allocations, &filterReason); err != nil {
		t.Fatalf("read Project-capacity scheduling evidence: %v", err)
	}
	if attempts != 0 || allocations != 0 ||
		filterReason != string(stagescheduler.FilterProjectCapacityExhausted) {
		t.Fatalf(
			"Project-capacity evidence attempts=%d allocations=%d reason=%q",
			attempts, allocations, filterReason,
		)
	}
}

func TestPostgresAssignmentBackendAssignsRunningGraphAtProjectRunningLimit(t *testing.T) {
	fixture := newStageSchedulerFixture(t, "stage-worker-running-project-capacity")
	tx, err := fixture.database.Admin.Begin()
	if err != nil {
		t.Fatalf("begin running graph capacity fixture: %v", err)
	}
	if _, err := tx.Exec(`SET LOCAL ROLE vela_attempt_coordinator_owner`); err != nil {
		_ = tx.Rollback()
		t.Fatalf("assume AttemptCoordinator owner role: %v", err)
	}
	var advanced int
	if err := tx.QueryRow(`
		WITH target AS MATERIALIZED (
			SELECT attempt.id AS attempt_id, attempt.job_id,
			       job.organization_id, job.project_id
			FROM stage_runs AS run
			JOIN attempts AS attempt ON attempt.id = run.attempt_id
			JOIN jobs AS job ON job.id = attempt.job_id
			WHERE run.id = $1
		), moment AS MATERIALIZED (
			SELECT clock_timestamp() AS advanced_at
		), advanced_job AS (
			UPDATE jobs AS job
			SET state = 'RUNNING', billable_started_at = moment.advanced_at,
			    version = job.version + 1, updated_at = moment.advanced_at
			FROM target, moment
			WHERE job.id = target.job_id
			  AND job.state = 'QUEUED'
			RETURNING job.id
		), advanced_attempt AS (
			UPDATE attempts AS attempt
			SET state = 'RUNNING', graph_state = 'RUNNING',
			    started_at = moment.advanced_at, updated_at = moment.advanced_at
			FROM target, moment, advanced_job
			WHERE attempt.id = target.attempt_id
			  AND attempt.state = 'ASSIGNED'
			  AND attempt.graph_state = 'QUEUED'
			RETURNING attempt.id
		)
		UPDATE projects AS project
		SET queued_count = project.queued_count - 1,
		    running_count = project.running_limit
		FROM target, advanced_attempt
		WHERE project.id = target.project_id
		  AND project.organization_id = target.organization_id
		  AND project.queued_count > 0
		RETURNING 1
	`, fixture.stageRunID).Scan(&advanced); err != nil || advanced != 1 {
		_ = tx.Rollback()
		t.Fatalf("advance graph to full-capacity RUNNING state = %d error=%v", advanced, err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit running graph capacity fixture: %v", err)
	}

	result, err := newPostgresAssignmentTestBackend(t, fixture).AcquireStage(
		context.Background(),
		stageWorkerAcquireCommand(fixture),
		stageWorkerAcquireRequest(fixture),
	)
	if err != nil || result.Assignment == nil || result.Command != nil || result.RetryAfter != 0 {
		t.Fatalf("AcquireStage for running graph at Project limit = %#v error=%v", result, err)
	}
}

func TestPostgresAssignmentBackendPersistsForgedAndStaleAuthorityDecisions(t *testing.T) {
	fixture := newStageSchedulerFixture(t, "stage-worker-authority-rejection")
	backend := newPostgresAssignmentTestBackend(t, fixture)
	request := stageWorkerAcquireRequest(fixture)
	forged := stageWorkerAcquireCommand(fixture)
	forged.Identity.SPIFFEID += "/forged"
	rejected, err := backend.AcquireStage(context.Background(), forged, request)
	if err != nil || rejected.Command == nil ||
		rejected.Command.Decision != velav1.StageWorkerCommandDecision_STAGE_WORKER_COMMAND_DECISION_REJECTED {
		t.Fatalf("forged identity result = %#v error=%v", rejected, err)
	}
	stale := stageWorkerAcquireCommand(fixture)
	stale.ControlSessionEpoch++
	staleResult, err := backend.AcquireStage(context.Background(), stale, request)
	if err != nil || staleResult.Command == nil ||
		staleResult.Command.Decision != velav1.StageWorkerCommandDecision_STAGE_WORKER_COMMAND_DECISION_STALE {
		t.Fatalf("stale session result = %#v error=%v", staleResult, err)
	}
	valid, err := backend.AcquireStage(
		context.Background(), stageWorkerAcquireCommand(fixture), request,
	)
	if err != nil || valid.Assignment == nil {
		t.Fatalf("valid authority after rejections = %#v error=%v", valid, err)
	}
	var attempts int
	if err := fixture.database.Admin.QueryRow(`
		SELECT count(*) FROM stage_attempts WHERE stage_run_id = $1
	`, fixture.stageRunID).Scan(&attempts); err != nil {
		t.Fatalf("count StageAttempts after rejected acquires: %v", err)
	}
	if attempts != 1 {
		t.Fatalf("StageAttempts after rejected acquires = %d, want 1", attempts)
	}
}

func TestPostgresAssignmentBackendRecoversCrashAfterSchedulerClaim(t *testing.T) {
	fixture := newStageSchedulerFixture(t, "stage-worker-claim-crash")
	crashingScheduler, err := stagescheduler.NewService(
		fixture.repository,
		panickingStageCoordinator{},
		stagescheduler.Config{
			SchedulerID:      "stage-worker-control/claim-crash",
			ClaimTTL:         30 * time.Second,
			LeaseTTL:         time.Minute,
			LocalDeadlineTTL: 50 * time.Second,
			SigningKeyID:     "stage-authority-key-v1",
		},
	)
	if err != nil {
		t.Fatalf("construct crashing StageScheduler: %v", err)
	}
	backend := newPostgresAssignmentTestBackendWithScheduler(t, fixture, crashingScheduler)
	command := stageWorkerAcquireCommand(fixture)
	func() {
		defer func() {
			if recovered := recover(); recovered == nil {
				t.Fatal("Stage Worker acquire did not stop after scheduler claim")
			}
		}()
		_, _ = backend.AcquireStage(
			context.Background(), command, stageWorkerAcquireRequest(fixture),
		)
	}()

	recovered, err := newPostgresAssignmentTestBackend(t, fixture).AcquireStage(
		context.Background(), command, stageWorkerAcquireRequest(fixture),
	)
	if err != nil || recovered.Assignment == nil {
		t.Fatalf("recover acquire after scheduler claim = %#v error=%v", recovered, err)
	}
	assertSingleDurableAssignment(t, fixture, command.CommandID)
}

func TestPostgresAssignmentBackendRecoversAfterCommittedSchedulerBeforeResult(t *testing.T) {
	fixture := newStageSchedulerFixture(t, "stage-worker-result-crash")
	normalScheduler := newStageSchedulerTestService(t, fixture)
	ctx, cancel := context.WithCancel(context.Background())
	backend := newPostgresAssignmentTestBackendWithScheduler(
		t, fixture, cancelAfterAssignmentScheduler{delegate: normalScheduler, cancel: cancel},
	)
	command := stageWorkerAcquireCommand(fixture)
	first, err := backend.AcquireStage(ctx, command, stageWorkerAcquireRequest(fixture))
	if err == nil || first != (stageworkercontrol.AcquireResult{}) {
		t.Fatalf("acquire canceled after scheduler commit = %#v error=%v", first, err)
	}
	var results int
	if err := fixture.database.Admin.QueryRow(`
		SELECT count(*) FROM stage_worker_acquire_results WHERE command_id = $1
	`, command.CommandID).Scan(&results); err != nil {
		t.Fatalf("count pre-recovery acquire results: %v", err)
	}
	if results != 0 {
		t.Fatalf("pre-recovery acquire results = %d, want 0", results)
	}

	recovered, err := newPostgresAssignmentTestBackend(t, fixture).AcquireStage(
		context.Background(), command, stageWorkerAcquireRequest(fixture),
	)
	if err != nil || recovered.Assignment == nil {
		t.Fatalf("recover acquire before result persistence = %#v error=%v", recovered, err)
	}
	assertSingleDurableAssignment(t, fixture, command.CommandID)
}

func TestStageWorkerAssignmentMigrationEmptyDownUp(t *testing.T) {
	migrations := filepath.Join(repositoryRoot(t), "db", "migrations")
	t.Run("empty Down Up", func(t *testing.T) {
		database := newPostgres(t)
		applyFoundationTo(t, database.Admin, 42)
		if err := goose.DownTo(database.Admin, migrations, 41); err != nil {
			t.Fatalf("migrate empty Stage Worker assignment down: %v", err)
		}
		var intents, completeFunction bool
		if err := database.Admin.QueryRow(`
			SELECT to_regclass('stage_worker_acquire_intents') IS NOT NULL,
			       to_regprocedure('vela_complete_stage_worker_acquire(jsonb)') IS NOT NULL
		`).Scan(&intents, &completeFunction); err != nil {
			t.Fatalf("inspect schema 41 assignment surface: %v", err)
		}
		if intents || completeFunction {
			t.Fatal("Stage Worker assignment surface survived empty Down")
		}
		if err := goose.UpTo(database.Admin, migrations, 42); err != nil {
			t.Fatalf("migrate Stage Worker assignment back up: %v", err)
		}
		version, err := goose.GetDBVersion(database.Admin)
		if err != nil || version != 42 {
			t.Fatalf("Stage Worker assignment version after Up = %d error=%v", version, err)
		}
	})
}

func TestStageAssignmentConnectorStatesMigrationEmptyDownUp(t *testing.T) {
	database := newPostgres(t)
	applyFoundationTo(t, database.Admin, 67)
	migrations := filepath.Join(repositoryRoot(t), "db", "migrations")
	assertConnectorStates := func(wantExpanded bool) {
		t.Helper()
		var definition string
		if err := database.Admin.QueryRow(`
			SELECT pg_get_functiondef(
				'public.vela_read_stage_assignment_execution_v1(uuid,uuid)'::regprocedure
			)
		`).Scan(&definition); err != nil {
			t.Fatalf("read StageAssignment execution definition: %v", err)
		}
		expanded := strings.Contains(
			definition,
			"revision.state IN ('CERTIFIED', 'CANARY', 'ACTIVE', 'DRAINING')",
		)
		if expanded != wantExpanded {
			t.Fatalf("StageAssignment Connector states expanded=%t want=%t", expanded, wantExpanded)
		}
	}

	assertConnectorStates(false)
	if err := goose.UpTo(database.Admin, migrations, 68); err != nil {
		t.Fatalf("expand StageAssignment Connector states migration: %v", err)
	}
	assertConnectorStates(true)
	if err := goose.DownTo(database.Admin, migrations, 67); err != nil {
		t.Fatalf("contract StageAssignment Connector states migration: %v", err)
	}
	assertConnectorStates(false)
	if err := goose.UpTo(database.Admin, migrations, 68); err != nil {
		t.Fatalf("re-expand StageAssignment Connector states migration: %v", err)
	}
	assertConnectorStates(true)
}

func newPostgresAssignmentTestBackend(
	t *testing.T,
	fixture stageSchedulerFixture,
) *stageworkercontrol.PostgresAssignmentBackend {
	t.Helper()
	return newPostgresAssignmentTestBackendWithScheduler(
		t, fixture, newStageSchedulerTestService(t, fixture),
	)
}

func newStageSchedulerTestService(
	t *testing.T,
	fixture stageSchedulerFixture,
) *stagescheduler.Service {
	t.Helper()
	scheduling, err := stagescheduler.NewService(
		fixture.repository,
		fixture.coordinator,
		stagescheduler.Config{
			SchedulerID:      "stage-worker-control/integration",
			ClaimTTL:         30 * time.Second,
			LeaseTTL:         time.Minute,
			LocalDeadlineTTL: 50 * time.Second,
			SigningKeyID:     "stage-authority-key-v1",
		},
	)
	if err != nil {
		t.Fatalf("construct StageScheduler: %v", err)
	}
	return scheduling
}

func newPostgresAssignmentTestBackendWithScheduler(
	t *testing.T,
	fixture stageSchedulerFixture,
	scheduling stageworkercontrol.AssignmentScheduler,
) *stageworkercontrol.PostgresAssignmentBackend {
	t.Helper()
	authoritySigner, err := stageauthority.NewSigner(map[string][]byte{
		"stage-authority-key-v1": bytes.Repeat([]byte{0x9a}, 32),
	})
	if err != nil {
		t.Fatalf("construct StageAuthority signer: %v", err)
	}
	transferSigner, err := stageartifact.NewTransferTicketKeyringSigner(
		"stage-authority-key-v1",
		map[string][]byte{"stage-authority-key-v1": bytes.Repeat([]byte{0x9a}, 32)},
	)
	if err != nil {
		t.Fatalf("construct TransferTicket signer: %v", err)
	}
	artifactRepository, err := stageartifact.NewPostgresRepository(newRolePool(
		t, fixture.database.DSN,
		"vela_stage_artifact_login", "vela-stage-artifact-password",
	))
	if err != nil {
		t.Fatalf("construct StageArtifact repository: %v", err)
	}
	transferIssuer, err := stageartifact.NewTransferTicketIssuer(
		artifactRepository, transferSigner,
	)
	if err != nil {
		t.Fatalf("construct TransferTicket issuer: %v", err)
	}
	backend, err := stageworkercontrol.NewPostgresAssignmentBackend(
		stageworkercontrol.PostgresAssignmentConfig{
			Pool: newRolePool(
				t, fixture.database.DSN,
				"vela_stage_worker_control_login", "vela-stage-worker-control-password",
			),
			Scheduler:          scheduling,
			AuthoritySigner:    authoritySigner,
			TransferTickets:    transferIssuer,
			IdentityKey:        bytes.Repeat([]byte{0x9c}, 32),
			NoWorkRetry:        250 * time.Millisecond,
			MemberStartTimeout: 30 * time.Second,
			TransferTicketTTL:  30 * time.Second,
		},
	)
	if err != nil {
		t.Fatalf("construct PostgresAssignmentBackend: %v", err)
	}
	return backend
}

type cancelAfterAssignmentScheduler struct {
	delegate stageworkercontrol.AssignmentScheduler
	cancel   context.CancelFunc
}

func (scheduler cancelAfterAssignmentScheduler) AcquireIdentified(
	ctx context.Context,
	authority stagescheduler.WorkerAuthority,
	observation stagescheduler.CapacityObservation,
	identity stagescheduler.AssignmentIdentity,
) (stagescheduler.Assignment, bool, error) {
	assignment, assigned, err := scheduler.delegate.AcquireIdentified(
		ctx, authority, observation, identity,
	)
	scheduler.cancel()
	return assignment, assigned, err
}

func assertSingleDurableAssignment(
	t *testing.T,
	fixture stageSchedulerFixture,
	commandID uuid.UUID,
) {
	t.Helper()
	var attempts, leases, results int
	if err := fixture.database.Admin.QueryRow(`
		SELECT
			(SELECT count(*) FROM stage_attempts WHERE stage_run_id = $1),
			(SELECT count(*) FROM stage_leases WHERE stage_run_id = $1),
			(SELECT count(*) FROM stage_worker_acquire_results WHERE command_id = $2)
	`, fixture.stageRunID, commandID).Scan(&attempts, &leases, &results); err != nil {
		t.Fatalf("read recovered durable Assignment: %v", err)
	}
	if attempts != 1 || leases != 1 || results != 1 {
		t.Fatalf(
			"recovered durable Assignment attempts=%d leases=%d results=%d",
			attempts, leases, results,
		)
	}
}

func stageWorkerAcquireCommand(
	fixture stageSchedulerFixture,
) stageworkercontrol.CommandContext {
	return stageworkercontrol.CommandContext{
		CommandID: uuid.New(),
		Identity: stageworkertransport.Identity{
			SPIFFEID: "spiffe://vela/worker/" + fixture.authority.WorkerInstanceID.String(),
		},
		ControlSessionEpoch: 1,
	}
}

func stageWorkerAcquireRequest(fixture stageSchedulerFixture) *velav1.AcquireStageRequest {
	return &velav1.AcquireStageRequest{
		WorkerInstanceId:            fixture.authority.WorkerInstanceID.String(),
		WorkerInstanceEpoch:         fixture.authority.WorkerInstanceEpoch,
		CapacityObservationSequence: fixture.observation.Sequence,
		ModelResidencyId:            fixture.authority.ModelResidencyID.String(),
		ModelRuntimeEpoch:           fixture.authority.ModelRuntimeEpoch,
		StageProfileRevisionId:      fixture.authority.StageProfileRevisionID.String(),
	}
}
