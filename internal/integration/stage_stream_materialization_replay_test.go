//go:build integration

package integration_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/artifactstore"
	"github.com/vivym/vela/internal/materializationauthority"
	"github.com/vivym/vela/internal/stageartifact"
	"github.com/vivym/vela/internal/stageauthority"
	"github.com/vivym/vela/internal/stageworkeragent"
	"github.com/vivym/vela/internal/stageworkercontrol"
	"github.com/vivym/vela/internal/stageworkertransport"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestStageStreamMaterializationJournalReplaysLostResponseAfterTTL(t *testing.T) {
	for _, kind := range []string{"COMMIT", "SOURCE_LOST"} {
		t.Run(kind, func(t *testing.T) {
			ctx := context.Background()
			database, _, coordinator, job, attemptID, runID, _ := newStageGraphCancellationFixture(t, "stream-replay-"+kind)
			assignment := assignAndStartEncoder(t, database, coordinator, attemptID, runID, time.Now().Add(time.Hour))
			stage := signedAssignedStageAuthorityWithoutRuntimeBarrier(t, database, job, assignment, 3)
			repository, err := stageartifact.NewPostgresRepository(newRolePool(t, database.DSN, "vela_stage_artifact_login", "vela-stage-artifact-password"))
			if err != nil {
				t.Fatal(err)
			}
			var now time.Time
			if err := database.Admin.QueryRow(`SELECT clock_timestamp()`).Scan(&now); err != nil {
				t.Fatal(err)
			}
			clockOffset := now.Sub(time.Now())
			clock := func() time.Time { return time.Now().Add(clockOffset) }
			payload := bytes.Repeat([]byte("x"), 64)
			manifest := stageartifact.LocalOutputManifestV1{
				SchemaVersion: 1, OutputPort: "conditioning", ContentType: "application/octet-stream",
				LocalLocator: assignment.StageAttemptID.String() + "/output.bin", SizeBytes: 64, PayloadSHA256: sha256.Sum256(payload),
				Lineage: stageartifact.LocalOutputLineageV1{AttemptID: attemptID, StageRunID: runID, StageAttemptID: assignment.StageAttemptID,
					StageLeaseID: assignment.StageLeaseID, StageProfileRevisionID: assignment.StageProfileRevisionID, AttemptFence: 1, StageFence: 1},
			}
			document, err := json.Marshal(struct {
				stageartifact.LocalOutputManifestV1
				PayloadSHA256 string `json:"payload_sha256"`
			}{manifest, hex.EncodeToString(manifest.PayloadSHA256[:])})
			if err != nil {
				t.Fatal(err)
			}
			manifestDigest := sha256.Sum256(document)
			lineage, err := manifest.LineageDigest()
			if err != nil {
				t.Fatal(err)
			}
			receipt := &velav1.LocalMaterializationReceipt{ReceiptId: "stream-" + uuid.NewString(), OutputManifestJson: document,
				ManifestSha256: manifestDigest[:], TotalSizeBytes: 64, SealedAt: timestamppb.New(now)}
			identity := stageworkertransport.Identity{SPIFFEID: "spiffe://vela/worker/" + assignment.WorkerInstanceID.String()}
			identityDigest := sha256.Sum256([]byte(identity.SPIFFEID))
			keys := map[string][]byte{"stream-replay-key": bytes.Repeat([]byte{0xc7}, 32)}
			signer, err := materializationauthority.NewSigner(keys)
			if err != nil {
				t.Fatal(err)
			}
			authority, err := signer.Sign(&velav1.MaterializationAuthority{
				SchemaVersion: 1, StageAuthorityDigest: stage.Digest[:], StageMaterializationLeaseId: uuid.NewString(), StageArtifactId: uuid.NewString(),
				ObjectKey: "artifacts/stage/stream-replay/" + assignment.StageAttemptID.String(), ContentType: manifest.ContentType,
				Sha256: manifest.PayloadSHA256[:], SizeBytes: manifest.SizeBytes, LocalReceiptId: receipt.ReceiptId, LocalReceiptDigest: manifestDigest[:],
				SigningKeyId: "stream-replay-key", IssuedAt: timestamppb.New(now), ExpiresAt: timestamppb.New(now.Add(3 * time.Second)),
				SourceWorkerInstanceId: assignment.WorkerInstanceID.String(), SourceWorkerInstanceEpoch: assignment.WorkerInstanceEpoch,
				SourceWorkerMemberId: stage.Authority.GetMembers()[0].GetWorkerMemberId(), SourceWorkerMemberEpoch: stage.Authority.GetMembers()[0].GetMemberEpoch(),
				SourceSpiffeIdDigest: identityDigest[:],
			})
			if err != nil {
				t.Fatal(err)
			}
			tokenDigest, err := materializationauthority.Digest(authority)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := repository.Seal(ctx, stageartifact.SealCommand{
				CommandID: uuid.New(), AttemptID: attemptID, StageRunID: runID, StageAttemptID: assignment.StageAttemptID,
				StageAllocationID: assignment.StageAllocationID, StageLeaseID: assignment.StageLeaseID,
				ExpectedAttemptFence: 1, ExpectedStageFence: 1, ExpectedStageVersion: 3, OutputPort: manifest.OutputPort,
				LocalReceiptID: receipt.ReceiptId, LocalReceiptDigest: manifestDigest, ManifestSHA256: manifestDigest,
				SHA256: manifest.PayloadSHA256, LineageDigest: lineage, TokenDigest: tokenDigest, SizeBytes: 64,
				ArtifactID: uuid.MustParse(authority.GetStageArtifactId()), MaterializationLeaseID: uuid.MustParse(authority.GetStageMaterializationLeaseId()),
				ObjectKey: authority.GetObjectKey(), ContentType: manifest.ContentType, SealedAt: now, LeaseExpiresAt: authority.GetExpiresAt().AsTime(),
			}); err != nil {
				t.Fatal(err)
			}
			unused := unusedMaterializationReplayDependencies{}
			backend, err := stageworkercontrol.NewPostgresOperationBackend(stageworkercontrol.PostgresOperationConfig{
				WorkerEvidence: unused, Assignments: unused, Execution: unused, MaterializationIssuer: unused,
				StageArtifacts: repository, StageAttempts: coordinator, Reattachments: unused, Transfers: unused,
			})
			if err != nil {
				t.Fatal(err)
			}
			executor, err := stageworkercontrol.NewProductionExecutor(backend)
			if err != nil {
				t.Fatal(err)
			}
			stageValidator, err := stageauthority.NewValidator(map[string][]byte{"stage-authority-key-v1": bytes.Repeat([]byte{0x9a}, 32)}, clock)
			if err != nil {
				t.Fatal(err)
			}
			validator, err := materializationauthority.NewValidator(keys, clock)
			if err != nil {
				t.Fatal(err)
			}
			authorizer, err := stageworkercontrol.NewPostgresAuthorizer(newRolePool(t, database.DSN, "vela_stage_worker_control_login", "vela-stage-worker-control-password"))
			if err != nil {
				t.Fatal(err)
			}
			handler, err := stageworkercontrol.NewHandler(stageworkercontrol.Config{Validator: stageValidator, Authorizer: authorizer,
				MaterializationValidator: validator, MaterializationAuthorizer: repository, Executor: executor})
			if err != nil {
				t.Fatal(err)
			}
			control := &streamReplayControl{handler: handler, identity: identity, session: 1, loseNext: true}
			inputRoot, outputRoot, journalRoot := t.TempDir(), t.TempDir(), t.TempDir()
			inputPath := filepath.Join(inputRoot, "stage-runs", runID.String(), "inputs", "input.bin")
			outputPath := filepath.Join(outputRoot, filepath.FromSlash(manifest.LocalLocator))
			for _, path := range []string{inputPath, outputPath} {
				if kind == "SOURCE_LOST" && path == outputPath {
					continue
				}
				if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(path, payload, 0o600); err != nil {
					t.Fatal(err)
				}
			}
			journal, err := stageworkeragent.NewFileMaterializationJournal(journalRoot, 8)
			if err != nil {
				t.Fatal(err)
			}
			if err := journal.Put(ctx, stageworkeragent.PendingMaterialization{ID: receipt.ReceiptId, StageAuthority: stage.Authority,
				LocalReceipt: receipt, MaterializationAuthority: authority}); err != nil {
				t.Fatal(err)
			}
			source, err := stageartifact.NewFilesystemLocalOutputSource(outputRoot)
			if err != nil {
				t.Fatal(err)
			}
			publisher, err := stageartifact.NewObjectStorePublisher(artifactstore.NewLocal(), clock)
			if err != nil {
				t.Fatal(err)
			}
			retirer, err := stageworkeragent.NewFilesystemScratchRetirer(inputRoot, outputRoot)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = retirer.Close() })
			runtime, err := stageworkeragent.New(stageworkeragent.Config{Members: []stageworkeragent.RuntimeMember{
				{ID: authority.GetSourceWorkerMemberId(), Client: unusedStreamReplayRuntime{}},
			}})
			if err != nil {
				t.Fatal(err)
			}
			newAgent := func(journal *stageworkeragent.FileMaterializationJournal) *stageworkeragent.StreamAgent {
				agent, err := stageworkeragent.NewMaterializingStreamAgent(runtime, control, stageworkeragent.MaterializationConfig{
					Validator: validator, Source: source, Publisher: publisher, Journal: journal, ScratchRetirer: retirer,
					OutputOwnershipContract: stageworkeragent.AttemptOwnedFilesystemScratchV1,
					SourceLossEvidence: stageworkeragent.MaterializationSourceLossEvidenceFunc(func(context.Context, stageworkeragent.PendingMaterialization) (stageworkeragent.MaterializationSourceLossEvidence, error) {
						return stageworkeragent.MaterializationSourceLossEvidence{FailureFingerprint: manifest.PayloadSHA256,
							ConsumedResourceUnits: 1, LostAt: now.Add(time.Millisecond), RetryAt: now.Add(time.Second)}, nil
					}),
				})
				if err != nil {
					t.Fatal(err)
				}
				return agent
			}
			if _, err := newAgent(journal).ResumeMaterializations(ctx); !errors.Is(err, errStreamReplayResponseLost) {
				t.Fatalf("first committed response was not lost at the control boundary: %v", err)
			}
			if _, err := os.Stat(inputPath); err != nil {
				t.Fatalf("lost response must preserve inputs: %v", err)
			}
			if kind == "COMMIT" {
				if _, err := os.Stat(outputPath); err != nil {
					t.Fatalf("lost response must preserve sealed output: %v", err)
				}
			}
			pending, err := journal.List(ctx)
			if err != nil || len(pending) != 1 || pending[0].ConfirmedDisposition != "" {
				t.Fatalf("lost response recovery record: %+v error=%v", pending, err)
			}
			fixture := &materializationReplayFixture{database: database, assignment: assignment, authority: authority}
			before := fixture.snapshot(t)
			fixture.waitExpired(t)
			fleet := newRolePool(t, database.DSN, "vela_fleet_login", "vela-fleet-password")
			if err := fleet.QueryRow(ctx, `SELECT control_session_epoch FROM vela_reconnect_worker_instance(
				$1, 1, 1, 'stream-replay-session-2', clock_timestamp(), 'worker-agent/h3-node-01')`, assignment.WorkerInstanceID).Scan(&control.session); err != nil {
				t.Fatal(err)
			}
			reopened, err := stageworkeragent.NewFileMaterializationJournal(journalRoot, 8)
			if err != nil {
				t.Fatal(err)
			}
			result, err := newAgent(reopened).ResumeMaterializations(ctx)
			if kind == "COMMIT" {
				if err != nil || !result.Committed {
					t.Fatalf("restarted COMMIT confirmation failed: %+v error=%v", result, err)
				}
				if _, err := os.Stat(inputPath); !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("committed input was not retired: %v", err)
				}
			} else if !errors.Is(err, stageworkeragent.ErrMaterializationSourceLostReported) || !result.SourceLostReported {
				t.Fatalf("restarted SOURCE_LOST confirmation failed: %+v error=%v", result, err)
			} else if _, err := os.Stat(inputPath); err != nil {
				t.Fatalf("retry input was retired: %v", err)
			}
			if len(control.ids) != 2 || control.ids[0] != control.ids[1] || control.ids[0] == "" {
				t.Fatalf("durable command identity changed across restart: %v", control.ids)
			}
			if control.decisions[0] != velav1.StageWorkerCommandDecision_STAGE_WORKER_COMMAND_DECISION_ACCEPTED ||
				control.decisions[1] != velav1.StageWorkerCommandDecision_STAGE_WORKER_COMMAND_DECISION_REPLAYED {
				t.Fatalf("unexpected durable outcomes: %v", control.decisions)
			}
			fixture.unchanged(t, before)
			if pending, err := reopened.List(ctx); err != nil || len(pending) != 0 {
				t.Fatalf("confirmed cleanup did not retire journal: %+v error=%v", pending, err)
			}
			if _, err := os.Stat(outputPath); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("old output was not retired: %v", err)
			}
		})
	}
}

var errStreamReplayResponseLost = errors.New("simulated loss after durable control result")

// This adapter models transport's random correlation ID for callers that omit
// one. The handler, executor, role-scoped database and restart journal are real.
type streamReplayControl struct {
	handler   *stageworkercontrol.Handler
	identity  stageworkertransport.Identity
	session   int64
	loseNext  bool
	ids       []string
	decisions []velav1.StageWorkerCommandDecision
}

func (control *streamReplayControl) NextCommand(ctx context.Context) (*velav1.StageWorkerControlServiceConnectResponse, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

func (control *streamReplayControl) Exchange(ctx context.Context, request *velav1.StageWorkerControlServiceConnectRequest) (*velav1.StageWorkerControlServiceConnectResponse, error) {
	message := proto.Clone(request).(*velav1.StageWorkerControlServiceConnectRequest)
	if message.RequestId == "" {
		message.RequestId = uuid.NewString()
	}
	control.ids = append(control.ids, message.RequestId)
	response, err := control.handler.Handle(ctx, control.identity, control.session, message)
	if err != nil {
		return nil, err
	}
	control.decisions = append(control.decisions, response.GetStageCommandResult().GetDecision())
	if control.loseNext && response.GetStageCommandResult().GetDecision() == velav1.StageWorkerCommandDecision_STAGE_WORKER_COMMAND_DECISION_ACCEPTED {
		control.loseNext = false
		return nil, errStreamReplayResponseLost
	}
	return response, nil
}

type unusedStreamReplayRuntime struct {
	velav1.ModelRuntimeServiceClient
}
