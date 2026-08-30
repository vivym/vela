package stageworkeragent_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"testing"
	"time"

	"github.com/vivym/vela/internal/materializationauthority"
	"github.com/vivym/vela/internal/modelruntime"
	"github.com/vivym/vela/internal/stageartifact"
	"github.com/vivym/vela/internal/stageauthority"
	"github.com/vivym/vela/internal/stageworkeragent"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestStreamAgentStartsControlAuthorityOnlyAfterRuntimeBarrier(t *testing.T) {
	fixture := newBarrierFixture(t, false)
	runtimeAgent, err := stageworkeragent.New(stageworkeragent.Config{
		Members: []stageworkeragent.RuntimeMember{
			{ID: fixture.memberIDs[0], Client: fixture.clients[0]},
			{ID: fixture.memberIDs[1], Client: fixture.clients[1]},
		},
	})
	if err != nil {
		t.Fatalf("New Agent: %v", err)
	}
	control := &recordingStreamControl{decision: velav1.StageWorkerCommandDecision_STAGE_WORKER_COMMAND_DECISION_ACCEPTED}
	streamAgent, err := stageworkeragent.NewStreamAgent(runtimeAgent, control)
	if err != nil {
		t.Fatalf("NewStreamAgent: %v", err)
	}
	result, err := streamAgent.ExecuteAssignment(context.Background(), fixture.assignment)
	if err != nil || !result.BarrierPassed || !result.ControlStartAccepted ||
		control.lastRequest.GetStartStage() == nil {
		t.Fatalf("ExecuteAssignment = %#v error=%v request=%#v", result, err, control.lastRequest)
	}
}

func TestStreamAgentCancelsRuntimeWhenControlStartRejects(t *testing.T) {
	fixture := newBarrierFixture(t, false)
	runtimeAgent, err := stageworkeragent.New(stageworkeragent.Config{
		Members: []stageworkeragent.RuntimeMember{
			{ID: fixture.memberIDs[0], Client: fixture.clients[0]},
			{ID: fixture.memberIDs[1], Client: fixture.clients[1]},
		},
	})
	if err != nil {
		t.Fatalf("New Agent: %v", err)
	}
	control := &recordingStreamControl{decision: velav1.StageWorkerCommandDecision_STAGE_WORKER_COMMAND_DECISION_STALE}
	streamAgent, err := stageworkeragent.NewStreamAgent(runtimeAgent, control)
	if err != nil {
		t.Fatalf("NewStreamAgent: %v", err)
	}
	result, err := streamAgent.ExecuteAssignment(context.Background(), fixture.assignment)
	if err == nil || result.ControlStartAccepted || result.CancellationAcknowledgedMembers != 2 {
		t.Fatalf("rejected ExecuteAssignment = %#v error=%v", result, err)
	}
}

func TestStreamAgentReattachesOnlyAfterSameRuntimeConfirmsAuthority(t *testing.T) {
	fixture := newBarrierFixture(t, false)
	runtimeAgent, err := stageworkeragent.New(stageworkeragent.Config{
		Members: []stageworkeragent.RuntimeMember{
			{ID: fixture.memberIDs[0], Client: fixture.clients[0]},
			{ID: fixture.memberIDs[1], Client: fixture.clients[1]},
		},
	})
	if err != nil {
		t.Fatalf("New Agent: %v", err)
	}
	if _, err := runtimeAgent.PrepareAndStart(context.Background(), fixture.assignment); err != nil {
		t.Fatalf("PrepareAndStart: %v", err)
	}
	control := &recordingStreamControl{decision: velav1.StageWorkerCommandDecision_STAGE_WORKER_COMMAND_DECISION_ACCEPTED}
	streamAgent, err := stageworkeragent.NewStreamAgent(runtimeAgent, control)
	if err != nil {
		t.Fatalf("NewStreamAgent: %v", err)
	}
	result, err := streamAgent.Reattach(context.Background(), fixture.authority, "", nil)
	if err != nil || !result.Accepted || control.lastRequest.GetReattachStage() == nil {
		t.Fatalf("Reattach = %#v error=%v request=%#v", result, err, control.lastRequest)
	}
	calls := control.calls
	stale := proto.Clone(fixture.authority).(*velav1.StageAuthority)
	stale.Members[0].ModelRuntimeEpoch++
	if _, err := streamAgent.Reattach(context.Background(), stale, "", nil); err == nil {
		t.Fatal("Reattach accepted a stale ModelRuntime epoch")
	}
	if control.calls != calls {
		t.Fatalf("stale reattach reached control stream: calls=%d want=%d", control.calls, calls)
	}
}

func TestStreamAgentReattachRequiresExactLocalReceipt(t *testing.T) {
	fixture := newBarrierFixture(t, false)
	runtimeAgent, err := stageworkeragent.New(stageworkeragent.Config{
		Members: []stageworkeragent.RuntimeMember{
			{ID: fixture.memberIDs[0], Client: fixture.clients[0]},
			{ID: fixture.memberIDs[1], Client: fixture.clients[1]},
		},
	})
	if err != nil {
		t.Fatalf("New Agent: %v", err)
	}
	if _, err := runtimeAgent.PrepareAndStart(context.Background(), fixture.assignment); err != nil {
		t.Fatalf("PrepareAndStart: %v", err)
	}
	manifest := []byte(`{"kind":"SHARDED_OUTPUT","path":"/local/output.bin"}`)
	var receipt *velav1.LocalMaterializationReceipt
	for index, backend := range fixture.backends {
		backend.MarkOutputReady(manifest)
		sealed, sealErr := fixture.clients[index].SealOutput(
			context.Background(),
			&velav1.ModelRuntimeServiceSealOutputRequest{Authority: fixture.authority},
		)
		if sealErr != nil || sealed.GetReceipt() == nil {
			t.Fatalf("SealOutput member %d = %#v error=%v", index, sealed, sealErr)
		}
		if receipt == nil {
			receipt = sealed.GetReceipt()
		}
	}
	control := &recordingStreamControl{decision: velav1.StageWorkerCommandDecision_STAGE_WORKER_COMMAND_DECISION_ACCEPTED}
	streamAgent, err := stageworkeragent.NewStreamAgent(runtimeAgent, control)
	if err != nil {
		t.Fatalf("NewStreamAgent: %v", err)
	}
	result, err := streamAgent.Reattach(
		context.Background(),
		fixture.authority,
		receipt.GetReceiptId(),
		receipt.GetManifestSha256(),
	)
	if err != nil || !result.Accepted {
		t.Fatalf("exact receipt Reattach = %#v error=%v", result, err)
	}
	calls := control.calls
	wrongDigest := append([]byte(nil), receipt.GetManifestSha256()...)
	wrongDigest[0] ^= 0xff
	if _, err := streamAgent.Reattach(
		context.Background(), fixture.authority, receipt.GetReceiptId(), wrongDigest,
	); err == nil {
		t.Fatal("Reattach accepted a mismatched local receipt")
	}
	if control.calls != calls {
		t.Fatalf("mismatched local receipt reached control: calls=%d want=%d", control.calls, calls)
	}
}

func TestStreamAgentConsumesUnsolicitedStopStage(t *testing.T) {
	fixture := newBarrierFixture(t, false)
	runtimeAgent, err := stageworkeragent.New(stageworkeragent.Config{
		Members: []stageworkeragent.RuntimeMember{
			{ID: fixture.memberIDs[0], Client: fixture.clients[0]},
			{ID: fixture.memberIDs[1], Client: fixture.clients[1]},
		},
	})
	if err != nil {
		t.Fatalf("New Agent: %v", err)
	}
	commands := make(chan *velav1.StageWorkerControlServiceConnectResponse, 1)
	control := &recordingStreamControl{
		decision: velav1.StageWorkerCommandDecision_STAGE_WORKER_COMMAND_DECISION_ACCEPTED,
		commands: commands,
	}
	streamAgent, err := stageworkeragent.NewStreamAgent(runtimeAgent, control)
	if err != nil {
		t.Fatalf("NewStreamAgent: %v", err)
	}
	if _, err := streamAgent.ExecuteAssignment(context.Background(), fixture.assignment); err != nil {
		t.Fatalf("ExecuteAssignment: %v", err)
	}
	commands <- &velav1.StageWorkerControlServiceConnectResponse{
		Result: &velav1.StageWorkerControlServiceConnectResponse_StopStage{
			StopStage: &velav1.StopStage{
				Authority: fixture.authority,
				Reason:    velav1.StageWorkerStopReason_STAGE_WORKER_STOP_REASON_AUTHORITY_REVOKED,
			},
		},
	}
	close(commands)
	if err := streamAgent.RunControlCommands(context.Background()); err != nil {
		t.Fatalf("RunControlCommands: %v", err)
	}
	status, err := runtimeAgent.Status(context.Background(), fixture.authority)
	if err != nil {
		t.Fatalf("Status after unsolicited StopStage: %v", err)
	}
	for memberID, state := range status.States {
		if state != velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_CANCELING {
			t.Errorf("member %s state = %s, want CANCELING", memberID, state)
		}
	}
}

func TestStreamAgentRetriesMaterializationWithoutHoldingOrRerunningGPU(t *testing.T) {
	fixture := newSingleMemberMaterializationFixture(t)
	runtimeAgent, err := stageworkeragent.New(stageworkeragent.Config{
		Members: []stageworkeragent.RuntimeMember{{
			ID: fixture.memberID, Client: fixture.client,
		}},
	})
	if err != nil {
		t.Fatalf("New Agent: %v", err)
	}
	control := newMaterializingStreamControl(t, fixture.authority)
	publisher := &outageOncePublisher{failures: 1, objectVersion: "l2-version-1"}
	source, err := stageartifact.NewFilesystemLocalOutputSource(fixture.localRoot)
	if err != nil {
		t.Fatalf("NewFilesystemLocalOutputSource: %v", err)
	}
	journal, err := stageworkeragent.NewMemoryMaterializationJournal(4)
	if err != nil {
		t.Fatalf("NewMemoryMaterializationJournal: %v", err)
	}
	streamAgent, err := stageworkeragent.NewMaterializingStreamAgent(
		runtimeAgent,
		control,
		stageworkeragent.MaterializationConfig{
			Validator: control.validator, Source: source, Publisher: publisher, Journal: journal,
			SourceLossEvidence: testSourceLossEvidenceProvider(),
		},
	)
	if err != nil {
		t.Fatalf("NewMaterializingStreamAgent: %v", err)
	}
	if _, err := streamAgent.ExecuteAssignment(context.Background(), fixture.assignment); err != nil {
		t.Fatalf("ExecuteAssignment: %v", err)
	}
	fixture.backend.MarkOutputReadyWithSize(fixture.manifest, int64(len(fixture.payload)))

	first, err := streamAgent.SealAndMaterialize(context.Background())
	if err == nil || !first.LocalSealed || !first.GPUReleased || first.Committed ||
		publisher.calls != 1 || fixture.countingBackend.sealCalls != 1 || control.sealCalls != 1 ||
		control.commitCalls != 0 {
		t.Fatalf(
			"first SealAndMaterialize = %#v error=%v publisher=%d runtime_seal=%d control_seal=%d commit=%d",
			first, err, publisher.calls, fixture.countingBackend.sealCalls,
			control.sealCalls, control.commitCalls,
		)
	}
	if _, err := streamAgent.Heartbeat(context.Background(), 1); err == nil {
		t.Fatal("GPU StageAuthority remained active after durable seal")
	}

	resumed, err := streamAgent.ResumeMaterializations(context.Background())
	if err != nil || !resumed.L2Published || !resumed.Committed || publisher.calls != 2 ||
		fixture.countingBackend.sealCalls != 1 || control.sealCalls != 1 || control.commitCalls != 1 {
		t.Fatalf(
			"ResumeMaterializations = %#v error=%v publisher=%d runtime_seal=%d control_seal=%d commit=%d",
			resumed, err, publisher.calls, fixture.countingBackend.sealCalls,
			control.sealCalls, control.commitCalls,
		)
	}
}

func TestStreamAgentRetriesCommitFromExactPublishedVersionAfterReconnect(t *testing.T) {
	fixture := newSingleMemberMaterializationFixture(t)
	runtimeAgent, err := stageworkeragent.New(stageworkeragent.Config{
		Members: []stageworkeragent.RuntimeMember{{ID: fixture.memberID, Client: fixture.client}},
	})
	if err != nil {
		t.Fatalf("New Agent: %v", err)
	}
	control := newMaterializingStreamControl(t, fixture.authority)
	control.commitFailures = 1
	publisher := &outageOncePublisher{objectVersion: "l2-version-commit-retry"}
	source, err := stageartifact.NewFilesystemLocalOutputSource(fixture.localRoot)
	if err != nil {
		t.Fatalf("NewFilesystemLocalOutputSource: %v", err)
	}
	journal, err := stageworkeragent.NewMemoryMaterializationJournal(4)
	if err != nil {
		t.Fatalf("NewMemoryMaterializationJournal: %v", err)
	}
	config := stageworkeragent.MaterializationConfig{
		Validator: control.validator, Source: source, Publisher: publisher, Journal: journal,
		SourceLossEvidence: testSourceLossEvidenceProvider(),
	}
	streamAgent, err := stageworkeragent.NewMaterializingStreamAgent(runtimeAgent, control, config)
	if err != nil {
		t.Fatalf("NewMaterializingStreamAgent: %v", err)
	}
	if _, err := streamAgent.ExecuteAssignment(context.Background(), fixture.assignment); err != nil {
		t.Fatalf("ExecuteAssignment: %v", err)
	}
	fixture.backend.MarkOutputReadyWithSize(fixture.manifest, int64(len(fixture.payload)))

	first, err := streamAgent.SealAndMaterialize(context.Background())
	if err == nil || !first.L2Published || first.Committed || publisher.calls != 1 ||
		control.commitCalls != 1 {
		t.Fatalf("first materialization = %#v error=%v publisher=%d commit=%d", first, err, publisher.calls, control.commitCalls)
	}
	if err := os.Remove(fixture.localRoot + "/dit.bin"); err != nil {
		t.Fatalf("remove local source after exact publication: %v", err)
	}

	reconnected, err := stageworkeragent.NewMaterializingStreamAgent(runtimeAgent, control, config)
	if err != nil {
		t.Fatalf("reconnect StreamAgent: %v", err)
	}
	resumed, err := reconnected.ResumeMaterializations(context.Background())
	if err != nil || !resumed.L2Published || !resumed.Committed || publisher.calls != 1 ||
		fixture.countingBackend.sealCalls != 1 || control.sealCalls != 1 || control.commitCalls != 2 {
		t.Fatalf(
			"reconnected ResumeMaterializations = %#v error=%v publisher=%d runtime_seal=%d control_seal=%d commit=%d",
			resumed, err, publisher.calls, fixture.countingBackend.sealCalls,
			control.sealCalls, control.commitCalls,
		)
	}
}

func TestStreamAgentReportsLocalSourceLossWithMaterializationAuthority(t *testing.T) {
	fixture := newSingleMemberMaterializationFixture(t)
	runtimeAgent, err := stageworkeragent.New(stageworkeragent.Config{
		Members: []stageworkeragent.RuntimeMember{{ID: fixture.memberID, Client: fixture.client}},
	})
	if err != nil {
		t.Fatalf("New Agent: %v", err)
	}
	control := newMaterializingStreamControl(t, fixture.authority)
	publisher := &outageOncePublisher{objectVersion: "must-not-publish"}
	source, err := stageartifact.NewFilesystemLocalOutputSource(fixture.localRoot)
	if err != nil {
		t.Fatalf("NewFilesystemLocalOutputSource: %v", err)
	}
	journal, err := stageworkeragent.NewMemoryMaterializationJournal(4)
	if err != nil {
		t.Fatalf("NewMemoryMaterializationJournal: %v", err)
	}
	lostAt := time.Now().UTC().Add(time.Second)
	streamAgent, err := stageworkeragent.NewMaterializingStreamAgent(
		runtimeAgent,
		control,
		stageworkeragent.MaterializationConfig{
			Validator: control.validator, Source: source, Publisher: publisher, Journal: journal,
			SourceLossEvidence: stageworkeragent.MaterializationSourceLossEvidenceFunc(
				func(context.Context, stageworkeragent.PendingMaterialization) (
					stageworkeragent.MaterializationSourceLossEvidence, error,
				) {
					return stageworkeragent.MaterializationSourceLossEvidence{
						FailureFingerprint:    sha256.Sum256([]byte("local output vanished")),
						ConsumedResourceUnits: 100,
						LostAt:                lostAt,
						RetryAt:               lostAt.Add(time.Second),
					}, nil
				},
			),
		},
	)
	if err != nil {
		t.Fatalf("NewMaterializingStreamAgent: %v", err)
	}
	if _, err := streamAgent.ExecuteAssignment(context.Background(), fixture.assignment); err != nil {
		t.Fatalf("ExecuteAssignment: %v", err)
	}
	fixture.backend.MarkOutputReadyWithSize(fixture.manifest, int64(len(fixture.payload)))
	if err := os.Remove(fixture.localRoot + "/dit.bin"); err != nil {
		t.Fatalf("remove local source: %v", err)
	}

	result, err := streamAgent.SealAndMaterialize(context.Background())
	if !errors.Is(err, stageworkeragent.ErrMaterializationSourceLostReported) ||
		!result.GPUReleased || !result.SourceLostReported || result.Committed ||
		publisher.calls != 0 || fixture.countingBackend.sealCalls != 1 ||
		control.sealCalls != 1 || control.sourceLossCalls != 1 || control.commitCalls != 0 {
		t.Fatalf(
			"source-loss result=%#v error=%v publisher=%d runtime_seal=%d control_seal=%d source_loss=%d commit=%d",
			result, err, publisher.calls, fixture.countingBackend.sealCalls, control.sealCalls,
			control.sourceLossCalls, control.commitCalls,
		)
	}
	pending, err := journal.List(context.Background())
	if err != nil || len(pending) != 0 {
		t.Fatalf("journal after accepted source loss = %#v error=%v", pending, err)
	}
}

type recordingStreamControl struct {
	decision    velav1.StageWorkerCommandDecision
	lastRequest *velav1.StageWorkerControlServiceConnectRequest
	calls       int
	err         error
	commands    <-chan *velav1.StageWorkerControlServiceConnectResponse
}

type singleMemberMaterializationFixture struct {
	authority       *velav1.StageAuthority
	assignment      *velav1.StageAssignment
	memberID        string
	client          velav1.ModelRuntimeServiceClient
	backend         *modelruntime.FakeRuntime
	countingBackend *countingSealBackend
	localRoot       string
	payload         []byte
	manifest        []byte
}

func newSingleMemberMaterializationFixture(t *testing.T) singleMemberMaterializationFixture {
	t.Helper()
	now := time.Now().UTC()
	memberID := "42000000-0000-0000-0000-000000000011"
	keys := map[string][]byte{"single-stage-key": bytes.Repeat([]byte{0x7b}, 32)}
	signer, err := stageauthority.NewSigner(keys)
	if err != nil {
		t.Fatalf("NewSigner: %v", err)
	}
	validator, err := stageauthority.NewValidator(keys, time.Now)
	if err != nil {
		t.Fatalf("NewValidator: %v", err)
	}
	executionSpec := &velav1.StageExecutionSpec{ParametersJson: []byte(`{"steps":30}`)}
	executionSpecDigest, err := stageauthority.ExecutionSpecDigest(executionSpec)
	if err != nil {
		t.Fatalf("ExecutionSpecDigest: %v", err)
	}
	authority, err := signer.Sign(&velav1.StageAuthority{
		SchemaVersion:     1,
		JobId:             "12000000-0000-0000-0000-000000000011",
		AttemptId:         "12000000-0000-0000-0000-000000000012",
		StageRunId:        "12000000-0000-0000-0000-000000000013",
		StageAttemptId:    "12000000-0000-0000-0000-000000000014",
		StageAllocationId: "12000000-0000-0000-0000-000000000015",
		StageLeaseId:      "12000000-0000-0000-0000-000000000016",
		AttemptFence:      2, StageFence: 3, StageVersion: 4,
		WorkerInstanceId:    "22000000-0000-0000-0000-000000000011",
		WorkerInstanceEpoch: 5,
		DeviceSetDigest:     bytes.Repeat([]byte{0x81}, 32),
		Devices: []*velav1.StageAuthorityDeviceEpoch{{
			DeviceId: "32000000-0000-0000-0000-000000000011", DeviceEpoch: 7,
		}},
		MembershipDigest: bytes.Repeat([]byte{0x82}, 32),
		Members: []*velav1.StageAuthorityMemberEpoch{{
			WorkerMemberId: memberID, MemberEpoch: 8, ModelRuntimeEpoch: 9,
		}},
		ModelResidencyId:            "52000000-0000-0000-0000-000000000011",
		ModelRuntimeIdentity:        "h3-dit-runtime-1",
		StageProfileRevisionId:      "62000000-0000-0000-0000-000000000011",
		CapacityObservationSequence: 10,
		CapacityVector:              map[string]int64{"active_stage_slots": 1, "gpu_count": 1},
		LeaseToken:                  bytes.Repeat([]byte{0x83}, 32),
		ExecutionNonce:              bytes.Repeat([]byte{0x84}, 32),
		ExecutionSpecDigest:         executionSpecDigest[:],
		SigningKeyId:                "single-stage-key",
		IssuedAt:                    timestamppb.New(now),
		ExpiresAt:                   timestamppb.New(now.Add(5 * time.Minute)),
		MonotonicValidFor:           durationpb.New(5 * time.Minute),
	})
	if err != nil {
		t.Fatalf("Sign StageAuthority: %v", err)
	}
	backend := modelruntime.NewFakeDiTRuntime()
	countingBackend := &countingSealBackend{Backend: backend}
	service, err := modelruntime.NewService(modelruntime.Config{
		Binding: stageauthority.RuntimeBinding{
			WorkerInstanceID: "22000000-0000-0000-0000-000000000011", WorkerInstanceEpoch: 5,
			WorkerMemberID: memberID, WorkerMemberEpoch: 8,
			DeviceSetDigest: bytes.Repeat([]byte{0x81}, 32),
			Devices: []stageauthority.DeviceEpoch{{
				ID: "32000000-0000-0000-0000-000000000011", Epoch: 7,
			}},
			MembershipDigest:     bytes.Repeat([]byte{0x82}, 32),
			Members:              []stageauthority.MemberEpoch{{ID: memberID, Epoch: 8}},
			ModelResidencyID:     "52000000-0000-0000-0000-000000000011",
			ModelRuntimeIdentity: "h3-dit-runtime-1", ModelRuntimeEpoch: 0,
			StageProfileRevisionID: "62000000-0000-0000-0000-000000000011",
		},
		EpochStore: modelruntime.EpochStoreFunc(func(stageauthority.RuntimeBinding) (int64, error) {
			return 9, nil
		}),
		Validator: validator, Backend: countingBackend, CancelTimeout: time.Second,
	})
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}
	t.Cleanup(service.Close)
	payload := []byte("sealed-dit-latent")
	payloadDigest := sha256.Sum256(payload)
	manifest := []byte(fmt.Sprintf(
		`{"schema_version":1,"output_port":"latent","local_locator":"dit.bin","content_type":"application/octet-stream","payload_sha256":"%s","size_bytes":%d,"lineage":{"attempt_id":"%s","stage_run_id":"%s","stage_attempt_id":"%s","stage_lease_id":"%s","attempt_fence":%d,"stage_fence":%d,"stage_profile_revision_id":"%s"}}`,
		hex.EncodeToString(payloadDigest[:]), len(payload), authority.GetAttemptId(),
		authority.GetStageRunId(), authority.GetStageAttemptId(), authority.GetStageLeaseId(),
		authority.GetAttemptFence(), authority.GetStageFence(), authority.GetStageProfileRevisionId(),
	))
	root := t.TempDir()
	if err := os.WriteFile(root+"/dit.bin", payload, 0o600); err != nil {
		t.Fatalf("write local output: %v", err)
	}
	return singleMemberMaterializationFixture{
		authority: authority,
		assignment: &velav1.StageAssignment{
			Authority: authority, ExecutionSpec: executionSpec,
			RequiredWorkerMemberIds: []string{memberID},
			MemberStartTimeout:      durationpb.New(5 * time.Second),
		},
		memberID: memberID, client: serveBarrierRuntime(t, service), backend: backend,
		countingBackend: countingBackend, localRoot: root, payload: payload, manifest: manifest,
	}
}

type countingSealBackend struct {
	modelruntime.Backend
	sealCalls int
}

func (backend *countingSealBackend) Seal(
	ctx context.Context,
	authority stageauthority.Verified,
) (modelruntime.SealedOutput, error) {
	backend.sealCalls++
	return backend.Backend.Seal(ctx, authority)
}

type outageOncePublisher struct {
	calls         int
	failures      int
	objectVersion string
}

func (publisher *outageOncePublisher) Publish(
	_ context.Context,
	lease stageartifact.MaterializationLease,
	source io.Reader,
) (stageartifact.PublishedObject, error) {
	publisher.calls++
	if publisher.failures > 0 {
		publisher.failures--
		return stageartifact.PublishedObject{}, errors.New("injected L2 outage")
	}
	payload, err := io.ReadAll(source)
	if err != nil {
		return stageartifact.PublishedObject{}, err
	}
	digest := sha256.Sum256(payload)
	if int64(len(payload)) != lease.SizeBytes || digest != lease.SHA256 {
		return stageartifact.PublishedObject{}, errors.New("publisher received mismatched local output")
	}
	return stageartifact.PublishedObject{
		ObjectKey: lease.ObjectKey, ObjectVersion: publisher.objectVersion,
	}, nil
}

type materializingStreamControl struct {
	t               *testing.T
	signer          *materializationauthority.Signer
	validator       *materializationauthority.Validator
	authority       *velav1.StageAuthority
	sealCalls       int
	commitCalls     int
	commitFailures  int
	sourceLossCalls int
	materialization *velav1.MaterializationAuthority
}

func newMaterializingStreamControl(
	t *testing.T,
	authority *velav1.StageAuthority,
) *materializingStreamControl {
	t.Helper()
	keys := map[string][]byte{"materialization-test-key": bytes.Repeat([]byte{0x8b}, 32)}
	signer, err := materializationauthority.NewSigner(keys)
	if err != nil {
		t.Fatalf("New materialization signer: %v", err)
	}
	validator, err := materializationauthority.NewValidator(keys, time.Now)
	if err != nil {
		t.Fatalf("New materialization validator: %v", err)
	}
	return &materializingStreamControl{
		t: t, signer: signer, validator: validator, authority: authority,
	}
}

func (control *materializingStreamControl) Commands() <-chan *velav1.StageWorkerControlServiceConnectResponse {
	return nil
}

func (control *materializingStreamControl) Exchange(
	_ context.Context,
	request *velav1.StageWorkerControlServiceConnectRequest,
) (*velav1.StageWorkerControlServiceConnectResponse, error) {
	switch operation := request.GetOperation().(type) {
	case *velav1.StageWorkerControlServiceConnectRequest_StartStage:
		return commandResultResponse(
			velav1.StageWorkerOperation_STAGE_WORKER_OPERATION_START_STAGE,
			velav1.StageWorkerCommandDecision_STAGE_WORKER_COMMAND_DECISION_ACCEPTED,
		), nil
	case *velav1.StageWorkerControlServiceConnectRequest_SealStageOutput:
		control.sealCalls++
		receipt := operation.SealStageOutput.GetLocalReceipt()
		manifest, err := stageartifact.ParseLocalOutputManifestV1(receipt.GetOutputManifestJson())
		if err != nil {
			return nil, err
		}
		stageDigest, err := stageauthority.Digest(control.authority)
		if err != nil {
			return nil, err
		}
		spiffeDigest := sha256.Sum256([]byte("spiffe://vela.test/worker/dit"))
		control.materialization, err = control.signer.Sign(&velav1.MaterializationAuthority{
			SchemaVersion: 1, StageAuthorityDigest: stageDigest[:],
			StageMaterializationLeaseId: "82000000-0000-0000-0000-000000000001",
			StageArtifactId:             "82000000-0000-0000-0000-000000000002",
			ObjectKey:                   "artifacts/stage/test/latent.bin", ContentType: manifest.ContentType,
			Sha256: manifest.PayloadSHA256[:], SizeBytes: manifest.SizeBytes,
			LocalReceiptId: receipt.GetReceiptId(), LocalReceiptDigest: receipt.GetManifestSha256(),
			SigningKeyId: "materialization-test-key", IssuedAt: receipt.GetSealedAt(),
			ExpiresAt:                 timestamppb.New(receipt.GetSealedAt().AsTime().Add(5 * time.Minute)),
			SourceWorkerInstanceId:    control.authority.GetWorkerInstanceId(),
			SourceWorkerInstanceEpoch: control.authority.GetWorkerInstanceEpoch(),
			SourceWorkerMemberId:      control.authority.GetMembers()[0].GetWorkerMemberId(),
			SourceWorkerMemberEpoch:   control.authority.GetMembers()[0].GetMemberEpoch(),
			SourceSpiffeIdDigest:      spiffeDigest[:],
		})
		if err != nil {
			return nil, err
		}
		return &velav1.StageWorkerControlServiceConnectResponse{
			Result: &velav1.StageWorkerControlServiceConnectResponse_MaterializationAuthority{
				MaterializationAuthority: control.materialization,
			},
		}, nil
	case *velav1.StageWorkerControlServiceConnectRequest_CommitStageMaterialization:
		control.commitCalls++
		if control.commitFailures > 0 {
			control.commitFailures--
			return nil, errors.New("injected CommitStageMaterialization outage")
		}
		if _, err := control.validator.Validate(operation.CommitStageMaterialization.GetMaterializationAuthority()); err != nil {
			return nil, err
		}
		if operation.CommitStageMaterialization.GetObjectVersion() == "" {
			return nil, errors.New("missing exact object version")
		}
		return commandResultResponse(
			velav1.StageWorkerOperation_STAGE_WORKER_OPERATION_COMMIT_STAGE_MATERIALIZATION,
			velav1.StageWorkerCommandDecision_STAGE_WORKER_COMMAND_DECISION_ACCEPTED,
		), nil
	case *velav1.StageWorkerControlServiceConnectRequest_ReportMaterializationSourceLost:
		control.sourceLossCalls++
		report := operation.ReportMaterializationSourceLost
		if _, err := control.validator.Validate(report.GetMaterializationAuthority()); err != nil {
			return nil, err
		}
		if len(report.GetFailureFingerprint()) != sha256.Size ||
			report.GetConsumedResourceUnits() <= 0 || report.GetLostAt() == nil ||
			report.GetRetryAt() == nil ||
			!report.GetRetryAt().AsTime().After(report.GetLostAt().AsTime()) {
			return nil, errors.New("invalid source-loss evidence")
		}
		return commandResultResponse(
			velav1.StageWorkerOperation_STAGE_WORKER_OPERATION_REPORT_MATERIALIZATION_SOURCE_LOST,
			velav1.StageWorkerCommandDecision_STAGE_WORKER_COMMAND_DECISION_ACCEPTED,
		), nil
	default:
		return nil, errors.New("unexpected Stage Worker operation")
	}
}

func commandResultResponse(
	operation velav1.StageWorkerOperation,
	decision velav1.StageWorkerCommandDecision,
) *velav1.StageWorkerControlServiceConnectResponse {
	return &velav1.StageWorkerControlServiceConnectResponse{
		Result: &velav1.StageWorkerControlServiceConnectResponse_StageCommandResult{
			StageCommandResult: &velav1.StageCommandResult{
				Operation: operation, Decision: decision,
			},
		},
	}
}

func testSourceLossEvidenceProvider() stageworkeragent.MaterializationSourceLossEvidenceProvider {
	return stageworkeragent.MaterializationSourceLossEvidenceFunc(
		func(
			_ context.Context,
			record stageworkeragent.PendingMaterialization,
		) (stageworkeragent.MaterializationSourceLossEvidence, error) {
			lostAt := record.LocalReceipt.GetSealedAt().AsTime().UTC().Add(time.Second)
			return stageworkeragent.MaterializationSourceLossEvidence{
				FailureFingerprint:    sha256.Sum256([]byte("default local source loss")),
				ConsumedResourceUnits: 1,
				LostAt:                lostAt,
				RetryAt:               lostAt.Add(time.Second),
			}, nil
		},
	)
}

func (control *recordingStreamControl) Commands() <-chan *velav1.StageWorkerControlServiceConnectResponse {
	return control.commands
}

func (control *recordingStreamControl) Exchange(
	_ context.Context,
	request *velav1.StageWorkerControlServiceConnectRequest,
) (*velav1.StageWorkerControlServiceConnectResponse, error) {
	control.calls++
	control.lastRequest = request
	if control.err != nil {
		return nil, control.err
	}
	var operation velav1.StageWorkerOperation
	switch request.GetOperation().(type) {
	case *velav1.StageWorkerControlServiceConnectRequest_StartStage:
		operation = velav1.StageWorkerOperation_STAGE_WORKER_OPERATION_START_STAGE
	case *velav1.StageWorkerControlServiceConnectRequest_HeartbeatStage:
		operation = velav1.StageWorkerOperation_STAGE_WORKER_OPERATION_HEARTBEAT_STAGE
	case *velav1.StageWorkerControlServiceConnectRequest_ReattachStage:
		operation = velav1.StageWorkerOperation_STAGE_WORKER_OPERATION_REATTACH_STAGE
	default:
		return nil, errors.New("unexpected Stage Worker operation")
	}
	return &velav1.StageWorkerControlServiceConnectResponse{
		Result: &velav1.StageWorkerControlServiceConnectResponse_StageCommandResult{
			StageCommandResult: &velav1.StageCommandResult{
				Decision: control.decision, Operation: operation,
			},
		},
	}, nil
}
