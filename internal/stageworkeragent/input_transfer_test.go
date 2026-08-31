package stageworkeragent_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/artifactstore"
	"github.com/vivym/vela/internal/stageartifact"
	"github.com/vivym/vela/internal/stageauthority"
	"github.com/vivym/vela/internal/stageworkeragent"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/protobuf/proto"
)

func TestControlTransferAuthorityResolvesAndConsumesExactTicket(t *testing.T) {
	fixture := newSingleMemberMaterializationFixture(t)
	ticketID := uuid.MustParse("85000000-0000-0000-0000-000000000001")
	artifactID := uuid.MustParse("85000000-0000-0000-0000-000000000002")
	connectorID := uuid.MustParse("85000000-0000-0000-0000-000000000003")
	digest := sha256.Sum256([]byte("exact stage input"))
	control := &transferControl{
		t:         t,
		authority: fixture.authority,
		descriptor: &velav1.ResolvedInputTransfer{
			TicketId: ticketID.String(), StageArtifactId: artifactID.String(),
			ObjectKey: "stage/input/exact.bin", ObjectVersion: "object-version-7",
			Sha256: digest[:], SizeBytes: 17, ContentType: "application/octet-stream",
		},
	}
	authority, err := stageworkeragent.NewControlTransferAuthority(control, fixture.authority)
	if err != nil {
		t.Fatalf("NewControlTransferAuthority: %v", err)
	}
	tokenDigest := sha256.Sum256([]byte("ticket"))
	destination := stageartifact.TransferDestination{
		WorkerInstanceID:    uuid.MustParse(fixture.authority.GetWorkerInstanceId()),
		WorkerInstanceEpoch: fixture.authority.GetWorkerInstanceEpoch(),
		ModelResidencyID:    uuid.MustParse(fixture.authority.GetModelResidencyId()),
		ModelRuntimeEpoch:   fixture.authority.GetMembers()[0].GetModelRuntimeEpoch(),
		ConnectorRevisionID: connectorID,
	}
	resolvedAt := time.Date(2026, 8, 30, 12, 0, 0, 0, time.UTC)
	descriptor, err := authority.Resolve(context.Background(), stageartifact.ResolveTransferCommand{
		TicketID: ticketID, TokenDigest: tokenDigest, Destination: destination,
		ResolvedAt: resolvedAt,
	})
	if err != nil || descriptor.TicketID != ticketID || descriptor.ArtifactID != artifactID ||
		descriptor.ObjectVersion != "object-version-7" || descriptor.SHA256 != digest ||
		descriptor.SizeBytes != 17 {
		t.Fatalf("Resolve = %#v error=%v", descriptor, err)
	}
	resolve := control.requests[0].GetResolveInputTransfer()
	if resolve == nil || !proto.Equal(resolve.GetAuthority(), fixture.authority) ||
		resolve.GetTicketId() != ticketID.String() ||
		!bytes.Equal(resolve.GetTokenDigest(), tokenDigest[:]) ||
		resolve.GetConnectorRevisionId() != connectorID.String() ||
		!resolve.GetResolvedAt().AsTime().Equal(resolvedAt) {
		t.Fatalf("resolve request = %#v", resolve)
	}

	commandID := uuid.MustParse("85000000-0000-0000-0000-000000000004")
	consumedAt := resolvedAt.Add(time.Second)
	if err := authority.Consume(context.Background(), stageartifact.ConsumeTransferCommand{
		CommandID: commandID, TicketID: ticketID, TokenDigest: tokenDigest,
		Destination: destination, OutcomeDigest: digest, ConsumedAt: consumedAt,
	}); err != nil {
		t.Fatalf("Consume: %v", err)
	}
	consumeRequest := control.requests[1]
	consume := consumeRequest.GetConsumeInputTransfer()
	if consumeRequest.GetRequestId() != commandID.String() || consume == nil ||
		!proto.Equal(consume.GetAuthority(), fixture.authority) ||
		consume.GetWorkerInstanceId() != destination.WorkerInstanceID.String() ||
		consume.GetWorkerInstanceEpoch() != destination.WorkerInstanceEpoch ||
		consume.GetModelResidencyId() != destination.ModelResidencyID.String() ||
		consume.GetModelRuntimeEpoch() != destination.ModelRuntimeEpoch ||
		consume.GetConnectorRevisionId() != destination.ConnectorRevisionID.String() ||
		!bytes.Equal(consume.GetOutcomeDigest(), digest[:]) ||
		!consume.GetConsumedAt().AsTime().Equal(consumedAt) {
		t.Fatalf("consume request = %#v", consumeRequest)
	}
}

func TestFilesystemInputTransferTargetCommitsDeterministicExactInput(t *testing.T) {
	root := t.TempDir()
	stageRunID := uuid.MustParse("85000000-0000-0000-0000-000000000011")
	artifactID := uuid.MustParse("85000000-0000-0000-0000-000000000012")
	payload := []byte("exact encoder conditioning tensor")
	digest := sha256.Sum256(payload)
	input := &velav1.StageInputArtifact{
		StageArtifactId: artifactID.String(), ObjectVersion: "object-version-9",
		Sha256: digest[:], SizeBytes: int64(len(payload)),
		StageInterfaceRevisionId: "85000000-0000-0000-0000-000000000013",
	}
	target, err := stageworkeragent.NewFilesystemInputTransferTarget(root, stageRunID, input)
	if err != nil {
		t.Fatalf("NewFilesystemInputTransferTarget: %v", err)
	}
	t.Cleanup(func() { _ = target.Close() })
	ticketID := uuid.MustParse("85000000-0000-0000-0000-000000000014")
	descriptor := stageartifact.TransferDescriptor{
		TicketID: ticketID, ArtifactID: artifactID,
		ObjectKey: "stage/input.bin", ObjectVersion: input.GetObjectVersion(),
		SHA256: digest, SizeBytes: int64(len(payload)), ContentType: "application/octet-stream",
	}
	writer, err := target.Begin(context.Background(), descriptor)
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	if _, err := writer.Write(payload); err != nil {
		t.Fatalf("write input: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("close input: %v", err)
	}
	completedAt := time.Date(2026, 8, 30, 13, 0, 0, 0, time.UTC)
	if err := target.Commit(context.Background(), stageartifact.PullReceipt{
		TicketID: ticketID, ArtifactID: artifactID, SHA256: digest,
		SizeBytes: int64(len(payload)), CompletedAt: completedAt,
	}); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	relative, err := stageworkeragent.StageInputRelativePath(stageRunID, artifactID, digest)
	if err != nil {
		t.Fatalf("StageInputRelativePath: %v", err)
	}
	got, err := os.ReadFile(filepath.Join(root, relative))
	if err != nil || !bytes.Equal(got, payload) {
		t.Fatalf("committed input = %q error=%v path=%q", got, err, relative)
	}
	entries, err := os.ReadDir(filepath.Dir(filepath.Join(root, relative)))
	if err != nil || len(entries) != 1 || entries[0].Name() != filepath.Base(relative) {
		t.Fatalf("input directory entries = %#v error=%v", entries, err)
	}
}

func TestFileInputTransferJournalPersistsExactConsumeReplay(t *testing.T) {
	root := filepath.Join(t.TempDir(), "journal")
	journal, err := stageworkeragent.NewFileInputTransferJournal(root)
	if err != nil {
		t.Fatalf("NewFileInputTransferJournal: %v", err)
	}
	tokenDigest := sha256.Sum256([]byte("durable ticket"))
	record := stageworkeragent.InputTransferJournalRecord{
		TokenDigest: tokenDigest,
		Command: stageartifact.ConsumeTransferCommand{
			CommandID:   uuid.MustParse("85000000-0000-0000-0000-000000000015"),
			TicketID:    uuid.MustParse("85000000-0000-0000-0000-000000000016"),
			TokenDigest: tokenDigest,
			Destination: stageartifact.TransferDestination{
				WorkerInstanceID:    uuid.MustParse("85000000-0000-0000-0000-000000000017"),
				WorkerInstanceEpoch: 7,
				ModelResidencyID:    uuid.MustParse("85000000-0000-0000-0000-000000000018"),
				ModelRuntimeEpoch:   11,
				ConnectorRevisionID: uuid.MustParse("85000000-0000-0000-0000-000000000019"),
			},
			OutcomeDigest: sha256.Sum256([]byte("exact outcome")),
			ConsumedAt:    time.Date(2026, 8, 30, 13, 30, 0, 0, time.UTC),
		},
	}
	if err := journal.PutPending(context.Background(), record); err != nil {
		t.Fatalf("PutPending: %v", err)
	}
	reopened, err := stageworkeragent.NewFileInputTransferJournal(root)
	if err != nil {
		t.Fatalf("reopen FileInputTransferJournal: %v", err)
	}
	loaded, found, err := reopened.Load(context.Background(), tokenDigest)
	if err != nil || !found || loaded.Consumed || loaded.Command != record.Command {
		t.Fatalf("Load pending = %#v found=%v error=%v", loaded, found, err)
	}
	tampered := record
	tampered.Command.Destination.WorkerInstanceEpoch++
	if err := reopened.PutPending(context.Background(), tampered); err == nil {
		t.Fatal("journal accepted consume replay for another destination")
	}
	if err := reopened.MarkConsumed(context.Background(), loaded); err != nil {
		t.Fatalf("MarkConsumed: %v", err)
	}
	reopened, err = stageworkeragent.NewFileInputTransferJournal(root)
	if err != nil {
		t.Fatalf("reopen consumed FileInputTransferJournal: %v", err)
	}
	loaded, found, err = reopened.Load(context.Background(), tokenDigest)
	if err != nil || !found || !loaded.Consumed || loaded.Command != record.Command {
		t.Fatalf("Load consumed = %#v found=%v error=%v", loaded, found, err)
	}
}

func TestStreamAgentPullsExactInputsBeforePreparingRuntime(t *testing.T) {
	fixture := newSingleMemberMaterializationFixture(t)
	now := time.Now().UTC()
	payload := []byte("encoder output consumed by dit")
	digest := sha256.Sum256(payload)
	store := artifactstore.NewLocal()
	version, err := store.PutIfAbsent(
		context.Background(), "artifacts/stage/org/project/attempt/encoder/output.bin", "application/octet-stream",
		bytes.NewReader(payload), int64(len(payload)), digest,
	)
	if err != nil {
		t.Fatalf("seed exact input: %v", err)
	}
	artifactID := uuid.MustParse("85000000-0000-0000-0000-000000000021")
	connectorID := uuid.MustParse("85000000-0000-0000-0000-000000000022")
	ticketID := uuid.MustParse("85000000-0000-0000-0000-000000000023")
	input := &velav1.StageInputArtifact{
		StageArtifactId: artifactID.String(), ObjectVersion: version.VersionID,
		Sha256: digest[:], SizeBytes: int64(len(payload)),
		StageInterfaceRevisionId: "85000000-0000-0000-0000-000000000024",
	}
	assignment := proto.Clone(fixture.assignment).(*velav1.StageAssignment)
	assignment.ExecutionSpec.Inputs = []*velav1.StageInputArtifact{input}
	executionDigest, err := stageauthority.ExecutionSpecDigest(assignment.ExecutionSpec)
	if err != nil {
		t.Fatalf("ExecutionSpecDigest: %v", err)
	}
	unsigned := proto.Clone(assignment.Authority).(*velav1.StageAuthority)
	unsigned.ExecutionSpecDigest = executionDigest[:]
	unsigned.Signature = nil
	stageSigner, err := stageauthority.NewSigner(map[string][]byte{
		"single-stage-key": bytes.Repeat([]byte{0x7b}, 32),
	})
	if err != nil {
		t.Fatalf("New StageAuthority signer: %v", err)
	}
	assignment.Authority, err = stageSigner.Sign(unsigned)
	if err != nil {
		t.Fatalf("sign StageAuthority with exact input: %v", err)
	}
	destination := stageartifact.TransferDestination{
		WorkerInstanceID:    uuid.MustParse(assignment.Authority.GetWorkerInstanceId()),
		WorkerInstanceEpoch: assignment.Authority.GetWorkerInstanceEpoch(),
		ModelResidencyID:    uuid.MustParse(assignment.Authority.GetModelResidencyId()),
		ModelRuntimeEpoch:   assignment.Authority.GetMembers()[0].GetModelRuntimeEpoch(),
		ConnectorRevisionID: connectorID,
	}
	ticketSigner, err := stageartifact.NewTransferTicketSigner(
		"single-stage-key", bytes.Repeat([]byte{0x7b}, 32),
	)
	if err != nil {
		t.Fatalf("New TransferTicket signer: %v", err)
	}
	ticket, err := ticketSigner.Sign(stageartifact.TransferTicketClaims{
		TicketID: ticketID, Destination: destination,
		IssuedAt: now.Add(-time.Second), ExpiresAt: now.Add(time.Minute),
	})
	if err != nil {
		t.Fatalf("sign TransferTicket: %v", err)
	}
	assignment.InputTransferTickets = []*velav1.StageInputTransferTicket{{
		StageArtifactId: artifactID.String(), ObjectVersion: version.VersionID,
		TransferTicket: ticket.Token,
	}}
	control := &transferControl{
		t: t, authority: assignment.Authority,
		consumeFailures: 1,
		descriptor: &velav1.ResolvedInputTransfer{
			TicketId: ticketID.String(), StageArtifactId: artifactID.String(),
			ObjectKey: version.ObjectKey, ObjectVersion: version.VersionID,
			Sha256: digest[:], SizeBytes: int64(len(payload)),
			ContentType: "application/octet-stream",
		},
	}
	inputRoot := t.TempDir()
	resolver, err := stageworkeragent.NewAssignmentInputResolver(
		stageworkeragent.AssignmentInputResolverConfig{
			Store: store, TicketSigner: ticketSigner, Control: control,
			InputRoot: inputRoot, ConnectorRevisionID: connectorID,
			Now: func() time.Time { return now }, Journal: stageworkeragent.NewMemoryInputTransferJournal(),
		},
	)
	if err != nil {
		t.Fatalf("NewAssignmentInputResolver: %v", err)
	}
	runtimeAgent, err := stageworkeragent.New(stageworkeragent.Config{
		Members: []stageworkeragent.RuntimeMember{{ID: fixture.memberID, Client: fixture.client}},
	})
	if err != nil {
		t.Fatalf("New runtime Agent: %v", err)
	}
	stream, err := stageworkeragent.NewInputResolvingStreamAgent(runtimeAgent, control, resolver)
	if err != nil {
		t.Fatalf("NewInputResolvingStreamAgent: %v", err)
	}
	first, firstErr := stream.ExecuteAssignment(context.Background(), assignment)
	if firstErr == nil || first.PreparedMembers != 0 || first.StartedMembers != 0 {
		t.Fatalf("first ExecuteAssignment = %#v error=%v", first, firstErr)
	}
	result, err := stream.ExecuteAssignment(context.Background(), assignment)
	if err != nil || !result.BarrierPassed || !result.ControlStartAccepted {
		t.Fatalf("ExecuteAssignment = %#v error=%v", result, err)
	}
	if len(control.requests) != 4 || control.requests[0].GetResolveInputTransfer() == nil ||
		control.requests[1].GetConsumeInputTransfer() == nil ||
		control.requests[2].GetConsumeInputTransfer() == nil ||
		control.requests[3].GetStartStage() == nil ||
		!proto.Equal(control.requests[1], control.requests[2]) {
		t.Fatalf("control request order = %#v", control.requests)
	}
	stageRunID := uuid.MustParse(assignment.Authority.GetStageRunId())
	relative, err := stageworkeragent.StageInputRelativePath(stageRunID, artifactID, digest)
	if err != nil {
		t.Fatalf("StageInputRelativePath: %v", err)
	}
	got, err := os.ReadFile(filepath.Join(inputRoot, relative))
	if err != nil || !bytes.Equal(got, payload) {
		t.Fatalf("resolved input = %q error=%v", got, err)
	}
}

type transferControl struct {
	t               *testing.T
	authority       *velav1.StageAuthority
	descriptor      *velav1.ResolvedInputTransfer
	requests        []*velav1.StageWorkerControlServiceConnectRequest
	consumeFailures int
}

func (control *transferControl) Commands() <-chan *velav1.StageWorkerControlServiceConnectResponse {
	return nil
}

func (control *transferControl) Exchange(
	_ context.Context,
	request *velav1.StageWorkerControlServiceConnectRequest,
) (*velav1.StageWorkerControlServiceConnectResponse, error) {
	control.requests = append(
		control.requests,
		proto.Clone(request).(*velav1.StageWorkerControlServiceConnectRequest),
	)
	switch request.GetOperation().(type) {
	case *velav1.StageWorkerControlServiceConnectRequest_ResolveInputTransfer:
		return &velav1.StageWorkerControlServiceConnectResponse{
			Result: &velav1.StageWorkerControlServiceConnectResponse_ResolvedInputTransfer{
				ResolvedInputTransfer: proto.Clone(control.descriptor).(*velav1.ResolvedInputTransfer),
			},
		}, nil
	case *velav1.StageWorkerControlServiceConnectRequest_ConsumeInputTransfer:
		if control.consumeFailures > 0 {
			control.consumeFailures--
			return nil, errors.New("injected consume response loss")
		}
		digest, err := stageauthority.Digest(control.authority)
		if err != nil {
			return nil, err
		}
		return &velav1.StageWorkerControlServiceConnectResponse{
			Result: &velav1.StageWorkerControlServiceConnectResponse_StageCommandResult{
				StageCommandResult: &velav1.StageCommandResult{
					AuthorityDigest: digest[:],
					Decision:        velav1.StageWorkerCommandDecision_STAGE_WORKER_COMMAND_DECISION_ACCEPTED,
					Operation:       velav1.StageWorkerOperation_STAGE_WORKER_OPERATION_CONSUME_INPUT_TRANSFER,
				},
			},
		}, nil
	case *velav1.StageWorkerControlServiceConnectRequest_StartStage:
		digest, err := stageauthority.Digest(control.authority)
		if err != nil {
			return nil, err
		}
		return &velav1.StageWorkerControlServiceConnectResponse{
			Result: &velav1.StageWorkerControlServiceConnectResponse_StageCommandResult{
				StageCommandResult: &velav1.StageCommandResult{
					AuthorityDigest: digest[:],
					Decision:        velav1.StageWorkerCommandDecision_STAGE_WORKER_COMMAND_DECISION_ACCEPTED,
					Operation:       velav1.StageWorkerOperation_STAGE_WORKER_OPERATION_START_STAGE,
				},
			},
		}, nil
	default:
		control.t.Fatalf("unexpected transfer operation: %#v", request.GetOperation())
		return nil, nil
	}
}
