package stageworkeragent

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"path/filepath"
	"time"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/artifactstore"
	"github.com/vivym/vela/internal/securefile"
	"github.com/vivym/vela/internal/stageartifact"
	"github.com/vivym/vela/internal/stageassignment"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
)

type InputResolver interface {
	Resolve(context.Context, *velav1.StageAssignment) error
}

type AssignmentInputResolverConfig struct {
	Store               artifactstore.VersionedStore
	TicketSigner        *stageartifact.TransferTicketSigner
	Control             ControlClient
	InputRoot           string
	ConnectorRevisionID uuid.UUID
	Now                 func() time.Time
	Journal             InputTransferJournal
}

type AssignmentInputResolver struct {
	store               artifactstore.VersionedStore
	ticketSigner        *stageartifact.TransferTicketSigner
	control             ControlClient
	inputRoot           string
	connectorRevisionID uuid.UUID
	now                 func() time.Time
	journal             InputTransferJournal
}

func NewAssignmentInputResolver(
	config AssignmentInputResolverConfig,
) (*AssignmentInputResolver, error) {
	cleanedRoot := filepath.Clean(config.InputRoot)
	if config.Store == nil || config.TicketSigner == nil || config.Control == nil ||
		config.ConnectorRevisionID == uuid.Nil || config.Now == nil || config.Journal == nil ||
		!filepath.IsAbs(cleanedRoot) || cleanedRoot != config.InputRoot {
		return nil, errors.New("StageAssignment input resolver configuration is incomplete")
	}
	if err := securefile.ValidateDirectory(cleanedRoot); err != nil {
		return nil, fmt.Errorf("validate StageAssignment input root: %w", err)
	}
	return &AssignmentInputResolver{
		store: config.Store, ticketSigner: config.TicketSigner, control: config.Control,
		inputRoot: cleanedRoot, connectorRevisionID: config.ConnectorRevisionID,
		now: config.Now, journal: config.Journal,
	}, nil
}

func (resolver *AssignmentInputResolver) Resolve(
	ctx context.Context,
	assignment *velav1.StageAssignment,
) error {
	if resolver == nil || resolver.store == nil || resolver.ticketSigner == nil ||
		resolver.control == nil || resolver.now == nil || resolver.journal == nil || ctx == nil {
		return errors.New("StageAssignment input resolver is not configured")
	}
	if _, err := stageassignment.Validate(assignment); err != nil {
		return fmt.Errorf("validate StageAssignment before input resolution: %w", err)
	}
	inputs := assignment.GetExecutionSpec().GetInputs()
	if len(inputs) == 0 {
		return nil
	}
	stageRunID, err := uuid.Parse(assignment.GetAuthority().GetStageRunId())
	if err != nil || stageRunID == uuid.Nil {
		return errors.New("StageAssignment input resolution lacks StageRun identity")
	}
	destination, err := resolver.destination(assignment.GetAuthority())
	if err != nil {
		return err
	}
	controlAuthority, err := NewControlTransferAuthority(resolver.control, assignment.GetAuthority())
	if err != nil {
		return err
	}
	journaledAuthority := &journaledInputTransferAuthority{
		base: controlAuthority, journal: resolver.journal,
	}
	connector, err := stageartifact.NewObjectStorePullConnector(
		resolver.store, journaledAuthority, resolver.ticketSigner, resolver.now,
	)
	if err != nil {
		return fmt.Errorf("construct Stage input Connector: %w", err)
	}
	tickets := make(map[string]*velav1.StageInputTransferTicket, len(inputs))
	for _, ticket := range assignment.GetInputTransferTickets() {
		tickets[inputVersionKey(ticket.GetStageArtifactId(), ticket.GetObjectVersion())] = ticket
	}
	for _, input := range inputs {
		ticket := tickets[inputVersionKey(input.GetStageArtifactId(), input.GetObjectVersion())]
		if ticket == nil {
			return errors.New("StageAssignment exact input has no TransferTicket")
		}
		token := stageartifact.SignedTransferTicket{
			Token: append([]byte(nil), ticket.GetTransferTicket()...),
		}
		tokenDigest := sha256.Sum256(token.Token)
		journalRecord, found, err := resolver.journal.Load(ctx, tokenDigest)
		if err != nil {
			return fmt.Errorf("read Stage input consume journal: %w", err)
		}
		if found {
			if err := resolver.resumeConsumedInput(
				ctx, stageRunID, input, token, destination, controlAuthority, journalRecord,
			); err != nil {
				return fmt.Errorf("resume exact Stage input %s: %w", input.GetStageArtifactId(), err)
			}
			continue
		}
		target, err := NewFilesystemInputTransferTarget(resolver.inputRoot, stageRunID, input)
		if err != nil {
			return fmt.Errorf("construct Stage input target: %w", err)
		}
		receipt, pullErr := connector.Pull(
			ctx,
			token,
			destination,
			target,
		)
		closeErr := target.Close()
		if pullErr != nil || closeErr != nil {
			return fmt.Errorf(
				"pull exact Stage input %s: %w",
				input.GetStageArtifactId(), errors.Join(pullErr, closeErr),
			)
		}
		artifactID, _ := uuid.Parse(input.GetStageArtifactId())
		if receipt.ArtifactID != artifactID || receipt.SizeBytes != input.GetSizeBytes() ||
			len(input.GetSha256()) != len(receipt.SHA256) ||
			!equalDigest(input.GetSha256(), receipt.SHA256) {
			return errors.New("Stage input Connector returned a mismatched PullReceipt")
		}
	}
	return nil
}

func (resolver *AssignmentInputResolver) destination(
	authority *velav1.StageAuthority,
) (stageartifact.TransferDestination, error) {
	workerID, workerErr := uuid.Parse(authority.GetWorkerInstanceId())
	residencyID, residencyErr := uuid.Parse(authority.GetModelResidencyId())
	if workerErr != nil || residencyErr != nil || authority.GetWorkerInstanceEpoch() <= 0 ||
		authority.GetModelRuntimeBarrierGeneration() <= 0 {
		return stageartifact.TransferDestination{}, errors.New(
			"StageAssignment TransferTicket destination is invalid",
		)
	}
	return stageartifact.TransferDestination{
		WorkerInstanceID: workerID, WorkerInstanceEpoch: authority.GetWorkerInstanceEpoch(),
		ModelResidencyID:    residencyID,
		ModelRuntimeEpoch:   authority.GetModelRuntimeBarrierGeneration(),
		ConnectorRevisionID: resolver.connectorRevisionID,
	}, nil
}

func (resolver *AssignmentInputResolver) resumeConsumedInput(
	ctx context.Context,
	stageRunID uuid.UUID,
	input *velav1.StageInputArtifact,
	ticket stageartifact.SignedTransferTicket,
	destination stageartifact.TransferDestination,
	authority *ControlTransferAuthority,
	record InputTransferJournalRecord,
) error {
	var inputDigest [sha256.Size]byte
	copy(inputDigest[:], input.GetSha256())
	if record.TokenDigest != sha256.Sum256(ticket.Token) ||
		record.Command.OutcomeDigest != inputDigest || record.Command.Destination != destination {
		return errors.New("input transfer journal does not match exact Artifact")
	}
	claims, err := resolver.ticketSigner.Verify(ticket, record.Command.ConsumedAt)
	if err != nil || claims.TicketID != record.Command.TicketID || claims.Destination != destination {
		return errors.New("input transfer journal does not match signed TransferTicket")
	}
	target, err := NewFilesystemInputTransferTarget(resolver.inputRoot, stageRunID, input)
	if err != nil {
		return err
	}
	verifyErr := target.VerifyCommitted(ctx)
	closeErr := target.Close()
	if verifyErr != nil || closeErr != nil {
		return fmt.Errorf("verify committed Stage input: %w", errors.Join(verifyErr, closeErr))
	}
	if record.Consumed {
		return nil
	}
	if err := authority.Consume(ctx, record.Command); err != nil {
		return err
	}
	if err := resolver.journal.MarkConsumed(ctx, record); err != nil {
		return fmt.Errorf("commit Stage input consume journal: %w", err)
	}
	return nil
}

type journaledInputTransferAuthority struct {
	base    *ControlTransferAuthority
	journal InputTransferJournal
}

func (authority *journaledInputTransferAuthority) Resolve(
	ctx context.Context,
	command stageartifact.ResolveTransferCommand,
) (stageartifact.TransferDescriptor, error) {
	return authority.base.Resolve(ctx, command)
}

func (authority *journaledInputTransferAuthority) Consume(
	ctx context.Context,
	command stageartifact.ConsumeTransferCommand,
) error {
	record := InputTransferJournalRecord{
		TokenDigest: command.TokenDigest, Command: command,
	}
	if err := authority.journal.PutPending(ctx, record); err != nil {
		return fmt.Errorf("persist Stage input consume intent: %w", err)
	}
	if err := authority.base.Consume(ctx, command); err != nil {
		return err
	}
	if err := authority.journal.MarkConsumed(ctx, record); err != nil {
		return fmt.Errorf("commit Stage input consume intent: %w", err)
	}
	return nil
}

func inputVersionKey(artifactID, objectVersion string) string {
	return artifactID + "\x00" + objectVersion
}

var _ InputResolver = (*AssignmentInputResolver)(nil)
var _ stageartifact.TransferAuthority = (*journaledInputTransferAuthority)(nil)
