package stageworkeragent

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"strings"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/stageartifact"
	"github.com/vivym/vela/internal/stageauthority"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type ControlTransferAuthority struct {
	control         ControlClient
	authority       *velav1.StageAuthority
	authorityDigest [sha256.Size]byte
}

func NewControlTransferAuthority(
	control ControlClient,
	authority *velav1.StageAuthority,
) (*ControlTransferAuthority, error) {
	if control == nil || authority == nil {
		return nil, errors.New("Stage input transfer control authority is incomplete")
	}
	digest, err := stageauthority.Digest(authority)
	if err != nil {
		return nil, fmt.Errorf("bind Stage input transfer authority: %w", err)
	}
	return &ControlTransferAuthority{
		control: control, authority: proto.Clone(authority).(*velav1.StageAuthority),
		authorityDigest: digest,
	}, nil
}

func (authority *ControlTransferAuthority) Resolve(
	ctx context.Context,
	command stageartifact.ResolveTransferCommand,
) (stageartifact.TransferDescriptor, error) {
	if authority == nil || authority.control == nil || authority.authority == nil || ctx == nil ||
		command.TicketID == uuid.Nil || command.TokenDigest == ([sha256.Size]byte{}) ||
		command.ResolvedAt.IsZero() || !authority.matchesDestination(command.Destination) {
		return stageartifact.TransferDescriptor{}, errors.New("Stage input transfer resolve authority is invalid")
	}
	response, err := authority.control.Exchange(ctx, &velav1.StageWorkerControlServiceConnectRequest{
		Operation: &velav1.StageWorkerControlServiceConnectRequest_ResolveInputTransfer{
			ResolveInputTransfer: &velav1.ResolveInputTransferRequest{
				Authority: proto.Clone(authority.authority).(*velav1.StageAuthority),
				TicketId:  command.TicketID.String(), TokenDigest: append([]byte(nil), command.TokenDigest[:]...),
				WorkerInstanceId:    command.Destination.WorkerInstanceID.String(),
				WorkerInstanceEpoch: command.Destination.WorkerInstanceEpoch,
				ModelResidencyId:    command.Destination.ModelResidencyID.String(),
				ModelRuntimeEpoch:   command.Destination.ModelRuntimeEpoch,
				ConnectorRevisionId: command.Destination.ConnectorRevisionID.String(),
				ResolvedAt:          timestamppb.New(command.ResolvedAt.UTC()),
			},
		},
	})
	if err != nil {
		return stageartifact.TransferDescriptor{}, err
	}
	resolved := response.GetResolvedInputTransfer()
	if resolved == nil {
		return stageartifact.TransferDescriptor{}, transferControlRejection(response, "resolve")
	}
	ticketID, ticketErr := uuid.Parse(resolved.GetTicketId())
	artifactID, artifactErr := uuid.Parse(resolved.GetStageArtifactId())
	if ticketErr != nil || artifactErr != nil || ticketID != command.TicketID ||
		strings.TrimSpace(resolved.GetObjectKey()) == "" ||
		resolved.GetObjectKey() != strings.TrimSpace(resolved.GetObjectKey()) ||
		strings.TrimSpace(resolved.GetObjectVersion()) == "" ||
		resolved.GetObjectVersion() != strings.TrimSpace(resolved.GetObjectVersion()) ||
		len(resolved.GetSha256()) != sha256.Size || resolved.GetSizeBytes() <= 0 ||
		strings.TrimSpace(resolved.GetContentType()) == "" || len(resolved.GetContentType()) > 200 {
		return stageartifact.TransferDescriptor{}, errors.New(
			"Stage input transfer control returned a malformed descriptor",
		)
	}
	var digest [sha256.Size]byte
	copy(digest[:], resolved.GetSha256())
	if digest == ([sha256.Size]byte{}) {
		return stageartifact.TransferDescriptor{}, errors.New(
			"Stage input transfer control returned an empty digest",
		)
	}
	return stageartifact.TransferDescriptor{
		TicketID: ticketID, ArtifactID: artifactID,
		ObjectKey: resolved.GetObjectKey(), ObjectVersion: resolved.GetObjectVersion(),
		SHA256: digest, SizeBytes: resolved.GetSizeBytes(), ContentType: resolved.GetContentType(),
	}, nil
}

func (authority *ControlTransferAuthority) Consume(
	ctx context.Context,
	command stageartifact.ConsumeTransferCommand,
) error {
	if authority == nil || authority.control == nil || authority.authority == nil || ctx == nil ||
		command.CommandID == uuid.Nil || command.TicketID == uuid.Nil ||
		command.TokenDigest == ([sha256.Size]byte{}) ||
		command.OutcomeDigest == ([sha256.Size]byte{}) || command.ConsumedAt.IsZero() ||
		!authority.matchesDestination(command.Destination) {
		return errors.New("Stage input transfer consume authority is invalid")
	}
	response, err := authority.control.Exchange(ctx, &velav1.StageWorkerControlServiceConnectRequest{
		RequestId: command.CommandID.String(),
		Operation: &velav1.StageWorkerControlServiceConnectRequest_ConsumeInputTransfer{
			ConsumeInputTransfer: &velav1.ConsumeInputTransferRequest{
				Authority:           proto.Clone(authority.authority).(*velav1.StageAuthority),
				TicketId:            command.TicketID.String(),
				TokenDigest:         append([]byte(nil), command.TokenDigest[:]...),
				OutcomeDigest:       append([]byte(nil), command.OutcomeDigest[:]...),
				ConsumedAt:          timestamppb.New(command.ConsumedAt.UTC()),
				WorkerInstanceId:    command.Destination.WorkerInstanceID.String(),
				WorkerInstanceEpoch: command.Destination.WorkerInstanceEpoch,
				ModelResidencyId:    command.Destination.ModelResidencyID.String(),
				ModelRuntimeEpoch:   command.Destination.ModelRuntimeEpoch,
				ConnectorRevisionId: command.Destination.ConnectorRevisionID.String(),
			},
		},
	})
	if err != nil {
		return err
	}
	result := response.GetStageCommandResult()
	if result == nil || result.GetOperation() !=
		velav1.StageWorkerOperation_STAGE_WORKER_OPERATION_CONSUME_INPUT_TRANSFER ||
		(result.GetDecision() != velav1.StageWorkerCommandDecision_STAGE_WORKER_COMMAND_DECISION_ACCEPTED &&
			result.GetDecision() != velav1.StageWorkerCommandDecision_STAGE_WORKER_COMMAND_DECISION_REPLAYED) ||
		!bytes.Equal(result.GetAuthorityDigest(), authority.authorityDigest[:]) {
		return transferControlRejection(response, "consume")
	}
	return nil
}

func (authority *ControlTransferAuthority) matchesDestination(
	destination stageartifact.TransferDestination,
) bool {
	if authority == nil || authority.authority == nil ||
		destination.WorkerInstanceID.String() != authority.authority.GetWorkerInstanceId() ||
		destination.WorkerInstanceEpoch != authority.authority.GetWorkerInstanceEpoch() ||
		destination.ModelResidencyID.String() != authority.authority.GetModelResidencyId() ||
		destination.ConnectorRevisionID == uuid.Nil ||
		destination.ModelRuntimeEpoch != authority.authority.GetModelRuntimeBarrierGeneration() {
		return false
	}
	return destination.ModelRuntimeEpoch > 0
}

func transferControlRejection(
	response *velav1.StageWorkerControlServiceConnectResponse,
	operation string,
) error {
	detail := "malformed control response"
	if response != nil && strings.TrimSpace(response.GetStageCommandResult().GetDetail()) != "" {
		detail = response.GetStageCommandResult().GetDetail()
	}
	return fmt.Errorf("control rejected Stage input transfer %s: %s", operation, detail)
}

var _ stageartifact.TransferAuthority = (*ControlTransferAuthority)(nil)
