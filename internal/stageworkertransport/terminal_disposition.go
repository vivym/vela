package stageworkertransport

import (
	"bytes"
	"context"
	"errors"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/stageauthority"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/protobuf/proto"
)

// ReadTerminalDisposition returns nil for RETAIN. A non-nil result authenticates
// terminal input non-use only; callers still need persistent exclusion and drain.
func (client *Client) ReadTerminalDisposition(
	ctx context.Context, validator *stageauthority.Validator, authority *velav1.StageAuthority,
	workerMemberID string, acquireID uuid.UUID,
) (*stageauthority.VerifiedTerminalDisposition, error) {
	if client == nil || ctx == nil || validator == nil || authority == nil {
		return nil, errors.New("terminal Stage query is not configured")
	}
	if _, err := validator.ValidateEnvelopeForReplay(authority, 0); err != nil {
		return nil, err
	}
	stream, epoch, generation, err := client.ensureStream()
	if err != nil {
		return nil, err
	}
	query := &velav1.ReadStageTerminalDispositionRequest{
		SchemaVersion: 1, Authority: proto.Clone(authority).(*velav1.StageAuthority),
	}
	if acquireID != uuid.Nil {
		query.AcquireCommandId = acquireID.String()
	}
	request := &velav1.StageWorkerControlServiceConnectRequest{
		RequestId: uuid.NewString(), ControlSessionEpoch: epoch,
		Operation: &velav1.StageWorkerControlServiceConnectRequest_ReadStageTerminalDisposition{ReadStageTerminalDisposition: query},
	}
	response, err := client.Exchange(ctx, request)
	if err != nil {
		return nil, err
	}
	verified, err := ValidateTerminalDispositionResponse(validator, request, response, workerMemberID)
	if err != nil {
		return nil, err
	}
	client.mu.Lock()
	current := !client.closed && client.stream == stream && client.generation == generation && stream.Context().Err() == nil
	client.mu.Unlock()
	if !current {
		return nil, errors.New("control session changed during terminal Stage query")
	}
	return verified, nil
}

func ValidateTerminalDispositionResponse(
	validator *stageauthority.Validator, request *velav1.StageWorkerControlServiceConnectRequest,
	response *velav1.StageWorkerControlServiceConnectResponse, workerMemberID string,
) (*stageauthority.VerifiedTerminalDisposition, error) {
	query, result := request.GetReadStageTerminalDisposition(), response.GetStageTerminalDispositionResult()
	if validator == nil || query == nil || query.GetSchemaVersion() != 1 || request.GetRequestId() == "" ||
		request.GetRequestId() != response.GetRequestId() || request.GetControlSessionEpoch() <= 0 ||
		result == nil || result.GetSchemaVersion() != 1 || len(result.ProtoReflect().GetUnknown()) != 0 {
		return nil, errors.New("terminal Stage response does not match query")
	}
	original, err := validator.ValidateEnvelopeForReplay(query.GetAuthority(), 0)
	if err != nil {
		return nil, err
	}
	if !bytes.Equal(result.GetOriginalAuthorityDigest(), original.Digest[:]) {
		return nil, errors.New("terminal Stage response changed original authority")
	}
	if result.GetInputDisposition() == velav1.StageInputDisposition_STAGE_INPUT_DISPOSITION_RETAIN && result.GetDisposition() == nil &&
		(result.GetReason() == "HISTORY_UNAVAILABLE" || result.GetReason() == "UNSUPPORTED_AUTHORITY") {
		return nil, nil
	}
	if result.GetInputDisposition() != velav1.StageInputDisposition_STAGE_INPUT_DISPOSITION_INPUTS_UNUSED || result.GetReason() != "HISTORY_COMPLETE" {
		return nil, errors.New("terminal Stage response disposition is inconsistent")
	}
	verified, err := validator.ValidateTerminalDisposition(result.GetDisposition(), original.Authority, workerMemberID, request.GetControlSessionEpoch())
	if err != nil {
		return nil, err
	}
	return &verified, nil
}
