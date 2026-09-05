package stageworkercontrol

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/stageauthority"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type TerminalDispositionOperations interface {
	ReadStageTerminalDisposition(context.Context, CommandContext, *velav1.ReadStageTerminalDispositionRequest, VerifiedAuthorities) (*velav1.StageTerminalDispositionResult, error)
}

type TerminalHistoryReader interface {
	Read(context.Context, CommandContext, *velav1.StageAuthority, uuid.UUID) (*TerminalStageHistory, error)
}

type TerminalDispositionBackend struct {
	reader    TerminalHistoryReader
	signer    *stageauthority.Signer
	validator *stageauthority.Validator
	keyID     string
}

func NewTerminalDispositionBackend(reader TerminalHistoryReader, signer *stageauthority.Signer, validator *stageauthority.Validator, keyID string) (*TerminalDispositionBackend, error) {
	if reader == nil || signer == nil || validator == nil || keyID == "" {
		return nil, errors.New("terminal Stage disposition dependencies are incomplete")
	}
	return &TerminalDispositionBackend{reader: reader, signer: signer, validator: validator, keyID: keyID}, nil
}

func (backend *PostgresOperationBackend) ReadStageTerminalDisposition(ctx context.Context, command CommandContext, request *velav1.ReadStageTerminalDispositionRequest, authorities VerifiedAuthorities) (*velav1.StageTerminalDispositionResult, error) {
	if backend == nil || backend.terminalDispositions == nil {
		return nil, errors.New("terminal Stage disposition backend is not configured")
	}
	return backend.terminalDispositions.ReadStageTerminalDisposition(ctx, command, request, authorities)
}

func (backend *TerminalDispositionBackend) ReadStageTerminalDisposition(ctx context.Context, command CommandContext, request *velav1.ReadStageTerminalDispositionRequest, authorities VerifiedAuthorities) (*velav1.StageTerminalDispositionResult, error) {
	if backend == nil || ctx == nil || request == nil || request.GetSchemaVersion() != 1 ||
		(request.GetAcquireCommandId() != "" && parseUUID(request.GetAcquireCommandId()) == uuid.Nil) {
		return nil, errors.New("terminal Stage disposition request is invalid")
	}
	if err := exactStageCommand(command, request.GetAuthority(), authorities); err != nil {
		return nil, err
	}
	verified, err := backend.validator.ValidateEnvelopeForReplay(request.GetAuthority(), 0)
	if err != nil {
		return nil, err
	}
	result := &velav1.StageTerminalDispositionResult{
		SchemaVersion: 1, InputDisposition: velav1.StageInputDisposition_STAGE_INPUT_DISPOSITION_RETAIN,
		OriginalAuthorityDigest: append([]byte(nil), verified.Digest[:]...), Reason: "HISTORY_UNAVAILABLE",
	}
	if verified.Authority.GetSchemaVersion() != stageauthority.SchemaVersionV2 {
		result.Reason = "UNSUPPORTED_AUTHORITY"
		return result, nil
	}
	history, err := backend.reader.Read(ctx, command, verified.Authority, parseUUID(request.GetAcquireCommandId()))
	if err != nil || history == nil {
		return result, err
	}
	spiffeDigest := sha256.Sum256([]byte(command.Identity.SPIFFEID))
	if history.OriginalAuthorityDigest != verified.Digest || !history.matches(command, verified.Authority, spiffeDigest) {
		return nil, errors.New("terminal Stage history does not match authenticated request")
	}
	disposition, err := backend.signer.SignTerminalDisposition(history.disposition(verified.Authority, backend.keyID))
	if err != nil {
		return nil, err
	}
	if _, err := backend.validator.ValidateTerminalDisposition(disposition, verified.Authority, history.WorkerMemberID.String(), command.ControlSessionEpoch); err != nil {
		return nil, err
	}
	result.InputDisposition = velav1.StageInputDisposition_STAGE_INPUT_DISPOSITION_INPUTS_UNUSED
	result.Reason = "HISTORY_COMPLETE"
	result.Disposition = disposition
	return result, nil
}

func (history *TerminalStageHistory) disposition(authority *velav1.StageAuthority, keyID string) *velav1.StageTerminalDisposition {
	d := &velav1.StageTerminalDisposition{
		SchemaVersion:           stageauthority.TerminalDispositionSchemaVersion,
		InputDisposition:        velav1.StageInputDisposition_STAGE_INPUT_DISPOSITION_INPUTS_UNUSED,
		OriginalAuthorityDigest: history.OriginalAuthorityDigest[:],
		OrganizationId:          history.OrganizationID.String(), ProjectId: history.ProjectID.String(),
		JobId: history.JobID.String(), AttemptId: history.AttemptID.String(), StageRunId: history.StageRunID.String(),
		StageAttemptId: authority.GetStageAttemptId(), StageAllocationId: authority.GetStageAllocationId(), StageLeaseId: authority.GetStageLeaseId(),
		TerminalState: velav1.StageTerminalState(velav1.StageTerminalState_value["STAGE_TERMINAL_STATE_"+history.TerminalState]),
		StageFence:    history.StageFence, StageVersion: history.StageVersion,
		WorkerInstanceId: history.WorkerInstanceID.String(), WorkerInstanceEpoch: history.WorkerInstanceEpoch,
		WorkerMemberId: history.WorkerMemberID.String(), ControlSessionEpoch: history.ControlSessionEpoch,
		DeviceSetDigest: history.DeviceSetDigest[:], MembershipDigest: history.MembershipDigest[:], Cutoff: history.Cutoff,
		ObservedAt: timestamppb.New(history.ObservedAt), ExpiresAt: timestamppb.New(history.ObservedAt.Add(stageauthority.MaxTerminalDispositionValidity)),
		SigningKeyId: keyID,
	}
	for _, device := range history.Devices {
		d.Devices = append(d.Devices, &velav1.StageAuthorityDeviceEpoch{DeviceId: device.ID.String(), DeviceEpoch: device.Epoch})
	}
	for _, allocation := range history.Allocations {
		wire := &velav1.StageTerminalAllocation{
			StageAttemptId: allocation.StageAttemptID.String(), StageAllocationId: allocation.StageAllocationID.String(), StageLeaseId: allocation.StageLeaseID.String(),
			ExecutionSequence: allocation.ExecutionSequence, ExecutionNonce: allocation.ExecutionNonce[:],
			ModelResidencyId: allocation.ModelResidencyID.String(), ModelRuntimeIdentity: allocation.ModelRuntimeIdentity,
			BarrierGeneration: allocation.BarrierGeneration, StageProfileRevisionId: allocation.StageProfileRevisionID.String(),
		}
		for _, member := range allocation.Members {
			wire.Members = append(wire.Members, &velav1.StageTerminalMember{
				WorkerMemberId: member.WorkerMemberID.String(), MemberEpoch: member.MemberEpoch, ModelRuntimeEpoch: member.ModelRuntimeEpoch,
				IdentityDigest: member.IdentityDigest[:], DeviceSubsetDigest: member.DeviceSubsetDigest[:],
			})
		}
		d.Allocations = append(d.Allocations, wire)
	}
	return d
}

func validateTerminalDispositionResult(result *velav1.StageTerminalDispositionResult, authorities VerifiedAuthorities) error {
	if result == nil || result.GetSchemaVersion() != 1 || authorities.Stage == nil ||
		!bytes.Equal(result.GetOriginalAuthorityDigest(), authorities.Stage.Digest[:]) {
		return errors.New("terminal Stage backend returned a mismatched result")
	}
	switch result.GetInputDisposition() {
	case velav1.StageInputDisposition_STAGE_INPUT_DISPOSITION_RETAIN:
		if result.GetDisposition() == nil && (result.GetReason() == "HISTORY_UNAVAILABLE" || result.GetReason() == "UNSUPPORTED_AUTHORITY") {
			return nil
		}
	case velav1.StageInputDisposition_STAGE_INPUT_DISPOSITION_INPUTS_UNUSED:
		if result.GetReason() == "HISTORY_COMPLETE" && result.GetDisposition().GetInputDisposition() == result.GetInputDisposition() &&
			bytes.Equal(result.GetDisposition().GetOriginalAuthorityDigest(), result.GetOriginalAuthorityDigest()) {
			return nil
		}
	}
	return errors.New("terminal Stage backend returned an inconsistent result")
}
