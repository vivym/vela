package nodeagent

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/remediation"
)

// OperationReader is the authoritative remediation operation lookup seam.
// *remediation.Service satisfies this interface.
type OperationReader interface {
	Get(context.Context, uuid.UUID) (remediation.Operation, error)
}

// CompletionWriter is the authoritative remediation completion seam.
// *remediation.Service satisfies this interface.
type CompletionWriter interface {
	Complete(context.Context, remediation.Completion) (remediation.Result, error)
}

type ExecutionClaimer interface {
	ClaimExecution(context.Context, uuid.UUID, uuid.UUID, int64, uuid.UUID, string) (remediation.ClaimResult, error)
}

// RemoteExecutor is the control-plane side of the controller-to-agent
// contract. Authorization and the persistent completion write happen here;
// the host Agent only receives an already identity-bound execution request.
type RemoteExecutor struct {
	client        *Client
	authorizer    Authorizer
	ledger        Ledger
	actorIdentity string
}

func NewRemoteExecutor(
	client *Client,
	authorizer Authorizer,
	ledger Ledger,
	actorIdentity string,
) (*RemoteExecutor, error) {
	if client == nil {
		return nil, errors.New("node Agent remote client is required")
	}
	if authorizer == nil {
		return nil, errors.New("node Agent remote authorizer is required")
	}
	if ledger == nil {
		return nil, errors.New("node Agent remote ledger is required")
	}
	if !validText(actorIdentity, maxIdentityText) {
		return nil, errors.New("node Agent remote actor identity is invalid")
	}
	if client.actorIdentity != actorIdentity {
		return nil, errors.New("node Agent client and remote actor identities differ")
	}
	return &RemoteExecutor{
		client: client, authorizer: authorizer, ledger: ledger, actorIdentity: actorIdentity,
	}, nil
}

func (executor *RemoteExecutor) Execute(ctx context.Context, plan remediation.Plan) (remediation.ExecutionResult, error) {
	if executor == nil || executor.client == nil || executor.authorizer == nil || executor.ledger == nil {
		return remediation.ExecutionResult{}, errors.New("node Agent remote executor is not configured")
	}
	if ctx == nil {
		return remediation.ExecutionResult{}, errors.New("node Agent remote execution context is required")
	}
	if plan.DeadlineAt.IsZero() {
		return remediation.ExecutionResult{}, errors.New("node Agent remote execution deadline is required")
	}
	request := Request{
		OperationID: plan.OperationID, ExecutionClaimID: plan.ExecutionClaimID,
		WorkerID: plan.WorkerID, WorkerEpoch: plan.WorkerEpoch,
		NodeIdentity: plan.NodeIdentity, DeviceIdentity: plan.DeviceIdentity,
		ActionLevel: plan.ActionLevel, CertificationRevision: plan.CertificationRevision,
		FailureEvidenceDigest: append([]byte(nil), plan.FailureEvidenceDigest...),
		DeadlineAt:            plan.DeadlineAt,
	}
	if err := validateClientRequest(request); err != nil {
		return remediation.ExecutionResult{}, err
	}
	if err := executor.authorizer.Authorize(ctx, request); err != nil {
		return remediation.ExecutionResult{}, err
	}
	result, err := executor.client.Execute(ctx, request)
	if err != nil {
		return remediation.ExecutionResult{}, err
	}
	if err := saveReceipt(executor.ledger, ctx, Receipt{
		RequestHash: hashRequest(request), ActorIdentity: executor.actorIdentity, Result: result,
	}); err != nil {
		return remediation.ExecutionResult{}, fmt.Errorf("persist control-plane remediation completion: %w", err)
	}
	var postcheckDigest [sha256.Size]byte
	if len(result.PostcheckHash) == sha256.Size {
		copy(postcheckDigest[:], result.PostcheckHash)
	}
	return remediation.ExecutionResult{
		PostcheckDigest: postcheckDigest, PostcheckVerified: result.Success,
		Detail: result.ResultDetail, ResultCode: result.ResultCode,
	}, nil
}

// ControlPlaneAuthorizer rejects host execution unless the request matches the
// currently EXECUTING operation and its immutable Worker/deadline contract.
type ControlPlaneAuthorizer struct {
	reader        OperationReader
	claimer       ExecutionClaimer
	actorIdentity string
	clock         func() time.Time
}

func NewControlPlaneAuthorizer(
	reader OperationReader,
	claimer ExecutionClaimer,
	actorIdentity string,
) (*ControlPlaneAuthorizer, error) {
	if reader == nil {
		return nil, errors.New("node Agent operation reader is required")
	}
	if claimer == nil {
		return nil, errors.New("node Agent execution claimer is required")
	}
	if !validText(actorIdentity, maxIdentityText) {
		return nil, errors.New("node Agent authorization actor identity is invalid")
	}
	return &ControlPlaneAuthorizer{
		reader: reader, claimer: claimer, actorIdentity: actorIdentity, clock: time.Now,
	}, nil
}

func (authorizer *ControlPlaneAuthorizer) Authorize(ctx context.Context, request Request) error {
	if authorizer == nil || authorizer.reader == nil || authorizer.claimer == nil {
		return errors.New("node Agent control-plane authorizer is not configured")
	}
	operation, err := authorizer.reader.Get(ctx, request.OperationID)
	if err != nil {
		return fmt.Errorf("load remediation operation: %w", err)
	}
	if operation.ID != request.OperationID || operation.WorkerID != request.WorkerID ||
		operation.WorkerEpoch != request.WorkerEpoch || operation.NodeIdentity != request.NodeIdentity ||
		operation.DeviceIdentity != request.DeviceIdentity || operation.ActionLevel != request.ActionLevel ||
		operation.CertificationRevision != request.CertificationRevision ||
		!equalDigest(operation.EvidenceDigest, request.FailureEvidenceDigest) {
		return errors.New("node Agent request does not match the authoritative remediation operation")
	}
	if operation.State != remediation.StateExecuting {
		return errors.New("remediation operation is not executing")
	}
	if operation.DeadlineAt.IsZero() || !request.DeadlineAt.Equal(operation.DeadlineAt) {
		return errors.New("node Agent deadline does not match the authoritative remediation operation")
	}
	if !authorizer.clock().Before(operation.DeadlineAt) {
		return errors.New("remediation operation deadline has expired")
	}
	if _, err := authorizer.claimer.ClaimExecution(
		ctx, operation.ID, operation.WorkerID, operation.WorkerEpoch,
		request.ExecutionClaimID, authorizer.actorIdentity,
	); err != nil {
		return fmt.Errorf("claim remediation execution: %w", err)
	}
	return nil
}

// ControlPlaneLedger persists transport receipts through remediation's
// authoritative operation row. It deliberately does not create a second
// receipt authority or a second terminal state machine.
type ControlPlaneLedger struct {
	reader        OperationReader
	writer        CompletionWriter
	actorIdentity string
}

func NewControlPlaneLedger(
	reader OperationReader,
	writer CompletionWriter,
	actorIdentity string,
) (*ControlPlaneLedger, error) {
	if reader == nil {
		return nil, errors.New("node Agent operation reader is required")
	}
	if writer == nil {
		return nil, errors.New("node Agent completion writer is required")
	}
	if !validText(actorIdentity, maxIdentityText) {
		return nil, errors.New("node Agent completion actor identity is invalid")
	}
	return &ControlPlaneLedger{reader: reader, writer: writer, actorIdentity: actorIdentity}, nil
}

func (ledger *ControlPlaneLedger) Load(ctx context.Context, operationID uuid.UUID) (Receipt, bool, error) {
	if ledger == nil || ledger.reader == nil {
		return Receipt{}, false, errors.New("node Agent control-plane ledger is not configured")
	}
	operation, err := ledger.reader.Get(ctx, operationID)
	if err != nil {
		var failure *remediation.Failure
		if errors.As(err, &failure) && failure.Code == remediation.FailureNotFound {
			return Receipt{}, false, nil
		}
		return Receipt{}, false, err
	}
	if (operation.State != remediation.StateSucceeded && operation.State != remediation.StateQuarantined) ||
		operation.FinishedAt == nil {
		return Receipt{}, false, nil
	}
	request := requestFromOperation(operation)
	resultCode := operation.ResultCode
	if resultCode == "" {
		resultCode = "REMEDIATION_QUARANTINED"
	}
	resultDetail := operation.ResultDetail
	if resultDetail == "" {
		resultDetail = "remediation operation is quarantined"
	}
	startedAt := operation.RequestedAt
	if operation.StartedAt != nil {
		startedAt = *operation.StartedAt
	}
	return Receipt{
		RequestHash: hashRequest(request),
		Result: Result{
			OperationID:   operation.ID,
			Success:       operation.State == remediation.StateSucceeded,
			ResultCode:    resultCode,
			ResultDetail:  resultDetail,
			PostcheckHash: append([]byte(nil), operation.PostcheckDigest...),
			StartedAt:     startedAt,
			FinishedAt:    *operation.FinishedAt,
		},
	}, true, nil
}

func (ledger *ControlPlaneLedger) Save(ctx context.Context, receipt Receipt) error {
	if ledger == nil || ledger.reader == nil || ledger.writer == nil {
		return errors.New("node Agent control-plane ledger is not configured")
	}
	operation, err := ledger.reader.Get(ctx, receipt.Result.OperationID)
	if err != nil {
		return err
	}
	expectedHash := hashRequest(requestFromOperation(operation))
	if receipt.RequestHash != expectedHash {
		return errors.New("node Agent receipt request hash does not match the authoritative operation")
	}
	if operation.State == remediation.StateSucceeded || operation.State == remediation.StateQuarantined {
		return nil
	}
	if operation.State != remediation.StateExecuting {
		return errors.New("remediation operation is not executing")
	}
	if !validText(receipt.Result.ResultCode, maxDetailText) || !validText(receipt.Result.ResultDetail, maxDetailText) {
		return errors.New("node Agent receipt result is invalid")
	}
	_, err = ledger.writer.Complete(ctx, remediation.Completion{
		OperationID:   operation.ID,
		WorkerID:      operation.WorkerID,
		WorkerEpoch:   operation.WorkerEpoch,
		Success:       receipt.Result.Success,
		ResultCode:    receipt.Result.ResultCode,
		ResultDetail:  receipt.Result.ResultDetail,
		PostcheckHash: append([]byte(nil), receipt.Result.PostcheckHash...),
		ActorIdentity: ledger.actorIdentity,
	})
	return err
}

func requestFromOperation(operation remediation.Operation) Request {
	return Request{
		OperationID:           operation.ID,
		WorkerID:              operation.WorkerID,
		WorkerEpoch:           operation.WorkerEpoch,
		NodeIdentity:          operation.NodeIdentity,
		DeviceIdentity:        operation.DeviceIdentity,
		ActionLevel:           operation.ActionLevel,
		CertificationRevision: operation.CertificationRevision,
		FailureEvidenceDigest: append([]byte(nil), operation.EvidenceDigest...),
		DeadlineAt:            operation.DeadlineAt,
	}
}

func equalDigest(left, right []byte) bool {
	if len(left) != sha256.Size || len(right) != sha256.Size {
		return false
	}
	return string(left) == string(right)
}

var _ Authorizer = (*ControlPlaneAuthorizer)(nil)
var _ Ledger = (*ControlPlaneLedger)(nil)
var _ OperationReader = (*remediation.Service)(nil)
var _ CompletionWriter = (*remediation.Service)(nil)
var _ ExecutionClaimer = (*remediation.Service)(nil)
