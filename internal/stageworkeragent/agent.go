package stageworkeragent

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/stageassignment"
	"github.com/vivym/vela/internal/stageauthority"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/protobuf/proto"
)

const (
	defaultMemberCancellationTimeout = 30 * time.Second
	maxMemberCancellationTimeout     = time.Minute
)

type RuntimeMember struct {
	ID     string
	Client velav1.ModelRuntimeServiceClient
}

type Config struct {
	Members             []RuntimeMember
	CancellationTimeout time.Duration
}

type Agent struct {
	members             map[string]velav1.ModelRuntimeServiceClient
	ids                 []string
	cancellationTimeout time.Duration
}

type StartBarrierResult struct {
	PreparedMembers                 int
	StartedMembers                  int
	CancellationAcknowledgedMembers int
	BarrierPassed                   bool
	StartedAt                       time.Time
}

type CancellationResult struct {
	AcknowledgedMembers int
	AllStopped          bool
}

type AggregateStatus struct {
	ReportingMembers int
	AllStopped       bool
	States           map[string]velav1.ModelRuntimeExecutionState
	LocalReceipts    map[string]LocalReceipt
	Failures         map[string]*velav1.ModelRuntimeFailureEvidence
}

type LocalReceipt struct {
	ID     string
	Digest []byte
}

func New(config Config) (*Agent, error) {
	if len(config.Members) == 0 || len(config.Members) > stageassignment.MaxRequiredMembers {
		return nil, errors.New("invalid Stage Worker Agent member set")
	}
	if config.CancellationTimeout == 0 {
		config.CancellationTimeout = defaultMemberCancellationTimeout
	}
	if config.CancellationTimeout < time.Millisecond ||
		config.CancellationTimeout > maxMemberCancellationTimeout {
		return nil, errors.New("invalid Stage Worker Agent cancellation timeout")
	}
	members := make(map[string]velav1.ModelRuntimeServiceClient, len(config.Members))
	ids := make([]string, 0, len(config.Members))
	for _, member := range config.Members {
		if _, err := uuid.Parse(member.ID); err != nil || member.Client == nil {
			return nil, errors.New("invalid Stage Worker Agent member")
		}
		if _, exists := members[member.ID]; exists {
			return nil, errors.New("duplicated Stage Worker Agent member identity")
		}
		members[member.ID] = member.Client
		ids = append(ids, member.ID)
	}
	slices.Sort(ids)
	return &Agent{
		members: members, ids: ids, cancellationTimeout: config.CancellationTimeout,
	}, nil
}

func (agent *Agent) PrepareAndStart(
	ctx context.Context,
	assignment *velav1.StageAssignment,
) (StartBarrierResult, error) {
	result := StartBarrierResult{}
	memberIDs, timeout, err := agent.validateAssignment(assignment)
	if err != nil {
		return result, err
	}
	digest, err := stageauthority.Digest(assignment.GetAuthority())
	if err != nil {
		return result, fmt.Errorf("digest StageAuthority for member barrier: %w", err)
	}
	barrierContext, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	prepareResults := agent.prepareMembers(barrierContext, memberIDs, assignment)
	for _, memberResult := range prepareResults {
		if memberResult.err == nil && validPrepareResponse(
			memberResult.prepare, digest, assignment.GetAuthority(), memberResult.id,
		) {
			result.PreparedMembers++
			continue
		}
		acknowledged, cancelErr := agent.cancelAfterBarrierFailure(
			ctx,
			memberIDs,
			assignment.GetAuthority(),
		)
		result.CancellationAcknowledgedMembers = acknowledged
		if cancelErr != nil {
			return result, errors.Join(
				fmt.Errorf("prepare member %s: %w", memberResult.id, memberResult.errorValue()),
				cancelErr,
			)
		}
		return result, fmt.Errorf("prepare member %s: %w", memberResult.id, memberResult.errorValue())
	}

	startResults := agent.startMembers(barrierContext, memberIDs, assignment.GetAuthority())
	for _, memberResult := range startResults {
		if memberResult.err == nil && validStartResponse(
			memberResult.start, digest, assignment.GetAuthority(), memberResult.id,
		) {
			result.StartedMembers++
			startedAt := memberResult.start.GetStartedAt().AsTime().UTC()
			if startedAt.After(result.StartedAt) {
				result.StartedAt = startedAt
			}
			continue
		}
		acknowledged, cancelErr := agent.cancelAfterBarrierFailure(
			ctx,
			memberIDs,
			assignment.GetAuthority(),
		)
		result.CancellationAcknowledgedMembers = acknowledged
		if cancelErr != nil {
			return result, errors.Join(
				fmt.Errorf("start member %s: %w", memberResult.id, memberResult.errorValue()),
				cancelErr,
			)
		}
		return result, fmt.Errorf("start member %s: %w", memberResult.id, memberResult.errorValue())
	}
	result.BarrierPassed = true
	return result, nil
}

func (agent *Agent) Cancel(
	ctx context.Context,
	authority *velav1.StageAuthority,
	reason velav1.ModelRuntimeCancelReason,
) (CancellationResult, error) {
	result := CancellationResult{}
	if agent == nil || len(agent.ids) == 0 {
		return result, errors.New("missing Stage Worker Agent configuration")
	}
	if _, err := stageauthority.Digest(authority); err != nil {
		return result, err
	}
	if reason < velav1.ModelRuntimeCancelReason_MODEL_RUNTIME_CANCEL_REASON_CONTROL_PLANE_STOP ||
		reason > velav1.ModelRuntimeCancelReason_MODEL_RUNTIME_CANCEL_REASON_MEMBER_BARRIER_FAILED {
		return result, errors.New("invalid Stage Worker Agent cancellation reason")
	}
	acknowledged, err := agent.cancelMembers(ctx, agent.ids, authority, reason)
	result.AcknowledgedMembers = acknowledged
	// A signal acknowledgement never proves that the backend has actually stopped.
	result.AllStopped = false
	return result, err
}

func (agent *Agent) Status(
	ctx context.Context,
	authority *velav1.StageAuthority,
) (AggregateStatus, error) {
	result := AggregateStatus{
		States:        make(map[string]velav1.ModelRuntimeExecutionState),
		LocalReceipts: make(map[string]LocalReceipt),
		Failures:      make(map[string]*velav1.ModelRuntimeFailureEvidence),
	}
	if agent == nil || len(agent.ids) == 0 {
		return result, errors.New("missing Stage Worker Agent configuration")
	}
	digest, err := stageauthority.Digest(authority)
	if err != nil {
		return result, err
	}
	type statusResult struct {
		id       string
		response *velav1.ModelRuntimeServiceStatusResponse
		err      error
	}
	results := make(chan statusResult, len(agent.ids))
	for _, memberID := range agent.ids {
		client := agent.members[memberID]
		go func() {
			response, callErr := client.Status(
				ctx,
				&velav1.ModelRuntimeServiceStatusRequest{Authority: authority},
			)
			results <- statusResult{id: memberID, response: response, err: callErr}
		}()
	}
	result.AllStopped = true
	var joined error
	for range agent.ids {
		memberResult := <-results
		if memberResult.err != nil {
			joined = errors.Join(joined, fmt.Errorf("status member %s: %w", memberResult.id, memberResult.err))
			result.AllStopped = false
			continue
		}
		response := memberResult.response
		if response == nil ||
			response.GetDecision() != velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_ACCEPTED ||
			!bytes.Equal(response.GetAuthorityDigest(), digest[:]) ||
			!runtimeIdentityMatchesMember(response.GetRuntimeIdentity(), authority, memberResult.id) {
			joined = errors.Join(joined, fmt.Errorf("status member %s returned stale authority", memberResult.id))
			result.AllStopped = false
			continue
		}
		result.ReportingMembers++
		result.States[memberResult.id] = response.GetState()
		failure := response.GetFailureEvidence()
		if (response.GetState() == velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_FAILED) !=
			(failure != nil) || (failure != nil && !validRuntimeFailureEvidence(failure)) {
			joined = errors.Join(joined, fmt.Errorf(
				"status member %s returned malformed failure evidence", memberResult.id,
			))
			result.AllStopped = false
			continue
		}
		if failure != nil {
			result.Failures[memberResult.id] = proto.Clone(failure).(*velav1.ModelRuntimeFailureEvidence)
		}
		if response.GetLocalReceiptId() != "" || len(response.GetLocalReceiptDigest()) != 0 {
			if response.GetLocalReceiptId() == "" || len(response.GetLocalReceiptDigest()) != 32 {
				joined = errors.Join(joined, fmt.Errorf("status member %s returned malformed local receipt", memberResult.id))
				result.AllStopped = false
				continue
			}
			result.LocalReceipts[memberResult.id] = LocalReceipt{
				ID:     response.GetLocalReceiptId(),
				Digest: append([]byte(nil), response.GetLocalReceiptDigest()...),
			}
		}
		if response.GetState() != velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_STOPPED {
			result.AllStopped = false
		}
	}
	return result, joined
}

func validRuntimeFailureEvidence(evidence *velav1.ModelRuntimeFailureEvidence) bool {
	if evidence == nil || strings.TrimSpace(evidence.GetFailureClass()) == "" ||
		evidence.GetFailureClass() != strings.TrimSpace(evidence.GetFailureClass()) ||
		len(evidence.GetFailureClass()) > 100 || !utf8.ValidString(evidence.GetFailureClass()) ||
		len(evidence.GetFailureFingerprint()) != sha256.Size ||
		!utf8.ValidString(evidence.GetDetail()) || utf8.RuneCountInString(evidence.GetDetail()) > 1000 ||
		evidence.GetConsumedResourceUnits() <= 0 || evidence.GetFailedAt() == nil ||
		evidence.GetRetryAt() == nil || evidence.GetFailedAt().CheckValid() != nil ||
		evidence.GetRetryAt().CheckValid() != nil {
		return false
	}
	return evidence.GetRetryAt().AsTime().After(evidence.GetFailedAt().AsTime())
}

func (agent *Agent) SealOutput(
	ctx context.Context,
	authority *velav1.StageAuthority,
) (*velav1.LocalMaterializationReceipt, error) {
	if agent == nil || len(agent.ids) == 0 || ctx == nil {
		return nil, errors.New("single-output seal requires a configured ModelRuntime leader")
	}
	digest, err := stageauthority.Digest(authority)
	if err != nil {
		return nil, err
	}
	// Fleet and durable gang authority select the lexicographically smallest
	// WorkerMember UUID as leader. One logical distributed stage still publishes
	// exactly one output through that member.
	memberID := agent.ids[0]
	response, err := agent.members[memberID].SealOutput(
		ctx,
		&velav1.ModelRuntimeServiceSealOutputRequest{Authority: authority},
	)
	if err != nil {
		return nil, fmt.Errorf("seal ModelRuntime member %s: %w", memberID, err)
	}
	if response == nil ||
		(response.GetDecision() != velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_ACCEPTED &&
			response.GetDecision() != velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_REPLAYED) ||
		!bytes.Equal(response.GetAuthorityDigest(), digest[:]) ||
		!runtimeIdentityMatchesMember(response.GetRuntimeIdentity(), authority, memberID) {
		return nil, errors.New("ModelRuntime rejected output seal authority")
	}
	receipt := response.GetReceipt()
	if receipt == nil || receipt.GetReceiptId() == "" ||
		len(receipt.GetManifestSha256()) != sha256.Size || receipt.GetTotalSizeBytes() <= 0 ||
		receipt.GetSealedAt() == nil || receipt.GetSealedAt().CheckValid() != nil ||
		len(receipt.GetOutputManifestJson()) == 0 ||
		sha256.Sum256(receipt.GetOutputManifestJson()) != [sha256.Size]byte(receiptDigest(receipt)) {
		return nil, errors.New("ModelRuntime returned malformed local materialization receipt")
	}
	return proto.Clone(receipt).(*velav1.LocalMaterializationReceipt), nil
}

func receiptDigest(receipt *velav1.LocalMaterializationReceipt) [sha256.Size]byte {
	var digest [sha256.Size]byte
	copy(digest[:], receipt.GetManifestSha256())
	return digest
}

func (agent *Agent) validateAssignment(
	assignment *velav1.StageAssignment,
) ([]string, time.Duration, error) {
	if agent == nil || len(agent.ids) == 0 {
		return nil, 0, errors.New("missing configured Stage Worker Agent")
	}
	contract, err := stageassignment.Validate(assignment)
	if err != nil {
		return nil, 0, err
	}
	if !slices.Equal(contract.RequiredMemberIDs, agent.ids) {
		return nil, 0, errors.New("StageAssignment required member set is missing or mismatched")
	}
	return contract.RequiredMemberIDs, contract.MemberStartTimeout, nil
}

func (agent *Agent) prepareMembers(
	ctx context.Context,
	memberIDs []string,
	assignment *velav1.StageAssignment,
) []memberCallResult {
	results := make(chan memberCallResult, len(memberIDs))
	for _, memberID := range memberIDs {
		client := agent.members[memberID]
		go func() {
			response, err := client.PrepareStage(
				ctx,
				&velav1.ModelRuntimeServicePrepareStageRequest{
					Authority: assignment.GetAuthority(), ExecutionSpec: assignment.GetExecutionSpec(),
				},
			)
			results <- memberCallResult{id: memberID, prepare: response, err: err}
		}()
	}
	return collectMemberResults(results, len(memberIDs))
}

func (agent *Agent) startMembers(
	ctx context.Context,
	memberIDs []string,
	authority *velav1.StageAuthority,
) []memberCallResult {
	results := make(chan memberCallResult, len(memberIDs))
	for _, memberID := range memberIDs {
		client := agent.members[memberID]
		go func() {
			response, err := client.StartStage(
				ctx,
				&velav1.ModelRuntimeServiceStartStageRequest{Authority: authority},
			)
			results <- memberCallResult{id: memberID, start: response, err: err}
		}()
	}
	return collectMemberResults(results, len(memberIDs))
}

func (agent *Agent) cancelMembers(
	ctx context.Context,
	memberIDs []string,
	authority *velav1.StageAuthority,
	reason velav1.ModelRuntimeCancelReason,
) (int, error) {
	type cancelResult struct {
		id       string
		response *velav1.ModelRuntimeServiceCancelStageResponse
		err      error
	}
	results := make(chan cancelResult, len(memberIDs))
	for _, memberID := range memberIDs {
		client := agent.members[memberID]
		go func() {
			response, err := client.CancelStage(
				ctx,
				&velav1.ModelRuntimeServiceCancelStageRequest{Authority: authority, Reason: reason},
			)
			results <- cancelResult{id: memberID, response: response, err: err}
		}()
	}
	acknowledged := 0
	var joined error
	for range memberIDs {
		memberResult := <-results
		if memberResult.err != nil {
			joined = errors.Join(joined, fmt.Errorf("cancel member %s: %w", memberResult.id, memberResult.err))
			continue
		}
		response := memberResult.response
		if response == nil || !response.GetCancellationAcknowledged() ||
			(response.GetDecision() != velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_ACCEPTED &&
				response.GetDecision() != velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_REPLAYED) ||
			!runtimeIdentityMatchesMember(response.GetRuntimeIdentity(), authority, memberResult.id) {
			joined = errors.Join(joined, fmt.Errorf("cancel member %s was not acknowledged", memberResult.id))
			continue
		}
		acknowledged++
	}
	return acknowledged, joined
}

func (agent *Agent) cancelAfterBarrierFailure(
	ctx context.Context,
	memberIDs []string,
	authority *velav1.StageAuthority,
) (int, error) {
	cleanupContext, cancel := context.WithTimeout(
		context.WithoutCancel(ctx), agent.cancellationTimeout,
	)
	defer cancel()
	return agent.cancelMembers(
		cleanupContext,
		memberIDs,
		authority,
		velav1.ModelRuntimeCancelReason_MODEL_RUNTIME_CANCEL_REASON_MEMBER_BARRIER_FAILED,
	)
}

type memberCallResult struct {
	id      string
	prepare *velav1.ModelRuntimeServicePrepareStageResponse
	start   *velav1.ModelRuntimeServiceStartStageResponse
	err     error
}

func (result memberCallResult) errorValue() error {
	if result.err != nil {
		return result.err
	}
	if result.prepare != nil {
		return errors.New(boundedResponseDetail(result.prepare.GetDetail()))
	}
	if result.start != nil {
		return errors.New(boundedResponseDetail(result.start.GetDetail()))
	}
	return errors.New("ModelRuntime member returned no response")
}

func collectMemberResults(
	results <-chan memberCallResult,
	count int,
) []memberCallResult {
	collected := make([]memberCallResult, 0, count)
	for range count {
		collected = append(collected, <-results)
	}
	slices.SortFunc(collected, func(left, right memberCallResult) int {
		return strings.Compare(left.id, right.id)
	})
	return collected
}

func validPrepareResponse(
	response *velav1.ModelRuntimeServicePrepareStageResponse,
	digest [32]byte,
	authority *velav1.StageAuthority,
	memberID string,
) bool {
	return response != nil && bytes.Equal(response.GetAuthorityDigest(), digest[:]) &&
		runtimeIdentityMatchesMember(response.GetRuntimeIdentity(), authority, memberID) &&
		(response.GetDecision() == velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_ACCEPTED ||
			response.GetDecision() == velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_REPLAYED) &&
		response.GetState() == velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_PREPARED
}

func validStartResponse(
	response *velav1.ModelRuntimeServiceStartStageResponse,
	digest [32]byte,
	authority *velav1.StageAuthority,
	memberID string,
) bool {
	return response != nil && bytes.Equal(response.GetAuthorityDigest(), digest[:]) &&
		response.GetStartedAt() != nil && response.GetStartedAt().CheckValid() == nil &&
		runtimeIdentityMatchesMember(response.GetRuntimeIdentity(), authority, memberID) &&
		(response.GetDecision() == velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_ACCEPTED ||
			response.GetDecision() == velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_REPLAYED) &&
		response.GetState() == velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_RUNNING
}

func runtimeIdentityMatchesMember(
	identity *velav1.ModelRuntimeIdentity,
	authority *velav1.StageAuthority,
	memberID string,
) bool {
	if identity == nil || authority == nil || identity.GetWorkerMemberId() != memberID ||
		identity.GetWorkerInstanceId() != authority.GetWorkerInstanceId() ||
		identity.GetWorkerInstanceEpoch() != authority.GetWorkerInstanceEpoch() ||
		!bytes.Equal(identity.GetDeviceSetDigest(), authority.GetDeviceSetDigest()) ||
		!bytes.Equal(identity.GetMembershipDigest(), authority.GetMembershipDigest()) ||
		identity.GetModelResidencyId() != authority.GetModelResidencyId() ||
		identity.GetRuntimeIdentity() != authority.GetModelRuntimeIdentity() ||
		identity.GetStageProfileRevisionId() != authority.GetStageProfileRevisionId() {
		return false
	}
	for _, member := range authority.GetMembers() {
		if member.GetWorkerMemberId() == memberID {
			return member.GetMemberEpoch() == identity.GetWorkerMemberEpoch() &&
				member.GetModelRuntimeEpoch() == identity.GetModelRuntimeEpoch()
		}
	}
	return false
}

func boundedResponseDetail(detail string) string {
	if detail == "" {
		return "ModelRuntime member rejected command"
	}
	if len(detail) > 1000 {
		return detail[:1000]
	}
	return detail
}
