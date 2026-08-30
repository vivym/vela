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

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/stageauthority"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/protobuf/proto"
)

const (
	maxMemberStartTimeout            = 10 * time.Minute
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
}

type LocalReceipt struct {
	ID     string
	Digest []byte
}

func New(config Config) (*Agent, error) {
	if len(config.Members) == 0 || len(config.Members) > 64 {
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

func (agent *Agent) SealOutput(
	ctx context.Context,
	authority *velav1.StageAuthority,
) (*velav1.LocalMaterializationReceipt, error) {
	if agent == nil || len(agent.ids) != 1 || ctx == nil {
		return nil, errors.New("single-output seal requires exactly one configured ModelRuntime")
	}
	digest, err := stageauthority.Digest(authority)
	if err != nil {
		return nil, err
	}
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
	if assignment == nil || assignment.GetAuthority() == nil || assignment.GetMemberStartTimeout() == nil {
		return nil, 0, errors.New("StageAssignment member barrier is incomplete")
	}
	if err := assignment.GetMemberStartTimeout().CheckValid(); err != nil {
		return nil, 0, fmt.Errorf("StageAssignment member start timeout: %w", err)
	}
	timeout := assignment.GetMemberStartTimeout().AsDuration()
	if timeout <= 0 || timeout > maxMemberStartTimeout {
		return nil, 0, errors.New("StageAssignment member start timeout is invalid")
	}
	required := append([]string(nil), assignment.GetRequiredWorkerMemberIds()...)
	slices.Sort(required)
	if len(required) == 0 || !slices.Equal(required, agent.ids) {
		return nil, 0, errors.New("StageAssignment required member set is missing or mismatched")
	}
	authorityMembers := make([]string, 0, len(assignment.GetAuthority().GetMembers()))
	for _, member := range assignment.GetAuthority().GetMembers() {
		if member == nil {
			return nil, 0, errors.New("StageAuthority member set is invalid")
		}
		authorityMembers = append(authorityMembers, member.GetWorkerMemberId())
	}
	slices.Sort(authorityMembers)
	if !slices.Equal(required, authorityMembers) {
		return nil, 0, errors.New("StageAssignment member barrier does not match signed authority")
	}
	executionSpecDigest, err := stageauthority.ExecutionSpecDigest(assignment.GetExecutionSpec())
	if err != nil || !bytes.Equal(
		executionSpecDigest[:], assignment.GetAuthority().GetExecutionSpecDigest(),
	) {
		return nil, 0, errors.New("StageAssignment execution spec does not match signed authority")
	}
	if err := validateInputTransferTickets(assignment); err != nil {
		return nil, 0, err
	}
	return required, timeout, nil
}

func validateInputTransferTickets(assignment *velav1.StageAssignment) error {
	inputs := assignment.GetExecutionSpec().GetInputs()
	tickets := assignment.GetInputTransferTickets()
	if len(inputs) != len(tickets) {
		return errors.New("StageAssignment input TransferTicket set is incomplete")
	}
	type artifactVersion struct {
		artifactID    string
		objectVersion string
	}
	expected := make(map[artifactVersion]struct{}, len(inputs))
	for _, input := range inputs {
		if input == nil {
			return errors.New("StageAssignment input Artifact is invalid")
		}
		if _, err := uuid.Parse(input.GetStageArtifactId()); err != nil || input.GetObjectVersion() == "" {
			return errors.New("StageAssignment input Artifact version identity is invalid")
		}
		key := artifactVersion{
			artifactID: input.GetStageArtifactId(), objectVersion: input.GetObjectVersion(),
		}
		if _, duplicated := expected[key]; duplicated {
			return errors.New("StageAssignment input Artifact version is duplicated")
		}
		expected[key] = struct{}{}
	}
	for _, ticket := range tickets {
		if ticket == nil || len(ticket.GetTransferTicket()) == 0 {
			return errors.New("StageAssignment input TransferTicket is invalid")
		}
		if _, err := uuid.Parse(ticket.GetStageArtifactId()); err != nil || ticket.GetObjectVersion() == "" {
			return errors.New("StageAssignment input TransferTicket version identity is invalid")
		}
		key := artifactVersion{
			artifactID: ticket.GetStageArtifactId(), objectVersion: ticket.GetObjectVersion(),
		}
		if _, ok := expected[key]; !ok {
			return errors.New("StageAssignment input TransferTicket does not match an exact input version")
		}
		delete(expected, key)
	}
	if len(expected) != 0 {
		return errors.New("StageAssignment input TransferTicket set is incomplete")
	}
	return nil
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
