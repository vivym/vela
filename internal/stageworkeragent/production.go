package stageworkeragent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
	"time"

	"github.com/google/uuid"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	maxReadinessEvidenceBytes = 64 << 10
	maxCapacityTTL            = time.Hour
	maxNoWorkRetry            = time.Hour
	defaultRetryMinimum       = time.Second
	defaultRetryMaximum       = 30 * time.Second
)

var productionReadinessChecks = []velav1.ModelRuntimeReadinessCheck{
	velav1.ModelRuntimeReadinessCheck_MODEL_RUNTIME_READINESS_CHECK_DEVICE,
	velav1.ModelRuntimeReadinessCheck_MODEL_RUNTIME_READINESS_CHECK_BACKEND,
	velav1.ModelRuntimeReadinessCheck_MODEL_RUNTIME_READINESS_CHECK_MODEL_WARMUP,
	velav1.ModelRuntimeReadinessCheck_MODEL_RUNTIME_READINESS_CHECK_CANARY,
}

var errControlReconnect = errors.New("stage worker control reconnect required")

type RuntimeReadinessClient interface {
	ProbeReadiness(
		context.Context,
		*velav1.ModelRuntimeServiceProbeReadinessRequest,
		...grpc.CallOption,
	) (*velav1.ModelRuntimeServiceProbeReadinessResponse, error)
}

type RuntimeIdentityDiscoveryClient interface {
	DiscoverRuntimeIdentities(
		context.Context,
		*velav1.ModelRuntimeServiceDiscoverRuntimeIdentitiesRequest,
		...grpc.CallOption,
	) (*velav1.ModelRuntimeServiceDiscoverRuntimeIdentitiesResponse, error)
}

type CapacityObservationSequenceSource interface {
	NextCapacityObservationSequence(context.Context) (int64, error)
}

type capacityObservationSequenceObserver interface {
	ObserveCapacityObservationSequence(context.Context, int64) error
}

type controlSessionEpochReader interface {
	CurrentControlSessionEpoch() int64
}

type RetryObserver func(operation string, err error)

type capacityPublicationState uint8

const (
	capacityPublicationUnknown capacityPublicationState = iota
	capacityPublicationUnavailable
	capacityPublicationReady
)

type RuntimeIdentityExpectation struct {
	WorkerInstanceID    string
	WorkerInstanceEpoch int64
	WorkerMemberID      string
	WorkerMemberEpoch   int64
}

func DiscoverRuntimeIdentity(
	ctx context.Context,
	runtime RuntimeReadinessClient,
	expected RuntimeIdentityExpectation,
) (*velav1.ModelRuntimeIdentity, error) {
	if ctx == nil || runtime == nil || uuid.Validate(expected.WorkerInstanceID) != nil ||
		expected.WorkerInstanceEpoch <= 0 || uuid.Validate(expected.WorkerMemberID) != nil ||
		expected.WorkerMemberEpoch <= 0 {
		return nil, errors.New("ModelRuntime identity discovery configuration is invalid")
	}
	response, err := runtime.ProbeReadiness(
		ctx,
		&velav1.ModelRuntimeServiceProbeReadinessRequest{
			Check: velav1.ModelRuntimeReadinessCheck_MODEL_RUNTIME_READINESS_CHECK_DEVICE,
		},
	)
	if err != nil {
		return nil, fmt.Errorf("discover resident ModelRuntime identity: %w", err)
	}
	identity := response.GetIdentity()
	if response == nil || response.GetCheck() !=
		velav1.ModelRuntimeReadinessCheck_MODEL_RUNTIME_READINESS_CHECK_DEVICE ||
		validateProductionRuntimeIdentity(identity) != nil {
		return nil, errors.New("resident ModelRuntime returned an invalid identity")
	}
	if identity.GetWorkerInstanceId() != expected.WorkerInstanceID ||
		identity.GetWorkerInstanceEpoch() != expected.WorkerInstanceEpoch ||
		identity.GetWorkerMemberId() != expected.WorkerMemberID ||
		identity.GetWorkerMemberEpoch() != expected.WorkerMemberEpoch {
		return nil, errors.New("resident ModelRuntime identity does not match configured WorkerInstance member")
	}
	return proto.Clone(identity).(*velav1.ModelRuntimeIdentity), nil
}

func DiscoverRuntimeIdentities(
	ctx context.Context,
	runtime RuntimeIdentityDiscoveryClient,
	expected RuntimeIdentityExpectation,
) ([]*velav1.ModelRuntimeIdentity, error) {
	if ctx == nil || runtime == nil || uuid.Validate(expected.WorkerInstanceID) != nil ||
		expected.WorkerInstanceEpoch <= 0 || uuid.Validate(expected.WorkerMemberID) != nil ||
		expected.WorkerMemberEpoch <= 0 {
		return nil, errors.New("ModelRuntime identity discovery configuration is invalid")
	}
	response, err := runtime.DiscoverRuntimeIdentities(
		ctx,
		&velav1.ModelRuntimeServiceDiscoverRuntimeIdentitiesRequest{
			WorkerInstanceId:    expected.WorkerInstanceID,
			WorkerInstanceEpoch: expected.WorkerInstanceEpoch,
			WorkerMemberId:      expected.WorkerMemberID,
			WorkerMemberEpoch:   expected.WorkerMemberEpoch,
		},
	)
	if err != nil {
		return nil, fmt.Errorf("discover resident ModelRuntime identities: %w", err)
	}
	if response == nil || len(response.GetIdentities()) == 0 || len(response.GetIdentities()) > 16 {
		return nil, errors.New("resident ModelRuntime returned no bounded identity set")
	}
	identities := make([]*velav1.ModelRuntimeIdentity, 0, len(response.GetIdentities()))
	seen := make(map[string]struct{}, len(response.GetIdentities()))
	var deviceSetDigest []byte
	var membershipDigest []byte
	for _, identity := range response.GetIdentities() {
		if validateProductionRuntimeIdentity(identity) != nil ||
			identity.GetWorkerInstanceId() != expected.WorkerInstanceID ||
			identity.GetWorkerInstanceEpoch() != expected.WorkerInstanceEpoch ||
			identity.GetWorkerMemberId() != expected.WorkerMemberID ||
			identity.GetWorkerMemberEpoch() != expected.WorkerMemberEpoch {
			return nil, errors.New("resident ModelRuntime identity set does not match configured WorkerInstance member")
		}
		key := strings.Join([]string{
			identity.GetModelResidencyId(), identity.GetRuntimeIdentity(),
			identity.GetStageProfileRevisionId(), fmt.Sprintf("%d", identity.GetModelRuntimeEpoch()),
		}, "\x00")
		if _, duplicate := seen[key]; duplicate {
			return nil, errors.New("resident ModelRuntime identity set is duplicated")
		}
		seen[key] = struct{}{}
		if len(deviceSetDigest) == 0 {
			deviceSetDigest = append([]byte(nil), identity.GetDeviceSetDigest()...)
			membershipDigest = append([]byte(nil), identity.GetMembershipDigest()...)
		} else if !slices.Equal(deviceSetDigest, identity.GetDeviceSetDigest()) ||
			!slices.Equal(membershipDigest, identity.GetMembershipDigest()) {
			return nil, errors.New("resident ModelRuntime identities disagree on WorkerInstance topology")
		}
		identities = append(identities, proto.Clone(identity).(*velav1.ModelRuntimeIdentity))
	}
	return identities, nil
}

type ProductionConfig struct {
	Control                   ControlClient
	Runtime                   RuntimeReadinessClient
	Stream                    *StreamAgent
	RuntimeIdentity           *velav1.ModelRuntimeIdentity
	RuntimeIdentities         []*velav1.ModelRuntimeIdentity
	Devices                   []*velav1.StageAuthorityDeviceEpoch
	Members                   []*velav1.StageAuthorityMemberEpoch
	CapacityVector            map[string]int64
	CapacityTTL               time.Duration
	HeartbeatInterval         time.Duration
	RetryMinimum              time.Duration
	RetryMaximum              time.Duration
	ObservationSequenceSource CapacityObservationSequenceSource
	RetryObserver             RetryObserver
	Now                       func() time.Time
	Wait                      func(context.Context, time.Duration) error
}

type ProductionAgent struct {
	control                     ControlClient
	runtime                     RuntimeReadinessClient
	stream                      *StreamAgent
	runtimeIdentities           []*velav1.ModelRuntimeIdentity
	runtimeBarrierGenerations   map[string]int64
	acquireCursor               int
	devices                     []*velav1.StageAuthorityDeviceEpoch
	members                     []*velav1.StageAuthorityMemberEpoch
	capacityVector              map[string]int64
	capacityTTL                 time.Duration
	heartbeatInterval           time.Duration
	retryMinimum                time.Duration
	retryMaximum                time.Duration
	observationSequenceSource   CapacityObservationSequenceSource
	retryObserver               RetryObserver
	capacityPublicationState    capacityPublicationState
	capacityObservationSequence int64
	capacityExpiresAt           time.Time
	capacityControlSessionEpoch int64
	now                         func() time.Time
	wait                        func(context.Context, time.Duration) error
}

type DiscoveryResult struct {
	Assignment *velav1.StageAssignment
	RetryAfter time.Duration
}

type capacityPublication struct {
	sequence            int64
	expiresAt           time.Time
	controlSessionEpoch int64
	accepted            bool
}

type readinessEvidenceV1 struct {
	SchemaVersion int                        `json:"schema_version"`
	Checks        []readinessCheckEvidenceV1 `json:"checks"`
}

type readinessCheckEvidenceV1 struct {
	Check    string `json:"check"`
	Evidence []byte `json:"evidence"`
	Detail   string `json:"detail,omitempty"`
}

func NewProductionAgent(config ProductionConfig) (*ProductionAgent, error) {
	if config.Control == nil || config.Runtime == nil || config.Now == nil ||
		config.CapacityTTL <= 0 || config.CapacityTTL > maxCapacityTTL {
		return nil, errors.New("stage worker production Agent configuration is incomplete")
	}
	identities, err := normalizeProductionRuntimeIdentities(config)
	if err != nil {
		return nil, err
	}
	for _, identity := range identities {
		if err := validateProductionTopology(
			identity, config.Devices, membersForRuntimeIdentity(config.Members, identity),
		); err != nil {
			return nil, err
		}
	}
	if err := validateProductionCapacity(config.CapacityVector); err != nil {
		return nil, err
	}
	if config.RetryMinimum == 0 {
		config.RetryMinimum = defaultRetryMinimum
	}
	if config.RetryMaximum == 0 {
		config.RetryMaximum = defaultRetryMaximum
	}
	if config.RetryMinimum <= 0 || config.RetryMaximum < config.RetryMinimum {
		return nil, errors.New("stage worker production retry configuration is invalid")
	}
	configuredExecution := config.Stream != nil || config.HeartbeatInterval != 0 || config.Wait != nil
	if configuredExecution &&
		(config.Stream == nil || config.HeartbeatInterval <= 0 || config.Wait == nil) {
		return nil, errors.New("stage worker production execution configuration is incomplete")
	}
	return &ProductionAgent{
		control: config.Control, runtime: config.Runtime, stream: config.Stream,
		runtimeIdentities:         identities,
		runtimeBarrierGenerations: make(map[string]int64, len(identities)),
		devices:                   cloneDevices(config.Devices),
		members:                   cloneMembers(config.Members),
		capacityVector:            maps.Clone(config.CapacityVector),
		capacityTTL:               config.CapacityTTL,
		heartbeatInterval:         config.HeartbeatInterval,
		retryMinimum:              config.RetryMinimum,
		retryMaximum:              config.RetryMaximum,
		observationSequenceSource: config.ObservationSequenceSource,
		retryObserver:             config.RetryObserver,
		now:                       config.Now,
		wait:                      config.Wait,
	}, nil
}

func (agent *ProductionAgent) Run(ctx context.Context) error {
	if agent == nil || agent.stream == nil || agent.wait == nil ||
		agent.observationSequenceSource == nil || ctx == nil {
		return errors.New("stage worker production service is not configured")
	}
	commandErrors := make(chan error, 1)
	if agent.control.Commands() == nil {
		return errors.New("stage worker production command stream is unavailable")
	}
	go func() {
		commandErrors <- agent.stream.RunControlCommands(ctx)
	}()
	backoff := agent.retryMinimum
	for {
		if ctx.Err() != nil {
			return nil
		}
		select {
		case err := <-commandErrors:
			if ctx.Err() != nil || errors.Is(err, context.Canceled) {
				return nil
			}
			return fmt.Errorf("consume Stage Worker control commands: %w", err)
		default:
		}
		if _, err := agent.stream.ResumeMaterializations(ctx); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			if err := agent.wait(ctx, backoff); err != nil {
				if ctx.Err() != nil {
					return nil
				}
				return err
			}
			backoff = nextBackoff(backoff, agent.retryMaximum)
			continue
		}
		discovery, err := agent.Discover(ctx, 0)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			if err := agent.wait(ctx, backoff); err != nil {
				if ctx.Err() != nil {
					return nil
				}
				return err
			}
			backoff = nextBackoff(backoff, agent.retryMaximum)
			continue
		}
		backoff = agent.retryMinimum
		if discovery.Assignment == nil {
			if err := agent.wait(ctx, discovery.RetryAfter); err != nil {
				if ctx.Err() != nil {
					return nil
				}
				return err
			}
			continue
		}
		result, heartbeatSequence, err := agent.startAndMonitor(ctx, discovery.Assignment)
		for errors.Is(err, errControlReconnect) && !result.GPUReleased {
			if err := agent.wait(ctx, backoff); err != nil {
				if ctx.Err() != nil {
					return nil
				}
				return err
			}
			backoff = nextBackoff(backoff, agent.retryMaximum)
			leader, _, refreshErr := agent.refreshEvidence(ctx, 0)
			if refreshErr != nil {
				err = refreshErr
				continue
			}
			if !leader {
				err = errors.New("active Stage Worker is no longer the gang leader")
				continue
			}
			if err = agent.reattachActive(ctx); err != nil {
				continue
			}
			backoff = agent.retryMinimum
			result, heartbeatSequence, err = agent.monitorActive(ctx, heartbeatSequence)
		}
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			if result.GPUReleased {
				continue
			}
			return err
		}
	}
}

func (agent *ProductionAgent) nextObservationSequence(ctx context.Context) (int64, error) {
	if agent == nil || agent.observationSequenceSource == nil || ctx == nil {
		return 0, errors.New("stage worker capacity observation sequencer is not configured")
	}
	sequence, err := agent.observationSequenceSource.NextCapacityObservationSequence(ctx)
	if err != nil {
		return 0, fmt.Errorf("allocate Stage Worker capacity observation sequence: %w", err)
	}
	if sequence <= 0 {
		return 0, errors.New("stage worker capacity observation sequencer returned an invalid sequence")
	}
	return sequence, nil
}

func (agent *ProductionAgent) RunAssignment(
	ctx context.Context,
	assignment *velav1.StageAssignment,
) (MaterializationResult, error) {
	result, _, err := agent.startAndMonitor(ctx, assignment)
	return result, err
}

func (agent *ProductionAgent) startAndMonitor(
	ctx context.Context,
	assignment *velav1.StageAssignment,
) (MaterializationResult, int64, error) {
	if agent == nil || agent.stream == nil || agent.wait == nil ||
		agent.heartbeatInterval <= 0 || ctx == nil || assignment == nil {
		return MaterializationResult{}, 1, errors.New("stage worker production execution is not configured")
	}
	if _, err := agent.stream.ExecuteAssignment(ctx, assignment); err != nil {
		return MaterializationResult{}, 1, fmt.Errorf("execute StageAssignment: %w", err)
	}
	return agent.monitorActive(ctx, 1)
}

func (agent *ProductionAgent) monitorActive(
	ctx context.Context,
	sequence int64,
) (MaterializationResult, int64, error) {
	for ; ; sequence++ {
		authority := agent.stream.activeAuthority()
		if authority == nil {
			return MaterializationResult{}, sequence, errors.New("stage worker lost active authority before completion")
		}
		status, err := agent.stream.runtime.Status(ctx, authority)
		if err != nil {
			return MaterializationResult{}, sequence, fmt.Errorf("observe ModelRuntime execution: %w", err)
		}
		if _, err := agent.stream.Heartbeat(ctx, sequence); err != nil {
			return MaterializationResult{}, sequence + 1, fmt.Errorf("%w: heartbeat StageAssignment: %v", errControlReconnect, err)
		}
		switch aggregateRuntimeState(status) {
		case velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_OUTPUT_READY,
			velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_OUTPUT_SEALED:
			result, err := agent.stream.SealAndMaterialize(ctx)
			if err != nil {
				return result, sequence + 1, fmt.Errorf("materialize StageAssignment output: %w", err)
			}
			return result, sequence + 1, nil
		case velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_FAILED:
			if _, err := agent.stream.Fail(ctx, status); err != nil {
				return MaterializationResult{}, sequence + 1, fmt.Errorf(
					"%w: report failed StageAssignment: %v", errControlReconnect, err,
				)
			}
			return MaterializationResult{GPUReleased: true}, sequence + 1,
				errors.New("ModelRuntime failure accepted by control")
		case velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_CANCELING,
			velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_STOPPED:
			return MaterializationResult{}, sequence + 1, errors.New("ModelRuntime stopped StageAssignment before output")
		}
		if err := agent.wait(ctx, agent.heartbeatInterval); err != nil {
			return MaterializationResult{}, sequence + 1, err
		}
	}
}

func (agent *ProductionAgent) reattachActive(ctx context.Context) error {
	authority := agent.stream.activeAuthority()
	if authority == nil {
		return errors.New("stage worker has no active authority to reattach")
	}
	status, err := agent.stream.runtime.Status(ctx, authority)
	if err != nil {
		return fmt.Errorf("observe ModelRuntime before reattach: %w", err)
	}
	receiptID, receiptDigest, err := aggregateReattachReceipt(status)
	if err != nil {
		return err
	}
	result, err := agent.stream.Reattach(ctx, authority, receiptID, receiptDigest)
	if err != nil {
		return fmt.Errorf("reattach active StageAssignment: %w", err)
	}
	if !result.Accepted {
		return errors.New("control did not accept StageAssignment reattach")
	}
	return nil
}

func (agent *ProductionAgent) Discover(
	ctx context.Context,
	observationSequence int64,
) (DiscoveryResult, error) {
	if agent == nil || agent.control == nil || agent.runtime == nil || agent.now == nil ||
		ctx == nil || observationSequence < 0 ||
		(observationSequence == 0 && agent.observationSequenceSource == nil) {
		return DiscoveryResult{}, errors.New("stage worker production discovery is not configured")
	}
	leader, readySequence, err := agent.refreshEvidence(ctx, observationSequence)
	if err != nil {
		return DiscoveryResult{}, err
	}
	if !leader {
		return DiscoveryResult{RetryAfter: agent.retryMinimum}, nil
	}
	return agent.acquire(ctx, readySequence)
}

func (agent *ProductionAgent) refreshEvidence(
	ctx context.Context,
	observationSequence int64,
) (bool, int64, error) {
	primary := agent.primaryRuntimeIdentity()
	capacityReporter := agent.isCapacityReporter()
	sequenceHint := observationSequence
	nextSequence := func() (int64, error) {
		if sequenceHint > 0 {
			sequence := sequenceHint
			sequenceHint = 0
			return sequence, nil
		}
		return agent.nextObservationSequence(ctx)
	}
	registrationSequence := agent.capacityObservationSequence
	if capacityReporter && agent.capacityPublicationState == capacityPublicationUnknown {
		unavailable := maps.Clone(agent.capacityVector)
		for resource := range unavailable {
			unavailable[resource] = 0
		}
		sequence, err := nextSequence()
		if err != nil {
			return false, 0, err
		}
		publication, err := agent.reportCapacity(ctx, primary, sequence, unavailable)
		agent.recordCapacityPublication(capacityPublicationUnavailable, publication)
		if err != nil {
			return false, 0, err
		}
		registrationSequence = publication.sequence
	}
	barrierGenerations := make(map[string]int64, len(agent.runtimeIdentities))
	leaderMemberID := ""
	for _, identity := range agent.runtimeIdentities {
		evidence, err := agent.probeReadiness(ctx, identity)
		if err != nil {
			return agent.rejectEvidence(ctx, primary, sequenceHint, err)
		}
		registered, err := agent.control.Exchange(ctx, &velav1.StageWorkerControlServiceConnectRequest{
			Operation: &velav1.StageWorkerControlServiceConnectRequest_RegisterWorkerEvidence{
				RegisterWorkerEvidence: &velav1.RegisterWorkerEvidenceRequest{
					RuntimeIdentity:             proto.Clone(identity).(*velav1.ModelRuntimeIdentity),
					CapacityObservationSequence: registrationSequence,
					Devices:                     cloneDevices(agent.devices),
					Members:                     membersForRuntimeIdentity(agent.members, identity),
					ReadinessEvidence:           evidence,
				},
			},
		})
		if err != nil {
			err = fmt.Errorf("register Stage Worker evidence: %w", err)
			agent.observeRetry("register-worker-evidence", err)
			return agent.rejectEvidence(ctx, primary, sequenceHint, err)
		}
		decision, err := agent.requireReady(registered, "registration")
		if err != nil {
			agent.observeRetry("register-worker-evidence", err)
			return agent.rejectEvidence(ctx, primary, observationSequence, err)
		}
		if decision.GetModelRuntimeBarrierGeneration() <= 0 ||
			uuid.Validate(decision.GetLeaderWorkerMemberId()) != nil ||
			(leaderMemberID != "" && leaderMemberID != decision.GetLeaderWorkerMemberId()) {
			return agent.rejectEvidence(
				ctx,
				primary,
				sequenceHint,
				errors.New("stage worker registration returned an inconsistent gang barrier"),
			)
		}
		leaderMemberID = decision.GetLeaderWorkerMemberId()
		barrierGenerations[identity.GetModelResidencyId()] =
			decision.GetModelRuntimeBarrierGeneration()
	}
	agent.runtimeBarrierGenerations = barrierGenerations
	if primary.GetWorkerMemberId() != leaderMemberID {
		return false, 0, nil
	}
	if agent.capacityPublicationFresh() {
		return true, agent.capacityObservationSequence, nil
	}
	readySequence, err := nextSequence()
	if err != nil {
		return false, 0, err
	}
	publication, err := agent.reportCapacity(ctx, primary, readySequence, maps.Clone(agent.capacityVector))
	agent.recordCapacityPublication(capacityPublicationReady, publication)
	if err != nil {
		if publication.accepted {
			return agent.rejectEvidence(ctx, primary, publication.sequence, err)
		}
		return false, 0, err
	}
	return true, publication.sequence, nil
}

func (agent *ProductionAgent) rejectEvidence(
	ctx context.Context,
	identity *velav1.ModelRuntimeIdentity,
	observationSequence int64,
	cause error,
) (bool, int64, error) {
	if !agent.isCapacityReporter() ||
		agent.capacityPublicationState != capacityPublicationReady {
		return false, 0, cause
	}
	if observationSequence <= 0 {
		var err error
		observationSequence, err = agent.nextObservationSequence(ctx)
		if err != nil {
			return false, 0, errors.Join(cause, err)
		}
	}
	unavailable := maps.Clone(agent.capacityVector)
	for resource := range unavailable {
		unavailable[resource] = 0
	}
	publication, err := agent.reportCapacity(ctx, identity, observationSequence, unavailable)
	agent.recordCapacityPublication(capacityPublicationUnavailable, publication)
	if err != nil {
		return false, 0, errors.Join(cause, fmt.Errorf("withdraw Stage Worker capacity: %w", err))
	}
	return false, 0, cause
}

func (agent *ProductionAgent) capacityPublicationFresh() bool {
	if agent.capacityPublicationState != capacityPublicationReady ||
		agent.capacityObservationSequence <= 0 || agent.capacityExpiresAt.IsZero() ||
		!agent.now().UTC().Before(agent.capacityExpiresAt.Add(-agent.capacityTTL/2)) {
		return false
	}
	currentEpoch := agent.currentControlSessionEpoch()
	return currentEpoch == 0 || agent.capacityControlSessionEpoch == 0 ||
		currentEpoch == agent.capacityControlSessionEpoch
}

func (agent *ProductionAgent) currentControlSessionEpoch() int64 {
	reader, ok := agent.control.(controlSessionEpochReader)
	if !ok {
		return 0
	}
	return reader.CurrentControlSessionEpoch()
}

func (agent *ProductionAgent) recordCapacityPublication(
	state capacityPublicationState,
	publication capacityPublication,
) {
	if !publication.accepted {
		return
	}
	agent.capacityPublicationState = state
	agent.capacityObservationSequence = publication.sequence
	agent.capacityExpiresAt = publication.expiresAt
	agent.capacityControlSessionEpoch = publication.controlSessionEpoch
}

func (agent *ProductionAgent) isCapacityReporter() bool {
	localMemberID := agent.primaryRuntimeIdentity().GetWorkerMemberId()
	leaderMemberID := localMemberID
	for _, member := range agent.members {
		if candidate := member.GetWorkerMemberId(); candidate < leaderMemberID {
			leaderMemberID = candidate
		}
	}
	return localMemberID == leaderMemberID
}

func (agent *ProductionAgent) reportCapacity(
	ctx context.Context,
	identity *velav1.ModelRuntimeIdentity,
	observationSequence int64,
	capacityVector map[string]int64,
) (capacityPublication, error) {
	now := agent.now().UTC()
	reported, err := agent.control.Exchange(ctx, &velav1.StageWorkerControlServiceConnectRequest{
		Operation: &velav1.StageWorkerControlServiceConnectRequest_ReportCapacityObservation{
			ReportCapacityObservation: &velav1.ReportStageCapacityObservationRequest{
				WorkerInstanceId:    identity.GetWorkerInstanceId(),
				WorkerInstanceEpoch: identity.GetWorkerInstanceEpoch(),
				ObservationSequence: observationSequence,
				CapacityVector:      capacityVector,
				ObservedAt:          timestamppb.New(now),
				ExpiresAt:           timestamppb.New(now.Add(agent.capacityTTL)),
			},
		},
	})
	if err != nil {
		err = fmt.Errorf("report Stage Worker capacity: %w", err)
		agent.observeRetry("report-capacity", err)
		return capacityPublication{}, err
	}
	decision, err := agent.requireReady(reported, "capacity")
	if err != nil {
		agent.observeRetry("report-capacity", err)
		return capacityPublication{}, err
	}
	acceptedSequence := decision.GetCapacityObservationSequence()
	if acceptedSequence <= 0 {
		return capacityPublication{}, errors.New("stage worker capacity response omitted its durable sequence")
	}
	publication := capacityPublication{
		sequence: acceptedSequence, expiresAt: now.Add(agent.capacityTTL),
		controlSessionEpoch: agent.currentControlSessionEpoch(), accepted: true,
	}
	if observer, ok := agent.observationSequenceSource.(capacityObservationSequenceObserver); ok {
		if err := observer.ObserveCapacityObservationSequence(ctx, acceptedSequence); err != nil {
			return publication, fmt.Errorf("persist durable Stage Worker capacity sequence: %w", err)
		}
	}
	return publication, nil
}

func (agent *ProductionAgent) observeRetry(operation string, err error) {
	if agent != nil && agent.retryObserver != nil && err != nil {
		agent.retryObserver(operation, err)
	}
}

func (agent *ProductionAgent) acquire(
	ctx context.Context,
	observationSequence int64,
) (DiscoveryResult, error) {
	minimumRetry := time.Duration(0)
	for offset := range len(agent.runtimeIdentities) {
		index := (agent.acquireCursor + offset) % len(agent.runtimeIdentities)
		identity := agent.runtimeIdentities[index]
		barrierGeneration := agent.runtimeBarrierGenerations[identity.GetModelResidencyId()]
		if barrierGeneration <= 0 {
			return DiscoveryResult{}, errors.New("stage worker acquire has no ready ModelRuntime barrier")
		}
		response, err := agent.control.Exchange(ctx, &velav1.StageWorkerControlServiceConnectRequest{
			Operation: &velav1.StageWorkerControlServiceConnectRequest_AcquireStage{
				AcquireStage: &velav1.AcquireStageRequest{
					WorkerInstanceId:            identity.GetWorkerInstanceId(),
					WorkerInstanceEpoch:         identity.GetWorkerInstanceEpoch(),
					CapacityObservationSequence: observationSequence,
					ModelResidencyId:            identity.GetModelResidencyId(),
					ModelRuntimeEpoch:           barrierGeneration,
					StageProfileRevisionId:      identity.GetStageProfileRevisionId(),
				},
			},
		})
		if err != nil {
			return DiscoveryResult{}, fmt.Errorf("acquire StageAssignment: %w", err)
		}
		if assignment := response.GetStageAssignment(); assignment != nil {
			agent.acquireCursor = (index + 1) % len(agent.runtimeIdentities)
			return DiscoveryResult{
				Assignment: proto.Clone(assignment).(*velav1.StageAssignment),
			}, nil
		}
		if noWork := response.GetNoWork(); noWork != nil && noWork.GetRetryAfter() != nil {
			retryAfter := noWork.GetRetryAfter().AsDuration()
			if retryAfter > 0 && retryAfter <= maxNoWorkRetry &&
				(minimumRetry == 0 || retryAfter < minimumRetry) {
				minimumRetry = retryAfter
			}
			continue
		}
		if command := response.GetStageCommandResult(); command != nil &&
			(command.GetDecision() == velav1.StageWorkerCommandDecision_STAGE_WORKER_COMMAND_DECISION_STALE ||
				command.GetDecision() == velav1.StageWorkerCommandDecision_STAGE_WORKER_COMMAND_DECISION_REJECTED) {
			return DiscoveryResult{}, fmt.Errorf("stage worker acquire rejected: %s", command.GetDetail())
		}
		return DiscoveryResult{}, errors.New("stage worker acquire returned a malformed result")
	}
	if minimumRetry == 0 {
		return DiscoveryResult{}, errors.New("stage worker acquire returned no valid retry interval")
	}
	agent.acquireCursor = (agent.acquireCursor + 1) % len(agent.runtimeIdentities)
	return DiscoveryResult{RetryAfter: minimumRetry}, nil
}

func (agent *ProductionAgent) probeReadiness(
	ctx context.Context,
	identity *velav1.ModelRuntimeIdentity,
) ([]byte, error) {
	document := readinessEvidenceV1{SchemaVersion: 1}
	for _, check := range productionReadinessChecks {
		response, err := agent.runtime.ProbeReadiness(
			ctx,
			&velav1.ModelRuntimeServiceProbeReadinessRequest{
				Identity: proto.Clone(identity).(*velav1.ModelRuntimeIdentity),
				Check:    check,
			},
		)
		if err != nil {
			return nil, fmt.Errorf("probe ModelRuntime %s readiness: %w", check.String(), err)
		}
		if response == nil || response.GetCheck() != check || !response.GetReady() ||
			!proto.Equal(response.GetIdentity(), identity) || len(response.GetEvidence()) == 0 ||
			len(response.GetEvidence()) > maxReadinessEvidenceBytes {
			return nil, fmt.Errorf("ModelRuntime %s readiness evidence is invalid", check.String())
		}
		document.Checks = append(document.Checks, readinessCheckEvidenceV1{
			Check: check.String(), Evidence: append([]byte(nil), response.GetEvidence()...),
			Detail: response.GetDetail(),
		})
	}
	evidence, err := json.Marshal(document)
	if err != nil {
		return nil, fmt.Errorf("encode ModelRuntime readiness evidence: %w", err)
	}
	if len(evidence) == 0 || len(evidence) > maxReadinessEvidenceBytes {
		return nil, errors.New("aggregate ModelRuntime readiness evidence exceeds bound")
	}
	return evidence, nil
}

func (agent *ProductionAgent) requireReady(
	response *velav1.StageWorkerControlServiceConnectResponse,
	operation string,
) (*velav1.WorkerReadinessDecision, error) {
	decision := response.GetWorkerReadinessDecision()
	identity := agent.primaryRuntimeIdentity()
	if decision == nil || decision.GetWorkerInstanceId() != identity.GetWorkerInstanceId() ||
		decision.GetWorkerInstanceEpoch() != identity.GetWorkerInstanceEpoch() ||
		!decision.GetReady() {
		reason := "malformed readiness decision"
		if decision != nil && strings.TrimSpace(decision.GetReason()) != "" {
			reason = decision.GetReason()
		}
		return nil, fmt.Errorf("stage worker %s is not ready: %s", operation, reason)
	}
	return decision, nil
}

func normalizeProductionRuntimeIdentities(
	config ProductionConfig,
) ([]*velav1.ModelRuntimeIdentity, error) {
	values := config.RuntimeIdentities
	if len(values) == 0 && config.RuntimeIdentity != nil {
		values = []*velav1.ModelRuntimeIdentity{config.RuntimeIdentity}
	}
	if len(values) == 0 || len(values) > 16 ||
		(config.RuntimeIdentity != nil && len(config.RuntimeIdentities) != 0) {
		return nil, errors.New("stage worker production runtime identity set is invalid")
	}
	identities := make([]*velav1.ModelRuntimeIdentity, 0, len(values))
	seen := make(map[string]struct{}, len(values))
	for _, identity := range values {
		if err := validateProductionRuntimeIdentity(identity); err != nil {
			return nil, err
		}
		key := strings.Join([]string{
			identity.GetModelResidencyId(), identity.GetRuntimeIdentity(),
			identity.GetStageProfileRevisionId(), fmt.Sprintf("%d", identity.GetModelRuntimeEpoch()),
		}, "\x00")
		if _, duplicate := seen[key]; duplicate {
			return nil, errors.New("stage worker production runtime identity set is duplicated")
		}
		seen[key] = struct{}{}
		identities = append(identities, proto.Clone(identity).(*velav1.ModelRuntimeIdentity))
	}
	primary := identities[0]
	for _, identity := range identities[1:] {
		if identity.GetWorkerInstanceId() != primary.GetWorkerInstanceId() ||
			identity.GetWorkerInstanceEpoch() != primary.GetWorkerInstanceEpoch() ||
			identity.GetWorkerMemberId() != primary.GetWorkerMemberId() ||
			identity.GetWorkerMemberEpoch() != primary.GetWorkerMemberEpoch() ||
			!slices.Equal(identity.GetDeviceSetDigest(), primary.GetDeviceSetDigest()) ||
			!slices.Equal(identity.GetMembershipDigest(), primary.GetMembershipDigest()) {
			return nil, errors.New("stage worker production runtime identities do not share one WorkerInstance member")
		}
	}
	return identities, nil
}

func membersForRuntimeIdentity(
	members []*velav1.StageAuthorityMemberEpoch,
	identity *velav1.ModelRuntimeIdentity,
) []*velav1.StageAuthorityMemberEpoch {
	for _, member := range members {
		if member == nil || member.GetWorkerMemberId() != identity.GetWorkerMemberId() ||
			member.GetMemberEpoch() != identity.GetWorkerMemberEpoch() {
			continue
		}
		local := proto.Clone(member).(*velav1.StageAuthorityMemberEpoch)
		local.ModelRuntimeEpoch = identity.GetModelRuntimeEpoch()
		return []*velav1.StageAuthorityMemberEpoch{local}
	}
	return nil
}

func (agent *ProductionAgent) primaryRuntimeIdentity() *velav1.ModelRuntimeIdentity {
	if agent == nil || len(agent.runtimeIdentities) == 0 {
		return nil
	}
	return agent.runtimeIdentities[0]
}

func aggregateReattachReceipt(status AggregateStatus) (string, []byte, error) {
	if len(status.LocalReceipts) == 0 {
		return "", nil, nil
	}
	var receiptID string
	var receiptDigest []byte
	for _, receipt := range status.LocalReceipts {
		if receipt.ID == "" || len(receipt.Digest) != 32 {
			return "", nil, errors.New("ModelRuntime reattach receipt is invalid")
		}
		if receiptID == "" {
			receiptID = receipt.ID
			receiptDigest = append([]byte(nil), receipt.Digest...)
			continue
		}
		if receipt.ID != receiptID || !slices.Equal(receipt.Digest, receiptDigest) {
			return "", nil, errors.New("ModelRuntime members disagree on reattach receipt")
		}
	}
	if len(status.LocalReceipts) != status.ReportingMembers {
		return "", nil, errors.New("ModelRuntime reattach receipt is missing from a required member")
	}
	return receiptID, receiptDigest, nil
}

func nextBackoff(current, maximum time.Duration) time.Duration {
	if current >= maximum || current > maximum/2 {
		return maximum
	}
	return current * 2
}

func validateProductionRuntimeIdentity(identity *velav1.ModelRuntimeIdentity) error {
	if identity == nil || uuid.Validate(identity.GetWorkerInstanceId()) != nil ||
		uuid.Validate(identity.GetModelResidencyId()) != nil ||
		uuid.Validate(identity.GetStageProfileRevisionId()) != nil ||
		uuid.Validate(identity.GetWorkerMemberId()) != nil ||
		identity.GetWorkerInstanceEpoch() <= 0 || identity.GetModelRuntimeEpoch() <= 0 ||
		identity.GetWorkerMemberEpoch() <= 0 || strings.TrimSpace(identity.GetRuntimeIdentity()) == "" ||
		len(identity.GetDeviceSetDigest()) != 32 || len(identity.GetMembershipDigest()) != 32 {
		return errors.New("stage worker production runtime identity is incomplete")
	}
	return nil
}

func validateProductionTopology(
	identity *velav1.ModelRuntimeIdentity,
	devices []*velav1.StageAuthorityDeviceEpoch,
	members []*velav1.StageAuthorityMemberEpoch,
) error {
	if len(devices) == 0 || len(devices) > 64 || len(members) == 0 || len(members) > 64 {
		return errors.New("stage worker production topology is incomplete")
	}
	seenDevices := make(map[string]struct{}, len(devices))
	for _, device := range devices {
		if device == nil || uuid.Validate(device.GetDeviceId()) != nil || device.GetDeviceEpoch() <= 0 {
			return errors.New("stage worker production device evidence is invalid")
		}
		if _, duplicate := seenDevices[device.GetDeviceId()]; duplicate {
			return errors.New("stage worker production device evidence is duplicated")
		}
		seenDevices[device.GetDeviceId()] = struct{}{}
	}
	seenMembers := make(map[string]struct{}, len(members))
	identityMember := false
	for _, member := range members {
		if member == nil || uuid.Validate(member.GetWorkerMemberId()) != nil ||
			member.GetMemberEpoch() <= 0 || member.GetModelRuntimeEpoch() != identity.GetModelRuntimeEpoch() {
			return errors.New("stage worker production member evidence is invalid")
		}
		if _, duplicate := seenMembers[member.GetWorkerMemberId()]; duplicate {
			return errors.New("stage worker production member evidence is duplicated")
		}
		seenMembers[member.GetWorkerMemberId()] = struct{}{}
		identityMember = identityMember ||
			(member.GetWorkerMemberId() == identity.GetWorkerMemberId() &&
				member.GetMemberEpoch() == identity.GetWorkerMemberEpoch())
	}
	if !identityMember {
		return errors.New("stage worker production topology omits the local WorkerMember")
	}
	return nil
}

func validateProductionCapacity(vector map[string]int64) error {
	if len(vector) == 0 || len(vector) > 100 {
		return errors.New("stage worker production capacity vector is invalid")
	}
	for key, value := range vector {
		if strings.TrimSpace(key) == "" || strings.TrimSpace(key) != key || len(key) > 100 || value < 0 {
			return errors.New("stage worker production capacity vector is invalid")
		}
	}
	return nil
}

func cloneDevices(values []*velav1.StageAuthorityDeviceEpoch) []*velav1.StageAuthorityDeviceEpoch {
	cloned := make([]*velav1.StageAuthorityDeviceEpoch, 0, len(values))
	for _, value := range values {
		if value == nil {
			cloned = append(cloned, nil)
			continue
		}
		cloned = append(cloned, proto.Clone(value).(*velav1.StageAuthorityDeviceEpoch))
	}
	return cloned
}

func cloneMembers(values []*velav1.StageAuthorityMemberEpoch) []*velav1.StageAuthorityMemberEpoch {
	cloned := make([]*velav1.StageAuthorityMemberEpoch, 0, len(values))
	for _, value := range values {
		if value == nil {
			cloned = append(cloned, nil)
			continue
		}
		cloned = append(cloned, proto.Clone(value).(*velav1.StageAuthorityMemberEpoch))
	}
	return cloned
}
