package nodeagent

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/remediation"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/grpc"
)

type ExecutionSource interface {
	OperationReader
	CompletionWriter
	ExecutionClaimer
	ListExecuting(context.Context, int) ([]remediation.Operation, error)
	Recover(context.Context, remediation.Recovery) (remediation.Result, error)
}

type AgentEndpoint struct {
	Address        string    `json:"address"`
	ServerName     string    `json:"server_name"`
	WorkerID       uuid.UUID `json:"worker_id"`
	WorkerEpoch    int64     `json:"worker_epoch"`
	SPIFFEIdentity string    `json:"spiffe_identity"`
}

type ClientTLSConfig struct {
	CertificatePath string
	PrivateKeyPath  string
	RootCAPath      string
}

type StaticAgentResolver struct {
	endpoints   map[string]AgentEndpoint
	tls         ClientTLSConfig
	actor       string
	mu          sync.Mutex
	clients     map[string]*Client
	connections map[string]*grpc.ClientConn
}

type AgentResolver interface {
	Resolve(context.Context, NodeAgentIdentity) (*Client, error)
}

func NewStaticAgentResolver(endpoints map[string]AgentEndpoint, tlsConfig ClientTLSConfig, actorIdentity string) (*StaticAgentResolver, error) {
	if len(endpoints) == 0 {
		return nil, errors.New("at least one Node Agent endpoint is required")
	}
	if !validText(actorIdentity, maxIdentityText) {
		return nil, errors.New("node Agent resolver actor identity is invalid")
	}
	if tlsConfig.CertificatePath == "" || tlsConfig.PrivateKeyPath == "" || tlsConfig.RootCAPath == "" {
		return nil, errors.New("node Agent client TLS files are required")
	}
	validated := make(map[string]AgentEndpoint, len(endpoints))
	for nodeIdentity, endpoint := range endpoints {
		if !validText(nodeIdentity, maxIdentityText) || !validAgentEndpoint(nodeIdentity, endpoint) {
			return nil, errors.New("node Agent endpoint is invalid")
		}
		validated[nodeIdentity] = endpoint
	}
	return &StaticAgentResolver{
		endpoints: validated, tls: tlsConfig, actor: actorIdentity,
		clients: make(map[string]*Client), connections: make(map[string]*grpc.ClientConn),
	}, nil
}

func (resolver *StaticAgentResolver) Resolve(ctx context.Context, identity NodeAgentIdentity) (*Client, error) {
	if resolver == nil {
		return nil, errors.New("node Agent resolver is not configured")
	}
	if ctx == nil {
		return nil, errors.New("node Agent resolver context is required")
	}
	if !validIdentity(identity) {
		return nil, errors.New("node Agent resolver target identity is invalid")
	}
	endpoint, ok := resolver.endpoints[identity.NodeIdentity]
	if !ok || endpoint.WorkerID != identity.WorkerID {
		return nil, fmt.Errorf("node Agent endpoint for %q is not registered", identity.NodeIdentity)
	}
	if endpoint.WorkerEpoch != identity.WorkerEpoch {
		return nil, fmt.Errorf("node Agent endpoint for %q has a stale Worker epoch", identity.NodeIdentity)
	}
	resolver.mu.Lock()
	defer resolver.mu.Unlock()
	if client := resolver.clients[identity.NodeIdentity]; client != nil {
		return client, nil
	}
	transportCredentials, err := NewClientTLSCredentials(
		resolver.tls.CertificatePath, resolver.tls.PrivateKeyPath,
		resolver.tls.RootCAPath, endpoint.ServerName, endpoint.SPIFFEIdentity, resolver.actor,
	)
	if err != nil {
		return nil, err
	}
	connection, err := grpc.NewClient(endpoint.Address, grpc.WithTransportCredentials(transportCredentials))
	if err != nil {
		return nil, fmt.Errorf("dial Node Agent %q: %w", identity.NodeIdentity, err)
	}
	client, err := NewClient(velav1.NewNodeAgentServiceClient(connection), resolver.actor)
	if err != nil {
		_ = connection.Close()
		return nil, err
	}
	resolver.connections[identity.NodeIdentity] = connection
	resolver.clients[identity.NodeIdentity] = client
	return client, nil
}

func (resolver *StaticAgentResolver) Close() error {
	if resolver == nil {
		return nil
	}
	resolver.mu.Lock()
	defer resolver.mu.Unlock()
	var firstErr error
	for nodeIdentity, connection := range resolver.connections {
		if err := connection.Close(); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("close Node Agent %q connection: %w", nodeIdentity, err)
		}
	}
	resolver.connections = make(map[string]*grpc.ClientConn)
	resolver.clients = make(map[string]*Client)
	return firstErr
}

type DispatchResult struct {
	Listed     int
	Dispatched int
	Recovered  int
	Deferred   int
}

type ExecutionDispatcher struct {
	source        ExecutionSource
	agents        AgentResolver
	actorIdentity string
	batchSize     int
	clock         func() time.Time
}

var executionClaimNamespace = uuid.MustParse("6429aa4f-e3e4-45d8-bae8-b573438da85f")

func NewExecutionDispatcher(source ExecutionSource, agents AgentResolver, actorIdentity string, batchSize int) (*ExecutionDispatcher, error) {
	if source == nil || agents == nil {
		return nil, errors.New("remediation execution dispatcher dependencies are required")
	}
	if !validText(actorIdentity, maxIdentityText) || batchSize < 1 || batchSize > 1000 {
		return nil, errors.New("remediation execution dispatcher configuration is invalid")
	}
	return &ExecutionDispatcher{source: source, agents: agents, actorIdentity: actorIdentity, batchSize: batchSize, clock: time.Now}, nil
}

func (dispatcher *ExecutionDispatcher) RunOnce(ctx context.Context) (DispatchResult, error) {
	if dispatcher == nil || dispatcher.source == nil || dispatcher.agents == nil {
		return DispatchResult{}, errors.New("remediation execution dispatcher is not configured")
	}
	operations, err := dispatcher.source.ListExecuting(ctx, dispatcher.batchSize)
	if err != nil {
		return DispatchResult{}, err
	}
	result := DispatchResult{Listed: len(operations)}
	authorizer, err := NewControlPlaneAuthorizer(dispatcher.source, dispatcher.source, dispatcher.actorIdentity)
	if err != nil {
		return result, err
	}
	ledger, err := NewControlPlaneLedger(dispatcher.source, dispatcher.source, dispatcher.actorIdentity)
	if err != nil {
		return result, err
	}
	for _, operation := range operations {
		if !dispatcher.clock().Before(operation.DeadlineAt) {
			if _, recoverErr := dispatcher.source.Recover(ctx, remediation.Recovery{OperationID: operation.ID, ActorIdentity: dispatcher.actorIdentity}); recoverErr != nil {
				result.Deferred++
				continue
			}
			result.Recovered++
			continue
		}
		client, resolveErr := dispatcher.agents.Resolve(ctx, NodeAgentIdentity{
			NodeIdentity: operation.NodeIdentity,
			WorkerID:     operation.WorkerID,
			WorkerEpoch:  operation.WorkerEpoch,
		})
		if resolveErr != nil {
			result.Deferred++
			continue
		}
		remote, remoteErr := NewRemoteExecutor(client, authorizer, ledger, dispatcher.actorIdentity)
		if remoteErr != nil {
			return result, remoteErr
		}
		_, executeErr := remote.Execute(ctx, remediation.Plan{
			OperationID:      operation.ID,
			ExecutionClaimID: stableExecutionClaimID(operation.ID, dispatcher.actorIdentity),
			WorkerID:         operation.WorkerID,
			WorkerEpoch:      operation.WorkerEpoch, DeadlineAt: operation.DeadlineAt,
			NodeIdentity: operation.NodeIdentity, DeviceIdentity: operation.DeviceIdentity,
			FailureClass: operation.FailureClass,
			ActionLevel:  operation.ActionLevel, CertificationRevision: operation.CertificationRevision,
			FailureEvidenceDigest: append([]byte(nil), operation.EvidenceDigest...),
		})
		if executeErr != nil {
			result.Deferred++
			continue
		}
		result.Dispatched++
	}
	return result, nil
}

func stableExecutionClaimID(operationID uuid.UUID, actorIdentity string) uuid.UUID {
	return uuid.NewSHA1(executionClaimNamespace, []byte(operationID.String()+"/"+actorIdentity))
}

func (dispatcher *ExecutionDispatcher) Run(ctx context.Context, tick time.Duration, report func(DispatchResult, error)) {
	if tick <= 0 {
		return
	}
	ticker := time.NewTicker(tick)
	defer ticker.Stop()
	for {
		result, err := dispatcher.RunOnce(ctx)
		if report != nil {
			report(result, err)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func validAgentEndpoint(nodeIdentity string, endpoint AgentEndpoint) bool {
	if !validText(endpoint.ServerName, maxIdentityText) {
		return false
	}
	identity := NodeAgentIdentity{
		NodeIdentity: nodeIdentity,
		WorkerID:     endpoint.WorkerID,
		WorkerEpoch:  endpoint.WorkerEpoch,
	}
	if !validIdentity(identity) || endpoint.SPIFFEIdentity != NodeAgentSPIFFEIdentity(identity) {
		return false
	}
	host, port, err := net.SplitHostPort(endpoint.Address)
	if err != nil || host == "" || port == "" || strings.TrimSpace(endpoint.Address) != endpoint.Address {
		return false
	}
	return true
}

var _ ExecutionSource = (*remediation.Service)(nil)
var _ AgentResolver = (*StaticAgentResolver)(nil)
