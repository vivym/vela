package main

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	veladb "github.com/vivym/vela/internal/database"
	"github.com/vivym/vela/internal/materializationauthority"
	"github.com/vivym/vela/internal/stageartifact"
	"github.com/vivym/vela/internal/stageauthority"
	"github.com/vivym/vela/internal/stageworkercontrol"
	"github.com/vivym/vela/internal/stageworkertransport"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/grpc"
)

const (
	defaultStageWorkerNoWorkRetry        = 5 * time.Second
	defaultStageWorkerMemberStartTimeout = 30 * time.Second
	defaultStageWorkerTransferTicketTTL  = 30 * time.Second
	defaultStageMaterializationTTL       = 15 * time.Minute
	defaultStageWorkerStopPoll           = 250 * time.Millisecond
	defaultStageWorkerMaxClockSkew       = 30 * time.Second
)

type stageWorkerGRPCServer interface {
	Serve(net.Listener) error
	GracefulStop()
	Stop()
}

type stageWorkerControlLifecycle struct {
	address     string
	pool        *pgxpool.Pool
	server      stageWorkerGRPCServer
	listener    net.Listener
	serveErrors chan error
	startOnce   sync.Once
	cleanupOnce sync.Once
	serverMu    sync.Mutex
	serverEnded bool
}

func newStageWorkerControlLifecycle(
	ctx context.Context,
	configuration config,
	artifactPool *pgxpool.Pool,
	scheduling stageworkercontrol.AssignmentScheduler,
	stageAttempts stageworkercontrol.StageAttemptOperations,
) (*stageWorkerControlLifecycle, error) {
	controlPool, err := openPool(
		ctx,
		configuration.stageWorkerControlDatabaseURL,
		10,
		veladb.RoleStageWorkerControl,
	)
	if err != nil {
		return nil, fmt.Errorf("open StageWorkerControl database pool: %w", err)
	}
	closePool := true
	defer func() {
		if closePool {
			controlPool.Close()
		}
	}()
	adapter, err := newStageWorkerControlAdapter(
		configuration,
		controlPool,
		artifactPool,
		scheduling,
		stageAttempts,
	)
	if err != nil {
		return nil, fmt.Errorf("configure StageWorkerControl: %w", err)
	}
	transportCredentials, err := stageworkertransport.NewServerTLSCredentials(
		configuration.stageWorkerControlTLSCertFile,
		configuration.stageWorkerControlTLSKeyFile,
		configuration.stageWorkerControlClientCAFile,
	)
	if err != nil {
		return nil, err
	}
	server := grpc.NewServer(
		grpc.Creds(transportCredentials),
		grpc.MaxRecvMsgSize(4<<20),
		grpc.MaxSendMsgSize(4<<20),
	)
	velav1.RegisterStageWorkerControlServiceServer(server, adapter)
	listener, err := net.Listen("tcp", configuration.stageWorkerControlAddress)
	if err != nil {
		return nil, fmt.Errorf("listen for StageWorkerControl gRPC: %w", err)
	}
	closePool = false
	return &stageWorkerControlLifecycle{
		address: configuration.stageWorkerControlAddress, pool: controlPool,
		server: server, listener: listener, serveErrors: make(chan error, 1),
	}, nil
}

func (lifecycle *stageWorkerControlLifecycle) Start() <-chan error {
	if lifecycle == nil {
		closed := make(chan error)
		close(closed)
		return closed
	}
	lifecycle.startOnce.Do(func() {
		go func() {
			slog.Info(
				"vela-control StageWorkerControl gRPC server started",
				"address",
				lifecycle.address,
			)
			lifecycle.serveErrors <- lifecycle.server.Serve(lifecycle.listener)
			close(lifecycle.serveErrors)
		}()
	})
	return lifecycle.serveErrors
}

func (lifecycle *stageWorkerControlLifecycle) Shutdown(ctx context.Context) error {
	if lifecycle == nil || lifecycle.server == nil || lifecycle.listener == nil || ctx == nil {
		return errors.New("StageWorkerControl lifecycle is not configured")
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		lifecycle.server.GracefulStop()
		lifecycle.markServerEnded()
	}()
	select {
	case <-done:
		lifecycle.cleanup()
		return nil
	case <-ctx.Done():
		lifecycle.stopServer()
		lifecycle.cleanup()
		return errors.New("StageWorkerControl gRPC server did not stop before shutdown deadline")
	}
}

func (lifecycle *stageWorkerControlLifecycle) Close() {
	if lifecycle == nil {
		return
	}
	lifecycle.stopServer()
	lifecycle.cleanup()
}

func (lifecycle *stageWorkerControlLifecycle) stopServer() {
	if lifecycle.server == nil {
		return
	}
	lifecycle.serverMu.Lock()
	defer lifecycle.serverMu.Unlock()
	if !lifecycle.serverEnded {
		lifecycle.server.Stop()
		lifecycle.serverEnded = true
	}
}

func (lifecycle *stageWorkerControlLifecycle) markServerEnded() {
	lifecycle.serverMu.Lock()
	lifecycle.serverEnded = true
	lifecycle.serverMu.Unlock()
}

func (lifecycle *stageWorkerControlLifecycle) cleanup() {
	lifecycle.cleanupOnce.Do(func() {
		if lifecycle.listener != nil {
			_ = lifecycle.listener.Close()
		}
		if lifecycle.pool != nil {
			lifecycle.pool.Close()
		}
	})
}

func newStageWorkerControlAdapter(
	configuration config,
	controlPool *pgxpool.Pool,
	artifactPool *pgxpool.Pool,
	scheduling stageworkercontrol.AssignmentScheduler,
	stageAttempts stageworkercontrol.StageAttemptOperations,
) (*stageworkertransport.Server, error) {
	keyring, err := readLeaseKeyring(configuration.leaseKeyringFile)
	if err != nil {
		return nil, err
	}
	defer clearKeyring(keyring)
	if _, ok := keyring[configuration.leaseActiveKeyID]; !ok {
		return nil, errors.New("StageWorkerControl active signing key is absent from Lease keyring")
	}
	identityKey, err := readStageWorkerIdentityKey(configuration.stageWorkerIdentityKeyFile)
	if err != nil {
		return nil, err
	}
	defer clear(identityKey)

	stageSigner, err := stageauthority.NewSigner(keyring)
	if err != nil {
		return nil, fmt.Errorf("configure StageAuthority signer: %w", err)
	}
	stageValidator, err := stageauthority.NewValidator(keyring, time.Now)
	if err != nil {
		return nil, fmt.Errorf("configure StageAuthority validator: %w", err)
	}
	materializationSigner, err := materializationauthority.NewSigner(keyring)
	if err != nil {
		return nil, fmt.Errorf("configure MaterializationAuthority signer: %w", err)
	}
	materializationValidator, err := materializationauthority.NewValidator(keyring, time.Now)
	if err != nil {
		return nil, fmt.Errorf("configure MaterializationAuthority validator: %w", err)
	}
	transferSigner, err := stageartifact.NewTransferTicketKeyringSigner(
		configuration.leaseActiveKeyID,
		keyring,
	)
	if err != nil {
		return nil, fmt.Errorf("configure TransferTicket signer: %w", err)
	}
	artifactRepository, err := stageartifact.NewPostgresRepository(artifactPool)
	if err != nil {
		return nil, err
	}
	transferIssuer, err := stageartifact.NewTransferTicketIssuer(
		artifactRepository,
		transferSigner,
	)
	if err != nil {
		return nil, err
	}
	materializationIssuer, err := stageartifact.NewMaterializationAuthorityIssuer(
		artifactRepository,
		materializationSigner,
		defaultStageMaterializationTTL,
	)
	if err != nil {
		return nil, err
	}
	workerEvidence, err := stageworkercontrol.NewPostgresWorkerEvidenceBackend(controlPool)
	if err != nil {
		return nil, err
	}
	assignments, err := stageworkercontrol.NewPostgresAssignmentBackend(
		stageworkercontrol.PostgresAssignmentConfig{
			Pool:               controlPool,
			Scheduler:          scheduling,
			AuthoritySigner:    stageSigner,
			TransferTickets:    transferIssuer,
			IdentityKey:        identityKey,
			NoWorkRetry:        defaultStageWorkerNoWorkRetry,
			MemberStartTimeout: defaultStageWorkerMemberStartTimeout,
			TransferTicketTTL:  defaultStageWorkerTransferTicketTTL,
		},
	)
	if err != nil {
		return nil, err
	}
	execution, err := stageworkercontrol.NewPostgresExecutionBackend(
		controlPool,
		stageSigner,
		stageworkercontrol.PostgresExecutionConfig{
			ActiveSigningKeyID: configuration.leaseActiveKeyID,
			AuthorityTTL:       configuration.stageSchedulerLeaseTTL,
			LocalDeadlineTTL:   configuration.stageSchedulerLocalDeadlineTTL,
			MaxClockSkew:       defaultStageWorkerMaxClockSkew,
			Now:                time.Now,
		},
	)
	if err != nil {
		return nil, err
	}
	reattachments, err := stageworkercontrol.NewPostgresReattachmentBackend(controlPool)
	if err != nil {
		return nil, err
	}
	operations, err := stageworkercontrol.NewPostgresOperationBackend(
		stageworkercontrol.PostgresOperationConfig{
			WorkerEvidence:        workerEvidence,
			Assignments:           assignments,
			Execution:             execution,
			MaterializationIssuer: materializationIssuer,
			StageArtifacts:        artifactRepository,
			StageAttempts:         stageAttempts,
			Reattachments:         reattachments,
			Transfers:             artifactRepository,
		},
	)
	if err != nil {
		return nil, err
	}
	executor, err := stageworkercontrol.NewProductionExecutor(operations)
	if err != nil {
		return nil, err
	}
	authorizer, err := stageworkercontrol.NewPostgresAuthorizer(controlPool)
	if err != nil {
		return nil, err
	}
	handler, stopSource, err := newStageWorkerAuthorityIngress(
		stageValidator,
		authorizer,
		materializationValidator,
		artifactRepository,
		executor,
		time.Now,
	)
	if err != nil {
		return nil, err
	}
	server, err := stageworkertransport.NewServer(stageworkertransport.ServerConfig{
		Authenticator: stageworkertransport.PeerAuthenticator{},
		Handler:       handler,
		StopSource:    stopSource,
	})
	if err != nil {
		return nil, err
	}
	return server, nil
}

func newStageWorkerAuthorityIngress(
	stageValidator *stageauthority.Validator,
	authorizer stageworkercontrol.Authorizer,
	materializationValidator *materializationauthority.Validator,
	materializationAuthorizer stageworkercontrol.MaterializationAuthorizer,
	executor stageworkercontrol.Executor,
	now func() time.Time,
) (*stageworkercontrol.Handler, *stageworkercontrol.AuthorityStopSource, error) {
	handler, err := stageworkercontrol.NewHandler(stageworkercontrol.Config{
		Validator:                 stageValidator,
		Authorizer:                authorizer,
		MaterializationValidator:  materializationValidator,
		MaterializationAuthorizer: materializationAuthorizer,
		Executor:                  executor,
		MaxClockSkew:              defaultStageWorkerMaxClockSkew,
	})
	if err != nil {
		return nil, nil, err
	}
	stopSource, err := stageworkercontrol.NewAuthorityStopSource(
		stageValidator,
		authorizer,
		stageworkercontrol.AuthorityStopSourceConfig{
			PollInterval: defaultStageWorkerStopPoll,
			MaxClockSkew: defaultStageWorkerMaxClockSkew,
			Now:          now,
		},
	)
	if err != nil {
		return nil, nil, err
	}
	return handler, stopSource, nil
}

func readStageWorkerIdentityKey(path string) ([]byte, error) {
	encoded, err := readSecretFile(path, "Stage Worker assignment identity key", 4096)
	if err != nil {
		return nil, err
	}
	key, err := base64.StdEncoding.Strict().DecodeString(strings.TrimSpace(encoded))
	if err != nil || len(key) < 32 || len(key) > 64 {
		clear(key)
		return nil, errors.New(
			"stage worker assignment identity key must encode 32 to 64 bytes",
		)
	}
	return key, nil
}
