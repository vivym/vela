package main

import (
	"encoding/base64"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/vivym/vela/internal/materializationauthority"
	"github.com/vivym/vela/internal/stageartifact"
	"github.com/vivym/vela/internal/stageauthority"
	"github.com/vivym/vela/internal/stageworkercontrol"
	"github.com/vivym/vela/internal/stageworkertransport"
)

const (
	defaultStageWorkerNoWorkRetry        = 250 * time.Millisecond
	defaultStageWorkerMemberStartTimeout = 30 * time.Second
	defaultStageWorkerTransferTicketTTL  = 30 * time.Second
	defaultStageMaterializationTTL       = 15 * time.Minute
	defaultStageWorkerStopPoll           = 250 * time.Millisecond
	defaultStageWorkerMaxClockSkew       = 30 * time.Second
)

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
	handler, err := stageworkercontrol.NewHandler(stageworkercontrol.Config{
		Validator:                 stageValidator,
		Authorizer:                authorizer,
		MaterializationValidator:  materializationValidator,
		MaterializationAuthorizer: artifactRepository,
		Executor:                  executor,
	})
	if err != nil {
		return nil, err
	}
	stopSource, err := stageworkercontrol.NewAuthorityStopSource(
		stageValidator,
		authorizer,
		stageworkercontrol.AuthorityStopSourceConfig{
			PollInterval: defaultStageWorkerStopPoll,
			Now:          time.Now,
		},
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

func readStageWorkerIdentityKey(path string) ([]byte, error) {
	encoded, err := readSecretFile(path, "Stage Worker assignment identity key", 4096)
	if err != nil {
		return nil, err
	}
	key, err := base64.StdEncoding.Strict().DecodeString(strings.TrimSpace(encoded))
	if err != nil || len(key) < 32 || len(key) > 64 {
		clear(key)
		return nil, errors.New(
			"Stage Worker assignment identity key must encode 32 to 64 bytes",
		)
	}
	return key, nil
}
