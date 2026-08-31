package main

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/vivym/vela/internal/artifactstore"
	"github.com/vivym/vela/internal/materializationauthority"
	"github.com/vivym/vela/internal/modelruntimetransport"
	"github.com/vivym/vela/internal/securefile"
	"github.com/vivym/vela/internal/stageartifact"
	"github.com/vivym/vela/internal/stageauthority"
	"github.com/vivym/vela/internal/stageworkeragent"
	"github.com/vivym/vela/internal/stageworkertransport"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
)

const maxArtifactRootCABytes = 4 << 20

type stageWorkerRuntime interface {
	Run(context.Context) error
	Close() error
}

type stageWorkerRuntimeBuilder func(context.Context, config) (stageWorkerRuntime, error)

type productionRuntime struct {
	agent        *stageworkeragent.ProductionAgent
	inputJournal *stageworkeragent.FileInputTransferJournal
	control      *stageworkertransport.Client
	modelRuntime *modelruntimetransport.Client
	state        *stageworkeragent.FileProductionState
}

func runWithContext(ctx context.Context, configuration config) error {
	return runWithContextUsing(ctx, configuration, newProductionRuntime)
}

func runWithContextUsing(
	ctx context.Context,
	configuration config,
	builder stageWorkerRuntimeBuilder,
) error {
	if ctx == nil || builder == nil {
		return errors.New("Stage Worker run context and builder are required")
	}
	runtime, err := builder(ctx, configuration)
	if err != nil {
		return err
	}
	if runtime == nil {
		return errors.New("Stage Worker runtime builder returned no runtime")
	}
	runErr := runtime.Run(ctx)
	closeErr := runtime.Close()
	return errors.Join(runErr, closeErr)
}

func newProductionRuntime(ctx context.Context, configuration config) (stageWorkerRuntime, error) {
	if ctx == nil {
		return nil, errors.New("Stage Worker production context is required")
	}
	if err := ensureStageWorkerDirectories(configuration); err != nil {
		return nil, err
	}
	keyring, err := readAuthorityKeyring(configuration.authorityKeyringFile)
	if err != nil {
		return nil, err
	}
	defer clearAuthorityKeyring(keyring)
	if _, ok := keyring[configuration.authorityActiveKeyID]; !ok {
		return nil, errors.New("Stage Worker active authority key is absent from keyring")
	}
	materializationValidator, err := materializationauthority.NewValidator(keyring, time.Now)
	if err != nil {
		return nil, fmt.Errorf("configure MaterializationAuthority validator: %w", err)
	}
	transferTicketSigner, err := stageartifact.NewTransferTicketKeyringSigner(
		configuration.authorityActiveKeyID,
		keyring,
	)
	if err != nil {
		return nil, fmt.Errorf("configure TransferTicket verifier: %w", err)
	}
	accessKeyID, err := readSecretText(
		configuration.artifactS3AccessKeyFile,
		"Stage Worker Artifact Store access key id",
	)
	if err != nil {
		return nil, err
	}
	secretAccessKey, err := readSecretText(
		configuration.artifactS3SecretKeyFile,
		"Stage Worker Artifact Store secret access key",
	)
	if err != nil {
		return nil, err
	}
	rootCAPEM, err := securefile.Read(
		configuration.artifactS3CAFile,
		maxArtifactRootCABytes,
		false,
	)
	if err != nil {
		return nil, fmt.Errorf("read Stage Worker Artifact Store root CA: %w", err)
	}
	defer clear(rootCAPEM)
	store, err := artifactstore.NewS3(artifactstore.S3Config{
		Endpoint:        configuration.artifactS3Endpoint,
		Region:          configuration.artifactS3Region,
		Bucket:          configuration.artifactS3Bucket,
		AccessKeyID:     accessKeyID,
		SecretAccessKey: secretAccessKey,
		UsePathStyle:    configuration.artifactS3PathStyle,
		SignedGETTTL:    configuration.artifactSignedGETTTL,
		RootCAPEM:       rootCAPEM,
	})
	if err != nil {
		return nil, fmt.Errorf("configure Stage Worker Artifact Store: %w", err)
	}
	if err := store.ValidateBucket(ctx); err != nil {
		return nil, fmt.Errorf("validate Stage Worker Artifact Store: %w", err)
	}

	runtime := &productionRuntime{}
	fail := func(cause error) (stageWorkerRuntime, error) {
		return nil, errors.Join(cause, runtime.Close())
	}
	runtime.state, err = stageworkeragent.NewFileProductionState(
		stageworkeragent.FileProductionStateConfig{
			Directory:           configuration.productionStateRoot,
			WorkerInstanceID:    configuration.workerInstanceID,
			WorkerInstanceEpoch: configuration.workerInstanceEpoch,
			WorkerMemberID:      configuration.workerMemberID,
		},
	)
	if err != nil {
		return fail(fmt.Errorf("configure Stage Worker production state: %w", err))
	}
	transportCredentials, err := stageworkertransport.NewClientTLSCredentials(
		configuration.tlsCertificateFile,
		configuration.tlsPrivateKeyFile,
		configuration.controlCAFile,
		configuration.controlServerName,
	)
	if err != nil {
		return fail(fmt.Errorf("configure Stage Worker control mTLS: %w", err))
	}
	runtime.modelRuntime, err = modelruntimetransport.Dial(
		ctx,
		modelruntimetransport.Config{
			SocketPath:  configuration.runtimeSocket,
			ExpectedUID: configuration.runtimeExpectedUID,
		},
	)
	if err != nil {
		return fail(fmt.Errorf("connect to resident ModelRuntime: %w", err))
	}
	runtimeIdentity, err := stageworkeragent.DiscoverRuntimeIdentity(
		ctx,
		runtime.modelRuntime,
		stageworkeragent.RuntimeIdentityExpectation{
			WorkerInstanceID:    configuration.workerInstanceID.String(),
			WorkerInstanceEpoch: configuration.workerInstanceEpoch,
			WorkerMemberID:      configuration.workerMemberID.String(),
			WorkerMemberEpoch:   configuration.workerMemberEpoch,
		},
	)
	if err != nil {
		return fail(err)
	}
	runtime.control, err = stageworkertransport.DialClient(
		ctx,
		stageworkertransport.ClientConfig{
			Address:                   configuration.controlAddress,
			TransportCredentials:      transportCredentials,
			ControlSessionEpochSource: runtime.state,
		},
	)
	if err != nil {
		return fail(err)
	}
	runtimeAgent, err := stageworkeragent.New(stageworkeragent.Config{
		Members: []stageworkeragent.RuntimeMember{{
			ID:     configuration.workerMemberID.String(),
			Client: runtime.modelRuntime,
		}},
	})
	if err != nil {
		return fail(err)
	}
	materializationJournal, err := stageworkeragent.NewFileMaterializationJournal(
		configuration.materializationJournalRoot,
		configuration.materializationJournalLimit,
	)
	if err != nil {
		return fail(fmt.Errorf("configure StageArtifact materialization journal: %w", err))
	}
	runtime.inputJournal, err = stageworkeragent.NewFileInputTransferJournal(
		configuration.inputTransferJournalRoot,
	)
	if err != nil {
		return fail(fmt.Errorf("configure Stage input transfer journal: %w", err))
	}
	outputSource, err := stageartifact.NewFilesystemLocalOutputSource(configuration.outputRoot)
	if err != nil {
		return fail(err)
	}
	publisher, err := stageartifact.NewObjectStorePublisher(store, time.Now)
	if err != nil {
		return fail(err)
	}
	inputResolver, err := stageworkeragent.NewAssignmentInputResolver(
		stageworkeragent.AssignmentInputResolverConfig{
			Store:               store,
			TicketSigner:        transferTicketSigner,
			Control:             runtime.control,
			InputRoot:           configuration.inputRoot,
			ConnectorRevisionID: configuration.connectorRevisionID,
			Now:                 time.Now,
			Journal:             runtime.inputJournal,
		},
	)
	if err != nil {
		return fail(err)
	}
	stream, err := stageworkeragent.NewInputResolvingMaterializingStreamAgent(
		runtimeAgent,
		runtime.control,
		stageworkeragent.MaterializationConfig{
			Validator:          materializationValidator,
			Source:             outputSource,
			Publisher:          publisher,
			Journal:            materializationJournal,
			SourceLossEvidence: sourceLossEvidenceProvider(configuration, time.Now),
		},
		inputResolver,
	)
	if err != nil {
		return fail(err)
	}
	runtime.agent, err = stageworkeragent.NewProductionAgent(stageworkeragent.ProductionConfig{
		Control:         runtime.control,
		Runtime:         runtime.modelRuntime,
		Stream:          stream,
		RuntimeIdentity: runtimeIdentity,
		Devices:         configuration.devices,
		Members: []*velav1.StageAuthorityMemberEpoch{{
			WorkerMemberId:    configuration.workerMemberID.String(),
			MemberEpoch:       configuration.workerMemberEpoch,
			ModelRuntimeEpoch: runtimeIdentity.GetModelRuntimeEpoch(),
		}},
		CapacityVector:            configuration.capacityVector,
		CapacityTTL:               configuration.capacityTTL,
		HeartbeatInterval:         configuration.heartbeatInterval,
		RetryMinimum:              configuration.retryMinimum,
		RetryMaximum:              configuration.retryMaximum,
		ObservationSequenceSource: runtime.state,
		Now:                       time.Now,
		Wait:                      waitContext,
	})
	if err != nil {
		return fail(err)
	}
	return runtime, nil
}

func (runtime *productionRuntime) Run(ctx context.Context) error {
	if runtime == nil || runtime.agent == nil {
		return errors.New("Stage Worker production runtime is incomplete")
	}
	return runtime.agent.Run(ctx)
}

func (runtime *productionRuntime) Close() error {
	if runtime == nil {
		return nil
	}
	var closeErr error
	if runtime.inputJournal != nil {
		closeErr = errors.Join(closeErr, runtime.inputJournal.Close())
		runtime.inputJournal = nil
	}
	if runtime.control != nil {
		closeErr = errors.Join(closeErr, runtime.control.Close())
		runtime.control = nil
	}
	if runtime.modelRuntime != nil {
		closeErr = errors.Join(closeErr, runtime.modelRuntime.Close())
		runtime.modelRuntime = nil
	}
	if runtime.state != nil {
		closeErr = errors.Join(closeErr, runtime.state.Close())
		runtime.state = nil
	}
	return closeErr
}

func ensureStageWorkerDirectories(configuration config) error {
	paths := []string{
		configuration.scratchRoot,
		configuration.productionStateRoot,
		configuration.inputRoot,
		configuration.inputTransferJournalRoot,
		configuration.outputRoot,
		configuration.materializationJournalRoot,
	}
	for _, path := range paths {
		if err := os.MkdirAll(path, 0o700); err != nil {
			return fmt.Errorf("create Stage Worker private directory %s: %w", path, err)
		}
		if err := securefile.ValidateDirectory(path); err != nil {
			return fmt.Errorf("validate Stage Worker private directory %s: %w", path, err)
		}
	}
	return nil
}

func sourceLossEvidenceProvider(
	configuration config,
	now func() time.Time,
) stageworkeragent.MaterializationSourceLossEvidenceProvider {
	return stageworkeragent.MaterializationSourceLossEvidenceFunc(func(
		ctx context.Context,
		record stageworkeragent.PendingMaterialization,
	) (stageworkeragent.MaterializationSourceLossEvidence, error) {
		if ctx == nil || now == nil {
			return stageworkeragent.MaterializationSourceLossEvidence{},
				errors.New("Stage Worker source-loss evidence context is invalid")
		}
		if err := ctx.Err(); err != nil {
			return stageworkeragent.MaterializationSourceLossEvidence{}, err
		}
		authorityDigest, err := stageauthority.Digest(record.StageAuthority)
		if err != nil || record.LocalReceipt == nil ||
			len(record.LocalReceipt.GetManifestSha256()) != sha256.Size {
			return stageworkeragent.MaterializationSourceLossEvidence{},
				errors.New("Stage Worker source-loss evidence record is invalid")
		}
		digest := sha256.New()
		_, _ = digest.Write([]byte("vela/stage-worker/materialization-source-loss/v1\x00"))
		_, _ = digest.Write(authorityDigest[:])
		_, _ = digest.Write(record.LocalReceipt.GetManifestSha256())
		var fingerprint [sha256.Size]byte
		copy(fingerprint[:], digest.Sum(nil))
		lostAt := now().UTC()
		return stageworkeragent.MaterializationSourceLossEvidence{
			FailureFingerprint:    fingerprint,
			ConsumedResourceUnits: configuration.sourceLossConsumedResourceUnits,
			LostAt:                lostAt,
			RetryAt:               lostAt.Add(configuration.sourceLossRetry),
		}, nil
	})
}

func waitContext(ctx context.Context, duration time.Duration) error {
	if ctx == nil || duration <= 0 {
		return errors.New("Stage Worker wait is invalid")
	}
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
