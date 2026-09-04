package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/artifactstore"
	"github.com/vivym/vela/internal/authoritypolicy"
	"github.com/vivym/vela/internal/materializationauthority"
	"github.com/vivym/vela/internal/modelruntimetransport"
	"github.com/vivym/vela/internal/securefile"
	"github.com/vivym/vela/internal/stageartifact"
	"github.com/vivym/vela/internal/stageauthority"
	"github.com/vivym/vela/internal/stageworkeragent"
	"github.com/vivym/vela/internal/stageworkermembertransport"
	"github.com/vivym/vela/internal/stageworkertransport"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/grpc"
)

const maxArtifactRootCABytes = 4 << 20

type productionAuthorityConsumers struct {
	newMemberServer func(
		stageworkermembertransport.ServerConfig,
	) (*stageworkermembertransport.Server, error)
	newInputResolver func(
		stageworkeragent.AssignmentInputResolverConfig,
	) (*stageworkeragent.AssignmentInputResolver, error)
	newMaterializingAgent func(
		*stageworkeragent.Agent,
		stageworkeragent.ControlClient,
		stageworkeragent.MaterializationConfig,
		stageworkeragent.InputResolver,
	) (*stageworkeragent.StreamAgent, error)
}

type stageWorkerRuntime interface {
	Run(context.Context) error
	Close() error
}

type stageWorkerRuntimeBuilder func(context.Context, config) (stageWorkerRuntime, error)

type productionRuntime struct {
	agent                 *stageworkeragent.ProductionAgent
	inputJournal          *stageworkeragent.FileInputTransferJournal
	control               *stageworkertransport.Client
	modelRuntime          *modelruntimetransport.Client
	memberClients         []*stageworkermembertransport.Client
	memberServer          *grpc.Server
	memberListener        net.Listener
	memberServeErrors     chan error
	memberShutdownTimeout time.Duration
	state                 *stageworkeragent.FileProductionState
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
		return errors.New("stage worker run context and builder are required")
	}
	runtime, err := builder(ctx, configuration)
	if err != nil {
		return err
	}
	if runtime == nil {
		return errors.New("stage worker runtime builder returned no runtime")
	}
	runErr := runtime.Run(ctx)
	closeErr := runtime.Close()
	return errors.Join(runErr, closeErr)
}

func newProductionRuntime(ctx context.Context, configuration config) (stageWorkerRuntime, error) {
	return newProductionRuntimeUsing(ctx, configuration, productionAuthorityConsumers{
		newMemberServer:       stageworkermembertransport.NewServer,
		newInputResolver:      stageworkeragent.NewAssignmentInputResolver,
		newMaterializingAgent: stageworkeragent.NewInputResolvingMaterializingStreamAgent,
	})
}

func newProductionRuntimeUsing(
	ctx context.Context,
	configuration config,
	consumers productionAuthorityConsumers,
) (stageWorkerRuntime, error) {
	if ctx == nil {
		return nil, errors.New("stage worker production context is required")
	}
	if consumers.newMemberServer == nil || consumers.newInputResolver == nil ||
		consumers.newMaterializingAgent == nil {
		return nil, errors.New("stage worker production authority consumers are required")
	}
	if err := ensureStageWorkerDirectories(configuration); err != nil {
		return nil, err
	}
	keyring, err := stageauthority.ReadKeyringFile(configuration.authorityKeyringFile)
	if err != nil {
		return nil, err
	}
	defer stageauthority.ClearKeyring(keyring)
	if _, ok := keyring[configuration.authorityActiveKeyID]; !ok {
		return nil, errors.New("stage worker active authority key is absent from keyring")
	}
	materializationValidator, err := materializationauthority.NewValidator(keyring, time.Now)
	if err != nil {
		return nil, fmt.Errorf("configure MaterializationAuthority validator: %w", err)
	}
	stageAuthorityValidator, err := stageauthority.NewValidator(keyring, time.Now)
	if err != nil {
		return nil, fmt.Errorf("configure StageAuthority validator: %w", err)
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
	runtimeDialContext, cancelRuntimeDial := context.WithTimeout(ctx, configuration.runtimeStartupTimeout)
	runtime.modelRuntime, err = modelruntimetransport.Dial(
		runtimeDialContext,
		modelruntimetransport.Config{
			SocketPath:  configuration.runtimeSocket,
			ExpectedUID: configuration.runtimeExpectedUID,
		},
	)
	cancelRuntimeDial()
	if err != nil {
		return fail(fmt.Errorf("connect to resident ModelRuntime: %w", err))
	}
	runtimeIdentities, err := stageworkeragent.DiscoverRuntimeIdentities(
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
	authorityMembers, memberBindings, localMember, err := configuredRuntimeMembers(configuration)
	if err != nil {
		return fail(err)
	}
	runtimeMembers := []stageworkeragent.RuntimeMember{{
		ID: configuration.workerMemberID.String(), Client: runtime.modelRuntime,
	}}
	if len(configuration.members) > 1 {
		if err := validateLocalMemberCertificateIdentity(
			configuration.memberClientCertificateFile,
			localMember.identityDigest,
		); err != nil {
			return fail(fmt.Errorf("validate Stage Worker member client identity: %w", err))
		}
		if err := validateLocalMemberCertificateIdentity(
			configuration.memberServerCertificateFile,
			localMember.identityDigest,
		); err != nil {
			return fail(fmt.Errorf("validate Stage Worker member server identity: %w", err))
		}
		serverCredentials, credentialsErr := stageworkertransport.NewServerTLSCredentials(
			configuration.memberServerCertificateFile,
			configuration.memberServerPrivateKeyFile,
			configuration.memberClientCAFile,
		)
		if credentialsErr != nil {
			return fail(fmt.Errorf("configure Stage Worker member server mTLS: %w", credentialsErr))
		}
		memberService, serviceErr := consumers.newMemberServer(
			stageworkermembertransport.ServerConfig{
				Authenticator:   stageworkertransport.PeerAuthenticator{},
				Validator:       stageAuthorityValidator,
				Runtime:         runtime.modelRuntime,
				LocalIdentities: runtimeIdentities,
				Members:         memberBindings,
				MaxClockSkew:    authoritypolicy.ProductionMaxClockSkew,
			},
		)
		if serviceErr != nil {
			return fail(fmt.Errorf("configure Stage Worker member service: %w", serviceErr))
		}
		runtime.memberListener, err = net.Listen("tcp", configuration.memberListenAddress)
		if err != nil {
			return fail(fmt.Errorf("listen for Stage Worker member service: %w", err))
		}
		runtime.memberServer = grpc.NewServer(
			grpc.Creds(serverCredentials),
			grpc.MaxRecvMsgSize(4<<20),
			grpc.MaxSendMsgSize(4<<20),
		)
		velav1.RegisterStageWorkerMemberServiceServer(runtime.memberServer, memberService)
		runtime.memberServeErrors = make(chan error, 1)
		runtime.memberShutdownTimeout = configuration.memberShutdownTimeout
		go func() {
			runtime.memberServeErrors <- runtime.memberServer.Serve(runtime.memberListener)
		}()

		if configuration.workerMemberID == configuration.members[0].workerMemberID {
			for _, member := range configuration.members[1:] {
				clientCredentials, credentialsErr := stageworkertransport.NewClientTLSCredentials(
					configuration.memberClientCertificateFile,
					configuration.memberClientPrivateKeyFile,
					configuration.memberServerCAFile,
					member.serverName,
				)
				if credentialsErr != nil {
					return fail(fmt.Errorf("configure Stage Worker member client mTLS: %w", credentialsErr))
				}
				dialContext, cancel := context.WithTimeout(ctx, configuration.memberDialTimeout)
				client, dialErr := stageworkermembertransport.Dial(
					dialContext,
					stageworkermembertransport.ClientConfig{
						Address:              member.address,
						TargetWorkerMemberID: member.workerMemberID.String(),
						TargetIdentityDigest: member.identityDigest[:],
						TransportCredentials: clientCredentials,
					},
				)
				cancel()
				if dialErr != nil {
					return fail(fmt.Errorf(
						"connect to Stage Worker member %s: %w",
						member.workerMemberID,
						dialErr,
					))
				}
				runtime.memberClients = append(runtime.memberClients, client)
				runtimeMembers = append(runtimeMembers, stageworkeragent.RuntimeMember{
					ID: member.workerMemberID.String(), Client: client,
				})
			}
		}
		select {
		case serveErr := <-runtime.memberServeErrors:
			return fail(fmt.Errorf("serve Stage Worker member service during startup: %w", serveErr))
		default:
		}
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
		Members: runtimeMembers,
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
	artifactInputResolver, err := consumers.newInputResolver(
		stageworkeragent.AssignmentInputResolverConfig{
			Store:               store,
			TicketSigner:        transferTicketSigner,
			Control:             runtime.control,
			InputRoot:           configuration.inputRoot,
			ConnectorRevisionID: configuration.connectorRevisionID,
			Now:                 time.Now,
			MaxClockSkew:        authoritypolicy.ProductionMaxClockSkew,
			Journal:             runtime.inputJournal,
		},
	)
	if err != nil {
		return fail(err)
	}
	rootInputResolver, err := stageworkeragent.NewHTTPSRootInputResolver(
		stageworkeragent.HTTPSRootInputResolverConfig{InputRoot: configuration.inputRoot},
	)
	if err != nil {
		return fail(err)
	}
	inputResolver, err := stageworkeragent.NewCompositeInputResolver(
		artifactInputResolver,
		rootInputResolver,
	)
	if err != nil {
		return fail(err)
	}
	stream, err := consumers.newMaterializingAgent(
		runtimeAgent,
		runtime.control,
		stageworkeragent.MaterializationConfig{
			Validator:          materializationValidator,
			Source:             outputSource,
			Publisher:          publisher,
			Journal:            materializationJournal,
			SourceLossEvidence: sourceLossEvidenceProvider(configuration, time.Now),
			MaxClockSkew:       authoritypolicy.ProductionMaxClockSkew,
		},
		inputResolver,
	)
	if err != nil {
		return fail(err)
	}
	runtime.agent, err = stageworkeragent.NewProductionAgent(stageworkeragent.ProductionConfig{
		Control:                   runtime.control,
		Runtime:                   runtime.modelRuntime,
		Stream:                    stream,
		RuntimeIdentities:         runtimeIdentities,
		Devices:                   configuration.devices,
		Members:                   authorityMembers,
		CapacityVector:            configuration.capacityVector,
		CapacityTTL:               configuration.capacityTTL,
		HeartbeatInterval:         configuration.heartbeatInterval,
		RetryMinimum:              configuration.retryMinimum,
		RetryMaximum:              configuration.retryMaximum,
		ObservationSequenceSource: runtime.state,
		RetryObserver: func(operation string, cause error) {
			slog.Warn("Stage Worker control operation will retry", "operation", operation, "error", cause)
		},
		Now:  time.Now,
		Wait: waitContext,
	})
	if err != nil {
		return fail(err)
	}
	return runtime, nil
}

func (runtime *productionRuntime) Run(ctx context.Context) error {
	if runtime == nil || runtime.agent == nil {
		return errors.New("stage worker production runtime is incomplete")
	}
	if runtime.memberServer == nil {
		return runtime.agent.Run(ctx)
	}
	runContext, cancel := context.WithCancel(ctx)
	defer cancel()
	agentErrors := make(chan error, 1)
	go func() { agentErrors <- runtime.agent.Run(runContext) }()
	select {
	case err := <-agentErrors:
		return err
	case err := <-runtime.memberServeErrors:
		cancel()
		agentErr := <-agentErrors
		if err == nil || errors.Is(err, grpc.ErrServerStopped) {
			return errors.Join(errors.New("stage worker member service stopped unexpectedly"), agentErr)
		}
		return errors.Join(fmt.Errorf("serve Stage Worker member service: %w", err), agentErr)
	case <-ctx.Done():
		cancel()
		return <-agentErrors
	}
}

func (runtime *productionRuntime) Close() error {
	if runtime == nil {
		return nil
	}
	var closeErr error
	for index := len(runtime.memberClients) - 1; index >= 0; index-- {
		closeErr = errors.Join(closeErr, runtime.memberClients[index].Close())
	}
	runtime.memberClients = nil
	if runtime.memberServer != nil {
		stopped := make(chan struct{})
		go func() {
			runtime.memberServer.GracefulStop()
			close(stopped)
		}()
		timeout := runtime.memberShutdownTimeout
		if timeout <= 0 {
			timeout = time.Second
		}
		timer := time.NewTimer(timeout)
		select {
		case <-stopped:
			if !timer.Stop() {
				<-timer.C
			}
		case <-timer.C:
			runtime.memberServer.Stop()
			<-stopped
		}
		runtime.memberServer = nil
	}
	if runtime.memberListener != nil {
		if err := runtime.memberListener.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			closeErr = errors.Join(closeErr, err)
		}
		runtime.memberListener = nil
	}
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

func configuredRuntimeMembers(
	configuration config,
) (
	[]*velav1.StageAuthorityMemberEpoch,
	[]stageworkermembertransport.MemberBinding,
	memberConfig,
	error,
) {
	if len(configuration.members) == 0 || len(configuration.members) > 64 {
		return nil, nil, memberConfig{}, errors.New("stage worker member topology is incomplete")
	}
	authorityMembers := make([]*velav1.StageAuthorityMemberEpoch, 0, len(configuration.members))
	bindings := make([]stageworkermembertransport.MemberBinding, 0, len(configuration.members))
	var local memberConfig
	previousID := ""
	for _, member := range configuration.members {
		id := member.workerMemberID.String()
		if member.workerMemberID == uuid.Nil || member.memberEpoch <= 0 ||
			len(member.identityDigest) != sha256.Size ||
			(previousID != "" && id <= previousID) {
			return nil, nil, memberConfig{}, errors.New("stage worker member topology is invalid")
		}
		authorityMembers = append(authorityMembers, &velav1.StageAuthorityMemberEpoch{
			WorkerMemberId: id,
			MemberEpoch:    member.memberEpoch,
			IdentityDigest: append([]byte(nil), member.identityDigest[:]...),
		})
		bindings = append(bindings, stageworkermembertransport.MemberBinding{
			ID: id, Epoch: member.memberEpoch,
		})
		if member.workerMemberID == configuration.workerMemberID {
			if local.workerMemberID != uuid.Nil || member.memberEpoch != configuration.workerMemberEpoch {
				return nil, nil, memberConfig{}, errors.New("stage worker member topology does not match local member")
			}
			local = member
		}
		previousID = id
	}
	if local.workerMemberID == uuid.Nil {
		return nil, nil, memberConfig{}, errors.New("stage worker member topology omits local member")
	}
	return authorityMembers, bindings, local, nil
}

func validateLocalMemberCertificateIdentity(path string, expected [sha256.Size]byte) error {
	certificatePEM, err := securefile.Read(path, 1<<20, false)
	if err != nil {
		return err
	}
	block, _ := pem.Decode(certificatePEM)
	if block == nil || block.Type != "CERTIFICATE" {
		return errors.New("stage worker member certificate contains no leaf certificate")
	}
	certificate, err := x509.ParseCertificate(block.Bytes)
	if err != nil || len(certificate.URIs) != 1 || !validMemberSPIFFEURI(certificate.URIs[0]) {
		return errors.New("stage worker member certificate has no unique SPIFFE identity")
	}
	digest := sha256.Sum256([]byte(certificate.URIs[0].String()))
	if !bytes.Equal(digest[:], expected[:]) {
		return errors.New("stage worker member certificate identity digest does not match topology")
	}
	return nil
}

func validMemberSPIFFEURI(identity *url.URL) bool {
	if identity == nil {
		return false
	}
	value := identity.String()
	return identity.Scheme == "spiffe" && identity.Host != "" && identity.User == nil &&
		identity.Path != "" && identity.Opaque == "" && identity.RawQuery == "" &&
		identity.Fragment == "" && len(value) <= 500 && strings.TrimSpace(value) == value &&
		!strings.ContainsRune(value, '\x00') && identity.String() == value
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
				errors.New("stage worker source-loss evidence context is invalid")
		}
		if err := ctx.Err(); err != nil {
			return stageworkeragent.MaterializationSourceLossEvidence{}, err
		}
		authorityDigest, err := stageauthority.Digest(record.StageAuthority)
		if err != nil || record.LocalReceipt == nil ||
			len(record.LocalReceipt.GetManifestSha256()) != sha256.Size {
			return stageworkeragent.MaterializationSourceLossEvidence{},
				errors.New("stage worker source-loss evidence record is invalid")
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
		return errors.New("stage worker wait is invalid")
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
