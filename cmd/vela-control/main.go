package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/nats-io/nats.go"
	"github.com/vivym/vela/internal/admission"
	"github.com/vivym/vela/internal/artifactaccess"
	"github.com/vivym/vela/internal/artifactcleanup"
	"github.com/vivym/vela/internal/artifactstore"
	"github.com/vivym/vela/internal/artifactvalidator"
	"github.com/vivym/vela/internal/billingexport"
	"github.com/vivym/vela/internal/cancellation"
	veladb "github.com/vivym/vela/internal/database"
	"github.com/vivym/vela/internal/finalizationreconciler"
	"github.com/vivym/vela/internal/httpapi"
	"github.com/vivym/vela/internal/identity"
	"github.com/vivym/vela/internal/outbox"
	"github.com/vivym/vela/internal/scheduler"
	"github.com/vivym/vela/internal/webhook"
	"github.com/vivym/vela/internal/workercontrol"
	"github.com/vivym/vela/internal/workertransport"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/grpc"
)

const (
	defaultHTTPAddress                          = ":8080"
	defaultWorkerGRPCAddress                    = ":8443"
	defaultPublisherBatch                       = 100
	defaultPublisherTick                        = 500 * time.Millisecond
	defaultCancellationReconciliationTick       = 500 * time.Millisecond
	defaultFinalizationReconciliationTick       = 500 * time.Millisecond
	defaultFailureReconciliationTick            = 500 * time.Millisecond
	defaultSchedulerTick                        = 500 * time.Millisecond
	defaultSchedulerClaimTTL                    = 30 * time.Second
	defaultSchedulerCandidateAttempts           = 5
	defaultArtifactCleanupTick                  = time.Minute
	defaultInvoiceExportTick                    = 500 * time.Millisecond
	defaultInvoiceExportClaimTTL                = 30 * time.Second
	defaultInvoiceExportRetryDelay              = 5 * time.Second
	defaultInvoiceExportHTTPTimeout             = 15 * time.Second
	defaultInvoiceExportBatchSize         int32 = 100
	defaultWebhookTick                          = 500 * time.Millisecond
	defaultWebhookClaimTTL                      = 30 * time.Second
	defaultWebhookHTTPTimeout                   = 15 * time.Second
	defaultOIDCJWKSHTTPTimeout                  = 15 * time.Second
	defaultWebhookBatchSize               int32 = 100
	defaultExecutionLeaseTTL                    = 2 * time.Minute
	defaultWorkerLostGrace                      = 30 * time.Second
	defaultArtifactInspectionTimeout            = 30 * time.Second
	defaultArtifactOrphanMinimumAge             = 10 * time.Minute
	defaultArtifactMaxInputBytes          int64 = 100 * 1024 * 1024 * 1024
	defaultArtifactMaxProbeBytes          int64 = 1024 * 1024
	defaultArtifactMaxStderrBytes         int64 = 64 * 1024
	defaultArtifactCleanupBatch                 = 100
)

type config struct {
	httpAddress                  string
	workerGRPCAddress            string
	workerGRPCTLSCertFile        string
	workerGRPCTLSKeyFile         string
	workerGRPCClientCAFile       string
	authDatabaseURL              string
	humanAuthDatabaseURL         string
	requestDatabaseURL           string
	artifactRequestDatabaseURL   string
	cancelDatabaseURL            string
	internalDatabaseURL          string
	schedulerDatabaseURL         string
	schedulerID                  string
	schedulerTick                time.Duration
	schedulerClaimTTL            time.Duration
	schedulerCandidateAttempts   int
	billingDatabaseURL           string
	webhookRequestDatabaseURL    string
	webhookDatabaseURL           string
	webhookEncryptionActiveKeyID string
	webhookEncryptionKeyringFile string
	webhookDispatcherID          string
	webhookTick                  time.Duration
	webhookClaimTTL              time.Duration
	webhookHTTPTimeout           time.Duration
	webhookBatchSize             int32
	invoiceExporterID            string
	invoiceExportEndpoint        string
	invoiceExportTokenFile       string
	invoiceExportTick            time.Duration
	invoiceExportClaimTTL        time.Duration
	invoiceExportRetryDelay      time.Duration
	invoiceExportHTTPTimeout     time.Duration
	invoiceExportBatchSize       int32
	credentialPepper             []byte
	oidcIssuer                   string
	oidcAudience                 string
	oidcJWKSURL                  string
	natsURL                      string
	natsCredentials              string
	natsRootCA                   string
	natsClientCert               string
	natsClientKey                string
	publisherBatchSize           int32
	publisherTick                time.Duration
	cancellationTick             time.Duration
	finalizationTick             time.Duration
	failureTick                  time.Duration
	artifactCleanupTick          time.Duration
	artifactS3Endpoint           string
	artifactS3Region             string
	artifactS3Bucket             string
	artifactS3AccessKeyFile      string
	artifactS3SecretKeyFile      string
	artifactS3PathStyle          bool
	leaseActiveKeyID             string
	leaseKeyringFile             string
	executionLeaseTTL            time.Duration
	workerLostGrace              time.Duration
	artifactValidatorHelper      string
	artifactFFprobePath          string
	artifactSandboxRoot          string
	artifactSpoolDirectory       string
	artifactFFprobeVersion       string
	artifactValidatorRevision    string
	artifactInspectionTimeout    time.Duration
	artifactMaxInputBytes        int64
	artifactMaxProbeBytes        int64
	artifactMaxStderrBytes       int64
	artifactReconcilerID         string
	artifactOrphanMinimumAge     time.Duration
	artifactCleanupBatch         int
}

type cancellationStopReconciler interface {
	ReconcileNextCancellationStop(context.Context) (cancellation.StopResult, error)
}

type artifactFinalizationReconciler interface {
	ReconcileNext(context.Context) (finalizationreconciler.Result, error)
}

type executionFailureReconciler interface {
	ReconcileNextExecutionFailure(context.Context) (workercontrol.ReconciliationResult, error)
}

type artifactMultipartCleaner interface {
	Reconcile(context.Context) (artifactcleanup.Result, error)
}

type hierarchicalScheduler interface {
	ReconcileExpired(context.Context) (int64, error)
	RunCycle(context.Context) ([]scheduler.Dispatch, error)
}

type invoiceExporter interface {
	ExportBatch(context.Context) (billingexport.BatchResult, error)
}

type webhookDispatcher interface {
	DispatchBatch(context.Context) (webhook.BatchResult, error)
}

var newProductionArtifactSandbox = artifactvalidator.NewProductionSandbox

func main() {
	if err := run(); err != nil {
		slog.Error("vela-control stopped", "error", err)
		os.Exit(1)
	}
}

func run() error {
	configuration, err := loadConfig()
	if err != nil {
		return err
	}
	oidcVerifier, err := identity.NewRemoteOIDCTokenVerifier(identity.OIDCVerifierConfig{
		Issuer:     configuration.oidcIssuer,
		Audience:   configuration.oidcAudience,
		JWKSURL:    configuration.oidcJWKSURL,
		HTTPClient: &http.Client{Timeout: defaultOIDCJWKSHTTPTimeout},
	})
	if err != nil {
		return fmt.Errorf("configure Human OIDC verifier: %w", err)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	authPool, err := openPool(ctx, configuration.authDatabaseURL, 5, veladb.RoleAuth)
	if err != nil {
		return fmt.Errorf("open auth database pool: %w", err)
	}
	defer authPool.Close()
	humanAuthPool, err := openPool(
		ctx,
		configuration.humanAuthDatabaseURL,
		5,
		veladb.RoleHumanAuth,
	)
	if err != nil {
		return fmt.Errorf("open Human auth database pool: %w", err)
	}
	defer humanAuthPool.Close()
	requestPool, err := openPool(ctx, configuration.requestDatabaseURL, 20, veladb.RoleRequest)
	if err != nil {
		return fmt.Errorf("open request database pool: %w", err)
	}
	defer requestPool.Close()
	artifactRequestPool, err := openPool(
		ctx,
		configuration.artifactRequestDatabaseURL,
		20,
		veladb.RoleArtifactRequest,
	)
	if err != nil {
		return fmt.Errorf("open Artifact request database pool: %w", err)
	}
	defer artifactRequestPool.Close()
	cancelPool, err := openPool(ctx, configuration.cancelDatabaseURL, 10, veladb.RoleCancel)
	if err != nil {
		return fmt.Errorf("open cancellation database pool: %w", err)
	}
	defer cancelPool.Close()
	internalPool, err := openPool(ctx, configuration.internalDatabaseURL, 5, veladb.RoleInternal)
	if err != nil {
		return fmt.Errorf("open internal database pool: %w", err)
	}
	defer internalPool.Close()
	schedulerPool, err := openPool(ctx, configuration.schedulerDatabaseURL, 5, veladb.RoleScheduler)
	if err != nil {
		return fmt.Errorf("open Scheduler database pool: %w", err)
	}
	defer schedulerPool.Close()
	billingPool, err := openPool(ctx, configuration.billingDatabaseURL, 5, veladb.RoleBilling)
	if err != nil {
		return fmt.Errorf("open billing database pool: %w", err)
	}
	defer billingPool.Close()
	webhookRequestPool, err := openPool(
		ctx,
		configuration.webhookRequestDatabaseURL,
		20,
		veladb.RoleWebhookRequest,
	)
	if err != nil {
		return fmt.Errorf("open webhook request database pool: %w", err)
	}
	defer webhookRequestPool.Close()
	webhookPool, err := openPool(ctx, configuration.webhookDatabaseURL, 10, veladb.RoleWebhook)
	if err != nil {
		return fmt.Errorf("open webhook database pool: %w", err)
	}
	defer webhookPool.Close()
	webhookKeyring, err := readWebhookKeyring(configuration.webhookEncryptionKeyringFile)
	if err != nil {
		return err
	}
	webhookSealer, err := webhook.NewAESGCMSealer(
		configuration.webhookEncryptionActiveKeyID,
		webhookKeyring,
	)
	clearKeyring(webhookKeyring)
	if err != nil {
		return fmt.Errorf("configure webhook secret encryption: %w", err)
	}
	webhookHTTPClient, err := webhook.NewProductionHTTPClient(configuration.webhookHTTPTimeout)
	if err != nil {
		return fmt.Errorf("configure webhook HTTP client: %w", err)
	}
	webhookAdapter, err := webhook.NewHTTPAdapter(webhookHTTPClient)
	if err != nil {
		return fmt.Errorf("configure webhook HTTP adapter: %w", err)
	}
	webhookDeliveryDispatcher, err := webhook.NewDispatcher(
		webhookPool,
		webhookSealer,
		webhookAdapter,
		webhook.DispatcherConfig{
			InstanceID:      configuration.webhookDispatcherID,
			BatchSize:       configuration.webhookBatchSize,
			ClaimTTL:        configuration.webhookClaimTTL,
			DeliveryTimeout: configuration.webhookHTTPTimeout,
		},
	)
	if err != nil {
		return fmt.Errorf("configure webhook Dispatcher: %w", err)
	}
	webhookService, err := webhook.NewService(
		webhookRequestPool,
		webhookSealer,
		net.DefaultResolver,
	)
	if err != nil {
		return fmt.Errorf("configure webhook service: %w", err)
	}
	invoiceBearerToken, err := readSecretFile(
		configuration.invoiceExportTokenFile,
		"Invoice export bearer token",
		8192,
	)
	if err != nil {
		return err
	}
	invoiceAdapter, err := billingexport.NewHTTPAdapter(
		&http.Client{Timeout: configuration.invoiceExportHTTPTimeout},
		billingexport.HTTPConfig{
			Endpoint:    configuration.invoiceExportEndpoint,
			BearerToken: invoiceBearerToken,
		},
	)
	if err != nil {
		return fmt.Errorf("configure Invoice export adapter: %w", err)
	}
	invoiceService, err := billingexport.NewService(
		billingPool,
		invoiceAdapter,
		billingexport.Config{
			ExporterID: configuration.invoiceExporterID,
			BatchSize:  configuration.invoiceExportBatchSize,
			ClaimTTL:   configuration.invoiceExportClaimTTL,
			RetryDelay: configuration.invoiceExportRetryDelay,
		},
	)
	if err != nil {
		return fmt.Errorf("configure Invoice exporter: %w", err)
	}
	artifactStore, err := openArtifactStore(ctx, configuration)
	if err != nil {
		return err
	}
	workerCoordinator, err := openWorkerCoordinator(
		ctx,
		internalPool,
		artifactStore,
		configuration,
	)
	if err != nil {
		return err
	}
	scheduling, err := scheduler.NewService(schedulerPool, workerCoordinator, scheduler.Config{
		SchedulerID:       configuration.schedulerID,
		ClaimTTL:          configuration.schedulerClaimTTL,
		CandidateAttempts: configuration.schedulerCandidateAttempts,
	})
	if err != nil {
		return fmt.Errorf("configure Scheduler: %w", err)
	}
	capacityPredictor, err := scheduler.NewCapacityPredictor(schedulerPool)
	if err != nil {
		return fmt.Errorf("configure Scheduler capacity predictor: %w", err)
	}
	workerIdentityResolver, err := workertransport.NewPostgresIdentityResolver(internalPool)
	if err != nil {
		return err
	}
	workerControlAdapter, err := workertransport.NewServer(
		workerIdentityResolver,
		workerCoordinator,
		artifactStore,
	)
	if err != nil {
		return err
	}
	workerTransportCredentials, err := workertransport.NewServerTLSCredentials(
		configuration.workerGRPCTLSCertFile,
		configuration.workerGRPCTLSKeyFile,
		configuration.workerGRPCClientCAFile,
	)
	if err != nil {
		return err
	}
	workerGRPCServer := grpc.NewServer(
		grpc.Creds(workerTransportCredentials),
		grpc.MaxRecvMsgSize(1<<20),
		grpc.MaxSendMsgSize(4<<20),
	)
	velav1.RegisterWorkerControlServiceServer(workerGRPCServer, workerControlAdapter)
	workerListener, err := net.Listen("tcp", configuration.workerGRPCAddress)
	if err != nil {
		return fmt.Errorf("listen for Worker gRPC: %w", err)
	}
	defer func() { _ = workerListener.Close() }()
	artifactReconciler, err := finalizationreconciler.New(
		workerCoordinator,
		artifactStore,
		workercontrol.AuthenticatedReconciler{ID: configuration.artifactReconcilerID},
	)
	if err != nil {
		return err
	}
	multipartRegistry, err := artifactcleanup.NewPostgresRegistry(internalPool)
	if err != nil {
		return err
	}
	multipartCleaner, err := artifactcleanup.New(
		artifactStore,
		multipartRegistry,
		artifactcleanup.Config{
			ObjectPrefix: "artifacts/",
			MinimumAge:   configuration.artifactOrphanMinimumAge,
			MaxAborts:    configuration.artifactCleanupBatch,
		},
	)
	if err != nil {
		return err
	}

	natsConnection, err := connectNATS(configuration)
	if err != nil {
		return err
	}
	defer natsConnection.Close()
	broker, err := outbox.NewJetStreamBroker(natsConnection)
	if err != nil {
		return err
	}
	publisher, err := outbox.NewPublisher(internalPool, broker, outbox.Config{
		InstanceID: "vela-control-" + uuid.NewString(),
		BatchSize:  configuration.publisherBatchSize,
		ClaimTTL:   30 * time.Second,
		RetryDelay: 5 * time.Second,
	})
	if err != nil {
		return err
	}

	cancellationService := cancellation.NewService(cancelPool, internalPool)
	apiHandler, err := httpapi.NewHandler(httpapi.Config{
		Authenticator: identity.NewAuthenticatorWithOIDC(
			authPool,
			humanAuthPool,
			configuration.credentialPepper,
			oidcVerifier,
		),
		Admission:    admission.NewService(requestPool, capacityPredictor),
		Cancellation: cancellationService,
		Artifacts:    artifactaccess.NewService(artifactRequestPool, artifactStore),
		Webhooks:     webhookService,
	})
	if err != nil {
		return err
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc(
		"GET /readyz",
		readinessHandler(
			artifactStore,
			authPool,
			humanAuthPool,
			requestPool,
			artifactRequestPool,
			cancelPool,
			internalPool,
			schedulerPool,
			billingPool,
			webhookRequestPool,
			webhookPool,
		),
	)
	mux.Handle("/", apiHandler)
	httpServer := &http.Server{
		Addr:              configuration.httpAddress,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       2 * time.Minute,
	}

	schedulerDone := make(chan struct{})
	go func() {
		defer close(schedulerDone)
		runScheduler(ctx, scheduling, configuration.schedulerTick)
	}()
	publisherDone := make(chan struct{})
	go func() {
		defer close(publisherDone)
		runPublisher(ctx, publisher, configuration.publisherTick)
	}()
	reconcilerDone := make(chan struct{})
	go func() {
		defer close(reconcilerDone)
		runCancellationStopReconciler(ctx, cancellationService, configuration.cancellationTick)
	}()
	finalizationDone := make(chan struct{})
	go func() {
		defer close(finalizationDone)
		runArtifactFinalizationReconciler(
			ctx,
			artifactReconciler,
			configuration.finalizationTick,
		)
	}()
	failureDone := make(chan struct{})
	go func() {
		defer close(failureDone)
		runExecutionFailureReconciler(ctx, workerCoordinator, configuration.failureTick)
	}()
	cleanupDone := make(chan struct{})
	go func() {
		defer close(cleanupDone)
		runArtifactMultipartCleaner(ctx, multipartCleaner, configuration.artifactCleanupTick)
	}()
	invoiceDone := make(chan struct{})
	go func() {
		defer close(invoiceDone)
		runInvoiceExporter(ctx, invoiceService, configuration.invoiceExportTick)
	}()
	webhookDone := make(chan struct{})
	go func() {
		defer close(webhookDone)
		runWebhookDispatcher(ctx, webhookDeliveryDispatcher, configuration.webhookTick)
	}()
	httpServerErrors := make(chan error, 1)
	go func() {
		slog.Info("vela-control HTTP server started", "address", configuration.httpAddress)
		httpServerErrors <- httpServer.ListenAndServe()
	}()
	workerServerErrors := make(chan error, 1)
	go func() {
		slog.Info(
			"vela-control Worker gRPC server started",
			"address",
			configuration.workerGRPCAddress,
		)
		workerServerErrors <- workerGRPCServer.Serve(workerListener)
	}()

	var serveErr error
	select {
	case <-ctx.Done():
	case err := <-httpServerErrors:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			stop()
			serveErr = fmt.Errorf("serve HTTP: %w", err)
		}
	case err := <-workerServerErrors:
		if err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			stop()
			serveErr = fmt.Errorf("serve Worker gRPC: %w", err)
		}
	}
	shutdownContext, cancelShutdown := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancelShutdown()
	if err := httpServer.Shutdown(shutdownContext); err != nil {
		return fmt.Errorf("shut down HTTP server: %w", err)
	}
	workerServerDone := make(chan struct{})
	go func() {
		defer close(workerServerDone)
		workerGRPCServer.GracefulStop()
	}()
	select {
	case <-workerServerDone:
	case <-shutdownContext.Done():
		workerGRPCServer.Stop()
		return errors.New("worker gRPC server did not stop before shutdown deadline")
	}
	stop()
	select {
	case <-schedulerDone:
	case <-shutdownContext.Done():
		return errors.New("scheduler did not stop before shutdown deadline")
	}
	select {
	case <-publisherDone:
	case <-shutdownContext.Done():
		return errors.New("outbox Publisher did not stop before shutdown deadline")
	}
	select {
	case <-reconcilerDone:
	case <-shutdownContext.Done():
		return errors.New("cancellation stop reconciler did not stop before shutdown deadline")
	}
	select {
	case <-finalizationDone:
	case <-shutdownContext.Done():
		return errors.New("artifact finalization reconciler did not stop before shutdown deadline")
	}
	select {
	case <-failureDone:
	case <-shutdownContext.Done():
		return errors.New("execution failure reconciler did not stop before shutdown deadline")
	}
	select {
	case <-cleanupDone:
	case <-shutdownContext.Done():
		return errors.New("artifact multipart cleaner did not stop before shutdown deadline")
	}
	select {
	case <-invoiceDone:
	case <-shutdownContext.Done():
		return errors.New("invoice exporter did not stop before shutdown deadline")
	}
	select {
	case <-webhookDone:
	case <-shutdownContext.Done():
		return errors.New("webhook Dispatcher did not stop before shutdown deadline")
	}
	if err := natsConnection.Drain(); err != nil && !errors.Is(err, nats.ErrConnectionClosed) {
		return fmt.Errorf("drain NATS connection: %w", err)
	}
	if serveErr != nil {
		return serveErr
	}
	return nil
}

func loadConfig() (config, error) {
	configuration := config{
		httpAddress:                  envOrDefault("VELA_HTTP_ADDRESS", defaultHTTPAddress),
		workerGRPCAddress:            envOrDefault("VELA_WORKER_GRPC_ADDRESS", defaultWorkerGRPCAddress),
		workerGRPCTLSCertFile:        os.Getenv("VELA_WORKER_GRPC_TLS_CERT_FILE"),
		workerGRPCTLSKeyFile:         os.Getenv("VELA_WORKER_GRPC_TLS_KEY_FILE"),
		workerGRPCClientCAFile:       os.Getenv("VELA_WORKER_GRPC_CLIENT_CA_FILE"),
		authDatabaseURL:              os.Getenv("VELA_AUTH_DATABASE_URL"),
		humanAuthDatabaseURL:         os.Getenv("VELA_HUMAN_AUTH_DATABASE_URL"),
		requestDatabaseURL:           os.Getenv("VELA_REQUEST_DATABASE_URL"),
		artifactRequestDatabaseURL:   os.Getenv("VELA_ARTIFACT_REQUEST_DATABASE_URL"),
		oidcIssuer:                   os.Getenv("VELA_OIDC_ISSUER"),
		oidcAudience:                 os.Getenv("VELA_OIDC_AUDIENCE"),
		oidcJWKSURL:                  os.Getenv("VELA_OIDC_JWKS_URL"),
		cancelDatabaseURL:            os.Getenv("VELA_CANCEL_DATABASE_URL"),
		internalDatabaseURL:          os.Getenv("VELA_INTERNAL_DATABASE_URL"),
		schedulerDatabaseURL:         os.Getenv("VELA_SCHEDULER_DATABASE_URL"),
		schedulerID:                  os.Getenv("VELA_SCHEDULER_ID"),
		schedulerTick:                defaultSchedulerTick,
		schedulerClaimTTL:            defaultSchedulerClaimTTL,
		schedulerCandidateAttempts:   defaultSchedulerCandidateAttempts,
		billingDatabaseURL:           os.Getenv("VELA_BILLING_DATABASE_URL"),
		webhookRequestDatabaseURL:    os.Getenv("VELA_WEBHOOK_REQUEST_DATABASE_URL"),
		webhookDatabaseURL:           os.Getenv("VELA_WEBHOOK_DATABASE_URL"),
		webhookEncryptionActiveKeyID: os.Getenv("VELA_WEBHOOK_ENCRYPTION_ACTIVE_KEY_ID"),
		webhookEncryptionKeyringFile: os.Getenv("VELA_WEBHOOK_ENCRYPTION_KEYRING_FILE"),
		webhookDispatcherID:          os.Getenv("VELA_WEBHOOK_DISPATCHER_ID"),
		webhookTick:                  defaultWebhookTick,
		webhookClaimTTL:              defaultWebhookClaimTTL,
		webhookHTTPTimeout:           defaultWebhookHTTPTimeout,
		webhookBatchSize:             defaultWebhookBatchSize,
		invoiceExporterID:            os.Getenv("VELA_INVOICE_EXPORTER_ID"),
		invoiceExportEndpoint:        os.Getenv("VELA_INVOICE_EXPORT_ENDPOINT"),
		invoiceExportTokenFile:       os.Getenv("VELA_INVOICE_EXPORT_BEARER_TOKEN_FILE"),
		invoiceExportTick:            defaultInvoiceExportTick,
		invoiceExportClaimTTL:        defaultInvoiceExportClaimTTL,
		invoiceExportRetryDelay:      defaultInvoiceExportRetryDelay,
		invoiceExportHTTPTimeout:     defaultInvoiceExportHTTPTimeout,
		invoiceExportBatchSize:       defaultInvoiceExportBatchSize,
		natsURL:                      os.Getenv("VELA_NATS_URL"),
		natsCredentials:              os.Getenv("VELA_NATS_CREDENTIALS_FILE"),
		natsRootCA:                   os.Getenv("VELA_NATS_ROOT_CA_FILE"),
		natsClientCert:               os.Getenv("VELA_NATS_CLIENT_CERT_FILE"),
		natsClientKey:                os.Getenv("VELA_NATS_CLIENT_KEY_FILE"),
		artifactS3Endpoint:           os.Getenv("VELA_ARTIFACT_S3_ENDPOINT"),
		artifactS3Region:             os.Getenv("VELA_ARTIFACT_S3_REGION"),
		artifactS3Bucket:             os.Getenv("VELA_ARTIFACT_S3_BUCKET"),
		artifactS3AccessKeyFile:      os.Getenv("VELA_ARTIFACT_S3_ACCESS_KEY_ID_FILE"),
		artifactS3SecretKeyFile:      os.Getenv("VELA_ARTIFACT_S3_SECRET_ACCESS_KEY_FILE"),
		publisherBatchSize:           defaultPublisherBatch,
		publisherTick:                defaultPublisherTick,
		cancellationTick:             defaultCancellationReconciliationTick,
		finalizationTick:             defaultFinalizationReconciliationTick,
		failureTick:                  defaultFailureReconciliationTick,
		artifactCleanupTick:          defaultArtifactCleanupTick,
		leaseActiveKeyID:             os.Getenv("VELA_LEASE_ACTIVE_KEY_ID"),
		leaseKeyringFile:             os.Getenv("VELA_LEASE_KEYRING_FILE"),
		executionLeaseTTL:            defaultExecutionLeaseTTL,
		workerLostGrace:              defaultWorkerLostGrace,
		artifactValidatorHelper:      os.Getenv("VELA_ARTIFACT_VALIDATOR_HELPER_PATH"),
		artifactFFprobePath:          os.Getenv("VELA_ARTIFACT_FFPROBE_PATH"),
		artifactSandboxRoot:          os.Getenv("VELA_ARTIFACT_SANDBOX_ROOT"),
		artifactSpoolDirectory:       os.Getenv("VELA_ARTIFACT_SPOOL_DIRECTORY"),
		artifactFFprobeVersion:       os.Getenv("VELA_ARTIFACT_FFPROBE_VERSION"),
		artifactValidatorRevision:    os.Getenv("VELA_ARTIFACT_VALIDATOR_REVISION"),
		artifactInspectionTimeout:    defaultArtifactInspectionTimeout,
		artifactMaxInputBytes:        defaultArtifactMaxInputBytes,
		artifactMaxProbeBytes:        defaultArtifactMaxProbeBytes,
		artifactMaxStderrBytes:       defaultArtifactMaxStderrBytes,
		artifactReconcilerID:         os.Getenv("VELA_ARTIFACT_RECONCILER_ID"),
		artifactOrphanMinimumAge:     defaultArtifactOrphanMinimumAge,
		artifactCleanupBatch:         defaultArtifactCleanupBatch,
	}
	for name, value := range map[string]string{
		"VELA_AUTH_DATABASE_URL":                  configuration.authDatabaseURL,
		"VELA_HUMAN_AUTH_DATABASE_URL":            configuration.humanAuthDatabaseURL,
		"VELA_REQUEST_DATABASE_URL":               configuration.requestDatabaseURL,
		"VELA_ARTIFACT_REQUEST_DATABASE_URL":      configuration.artifactRequestDatabaseURL,
		"VELA_OIDC_ISSUER":                        configuration.oidcIssuer,
		"VELA_OIDC_AUDIENCE":                      configuration.oidcAudience,
		"VELA_OIDC_JWKS_URL":                      configuration.oidcJWKSURL,
		"VELA_CANCEL_DATABASE_URL":                configuration.cancelDatabaseURL,
		"VELA_INTERNAL_DATABASE_URL":              configuration.internalDatabaseURL,
		"VELA_SCHEDULER_DATABASE_URL":             configuration.schedulerDatabaseURL,
		"VELA_SCHEDULER_ID":                       configuration.schedulerID,
		"VELA_BILLING_DATABASE_URL":               configuration.billingDatabaseURL,
		"VELA_WEBHOOK_REQUEST_DATABASE_URL":       configuration.webhookRequestDatabaseURL,
		"VELA_WEBHOOK_DATABASE_URL":               configuration.webhookDatabaseURL,
		"VELA_WEBHOOK_ENCRYPTION_ACTIVE_KEY_ID":   configuration.webhookEncryptionActiveKeyID,
		"VELA_WEBHOOK_ENCRYPTION_KEYRING_FILE":    configuration.webhookEncryptionKeyringFile,
		"VELA_WEBHOOK_DISPATCHER_ID":              configuration.webhookDispatcherID,
		"VELA_INVOICE_EXPORTER_ID":                configuration.invoiceExporterID,
		"VELA_INVOICE_EXPORT_ENDPOINT":            configuration.invoiceExportEndpoint,
		"VELA_INVOICE_EXPORT_BEARER_TOKEN_FILE":   configuration.invoiceExportTokenFile,
		"VELA_NATS_URL":                           configuration.natsURL,
		"VELA_NATS_CREDENTIALS_FILE":              configuration.natsCredentials,
		"VELA_NATS_ROOT_CA_FILE":                  configuration.natsRootCA,
		"VELA_ARTIFACT_S3_ENDPOINT":               configuration.artifactS3Endpoint,
		"VELA_ARTIFACT_S3_REGION":                 configuration.artifactS3Region,
		"VELA_ARTIFACT_S3_BUCKET":                 configuration.artifactS3Bucket,
		"VELA_ARTIFACT_S3_ACCESS_KEY_ID_FILE":     configuration.artifactS3AccessKeyFile,
		"VELA_ARTIFACT_S3_SECRET_ACCESS_KEY_FILE": configuration.artifactS3SecretKeyFile,
		"VELA_LEASE_ACTIVE_KEY_ID":                configuration.leaseActiveKeyID,
		"VELA_LEASE_KEYRING_FILE":                 configuration.leaseKeyringFile,
		"VELA_ARTIFACT_VALIDATOR_HELPER_PATH":     configuration.artifactValidatorHelper,
		"VELA_ARTIFACT_FFPROBE_PATH":              configuration.artifactFFprobePath,
		"VELA_ARTIFACT_SANDBOX_ROOT":              configuration.artifactSandboxRoot,
		"VELA_ARTIFACT_SPOOL_DIRECTORY":           configuration.artifactSpoolDirectory,
		"VELA_ARTIFACT_FFPROBE_VERSION":           configuration.artifactFFprobeVersion,
		"VELA_ARTIFACT_VALIDATOR_REVISION":        configuration.artifactValidatorRevision,
		"VELA_ARTIFACT_RECONCILER_ID":             configuration.artifactReconcilerID,
		"VELA_WORKER_GRPC_TLS_CERT_FILE":          configuration.workerGRPCTLSCertFile,
		"VELA_WORKER_GRPC_TLS_KEY_FILE":           configuration.workerGRPCTLSKeyFile,
		"VELA_WORKER_GRPC_CLIENT_CA_FILE":         configuration.workerGRPCClientCAFile,
	} {
		if value == "" {
			return config{}, fmt.Errorf("%s is required", name)
		}
	}
	encodedPepper := os.Getenv("VELA_CREDENTIAL_PEPPER_BASE64")
	pepper, err := base64.StdEncoding.DecodeString(encodedPepper)
	if err != nil || len(pepper) < 32 {
		return config{}, errors.New("environment variable VELA_CREDENTIAL_PEPPER_BASE64 must encode at least 32 random bytes")
	}
	configuration.credentialPepper = pepper
	if value := os.Getenv("VELA_ARTIFACT_S3_PATH_STYLE"); value != "" {
		pathStyle, err := strconv.ParseBool(value)
		if err != nil {
			return config{}, errors.New("environment variable VELA_ARTIFACT_S3_PATH_STYLE must be true or false")
		}
		configuration.artifactS3PathStyle = pathStyle
	}
	if value := os.Getenv("VELA_OUTBOX_BATCH_SIZE"); value != "" {
		batchSize, err := strconv.ParseInt(value, 10, 32)
		if err != nil || batchSize < 1 || batchSize > 1000 {
			return config{}, errors.New("environment variable VELA_OUTBOX_BATCH_SIZE must be between 1 and 1000")
		}
		configuration.publisherBatchSize = int32(batchSize)
	}
	if value := os.Getenv("VELA_SCHEDULER_TICK"); value != "" {
		tick, err := time.ParseDuration(value)
		if err != nil || tick <= 0 || tick > time.Minute {
			return config{}, errors.New("environment variable VELA_SCHEDULER_TICK must be in (0, 1m]")
		}
		configuration.schedulerTick = tick
	}
	if value := os.Getenv("VELA_SCHEDULER_CLAIM_TTL"); value != "" {
		claimTTL, err := time.ParseDuration(value)
		if err != nil || claimTTL <= 0 || claimTTL > 5*time.Minute {
			return config{}, errors.New("environment variable VELA_SCHEDULER_CLAIM_TTL must be in (0, 5m]")
		}
		configuration.schedulerClaimTTL = claimTTL
	}
	if value := os.Getenv("VELA_SCHEDULER_CANDIDATE_ATTEMPTS"); value != "" {
		candidateAttempts, err := strconv.Atoi(value)
		if err != nil || candidateAttempts < 1 || candidateAttempts > 20 {
			return config{}, errors.New("environment variable VELA_SCHEDULER_CANDIDATE_ATTEMPTS must be in 1..20")
		}
		configuration.schedulerCandidateAttempts = candidateAttempts
	}
	if value := os.Getenv("VELA_INVOICE_EXPORT_TICK"); value != "" {
		tick, err := time.ParseDuration(value)
		if err != nil || tick <= 0 || tick > time.Minute {
			return config{}, errors.New("environment variable VELA_INVOICE_EXPORT_TICK must be in (0, 1m]")
		}
		configuration.invoiceExportTick = tick
	}
	if value := os.Getenv("VELA_INVOICE_EXPORT_CLAIM_TTL"); value != "" {
		claimTTL, err := time.ParseDuration(value)
		if err != nil || claimTTL <= 0 || claimTTL > 5*time.Minute {
			return config{}, errors.New("environment variable VELA_INVOICE_EXPORT_CLAIM_TTL must be in (0, 5m]")
		}
		configuration.invoiceExportClaimTTL = claimTTL
	}
	if value := os.Getenv("VELA_INVOICE_EXPORT_RETRY_DELAY"); value != "" {
		retryDelay, err := time.ParseDuration(value)
		if err != nil || retryDelay < 0 || retryDelay > time.Hour {
			return config{}, errors.New("environment variable VELA_INVOICE_EXPORT_RETRY_DELAY must be in [0, 1h]")
		}
		configuration.invoiceExportRetryDelay = retryDelay
	}
	if value := os.Getenv("VELA_INVOICE_EXPORT_BATCH_SIZE"); value != "" {
		batchSize, err := strconv.ParseInt(value, 10, 32)
		if err != nil || batchSize < 1 || batchSize > 1000 {
			return config{}, errors.New("environment variable VELA_INVOICE_EXPORT_BATCH_SIZE must be between 1 and 1000")
		}
		configuration.invoiceExportBatchSize = int32(batchSize)
	}
	if value := os.Getenv("VELA_INVOICE_EXPORT_HTTP_TIMEOUT"); value != "" {
		timeout, err := time.ParseDuration(value)
		if err != nil || timeout <= 0 || timeout > time.Minute {
			return config{}, errors.New("environment variable VELA_INVOICE_EXPORT_HTTP_TIMEOUT must be in (0, 1m]")
		}
		configuration.invoiceExportHTTPTimeout = timeout
	}
	if value := os.Getenv("VELA_WEBHOOK_TICK"); value != "" {
		tick, err := time.ParseDuration(value)
		if err != nil || tick <= 0 || tick > time.Minute {
			return config{}, errors.New("environment variable VELA_WEBHOOK_TICK must be in (0, 1m]")
		}
		configuration.webhookTick = tick
	}
	if value := os.Getenv("VELA_WEBHOOK_CLAIM_TTL"); value != "" {
		claimTTL, err := time.ParseDuration(value)
		if err != nil || claimTTL <= 0 || claimTTL > time.Hour {
			return config{}, errors.New("environment variable VELA_WEBHOOK_CLAIM_TTL must be in (0, 1h]")
		}
		configuration.webhookClaimTTL = claimTTL
	}
	if value := os.Getenv("VELA_WEBHOOK_BATCH_SIZE"); value != "" {
		batchSize, err := strconv.ParseInt(value, 10, 32)
		if err != nil || batchSize < 1 || batchSize > 1000 {
			return config{}, errors.New("environment variable VELA_WEBHOOK_BATCH_SIZE must be between 1 and 1000")
		}
		configuration.webhookBatchSize = int32(batchSize)
	}
	if value := os.Getenv("VELA_WEBHOOK_HTTP_TIMEOUT"); value != "" {
		timeout, err := time.ParseDuration(value)
		if err != nil || timeout <= 0 || timeout > time.Minute {
			return config{}, errors.New("environment variable VELA_WEBHOOK_HTTP_TIMEOUT must be in (0, 1m]")
		}
		configuration.webhookHTTPTimeout = timeout
	}
	return configuration, nil
}

func openPool(
	ctx context.Context,
	databaseURL string,
	maxConnections int32,
	role veladb.Role,
) (*pgxpool.Pool, error) {
	poolConfig, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		return nil, err
	}
	poolConfig.MaxConns = maxConnections
	poolConfig.MinConns = 1
	poolConfig.MaxConnLifetime = 30 * time.Minute
	poolConfig.MaxConnIdleTime = 5 * time.Minute
	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		return nil, err
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	if err := veladb.VerifyRole(ctx, pool, role); err != nil {
		pool.Close()
		return nil, fmt.Errorf("verify %s database pool: %w", role, err)
	}
	return pool, nil
}

func connectNATS(configuration config) (*nats.Conn, error) {
	options := []nats.Option{
		nats.Name("vela-control-outbox"),
		nats.RetryOnFailedConnect(true),
		nats.MaxReconnects(-1),
		nats.ReconnectWait(time.Second),
		nats.Timeout(5 * time.Second),
		nats.DisconnectErrHandler(func(_ *nats.Conn, err error) {
			if err != nil {
				slog.Warn("NATS disconnected; Outbox will remain durable", "error", err)
			}
		}),
		nats.ReconnectHandler(func(connection *nats.Conn) {
			slog.Info("NATS reconnected", "url", connection.ConnectedUrl())
		}),
	}
	options = append(
		options,
		nats.UserCredentials(configuration.natsCredentials),
		nats.RootCAs(configuration.natsRootCA),
	)
	if configuration.natsClientCert != "" || configuration.natsClientKey != "" {
		if configuration.natsClientCert == "" || configuration.natsClientKey == "" {
			return nil, errors.New("both VELA_NATS_CLIENT_CERT_FILE and VELA_NATS_CLIENT_KEY_FILE are required")
		}
		options = append(options, nats.ClientCert(configuration.natsClientCert, configuration.natsClientKey))
	}
	connection, err := nats.Connect(configuration.natsURL, options...)
	if err != nil {
		return nil, fmt.Errorf("connect NATS: %w", err)
	}
	return connection, nil
}

func openArtifactStore(ctx context.Context, configuration config) (*artifactstore.S3, error) {
	accessKeyID, err := readSecretFile(
		configuration.artifactS3AccessKeyFile,
		"Artifact S3 access key ID",
		1024,
	)
	if err != nil {
		return nil, err
	}
	secretAccessKey, err := readSecretFile(
		configuration.artifactS3SecretKeyFile,
		"Artifact S3 secret access key",
		4096,
	)
	if err != nil {
		return nil, err
	}
	store, err := artifactstore.NewS3(artifactstore.S3Config{
		Endpoint:        configuration.artifactS3Endpoint,
		Region:          configuration.artifactS3Region,
		Bucket:          configuration.artifactS3Bucket,
		AccessKeyID:     accessKeyID,
		SecretAccessKey: secretAccessKey,
		UsePathStyle:    configuration.artifactS3PathStyle,
		SignedGETTTL:    artifactstore.MaxSignedGETTTL,
	})
	if err != nil {
		return nil, fmt.Errorf("configure Artifact Store: %w", err)
	}
	if err := store.ValidateBucket(ctx); err != nil {
		return nil, fmt.Errorf("validate Artifact Store bucket: %w", err)
	}
	return store, nil
}

func openWorkerCoordinator(
	ctx context.Context,
	pool *pgxpool.Pool,
	artifactStore *artifactstore.S3,
	configuration config,
) (*workercontrol.Service, error) {
	sandbox, err := newProductionArtifactSandbox(artifactvalidator.SandboxConfig{
		HelperPath:     configuration.artifactValidatorHelper,
		FFprobePath:    configuration.artifactFFprobePath,
		RootDirectory:  configuration.artifactSandboxRoot,
		MaxOutputBytes: configuration.artifactMaxProbeBytes,
		MaxStderrBytes: configuration.artifactMaxStderrBytes,
	})
	if err != nil {
		return nil, fmt.Errorf("configure production Artifact sandbox: %w", err)
	}
	inspector, err := artifactvalidator.NewInspector(
		artifactStore,
		sandbox,
		artifactvalidator.Config{
			MaxInputBytes:          configuration.artifactMaxInputBytes,
			MaxProbeOutputBytes:    configuration.artifactMaxProbeBytes,
			Timeout:                configuration.artifactInspectionTimeout,
			ExpectedFFprobeVersion: configuration.artifactFFprobeVersion,
			ValidatorRevision:      configuration.artifactValidatorRevision,
			SpoolDirectory:         configuration.artifactSpoolDirectory,
		},
	)
	if err != nil {
		return nil, fmt.Errorf("configure production Artifact inspector: %w", err)
	}
	keyring, err := readLeaseKeyring(configuration.leaseKeyringFile)
	if err != nil {
		return nil, err
	}
	defer clearLeaseKeyring(keyring)
	coordinator, err := workercontrol.NewService(ctx, pool, workercontrol.Config{
		LeaseTTL:          configuration.executionLeaseTTL,
		WorkerLostGrace:   configuration.workerLostGrace,
		ActiveLeaseKeyID:  configuration.leaseActiveKeyID,
		LeaseKeys:         keyring,
		ArtifactInspector: inspector,
	})
	if err != nil {
		return nil, fmt.Errorf("configure Worker coordinator: %w", err)
	}
	return coordinator, nil
}

func readSecretFile(path string, description string, maxBytes int64) (string, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", fmt.Errorf("open %s file: %w", description, err)
	}
	defer func() { _ = file.Close() }()
	content, err := io.ReadAll(io.LimitReader(file, maxBytes+1))
	if err != nil {
		return "", fmt.Errorf("read %s file: %w", description, err)
	}
	secret := strings.TrimSpace(string(content))
	clear(content)
	if secret == "" || int64(len(secret)) > maxBytes || strings.ContainsRune(secret, '\x00') {
		return "", fmt.Errorf("%s file is empty or invalid", description)
	}
	return secret, nil
}

func readLeaseKeyring(path string) (map[string][]byte, error) {
	return readKeyring(path, "Lease", 32, 4096)
}

func readWebhookKeyring(path string) (map[string][]byte, error) {
	return readKeyring(path, "webhook encryption", 32, 32)
}

func readKeyring(path, description string, minimumKeyBytes, maximumKeyBytes int) (map[string][]byte, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open %s keyring file: %w", description, err)
	}
	defer func() { _ = file.Close() }()
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() {
		return nil, fmt.Errorf("%s keyring must be a regular file", description)
	}
	content, err := io.ReadAll(io.LimitReader(file, 64*1024+1))
	if err != nil {
		return nil, fmt.Errorf("read %s keyring file: %w", description, err)
	}
	defer clear(content)
	if len(content) == 0 || len(content) > 64*1024 {
		return nil, fmt.Errorf("%s keyring file is empty or exceeds configured bounds", description)
	}
	decoder := json.NewDecoder(bytes.NewReader(content))
	opening, err := decoder.Token()
	if err != nil || opening != json.Delim('{') {
		return nil, fmt.Errorf("%s keyring must be one JSON object", description)
	}
	keyring := make(map[string][]byte)
	for decoder.More() {
		keyToken, tokenErr := decoder.Token()
		keyID, ok := keyToken.(string)
		if tokenErr != nil || !ok || keyID == "" || len(keyID) > 200 ||
			strings.TrimSpace(keyID) != keyID || strings.ContainsAny(keyID, "\x00\r\n\t ") {
			clearKeyring(keyring)
			return nil, fmt.Errorf("%s keyring contains an invalid key id", description)
		}
		if _, duplicate := keyring[keyID]; duplicate {
			clearKeyring(keyring)
			return nil, fmt.Errorf("%s keyring contains a duplicate key id", description)
		}
		var encoded string
		if err := decoder.Decode(&encoded); err != nil {
			clearKeyring(keyring)
			return nil, fmt.Errorf("%s keyring values must be base64 strings", description)
		}
		key, decodeErr := base64.StdEncoding.Strict().DecodeString(encoded)
		if decodeErr != nil || len(key) < minimumKeyBytes || len(key) > maximumKeyBytes {
			clear(key)
			clearKeyring(keyring)
			return nil, fmt.Errorf(
				"%s keyring values must encode %d to %d bytes",
				description,
				minimumKeyBytes,
				maximumKeyBytes,
			)
		}
		keyring[keyID] = key
		if len(keyring) > 32 {
			clearKeyring(keyring)
			return nil, fmt.Errorf("%s keyring contains too many keys", description)
		}
	}
	closing, err := decoder.Token()
	if err != nil || closing != json.Delim('}') {
		clearKeyring(keyring)
		return nil, fmt.Errorf("%s keyring JSON object is incomplete", description)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		clearKeyring(keyring)
		return nil, fmt.Errorf("%s keyring must contain one JSON document", description)
	}
	if len(keyring) == 0 {
		return nil, fmt.Errorf("%s keyring contains no keys", description)
	}
	return keyring, nil
}

func clearLeaseKeyring(keyring map[string][]byte) {
	clearKeyring(keyring)
}

func clearKeyring(keyring map[string][]byte) {
	for keyID, key := range keyring {
		clear(key)
		delete(keyring, keyID)
	}
}

type artifactBucketValidator interface {
	ValidateBucket(context.Context) error
}

func readinessHandler(
	artifactStore artifactBucketValidator,
	pools ...*pgxpool.Pool,
) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		for _, pool := range pools {
			if err := pool.Ping(ctx); err != nil {
				http.Error(w, "database unavailable", http.StatusServiceUnavailable)
				return
			}
		}
		if artifactStore == nil || artifactStore.ValidateBucket(ctx) != nil {
			http.Error(w, "Artifact Store unavailable", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}
}

func runPublisher(ctx context.Context, publisher *outbox.Publisher, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		published, err := publisher.PublishBatch(ctx)
		if err != nil && !errors.Is(err, context.Canceled) {
			slog.Warn("Outbox publish batch incomplete", "published", published, "error", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func runScheduler(ctx context.Context, scheduling hierarchicalScheduler, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		reconciled, err := scheduling.ReconcileExpired(ctx)
		if err != nil && !errors.Is(err, context.Canceled) {
			slog.Warn("Scheduler claim reconciliation incomplete", "error", err)
		} else if err == nil && reconciled > 0 {
			slog.Info("Scheduler expired claims reconciled", "claims", reconciled)
		}
		if ctx.Err() != nil {
			return
		}
		dispatches, err := scheduling.RunCycle(ctx)
		if err != nil && !errors.Is(err, context.Canceled) {
			slog.Warn("Scheduler cycle incomplete", "error", err)
		} else if err == nil && len(dispatches) > 0 {
			slog.Info("Scheduler cycle dispatched Assignments", "dispatches", len(dispatches))
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func runInvoiceExporter(ctx context.Context, exporter invoiceExporter, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		result, err := exporter.ExportBatch(ctx)
		if err != nil && !errors.Is(err, context.Canceled) {
			slog.Warn(
				"Invoice export batch incomplete",
				"claimed", result.Claimed,
				"exported", result.Exported,
				"error", err,
			)
		} else if err == nil && result.Exported > 0 {
			slog.Info("Invoice lines exported", "exported", result.Exported)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func runWebhookDispatcher(ctx context.Context, dispatcher webhookDispatcher, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		result, err := dispatcher.DispatchBatch(ctx)
		if err != nil && !errors.Is(err, context.Canceled) {
			slog.Warn(
				"Webhook Delivery batch incomplete",
				"claimed", result.Claimed,
				"delivered", result.Delivered,
				"failed", result.Failed,
				"error", err,
			)
		} else if err == nil && result.Delivered > 0 {
			slog.Info(
				"Webhook Deliveries completed",
				"delivered", result.Delivered,
				"failed", result.Failed,
			)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func runCancellationStopReconciler(
	ctx context.Context,
	reconciler cancellationStopReconciler,
	interval time.Duration,
) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		result, err := reconciler.ReconcileNextCancellationStop(ctx)
		if err != nil && !errors.Is(err, context.Canceled) {
			slog.Warn("Cancellation stop reconciliation incomplete", "error", err)
		} else if err == nil && result.Decision != cancellation.StopNoWork {
			slog.Info(
				"Cancellation stop reconciled",
				"decision", result.Decision,
				"cancellation_id", result.CancellationID,
				"job_id", result.JobID,
				"receipt_id", result.ReceiptID,
			)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func runArtifactFinalizationReconciler(
	ctx context.Context,
	reconciler artifactFinalizationReconciler,
	interval time.Duration,
) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		result, err := reconciler.ReconcileNext(ctx)
		if err != nil && !errors.Is(err, context.Canceled) {
			slog.Warn("Artifact finalization reconciliation incomplete", "error", err)
		} else if err == nil && result.Takeover != workercontrol.FinalizationTakeoverNoWork {
			slog.Info(
				"Artifact finalization reconciled",
				"takeover", result.Takeover,
				"attempt_id", result.AttemptID,
				"job_id", result.JobID,
				"verified_artifacts", result.Verified,
				"completion", result.Completion.Decision,
			)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func runExecutionFailureReconciler(
	ctx context.Context,
	reconciler executionFailureReconciler,
	interval time.Duration,
) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		result, err := reconciler.ReconcileNextExecutionFailure(ctx)
		if err != nil && !errors.Is(err, context.Canceled) {
			slog.Warn("Execution failure reconciliation incomplete", "error", err)
		} else if err == nil && result.Processed {
			slog.Info(
				"Execution failure reconciled",
				"source", result.Source,
				"job_id", result.Decision.JobID,
				"attempt_id", result.Decision.AttemptID,
				"disposition", result.Decision.Disposition,
			)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func runArtifactMultipartCleaner(
	ctx context.Context,
	cleaner artifactMultipartCleaner,
	interval time.Duration,
) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		result, err := cleaner.Reconcile(ctx)
		if err != nil && !errors.Is(err, context.Canceled) {
			slog.Warn("Artifact multipart cleanup incomplete", "error", err)
		} else if err == nil && result.Aborted > 0 {
			slog.Info(
				"Artifact multipart orphans cleaned",
				"listed", result.Listed,
				"eligible", result.Eligible,
				"recorded", result.Recorded,
				"aborted", result.Aborted,
			)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func envOrDefault(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}
