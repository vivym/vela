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
	"net/netip"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/nats-io/nats.go"
	"github.com/vivym/vela/internal/admission"
	"github.com/vivym/vela/internal/artifactaccess"
	"github.com/vivym/vela/internal/artifactcleanup"
	"github.com/vivym/vela/internal/artifactreplication"
	"github.com/vivym/vela/internal/artifactstore"
	"github.com/vivym/vela/internal/artifactvalidator"
	"github.com/vivym/vela/internal/attemptcoordinator"
	"github.com/vivym/vela/internal/billingexport"
	"github.com/vivym/vela/internal/breakglass"
	"github.com/vivym/vela/internal/cancellation"
	veladb "github.com/vivym/vela/internal/database"
	"github.com/vivym/vela/internal/debugdump"
	"github.com/vivym/vela/internal/finalizationreconciler"
	"github.com/vivym/vela/internal/financereconciliation"
	"github.com/vivym/vela/internal/fleet"
	"github.com/vivym/vela/internal/fleettransport"
	"github.com/vivym/vela/internal/httpapi"
	"github.com/vivym/vela/internal/identity"
	"github.com/vivym/vela/internal/inbox"
	"github.com/vivym/vela/internal/legalhold"
	"github.com/vivym/vela/internal/natsauth"
	"github.com/vivym/vela/internal/nodeagent"
	"github.com/vivym/vela/internal/noncontentexpiry"
	"github.com/vivym/vela/internal/organizationreporting"
	"github.com/vivym/vela/internal/outbox"
	"github.com/vivym/vela/internal/remediation"
	"github.com/vivym/vela/internal/retention"
	"github.com/vivym/vela/internal/scheduler"
	"github.com/vivym/vela/internal/securefile"
	"github.com/vivym/vela/internal/strictjson"
	"github.com/vivym/vela/internal/telemetry"
	"github.com/vivym/vela/internal/webhook"
	"github.com/vivym/vela/internal/workercontrol"
	"github.com/vivym/vela/internal/workertransport"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/grpc"
)

const (
	defaultHTTPAddress                          = ":8080"
	defaultManagementAddress                    = ":8081"
	defaultWorkerGRPCAddress                    = ":8443"
	defaultFleetGRPCAddress                     = ":8444"
	defaultRemediationTick                      = 500 * time.Millisecond
	defaultRemediationBatch                     = 100
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
	defaultRetentionTick                        = time.Minute
	defaultRetentionClaimTTL                    = time.Minute
	defaultRetentionRetryDelay                  = 5 * time.Minute
	defaultRetentionBatchSize                   = 100
	defaultNonContentExpiryTick                 = time.Minute
	defaultNonContentExpiryClaimTTL             = time.Minute
	defaultNonContentExpiryHeldRetry            = 5 * time.Minute
	defaultNonContentExpiryBatchSize            = 100
	defaultArtifactReplicationTick              = time.Minute
	defaultArtifactReplicationClaimTTL          = 20 * time.Minute
	defaultArtifactReplicationRetryDelay        = 5 * time.Minute
	defaultArtifactReplicationTimeout           = 15 * time.Minute
	defaultArtifactReplicationBatchSize         = 10
	defaultShutdownTimeout                      = 20 * time.Second
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
	httpAddress                            string
	managementAddress                      string
	workerGRPCAddress                      string
	workerGRPCTLSCertFile                  string
	workerGRPCTLSKeyFile                   string
	workerGRPCClientCAFile                 string
	fleetGRPCAddress                       string
	fleetGRPCTLSCertFile                   string
	fleetGRPCTLSKeyFile                    string
	fleetGRPCClientCAFile                  string
	fleetDatabaseURL                       string
	fleetControllerSPIFFEIdentity          string
	fleetControllerActorIdentity           string
	authDatabaseURL                        string
	humanAuthDatabaseURL                   string
	humanMembershipAuthDatabaseURL         string
	identityRequestDatabaseURL             string
	humanMembershipRequestDatabaseURL      string
	organizationBillingDatabaseURL         string
	organizationAuditDatabaseURL           string
	retentionRequestDatabaseURL            string
	debugDumpRequestDatabaseURL            string
	debugDumpAuditRequestDatabaseURL       string
	retentionDatabaseURL                   string
	backupRetentionDatabaseURL             string
	artifactReplicationDatabaseURL         string
	platformOperatorAuthDatabaseURL        string
	breakGlassRequestDatabaseURL           string
	breakGlassAuditDatabaseURL             string
	retentionReconcilerID                  string
	retentionTick                          time.Duration
	retentionClaimTTL                      time.Duration
	retentionRetryDelay                    time.Duration
	retentionBatchSize                     int
	nonContentExpiryDatabaseURL            string
	nonContentExpiryReconcilerID           string
	nonContentExpiryTick                   time.Duration
	nonContentExpiryClaimTTL               time.Duration
	nonContentExpiryHeldRetry              time.Duration
	nonContentExpiryBatchSize              int
	artifactReplicationID                  string
	artifactReplicationTick                time.Duration
	artifactReplicationClaimTTL            time.Duration
	artifactReplicationRetryDelay          time.Duration
	artifactReplicationTimeout             time.Duration
	artifactReplicationBatchSize           int
	requestDatabaseURL                     string
	artifactRequestDatabaseURL             string
	cancelDatabaseURL                      string
	internalDatabaseURL                    string
	remediationDatabaseURL                 string
	remediationActorIdentity               string
	remediationNodeAgentsFile              string
	remediationTLSCertFile                 string
	remediationTLSKeyFile                  string
	remediationTLSRootCAFile               string
	remediationTick                        time.Duration
	remediationBatch                       int
	schedulerDatabaseURL                   string
	schedulerInboxDatabaseURL              string
	schedulerID                            string
	schedulerTick                          time.Duration
	schedulerClaimTTL                      time.Duration
	schedulerCandidateAttempts             int
	billingDatabaseURL                     string
	financeReconciliationDatabaseURL       string
	financeReconciliationAddress           string
	financeReconciliationTLSCertFile       string
	financeReconciliationTLSKeyFile        string
	financeReconciliationClientCAFile      string
	complianceDatabaseURL                  string
	complianceAddress                      string
	complianceTLSCertFile                  string
	complianceTLSKeyFile                   string
	complianceClientCAFile                 string
	webhookRequestDatabaseURL              string
	webhookDatabaseURL                     string
	webhookEncryptionActiveKeyID           string
	webhookEncryptionKeyringFile           string
	webhookDispatcherID                    string
	webhookTick                            time.Duration
	webhookClaimTTL                        time.Duration
	webhookHTTPTimeout                     time.Duration
	webhookBatchSize                       int32
	invoiceExporterID                      string
	invoiceExportEndpoint                  string
	invoiceExportTokenFile                 string
	invoiceExportTick                      time.Duration
	invoiceExportClaimTTL                  time.Duration
	invoiceExportRetryDelay                time.Duration
	invoiceExportHTTPTimeout               time.Duration
	invoiceExportBatchSize                 int32
	credentialPepper                       []byte
	oidcIssuer                             string
	oidcAudience                           string
	oidcJWKSURL                            string
	platformOIDCIssuer                     string
	platformOIDCAudience                   string
	platformOIDCJWKSURL                    string
	natsURL                                string
	natsCredentials                        string
	natsRootCA                             string
	natsOutboxAccountPublicKey             string
	natsOutboxAccountSignerPublicKeys      string
	natsOutboxUserPublicKeys               string
	natsSchedulerCredentials               string
	natsSchedulerUserPublicKeys            string
	natsClientCert                         string
	natsClientKey                          string
	publisherBatchSize                     int32
	publisherTick                          time.Duration
	cancellationTick                       time.Duration
	finalizationTick                       time.Duration
	failureTick                            time.Duration
	artifactCleanupTick                    time.Duration
	artifactS3Endpoint                     string
	artifactS3Region                       string
	artifactS3Bucket                       string
	artifactS3AccessKeyFile                string
	artifactS3SecretKeyFile                string
	artifactS3PathStyle                    bool
	artifactBackupS3Endpoint               string
	artifactBackupS3Region                 string
	artifactBackupS3Bucket                 string
	artifactBackupS3AccessKeyFile          string
	artifactBackupS3SecretKeyFile          string
	artifactBackupS3PathStyle              bool
	artifactReplicationSourceAccessKeyFile string
	artifactReplicationSourceSecretKeyFile string
	artifactReplicationBackupAccessKeyFile string
	artifactReplicationBackupSecretKeyFile string
	leaseActiveKeyID                       string
	leaseKeyringFile                       string
	executionLeaseTTL                      time.Duration
	workerLostGrace                        time.Duration
	artifactValidatorHelper                string
	artifactFFprobePath                    string
	artifactSandboxRoot                    string
	artifactSpoolDirectory                 string
	artifactFFprobeVersion                 string
	artifactValidatorRevision              string
	artifactInspectionTimeout              time.Duration
	artifactMaxInputBytes                  int64
	artifactMaxProbeBytes                  int64
	artifactMaxStderrBytes                 int64
	artifactReconcilerID                   string
	artifactOrphanMinimumAge               time.Duration
	artifactCleanupBatch                   int
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

type contentRetentionReconciler interface {
	ReconcileBatch(context.Context) (retention.ReconcileResult, error)
}

type nonContentExpiryReconciler interface {
	ReconcileBatch(context.Context) (noncontentexpiry.Result, error)
}

type artifactBackupReplicator interface {
	ReplicateBatch(context.Context) (artifactreplication.Result, error)
}

type databasePinger interface {
	Ping(context.Context) error
}

type tlsHTTPServer interface {
	ServeTLS(net.Listener, string, string) error
}

type httpServerShutdown interface {
	Shutdown(context.Context) error
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
	platformOIDCVerifier, err := identity.NewRemoteOIDCTokenVerifier(identity.OIDCVerifierConfig{
		Issuer:     configuration.platformOIDCIssuer,
		Audience:   configuration.platformOIDCAudience,
		JWKSURL:    configuration.platformOIDCJWKSURL,
		HTTPClient: &http.Client{Timeout: defaultOIDCJWKSHTTPTimeout},
	})
	if err != nil {
		return fmt.Errorf("configure Platform Operator OIDC verifier: %w", err)
	}
	if configuration.platformOIDCIssuer == configuration.oidcIssuer ||
		configuration.platformOIDCAudience == configuration.oidcAudience {
		return errors.New(
			"OIDC issuers and audiences for Platform Operator and Customer Human trust domains must each differ",
		)
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
	humanMembershipAuthPool, err := openPool(
		ctx,
		configuration.humanMembershipAuthDatabaseURL,
		5,
		veladb.RoleHumanMembershipAuth,
	)
	if err != nil {
		return fmt.Errorf("open Human membership auth database pool: %w", err)
	}
	defer humanMembershipAuthPool.Close()
	identityRequestPool, err := openPool(
		ctx,
		configuration.identityRequestDatabaseURL,
		10,
		veladb.RoleIdentityRequest,
	)
	if err != nil {
		return fmt.Errorf("open identity request database pool: %w", err)
	}
	defer identityRequestPool.Close()
	humanMembershipRequestPool, err := openPool(
		ctx,
		configuration.humanMembershipRequestDatabaseURL,
		10,
		veladb.RoleHumanMembershipRequest,
	)
	if err != nil {
		return fmt.Errorf("open Human membership request database pool: %w", err)
	}
	defer humanMembershipRequestPool.Close()
	organizationBillingPool, err := openPool(
		ctx,
		configuration.organizationBillingDatabaseURL,
		10,
		veladb.RoleOrganizationBillingRequest,
	)
	if err != nil {
		return fmt.Errorf("open Organization billing request database pool: %w", err)
	}
	defer organizationBillingPool.Close()
	organizationAuditPool, err := openPool(
		ctx,
		configuration.organizationAuditDatabaseURL,
		10,
		veladb.RoleOrganizationAuditRequest,
	)
	if err != nil {
		return fmt.Errorf("open Organization audit request database pool: %w", err)
	}
	defer organizationAuditPool.Close()
	retentionRequestPool, err := openPool(
		ctx,
		configuration.retentionRequestDatabaseURL,
		10,
		veladb.RoleRetentionRequest,
	)
	if err != nil {
		return fmt.Errorf("open retention request database pool: %w", err)
	}
	defer retentionRequestPool.Close()
	platformOperatorAuthPool, err := openPool(
		ctx,
		configuration.platformOperatorAuthDatabaseURL,
		5,
		veladb.RolePlatformOperatorAuth,
	)
	if err != nil {
		return fmt.Errorf("open Platform Operator auth database pool: %w", err)
	}
	defer platformOperatorAuthPool.Close()
	breakGlassRequestPool, err := openPool(
		ctx,
		configuration.breakGlassRequestDatabaseURL,
		10,
		veladb.RoleBreakGlassRequest,
	)
	if err != nil {
		return fmt.Errorf("open Break-glass request database pool: %w", err)
	}
	defer breakGlassRequestPool.Close()
	breakGlassAuditPool, err := openPool(
		ctx,
		configuration.breakGlassAuditDatabaseURL,
		10,
		veladb.RoleBreakGlassAuditRequest,
	)
	if err != nil {
		return fmt.Errorf("open Break-glass audit request database pool: %w", err)
	}
	defer breakGlassAuditPool.Close()
	retentionPool, err := openPool(
		ctx,
		configuration.retentionDatabaseURL,
		5,
		veladb.RoleRetention,
	)
	if err != nil {
		return fmt.Errorf("open retention database pool: %w", err)
	}
	defer retentionPool.Close()
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
	financeReconciliationPool, err := openPool(
		ctx,
		configuration.financeReconciliationDatabaseURL,
		5,
		veladb.RoleFinanceReconciliation,
	)
	if err != nil {
		return fmt.Errorf("open Finance Reconciliation database pool: %w", err)
	}
	defer financeReconciliationPool.Close()
	compliancePool, err := openPool(
		ctx,
		configuration.complianceDatabaseURL,
		5,
		veladb.RoleCompliance,
	)
	if err != nil {
		return fmt.Errorf("open Compliance database pool: %w", err)
	}
	defer compliancePool.Close()
	nonContentExpiryPool, err := openPool(
		ctx,
		configuration.nonContentExpiryDatabaseURL,
		2,
		veladb.RoleNonContentExpiry,
	)
	if err != nil {
		return fmt.Errorf("open non-content expiry database pool: %w", err)
	}
	defer nonContentExpiryPool.Close()
	backupRetentionPool, err := openPool(
		ctx,
		configuration.backupRetentionDatabaseURL,
		5,
		veladb.RoleBackupRetention,
	)
	if err != nil {
		return fmt.Errorf("open off-cluster backup retention database pool: %w", err)
	}
	defer backupRetentionPool.Close()
	artifactReplicationPool, err := openPool(
		ctx,
		configuration.artifactReplicationDatabaseURL,
		2,
		veladb.RoleArtifactReplication,
	)
	if err != nil {
		return fmt.Errorf("open Artifact backup replication database pool: %w", err)
	}
	defer artifactReplicationPool.Close()
	fleetPool, err := openPool(ctx, configuration.fleetDatabaseURL, 10, veladb.RoleFleet)
	if err != nil {
		return fmt.Errorf("open Fleet database pool: %w", err)
	}
	defer fleetPool.Close()
	remediationPool, err := openPool(ctx, configuration.remediationDatabaseURL, 10, veladb.RoleRemediation)
	if err != nil {
		return fmt.Errorf("open Remediation database pool: %w", err)
	}
	defer remediationPool.Close()
	remediationService, err := remediation.NewService(remediationPool)
	if err != nil {
		return fmt.Errorf("configure Remediation service: %w", err)
	}
	remediationEndpoints, err := readNodeAgentEndpoints(configuration.remediationNodeAgentsFile)
	if err != nil {
		return err
	}
	nodeAgents, err := nodeagent.NewStaticAgentResolver(
		remediationEndpoints,
		nodeagent.ClientTLSConfig{
			CertificatePath: configuration.remediationTLSCertFile,
			PrivateKeyPath:  configuration.remediationTLSKeyFile,
			RootCAPath:      configuration.remediationTLSRootCAFile,
		},
		configuration.remediationActorIdentity,
	)
	if err != nil {
		return fmt.Errorf("configure Node Agent resolver: %w", err)
	}
	defer func() { _ = nodeAgents.Close() }()
	remediationDispatcher, err := nodeagent.NewExecutionDispatcher(
		remediationService,
		nodeAgents,
		configuration.remediationActorIdentity,
		configuration.remediationBatch,
	)
	if err != nil {
		return fmt.Errorf("configure Remediation dispatcher: %w", err)
	}
	schedulerPool, err := openPool(ctx, configuration.schedulerDatabaseURL, 5, veladb.RoleScheduler)
	if err != nil {
		return fmt.Errorf("open Scheduler database pool: %w", err)
	}
	defer schedulerPool.Close()
	schedulerInboxPool, err := openPool(
		ctx,
		configuration.schedulerInboxDatabaseURL,
		5,
		veladb.RoleSchedulerInbox,
	)
	if err != nil {
		return fmt.Errorf("open Scheduler Inbox database pool: %w", err)
	}
	defer schedulerInboxPool.Close()
	billingPool, err := openPool(ctx, configuration.billingDatabaseURL, 5, veladb.RoleBilling)
	if err != nil {
		return fmt.Errorf("open billing database pool: %w", err)
	}
	defer billingPool.Close()
	financeReconciliationService, err := financereconciliation.NewService(
		ctx,
		financeReconciliationPool,
	)
	if err != nil {
		return fmt.Errorf("configure Finance Reconciliation service: %w", err)
	}
	financeReconciliationHandler, err := financereconciliation.NewHTTPHandler(
		financeReconciliationService,
	)
	if err != nil {
		return fmt.Errorf("configure Finance Reconciliation HTTP handler: %w", err)
	}
	financeReconciliationTLS, err := financereconciliation.NewServerTLSConfig(
		configuration.financeReconciliationTLSCertFile,
		configuration.financeReconciliationTLSKeyFile,
		configuration.financeReconciliationClientCAFile,
	)
	if err != nil {
		return err
	}
	financeReconciliationRawListener, err := net.Listen(
		"tcp",
		configuration.financeReconciliationAddress,
	)
	if err != nil {
		return fmt.Errorf("listen for Finance Reconciliation HTTPS: %w", err)
	}
	defer func() { _ = financeReconciliationRawListener.Close() }()
	complianceService, err := legalhold.NewService(ctx, compliancePool)
	if err != nil {
		return fmt.Errorf("configure Compliance Legal Hold service: %w", err)
	}
	complianceHandler, err := legalhold.NewHTTPHandler(complianceService)
	if err != nil {
		return fmt.Errorf("configure Compliance Legal Hold HTTP handler: %w", err)
	}
	complianceTLS, err := legalhold.NewServerTLSConfig(
		configuration.complianceTLSCertFile,
		configuration.complianceTLSKeyFile,
		configuration.complianceClientCAFile,
	)
	if err != nil {
		return err
	}
	complianceRawListener, err := net.Listen("tcp", configuration.complianceAddress)
	if err != nil {
		return fmt.Errorf("listen for Compliance Legal Hold HTTPS: %w", err)
	}
	defer func() { _ = complianceRawListener.Close() }()
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
	debugDumpRequestPool, err := openPool(
		ctx,
		configuration.debugDumpRequestDatabaseURL,
		10,
		veladb.RoleDebugDumpRequest,
	)
	if err != nil {
		return fmt.Errorf("open debug dump request database pool: %w", err)
	}
	defer debugDumpRequestPool.Close()
	debugDumpAuditRequestPool, err := openPool(
		ctx,
		configuration.debugDumpAuditRequestDatabaseURL,
		10,
		veladb.RoleDebugDumpAuditRequest,
	)
	if err != nil {
		return fmt.Errorf("open debug dump audit request database pool: %w", err)
	}
	defer debugDumpAuditRequestPool.Close()
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
	artifactBackupStore, err := openArtifactBackupStore(ctx, configuration)
	if err != nil {
		return err
	}
	artifactReplicationSourceStore, artifactReplicationBackupStore, err :=
		openArtifactReplicationStores(ctx, configuration)
	if err != nil {
		return err
	}
	artifactReplicator, err := artifactreplication.New(
		artifactReplicationPool,
		artifactReplicationSourceStore,
		artifactReplicationBackupStore,
		artifactreplication.Config{
			InstanceID: configuration.artifactReplicationID,
			BatchSize:  configuration.artifactReplicationBatchSize,
			ClaimTTL:   configuration.artifactReplicationClaimTTL,
			RetryDelay: configuration.artifactReplicationRetryDelay,
		},
	)
	if err != nil {
		return fmt.Errorf("configure Artifact backup Replicator: %w", err)
	}
	retentionReconciler, err := retention.NewReconciler(
		retentionPool,
		artifactStore,
		retention.ReconcilerConfig{
			InstanceID:  configuration.retentionReconcilerID,
			BatchSize:   configuration.retentionBatchSize,
			ClaimTTL:    configuration.retentionClaimTTL,
			RetryDelay:  configuration.retentionRetryDelay,
			BackupPool:  backupRetentionPool,
			BackupStore: artifactBackupStore,
		},
	)
	if err != nil {
		return fmt.Errorf("configure retention Reconciler: %w", err)
	}
	nonContentExpiryRunner, err := noncontentexpiry.New(
		nonContentExpiryPool,
		noncontentexpiry.Config{
			InstanceID: configuration.nonContentExpiryReconcilerID,
			BatchSize:  configuration.nonContentExpiryBatchSize,
			ClaimTTL:   configuration.nonContentExpiryClaimTTL,
			HeldRetry:  configuration.nonContentExpiryHeldRetry,
		},
	)
	if err != nil {
		return fmt.Errorf("configure non-content expiry Reconciler: %w", err)
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
	fleetService, err := fleet.NewService(fleetPool, internalPool)
	if err != nil {
		return fmt.Errorf("configure Fleet service: %w", err)
	}
	workerControlAdapter, err := workertransport.NewServer(
		workerIdentityResolver,
		workerCoordinator,
		artifactStore,
		fleetService,
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
	fleetMaintenanceAdapter, err := fleettransport.NewServer(
		fleetService,
		fleettransport.Config{
			SPIFFEIdentity:         configuration.fleetControllerSPIFFEIdentity,
			ActorIdentity:          configuration.fleetControllerActorIdentity,
			NodeAgentRegistrations: nodeAgentRegistrations(remediationEndpoints),
		},
	)
	if err != nil {
		return fmt.Errorf("configure Fleet maintenance gRPC adapter: %w", err)
	}
	fleetTransportCredentials, err := fleettransport.NewServerTLSCredentials(
		configuration.fleetGRPCTLSCertFile,
		configuration.fleetGRPCTLSKeyFile,
		configuration.fleetGRPCClientCAFile,
	)
	if err != nil {
		return err
	}
	fleetGRPCServer := grpc.NewServer(
		grpc.Creds(fleetTransportCredentials),
		grpc.MaxRecvMsgSize(1<<20),
		grpc.MaxSendMsgSize(1<<20),
	)
	velav1.RegisterFleetMaintenanceServiceServer(fleetGRPCServer, fleetMaintenanceAdapter)
	fleetListener, err := net.Listen("tcp", configuration.fleetGRPCAddress)
	if err != nil {
		return fmt.Errorf("listen for Fleet maintenance gRPC: %w", err)
	}
	defer func() { _ = fleetListener.Close() }()
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
	schedulerNATSConnection, err := connectSchedulerNATS(configuration)
	if err != nil {
		return err
	}
	defer schedulerNATSConnection.Close()
	schedulerInboxProcessor, err := inbox.NewSchedulerProcessor(schedulerInboxPool)
	if err != nil {
		return fmt.Errorf("configure Scheduler Inbox processor: %w", err)
	}
	schedulerMessageConsumer, err := inbox.NewJetStreamConsumer(schedulerInboxProcessor)
	if err != nil {
		return fmt.Errorf("configure Scheduler JetStream Inbox consumer: %w", err)
	}
	schedulerWakeups := make(chan schedulerCycleRequest, 1)
	broker, err := outbox.NewJetStreamBroker(natsConnection.Conn)
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
	stageGraphCancellationService, err := attemptcoordinator.NewCancellationService(internalPool)
	if err != nil {
		return fmt.Errorf("configure Stage graph cancellation service: %w", err)
	}
	identityAdministration, err := identity.NewAdministrationServiceWithHumanMembership(
		identityRequestPool,
		humanMembershipRequestPool,
		configuration.credentialPepper,
		configuration.oidcIssuer,
	)
	if err != nil {
		return fmt.Errorf("configure identity Administration service: %w", err)
	}
	organizationReporting, err := organizationreporting.NewService(
		organizationBillingPool,
		organizationAuditPool,
		debugDumpAuditRequestPool,
	)
	if err != nil {
		return fmt.Errorf("configure Organization reporting service: %w", err)
	}
	retentionService, err := retention.NewService(retentionRequestPool)
	if err != nil {
		return fmt.Errorf("configure retention service: %w", err)
	}
	debugDumpService, err := debugdump.NewService(debugDumpRequestPool, artifactStore)
	if err != nil {
		return fmt.Errorf("configure debug dump service: %w", err)
	}
	breakGlassService, err := breakglass.NewService(breakGlassRequestPool, artifactStore)
	if err != nil {
		return fmt.Errorf("configure Break-glass service: %w", err)
	}
	httpMetrics := telemetry.NewHTTPMetrics()
	if err := httpMetrics.Register(telemetry.NewSLOCollector(
		telemetry.NewPostgresSLOReportReader(internalPool),
	)); err != nil {
		return fmt.Errorf("register statistical SLO metrics: %w", err)
	}
	apiHandler, err := httpapi.NewHandler(httpapi.Config{
		Observer: httpMetrics.Middleware,
		Authenticator: identity.NewAuthenticatorWithHumanMembershipOIDC(
			authPool,
			humanAuthPool,
			humanMembershipAuthPool,
			configuration.credentialPepper,
			oidcVerifier,
		),
		PlatformAuthenticator: breakglass.NewAuthenticator(
			platformOperatorAuthPool,
			platformOIDCVerifier,
		),
		BreakGlass:             breakGlassService,
		Remediation:            remediationService,
		IdentityAdministration: identityAdministration,
		OrganizationReporting:  organizationReporting,
		Retention:              retentionService,
		DebugDumps:             debugDumpService,
		Admission:              admission.NewService(requestPool, capacityPredictor),
		Cancellation:           cancellationService,
		StageGraphCancellation: stageGraphCancellationService,
		Artifacts:              artifactaccess.NewService(artifactRequestPool, artifactStore),
		Webhooks:               webhookService,
	})
	if err != nil {
		return err
	}
	publicHTTPHandler, managementHTTPHandler := controlHTTPHandlers(
		apiHandler,
		httpMetrics.Handler(),
		readinessHandler(
			artifactStore,
			authPool,
			humanAuthPool,
			humanMembershipAuthPool,
			identityRequestPool,
			humanMembershipRequestPool,
			organizationBillingPool,
			organizationAuditPool,
			retentionRequestPool,
			debugDumpRequestPool,
			debugDumpAuditRequestPool,
			platformOperatorAuthPool,
			breakGlassRequestPool,
			breakGlassAuditPool,
			retentionPool,
			requestPool,
			artifactRequestPool,
			cancelPool,
			internalPool,
			fleetPool,
			schedulerPool,
			schedulerInboxPool,
			billingPool,
			financeReconciliationPool,
			compliancePool,
			nonContentExpiryPool,
			webhookRequestPool,
			webhookPool,
		),
	)
	httpServer := &http.Server{
		Addr:              configuration.httpAddress,
		Handler:           publicHTTPHandler,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       2 * time.Minute,
	}
	managementServer := &http.Server{
		Addr:              configuration.managementAddress,
		Handler:           managementHTTPHandler,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       5 * time.Second,
		WriteTimeout:      5 * time.Second,
		IdleTimeout:       30 * time.Second,
		MaxHeaderBytes:    8 * 1024,
	}
	financeReconciliationServer := &http.Server{
		Handler:           financeReconciliationHandler,
		TLSConfig:         financeReconciliationTLS,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       30 * time.Second,
		MaxHeaderBytes:    16 * 1024,
	}
	complianceServer := &http.Server{
		Handler:           complianceHandler,
		TLSConfig:         complianceTLS,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       30 * time.Second,
		MaxHeaderBytes:    16 * 1024,
	}

	publisherStarted := make(chan struct{})
	publisherDone := make(chan struct{})
	go func() {
		defer close(publisherDone)
		close(publisherStarted)
		runPublisher(ctx, publisher, configuration.publisherTick)
	}()
	<-publisherStarted
	if err := natsConnection.Activate(); err != nil {
		stop()
		<-publisherDone
		return err
	}
	if err := schedulerNATSConnection.Activate(); err != nil {
		stop()
		<-publisherDone
		return err
	}
	schedulerWakeupDone := make(chan struct{})
	go func() {
		defer close(schedulerWakeupDone)
		runSchedulerWakeupConsumer(
			ctx,
			schedulerNATSConnection.Conn,
			schedulerMessageConsumer,
			schedulerWakeups,
		)
	}()
	schedulerDone := make(chan struct{})
	go func() {
		defer close(schedulerDone)
		runScheduler(ctx, scheduling, configuration.schedulerTick, schedulerWakeups)
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
	retentionDone := make(chan struct{})
	go func() {
		defer close(retentionDone)
		runRetentionReconciler(ctx, retentionReconciler, configuration.retentionTick)
	}()
	nonContentExpiryDone := make(chan struct{})
	go func() {
		defer close(nonContentExpiryDone)
		runNonContentExpiryReconciler(
			ctx,
			nonContentExpiryRunner,
			configuration.nonContentExpiryTick,
		)
	}()
	artifactReplicationDone := make(chan struct{})
	go func() {
		defer close(artifactReplicationDone)
		runArtifactBackupReplicator(
			ctx,
			artifactReplicator,
			configuration.artifactReplicationTick,
			configuration.artifactReplicationTimeout,
		)
	}()
	remediationDone := make(chan struct{})
	go func() {
		defer close(remediationDone)
		remediationDispatcher.Run(ctx, configuration.remediationTick, func(result nodeagent.DispatchResult, err error) {
			if err != nil {
				slog.Warn("remediation dispatcher cycle failed", "error", err)
				return
			}
			if result.Listed != 0 || result.Recovered != 0 || result.Deferred != 0 {
				slog.Info("remediation dispatcher cycle", "listed", result.Listed, "dispatched", result.Dispatched, "recovered", result.Recovered, "deferred", result.Deferred)
			}
		})
	}()
	httpServerErrors := make(chan error, 1)
	go func() {
		slog.Info("vela-control HTTP server started", "address", configuration.httpAddress)
		httpServerErrors <- httpServer.ListenAndServe()
	}()
	managementServerErrors := make(chan error, 1)
	go func() {
		slog.Info(
			"vela-control management server started",
			"address",
			configuration.managementAddress,
		)
		managementServerErrors <- managementServer.ListenAndServe()
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
	fleetServerErrors := make(chan error, 1)
	go func() {
		slog.Info(
			"vela-control Fleet maintenance gRPC server started",
			"address",
			configuration.fleetGRPCAddress,
		)
		fleetServerErrors <- fleetGRPCServer.Serve(fleetListener)
	}()
	financeReconciliationServerErrors := serveTLSHTTPServer(
		financeReconciliationServer,
		financeReconciliationRawListener,
		func() {
			slog.Info(
				"vela-control Finance Reconciliation HTTPS server started",
				"address",
				configuration.financeReconciliationAddress,
			)
		},
	)
	complianceServerErrors := serveTLSHTTPServer(complianceServer, complianceRawListener, func() {
		slog.Info(
			"vela-control Compliance Legal Hold HTTPS server started",
			"address",
			configuration.complianceAddress,
		)
	})

	var serveErr error
	select {
	case <-ctx.Done():
	case err := <-httpServerErrors:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			stop()
			serveErr = fmt.Errorf("serve HTTP: %w", err)
		}
	case err := <-managementServerErrors:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			stop()
			serveErr = fmt.Errorf("serve management HTTP: %w", err)
		}
	case err := <-workerServerErrors:
		if err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			stop()
			serveErr = fmt.Errorf("serve Worker gRPC: %w", err)
		}
	case err := <-fleetServerErrors:
		if err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			stop()
			serveErr = fmt.Errorf("serve Fleet maintenance gRPC: %w", err)
		}
	case err := <-financeReconciliationServerErrors:
		serveErr = handleHTTPServerExit(stop, "Finance Reconciliation HTTPS", err)
	case err := <-complianceServerErrors:
		serveErr = handleHTTPServerExit(stop, "Compliance Legal Hold HTTPS", err)
	}
	shutdownContext, cancelShutdown := context.WithTimeout(context.Background(), defaultShutdownTimeout)
	defer cancelShutdown()
	if err := shutdownHTTPServer(shutdownContext, complianceServer, "Compliance Legal Hold HTTPS"); err != nil {
		return err
	}
	if err := shutdownHTTPServer(shutdownContext, financeReconciliationServer, "Finance Reconciliation HTTPS"); err != nil {
		return err
	}
	if err := shutdownHTTPServer(shutdownContext, httpServer, "HTTP"); err != nil {
		return err
	}
	if err := shutdownHTTPServer(shutdownContext, managementServer, "management HTTP"); err != nil {
		return err
	}
	fleetServerDone := make(chan struct{})
	go func() {
		defer close(fleetServerDone)
		fleetGRPCServer.GracefulStop()
	}()
	select {
	case <-fleetServerDone:
	case <-shutdownContext.Done():
		fleetGRPCServer.Stop()
		return errors.New("fleet maintenance gRPC server did not stop before shutdown deadline")
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
	case <-schedulerWakeupDone:
	case <-shutdownContext.Done():
		return errors.New("scheduler JetStream wakeup consumer did not stop before shutdown deadline")
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
	select {
	case <-retentionDone:
	case <-shutdownContext.Done():
		return errors.New("retention Reconciler did not stop before shutdown deadline")
	}
	select {
	case <-nonContentExpiryDone:
	case <-shutdownContext.Done():
		return errors.New("non-content expiry Reconciler did not stop before shutdown deadline")
	}
	select {
	case <-artifactReplicationDone:
	case <-shutdownContext.Done():
		return errors.New("artifact backup Replicator did not stop before shutdown deadline")
	}
	select {
	case <-remediationDone:
	case <-shutdownContext.Done():
		return errors.New("remediation dispatcher did not stop before shutdown deadline")
	}
	if err := natsConnection.Drain(); err != nil && !errors.Is(err, nats.ErrConnectionClosed) {
		return fmt.Errorf("drain NATS connection: %w", err)
	}
	if err := schedulerNATSConnection.Drain(); err != nil && !errors.Is(err, nats.ErrConnectionClosed) {
		return fmt.Errorf("drain NATS Scheduler connection: %w", err)
	}
	if serveErr != nil {
		return serveErr
	}
	return nil
}

func loadConfig() (config, error) {
	configuration := config{
		httpAddress:                       envOrDefault("VELA_HTTP_ADDRESS", defaultHTTPAddress),
		managementAddress:                 envOrDefault("VELA_MANAGEMENT_ADDRESS", defaultManagementAddress),
		workerGRPCAddress:                 envOrDefault("VELA_WORKER_GRPC_ADDRESS", defaultWorkerGRPCAddress),
		workerGRPCTLSCertFile:             os.Getenv("VELA_WORKER_GRPC_TLS_CERT_FILE"),
		workerGRPCTLSKeyFile:              os.Getenv("VELA_WORKER_GRPC_TLS_KEY_FILE"),
		workerGRPCClientCAFile:            os.Getenv("VELA_WORKER_GRPC_CLIENT_CA_FILE"),
		fleetGRPCAddress:                  envOrDefault("VELA_FLEET_GRPC_ADDRESS", defaultFleetGRPCAddress),
		fleetGRPCTLSCertFile:              os.Getenv("VELA_FLEET_GRPC_TLS_CERT_FILE"),
		fleetGRPCTLSKeyFile:               os.Getenv("VELA_FLEET_GRPC_TLS_KEY_FILE"),
		fleetGRPCClientCAFile:             os.Getenv("VELA_FLEET_GRPC_CLIENT_CA_FILE"),
		fleetDatabaseURL:                  os.Getenv("VELA_FLEET_DATABASE_URL"),
		fleetControllerSPIFFEIdentity:     os.Getenv("VELA_FLEET_CONTROLLER_SPIFFE_ID"),
		fleetControllerActorIdentity:      os.Getenv("VELA_FLEET_CONTROLLER_ACTOR_IDENTITY"),
		authDatabaseURL:                   os.Getenv("VELA_AUTH_DATABASE_URL"),
		humanAuthDatabaseURL:              os.Getenv("VELA_HUMAN_AUTH_DATABASE_URL"),
		humanMembershipAuthDatabaseURL:    os.Getenv("VELA_HUMAN_MEMBERSHIP_AUTH_DATABASE_URL"),
		identityRequestDatabaseURL:        os.Getenv("VELA_IDENTITY_REQUEST_DATABASE_URL"),
		humanMembershipRequestDatabaseURL: os.Getenv("VELA_HUMAN_MEMBERSHIP_REQUEST_DATABASE_URL"),
		organizationBillingDatabaseURL:    os.Getenv("VELA_ORGANIZATION_BILLING_REQUEST_DATABASE_URL"),
		organizationAuditDatabaseURL:      os.Getenv("VELA_ORGANIZATION_AUDIT_REQUEST_DATABASE_URL"),
		retentionRequestDatabaseURL:       os.Getenv("VELA_RETENTION_REQUEST_DATABASE_URL"),
		debugDumpRequestDatabaseURL:       os.Getenv("VELA_DEBUG_DUMP_REQUEST_DATABASE_URL"),
		debugDumpAuditRequestDatabaseURL:  os.Getenv("VELA_DEBUG_DUMP_AUDIT_REQUEST_DATABASE_URL"),
		retentionDatabaseURL:              os.Getenv("VELA_RETENTION_DATABASE_URL"),
		backupRetentionDatabaseURL:        os.Getenv("VELA_BACKUP_RETENTION_DATABASE_URL"),
		artifactReplicationDatabaseURL:    os.Getenv("VELA_ARTIFACT_REPLICATION_DATABASE_URL"),
		platformOperatorAuthDatabaseURL:   os.Getenv("VELA_PLATFORM_OPERATOR_AUTH_DATABASE_URL"),
		breakGlassRequestDatabaseURL:      os.Getenv("VELA_BREAK_GLASS_REQUEST_DATABASE_URL"),
		breakGlassAuditDatabaseURL:        os.Getenv("VELA_BREAK_GLASS_AUDIT_DATABASE_URL"),
		retentionReconcilerID:             os.Getenv("VELA_RETENTION_RECONCILER_ID"),
		retentionTick:                     defaultRetentionTick,
		retentionClaimTTL:                 defaultRetentionClaimTTL,
		retentionRetryDelay:               defaultRetentionRetryDelay,
		retentionBatchSize:                defaultRetentionBatchSize,
		nonContentExpiryDatabaseURL:       os.Getenv("VELA_NON_CONTENT_EXPIRY_DATABASE_URL"),
		nonContentExpiryReconcilerID:      os.Getenv("VELA_NON_CONTENT_EXPIRY_RECONCILER_ID"),
		nonContentExpiryTick:              defaultNonContentExpiryTick,
		nonContentExpiryClaimTTL:          defaultNonContentExpiryClaimTTL,
		nonContentExpiryHeldRetry:         defaultNonContentExpiryHeldRetry,
		nonContentExpiryBatchSize:         defaultNonContentExpiryBatchSize,
		artifactReplicationID:             os.Getenv("VELA_ARTIFACT_REPLICATION_ID"),
		artifactReplicationTick:           defaultArtifactReplicationTick,
		artifactReplicationClaimTTL:       defaultArtifactReplicationClaimTTL,
		artifactReplicationRetryDelay:     defaultArtifactReplicationRetryDelay,
		artifactReplicationTimeout:        defaultArtifactReplicationTimeout,
		artifactReplicationBatchSize:      defaultArtifactReplicationBatchSize,
		requestDatabaseURL:                os.Getenv("VELA_REQUEST_DATABASE_URL"),
		artifactRequestDatabaseURL:        os.Getenv("VELA_ARTIFACT_REQUEST_DATABASE_URL"),
		oidcIssuer:                        os.Getenv("VELA_OIDC_ISSUER"),
		oidcAudience:                      os.Getenv("VELA_OIDC_AUDIENCE"),
		oidcJWKSURL:                       os.Getenv("VELA_OIDC_JWKS_URL"),
		platformOIDCIssuer:                os.Getenv("VELA_PLATFORM_OIDC_ISSUER"),
		platformOIDCAudience:              os.Getenv("VELA_PLATFORM_OIDC_AUDIENCE"),
		platformOIDCJWKSURL:               os.Getenv("VELA_PLATFORM_OIDC_JWKS_URL"),
		cancelDatabaseURL:                 os.Getenv("VELA_CANCEL_DATABASE_URL"),
		internalDatabaseURL:               os.Getenv("VELA_INTERNAL_DATABASE_URL"),
		remediationDatabaseURL:            os.Getenv("VELA_REMEDIATION_DATABASE_URL"),
		remediationActorIdentity:          os.Getenv("VELA_REMEDIATION_ACTOR_IDENTITY"),
		remediationNodeAgentsFile:         os.Getenv("VELA_REMEDIATION_NODE_AGENTS_FILE"),
		remediationTLSCertFile:            os.Getenv("VELA_REMEDIATION_TLS_CERT_FILE"),
		remediationTLSKeyFile:             os.Getenv("VELA_REMEDIATION_TLS_KEY_FILE"),
		remediationTLSRootCAFile:          os.Getenv("VELA_REMEDIATION_TLS_ROOT_CA_FILE"),
		remediationTick:                   defaultRemediationTick,
		remediationBatch:                  defaultRemediationBatch,
		schedulerDatabaseURL:              os.Getenv("VELA_SCHEDULER_DATABASE_URL"),
		schedulerInboxDatabaseURL:         os.Getenv("VELA_SCHEDULER_INBOX_DATABASE_URL"),
		schedulerID:                       os.Getenv("VELA_SCHEDULER_ID"),
		schedulerTick:                     defaultSchedulerTick,
		schedulerClaimTTL:                 defaultSchedulerClaimTTL,
		schedulerCandidateAttempts:        defaultSchedulerCandidateAttempts,
		billingDatabaseURL:                os.Getenv("VELA_BILLING_DATABASE_URL"),
		financeReconciliationDatabaseURL:  os.Getenv("VELA_FINANCE_RECONCILIATION_DATABASE_URL"),
		financeReconciliationAddress:      os.Getenv("VELA_FINANCE_RECONCILIATION_ADDR"),
		financeReconciliationTLSCertFile:  os.Getenv("VELA_FINANCE_RECONCILIATION_SERVER_CERT_FILE"),
		financeReconciliationTLSKeyFile:   os.Getenv("VELA_FINANCE_RECONCILIATION_SERVER_KEY_FILE"),
		financeReconciliationClientCAFile: os.Getenv("VELA_FINANCE_RECONCILIATION_CLIENT_CA_FILE"),
		complianceDatabaseURL:             os.Getenv("VELA_COMPLIANCE_DATABASE_URL"),
		complianceAddress:                 os.Getenv("VELA_COMPLIANCE_ADDR"),
		complianceTLSCertFile:             os.Getenv("VELA_COMPLIANCE_SERVER_CERT_FILE"),
		complianceTLSKeyFile:              os.Getenv("VELA_COMPLIANCE_SERVER_KEY_FILE"),
		complianceClientCAFile:            os.Getenv("VELA_COMPLIANCE_CLIENT_CA_FILE"),
		webhookRequestDatabaseURL:         os.Getenv("VELA_WEBHOOK_REQUEST_DATABASE_URL"),
		webhookDatabaseURL:                os.Getenv("VELA_WEBHOOK_DATABASE_URL"),
		webhookEncryptionActiveKeyID:      os.Getenv("VELA_WEBHOOK_ENCRYPTION_ACTIVE_KEY_ID"),
		webhookEncryptionKeyringFile:      os.Getenv("VELA_WEBHOOK_ENCRYPTION_KEYRING_FILE"),
		webhookDispatcherID:               os.Getenv("VELA_WEBHOOK_DISPATCHER_ID"),
		webhookTick:                       defaultWebhookTick,
		webhookClaimTTL:                   defaultWebhookClaimTTL,
		webhookHTTPTimeout:                defaultWebhookHTTPTimeout,
		webhookBatchSize:                  defaultWebhookBatchSize,
		invoiceExporterID:                 os.Getenv("VELA_INVOICE_EXPORTER_ID"),
		invoiceExportEndpoint:             os.Getenv("VELA_INVOICE_EXPORT_ENDPOINT"),
		invoiceExportTokenFile:            os.Getenv("VELA_INVOICE_EXPORT_BEARER_TOKEN_FILE"),
		invoiceExportTick:                 defaultInvoiceExportTick,
		invoiceExportClaimTTL:             defaultInvoiceExportClaimTTL,
		invoiceExportRetryDelay:           defaultInvoiceExportRetryDelay,
		invoiceExportHTTPTimeout:          defaultInvoiceExportHTTPTimeout,
		invoiceExportBatchSize:            defaultInvoiceExportBatchSize,
		natsURL:                           os.Getenv("VELA_NATS_URL"),
		natsCredentials:                   os.Getenv("VELA_NATS_CREDENTIALS_FILE"),
		natsRootCA:                        os.Getenv("VELA_NATS_ROOT_CA_FILE"),
		natsOutboxAccountPublicKey:        os.Getenv("VELA_NATS_OUTBOX_ACCOUNT_PUBLIC_KEY"),
		natsOutboxAccountSignerPublicKeys: os.Getenv("VELA_NATS_OUTBOX_ACCOUNT_SIGNER_PUBLIC_KEYS"),
		natsOutboxUserPublicKeys:          os.Getenv("VELA_NATS_OUTBOX_USER_PUBLIC_KEYS"),
		natsSchedulerCredentials:          os.Getenv("VELA_NATS_SCHEDULER_CREDENTIALS_FILE"),
		natsSchedulerUserPublicKeys:       os.Getenv("VELA_NATS_SCHEDULER_USER_PUBLIC_KEYS"),
		natsClientCert:                    os.Getenv("VELA_NATS_CLIENT_CERT_FILE"),
		natsClientKey:                     os.Getenv("VELA_NATS_CLIENT_KEY_FILE"),
		artifactS3Endpoint:                os.Getenv("VELA_ARTIFACT_S3_ENDPOINT"),
		artifactS3Region:                  os.Getenv("VELA_ARTIFACT_S3_REGION"),
		artifactS3Bucket:                  os.Getenv("VELA_ARTIFACT_S3_BUCKET"),
		artifactS3AccessKeyFile:           os.Getenv("VELA_ARTIFACT_S3_ACCESS_KEY_ID_FILE"),
		artifactS3SecretKeyFile:           os.Getenv("VELA_ARTIFACT_S3_SECRET_ACCESS_KEY_FILE"),
		artifactBackupS3Endpoint:          os.Getenv("VELA_ARTIFACT_BACKUP_S3_ENDPOINT"),
		artifactBackupS3Region:            os.Getenv("VELA_ARTIFACT_BACKUP_S3_REGION"),
		artifactBackupS3Bucket:            os.Getenv("VELA_ARTIFACT_BACKUP_S3_BUCKET"),
		artifactBackupS3AccessKeyFile:     os.Getenv("VELA_ARTIFACT_BACKUP_S3_ACCESS_KEY_ID_FILE"),
		artifactBackupS3SecretKeyFile:     os.Getenv("VELA_ARTIFACT_BACKUP_S3_SECRET_ACCESS_KEY_FILE"),
		artifactReplicationSourceAccessKeyFile: os.Getenv(
			"VELA_ARTIFACT_REPLICATION_SOURCE_S3_ACCESS_KEY_ID_FILE",
		),
		artifactReplicationSourceSecretKeyFile: os.Getenv(
			"VELA_ARTIFACT_REPLICATION_SOURCE_S3_SECRET_ACCESS_KEY_FILE",
		),
		artifactReplicationBackupAccessKeyFile: os.Getenv(
			"VELA_ARTIFACT_REPLICATION_BACKUP_S3_ACCESS_KEY_ID_FILE",
		),
		artifactReplicationBackupSecretKeyFile: os.Getenv(
			"VELA_ARTIFACT_REPLICATION_BACKUP_S3_SECRET_ACCESS_KEY_FILE",
		),
		publisherBatchSize:        defaultPublisherBatch,
		publisherTick:             defaultPublisherTick,
		cancellationTick:          defaultCancellationReconciliationTick,
		finalizationTick:          defaultFinalizationReconciliationTick,
		failureTick:               defaultFailureReconciliationTick,
		artifactCleanupTick:       defaultArtifactCleanupTick,
		leaseActiveKeyID:          os.Getenv("VELA_LEASE_ACTIVE_KEY_ID"),
		leaseKeyringFile:          os.Getenv("VELA_LEASE_KEYRING_FILE"),
		executionLeaseTTL:         defaultExecutionLeaseTTL,
		workerLostGrace:           defaultWorkerLostGrace,
		artifactValidatorHelper:   os.Getenv("VELA_ARTIFACT_VALIDATOR_HELPER_PATH"),
		artifactFFprobePath:       os.Getenv("VELA_ARTIFACT_FFPROBE_PATH"),
		artifactSandboxRoot:       os.Getenv("VELA_ARTIFACT_SANDBOX_ROOT"),
		artifactSpoolDirectory:    os.Getenv("VELA_ARTIFACT_SPOOL_DIRECTORY"),
		artifactFFprobeVersion:    os.Getenv("VELA_ARTIFACT_FFPROBE_VERSION"),
		artifactValidatorRevision: os.Getenv("VELA_ARTIFACT_VALIDATOR_REVISION"),
		artifactInspectionTimeout: defaultArtifactInspectionTimeout,
		artifactMaxInputBytes:     defaultArtifactMaxInputBytes,
		artifactMaxProbeBytes:     defaultArtifactMaxProbeBytes,
		artifactMaxStderrBytes:    defaultArtifactMaxStderrBytes,
		artifactReconcilerID:      os.Getenv("VELA_ARTIFACT_RECONCILER_ID"),
		artifactOrphanMinimumAge:  defaultArtifactOrphanMinimumAge,
		artifactCleanupBatch:      defaultArtifactCleanupBatch,
	}
	for name, value := range map[string]string{
		"VELA_AUTH_DATABASE_URL":                                     configuration.authDatabaseURL,
		"VELA_HUMAN_AUTH_DATABASE_URL":                               configuration.humanAuthDatabaseURL,
		"VELA_HUMAN_MEMBERSHIP_AUTH_DATABASE_URL":                    configuration.humanMembershipAuthDatabaseURL,
		"VELA_IDENTITY_REQUEST_DATABASE_URL":                         configuration.identityRequestDatabaseURL,
		"VELA_HUMAN_MEMBERSHIP_REQUEST_DATABASE_URL":                 configuration.humanMembershipRequestDatabaseURL,
		"VELA_ORGANIZATION_BILLING_REQUEST_DATABASE_URL":             configuration.organizationBillingDatabaseURL,
		"VELA_ORGANIZATION_AUDIT_REQUEST_DATABASE_URL":               configuration.organizationAuditDatabaseURL,
		"VELA_RETENTION_REQUEST_DATABASE_URL":                        configuration.retentionRequestDatabaseURL,
		"VELA_DEBUG_DUMP_REQUEST_DATABASE_URL":                       configuration.debugDumpRequestDatabaseURL,
		"VELA_DEBUG_DUMP_AUDIT_REQUEST_DATABASE_URL":                 configuration.debugDumpAuditRequestDatabaseURL,
		"VELA_RETENTION_DATABASE_URL":                                configuration.retentionDatabaseURL,
		"VELA_BACKUP_RETENTION_DATABASE_URL":                         configuration.backupRetentionDatabaseURL,
		"VELA_ARTIFACT_REPLICATION_DATABASE_URL":                     configuration.artifactReplicationDatabaseURL,
		"VELA_PLATFORM_OPERATOR_AUTH_DATABASE_URL":                   configuration.platformOperatorAuthDatabaseURL,
		"VELA_BREAK_GLASS_REQUEST_DATABASE_URL":                      configuration.breakGlassRequestDatabaseURL,
		"VELA_BREAK_GLASS_AUDIT_DATABASE_URL":                        configuration.breakGlassAuditDatabaseURL,
		"VELA_RETENTION_RECONCILER_ID":                               configuration.retentionReconcilerID,
		"VELA_NON_CONTENT_EXPIRY_DATABASE_URL":                       configuration.nonContentExpiryDatabaseURL,
		"VELA_NON_CONTENT_EXPIRY_RECONCILER_ID":                      configuration.nonContentExpiryReconcilerID,
		"VELA_ARTIFACT_REPLICATION_ID":                               configuration.artifactReplicationID,
		"VELA_REQUEST_DATABASE_URL":                                  configuration.requestDatabaseURL,
		"VELA_ARTIFACT_REQUEST_DATABASE_URL":                         configuration.artifactRequestDatabaseURL,
		"VELA_OIDC_ISSUER":                                           configuration.oidcIssuer,
		"VELA_OIDC_AUDIENCE":                                         configuration.oidcAudience,
		"VELA_OIDC_JWKS_URL":                                         configuration.oidcJWKSURL,
		"VELA_PLATFORM_OIDC_ISSUER":                                  configuration.platformOIDCIssuer,
		"VELA_PLATFORM_OIDC_AUDIENCE":                                configuration.platformOIDCAudience,
		"VELA_PLATFORM_OIDC_JWKS_URL":                                configuration.platformOIDCJWKSURL,
		"VELA_CANCEL_DATABASE_URL":                                   configuration.cancelDatabaseURL,
		"VELA_INTERNAL_DATABASE_URL":                                 configuration.internalDatabaseURL,
		"VELA_REMEDIATION_DATABASE_URL":                              configuration.remediationDatabaseURL,
		"VELA_REMEDIATION_ACTOR_IDENTITY":                            configuration.remediationActorIdentity,
		"VELA_REMEDIATION_NODE_AGENTS_FILE":                          configuration.remediationNodeAgentsFile,
		"VELA_REMEDIATION_TLS_CERT_FILE":                             configuration.remediationTLSCertFile,
		"VELA_REMEDIATION_TLS_KEY_FILE":                              configuration.remediationTLSKeyFile,
		"VELA_REMEDIATION_TLS_ROOT_CA_FILE":                          configuration.remediationTLSRootCAFile,
		"VELA_SCHEDULER_DATABASE_URL":                                configuration.schedulerDatabaseURL,
		"VELA_SCHEDULER_INBOX_DATABASE_URL":                          configuration.schedulerInboxDatabaseURL,
		"VELA_SCHEDULER_ID":                                          configuration.schedulerID,
		"VELA_BILLING_DATABASE_URL":                                  configuration.billingDatabaseURL,
		"VELA_FINANCE_RECONCILIATION_DATABASE_URL":                   configuration.financeReconciliationDatabaseURL,
		"VELA_FINANCE_RECONCILIATION_ADDR":                           configuration.financeReconciliationAddress,
		"VELA_FINANCE_RECONCILIATION_SERVER_CERT_FILE":               configuration.financeReconciliationTLSCertFile,
		"VELA_FINANCE_RECONCILIATION_SERVER_KEY_FILE":                configuration.financeReconciliationTLSKeyFile,
		"VELA_FINANCE_RECONCILIATION_CLIENT_CA_FILE":                 configuration.financeReconciliationClientCAFile,
		"VELA_COMPLIANCE_DATABASE_URL":                               configuration.complianceDatabaseURL,
		"VELA_COMPLIANCE_ADDR":                                       configuration.complianceAddress,
		"VELA_COMPLIANCE_SERVER_CERT_FILE":                           configuration.complianceTLSCertFile,
		"VELA_COMPLIANCE_SERVER_KEY_FILE":                            configuration.complianceTLSKeyFile,
		"VELA_COMPLIANCE_CLIENT_CA_FILE":                             configuration.complianceClientCAFile,
		"VELA_WEBHOOK_REQUEST_DATABASE_URL":                          configuration.webhookRequestDatabaseURL,
		"VELA_WEBHOOK_DATABASE_URL":                                  configuration.webhookDatabaseURL,
		"VELA_WEBHOOK_ENCRYPTION_ACTIVE_KEY_ID":                      configuration.webhookEncryptionActiveKeyID,
		"VELA_WEBHOOK_ENCRYPTION_KEYRING_FILE":                       configuration.webhookEncryptionKeyringFile,
		"VELA_WEBHOOK_DISPATCHER_ID":                                 configuration.webhookDispatcherID,
		"VELA_INVOICE_EXPORTER_ID":                                   configuration.invoiceExporterID,
		"VELA_INVOICE_EXPORT_ENDPOINT":                               configuration.invoiceExportEndpoint,
		"VELA_INVOICE_EXPORT_BEARER_TOKEN_FILE":                      configuration.invoiceExportTokenFile,
		"VELA_NATS_URL":                                              configuration.natsURL,
		"VELA_NATS_CREDENTIALS_FILE":                                 configuration.natsCredentials,
		"VELA_NATS_ROOT_CA_FILE":                                     configuration.natsRootCA,
		"VELA_NATS_OUTBOX_ACCOUNT_PUBLIC_KEY":                        configuration.natsOutboxAccountPublicKey,
		"VELA_NATS_OUTBOX_ACCOUNT_SIGNER_PUBLIC_KEYS":                configuration.natsOutboxAccountSignerPublicKeys,
		"VELA_NATS_OUTBOX_USER_PUBLIC_KEYS":                          configuration.natsOutboxUserPublicKeys,
		"VELA_NATS_SCHEDULER_CREDENTIALS_FILE":                       configuration.natsSchedulerCredentials,
		"VELA_NATS_SCHEDULER_USER_PUBLIC_KEYS":                       configuration.natsSchedulerUserPublicKeys,
		"VELA_ARTIFACT_S3_ENDPOINT":                                  configuration.artifactS3Endpoint,
		"VELA_ARTIFACT_S3_REGION":                                    configuration.artifactS3Region,
		"VELA_ARTIFACT_S3_BUCKET":                                    configuration.artifactS3Bucket,
		"VELA_ARTIFACT_S3_ACCESS_KEY_ID_FILE":                        configuration.artifactS3AccessKeyFile,
		"VELA_ARTIFACT_S3_SECRET_ACCESS_KEY_FILE":                    configuration.artifactS3SecretKeyFile,
		"VELA_ARTIFACT_BACKUP_S3_ENDPOINT":                           configuration.artifactBackupS3Endpoint,
		"VELA_ARTIFACT_BACKUP_S3_REGION":                             configuration.artifactBackupS3Region,
		"VELA_ARTIFACT_BACKUP_S3_BUCKET":                             configuration.artifactBackupS3Bucket,
		"VELA_ARTIFACT_BACKUP_S3_ACCESS_KEY_ID_FILE":                 configuration.artifactBackupS3AccessKeyFile,
		"VELA_ARTIFACT_BACKUP_S3_SECRET_ACCESS_KEY_FILE":             configuration.artifactBackupS3SecretKeyFile,
		"VELA_ARTIFACT_REPLICATION_SOURCE_S3_ACCESS_KEY_ID_FILE":     configuration.artifactReplicationSourceAccessKeyFile,
		"VELA_ARTIFACT_REPLICATION_SOURCE_S3_SECRET_ACCESS_KEY_FILE": configuration.artifactReplicationSourceSecretKeyFile,
		"VELA_ARTIFACT_REPLICATION_BACKUP_S3_ACCESS_KEY_ID_FILE":     configuration.artifactReplicationBackupAccessKeyFile,
		"VELA_ARTIFACT_REPLICATION_BACKUP_S3_SECRET_ACCESS_KEY_FILE": configuration.artifactReplicationBackupSecretKeyFile,
		"VELA_LEASE_ACTIVE_KEY_ID":                                   configuration.leaseActiveKeyID,
		"VELA_LEASE_KEYRING_FILE":                                    configuration.leaseKeyringFile,
		"VELA_ARTIFACT_VALIDATOR_HELPER_PATH":                        configuration.artifactValidatorHelper,
		"VELA_ARTIFACT_FFPROBE_PATH":                                 configuration.artifactFFprobePath,
		"VELA_ARTIFACT_SANDBOX_ROOT":                                 configuration.artifactSandboxRoot,
		"VELA_ARTIFACT_SPOOL_DIRECTORY":                              configuration.artifactSpoolDirectory,
		"VELA_ARTIFACT_FFPROBE_VERSION":                              configuration.artifactFFprobeVersion,
		"VELA_ARTIFACT_VALIDATOR_REVISION":                           configuration.artifactValidatorRevision,
		"VELA_ARTIFACT_RECONCILER_ID":                                configuration.artifactReconcilerID,
		"VELA_WORKER_GRPC_TLS_CERT_FILE":                             configuration.workerGRPCTLSCertFile,
		"VELA_WORKER_GRPC_TLS_KEY_FILE":                              configuration.workerGRPCTLSKeyFile,
		"VELA_WORKER_GRPC_CLIENT_CA_FILE":                            configuration.workerGRPCClientCAFile,
		"VELA_FLEET_DATABASE_URL":                                    configuration.fleetDatabaseURL,
		"VELA_FLEET_GRPC_TLS_CERT_FILE":                              configuration.fleetGRPCTLSCertFile,
		"VELA_FLEET_GRPC_TLS_KEY_FILE":                               configuration.fleetGRPCTLSKeyFile,
		"VELA_FLEET_GRPC_CLIENT_CA_FILE":                             configuration.fleetGRPCClientCAFile,
		"VELA_FLEET_CONTROLLER_SPIFFE_ID":                            configuration.fleetControllerSPIFFEIdentity,
		"VELA_FLEET_CONTROLLER_ACTOR_IDENTITY":                       configuration.fleetControllerActorIdentity,
	} {
		if value == "" {
			return config{}, fmt.Errorf("%s is required", name)
		}
	}
	for name, value := range map[string]string{
		"VELA_REMEDIATION_NODE_AGENTS_FILE": configuration.remediationNodeAgentsFile,
		"VELA_REMEDIATION_TLS_CERT_FILE":    configuration.remediationTLSCertFile,
		"VELA_REMEDIATION_TLS_KEY_FILE":     configuration.remediationTLSKeyFile,
		"VELA_REMEDIATION_TLS_ROOT_CA_FILE": configuration.remediationTLSRootCAFile,
	} {
		if !filepath.IsAbs(filepath.Clean(value)) {
			return config{}, fmt.Errorf("%s must be an absolute path", name)
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
	if value := os.Getenv("VELA_ARTIFACT_BACKUP_S3_PATH_STYLE"); value != "" {
		pathStyle, err := strconv.ParseBool(value)
		if err != nil {
			return config{}, errors.New("environment variable VELA_ARTIFACT_BACKUP_S3_PATH_STYLE must be true or false")
		}
		configuration.artifactBackupS3PathStyle = pathStyle
	}
	financeHost, financePortText, err := net.SplitHostPort(configuration.financeReconciliationAddress)
	if err != nil || financeHost == "" {
		return config{}, errors.New("environment variable VELA_FINANCE_RECONCILIATION_ADDR must contain a concrete host and port")
	}
	if address, parseErr := netip.ParseAddr(financeHost); parseErr == nil && address.IsUnspecified() {
		return config{}, errors.New("environment variable VELA_FINANCE_RECONCILIATION_ADDR must contain a concrete host and port")
	}
	financePort, err := strconv.Atoi(financePortText)
	if err != nil || financePort < 1 || financePort > 65535 {
		return config{}, errors.New("environment variable VELA_FINANCE_RECONCILIATION_ADDR must contain a concrete host and port")
	}
	complianceHost, compliancePortText, err := net.SplitHostPort(configuration.complianceAddress)
	if err != nil || complianceHost == "" {
		return config{}, errors.New("environment variable VELA_COMPLIANCE_ADDR must contain a concrete host and port")
	}
	if address, parseErr := netip.ParseAddr(complianceHost); parseErr == nil && address.IsUnspecified() {
		return config{}, errors.New("environment variable VELA_COMPLIANCE_ADDR must contain a concrete host and port")
	}
	compliancePort, err := strconv.Atoi(compliancePortText)
	if err != nil || compliancePort < 1 || compliancePort > 65535 {
		return config{}, errors.New("environment variable VELA_COMPLIANCE_ADDR must contain a concrete host and port")
	}
	if value := os.Getenv("VELA_OUTBOX_BATCH_SIZE"); value != "" {
		batchSize, err := strconv.ParseInt(value, 10, 32)
		if err != nil || batchSize < 1 || batchSize > 1000 {
			return config{}, errors.New("environment variable VELA_OUTBOX_BATCH_SIZE must be between 1 and 1000")
		}
		configuration.publisherBatchSize = int32(batchSize)
	}
	if value := os.Getenv("VELA_REMEDIATION_TICK"); value != "" {
		tick, err := time.ParseDuration(value)
		if err != nil || tick <= 0 || tick > time.Minute {
			return config{}, errors.New("environment variable VELA_REMEDIATION_TICK must be in (0, 1m]")
		}
		configuration.remediationTick = tick
	}
	if value := os.Getenv("VELA_REMEDIATION_BATCH_SIZE"); value != "" {
		batchSize, err := strconv.Atoi(value)
		if err != nil || batchSize < 1 || batchSize > 1000 {
			return config{}, errors.New("environment variable VELA_REMEDIATION_BATCH_SIZE must be between 1 and 1000")
		}
		configuration.remediationBatch = batchSize
	}
	if value := os.Getenv("VELA_RETENTION_TICK"); value != "" {
		tick, err := time.ParseDuration(value)
		if err != nil || tick <= 0 || tick > time.Minute {
			return config{}, errors.New("environment variable VELA_RETENTION_TICK must be in (0, 1m]")
		}
		configuration.retentionTick = tick
	}
	if value := os.Getenv("VELA_RETENTION_CLAIM_TTL"); value != "" {
		claimTTL, err := time.ParseDuration(value)
		if err != nil || claimTTL < time.Second || claimTTL > time.Hour ||
			claimTTL%time.Second != 0 {
			return config{}, errors.New("environment variable VELA_RETENTION_CLAIM_TTL must be in [1s, 1h]")
		}
		configuration.retentionClaimTTL = claimTTL
	}
	if value := os.Getenv("VELA_RETENTION_RETRY_DELAY"); value != "" {
		retryDelay, err := time.ParseDuration(value)
		if err != nil || retryDelay < time.Second || retryDelay > 24*time.Hour ||
			retryDelay%time.Second != 0 {
			return config{}, errors.New("environment variable VELA_RETENTION_RETRY_DELAY must be in [1s, 24h]")
		}
		configuration.retentionRetryDelay = retryDelay
	}
	if value := os.Getenv("VELA_RETENTION_BATCH_SIZE"); value != "" {
		batchSize, err := strconv.Atoi(value)
		if err != nil || batchSize < 1 || batchSize > 1000 {
			return config{}, errors.New("environment variable VELA_RETENTION_BATCH_SIZE must be in 1..1000")
		}
		configuration.retentionBatchSize = batchSize
	}
	if value := os.Getenv("VELA_NON_CONTENT_EXPIRY_TICK"); value != "" {
		tick, err := time.ParseDuration(value)
		if err != nil || tick <= 0 || tick > time.Hour {
			return config{}, errors.New("environment variable VELA_NON_CONTENT_EXPIRY_TICK must be in (0, 1h]")
		}
		configuration.nonContentExpiryTick = tick
	}
	if value := os.Getenv("VELA_NON_CONTENT_EXPIRY_CLAIM_TTL"); value != "" {
		claimTTL, err := time.ParseDuration(value)
		if err != nil || claimTTL < time.Second || claimTTL > time.Hour ||
			claimTTL%time.Second != 0 {
			return config{}, errors.New("environment variable VELA_NON_CONTENT_EXPIRY_CLAIM_TTL must be in [1s, 1h]")
		}
		configuration.nonContentExpiryClaimTTL = claimTTL
	}
	if value := os.Getenv("VELA_NON_CONTENT_EXPIRY_HELD_RETRY"); value != "" {
		heldRetry, err := time.ParseDuration(value)
		if err != nil || heldRetry < time.Second || heldRetry > 24*time.Hour ||
			heldRetry%time.Second != 0 {
			return config{}, errors.New("environment variable VELA_NON_CONTENT_EXPIRY_HELD_RETRY must be in [1s, 24h]")
		}
		configuration.nonContentExpiryHeldRetry = heldRetry
	}
	if value := os.Getenv("VELA_NON_CONTENT_EXPIRY_BATCH_SIZE"); value != "" {
		batchSize, err := strconv.Atoi(value)
		if err != nil || batchSize < 1 || batchSize > 1000 {
			return config{}, errors.New("environment variable VELA_NON_CONTENT_EXPIRY_BATCH_SIZE must be in 1..1000")
		}
		configuration.nonContentExpiryBatchSize = batchSize
	}
	if value := os.Getenv("VELA_ARTIFACT_REPLICATION_TICK"); value != "" {
		tick, err := time.ParseDuration(value)
		if err != nil || tick <= 0 || tick > time.Minute {
			return config{}, errors.New("environment variable VELA_ARTIFACT_REPLICATION_TICK must be in (0, 1m]")
		}
		configuration.artifactReplicationTick = tick
	}
	if value := os.Getenv("VELA_ARTIFACT_REPLICATION_CLAIM_TTL"); value != "" {
		claimTTL, err := time.ParseDuration(value)
		if err != nil || claimTTL < time.Second || claimTTL > time.Hour ||
			claimTTL%time.Second != 0 {
			return config{}, errors.New("environment variable VELA_ARTIFACT_REPLICATION_CLAIM_TTL must be in [1s, 1h]")
		}
		configuration.artifactReplicationClaimTTL = claimTTL
	}
	if value := os.Getenv("VELA_ARTIFACT_REPLICATION_RETRY_DELAY"); value != "" {
		retryDelay, err := time.ParseDuration(value)
		if err != nil || retryDelay < time.Second || retryDelay > 24*time.Hour ||
			retryDelay%time.Second != 0 {
			return config{}, errors.New("environment variable VELA_ARTIFACT_REPLICATION_RETRY_DELAY must be in [1s, 24h]")
		}
		configuration.artifactReplicationRetryDelay = retryDelay
	}
	if value := os.Getenv("VELA_ARTIFACT_REPLICATION_TIMEOUT"); value != "" {
		timeout, err := time.ParseDuration(value)
		if err != nil || timeout < time.Second || timeout > time.Hour ||
			timeout%time.Second != 0 {
			return config{}, errors.New("environment variable VELA_ARTIFACT_REPLICATION_TIMEOUT must be in [1s, 1h]")
		}
		configuration.artifactReplicationTimeout = timeout
	}
	if configuration.artifactReplicationTimeout >= configuration.artifactReplicationClaimTTL {
		return config{}, errors.New("VELA_ARTIFACT_REPLICATION_TIMEOUT must be less than VELA_ARTIFACT_REPLICATION_CLAIM_TTL")
	}
	if value := os.Getenv("VELA_ARTIFACT_REPLICATION_BATCH_SIZE"); value != "" {
		batchSize, err := strconv.Atoi(value)
		if err != nil || batchSize < 1 || batchSize > 1000 {
			return config{}, errors.New("environment variable VELA_ARTIFACT_REPLICATION_BATCH_SIZE must be in 1..1000")
		}
		configuration.artifactReplicationBatchSize = batchSize
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

func connectNATS(configuration config) (*natsauth.OutboxConnection, error) {
	connection, err := natsauth.ConnectOutbox(
		natsauth.OutboxConfig{
			URL:                      configuration.natsURL,
			CredentialsFile:          configuration.natsCredentials,
			RootCAFile:               configuration.natsRootCA,
			ExpectedAccountPublicKey: configuration.natsOutboxAccountPublicKey,
			ExpectedAccountSignerPublicKeys: splitCommaSeparated(
				configuration.natsOutboxAccountSignerPublicKeys,
			),
			ExpectedUserPublicKeys: splitCommaSeparated(configuration.natsOutboxUserPublicKeys),
			ClientCertificateFile:  configuration.natsClientCert,
			ClientKeyFile:          configuration.natsClientKey,
		},
		natsauth.Handlers{
			Disconnect: func(err error) {
				if err != nil {
					slog.Warn("NATS disconnected; Outbox will remain durable", "error", err)
				}
			},
			Reconnect: func(connectedURL string) {
				slog.Info("NATS reconnected", "url", connectedURL)
			},
			AsyncError: func(err error) {
				if err != nil {
					slog.Warn("NATS asynchronous error", "error", err)
				}
			},
			Closed: func(err error) {
				if err != nil {
					slog.Error("NATS connection closed", "error", err)
				}
			},
		},
	)
	if err != nil {
		return nil, fmt.Errorf("connect NATS Outbox workload: %w", err)
	}
	return connection, nil
}

func connectSchedulerNATS(configuration config) (*natsauth.SchedulerConnection, error) {
	connection, err := natsauth.ConnectScheduler(
		natsauth.SchedulerConfig{
			URL:                      configuration.natsURL,
			CredentialsFile:          configuration.natsSchedulerCredentials,
			RootCAFile:               configuration.natsRootCA,
			ExpectedAccountPublicKey: configuration.natsOutboxAccountPublicKey,
			ExpectedAccountSignerPublicKeys: splitCommaSeparated(
				configuration.natsOutboxAccountSignerPublicKeys,
			),
			ExpectedUserPublicKeys: splitCommaSeparated(
				configuration.natsSchedulerUserPublicKeys,
			),
			ClientCertificateFile: configuration.natsClientCert,
			ClientKeyFile:         configuration.natsClientKey,
		},
		natsauth.Handlers{
			Disconnect: func(err error) {
				if err != nil {
					slog.Warn(
						"NATS Scheduler consumer disconnected; PostgreSQL reconciliation remains authoritative",
						"error",
						err,
					)
				}
			},
			Reconnect: func(connectedURL string) {
				slog.Info("NATS Scheduler consumer reconnected", "url", connectedURL)
			},
			AsyncError: func(err error) {
				if err != nil {
					slog.Warn("NATS Scheduler consumer asynchronous error", "error", err)
				}
			},
			Closed: func(err error) {
				if err != nil {
					slog.Error("NATS Scheduler consumer connection closed", "error", err)
				}
			},
		},
	)
	if err != nil {
		return nil, fmt.Errorf("connect NATS Scheduler workload: %w", err)
	}
	return connection, nil
}

func splitCommaSeparated(value string) []string {
	parts := strings.Split(value, ",")
	values := make([]string, 0, len(parts))
	for _, part := range parts {
		values = append(values, strings.TrimSpace(part))
	}
	return values
}

func readNodeAgentEndpoints(path string) (map[string]nodeagent.AgentEndpoint, error) {
	cleaned := filepath.Clean(path)
	if !filepath.IsAbs(cleaned) {
		return nil, errors.New("node Agent endpoint file path must be absolute")
	}
	content, err := securefile.Read(cleaned, 1<<20, false)
	if err != nil {
		return nil, fmt.Errorf("open Node Agent endpoint file: %w", err)
	}
	if err := strictjson.RejectDuplicateKeys(content); err != nil {
		return nil, fmt.Errorf("decode Node Agent endpoint file: %w", err)
	}
	var endpoints map[string]nodeagent.AgentEndpoint
	decoder := json.NewDecoder(bytes.NewReader(content))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&endpoints); err != nil {
		return nil, fmt.Errorf("decode Node Agent endpoint file: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return nil, errors.New("node Agent endpoint file must contain exactly one JSON document")
	}
	return endpoints, nil
}

func nodeAgentRegistrations(
	endpoints map[string]nodeagent.AgentEndpoint,
) []fleettransport.NodeAgentRegistration {
	registrations := make([]fleettransport.NodeAgentRegistration, 0, len(endpoints))
	for nodeIdentity, endpoint := range endpoints {
		registrations = append(registrations, fleettransport.NodeAgentRegistration{
			NodeIdentity:   nodeIdentity,
			WorkerID:       endpoint.WorkerID,
			SPIFFEIdentity: endpoint.SPIFFEIdentity,
		})
	}
	return registrations
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

func openArtifactBackupStore(ctx context.Context, configuration config) (*artifactstore.S3, error) {
	accessKeyID, err := readSecretFile(
		configuration.artifactBackupS3AccessKeyFile,
		"Artifact backup S3 access key ID",
		1024,
	)
	if err != nil {
		return nil, err
	}
	secretAccessKey, err := readSecretFile(
		configuration.artifactBackupS3SecretKeyFile,
		"Artifact backup S3 secret access key",
		4096,
	)
	if err != nil {
		return nil, err
	}
	store, err := artifactstore.NewS3(artifactstore.S3Config{
		Endpoint:        configuration.artifactBackupS3Endpoint,
		Region:          configuration.artifactBackupS3Region,
		Bucket:          configuration.artifactBackupS3Bucket,
		AccessKeyID:     accessKeyID,
		SecretAccessKey: secretAccessKey,
		UsePathStyle:    configuration.artifactBackupS3PathStyle,
		SignedGETTTL:    artifactstore.MaxSignedGETTTL,
	})
	if err != nil {
		return nil, fmt.Errorf("configure off-cluster Artifact backup Store: %w", err)
	}
	if err := store.ValidateBucket(ctx); err != nil {
		return nil, fmt.Errorf("validate off-cluster Artifact backup bucket: %w", err)
	}
	return store, nil
}

func openArtifactReplicationStores(
	ctx context.Context,
	configuration config,
) (*artifactstore.S3, *artifactstore.S3, error) {
	sourceAccessKeyID, err := readSecretFile(
		configuration.artifactReplicationSourceAccessKeyFile,
		"Artifact replication source S3 access key ID",
		1024,
	)
	if err != nil {
		return nil, nil, err
	}
	sourceSecretAccessKey, err := readSecretFile(
		configuration.artifactReplicationSourceSecretKeyFile,
		"Artifact replication source S3 secret access key",
		4096,
	)
	if err != nil {
		return nil, nil, err
	}
	backupAccessKeyID, err := readSecretFile(
		configuration.artifactReplicationBackupAccessKeyFile,
		"Artifact replication backup S3 access key ID",
		1024,
	)
	if err != nil {
		return nil, nil, err
	}
	backupSecretAccessKey, err := readSecretFile(
		configuration.artifactReplicationBackupSecretKeyFile,
		"Artifact replication backup S3 secret access key",
		4096,
	)
	if err != nil {
		return nil, nil, err
	}
	source, err := artifactstore.NewS3(artifactstore.S3Config{
		Endpoint:        configuration.artifactS3Endpoint,
		Region:          configuration.artifactS3Region,
		Bucket:          configuration.artifactS3Bucket,
		AccessKeyID:     sourceAccessKeyID,
		SecretAccessKey: sourceSecretAccessKey,
		UsePathStyle:    configuration.artifactS3PathStyle,
		SignedGETTTL:    artifactstore.MaxSignedGETTTL,
	})
	if err != nil {
		return nil, nil, fmt.Errorf("configure Artifact replication source Store: %w", err)
	}
	if err := source.ValidateBucket(ctx); err != nil {
		return nil, nil, fmt.Errorf("validate Artifact replication source bucket: %w", err)
	}
	backup, err := artifactstore.NewS3(artifactstore.S3Config{
		Endpoint:        configuration.artifactBackupS3Endpoint,
		Region:          configuration.artifactBackupS3Region,
		Bucket:          configuration.artifactBackupS3Bucket,
		AccessKeyID:     backupAccessKeyID,
		SecretAccessKey: backupSecretAccessKey,
		UsePathStyle:    configuration.artifactBackupS3PathStyle,
		SignedGETTTL:    artifactstore.MaxSignedGETTTL,
	})
	if err != nil {
		return nil, nil, fmt.Errorf("configure Artifact replication backup Store: %w", err)
	}
	if err := backup.ValidateBucket(ctx); err != nil {
		return nil, nil, fmt.Errorf("validate Artifact replication backup bucket: %w", err)
	}
	return source, backup, nil
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

func controlHTTPHandlers(apiHandler, metrics, readiness http.Handler) (http.Handler, http.Handler) {
	management := http.NewServeMux()
	management.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})
	management.Handle("GET /readyz", readiness)
	management.Handle("GET /metrics", metrics)
	return apiHandler, management
}

func readinessHandler(
	artifactStore artifactBucketValidator,
	pools ...databasePinger,
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

func serveTLSHTTPServer(
	server tlsHTTPServer,
	listener net.Listener,
	onStart func(),
) <-chan error {
	errors := make(chan error, 1)
	go func() {
		if onStart != nil {
			onStart()
		}
		errors <- server.ServeTLS(listener, "", "")
	}()
	return errors
}

func handleHTTPServerExit(stop context.CancelFunc, name string, err error) error {
	if err == nil || errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	stop()
	return fmt.Errorf("serve %s: %w", name, err)
}

func shutdownHTTPServer(ctx context.Context, server httpServerShutdown, name string) error {
	if err := server.Shutdown(ctx); err != nil {
		return fmt.Errorf("shut down %s server: %w", name, err)
	}
	return nil
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

func runScheduler(
	ctx context.Context,
	scheduling hierarchicalScheduler,
	interval time.Duration,
	wakeups <-chan schedulerCycleRequest,
) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	if ctx.Err() != nil {
		return
	}
	_ = runSchedulerCycle(ctx, scheduling, true)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			_ = runSchedulerCycle(ctx, scheduling, true)
		case request, open := <-wakeups:
			if !open {
				wakeups = nil
				continue
			}
			cycleErr := runSchedulerCycle(ctx, scheduling, false)
			if request.result == nil {
				continue
			}
			select {
			case request.result <- cycleErr:
			case <-ctx.Done():
				return
			}
		}
	}
}

type schedulerCycleRequest struct {
	result chan<- error
}

func runSchedulerCycle(
	ctx context.Context,
	scheduling hierarchicalScheduler,
	reconcileExpired bool,
) error {
	if reconcileExpired {
		reconciled, err := scheduling.ReconcileExpired(ctx)
		if err != nil && !errors.Is(err, context.Canceled) {
			slog.Warn("Scheduler claim reconciliation incomplete", "error", err)
		} else if err == nil && reconciled > 0 {
			slog.Info("Scheduler expired claims reconciled", "claims", reconciled)
		}
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	dispatches, err := scheduling.RunCycle(ctx)
	if err != nil && !errors.Is(err, context.Canceled) {
		slog.Warn("Scheduler cycle incomplete", "error", err)
	} else if err == nil && len(dispatches) > 0 {
		slog.Info("Scheduler cycle dispatched Assignments", "dispatches", len(dispatches))
	}
	return err
}

func runSchedulerWakeupConsumer(
	ctx context.Context,
	connection *nats.Conn,
	messages *inbox.JetStreamConsumer,
	wakeups chan<- schedulerCycleRequest,
) {
	const retryDelay = time.Second
	for ctx.Err() == nil {
		bindContext, cancelBind := context.WithTimeout(ctx, 5*time.Second)
		consumer, err := scheduler.BindJetStreamWakeupConsumer(
			bindContext,
			connection,
			messages,
			func(handlerContext context.Context, _ pgx.Tx) error {
				return requestSchedulerCycle(handlerContext, wakeups)
			},
		)
		cancelBind()
		if err == nil {
			err = consumer.Run(ctx, func(processErr error) {
				slog.Warn(
					"Scheduler JetStream wakeup remains unacknowledged",
					"error",
					processErr,
				)
			})
		}
		if ctx.Err() != nil {
			return
		}
		if err != nil {
			slog.Warn(
				"Scheduler JetStream wakeup binding unavailable; PostgreSQL reconciliation remains authoritative",
				"error",
				err,
			)
		}
		timer := time.NewTimer(retryDelay)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return
		case <-timer.C:
		}
	}
}

func requestSchedulerCycle(
	ctx context.Context,
	wakeups chan<- schedulerCycleRequest,
) error {
	if wakeups == nil {
		return errors.New("scheduler cycle request channel is required")
	}
	result := make(chan error, 1)
	select {
	case wakeups <- schedulerCycleRequest{result: result}:
	case <-ctx.Done():
		return ctx.Err()
	}
	select {
	case err := <-result:
		return err
	case <-ctx.Done():
		return ctx.Err()
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

func runRetentionReconciler(
	ctx context.Context,
	reconciler contentRetentionReconciler,
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
		result, err := reconciler.ReconcileBatch(ctx)
		if err != nil && !errors.Is(err, context.Canceled) {
			slog.Warn("retention reconciliation incomplete", "error", err)
		} else if err == nil && (result.RequestContentExpired > 0 ||
			result.ContentDeletionRequestsCreated > 0 || result.Claimed > 0) {
			slog.Info(
				"retention reconciled",
				"request_content_expired", result.RequestContentExpired,
				"content_deletion_requests_created",
				result.ContentDeletionRequestsCreated,
				"claimed", result.Claimed,
				"completed", result.Completed,
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

func runNonContentExpiryReconciler(
	ctx context.Context,
	reconciler nonContentExpiryReconciler,
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
		result, err := reconciler.ReconcileBatch(ctx)
		if err != nil && !errors.Is(err, context.Canceled) {
			slog.Warn(
				"non-content expiry reconciliation incomplete",
				"claimed", result.Claimed,
				"expired", result.Expired,
				"held", result.Held,
				"stale", result.Stale,
				"error", err,
			)
		} else if err == nil && result.Claimed > 0 {
			slog.Info(
				"non-content records reconciled",
				"claimed", result.Claimed,
				"expired", result.Expired,
				"held", result.Held,
				"stale", result.Stale,
			)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func runArtifactBackupReplicator(
	ctx context.Context,
	replicator artifactBackupReplicator,
	interval time.Duration,
	operationTimeout time.Duration,
) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		operationContext, cancel := context.WithTimeout(ctx, operationTimeout)
		result, err := replicator.ReplicateBatch(operationContext)
		cancel()
		if err != nil && !errors.Is(err, context.Canceled) &&
			!errors.Is(err, context.DeadlineExceeded) {
			slog.Warn("Artifact backup replication incomplete", "error", err)
		} else if err == nil && result.Claimed > 0 {
			slog.Info(
				"Artifacts replicated to off-cluster backup",
				"claimed", result.Claimed,
				"completed", result.Completed,
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

func envOrDefault(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}
