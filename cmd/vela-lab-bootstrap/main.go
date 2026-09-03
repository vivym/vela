package main

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/jackc/pgx/v5"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/pressly/goose/v3"
	"github.com/vivym/vela/internal/eventstream"
	"github.com/vivym/vela/internal/labv2contract"
)

const (
	organizationID             = "84000000-0000-0000-0000-000000000001"
	projectID                  = "84000000-0000-0000-0000-000000000002"
	principalID                = "84000000-0000-0000-0000-000000000003"
	modelRevisionID            = "84000000-0000-0000-0000-000000000004"
	presetRevisionID           = "84000000-0000-0000-0000-000000000005"
	executionProfileID         = "84000000-0000-0000-0000-000000000006"
	outputSpecID               = "84000000-0000-0000-0000-000000000007"
	serviceClassID             = "84000000-0000-0000-0000-000000000009"
	certificationID            = "84000000-0000-0000-0000-00000000000a"
	rateCardRevisionID         = "84000000-0000-0000-0000-00000000000b"
	rateCardLineID             = "84000000-0000-0000-0000-00000000000c"
	credentialID               = "84000000-0000-0000-0000-00000000000d"
	financePrincipalID         = "84000000-0000-0000-0000-00000000000e"
	compliancePrincipalID      = "84000000-0000-0000-0000-00000000000f"
	worker1ID                  = labv2contract.Worker1ID
	worker2ID                  = labv2contract.Worker2ID
	thumbnailWorkerID          = labv2contract.ThumbnailWorkerID
	worker1MemberID            = labv2contract.Worker1MemberID
	worker2MemberID            = labv2contract.Worker2MemberID
	thumbnailWorkerMemberID    = labv2contract.ThumbnailWorkerMemberID
	worker1DeviceID            = labv2contract.Worker1DeviceID
	worker2DeviceID            = labv2contract.Worker2DeviceID
	thumbnailDeviceID          = labv2contract.ThumbnailDeviceID
	worker1NodeID              = labv2contract.Worker1NodeID
	worker2NodeID              = labv2contract.Worker2NodeID
	thumbnailNodeID            = labv2contract.ThumbnailNodeID
	worker1DeviceSetID         = labv2contract.Worker1DeviceSetID
	worker2DeviceSetID         = labv2contract.Worker2DeviceSetID
	thumbnailDeviceSetID       = labv2contract.ThumbnailDeviceSetID
	worker1Name                = labv2contract.Worker1Name
	worker2Name                = labv2contract.Worker2Name
	thumbnailWorkerName        = labv2contract.ThumbnailWorkerName
	auxWorkerProfileID         = labv2contract.AuxWorkerProfileID
	ditWorkerProfileID         = labv2contract.DiTWorkerProfileID
	thumbnailWorkerProfileID   = labv2contract.ThumbnailWorkerProfileID
	encoderResidencyID         = labv2contract.EncoderResidencyID
	vaeResidencyID             = labv2contract.VAEResidencyID
	ditResidencyID             = labv2contract.DiTResidencyID
	thumbnailResidencyID       = labv2contract.ThumbnailResidencyID
	encoderStageProfileID      = labv2contract.EncoderStageProfileID
	ditStageProfileID          = labv2contract.DiTStageProfileID
	vaeStageProfileID          = labv2contract.VAEStageProfileID
	thumbnailStageProfileID    = labv2contract.ThumbnailStageProfileID
	residencyPlanID            = "84000000-0000-0000-0000-000000000601"
	encoderCapacityPoolID      = labv2contract.EncoderCapacityPoolID
	vaeCapacityPoolID          = labv2contract.VAECapacityPoolID
	ditCapacityPoolID          = labv2contract.DiTCapacityPoolID
	thumbnailCapacityPoolID    = labv2contract.ThumbnailCapacityPoolID
	worker1BundleID            = labv2contract.Worker1BundleID
	worker2BundleID            = labv2contract.Worker2BundleID
	thumbnailWorkerBundleID    = labv2contract.ThumbnailWorkerBundleID
	thumbnailInterfaceID       = "84000000-0000-0000-0000-000000000514"
	thumbnailStageDefinitionID = labv2contract.ThumbnailStageDefinitionID
	thumbnailEquivalenceID     = labv2contract.ThumbnailEquivalenceID
	thumbnailConnectorID       = "84000000-0000-0000-0000-000000000552"
	thumbnailGraphEdgeID       = "84000000-0000-0000-0000-000000000562"
	primaryBucket              = "vela-lab-artifacts"
	backupBucket               = "vela-lab-artifacts-backup"
	defaultDatabaseRoot        = "/opt/vela/share/db"
	defaultNATSCreds           = "/etc/vela-lab-bootstrap/nats/bootstrap.creds"
	defaultNATSRootCA          = "/etc/vela-lab-bootstrap/nats/ca.crt"
	defaultNATSCert            = "/etc/vela-lab-bootstrap/nats/tls.crt"
	defaultNATSKey             = "/etc/vela-lab-bootstrap/nats/tls.key"
	defaultMinIORootCA         = "/etc/vela-lab-bootstrap/minio/ca.crt"
	defaultSmokeSecret         = "/etc/vela-lab-bootstrap/smoke/smoke-secret"
	defaultTimeout             = 4 * time.Minute
)

type options struct {
	databaseRoot   string
	natsCredential string
	natsRootCA     string
	natsCert       string
	natsKey        string
	minioRootCA    string
	smokeSecret    string
	timeout        time.Duration
}

type configuration struct {
	options
	adminDatabaseURL            string
	loginPasswords              map[string]string
	databaseURLs                map[string]string
	credentialPepper            []byte
	minioEndpoint               string
	minioAccessKey              string
	minioSecretKey              string
	natsURL                     string
	runtimeImageDigest          string
	thumbnailRuntimeImageDigest string
}

type labStageProfile struct {
	id                      string
	stableID                string
	stageDefinitionID       string
	modelComponentRevision  string
	runtimeImageDigest      string
	workerProfileID         string
	resultEquivalenceID     string
	certifiedCapacityVector string
	contentDigestByte       byte
}

type labRuntimeRoute struct {
	modelResidencyID string
	capacityPoolID   string
	stageProfileID   string
}

type labResidencyFixture struct {
	id                     string
	stageProfileID         string
	modelComponentRevision string
	runtimeIdentity        string
	runtimeImageDigest     string
}

type labWorkerFixture struct {
	id              string
	name            string
	memberID        string
	deviceID        string
	nodeID          string
	deviceSetID     string
	workerProfileID string
	resourceClass   string
	primaryPoolID   string
	bundleID        string
	gpuUUID         string
	pciBDF          string
	routes          []labRuntimeRoute
	residencies     []labResidencyFixture
	capacityVector  map[string]int64
}

type labWorkerProfile struct {
	id                     string
	stableID               string
	deviceSetShape         string
	residentModelRevisions string
	capacityLimits         string
	readinessChecks        string
	contentDigestByte      byte
}

type controlPrincipalFixture struct {
	name           string
	principalID    string
	stableID       string
	tlsURI         string
	databaseRole   string
	principalTable string
	bindingTable   string
}

var requiredControlPrincipals = []controlPrincipalFixture{
	{
		name: "Finance Reconciliation", principalID: financePrincipalID,
		stableID:       "vela-lab-finance-reconciliation",
		tlsURI:         "spiffe://vela.internal/control/finance-reconciliation",
		databaseRole:   "vela_finance_reconciliation_login",
		principalTable: "finance_reconciliation_principals",
		bindingTable:   "finance_reconciliation_database_bindings",
	},
	{
		name: "Compliance", principalID: compliancePrincipalID,
		stableID:       "vela-lab-compliance",
		tlsURI:         "spiffe://vela.internal/control/compliance",
		databaseRole:   "vela_compliance_login",
		principalTable: "compliance_principals",
		bindingTable:   "compliance_database_bindings",
	},
}

var identifierPattern = regexp.MustCompile(`^[a-z][a-z0-9_]{0,62}$`)

var requiredDatabaseEnvironments = []string{
	"VELA_ARTIFACT_REPLICATION_DATABASE_URL",
	"VELA_ARTIFACT_REQUEST_DATABASE_URL",
	"VELA_ATTEMPT_COORDINATOR_DATABASE_URL",
	"VELA_AUTH_DATABASE_URL",
	"VELA_BACKUP_RETENTION_DATABASE_URL",
	"VELA_BILLING_DATABASE_URL",
	"VELA_BREAK_GLASS_AUDIT_DATABASE_URL",
	"VELA_BREAK_GLASS_REQUEST_DATABASE_URL",
	"VELA_CANCEL_DATABASE_URL",
	"VELA_COMPLIANCE_DATABASE_URL",
	"VELA_DEBUG_DUMP_AUDIT_REQUEST_DATABASE_URL",
	"VELA_DEBUG_DUMP_REQUEST_DATABASE_URL",
	"VELA_FINANCE_RECONCILIATION_DATABASE_URL",
	"VELA_FLEET_DATABASE_URL",
	"VELA_HUMAN_AUTH_DATABASE_URL",
	"VELA_HUMAN_MEMBERSHIP_AUTH_DATABASE_URL",
	"VELA_HUMAN_MEMBERSHIP_REQUEST_DATABASE_URL",
	"VELA_IDENTITY_REQUEST_DATABASE_URL",
	"VELA_INTERNAL_DATABASE_URL",
	"VELA_NON_CONTENT_EXPIRY_DATABASE_URL",
	"VELA_ORGANIZATION_AUDIT_REQUEST_DATABASE_URL",
	"VELA_ORGANIZATION_BILLING_REQUEST_DATABASE_URL",
	"VELA_PLATFORM_OPERATOR_AUTH_DATABASE_URL",
	"VELA_REMEDIATION_DATABASE_URL",
	"VELA_REQUEST_DATABASE_URL",
	"VELA_RETENTION_DATABASE_URL",
	"VELA_RETENTION_REQUEST_DATABASE_URL",
	"VELA_STAGE_ARTIFACT_DATABASE_URL",
	"VELA_STAGE_SCHEDULER_DATABASE_URL",
	"VELA_STAGE_WORKER_CONTROL_DATABASE_URL",
	"VELA_WEBHOOK_DATABASE_URL",
	"VELA_WEBHOOK_REQUEST_DATABASE_URL",
}

func main() {
	if err := run(os.Args[1:]); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "vela lab bootstrap failed: %v\n", err)
		os.Exit(1)
	}
}

func run(arguments []string) error {
	flags := flag.NewFlagSet("vela-lab-bootstrap", flag.ContinueOnError)
	flags.SetOutput(os.Stderr)
	options := options{}
	flags.StringVar(&options.databaseRoot, "database-root", defaultDatabaseRoot, "directory containing roles.sql and migrations")
	flags.StringVar(&options.natsCredential, "nats-credential", defaultNATSCreds, "NATS bootstrap credential")
	flags.StringVar(&options.natsRootCA, "nats-root-ca", defaultNATSRootCA, "NATS Root CA")
	flags.StringVar(&options.natsCert, "nats-client-cert", defaultNATSCert, "NATS client certificate")
	flags.StringVar(&options.natsKey, "nats-client-key", defaultNATSKey, "NATS client key")
	flags.StringVar(&options.minioRootCA, "minio-root-ca", defaultMinIORootCA, "MinIO Root CA")
	flags.StringVar(&options.smokeSecret, "smoke-secret", defaultSmokeSecret, "raw smoke credential secret")
	flags.DurationVar(&options.timeout, "timeout", defaultTimeout, "overall bootstrap timeout")
	if err := flags.Parse(arguments); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return errors.New("unexpected positional arguments")
	}
	configuration, err := loadConfiguration(options)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), configuration.timeout)
	defer cancel()
	if err := bootstrapDatabase(ctx, configuration); err != nil {
		return err
	}
	if err := bootstrapMinIO(ctx, configuration); err != nil {
		return err
	}
	if err := bootstrapNATS(ctx, configuration); err != nil {
		return err
	}
	_, _ = fmt.Fprintln(os.Stdout, "LAB_BOOTSTRAP=complete production_gate_receipts=0")
	return nil
}

func loadConfiguration(options options) (configuration, error) {
	if options.timeout < 30*time.Second || options.timeout > 15*time.Minute {
		return configuration{}, errors.New("--timeout must be between 30s and 15m")
	}
	for name, path := range map[string]string{
		"database root":           options.databaseRoot,
		"NATS credential":         options.natsCredential,
		"NATS Root CA":            options.natsRootCA,
		"NATS client certificate": options.natsCert,
		"NATS client key":         options.natsKey,
		"MinIO Root CA":           options.minioRootCA,
		"smoke secret":            options.smokeSecret,
	} {
		if !filepath.IsAbs(filepath.Clean(path)) {
			return configuration{}, fmt.Errorf("%s path must be absolute", name)
		}
	}
	result := configuration{
		options:                     options,
		adminDatabaseURL:            strings.TrimSpace(os.Getenv("VELA_LAB_POSTGRES_ADMIN_URL")),
		minioEndpoint:               strings.TrimSpace(os.Getenv("VELA_LAB_MINIO_ENDPOINT")),
		minioAccessKey:              strings.TrimSpace(os.Getenv("VELA_LAB_MINIO_ACCESS_KEY")),
		minioSecretKey:              os.Getenv("VELA_LAB_MINIO_SECRET_KEY"),
		natsURL:                     strings.TrimSpace(os.Getenv("VELA_LAB_NATS_URL")),
		runtimeImageDigest:          strings.TrimSpace(os.Getenv("VELA_LAB_RUNTIME_IMAGE_DIGEST")),
		thumbnailRuntimeImageDigest: strings.TrimSpace(os.Getenv("VELA_LAB_THUMBNAIL_RUNTIME_IMAGE_DIGEST")),
		databaseURLs:                make(map[string]string, len(requiredDatabaseEnvironments)),
	}
	if result.adminDatabaseURL == "" || result.minioEndpoint == "" ||
		result.minioAccessKey == "" || result.minioSecretKey == "" ||
		result.natsURL == "" || result.runtimeImageDigest == "" || result.thumbnailRuntimeImageDigest == "" {
		return configuration{}, errors.New("lab dependency environment is incomplete")
	}
	if _, err := url.ParseRequestURI(result.adminDatabaseURL); err != nil {
		return configuration{}, fmt.Errorf("parse PostgreSQL admin URL: %w", err)
	}
	minioURL, err := url.Parse(result.minioEndpoint)
	if err != nil || minioURL.Scheme != "https" || minioURL.Hostname() == "" ||
		minioURL.User != nil || minioURL.RawQuery != "" || minioURL.Fragment != "" ||
		(minioURL.Path != "" && minioURL.Path != "/") {
		return configuration{}, errors.New("VELA_LAB_MINIO_ENDPOINT must be a canonical HTTPS endpoint")
	}
	if !regexp.MustCompile(`^sha256:[0-9a-f]{64}$`).MatchString(result.runtimeImageDigest) {
		return configuration{}, errors.New("VELA_LAB_RUNTIME_IMAGE_DIGEST must be a lowercase SHA-256 digest")
	}
	if !regexp.MustCompile(`^sha256:[0-9a-f]{64}$`).MatchString(result.thumbnailRuntimeImageDigest) {
		return configuration{}, errors.New("VELA_LAB_THUMBNAIL_RUNTIME_IMAGE_DIGEST must be a lowercase SHA-256 digest")
	}
	if result.runtimeImageDigest == result.thumbnailRuntimeImageDigest {
		return configuration{}, errors.New("H3 and thumbnail runtime image digests must be distinct")
	}
	for _, name := range requiredDatabaseEnvironments {
		value := strings.TrimSpace(os.Getenv(name))
		if value == "" {
			return configuration{}, fmt.Errorf("%s is required", name)
		}
		result.databaseURLs[name] = value
	}
	passwordJSON := os.Getenv("VELA_LAB_DATABASE_LOGIN_PASSWORDS")
	if err := json.Unmarshal([]byte(passwordJSON), &result.loginPasswords); err != nil || len(result.loginPasswords) != len(requiredDatabaseEnvironments) {
		return configuration{}, errors.New("VELA_LAB_DATABASE_LOGIN_PASSWORDS is invalid or incomplete")
	}
	encodedPepper := os.Getenv("VELA_CREDENTIAL_PEPPER_BASE64")
	pepper, err := base64.StdEncoding.DecodeString(encodedPepper)
	if err != nil || len(pepper) < 32 {
		return configuration{}, errors.New("VELA_CREDENTIAL_PEPPER_BASE64 must encode at least 32 bytes")
	}
	result.credentialPepper = pepper
	return result, nil
}

func bootstrapDatabase(ctx context.Context, configuration configuration) error {
	database, err := waitForDatabase(ctx, configuration.adminDatabaseURL)
	if err != nil {
		return err
	}
	defer func() { _ = database.Close() }()
	rolesSQL, err := os.ReadFile(filepath.Join(configuration.databaseRoot, "bootstrap", "roles.sql"))
	if err != nil {
		return fmt.Errorf("read database role bootstrap: %w", err)
	}
	if _, err := database.ExecContext(ctx, string(rolesSQL)); err != nil {
		return fmt.Errorf("apply database role bootstrap: %w", err)
	}
	if err := goose.SetDialect("postgres"); err != nil {
		return fmt.Errorf("configure Goose: %w", err)
	}
	if err := goose.UpContext(ctx, database, filepath.Join(configuration.databaseRoot, "migrations")); err != nil {
		return fmt.Errorf("apply database migrations: %w", err)
	}
	if err := createLoginRoles(ctx, database, configuration); err != nil {
		return err
	}
	secret, err := os.ReadFile(configuration.smokeSecret)
	if err != nil || len(secret) != 32 {
		return errors.New("read exact 32-byte smoke credential secret")
	}
	defer clear(secret)
	digest := hmac.New(sha256.New, configuration.credentialPepper)
	_, _ = digest.Write(secret)
	credentialDigest := digest.Sum(nil)
	defer clear(credentialDigest)
	if err := seedLabFixture(
		ctx,
		database,
		credentialDigest,
		configuration.runtimeImageDigest,
		configuration.thumbnailRuntimeImageDigest,
	); err != nil {
		return err
	}
	return nil
}

func waitForDatabase(ctx context.Context, databaseURL string) (*sql.DB, error) {
	database, err := sql.Open("pgx", databaseURL)
	if err != nil {
		return nil, fmt.Errorf("open PostgreSQL: %w", err)
	}
	for {
		if err := database.PingContext(ctx); err == nil {
			return database, nil
		}
		select {
		case <-ctx.Done():
			_ = database.Close()
			return nil, fmt.Errorf("wait for PostgreSQL: %w", ctx.Err())
		case <-time.After(time.Second):
		}
	}
}

func createLoginRoles(ctx context.Context, database *sql.DB, configuration configuration) error {
	names := make([]string, 0, len(configuration.databaseURLs))
	for environment := range configuration.databaseURLs {
		names = append(names, environment)
	}
	sort.Strings(names)
	seen := make(map[string]struct{}, len(names))
	for _, environment := range names {
		parsed, err := url.Parse(configuration.databaseURLs[environment])
		if err != nil || parsed.User == nil {
			return fmt.Errorf("parse %s: %w", environment, err)
		}
		login := parsed.User.Username()
		password, present := configuration.loginPasswords[login]
		if !present || !identifierPattern.MatchString(login) || !strings.HasSuffix(login, "_login") ||
			!regexp.MustCompile(`^[0-9a-f]{48}$`).MatchString(password) {
			return fmt.Errorf("login material for %s is invalid", environment)
		}
		if _, duplicate := seen[login]; duplicate {
			return fmt.Errorf("database login %s is reused", login)
		}
		seen[login] = struct{}{}
		group := strings.TrimSuffix(login, "_login")
		if !identifierPattern.MatchString(group) {
			return fmt.Errorf("database group %s is invalid", group)
		}
		var exists bool
		if err := database.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = $1)`, login).Scan(&exists); err != nil {
			return fmt.Errorf("inspect database login %s: %w", login, err)
		}
		attributes := "NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS"
		if group == "vela_internal" {
			attributes = "NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION BYPASSRLS"
		}
		loginIdentifier := pgx.Identifier{login}.Sanitize()
		groupIdentifier := pgx.Identifier{group}.Sanitize()
		passwordLiteral := "'" + password + "'"
		statement := fmt.Sprintf("CREATE ROLE %s LOGIN PASSWORD %s %s IN ROLE %s", loginIdentifier, passwordLiteral, attributes, groupIdentifier)
		if exists {
			statement = fmt.Sprintf("ALTER ROLE %s LOGIN PASSWORD %s %s", loginIdentifier, passwordLiteral, attributes)
		}
		if _, err := database.ExecContext(ctx, statement); err != nil {
			return fmt.Errorf("configure database login %s: %w", login, err)
		}
		if exists {
			if _, err := database.ExecContext(ctx, fmt.Sprintf("GRANT %s TO %s", groupIdentifier, loginIdentifier)); err != nil {
				return fmt.Errorf("grant database group %s: %w", group, err)
			}
		}
	}
	return nil
}

func seedLabFixture(
	ctx context.Context,
	database *sql.DB,
	credentialDigest []byte,
	runtimeImageDigest string,
	thumbnailRuntimeImageDigest string,
) error {
	transaction, err := database.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin lab fixture transaction: %w", err)
	}
	defer func() { _ = transaction.Rollback() }()
	statements := []struct {
		query string
		args  []any
	}{
		{`INSERT INTO customer_organizations (id, display_name) VALUES ($1, 'Vela non-production lab') ON CONFLICT (id) DO NOTHING`, []any{organizationID}},
		{`INSERT INTO projects (id, organization_id, display_name, queued_limit, running_limit) VALUES ($1, $2, 'Mock H3 lab', 32, 2) ON CONFLICT (id) DO NOTHING`, []any{projectID, organizationID}},
		{`INSERT INTO principals (id, organization_id, kind, display_name) VALUES ($1, $2, 'SERVICE', 'Lab smoke client') ON CONFLICT (id) DO NOTHING`, []any{principalID, organizationID}},
		{`INSERT INTO service_principals (principal_id, organization_id, project_id) VALUES ($1, $2, $3) ON CONFLICT (principal_id) DO NOTHING`, []any{principalID, organizationID, projectID}},
		{`INSERT INTO credentials (id, organization_id, project_id, principal_id, secret_digest, scopes, expires_at, created_by_principal_id) VALUES ($1, $2, $3, $4, $5, ARRAY['jobs:submit','jobs:read','artifacts:read','jobs:cancel'], clock_timestamp() + interval '30 days', $4) ON CONFLICT (id) DO NOTHING`, []any{credentialID, organizationID, projectID, principalID, credentialDigest}},
		{`INSERT INTO organization_credit_accounts (organization_id, currency, contract_credit_limit_minor) VALUES ($1, 'CNY', 100000000) ON CONFLICT (organization_id) DO NOTHING`, []any{organizationID}},
		{`INSERT INTO model_revisions (id, stable_id, revision, state, content_hash) VALUES ($1, 'h3-mock', 1, 'ACTIVE', 'mock-only-765077057011f16f852886601235f066') ON CONFLICT (id) DO NOTHING`, []any{modelRevisionID}},
		{`INSERT INTO generation_preset_revisions (id, model_revision_id, stable_id, revision, state, certified_p95_compute_seconds) VALUES ($1, $2, 'balanced', 1, 'ACTIVE', 30) ON CONFLICT (id) DO NOTHING`, []any{presetRevisionID, modelRevisionID}},
		{`INSERT INTO service_class_revisions (id, stable_id, revision, state, queue_retry_allowance_seconds, max_attempts, max_total_compute_multiplier_milli, max_finalization_seconds_per_attempt, retry_backoff_policy, retryable_failure_classes, circuit_breaker_policy) VALUES ($1, 'standard', 1, 'ACTIVE', 600, 2, 2000, 300, '{"kind":"exponential","initial_seconds":2,"max_seconds":10}', ARRAY['WORKER_LOST','TRANSIENT_BACKEND'], '{"policy_revision":"mock-lab-v1"}') ON CONFLICT (id) DO NOTHING`, []any{serviceClassID}},
		{`INSERT INTO output_specs (id, stable_id, revision, state, width, height, duration_milliseconds, frame_rate_milli, codec, container, thumbnail_required) VALUES ($1, 'mock-video-1080p-5s-24fps', 1, 'ACTIVE', 1920, 1080, 5000, 24000, 'h264', 'mp4', true) ON CONFLICT (id) DO NOTHING`, []any{outputSpecID}},
		{`INSERT INTO rate_card_revisions (id, revision, state, effective_at) VALUES ($1, 840000001, 'ACTIVE', clock_timestamp() - interval '1 hour') ON CONFLICT (id) DO NOTHING`, []any{rateCardRevisionID}},
		{`INSERT INTO rate_card_lines (id, rate_card_revision_id, model_revision_id, generation_preset_revision_id, service_class_revision_id, output_spec_id, unit_amount_minor, currency) VALUES ($1, $2, $3, $4, $5, $6, 1, 'CNY') ON CONFLICT (id) DO NOTHING`, []any{rateCardLineID, rateCardRevisionID, modelRevisionID, presetRevisionID, serviceClassID, outputSpecID}},
	}
	for _, statement := range statements {
		if _, err := transaction.ExecContext(ctx, statement.query, statement.args...); err != nil {
			return fmt.Errorf("seed lab fixture: %w", err)
		}
	}
	if err := seedRequiredControlPrincipals(ctx, transaction); err != nil {
		return err
	}
	var storedCredentialDigest []byte
	if err := transaction.QueryRowContext(ctx, `SELECT secret_digest FROM credentials WHERE id = $1`, credentialID).Scan(&storedCredentialDigest); err != nil {
		return fmt.Errorf("read lab smoke credential digest: %w", err)
	}
	if !hmac.Equal(storedCredentialDigest, credentialDigest) {
		return errors.New("existing lab smoke credential does not match the installed asset set")
	}
	if err := seedStageCatalog(ctx, transaction, runtimeImageDigest, thumbnailRuntimeImageDigest); err != nil {
		return err
	}
	if err := seedWorkerRegistry(ctx, transaction, runtimeImageDigest, thumbnailRuntimeImageDigest); err != nil {
		return err
	}
	var productionGateReceipts int
	if err := transaction.QueryRowContext(ctx, `SELECT count(*) FROM production_gate_receipts`).Scan(&productionGateReceipts); err != nil {
		return fmt.Errorf("count Production Gate receipts: %w", err)
	}
	if productionGateReceipts != 0 {
		return fmt.Errorf("refuse lab bootstrap with %d Production Gate receipts", productionGateReceipts)
	}
	if err := transaction.Commit(); err != nil {
		return fmt.Errorf("commit lab fixture: %w", err)
	}
	return nil
}

func labStageProfiles(runtimeImageDigest, thumbnailRuntimeImageDigest string) ([]labStageProfile, error) {
	if !regexp.MustCompile(`^sha256:[0-9a-f]{64}$`).MatchString(runtimeImageDigest) ||
		!regexp.MustCompile(`^sha256:[0-9a-f]{64}$`).MatchString(thumbnailRuntimeImageDigest) ||
		runtimeImageDigest == thumbnailRuntimeImageDigest {
		return nil, errors.New("runtime image digest must be a lowercase SHA-256 digest")
	}
	result := make([]labStageProfile, 0, len(labv2contract.StageDescriptors()))
	for _, descriptor := range labv2contract.StageDescriptors() {
		imageDigest := runtimeImageDigest
		if descriptor.RuntimeImageClass == labv2contract.BootstrapRuntimeImageClass {
			imageDigest = thumbnailRuntimeImageDigest
		}
		result = append(result, labStageProfile{
			id: descriptor.ProfileID, stableID: descriptor.StableID,
			stageDefinitionID:      descriptor.DefinitionID,
			modelComponentRevision: descriptor.ComponentRevision, runtimeImageDigest: imageDigest,
			workerProfileID: descriptor.WorkerProfileID, resultEquivalenceID: descriptor.EquivalenceID,
			certifiedCapacityVector: descriptor.CertifiedCapacityVector,
			contentDigestByte:       descriptor.ProfileContentDigest,
		})
	}
	return result, nil
}

func labWorkerFixtures(runtimeImageDigest, thumbnailRuntimeImageDigest string) ([]labWorkerFixture, error) {
	if !regexp.MustCompile(`^sha256:[0-9a-f]{64}$`).MatchString(runtimeImageDigest) ||
		!regexp.MustCompile(`^sha256:[0-9a-f]{64}$`).MatchString(thumbnailRuntimeImageDigest) ||
		runtimeImageDigest == thumbnailRuntimeImageDigest {
		return nil, errors.New("runtime image digest must be a lowercase SHA-256 digest")
	}
	stages := make(map[string]labv2contract.StageDescriptor)
	for _, stage := range labv2contract.StageDescriptors() {
		stages[stage.Key] = stage
	}
	workers := make([]labWorkerFixture, 0, len(labv2contract.WorkerDescriptors()))
	for _, descriptor := range labv2contract.WorkerDescriptors() {
		routes := make([]labRuntimeRoute, 0, len(descriptor.StageKeys))
		residencies := make([]labResidencyFixture, 0, len(descriptor.StageKeys))
		for _, stageKey := range descriptor.StageKeys {
			stage, ok := stages[stageKey]
			if !ok {
				return nil, fmt.Errorf("lab Worker %s references unknown Stage %s", descriptor.Name, stageKey)
			}
			imageDigest := runtimeImageDigest
			if stage.RuntimeImageClass == labv2contract.BootstrapRuntimeImageClass {
				imageDigest = thumbnailRuntimeImageDigest
			}
			routes = append(routes, labRuntimeRoute{
				modelResidencyID: stage.ResidencyID, capacityPoolID: stage.CapacityPoolID,
				stageProfileID: stage.ProfileID,
			})
			residencies = append(residencies, labResidencyFixture{
				id: stage.ResidencyID, stageProfileID: stage.ProfileID,
				modelComponentRevision: stage.ComponentRevision,
				runtimeIdentity:        stage.RuntimeIdentityPrefix + "@" + imageDigest,
				runtimeImageDigest:     imageDigest,
			})
		}
		workers = append(workers, labWorkerFixture{
			id: descriptor.InstanceID, name: descriptor.Name, memberID: descriptor.MemberID,
			deviceID: descriptor.DeviceID, nodeID: descriptor.NodeID, deviceSetID: descriptor.DeviceSetID,
			workerProfileID: descriptor.WorkerProfileID, resourceClass: descriptor.ResourceClass,
			primaryPoolID: descriptor.PrimaryCapacityPoolID, bundleID: descriptor.BundleID,
			gpuUUID: descriptor.GPUUUID, pciBDF: descriptor.PCIBDF,
			routes: routes, residencies: residencies, capacityVector: descriptor.CapacityVector,
		})
	}
	return workers, nil
}

func labWorkerProfiles() ([]labWorkerProfile, error) {
	stages := make(map[string]labv2contract.StageDescriptor)
	for _, stage := range labv2contract.StageDescriptors() {
		stages[stage.Key] = stage
	}
	workers := labv2contract.WorkerDescriptors()
	profiles := make([]labWorkerProfile, 0, len(workers))
	seen := make(map[string]struct{}, len(workers))
	for _, worker := range workers {
		if _, duplicate := seen[worker.WorkerProfileID]; duplicate {
			return nil, fmt.Errorf("duplicate lab WorkerProfile %s", worker.WorkerProfileID)
		}
		seen[worker.WorkerProfileID] = struct{}{}
		shape := map[string]string{"kind": worker.DeviceSetKind}
		if worker.SharedSlot != "" {
			shape["shared_slot_exception"] = worker.SharedSlot
		}
		residentRevisions := make([]string, 0, len(worker.StageKeys))
		for _, stageKey := range worker.StageKeys {
			stage, ok := stages[stageKey]
			if !ok {
				return nil, fmt.Errorf("lab WorkerProfile %s references unknown Stage %s", worker.WorkerProfileID, stageKey)
			}
			residentRevisions = append(residentRevisions, stage.ComponentRevision)
		}
		deviceSetShape, err := json.Marshal(shape)
		if err != nil {
			return nil, fmt.Errorf("encode WorkerProfile %s device set shape: %w", worker.WorkerProfileID, err)
		}
		residentModels, err := json.Marshal(residentRevisions)
		if err != nil {
			return nil, fmt.Errorf("encode WorkerProfile %s resident models: %w", worker.WorkerProfileID, err)
		}
		capacityLimits, err := json.Marshal(worker.CapacityVector)
		if err != nil {
			return nil, fmt.Errorf("encode WorkerProfile %s capacity limits: %w", worker.WorkerProfileID, err)
		}
		readinessChecks, err := json.Marshal(worker.ReadinessChecks)
		if err != nil {
			return nil, fmt.Errorf("encode WorkerProfile %s readiness checks: %w", worker.WorkerProfileID, err)
		}
		profiles = append(profiles, labWorkerProfile{
			id: worker.WorkerProfileID, stableID: worker.WorkerProfileStableID,
			deviceSetShape: string(deviceSetShape), residentModelRevisions: string(residentModels),
			capacityLimits: string(capacityLimits), readinessChecks: string(readinessChecks),
			contentDigestByte: worker.WorkerProfileDigest,
		})
	}
	return profiles, nil
}

func seedStageCatalog(ctx context.Context, transaction *sql.Tx, runtimeImageDigest, thumbnailRuntimeImageDigest string) error {
	profiles, err := labStageProfiles(runtimeImageDigest, thumbnailRuntimeImageDigest)
	if err != nil {
		return err
	}
	var existing bool
	if err := transaction.QueryRowContext(ctx, `
		SELECT EXISTS (SELECT 1 FROM execution_graph_revisions WHERE id = $1)
	`, "84000000-0000-0000-0000-000000000501").Scan(&existing); err != nil {
		return fmt.Errorf("inspect lab Stage catalog: %w", err)
	}
	if existing {
		return verifyStageCatalog(ctx, transaction, profiles)
	}

	const catalogSQL = `
		INSERT INTO stage_interface_revisions (
			id, stable_id, revision, state, payload_kind, dtype, layout,
			shape_contract, serialization, max_bytes, digest_algorithm,
			schema_digest, content_digest
		) VALUES
			('84000000-0000-0000-0000-000000000510', 'h3-request-lab-v2', 1, 'CERTIFIED',
			 'request', '', '', '{}', 'json', 1048576, 'sha256', decode(repeat('10', 32), 'hex'), decode(repeat('20', 32), 'hex')),
			('84000000-0000-0000-0000-000000000511', 'h3-conditioning-lab-v2', 1, 'CERTIFIED',
			 'tensor', 'bf16', 'h3-conditioning', '{}', 'safetensors', 67108864, 'sha256', decode(repeat('11', 32), 'hex'), decode(repeat('21', 32), 'hex')),
			('84000000-0000-0000-0000-000000000512', 'h3-latent-lab-v2', 1, 'CERTIFIED',
			 'tensor', 'bf16', 'h3-latent', '{}', 'safetensors', 1073741824, 'sha256', decode(repeat('12', 32), 'hex'), decode(repeat('22', 32), 'hex')),
			('84000000-0000-0000-0000-000000000513', 'h3-video-lab-v2', 1, 'CERTIFIED',
			 'video', '', 'frames-rgb', '{}', 'frame-bundle', 8589934592, 'sha256', decode(repeat('13', 32), 'hex'), decode(repeat('23', 32), 'hex')),
			('84000000-0000-0000-0000-000000000514', 'h3-thumbnail-lab-v2', 1, 'CERTIFIED',
			 'image', '', 'webp', '{}', 'webp', 16777216, 'sha256', decode(repeat('14', 32), 'hex'), decode(repeat('24', 32), 'hex'));

		INSERT INTO stage_cache_policy_revisions (
			id, stable_id, revision, state, allowed_stage_keys, scope_ceiling,
			ttl_seconds, quota_policy, encryption_policy, deletion_policy, content_digest
		) VALUES (
			'84000000-0000-0000-0000-000000000520', 'h3-exact-cache-lab-v2', 1, 'CERTIFIED',
			ARRAY['encoder', 'dit', 'vae', 'thumbnail'], 'PROJECT', 86400, '{}', '{}', '{}', decode(repeat('30', 32), 'hex')
		);
		INSERT INTO checkpoint_policy_revisions (
			id, stable_id, revision, state, resume_format, compatibility_contract,
			interval_policy, max_overhead_ppm, evidence_digest, content_digest
		) VALUES (
			'84000000-0000-0000-0000-000000000521', 'h3-no-checkpoint-lab-v2', 1, 'CERTIFIED',
			'none', '{}', '{}', 0, decode(repeat('31', 32), 'hex'), decode(repeat('32', 32), 'hex')
		);
		INSERT INTO stage_result_equivalence_revisions (
			id, stable_id, revision, state, exact_contract, evidence_receipt_ref,
			evidence_digest, content_digest
		) VALUES
			('49000000-0000-0000-0000-000000000023', 'h3-encoder-exact-lab-v2', 1, 'CERTIFIED',
			 '{"precision":"mock","rng":"exact"}', 'receipt://non-production-lab-v2/encoder', decode(repeat('35', 32), 'hex'), decode(repeat('36', 32), 'hex')),
			('49000000-0000-0000-0000-000000000024', 'h3-dit-exact-lab-v2', 1, 'CERTIFIED',
			 '{"precision":"mock","rng":"exact"}', 'receipt://non-production-lab-v2/dit', decode(repeat('37', 32), 'hex'), decode(repeat('38', 32), 'hex')),
			('49000000-0000-0000-0000-000000000025', 'h3-vae-exact-lab-v2', 1, 'CERTIFIED',
			 '{"precision":"mock","rng":"exact"}', 'receipt://non-production-lab-v2/vae', decode(repeat('39', 32), 'hex'), decode(repeat('3a', 32), 'hex')),
			('49000000-0000-0000-0000-000000000026', 'h3-thumbnail-exact-lab-v2', 1, 'CERTIFIED',
			 '{"mode":"BITWISE","backend":"lab-mock"}', 'receipt://non-production-lab-v2/thumbnail', decode(repeat('3b', 32), 'hex'), decode(repeat('3c', 32), 'hex'));

		INSERT INTO stage_definition_revisions (
			id, stable_id, revision, state, stage_kind, input_ports, output_ports,
			required_input_ports, required_output_ports, resource_class, retry_class,
			cache_policy_revision_id, checkpoint_policy_revision_id, public_phase, content_digest
		) VALUES
			('49000000-0000-0000-0000-000000000030', 'h3-encoder-lab-v2', 1, 'CERTIFIED', 'ENCODER',
			 jsonb_build_object('request', '84000000-0000-0000-0000-000000000510', 'cycle', '84000000-0000-0000-0000-000000000510'),
			 jsonb_build_object('conditioning', '84000000-0000-0000-0000-000000000511'), ARRAY['request'], ARRAY['conditioning'],
			 'GPU', 'STAGE_RETRY', '84000000-0000-0000-0000-000000000520', '84000000-0000-0000-0000-000000000521', 'PREPARING', decode(repeat('40', 32), 'hex')),
			('49000000-0000-0000-0000-000000000031', 'h3-dit-lab-v2', 1, 'CERTIFIED', 'DIT',
			 jsonb_build_object('conditioning', '84000000-0000-0000-0000-000000000511'),
			 jsonb_build_object('latent', '84000000-0000-0000-0000-000000000512'), ARRAY['conditioning'], ARRAY['latent'],
			 'GPU', 'STAGE_RETRY', '84000000-0000-0000-0000-000000000520', '84000000-0000-0000-0000-000000000521', 'GENERATING', decode(repeat('41', 32), 'hex')),
			('49000000-0000-0000-0000-000000000032', 'h3-vae-lab-v2', 1, 'CERTIFIED', 'VAE_DECODER',
			 jsonb_build_object('latent', '84000000-0000-0000-0000-000000000512'),
			 jsonb_build_object('video', '84000000-0000-0000-0000-000000000513', 'cycle', '84000000-0000-0000-0000-000000000510'),
			 ARRAY['latent'], ARRAY['video'], 'GPU', 'STAGE_RETRY', '84000000-0000-0000-0000-000000000520',
			 '84000000-0000-0000-0000-000000000521', 'GENERATING', decode(repeat('42', 32), 'hex')),
			('49000000-0000-0000-0000-000000000033', 'h3-cpu-thumbnail-lab-v2', 1, 'CERTIFIED', 'THUMBNAIL',
			 jsonb_build_object('frames', '84000000-0000-0000-0000-000000000513'),
			 jsonb_build_object('thumbnail', '84000000-0000-0000-0000-000000000514'),
			 ARRAY['frames'], ARRAY['thumbnail'], 'CPU', 'STAGE_RETRY', '84000000-0000-0000-0000-000000000520',
			 '84000000-0000-0000-0000-000000000521', 'FINALIZING', decode(repeat('43', 32), 'hex'));

		INSERT INTO connector_revisions (
			id, stable_id, revision, state, source_interface_revision_id,
			destination_interface_revision_id, transport, durable_fallback,
			topology_policy, integrity_policy, security_policy, limits, content_digest
		) VALUES
			('84000000-0000-0000-0000-000000000550', 'h3-conditioning-l2-lab-v2', 1, 'CERTIFIED',
			 '84000000-0000-0000-0000-000000000511', '84000000-0000-0000-0000-000000000511', 'OBJECT_STORE', true, '{}', '{}', '{}', '{}', decode(repeat('60', 32), 'hex')),
			('84000000-0000-0000-0000-000000000551', 'h3-latent-l2-lab-v2', 1, 'CERTIFIED',
			 '84000000-0000-0000-0000-000000000512', '84000000-0000-0000-0000-000000000512', 'OBJECT_STORE', true, '{}', '{}', '{}', '{}', decode(repeat('61', 32), 'hex')),
			('84000000-0000-0000-0000-000000000552', 'h3-frames-to-thumbnail-l2-lab-v2', 1, 'CERTIFIED',
			 '84000000-0000-0000-0000-000000000513', '84000000-0000-0000-0000-000000000513', 'OBJECT_STORE', true, '{}', '{}', '{}', '{}', decode(repeat('62', 32), 'hex'));

		INSERT INTO execution_graph_revisions (
			id, model_revision_id, stable_id, revision, schema_version, state,
			final_output_contract, public_phase_map, content_digest
		) VALUES (
			'84000000-0000-0000-0000-000000000501', '84000000-0000-0000-0000-000000000004', 'h3-stage-graph-lab-v2', 1, 1, 'CERTIFIED',
			 '{"output_key":"video"}', '{"encoder":"PREPARING","dit":"GENERATING","vae":"GENERATING","thumbnail":"FINALIZING"}', decode(repeat('91', 32), 'hex')
		);
		INSERT INTO execution_graph_stages (
			execution_graph_revision_id, stage_key, stage_definition_revision_id, required, max_fan_out
		) VALUES
			('84000000-0000-0000-0000-000000000501', 'encoder', '49000000-0000-0000-0000-000000000030', true, 1),
			('84000000-0000-0000-0000-000000000501', 'dit', '49000000-0000-0000-0000-000000000031', true, 1),
			('84000000-0000-0000-0000-000000000501', 'vae', '49000000-0000-0000-0000-000000000032', true, 1),
			('84000000-0000-0000-0000-000000000501', 'thumbnail', '49000000-0000-0000-0000-000000000033', true, 1);
		INSERT INTO execution_graph_edges (
			id, execution_graph_revision_id, source_stage_key, source_port, destination_stage_key, destination_port, buffer_class
		) VALUES
			('84000000-0000-0000-0000-000000000560', '84000000-0000-0000-0000-000000000501', 'encoder', 'conditioning', 'dit', 'conditioning', 'L2_DURABLE'),
			('84000000-0000-0000-0000-000000000561', '84000000-0000-0000-0000-000000000501', 'dit', 'latent', 'vae', 'latent', 'L2_DURABLE'),
			('84000000-0000-0000-0000-000000000562', '84000000-0000-0000-0000-000000000501', 'vae', 'video', 'thumbnail', 'frames', 'L2_DURABLE');
		INSERT INTO execution_graph_inputs (
			execution_graph_revision_id, input_key, interface_revision_id, destination_stage_key, destination_port
		) VALUES ('84000000-0000-0000-0000-000000000501', 'request', '84000000-0000-0000-0000-000000000510', 'encoder', 'request');
		INSERT INTO execution_graph_outputs (
			execution_graph_revision_id, output_key, interface_revision_id, source_stage_key, source_port, required
		) VALUES
			('84000000-0000-0000-0000-000000000501', 'video', '84000000-0000-0000-0000-000000000513', 'vae', 'video', true),
			('84000000-0000-0000-0000-000000000501', 'thumbnail', '84000000-0000-0000-0000-000000000514', 'thumbnail', 'thumbnail', true);
	`
	if _, err := transaction.ExecContext(ctx, catalogSQL); err != nil {
		return fmt.Errorf("seed lab Stage catalog: %w", err)
	}
	workerProfiles, err := labWorkerProfiles()
	if err != nil {
		return err
	}
	for _, profile := range workerProfiles {
		if _, err := transaction.ExecContext(ctx, `
			INSERT INTO worker_profile_revisions (
				id, stable_id, revision, state, device_count, member_count,
				device_set_shape, resident_model_revisions, capacity_limits,
				readiness_checks, content_digest
			) VALUES ($1, $2, 1, 'CERTIFIED', 1, 1, $3::jsonb, $4::jsonb, $5::jsonb,
				$6::jsonb, decode(repeat($7, 32), 'hex'))
		`, profile.id, profile.stableID, profile.deviceSetShape, profile.residentModelRevisions,
			profile.capacityLimits, profile.readinessChecks,
			fmt.Sprintf("%02x", profile.contentDigestByte)); err != nil {
			return fmt.Errorf("seed WorkerProfile %s: %w", profile.id, err)
		}
	}
	for _, profile := range profiles {
		if _, err := transaction.ExecContext(ctx, `
			INSERT INTO stage_profile_revisions (
				id, stable_id, revision, state, stage_definition_revision_id,
				model_component_revision, runtime_image_digest, worker_profile_revision_id,
				result_equivalence_revision_id, certified_capacity_vector, content_digest
			) VALUES ($1, $2, 1, 'CERTIFIED', $3, $4, $5, $6, $7, $8::jsonb, decode(repeat($9, 32), 'hex'))
		`, profile.id, profile.stableID, profile.stageDefinitionID,
			profile.modelComponentRevision, profile.runtimeImageDigest, profile.workerProfileID,
			profile.resultEquivalenceID, profile.certifiedCapacityVector,
			fmt.Sprintf("%02x", profile.contentDigestByte)); err != nil {
			return fmt.Errorf("seed StageProfile %s: %w", profile.id, err)
		}
	}
	const graphOptionsSQL = `
		INSERT INTO execution_profile_revisions (
			id, model_revision_id, execution_graph_revision_id, stable_id, revision, state
		) VALUES ('84000000-0000-0000-0000-000000000006', '84000000-0000-0000-0000-000000000004', '84000000-0000-0000-0000-000000000501', 'h3-mock-stage-lab-v2', 1, 'CERTIFIED');
		INSERT INTO execution_profile_stage_options (
			execution_profile_revision_id, execution_graph_revision_id, stage_key,
			stage_definition_revision_id, stage_profile_revision_id, preference, eligibility_metadata
		) VALUES
			('84000000-0000-0000-0000-000000000006', '84000000-0000-0000-0000-000000000501', 'encoder', '49000000-0000-0000-0000-000000000030', '49000000-0000-0000-0000-000000000040', 0, '{}'),
			('84000000-0000-0000-0000-000000000006', '84000000-0000-0000-0000-000000000501', 'dit', '49000000-0000-0000-0000-000000000031', '49000000-0000-0000-0000-000000000041', 0, '{}'),
			('84000000-0000-0000-0000-000000000006', '84000000-0000-0000-0000-000000000501', 'vae', '49000000-0000-0000-0000-000000000032', '49000000-0000-0000-0000-000000000042', 0, '{}'),
			('84000000-0000-0000-0000-000000000006', '84000000-0000-0000-0000-000000000501', 'thumbnail', '49000000-0000-0000-0000-000000000033', '49000000-0000-0000-0000-000000000043', 0, '{}');
		INSERT INTO execution_profile_connector_options (
			execution_profile_revision_id, execution_graph_revision_id, execution_graph_edge_id,
			connector_revision_id, required_topology_policy, preference
		) VALUES
			('84000000-0000-0000-0000-000000000006', '84000000-0000-0000-0000-000000000501', '84000000-0000-0000-0000-000000000560', '84000000-0000-0000-0000-000000000550', '{}', 0),
			('84000000-0000-0000-0000-000000000006', '84000000-0000-0000-0000-000000000501', '84000000-0000-0000-0000-000000000561', '84000000-0000-0000-0000-000000000551', '{}', 0),
			('84000000-0000-0000-0000-000000000006', '84000000-0000-0000-0000-000000000501', '84000000-0000-0000-0000-000000000562', '84000000-0000-0000-0000-000000000552', '{}', 0);
		UPDATE execution_graph_revisions
		SET content_digest = vela_execution_graph_content_digest(id)
		WHERE id = '84000000-0000-0000-0000-000000000501';
		SELECT * FROM vela_activate_execution_graph(
			'84000000-0000-0000-0000-000000000501',
			(SELECT content_digest FROM execution_graph_revisions WHERE id = '84000000-0000-0000-0000-000000000501')
		);
		INSERT INTO profile_certifications (
			id, model_revision_id, generation_preset_revision_id, output_spec_id,
			execution_profile_revision_id, state, evidence_digest, certified_at
		) VALUES (
			'84000000-0000-0000-0000-00000000000a',
			'84000000-0000-0000-0000-000000000004',
			'84000000-0000-0000-0000-000000000005',
			'84000000-0000-0000-0000-000000000007',
			'84000000-0000-0000-0000-000000000006',
			'ACTIVE', 'mock-only-stage-lab-v2-evidence', TIMESTAMPTZ '2026-09-04 00:00:00+00'
		);
	`
	if _, err := transaction.ExecContext(ctx, graphOptionsSQL); err != nil {
		return fmt.Errorf("seed and activate lab ExecutionGraph: %w", err)
	}
	return verifyStageCatalog(ctx, transaction, profiles)
}

func verifyStageCatalog(ctx context.Context, transaction *sql.Tx, profiles []labStageProfile) error {
	workerProfiles, err := labWorkerProfiles()
	if err != nil {
		return err
	}
	for _, profile := range workerProfiles {
		var matches bool
		if err := transaction.QueryRowContext(ctx, `
			SELECT stable_id = $2 AND device_count = 1 AND member_count = 1
			       AND device_set_shape = $3::jsonb
			       AND resident_model_revisions = $4::jsonb
			       AND capacity_limits = $5::jsonb
			       AND readiness_checks = $6::jsonb
			       AND encode(content_digest, 'hex') = repeat($7, 32)
			FROM worker_profile_revisions WHERE id = $1
		`, profile.id, profile.stableID, profile.deviceSetShape, profile.residentModelRevisions,
			profile.capacityLimits, profile.readinessChecks,
			fmt.Sprintf("%02x", profile.contentDigestByte)).Scan(&matches); err != nil {
			return fmt.Errorf("verify WorkerProfile %s: %w", profile.id, err)
		}
		if !matches {
			return fmt.Errorf("existing WorkerProfile %s does not match the lab contract", profile.id)
		}
	}
	for _, profile := range profiles {
		var runtimeDigest, workerProfileID, component string
		if err := transaction.QueryRowContext(ctx, `
			SELECT runtime_image_digest, worker_profile_revision_id::text, model_component_revision
			FROM stage_profile_revisions WHERE id = $1
		`, profile.id).Scan(&runtimeDigest, &workerProfileID, &component); err != nil {
			return fmt.Errorf("verify StageProfile %s: %w", profile.id, err)
		}
		if runtimeDigest != profile.runtimeImageDigest || workerProfileID != profile.workerProfileID ||
			component != profile.modelComponentRevision {
			return fmt.Errorf("existing StageProfile %s does not match the lab asset set", profile.id)
		}
	}
	var graphState string
	var topologicalOrderJSON string
	if err := transaction.QueryRowContext(ctx, `
		SELECT state::text, to_json(topological_order)::text FROM execution_graph_revisions
		WHERE id = '84000000-0000-0000-0000-000000000501'
	`).Scan(&graphState, &topologicalOrderJSON); err != nil {
		return fmt.Errorf("verify lab ExecutionGraph: %w", err)
	}
	var topologicalOrder []string
	if err := json.Unmarshal([]byte(topologicalOrderJSON), &topologicalOrder); err != nil {
		return fmt.Errorf("decode lab ExecutionGraph topological order: %w", err)
	}
	if graphState != "ACTIVE" || strings.Join(topologicalOrder, ",") != "encoder,dit,vae,thumbnail" {
		return errors.New("existing lab ExecutionGraph does not match the activated Stage graph")
	}
	return nil
}

func seedWorkerRegistry(
	ctx context.Context,
	transaction *sql.Tx,
	runtimeImageDigest, thumbnailRuntimeImageDigest string,
) error {
	workers, err := labWorkerFixtures(runtimeImageDigest, thumbnailRuntimeImageDigest)
	if err != nil {
		return err
	}
	plan, err := buildResidencyPlan(workers)
	if err != nil {
		return err
	}
	var planID string
	var workerCount int
	if err := transaction.QueryRowContext(ctx, `
		SELECT plan_revision_id::text, worker_instance_count
		FROM vela_apply_residency_plan($1::jsonb)
	`, plan).Scan(&planID, &workerCount); err != nil {
		return fmt.Errorf("apply lab ResidencyPlan: %w", err)
	}
	if planID != residencyPlanID || workerCount != len(workers) {
		return errors.New("lab ResidencyPlan result does not match the requested WorkerInstances")
	}
	for _, worker := range workers {
		var sequence int64
		if err := transaction.QueryRowContext(ctx, `
			SELECT COALESCE(max(observation_sequence), 0) + 1
			FROM capacity_observations WHERE worker_instance_id = $1
		`, worker.id).Scan(&sequence); err != nil {
			return fmt.Errorf("read %s CapacityObservation sequence: %w", worker.name, err)
		}
		evidence, err := buildWorkerEvidence(worker, sequence, time.Now().UTC().Truncate(time.Millisecond))
		if err != nil {
			return err
		}
		var observedID, readiness string
		var instanceEpoch, controlSessionEpoch, modelRuntimeEpoch int64
		if err := transaction.QueryRowContext(ctx, `
			SELECT worker_instance_id::text, instance_epoch, control_session_epoch,
			       model_runtime_epoch, readiness::text
			FROM vela_observe_worker_instance($1::jsonb)
		`, evidence).Scan(
			&observedID, &instanceEpoch, &controlSessionEpoch, &modelRuntimeEpoch, &readiness,
		); err != nil {
			return fmt.Errorf("observe %s WorkerInstance: %w", worker.name, err)
		}
		if observedID != worker.id || instanceEpoch != 1 || controlSessionEpoch != 1 ||
			modelRuntimeEpoch != 1 || readiness != "READY" {
			return fmt.Errorf("%s WorkerInstance observation returned inconsistent authority", worker.name)
		}
	}
	return nil
}

func buildResidencyPlan(workers []labWorkerFixture) ([]byte, error) {
	type capacityPool struct {
		ID                     string `json:"id"`
		StableID               string `json:"stable_id"`
		StageProfileRevisionID string `json:"stage_profile_revision_id"`
		ResourceClass          string `json:"resource_class"`
		SecurityClass          string `json:"security_class"`
		Region                 string `json:"region"`
		MaxReadyQueueDepth     int    `json:"max_ready_queue_depth"`
	}
	pools := []capacityPool{
		{encoderCapacityPoolID, "h3-encoder-lab-v2", encoderStageProfileID, "GPU", "NON_PRODUCTION", "lab", 64},
		{vaeCapacityPoolID, "h3-vae-lab-v2", vaeStageProfileID, "GPU", "NON_PRODUCTION", "lab", 64},
		{ditCapacityPoolID, "h3-dit-lab-v2", ditStageProfileID, "GPU", "NON_PRODUCTION", "lab", 64},
		{thumbnailCapacityPoolID, "h3-cpu-thumbnail-lab-v2", thumbnailStageProfileID, "CPU", "NON_PRODUCTION", "lab", 64},
	}
	bundles := make([]map[string]any, 0, len(workers))
	plannedWorkers := make([]map[string]any, 0, len(workers))
	for _, worker := range workers {
		routes := make([]map[string]string, 0, len(worker.routes))
		for _, route := range worker.routes {
			routes = append(routes, map[string]string{
				"model_residency_id":        route.modelResidencyID,
				"capacity_pool_id":          route.capacityPoolID,
				"stage_profile_revision_id": route.stageProfileID,
			})
		}
		bundles = append(bundles, map[string]any{
			"id": worker.bundleID, "stable_id": worker.name + "-bundle",
			"desired_generation": 1, "layout_digest": digestText("vela/lab-v2/" + worker.name + "/layout/v1"),
		})
		plannedWorkers = append(plannedWorkers, map[string]any{
			"id": worker.id, "worker_profile_revision_id": worker.workerProfileID,
			"capacity_pool_id": worker.primaryPoolID, "worker_bundle_id": worker.bundleID,
			"desired_member_count": 1, "desired_device_count": 1,
			"model_runtime_routes": routes,
		})
	}
	payload := map[string]any{
		"schema_version": 1, "id": residencyPlanID, "stable_id": "h3-mock-lab-v2", "revision": 1,
		"content_digest":           digestText("vela/lab-v2/residency-plan/v1"),
		"approval_evidence_digest": digestText("non-production approval: vela/lab-v2/residency-plan/v1"),
		"approved_at":              "2026-09-04T00:00:00Z", "approved_by": "bootstrap/non-production-lab-v2",
		"capacity_pools": pools, "worker_bundles": bundles, "worker_instances": plannedWorkers,
	}
	encoded, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("encode lab ResidencyPlan: %w", err)
	}
	return encoded, nil
}

func buildWorkerEvidence(worker labWorkerFixture, sequence int64, observedAt time.Time) ([]byte, error) {
	if sequence <= 0 || observedAt.IsZero() {
		return nil, errors.New("WorkerInstance observation sequence and time are required")
	}
	membershipDigest := digestText("vela/lab-v2/" + worker.name + "/membership/v1")
	topologyDigest := digestText("vela/lab-v2/" + worker.name + "/topology/v1")
	residencies := make([]map[string]any, 0, len(worker.residencies))
	for _, residency := range worker.residencies {
		residencies = append(residencies, map[string]any{
			"id": residency.id, "model_component_revision": residency.modelComponentRevision,
			"runtime_identity": residency.runtimeIdentity, "runtime_image_digest": residency.runtimeImageDigest,
			"model_runtime_epoch": 1, "state": "READY",
			"warmup_evidence_digest": digestText("vela/lab-v2/" + residency.id + "/warmup/v1"),
			"canary_evidence_digest": digestText("vela/lab-v2/" + residency.id + "/canary/v1"),
		})
	}
	device := map[string]any{
		"id": worker.deviceID, "compute_node_id": worker.nodeID, "node_identity": worker.name,
		"region": "lab", "network_domain": "rke2-overlay", "fault_domain": worker.name,
		"node_epoch": 1, "agent_session_epoch": 1,
		"node_attestation_digest": digestText("vela/lab-v2/" + worker.name + "/node-attestation/v1"),
		"kind":                    worker.resourceClass,
		"device_epoch":            1, "ordinal": 0, "health": "HEALTHY",
		"attestation_digest": digestText("vela/lab-v2/" + worker.name + "/device-attestation/v1"),
	}
	if worker.resourceClass == "GPU" {
		device["gpu_uuid"] = worker.gpuUUID
		device["pci_bdf"] = worker.pciBDF
	}
	payload := map[string]any{
		"schema_version": 1, "worker_instance_id": worker.id,
		"instance_epoch": 1, "control_session_epoch": 1,
		"device_set": map[string]any{
			"id": worker.deviceSetID, "membership_digest": membershipDigest, "topology_digest": topologyDigest,
			"devices": []map[string]any{device},
		},
		"members": []map[string]any{{
			"id": worker.memberID, "member_key": "member-0", "compute_node_id": worker.nodeID,
			"member_epoch": 1, "device_ids": []string{worker.deviceID},
			"device_subset_digest": digestText("vela/lab-v2/" + worker.name + "/device-subset/v1"),
			"identity_digest":      digestText("spiffe://vela.internal/stage-worker/" + worker.memberID),
			"readiness":            "READY",
		}},
		"residencies": residencies,
		"capacity": map[string]any{
			"sequence": sequence, "vector": worker.capacityVector,
			"observed_at": observedAt.Format(time.RFC3339Nano),
			"expires_at":  observedAt.Add(15 * time.Minute).Format(time.RFC3339Nano),
		},
		"observed_at": observedAt.Format(time.RFC3339Nano), "observed_by": "bootstrap/non-production-lab-v2",
	}
	encoded, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("encode %s WorkerInstance evidence: %w", worker.name, err)
	}
	return encoded, nil
}

func digestText(value string) string {
	digest := sha256.Sum256([]byte(value))
	return fmt.Sprintf("%x", digest[:])
}

func seedRequiredControlPrincipals(ctx context.Context, transaction *sql.Tx) error {
	for _, fixture := range requiredControlPrincipals {
		principalTable := pgx.Identifier{fixture.principalTable}.Sanitize()
		bindingTable := pgx.Identifier{fixture.bindingTable}.Sanitize()
		if _, err := transaction.ExecContext(ctx, fmt.Sprintf(`
			INSERT INTO %s (id, stable_id, tls_uri_identity)
			VALUES ($1, $2, $3)
			ON CONFLICT (id) DO NOTHING
		`, principalTable), fixture.principalID, fixture.stableID, fixture.tlsURI); err != nil {
			return fmt.Errorf("seed %s Principal: %w", fixture.name, err)
		}
		if _, err := transaction.ExecContext(ctx, fmt.Sprintf(`
			INSERT INTO %s (database_role, principal_id)
			VALUES ($1, $2)
			ON CONFLICT (database_role) DO NOTHING
		`, bindingTable), fixture.databaseRole, fixture.principalID); err != nil {
			return fmt.Errorf("bind %s Principal: %w", fixture.name, err)
		}
		var stableID, tlsURI, status, boundPrincipalID string
		var principalEnabled, bindingEnabled bool
		if err := transaction.QueryRowContext(ctx, fmt.Sprintf(`
			SELECT principal.stable_id, principal.tls_uri_identity, principal.status,
				principal.disabled_at IS NULL, binding.principal_id::text,
				binding.disabled_at IS NULL
			FROM %s AS principal
			JOIN %s AS binding ON binding.database_role = $2
			WHERE principal.id = $1
		`, principalTable, bindingTable), fixture.principalID, fixture.databaseRole).Scan(
			&stableID, &tlsURI, &status, &principalEnabled, &boundPrincipalID, &bindingEnabled,
		); err != nil {
			return fmt.Errorf("verify %s Principal binding: %w", fixture.name, err)
		}
		if stableID != fixture.stableID || tlsURI != fixture.tlsURI || status != "ACTIVE" ||
			!principalEnabled || boundPrincipalID != fixture.principalID || !bindingEnabled {
			return fmt.Errorf("existing %s Principal binding does not match the lab asset set", fixture.name)
		}
	}
	return nil
}

func bootstrapMinIO(ctx context.Context, configuration configuration) error {
	rootPEM, err := os.ReadFile(configuration.minioRootCA)
	if err != nil {
		return fmt.Errorf("read MinIO Root CA: %w", err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(rootPEM) {
		return errors.New("MinIO Root CA contains no certificates")
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.TLSClientConfig = &tls.Config{MinVersion: tls.VersionTLS13, RootCAs: roots}
	client := s3.NewFromConfig(aws.Config{
		Region:     "us-east-1",
		HTTPClient: &http.Client{Transport: transport, Timeout: 10 * time.Second},
		Credentials: credentials.NewStaticCredentialsProvider(
			configuration.minioAccessKey, configuration.minioSecretKey, "",
		),
	}, func(options *s3.Options) {
		options.BaseEndpoint = aws.String(strings.TrimRight(configuration.minioEndpoint, "/"))
		options.UsePathStyle = true
	})
	for _, bucket := range []string{primaryBucket, backupBucket} {
		if err := ensureVersionedBucket(ctx, client, bucket); err != nil {
			return err
		}
	}
	return nil
}

func ensureVersionedBucket(ctx context.Context, client *s3.Client, bucket string) error {
	for {
		_, err := client.HeadBucket(ctx, &s3.HeadBucketInput{Bucket: aws.String(bucket)})
		if err == nil {
			break
		}
		if ctx.Err() != nil {
			return fmt.Errorf("wait for MinIO bucket %s: %w", bucket, ctx.Err())
		}
		if _, createErr := client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)}); createErr == nil {
			break
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("wait for MinIO bucket %s: %w", bucket, ctx.Err())
		case <-time.After(time.Second):
		}
	}
	if _, err := client.PutBucketVersioning(ctx, &s3.PutBucketVersioningInput{
		Bucket:                  aws.String(bucket),
		VersioningConfiguration: &types.VersioningConfiguration{Status: types.BucketVersioningStatusEnabled},
	}); err != nil {
		return fmt.Errorf("enable MinIO bucket versioning for %s: %w", bucket, err)
	}
	return nil
}

func bootstrapNATS(ctx context.Context, configuration configuration) error {
	rootPEM, err := os.ReadFile(configuration.natsRootCA)
	if err != nil {
		return fmt.Errorf("read NATS Root CA: %w", err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(rootPEM) {
		return errors.New("NATS Root CA contains no certificates")
	}
	certificate, err := tls.LoadX509KeyPair(configuration.natsCert, configuration.natsKey)
	if err != nil {
		return fmt.Errorf("load NATS client certificate: %w", err)
	}
	parsed, err := url.Parse(configuration.natsURL)
	if err != nil || parsed.Hostname() == "" {
		return errors.New("VELA_LAB_NATS_URL is invalid")
	}
	tlsConfiguration := &tls.Config{
		MinVersion: tls.VersionTLS13, RootCAs: roots,
		Certificates: []tls.Certificate{certificate}, ServerName: parsed.Hostname(),
	}
	var connection *nats.Conn
	for {
		connection, err = nats.Connect(
			configuration.natsURL,
			nats.UserCredentials(configuration.natsCredential),
			nats.Secure(tlsConfiguration),
			nats.Timeout(2*time.Second),
		)
		if err == nil {
			break
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("wait for NATS: %w", ctx.Err())
		case <-time.After(time.Second):
		}
	}
	defer connection.Close()
	client, err := jetstream.New(connection)
	if err != nil {
		return fmt.Errorf("create JetStream bootstrap client: %w", err)
	}
	var stream jetstream.Stream
	for {
		stream, err = client.Stream(ctx, eventstream.StreamName)
		if errors.Is(err, jetstream.ErrStreamNotFound) {
			stream, err = client.CreateStream(ctx, eventstream.StreamConfig())
		}
		if err == nil {
			break
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("create three-replica lab JetStream stream: %w", ctx.Err())
		case <-time.After(2 * time.Second):
		}
	}
	information, err := stream.Info(ctx)
	if err != nil {
		return fmt.Errorf("read JetStream stream: %w", err)
	}
	if err := eventstream.ValidateStreamConfig(information.Config); err != nil {
		return fmt.Errorf("JetStream stream contract: %w", err)
	}
	consumer, err := stream.Consumer(ctx, eventstream.SchedulerConsumerName)
	if errors.Is(err, jetstream.ErrConsumerNotFound) {
		consumer, err = stream.CreateConsumer(ctx, eventstream.SchedulerConsumerConfig())
	}
	if err != nil {
		return fmt.Errorf("create Scheduler JetStream consumer: %w", err)
	}
	consumerInformation, err := consumer.Info(ctx)
	if err != nil {
		return fmt.Errorf("read Scheduler JetStream consumer: %w", err)
	}
	if err := eventstream.ValidateSchedulerConsumerConfig(consumerInformation.Config); err != nil {
		return fmt.Errorf("scheduler JetStream consumer contract: %w", err)
	}
	return nil
}

func clear(value []byte) {
	for index := range value {
		value[index] = 0
	}
}
