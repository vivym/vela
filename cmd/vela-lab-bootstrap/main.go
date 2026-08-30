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
)

const (
	organizationID               = "84000000-0000-0000-0000-000000000001"
	projectID                    = "84000000-0000-0000-0000-000000000002"
	principalID                  = "84000000-0000-0000-0000-000000000003"
	modelRevisionID              = "84000000-0000-0000-0000-000000000004"
	presetRevisionID             = "84000000-0000-0000-0000-000000000005"
	executionProfileID           = "84000000-0000-0000-0000-000000000006"
	outputSpecID                 = "84000000-0000-0000-0000-000000000007"
	workerPoolID                 = "84000000-0000-0000-0000-000000000008"
	serviceClassID               = "84000000-0000-0000-0000-000000000009"
	certificationID              = "84000000-0000-0000-0000-00000000000a"
	rateCardRevisionID           = "84000000-0000-0000-0000-00000000000b"
	rateCardLineID               = "84000000-0000-0000-0000-00000000000c"
	credentialID                 = "84000000-0000-0000-0000-00000000000d"
	financePrincipalID           = "84000000-0000-0000-0000-00000000000e"
	compliancePrincipalID        = "84000000-0000-0000-0000-00000000000f"
	worker1ID                    = "84000000-0000-0000-0000-000000000101"
	worker2ID                    = "84000000-0000-0000-0000-000000000102"
	worker1Name                  = "vela-lab-worker-1"
	worker2Name                  = "vela-lab-worker-2"
	primaryBucket                = "vela-lab-artifacts"
	backupBucket                 = "vela-lab-artifacts-backup"
	defaultDatabaseRoot          = "/opt/vela/share/db"
	defaultNATSCreds             = "/etc/vela-lab-bootstrap/nats/bootstrap.creds"
	defaultNATSRootCA            = "/etc/vela-lab-bootstrap/nats/ca.crt"
	defaultNATSCert              = "/etc/vela-lab-bootstrap/nats/tls.crt"
	defaultNATSKey               = "/etc/vela-lab-bootstrap/nats/tls.key"
	defaultSmokeSecret           = "/etc/vela-lab-bootstrap/smoke/smoke-secret"
	defaultTimeout               = 4 * time.Minute
	executionLeaseRenewalReceipt = "non-production lab bootstrap verified zero active EXECUTION Leases"
)

type options struct {
	databaseRoot   string
	natsCredential string
	natsRootCA     string
	natsCert       string
	natsKey        string
	smokeSecret    string
	timeout        time.Duration
}

type configuration struct {
	options
	adminDatabaseURL string
	loginPasswords   map[string]string
	databaseURLs     map[string]string
	credentialPepper []byte
	minioEndpoint    string
	minioAccessKey   string
	minioSecretKey   string
	natsURL          string
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
	"VELA_SCHEDULER_DATABASE_URL",
	"VELA_SCHEDULER_INBOX_DATABASE_URL",
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
		"smoke secret":            options.smokeSecret,
	} {
		if !filepath.IsAbs(filepath.Clean(path)) {
			return configuration{}, fmt.Errorf("%s path must be absolute", name)
		}
	}
	result := configuration{
		options:          options,
		adminDatabaseURL: strings.TrimSpace(os.Getenv("VELA_LAB_POSTGRES_ADMIN_URL")),
		minioEndpoint:    strings.TrimSpace(os.Getenv("VELA_LAB_MINIO_ENDPOINT")),
		minioAccessKey:   strings.TrimSpace(os.Getenv("VELA_LAB_MINIO_ACCESS_KEY")),
		minioSecretKey:   os.Getenv("VELA_LAB_MINIO_SECRET_KEY"),
		natsURL:          strings.TrimSpace(os.Getenv("VELA_LAB_NATS_URL")),
		databaseURLs:     make(map[string]string, len(requiredDatabaseEnvironments)),
	}
	if result.adminDatabaseURL == "" || result.minioEndpoint == "" ||
		result.minioAccessKey == "" || result.minioSecretKey == "" ||
		result.natsURL == "" {
		return configuration{}, errors.New("lab dependency environment is incomplete")
	}
	if _, err := url.ParseRequestURI(result.adminDatabaseURL); err != nil {
		return configuration{}, fmt.Errorf("parse PostgreSQL admin URL: %w", err)
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
	if err := seedLabFixture(ctx, database, credentialDigest); err != nil {
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

func seedLabFixture(ctx context.Context, database *sql.DB, credentialDigest []byte) error {
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
		{`INSERT INTO worker_pools (id, stable_id, queued_limit, retry_running_limit) VALUES ($1, 'h3-mock-lab', 32, 1) ON CONFLICT (id) DO NOTHING`, []any{workerPoolID}},
		{`INSERT INTO model_revisions (id, stable_id, revision, state, content_hash) VALUES ($1, 'h3-mock', 1, 'ACTIVE', 'mock-only-765077057011f16f852886601235f066') ON CONFLICT (id) DO NOTHING`, []any{modelRevisionID}},
		{`INSERT INTO generation_preset_revisions (id, model_revision_id, stable_id, revision, state, certified_p95_compute_seconds) VALUES ($1, $2, 'balanced', 1, 'ACTIVE', 30) ON CONFLICT (id) DO NOTHING`, []any{presetRevisionID, modelRevisionID}},
		{`INSERT INTO service_class_revisions (id, stable_id, revision, state, queue_retry_allowance_seconds, max_attempts, max_total_compute_multiplier_milli, max_finalization_seconds_per_attempt, retry_backoff_policy, retryable_failure_classes, circuit_breaker_policy) VALUES ($1, 'standard', 1, 'ACTIVE', 600, 2, 2000, 300, '{"kind":"exponential","initial_seconds":2,"max_seconds":10}', ARRAY['WORKER_LOST','TRANSIENT_BACKEND'], '{"policy_revision":"mock-lab-v1"}') ON CONFLICT (id) DO NOTHING`, []any{serviceClassID}},
		{`INSERT INTO output_specs (id, stable_id, revision, state, width, height, duration_milliseconds, frame_rate_milli, codec, container, thumbnail_required) VALUES ($1, 'mock-video-1080p-5s-24fps', 1, 'ACTIVE', 1920, 1080, 5000, 24000, 'h264', 'mp4', true) ON CONFLICT (id) DO NOTHING`, []any{outputSpecID}},
		{`INSERT INTO execution_profile_revisions (id, model_revision_id, worker_pool_id, stable_id, revision, state) VALUES ($1, $2, $3, 'h3-mock-balanced', 1, 'ACTIVE') ON CONFLICT (id) DO NOTHING`, []any{executionProfileID, modelRevisionID, workerPoolID}},
		{`INSERT INTO profile_certifications (id, model_revision_id, generation_preset_revision_id, output_spec_id, execution_profile_revision_id, state, evidence_digest, certified_at) VALUES ($1, $2, $3, $4, $5, 'ACTIVE', 'mock-only-lab-protocol-evidence', clock_timestamp()) ON CONFLICT (id) DO NOTHING`, []any{certificationID, modelRevisionID, presetRevisionID, outputSpecID, executionProfileID}},
		{`INSERT INTO rate_card_revisions (id, revision, state, effective_at) VALUES ($1, 840000001, 'ACTIVE', clock_timestamp() - interval '1 hour') ON CONFLICT (id) DO NOTHING`, []any{rateCardRevisionID}},
		{`INSERT INTO rate_card_lines (id, rate_card_revision_id, model_revision_id, generation_preset_revision_id, service_class_revision_id, output_spec_id, unit_amount_minor, currency) VALUES ($1, $2, $3, $4, $5, $6, 1, 'CNY') ON CONFLICT (id) DO NOTHING`, []any{rateCardLineID, rateCardRevisionID, modelRevisionID, presetRevisionID, serviceClassID, outputSpecID}},
		{`INSERT INTO organization_capacity_shares (worker_pool_id, organization_id, weight, running_limit) VALUES ($1, $2, 1, 2) ON CONFLICT (worker_pool_id, organization_id) DO NOTHING`, []any{workerPoolID, organizationID}},
		{`INSERT INTO project_capacity_shares (worker_pool_id, organization_id, project_id, weight) VALUES ($1, $2, $3, 1) ON CONFLICT (worker_pool_id, organization_id, project_id) DO NOTHING`, []any{workerPoolID, organizationID, projectID}},
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
	workers := []struct{ id, name string }{{worker1ID, worker1Name}, {worker2ID, worker2Name}}
	for _, worker := range workers {
		if _, err := transaction.ExecContext(ctx, `
			INSERT INTO workers (id, worker_pool_id, spiffe_id, epoch, lifecycle_state, reachability_condition, node_identity)
			VALUES ($1, $2, $3, 1, 'READY', 'HEALTHY', $4)
			ON CONFLICT (id) DO NOTHING
		`, worker.id, workerPoolID, "spiffe://vela.internal/worker/"+worker.name, worker.name); err != nil {
			return fmt.Errorf("seed Worker %s: %w", worker.name, err)
		}
		if _, err := transaction.ExecContext(ctx, `
			INSERT INTO worker_profile_readiness (
				worker_id, worker_epoch, execution_profile_revision_id, readiness,
				model_cold_start_penalty_seconds, locality_penalty_seconds, health_risk_penalty_seconds
			) VALUES ($1, 1, $2, 'WARM', 0, 0, 0)
			ON CONFLICT (worker_id, worker_epoch, execution_profile_revision_id) DO NOTHING
		`, worker.id, executionProfileID); err != nil {
			return fmt.Errorf("seed Worker readiness %s: %w", worker.name, err)
		}
	}
	if _, err := transaction.ExecContext(ctx, `
		SELECT vela_configure_worker_pool_capacity(
			$1, repeat('8', 64), 644245094400, 536870912000, 107374182400,
			1288490188800, 1073741824000, 90, 'bootstrap/non-production-lab'
		)
	`, workerPoolID); err != nil {
		return fmt.Errorf("configure lab Worker capacity policy: %w", err)
	}
	var protocolEnforced bool
	if err := transaction.QueryRowContext(ctx, `SELECT enforced FROM fleet_assignment_protocol_state WHERE singleton`).Scan(&protocolEnforced); err != nil {
		return fmt.Errorf("read Fleet protocol state: %w", err)
	}
	if !protocolEnforced {
		if _, err := transaction.ExecContext(ctx, `SELECT vela_transition_fleet_assignment_protocol(true, 'non-production lab verified zero legacy Assignment writers', 0)`); err != nil {
			return fmt.Errorf("enable lab Fleet Assignment protocol: %w", err)
		}
	}
	var leaseRenewalEnabled bool
	if err := transaction.QueryRowContext(ctx, `SELECT enabled FROM execution_lease_renewal_protocol WHERE singleton`).Scan(&leaseRenewalEnabled); err != nil {
		return fmt.Errorf("read execution Lease renewal protocol state: %w", err)
	}
	if !leaseRenewalEnabled {
		if _, err := transaction.ExecContext(
			ctx,
			`SELECT vela_transition_execution_lease_renewal_protocol(true, $1)`,
			executionLeaseRenewalReceipt,
		); err != nil {
			return fmt.Errorf("enable lab execution Lease renewal protocol: %w", err)
		}
		if err := transaction.QueryRowContext(ctx, `SELECT enabled FROM execution_lease_renewal_protocol WHERE singleton`).Scan(&leaseRenewalEnabled); err != nil {
			return fmt.Errorf("verify execution Lease renewal protocol state: %w", err)
		}
	}
	if !leaseRenewalEnabled {
		return errors.New("execution Lease renewal protocol remains disabled")
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
	client := s3.NewFromConfig(aws.Config{
		Region: "us-east-1",
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
