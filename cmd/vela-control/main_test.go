package main

import (
	"context"
	"encoding/base64"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/vivym/vela/internal/billingexport"
	"github.com/vivym/vela/internal/cancellation"
	"github.com/vivym/vela/internal/retention"
	"github.com/vivym/vela/internal/scheduler"
	"github.com/vivym/vela/internal/webhook"
)

func TestLoadConfigRequiresNATSWorkloadCredentialsAndRootCA(t *testing.T) {
	tests := []struct {
		name       string
		missingEnv string
	}{
		{name: "workload credentials", missingEnv: "VELA_NATS_CREDENTIALS_FILE"},
		{name: "root CA", missingEnv: "VELA_NATS_ROOT_CA_FILE"},
		{name: "Outbox workload account", missingEnv: "VELA_NATS_OUTBOX_ACCOUNT_PUBLIC_KEY"},
		{name: "Outbox workload account signers", missingEnv: "VELA_NATS_OUTBOX_ACCOUNT_SIGNER_PUBLIC_KEYS"},
		{name: "Outbox workload users", missingEnv: "VELA_NATS_OUTBOX_USER_PUBLIC_KEYS"},
		{name: "Human auth database", missingEnv: "VELA_HUMAN_AUTH_DATABASE_URL"},
		{name: "Human membership auth database", missingEnv: "VELA_HUMAN_MEMBERSHIP_AUTH_DATABASE_URL"},
		{name: "identity request database", missingEnv: "VELA_IDENTITY_REQUEST_DATABASE_URL"},
		{name: "Human membership request database", missingEnv: "VELA_HUMAN_MEMBERSHIP_REQUEST_DATABASE_URL"},
		{name: "Organization billing request database", missingEnv: "VELA_ORGANIZATION_BILLING_REQUEST_DATABASE_URL"},
		{name: "Organization audit request database", missingEnv: "VELA_ORGANIZATION_AUDIT_REQUEST_DATABASE_URL"},
		{name: "retention request database", missingEnv: "VELA_RETENTION_REQUEST_DATABASE_URL"},
		{name: "retention database", missingEnv: "VELA_RETENTION_DATABASE_URL"},
		{name: "Platform Operator auth database", missingEnv: "VELA_PLATFORM_OPERATOR_AUTH_DATABASE_URL"},
		{name: "Break-glass request database", missingEnv: "VELA_BREAK_GLASS_REQUEST_DATABASE_URL"},
		{name: "Break-glass audit database", missingEnv: "VELA_BREAK_GLASS_AUDIT_DATABASE_URL"},
		{name: "retention Reconciler identity", missingEnv: "VELA_RETENTION_RECONCILER_ID"},
		{name: "Artifact request database", missingEnv: "VELA_ARTIFACT_REQUEST_DATABASE_URL"},
		{name: "OIDC issuer", missingEnv: "VELA_OIDC_ISSUER"},
		{name: "OIDC audience", missingEnv: "VELA_OIDC_AUDIENCE"},
		{name: "OIDC JWKS URL", missingEnv: "VELA_OIDC_JWKS_URL"},
		{name: "Platform OIDC issuer", missingEnv: "VELA_PLATFORM_OIDC_ISSUER"},
		{name: "Platform OIDC audience", missingEnv: "VELA_PLATFORM_OIDC_AUDIENCE"},
		{name: "Platform OIDC JWKS URL", missingEnv: "VELA_PLATFORM_OIDC_JWKS_URL"},
		{name: "Scheduler database", missingEnv: "VELA_SCHEDULER_DATABASE_URL"},
		{name: "Scheduler identity", missingEnv: "VELA_SCHEDULER_ID"},
		{name: "billing database", missingEnv: "VELA_BILLING_DATABASE_URL"},
		{name: "Finance Reconciliation database", missingEnv: "VELA_FINANCE_RECONCILIATION_DATABASE_URL"},
		{name: "Finance Reconciliation address", missingEnv: "VELA_FINANCE_RECONCILIATION_ADDR"},
		{name: "Finance Reconciliation server certificate", missingEnv: "VELA_FINANCE_RECONCILIATION_SERVER_CERT_FILE"},
		{name: "Finance Reconciliation server key", missingEnv: "VELA_FINANCE_RECONCILIATION_SERVER_KEY_FILE"},
		{name: "Finance Reconciliation client CA", missingEnv: "VELA_FINANCE_RECONCILIATION_CLIENT_CA_FILE"},
		{name: "Remediation database", missingEnv: "VELA_REMEDIATION_DATABASE_URL"},
		{name: "Remediation actor", missingEnv: "VELA_REMEDIATION_ACTOR_IDENTITY"},
		{name: "Remediation Node Agent endpoints", missingEnv: "VELA_REMEDIATION_NODE_AGENTS_FILE"},
		{name: "Remediation TLS certificate", missingEnv: "VELA_REMEDIATION_TLS_CERT_FILE"},
		{name: "Remediation TLS key", missingEnv: "VELA_REMEDIATION_TLS_KEY_FILE"},
		{name: "Remediation TLS root CA", missingEnv: "VELA_REMEDIATION_TLS_ROOT_CA_FILE"},
		{name: "webhook request database", missingEnv: "VELA_WEBHOOK_REQUEST_DATABASE_URL"},
		{name: "webhook database", missingEnv: "VELA_WEBHOOK_DATABASE_URL"},
		{name: "webhook encryption active key", missingEnv: "VELA_WEBHOOK_ENCRYPTION_ACTIVE_KEY_ID"},
		{name: "webhook encryption keyring", missingEnv: "VELA_WEBHOOK_ENCRYPTION_KEYRING_FILE"},
		{name: "webhook Dispatcher identity", missingEnv: "VELA_WEBHOOK_DISPATCHER_ID"},
		{name: "Invoice exporter identity", missingEnv: "VELA_INVOICE_EXPORTER_ID"},
		{name: "Invoice endpoint", missingEnv: "VELA_INVOICE_EXPORT_ENDPOINT"},
		{name: "Invoice bearer token", missingEnv: "VELA_INVOICE_EXPORT_BEARER_TOKEN_FILE"},
		{name: "Artifact S3 endpoint", missingEnv: "VELA_ARTIFACT_S3_ENDPOINT"},
		{name: "Artifact S3 region", missingEnv: "VELA_ARTIFACT_S3_REGION"},
		{name: "Artifact S3 bucket", missingEnv: "VELA_ARTIFACT_S3_BUCKET"},
		{name: "Artifact S3 access key", missingEnv: "VELA_ARTIFACT_S3_ACCESS_KEY_ID_FILE"},
		{name: "Artifact S3 secret key", missingEnv: "VELA_ARTIFACT_S3_SECRET_ACCESS_KEY_FILE"},
		{name: "Lease active key", missingEnv: "VELA_LEASE_ACTIVE_KEY_ID"},
		{name: "Lease keyring", missingEnv: "VELA_LEASE_KEYRING_FILE"},
		{name: "Artifact validator helper", missingEnv: "VELA_ARTIFACT_VALIDATOR_HELPER_PATH"},
		{name: "ffprobe", missingEnv: "VELA_ARTIFACT_FFPROBE_PATH"},
		{name: "Artifact sandbox root", missingEnv: "VELA_ARTIFACT_SANDBOX_ROOT"},
		{name: "Artifact spool", missingEnv: "VELA_ARTIFACT_SPOOL_DIRECTORY"},
		{name: "ffprobe version", missingEnv: "VELA_ARTIFACT_FFPROBE_VERSION"},
		{name: "Artifact validator revision", missingEnv: "VELA_ARTIFACT_VALIDATOR_REVISION"},
		{name: "Artifact Reconciler identity", missingEnv: "VELA_ARTIFACT_RECONCILER_ID"},
		{name: "Worker gRPC server certificate", missingEnv: "VELA_WORKER_GRPC_TLS_CERT_FILE"},
		{name: "Worker gRPC server key", missingEnv: "VELA_WORKER_GRPC_TLS_KEY_FILE"},
		{name: "Worker gRPC client CA", missingEnv: "VELA_WORKER_GRPC_CLIENT_CA_FILE"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			setValidConfigEnvironment(t)
			t.Setenv(test.missingEnv, "")

			_, err := loadConfig()
			if err == nil || !strings.Contains(err.Error(), test.missingEnv+" is required") {
				t.Fatalf("loadConfig error = %v, want missing %s", err, test.missingEnv)
			}
		})
	}
}

func TestReadNodeAgentEndpointsRejectsWritableOrUnknownRegistry(t *testing.T) {
	directory := t.TempDir()
	path := filepath.Join(directory, "node-agents.json")
	valid := `{"node-1":{"address":"127.0.0.1:9443","server_name":"node-agent.internal","worker_id":"10000000-0000-0000-0000-000000000001","spiffe_identity":"spiffe://vela.internal/node-agent/bm9kZS0x/10000000-0000-0000-0000-000000000001"}}`
	if err := os.WriteFile(path, []byte(valid), 0o600); err != nil {
		t.Fatalf("write endpoint registry: %v", err)
	}
	if endpoints, err := readNodeAgentEndpoints(path); err != nil || len(endpoints) != 1 {
		t.Fatalf("read endpoint registry = %#v error=%v", endpoints, err)
	}
	unknown := strings.Replace(valid, `"worker_id"`, `"unknown":true,"worker_id"`, 1)
	if err := os.WriteFile(path, []byte(unknown), 0o600); err != nil {
		t.Fatalf("write endpoint registry with unknown field: %v", err)
	}
	if _, err := readNodeAgentEndpoints(path); err == nil {
		t.Fatal("unknown endpoint registry field was accepted")
	}
	if err := os.WriteFile(path, []byte(valid), 0o600); err != nil {
		t.Fatalf("restore endpoint registry: %v", err)
	}
	if err := os.Chmod(path, 0o622); err != nil {
		t.Fatalf("relax endpoint registry permissions: %v", err)
	}
	if _, err := readNodeAgentEndpoints(path); err == nil {
		t.Fatal("group/world-writable endpoint registry was accepted")
	}
}

func setValidConfigEnvironment(t *testing.T) {
	t.Helper()
	t.Setenv("VELA_AUTH_DATABASE_URL", "postgres://auth.example/vela")
	t.Setenv("VELA_HUMAN_AUTH_DATABASE_URL", "postgres://human-auth.example/vela")
	t.Setenv(
		"VELA_HUMAN_MEMBERSHIP_AUTH_DATABASE_URL",
		"postgres://human-membership-auth.example/vela",
	)
	t.Setenv("VELA_IDENTITY_REQUEST_DATABASE_URL", "postgres://identity-request.example/vela")
	t.Setenv(
		"VELA_HUMAN_MEMBERSHIP_REQUEST_DATABASE_URL",
		"postgres://human-membership-request.example/vela",
	)
	t.Setenv(
		"VELA_ORGANIZATION_BILLING_REQUEST_DATABASE_URL",
		"postgres://organization-billing-request.example/vela",
	)
	t.Setenv(
		"VELA_ORGANIZATION_AUDIT_REQUEST_DATABASE_URL",
		"postgres://organization-audit-request.example/vela",
	)
	t.Setenv("VELA_RETENTION_REQUEST_DATABASE_URL", "postgres://retention-request.example/vela")
	t.Setenv("VELA_RETENTION_DATABASE_URL", "postgres://retention.example/vela")
	t.Setenv(
		"VELA_PLATFORM_OPERATOR_AUTH_DATABASE_URL",
		"postgres://platform-operator-auth.example/vela",
	)
	t.Setenv(
		"VELA_BREAK_GLASS_REQUEST_DATABASE_URL",
		"postgres://break-glass-request.example/vela",
	)
	t.Setenv(
		"VELA_BREAK_GLASS_AUDIT_DATABASE_URL",
		"postgres://break-glass-audit.example/vela",
	)
	t.Setenv("VELA_RETENTION_RECONCILER_ID", "vela-control-retention-reconciler-1")
	t.Setenv("VELA_REQUEST_DATABASE_URL", "postgres://request.example/vela")
	t.Setenv("VELA_ARTIFACT_REQUEST_DATABASE_URL", "postgres://artifact-request.example/vela")
	t.Setenv("VELA_OIDC_ISSUER", "https://identity.example.com")
	t.Setenv("VELA_OIDC_AUDIENCE", "vela-control")
	t.Setenv("VELA_OIDC_JWKS_URL", "https://identity.example.com/.well-known/jwks.json")
	t.Setenv("VELA_PLATFORM_OIDC_ISSUER", "https://platform-identity.example.com")
	t.Setenv("VELA_PLATFORM_OIDC_AUDIENCE", "vela-platform-control")
	t.Setenv(
		"VELA_PLATFORM_OIDC_JWKS_URL",
		"https://platform-identity.example.com/.well-known/jwks.json",
	)
	t.Setenv("VELA_CANCEL_DATABASE_URL", "postgres://cancel.example/vela")
	t.Setenv("VELA_INTERNAL_DATABASE_URL", "postgres://internal.example/vela")
	t.Setenv("VELA_REMEDIATION_DATABASE_URL", "postgres://remediation.example/vela")
	t.Setenv("VELA_REMEDIATION_ACTOR_IDENTITY", "controller/control-1")
	t.Setenv("VELA_REMEDIATION_NODE_AGENTS_FILE", "/run/vela/node-agents.json")
	t.Setenv("VELA_REMEDIATION_TLS_CERT_FILE", "/run/tls/remediation/client.crt")
	t.Setenv("VELA_REMEDIATION_TLS_KEY_FILE", "/run/tls/remediation/client.key")
	t.Setenv("VELA_REMEDIATION_TLS_ROOT_CA_FILE", "/run/tls/remediation/ca.crt")
	t.Setenv("VELA_SCHEDULER_DATABASE_URL", "postgres://scheduler.example/vela")
	t.Setenv("VELA_SCHEDULER_ID", "vela-control-scheduler-1")
	t.Setenv("VELA_BILLING_DATABASE_URL", "postgres://billing.example/vela")
	t.Setenv(
		"VELA_FINANCE_RECONCILIATION_DATABASE_URL",
		"postgres://finance-reconciliation.example/vela",
	)
	t.Setenv("VELA_FINANCE_RECONCILIATION_ADDR", "127.0.0.1:9444")
	t.Setenv(
		"VELA_FINANCE_RECONCILIATION_SERVER_CERT_FILE",
		"/run/tls/finance-reconciliation/tls.crt",
	)
	t.Setenv(
		"VELA_FINANCE_RECONCILIATION_SERVER_KEY_FILE",
		"/run/tls/finance-reconciliation/tls.key",
	)
	t.Setenv(
		"VELA_FINANCE_RECONCILIATION_CLIENT_CA_FILE",
		"/run/tls/finance-reconciliation/client-ca.crt",
	)
	t.Setenv("VELA_WEBHOOK_REQUEST_DATABASE_URL", "postgres://webhook-request.example/vela")
	t.Setenv("VELA_WEBHOOK_DATABASE_URL", "postgres://webhook.example/vela")
	t.Setenv("VELA_WEBHOOK_ENCRYPTION_ACTIVE_KEY_ID", "webhook-key-v1")
	t.Setenv("VELA_WEBHOOK_ENCRYPTION_KEYRING_FILE", "/run/secrets/webhook-keyring.json")
	t.Setenv("VELA_WEBHOOK_DISPATCHER_ID", "vela-control-webhook-dispatcher-1")
	t.Setenv("VELA_INVOICE_EXPORTER_ID", "vela-control-invoice-exporter-1")
	t.Setenv("VELA_INVOICE_EXPORT_ENDPOINT", "https://finance.example/v1/invoice-lines")
	t.Setenv("VELA_INVOICE_EXPORT_BEARER_TOKEN_FILE", "/run/secrets/invoice-bearer-token")
	t.Setenv("VELA_NATS_URL", "tls://nats.example:4222")
	t.Setenv("VELA_NATS_CREDENTIALS_FILE", "/run/secrets/vela-control.creds")
	t.Setenv("VELA_NATS_ROOT_CA_FILE", "/run/secrets/nats-root-ca.pem")
	t.Setenv(
		"VELA_NATS_OUTBOX_ACCOUNT_PUBLIC_KEY",
		"AB46MZ7D6VS7MGXZLQYRYSBZB63GEI2CIKAZSGGFKUPZLDQN5V65QIYB",
	)
	t.Setenv(
		"VELA_NATS_OUTBOX_ACCOUNT_SIGNER_PUBLIC_KEYS",
		"AB46MZ7D6VS7MGXZLQYRYSBZB63GEI2CIKAZSGGFKUPZLDQN5V65QIYB",
	)
	t.Setenv(
		"VELA_NATS_OUTBOX_USER_PUBLIC_KEYS",
		"UD6QZ5NLFZEZLTEQDDBY5RKG6YCEY7QUET2HJHJ3MSQB5JEIOYXRUHDS",
	)
	t.Setenv("VELA_ARTIFACT_S3_ENDPOINT", "https://s3.example")
	t.Setenv("VELA_ARTIFACT_S3_REGION", "us-east-1")
	t.Setenv("VELA_ARTIFACT_S3_BUCKET", "vela-artifacts")
	t.Setenv("VELA_ARTIFACT_S3_ACCESS_KEY_ID_FILE", "/run/secrets/s3-access-key-id")
	t.Setenv("VELA_ARTIFACT_S3_SECRET_ACCESS_KEY_FILE", "/run/secrets/s3-secret-access-key")
	t.Setenv("VELA_ARTIFACT_S3_PATH_STYLE", "false")
	t.Setenv("VELA_LEASE_ACTIVE_KEY_ID", "lease-key-v2")
	t.Setenv("VELA_LEASE_KEYRING_FILE", "/run/secrets/lease-keyring.json")
	t.Setenv("VELA_ARTIFACT_VALIDATOR_HELPER_PATH", "/usr/local/bin/vela-artifact-validator")
	t.Setenv("VELA_ARTIFACT_FFPROBE_PATH", "/usr/local/libexec/ffprobe-static")
	t.Setenv("VELA_ARTIFACT_SANDBOX_ROOT", "/var/lib/vela/sandbox")
	t.Setenv("VELA_ARTIFACT_SPOOL_DIRECTORY", "/var/lib/vela/spool")
	t.Setenv("VELA_ARTIFACT_FFPROBE_VERSION", "8.0.1")
	t.Setenv("VELA_ARTIFACT_VALIDATOR_REVISION", "ffprobe-8.0.1-sandbox-v1")
	t.Setenv(
		"VELA_ARTIFACT_RECONCILER_ID",
		"spiffe://vela.internal/reconciler/artifact-finalization",
	)
	t.Setenv("VELA_WORKER_GRPC_TLS_CERT_FILE", "/run/tls/worker-control/tls.crt")
	t.Setenv("VELA_WORKER_GRPC_TLS_KEY_FILE", "/run/tls/worker-control/tls.key")
	t.Setenv("VELA_WORKER_GRPC_CLIENT_CA_FILE", "/run/tls/worker-control/client-ca.crt")
	t.Setenv(
		"VELA_CREDENTIAL_PEPPER_BASE64",
		base64.StdEncoding.EncodeToString(make([]byte, 32)),
	)
	t.Setenv("VELA_NATS_CLIENT_CERT_FILE", "")
	t.Setenv("VELA_NATS_CLIENT_KEY_FILE", "")
	t.Setenv("VELA_OUTBOX_BATCH_SIZE", "")
	t.Setenv("VELA_REMEDIATION_TICK", "")
	t.Setenv("VELA_REMEDIATION_BATCH_SIZE", "")
	t.Setenv("VELA_SCHEDULER_TICK", "")
	t.Setenv("VELA_SCHEDULER_CLAIM_TTL", "")
	t.Setenv("VELA_SCHEDULER_CANDIDATE_ATTEMPTS", "")
	t.Setenv("VELA_INVOICE_EXPORT_TICK", "")
	t.Setenv("VELA_INVOICE_EXPORT_CLAIM_TTL", "")
	t.Setenv("VELA_INVOICE_EXPORT_RETRY_DELAY", "")
	t.Setenv("VELA_INVOICE_EXPORT_BATCH_SIZE", "")
	t.Setenv("VELA_INVOICE_EXPORT_HTTP_TIMEOUT", "")
	t.Setenv("VELA_WEBHOOK_TICK", "")
	t.Setenv("VELA_WEBHOOK_CLAIM_TTL", "")
	t.Setenv("VELA_WEBHOOK_BATCH_SIZE", "")
	t.Setenv("VELA_WEBHOOK_HTTP_TIMEOUT", "")
}

func TestLoadConfigPreservesFinanceReconciliationBoundary(t *testing.T) {
	setValidConfigEnvironment(t)
	configuration, err := loadConfig()
	if err != nil {
		t.Fatalf("load Finance Reconciliation configuration: %v", err)
	}
	if configuration.financeReconciliationDatabaseURL !=
		"postgres://finance-reconciliation.example/vela" ||
		configuration.financeReconciliationAddress != "127.0.0.1:9444" ||
		configuration.financeReconciliationTLSCertFile !=
			"/run/tls/finance-reconciliation/tls.crt" ||
		configuration.financeReconciliationTLSKeyFile !=
			"/run/tls/finance-reconciliation/tls.key" ||
		configuration.financeReconciliationClientCAFile !=
			"/run/tls/finance-reconciliation/client-ca.crt" {
		t.Fatalf("Finance Reconciliation configuration = %#v", configuration)
	}

	for _, address := range []string{"", ":9444", "127.0.0.1", "127.0.0.1:0", "0.0.0.0:9444", "[::]:9444"} {
		t.Run("address="+address, func(t *testing.T) {
			setValidConfigEnvironment(t)
			t.Setenv("VELA_FINANCE_RECONCILIATION_ADDR", address)
			_, err := loadConfig()
			if err == nil || !strings.Contains(err.Error(), "VELA_FINANCE_RECONCILIATION_ADDR") {
				t.Fatalf("Finance Reconciliation address %q error = %v", address, err)
			}
		})
	}
}

func TestLoadConfigPreservesExplicitHumanOIDCConfiguration(t *testing.T) {
	setValidConfigEnvironment(t)
	configuration, err := loadConfig()
	if err != nil {
		t.Fatalf("load Human OIDC configuration: %v", err)
	}
	if configuration.oidcIssuer != "https://identity.example.com" ||
		configuration.oidcAudience != "vela-control" ||
		configuration.oidcJWKSURL != "https://identity.example.com/.well-known/jwks.json" {
		t.Fatalf(
			"Human OIDC configuration = issuer %q audience %q JWKS %q",
			configuration.oidcIssuer,
			configuration.oidcAudience,
			configuration.oidcJWKSURL,
		)
	}
}

func TestLoadConfigPreservesIndependentPlatformOIDCConfiguration(t *testing.T) {
	setValidConfigEnvironment(t)
	configuration, err := loadConfig()
	if err != nil {
		t.Fatalf("load Platform Operator OIDC configuration: %v", err)
	}
	if configuration.platformOIDCIssuer != "https://platform-identity.example.com" ||
		configuration.platformOIDCAudience != "vela-platform-control" ||
		configuration.platformOIDCJWKSURL != "https://platform-identity.example.com/.well-known/jwks.json" {
		t.Fatalf(
			"Platform Operator OIDC configuration = issuer %q audience %q JWKS %q",
			configuration.platformOIDCIssuer,
			configuration.platformOIDCAudience,
			configuration.platformOIDCJWKSURL,
		)
	}
}

func TestRunRejectsInsecureHumanOIDCConfigurationBeforeDatabaseStartup(t *testing.T) {
	setValidConfigEnvironment(t)
	t.Setenv("VELA_OIDC_ISSUER", "http://identity.example.com")

	err := run()
	if err == nil || !strings.Contains(err.Error(), "configure Human OIDC verifier") ||
		!strings.Contains(err.Error(), "absolute HTTPS URL") {
		t.Fatalf("run with insecure Human OIDC issuer error = %v", err)
	}
	if strings.Contains(err.Error(), "database") {
		t.Fatalf("insecure Human OIDC configuration reached database startup: %v", err)
	}
}

func TestRunRejectsSharedCustomerAndPlatformOIDCTrustDomainBeforeDatabaseStartup(t *testing.T) {
	for _, test := range []struct {
		name     string
		variable string
		value    string
	}{
		{
			name:     "issuer",
			variable: "VELA_PLATFORM_OIDC_ISSUER",
			value:    "https://identity.example.com",
		},
		{
			name:     "audience",
			variable: "VELA_PLATFORM_OIDC_AUDIENCE",
			value:    "vela-control",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			setValidConfigEnvironment(t)
			t.Setenv(test.variable, test.value)

			err := run()
			if err == nil || !strings.Contains(
				err.Error(),
				"OIDC issuers and audiences for Platform Operator and Customer Human trust domains must each differ",
			) {
				t.Fatalf("run with shared OIDC %s error = %v", test.name, err)
			}
			if strings.Contains(err.Error(), "database") {
				t.Fatalf("shared OIDC %s reached database startup: %v", test.name, err)
			}
		})
	}
}

func TestRunRejectsInsecurePlatformOIDCConfigurationBeforeDatabaseStartup(t *testing.T) {
	setValidConfigEnvironment(t)
	t.Setenv("VELA_PLATFORM_OIDC_JWKS_URL", "http://platform-identity.example.com/jwks.json")

	err := run()
	if err == nil || !strings.Contains(err.Error(), "configure Platform Operator OIDC verifier") ||
		!strings.Contains(err.Error(), "absolute HTTPS URL") {
		t.Fatalf("run with insecure Platform Operator OIDC JWKS URL error = %v", err)
	}
	if strings.Contains(err.Error(), "database") {
		t.Fatalf("insecure Platform Operator OIDC configuration reached database startup: %v", err)
	}
}

func TestLoadConfigParsesBoundedWebhookControls(t *testing.T) {
	setValidConfigEnvironment(t)
	configuration, err := loadConfig()
	if err != nil {
		t.Fatalf("load default webhook config: %v", err)
	}
	if configuration.webhookTick != 500*time.Millisecond ||
		configuration.webhookClaimTTL != 30*time.Second ||
		configuration.webhookBatchSize != 100 ||
		configuration.webhookHTTPTimeout != 15*time.Second {
		t.Fatalf("default webhook controls = %#v", configuration)
	}

	t.Setenv("VELA_WEBHOOK_TICK", "125ms")
	t.Setenv("VELA_WEBHOOK_CLAIM_TTL", "45s")
	t.Setenv("VELA_WEBHOOK_BATCH_SIZE", "25")
	t.Setenv("VELA_WEBHOOK_HTTP_TIMEOUT", "20s")
	configuration, err = loadConfig()
	if err != nil {
		t.Fatalf("load explicit webhook config: %v", err)
	}
	if configuration.webhookTick != 125*time.Millisecond ||
		configuration.webhookClaimTTL != 45*time.Second ||
		configuration.webhookBatchSize != 25 ||
		configuration.webhookHTTPTimeout != 20*time.Second {
		t.Fatalf("explicit webhook controls = %#v", configuration)
	}

	for _, test := range []struct {
		env   string
		value string
	}{
		{env: "VELA_WEBHOOK_TICK", value: "0s"},
		{env: "VELA_WEBHOOK_TICK", value: "1m1s"},
		{env: "VELA_WEBHOOK_CLAIM_TTL", value: "0s"},
		{env: "VELA_WEBHOOK_CLAIM_TTL", value: "1h1s"},
		{env: "VELA_WEBHOOK_BATCH_SIZE", value: "0"},
		{env: "VELA_WEBHOOK_BATCH_SIZE", value: "1001"},
		{env: "VELA_WEBHOOK_HTTP_TIMEOUT", value: "0s"},
		{env: "VELA_WEBHOOK_HTTP_TIMEOUT", value: "1m1s"},
	} {
		t.Run(test.env+"="+test.value, func(t *testing.T) {
			setValidConfigEnvironment(t)
			t.Setenv(test.env, test.value)
			if _, loadErr := loadConfig(); loadErr == nil || !strings.Contains(loadErr.Error(), test.env) {
				t.Fatalf("loadConfig error = %v, want bounded %s rejection", loadErr, test.env)
			}
		})
	}
}

func TestLoadConfigParsesBoundedRetentionControls(t *testing.T) {
	setValidConfigEnvironment(t)
	configuration, err := loadConfig()
	if err != nil {
		t.Fatalf("load default retention config: %v", err)
	}
	if configuration.retentionTick != time.Minute ||
		configuration.retentionClaimTTL != time.Minute ||
		configuration.retentionRetryDelay != 5*time.Minute ||
		configuration.retentionBatchSize != 100 {
		t.Fatalf("default retention controls = %#v", configuration)
	}

	t.Setenv("VELA_RETENTION_TICK", "15s")
	t.Setenv("VELA_RETENTION_CLAIM_TTL", "45s")
	t.Setenv("VELA_RETENTION_RETRY_DELAY", "10m")
	t.Setenv("VELA_RETENTION_BATCH_SIZE", "25")
	configuration, err = loadConfig()
	if err != nil {
		t.Fatalf("load explicit retention config: %v", err)
	}
	if configuration.retentionTick != 15*time.Second ||
		configuration.retentionClaimTTL != 45*time.Second ||
		configuration.retentionRetryDelay != 10*time.Minute ||
		configuration.retentionBatchSize != 25 {
		t.Fatalf("explicit retention controls = %#v", configuration)
	}

	for _, test := range []struct {
		env   string
		value string
	}{
		{env: "VELA_RETENTION_TICK", value: "0s"},
		{env: "VELA_RETENTION_TICK", value: "1m1s"},
		{env: "VELA_RETENTION_CLAIM_TTL", value: "0s"},
		{env: "VELA_RETENTION_CLAIM_TTL", value: "1500ms"},
		{env: "VELA_RETENTION_CLAIM_TTL", value: "1h1s"},
		{env: "VELA_RETENTION_RETRY_DELAY", value: "0s"},
		{env: "VELA_RETENTION_RETRY_DELAY", value: "1500ms"},
		{env: "VELA_RETENTION_RETRY_DELAY", value: "24h1s"},
		{env: "VELA_RETENTION_BATCH_SIZE", value: "0"},
		{env: "VELA_RETENTION_BATCH_SIZE", value: "1001"},
	} {
		t.Run(test.env+"="+test.value, func(t *testing.T) {
			setValidConfigEnvironment(t)
			t.Setenv(test.env, test.value)
			if _, loadErr := loadConfig(); loadErr == nil || !strings.Contains(loadErr.Error(), test.env) {
				t.Fatalf("loadConfig error = %v, want bounded %s rejection", loadErr, test.env)
			}
		})
	}
}

func TestReadWebhookKeyringRequiresExactAES256Keys(t *testing.T) {
	path := filepath.Join(t.TempDir(), "webhook-keyring.json")
	strongKey := base64.StdEncoding.EncodeToString([]byte("0123456789abcdef0123456789abcdef"))
	if err := os.WriteFile(path, []byte(`{"webhook-key-v1":"`+strongKey+`"}`), 0o600); err != nil {
		t.Fatalf("write webhook keyring: %v", err)
	}
	keyring, err := readWebhookKeyring(path)
	if err != nil {
		t.Fatalf("readWebhookKeyring: %v", err)
	}
	if string(keyring["webhook-key-v1"]) != "0123456789abcdef0123456789abcdef" {
		t.Fatalf("decoded webhook keyring = %#v", keyring)
	}

	for _, document := range []string{
		`{"webhook-key-v1":"` + base64.StdEncoding.EncodeToString([]byte("too-short")) + `"}`,
		`{"webhook-key-v1":"` + base64.StdEncoding.EncodeToString(make([]byte, 33)) + `"}`,
		`{"webhook-key-v1":"` + strongKey + `"} {}`,
		`{"":"` + strongKey + `"}`,
	} {
		if err := os.WriteFile(path, []byte(document), 0o600); err != nil {
			t.Fatalf("write invalid webhook keyring: %v", err)
		}
		if _, err := readWebhookKeyring(path); err == nil {
			t.Fatalf("readWebhookKeyring accepted %q", document)
		}
	}
}

func TestLoadConfigParsesBoundedInvoiceExportControls(t *testing.T) {
	setValidConfigEnvironment(t)
	configuration, err := loadConfig()
	if err != nil {
		t.Fatalf("load default Invoice export config: %v", err)
	}
	if configuration.invoiceExportTick != 500*time.Millisecond ||
		configuration.invoiceExportClaimTTL != 30*time.Second ||
		configuration.invoiceExportRetryDelay != 5*time.Second ||
		configuration.invoiceExportBatchSize != 100 ||
		configuration.invoiceExportHTTPTimeout != 15*time.Second {
		t.Fatalf(
			"default Invoice controls = tick %s claim %s retry %s batch %d HTTP %s",
			configuration.invoiceExportTick,
			configuration.invoiceExportClaimTTL,
			configuration.invoiceExportRetryDelay,
			configuration.invoiceExportBatchSize,
			configuration.invoiceExportHTTPTimeout,
		)
	}

	t.Setenv("VELA_INVOICE_EXPORT_TICK", "250ms")
	t.Setenv("VELA_INVOICE_EXPORT_CLAIM_TTL", "45s")
	t.Setenv("VELA_INVOICE_EXPORT_RETRY_DELAY", "10s")
	t.Setenv("VELA_INVOICE_EXPORT_BATCH_SIZE", "25")
	t.Setenv("VELA_INVOICE_EXPORT_HTTP_TIMEOUT", "20s")
	configuration, err = loadConfig()
	if err != nil {
		t.Fatalf("load explicit Invoice export config: %v", err)
	}
	if configuration.invoiceExportTick != 250*time.Millisecond ||
		configuration.invoiceExportClaimTTL != 45*time.Second ||
		configuration.invoiceExportRetryDelay != 10*time.Second ||
		configuration.invoiceExportBatchSize != 25 ||
		configuration.invoiceExportHTTPTimeout != 20*time.Second {
		t.Fatalf("explicit Invoice controls = %#v", configuration)
	}

	for _, test := range []struct {
		env   string
		value string
	}{
		{env: "VELA_INVOICE_EXPORT_TICK", value: "0s"},
		{env: "VELA_INVOICE_EXPORT_TICK", value: "1m1s"},
		{env: "VELA_INVOICE_EXPORT_CLAIM_TTL", value: "0s"},
		{env: "VELA_INVOICE_EXPORT_CLAIM_TTL", value: "5m1s"},
		{env: "VELA_INVOICE_EXPORT_RETRY_DELAY", value: "-1s"},
		{env: "VELA_INVOICE_EXPORT_RETRY_DELAY", value: "1h1s"},
		{env: "VELA_INVOICE_EXPORT_BATCH_SIZE", value: "0"},
		{env: "VELA_INVOICE_EXPORT_BATCH_SIZE", value: "1001"},
		{env: "VELA_INVOICE_EXPORT_HTTP_TIMEOUT", value: "0s"},
		{env: "VELA_INVOICE_EXPORT_HTTP_TIMEOUT", value: "1m1s"},
	} {
		t.Run(test.env+"="+test.value, func(t *testing.T) {
			setValidConfigEnvironment(t)
			t.Setenv(test.env, test.value)
			if _, loadErr := loadConfig(); loadErr == nil || !strings.Contains(loadErr.Error(), test.env) {
				t.Fatalf("loadConfig error = %v, want bounded %s rejection", loadErr, test.env)
			}
		})
	}
}

func TestLoadConfigParsesBoundedSchedulerControls(t *testing.T) {
	setValidConfigEnvironment(t)
	configuration, err := loadConfig()
	if err != nil {
		t.Fatalf("load default Scheduler config: %v", err)
	}
	if configuration.schedulerTick != 500*time.Millisecond ||
		configuration.schedulerClaimTTL != 30*time.Second ||
		configuration.schedulerCandidateAttempts != 5 {
		t.Fatalf(
			"default Scheduler controls = tick %s claim TTL %s attempts %d",
			configuration.schedulerTick,
			configuration.schedulerClaimTTL,
			configuration.schedulerCandidateAttempts,
		)
	}

	t.Setenv("VELA_SCHEDULER_TICK", "125ms")
	t.Setenv("VELA_SCHEDULER_CLAIM_TTL", "45s")
	t.Setenv("VELA_SCHEDULER_CANDIDATE_ATTEMPTS", "7")
	configuration, err = loadConfig()
	if err != nil {
		t.Fatalf("load explicit Scheduler config: %v", err)
	}
	if configuration.schedulerTick != 125*time.Millisecond ||
		configuration.schedulerClaimTTL != 45*time.Second ||
		configuration.schedulerCandidateAttempts != 7 {
		t.Fatalf(
			"explicit Scheduler controls = tick %s claim TTL %s attempts %d",
			configuration.schedulerTick,
			configuration.schedulerClaimTTL,
			configuration.schedulerCandidateAttempts,
		)
	}

	for _, test := range []struct {
		name  string
		env   string
		value string
	}{
		{name: "zero tick", env: "VELA_SCHEDULER_TICK", value: "0s"},
		{name: "oversized tick", env: "VELA_SCHEDULER_TICK", value: "1m1s"},
		{name: "invalid claim TTL", env: "VELA_SCHEDULER_CLAIM_TTL", value: "invalid"},
		{name: "oversized claim TTL", env: "VELA_SCHEDULER_CLAIM_TTL", value: "5m1s"},
		{name: "zero attempts", env: "VELA_SCHEDULER_CANDIDATE_ATTEMPTS", value: "0"},
		{name: "too many attempts", env: "VELA_SCHEDULER_CANDIDATE_ATTEMPTS", value: "21"},
	} {
		t.Run(test.name, func(t *testing.T) {
			setValidConfigEnvironment(t)
			t.Setenv(test.env, test.value)
			if _, loadErr := loadConfig(); loadErr == nil || !strings.Contains(loadErr.Error(), test.env) {
				t.Fatalf("loadConfig error = %v, want bounded %s rejection", loadErr, test.env)
			}
		})
	}
}

func TestReadLeaseKeyringRequiresOneStrictStrongKeyMap(t *testing.T) {
	path := filepath.Join(t.TempDir(), "lease-keyring.json")
	strongKey := base64.StdEncoding.EncodeToString([]byte("0123456789abcdef0123456789abcdef"))
	if err := os.WriteFile(
		path,
		[]byte(`{"lease-key-v2":"`+strongKey+`"}`),
		0o600,
	); err != nil {
		t.Fatalf("write Lease keyring: %v", err)
	}
	keyring, err := readLeaseKeyring(path)
	if err != nil {
		t.Fatalf("readLeaseKeyring: %v", err)
	}
	if string(keyring["lease-key-v2"]) != "0123456789abcdef0123456789abcdef" {
		t.Fatalf("decoded Lease keyring = %#v", keyring)
	}

	for _, document := range []string{
		`{"lease-key-v2":"` + base64.StdEncoding.EncodeToString([]byte("too-short")) + `"}`,
		`{"lease-key-v2":"` + strongKey + `"} {}`,
		`{"":"` + strongKey + `"}`,
	} {
		if err := os.WriteFile(path, []byte(document), 0o600); err != nil {
			t.Fatalf("write invalid Lease keyring: %v", err)
		}
		if _, err := readLeaseKeyring(path); err == nil {
			t.Fatalf("readLeaseKeyring accepted %q", document)
		}
	}
}

func TestCancellationStopReconcilerRetriesAndStopsWithContext(t *testing.T) {
	reconciler := &testCancellationStopReconciler{calls: make(chan struct{}, 2)}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		runCancellationStopReconciler(ctx, reconciler, time.Millisecond)
	}()

	for range 2 {
		select {
		case <-reconciler.calls:
		case <-time.After(time.Second):
			t.Fatal("cancellation stop reconciler did not retry")
		}
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("cancellation stop reconciler did not stop with context")
	}
}

func TestSchedulerRetriesTransientFailureAndStopsWithContext(t *testing.T) {
	scheduling := &testHierarchicalScheduler{
		calls:      make(chan struct{}, 2),
		reconciles: make(chan struct{}, 2),
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		runScheduler(ctx, scheduling, time.Millisecond)
	}()

	for range 2 {
		select {
		case <-scheduling.reconciles:
		case <-time.After(time.Second):
			t.Fatal("Scheduler did not reconcile expired claims")
		}
		select {
		case <-scheduling.calls:
		case <-time.After(time.Second):
			t.Fatal("Scheduler did not retry")
		}
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Scheduler did not stop with context")
	}
}

func TestInvoiceExporterRetriesTransientFailureAndStopsWithContext(t *testing.T) {
	exporter := &testInvoiceExporter{calls: make(chan struct{}, 2)}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		runInvoiceExporter(ctx, exporter, time.Millisecond)
	}()

	for range 2 {
		select {
		case <-exporter.calls:
		case <-time.After(time.Second):
			t.Fatal("Invoice exporter did not retry")
		}
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Invoice exporter did not stop with context")
	}
}

func TestWebhookDispatcherRetriesTransientFailureAndStopsWithContext(t *testing.T) {
	dispatcher := &testWebhookDispatcher{calls: make(chan struct{}, 2)}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		runWebhookDispatcher(ctx, dispatcher, time.Millisecond)
	}()

	for range 2 {
		select {
		case <-dispatcher.calls:
		case <-time.After(time.Second):
			t.Fatal("webhook Dispatcher did not retry")
		}
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("webhook Dispatcher did not stop with context")
	}
}

func TestRetentionReconcilerRetriesTransientFailureAndStopsWithContext(t *testing.T) {
	reconciler := &testRetentionReconciler{calls: make(chan struct{}, 2)}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		runRetentionReconciler(ctx, reconciler, time.Millisecond)
	}()

	for range 2 {
		select {
		case <-reconciler.calls:
		case <-time.After(time.Second):
			t.Fatal("retention Reconciler did not retry")
		}
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("retention Reconciler did not stop with context")
	}
}

type testHierarchicalScheduler struct {
	invocations atomic.Int32
	calls       chan struct{}
	reconciles  chan struct{}
}

type testInvoiceExporter struct {
	invocations atomic.Int32
	calls       chan struct{}
}

type testWebhookDispatcher struct {
	invocations atomic.Int32
	calls       chan struct{}
}

type testRetentionReconciler struct {
	invocations atomic.Int32
	calls       chan struct{}
}

func (r *testRetentionReconciler) ReconcileBatch(context.Context) (retention.ReconcileResult, error) {
	invocation := r.invocations.Add(1)
	r.calls <- struct{}{}
	if invocation == 1 {
		return retention.ReconcileResult{Claimed: 1, Failed: 1}, errors.New("transient retention failure")
	}
	return retention.ReconcileResult{}, nil
}

func (d *testWebhookDispatcher) DispatchBatch(context.Context) (webhook.BatchResult, error) {
	invocation := d.invocations.Add(1)
	d.calls <- struct{}{}
	if invocation == 1 {
		return webhook.BatchResult{Claimed: 1, Failed: 1}, errors.New("transient webhook failure")
	}
	return webhook.BatchResult{}, nil
}

func (e *testInvoiceExporter) ExportBatch(context.Context) (billingexport.BatchResult, error) {
	invocation := e.invocations.Add(1)
	e.calls <- struct{}{}
	if invocation == 1 {
		return billingexport.BatchResult{Claimed: 1}, errors.New("transient Invoice failure")
	}
	return billingexport.BatchResult{}, nil
}

func (s *testHierarchicalScheduler) ReconcileExpired(context.Context) (int64, error) {
	s.reconciles <- struct{}{}
	return 1, nil
}

func (s *testHierarchicalScheduler) RunCycle(context.Context) ([]scheduler.Dispatch, error) {
	invocation := s.invocations.Add(1)
	s.calls <- struct{}{}
	if invocation == 1 {
		return nil, errors.New("transient Scheduler failure")
	}
	return nil, nil
}

type testCancellationStopReconciler struct {
	invocations atomic.Int32
	calls       chan struct{}
}

func (r *testCancellationStopReconciler) ReconcileNextCancellationStop(
	context.Context,
) (cancellation.StopResult, error) {
	invocation := r.invocations.Add(1)
	r.calls <- struct{}{}
	if invocation == 1 {
		return cancellation.StopResult{}, errors.New("transient reconciliation failure")
	}
	return cancellation.StopResult{Decision: cancellation.StopNoWork}, nil
}
