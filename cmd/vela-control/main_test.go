package main

import (
	"context"
	"encoding/base64"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/vivym/vela/internal/artifactreplication"
	"github.com/vivym/vela/internal/billingexport"
	"github.com/vivym/vela/internal/cancellation"
	"github.com/vivym/vela/internal/noncontentexpiry"
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
		{name: "Scheduler workload credentials", missingEnv: "VELA_NATS_SCHEDULER_CREDENTIALS_FILE"},
		{name: "Scheduler workload users", missingEnv: "VELA_NATS_SCHEDULER_USER_PUBLIC_KEYS"},
		{name: "Human auth database", missingEnv: "VELA_HUMAN_AUTH_DATABASE_URL"},
		{name: "Human membership auth database", missingEnv: "VELA_HUMAN_MEMBERSHIP_AUTH_DATABASE_URL"},
		{name: "identity request database", missingEnv: "VELA_IDENTITY_REQUEST_DATABASE_URL"},
		{name: "Human membership request database", missingEnv: "VELA_HUMAN_MEMBERSHIP_REQUEST_DATABASE_URL"},
		{name: "Organization billing request database", missingEnv: "VELA_ORGANIZATION_BILLING_REQUEST_DATABASE_URL"},
		{name: "Organization audit request database", missingEnv: "VELA_ORGANIZATION_AUDIT_REQUEST_DATABASE_URL"},
		{name: "retention request database", missingEnv: "VELA_RETENTION_REQUEST_DATABASE_URL"},
		{name: "debug dump request database", missingEnv: "VELA_DEBUG_DUMP_REQUEST_DATABASE_URL"},
		{name: "debug dump audit request database", missingEnv: "VELA_DEBUG_DUMP_AUDIT_REQUEST_DATABASE_URL"},
		{name: "retention database", missingEnv: "VELA_RETENTION_DATABASE_URL"},
		{name: "non-content expiry database", missingEnv: "VELA_NON_CONTENT_EXPIRY_DATABASE_URL"},
		{name: "off-cluster backup retention database", missingEnv: "VELA_BACKUP_RETENTION_DATABASE_URL"},
		{name: "Artifact replication database", missingEnv: "VELA_ARTIFACT_REPLICATION_DATABASE_URL"},
		{name: "Platform Operator auth database", missingEnv: "VELA_PLATFORM_OPERATOR_AUTH_DATABASE_URL"},
		{name: "Break-glass request database", missingEnv: "VELA_BREAK_GLASS_REQUEST_DATABASE_URL"},
		{name: "Break-glass audit database", missingEnv: "VELA_BREAK_GLASS_AUDIT_DATABASE_URL"},
		{name: "retention Reconciler identity", missingEnv: "VELA_RETENTION_RECONCILER_ID"},
		{name: "non-content expiry Reconciler identity", missingEnv: "VELA_NON_CONTENT_EXPIRY_RECONCILER_ID"},
		{name: "Artifact Replicator identity", missingEnv: "VELA_ARTIFACT_REPLICATION_ID"},
		{name: "Artifact request database", missingEnv: "VELA_ARTIFACT_REQUEST_DATABASE_URL"},
		{name: "OIDC issuer", missingEnv: "VELA_OIDC_ISSUER"},
		{name: "OIDC audience", missingEnv: "VELA_OIDC_AUDIENCE"},
		{name: "OIDC JWKS URL", missingEnv: "VELA_OIDC_JWKS_URL"},
		{name: "Platform OIDC issuer", missingEnv: "VELA_PLATFORM_OIDC_ISSUER"},
		{name: "Platform OIDC audience", missingEnv: "VELA_PLATFORM_OIDC_AUDIENCE"},
		{name: "Platform OIDC JWKS URL", missingEnv: "VELA_PLATFORM_OIDC_JWKS_URL"},
		{name: "Scheduler database", missingEnv: "VELA_SCHEDULER_DATABASE_URL"},
		{name: "Scheduler Inbox database", missingEnv: "VELA_SCHEDULER_INBOX_DATABASE_URL"},
		{name: "Scheduler identity", missingEnv: "VELA_SCHEDULER_ID"},
		{name: "billing database", missingEnv: "VELA_BILLING_DATABASE_URL"},
		{name: "Finance Reconciliation database", missingEnv: "VELA_FINANCE_RECONCILIATION_DATABASE_URL"},
		{name: "Finance Reconciliation address", missingEnv: "VELA_FINANCE_RECONCILIATION_ADDR"},
		{name: "Finance Reconciliation server certificate", missingEnv: "VELA_FINANCE_RECONCILIATION_SERVER_CERT_FILE"},
		{name: "Finance Reconciliation server key", missingEnv: "VELA_FINANCE_RECONCILIATION_SERVER_KEY_FILE"},
		{name: "Finance Reconciliation client CA", missingEnv: "VELA_FINANCE_RECONCILIATION_CLIENT_CA_FILE"},
		{name: "Compliance database", missingEnv: "VELA_COMPLIANCE_DATABASE_URL"},
		{name: "Compliance address", missingEnv: "VELA_COMPLIANCE_ADDR"},
		{name: "Compliance server certificate", missingEnv: "VELA_COMPLIANCE_SERVER_CERT_FILE"},
		{name: "Compliance server key", missingEnv: "VELA_COMPLIANCE_SERVER_KEY_FILE"},
		{name: "Compliance client CA", missingEnv: "VELA_COMPLIANCE_CLIENT_CA_FILE"},
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
		{name: "Artifact backup S3 endpoint", missingEnv: "VELA_ARTIFACT_BACKUP_S3_ENDPOINT"},
		{name: "Artifact backup S3 region", missingEnv: "VELA_ARTIFACT_BACKUP_S3_REGION"},
		{name: "Artifact backup S3 bucket", missingEnv: "VELA_ARTIFACT_BACKUP_S3_BUCKET"},
		{name: "Artifact backup S3 access key", missingEnv: "VELA_ARTIFACT_BACKUP_S3_ACCESS_KEY_ID_FILE"},
		{name: "Artifact backup S3 secret key", missingEnv: "VELA_ARTIFACT_BACKUP_S3_SECRET_ACCESS_KEY_FILE"},
		{name: "Artifact replication source S3 access key", missingEnv: "VELA_ARTIFACT_REPLICATION_SOURCE_S3_ACCESS_KEY_ID_FILE"},
		{name: "Artifact replication source S3 secret key", missingEnv: "VELA_ARTIFACT_REPLICATION_SOURCE_S3_SECRET_ACCESS_KEY_FILE"},
		{name: "Artifact replication backup S3 access key", missingEnv: "VELA_ARTIFACT_REPLICATION_BACKUP_S3_ACCESS_KEY_ID_FILE"},
		{name: "Artifact replication backup S3 secret key", missingEnv: "VELA_ARTIFACT_REPLICATION_BACKUP_S3_SECRET_ACCESS_KEY_FILE"},
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
		{name: "Fleet database", missingEnv: "VELA_FLEET_DATABASE_URL"},
		{name: "Fleet gRPC server certificate", missingEnv: "VELA_FLEET_GRPC_TLS_CERT_FILE"},
		{name: "Fleet gRPC server key", missingEnv: "VELA_FLEET_GRPC_TLS_KEY_FILE"},
		{name: "Fleet gRPC client CA", missingEnv: "VELA_FLEET_GRPC_CLIENT_CA_FILE"},
		{name: "Fleet Controller SPIFFE identity", missingEnv: "VELA_FLEET_CONTROLLER_SPIFFE_ID"},
		{name: "Fleet Controller actor identity", missingEnv: "VELA_FLEET_CONTROLLER_ACTOR_IDENTITY"},
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
	valid := `{"node-1":{"address":"127.0.0.1:9443","server_name":"node-agent.internal","worker_id":"10000000-0000-0000-0000-000000000001","worker_epoch":7,"spiffe_identity":"spiffe://vela.internal/node-agent/bm9kZS0x/10000000-0000-0000-0000-000000000001"}}`
	if err := os.WriteFile(path, []byte(valid), 0o600); err != nil {
		t.Fatalf("write endpoint registry: %v", err)
	}
	if endpoints, err := readNodeAgentEndpoints(path); err != nil || len(endpoints) != 1 || endpoints["node-1"].WorkerEpoch != 7 {
		t.Fatalf("read endpoint registry = %#v error=%v", endpoints, err)
	}
	unknown := strings.Replace(valid, `"worker_id"`, `"unknown":true,"worker_id"`, 1)
	if err := os.WriteFile(path, []byte(unknown), 0o600); err != nil {
		t.Fatalf("write endpoint registry with unknown field: %v", err)
	}
	if _, err := readNodeAgentEndpoints(path); err == nil {
		t.Fatal("unknown endpoint registry field was accepted")
	}
	duplicateRegistries := map[string]string{
		"top-level Node identity": strings.TrimSuffix(valid, "}") + "," + strings.TrimPrefix(valid, "{"),
		"nested endpoint field": strings.Replace(
			valid,
			`"worker_epoch":7`,
			`"worker_epoch":7,"worker_epoch":8`,
			1,
		),
	}
	for name, duplicate := range duplicateRegistries {
		t.Run("duplicate "+name, func(t *testing.T) {
			if err := os.WriteFile(path, []byte(duplicate), 0o600); err != nil {
				t.Fatalf("write duplicate endpoint registry: %v", err)
			}
			if _, err := readNodeAgentEndpoints(path); err == nil {
				t.Fatal("duplicate endpoint registry key was accepted")
			}
		})
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
	t.Setenv("VELA_DEBUG_DUMP_REQUEST_DATABASE_URL", "postgres://debug-dump-request.example/vela")
	t.Setenv(
		"VELA_DEBUG_DUMP_AUDIT_REQUEST_DATABASE_URL",
		"postgres://debug-dump-audit-request.example/vela",
	)
	t.Setenv("VELA_RETENTION_DATABASE_URL", "postgres://retention.example/vela")
	t.Setenv(
		"VELA_NON_CONTENT_EXPIRY_DATABASE_URL",
		"postgres://non-content-expiry.example/vela",
	)
	t.Setenv(
		"VELA_BACKUP_RETENTION_DATABASE_URL",
		"postgres://backup-retention.example/vela",
	)
	t.Setenv(
		"VELA_ARTIFACT_REPLICATION_DATABASE_URL",
		"postgres://artifact-replication.example/vela",
	)
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
	t.Setenv("VELA_NON_CONTENT_EXPIRY_RECONCILER_ID", "vela-control-non-content-expiry-1")
	t.Setenv("VELA_ARTIFACT_REPLICATION_ID", "vela-control-artifact-replicator-1")
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
	t.Setenv("VELA_SCHEDULER_INBOX_DATABASE_URL", "postgres://scheduler-inbox.example/vela")
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
	t.Setenv("VELA_COMPLIANCE_DATABASE_URL", "postgres://compliance.example/vela")
	t.Setenv("VELA_COMPLIANCE_ADDR", "127.0.0.1:9445")
	t.Setenv("VELA_COMPLIANCE_SERVER_CERT_FILE", "/run/tls/compliance/tls.crt")
	t.Setenv("VELA_COMPLIANCE_SERVER_KEY_FILE", "/run/tls/compliance/tls.key")
	t.Setenv("VELA_COMPLIANCE_CLIENT_CA_FILE", "/run/tls/compliance/client-ca.crt")
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
	t.Setenv("VELA_NATS_SCHEDULER_CREDENTIALS_FILE", "/run/secrets/vela-scheduler.creds")
	t.Setenv(
		"VELA_NATS_SCHEDULER_USER_PUBLIC_KEYS",
		"UAWF3LT7L6HYUQF4YB7QTKGMBMTNHTL23W5EPJSGQ2AEVKXOJMLX5MQR",
	)
	t.Setenv("VELA_ARTIFACT_S3_ENDPOINT", "https://s3.example")
	t.Setenv("VELA_ARTIFACT_S3_REGION", "us-east-1")
	t.Setenv("VELA_ARTIFACT_S3_BUCKET", "vela-artifacts")
	t.Setenv("VELA_ARTIFACT_S3_ACCESS_KEY_ID_FILE", "/run/secrets/s3-access-key-id")
	t.Setenv("VELA_ARTIFACT_S3_SECRET_ACCESS_KEY_FILE", "/run/secrets/s3-secret-access-key")
	t.Setenv("VELA_ARTIFACT_S3_PATH_STYLE", "false")
	t.Setenv("VELA_ARTIFACT_BACKUP_S3_ENDPOINT", "https://s3-backup.example")
	t.Setenv("VELA_ARTIFACT_BACKUP_S3_REGION", "us-east-1")
	t.Setenv("VELA_ARTIFACT_BACKUP_S3_BUCKET", "vela-artifacts-backup")
	t.Setenv(
		"VELA_ARTIFACT_BACKUP_S3_ACCESS_KEY_ID_FILE",
		"/run/secrets/backup-s3-access-key-id",
	)
	t.Setenv(
		"VELA_ARTIFACT_BACKUP_S3_SECRET_ACCESS_KEY_FILE",
		"/run/secrets/backup-s3-secret-access-key",
	)
	t.Setenv("VELA_ARTIFACT_BACKUP_S3_PATH_STYLE", "false")
	t.Setenv(
		"VELA_ARTIFACT_REPLICATION_SOURCE_S3_ACCESS_KEY_ID_FILE",
		"/run/secrets/artifact-replication-source-access-key-id",
	)
	t.Setenv(
		"VELA_ARTIFACT_REPLICATION_SOURCE_S3_SECRET_ACCESS_KEY_FILE",
		"/run/secrets/artifact-replication-source-secret-access-key",
	)
	t.Setenv(
		"VELA_ARTIFACT_REPLICATION_BACKUP_S3_ACCESS_KEY_ID_FILE",
		"/run/secrets/artifact-replication-backup-access-key-id",
	)
	t.Setenv(
		"VELA_ARTIFACT_REPLICATION_BACKUP_S3_SECRET_ACCESS_KEY_FILE",
		"/run/secrets/artifact-replication-backup-secret-access-key",
	)
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
	t.Setenv("VELA_FLEET_DATABASE_URL", "postgres://fleet.example/vela")
	t.Setenv("VELA_FLEET_GRPC_ADDRESS", "127.0.0.1:8444")
	t.Setenv("VELA_FLEET_GRPC_TLS_CERT_FILE", "/run/tls/fleet-control/tls.crt")
	t.Setenv("VELA_FLEET_GRPC_TLS_KEY_FILE", "/run/tls/fleet-control/tls.key")
	t.Setenv("VELA_FLEET_GRPC_CLIENT_CA_FILE", "/run/tls/fleet-control/client-ca.crt")
	t.Setenv("VELA_FLEET_CONTROLLER_SPIFFE_ID", "spiffe://vela.internal/fleet-controller/primary")
	t.Setenv("VELA_FLEET_CONTROLLER_ACTOR_IDENTITY", "fleet-controller/primary")
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

func TestLoadConfigPreservesIndependentFleetMaintenanceBoundary(t *testing.T) {
	setValidConfigEnvironment(t)
	configuration, err := loadConfig()
	if err != nil {
		t.Fatalf("load Fleet maintenance configuration: %v", err)
	}
	if configuration.fleetDatabaseURL != "postgres://fleet.example/vela" ||
		configuration.fleetGRPCAddress != "127.0.0.1:8444" ||
		configuration.fleetGRPCTLSCertFile != "/run/tls/fleet-control/tls.crt" ||
		configuration.fleetGRPCTLSKeyFile != "/run/tls/fleet-control/tls.key" ||
		configuration.fleetGRPCClientCAFile != "/run/tls/fleet-control/client-ca.crt" ||
		configuration.fleetControllerSPIFFEIdentity !=
			"spiffe://vela.internal/fleet-controller/primary" ||
		configuration.fleetControllerActorIdentity != "fleet-controller/primary" {
		t.Fatalf("Fleet maintenance configuration = %#v", configuration)
	}
}

func TestLoadConfigPreservesPrivateManagementBoundary(t *testing.T) {
	setValidConfigEnvironment(t)
	t.Setenv("VELA_MANAGEMENT_ADDRESS", "127.0.0.1:9081")

	configuration, err := loadConfig()
	if err != nil {
		t.Fatalf("load management configuration: %v", err)
	}
	if configuration.managementAddress != "127.0.0.1:9081" {
		t.Fatalf("management address = %q", configuration.managementAddress)
	}
}

func TestLoadConfigRejectsInvalidArtifactBackupS3PathStyle(t *testing.T) {
	setValidConfigEnvironment(t)
	t.Setenv("VELA_ARTIFACT_BACKUP_S3_PATH_STYLE", "sometimes")

	_, err := loadConfig()
	if err == nil || err.Error() !=
		"environment variable VELA_ARTIFACT_BACKUP_S3_PATH_STYLE must be true or false" {
		t.Fatalf("load invalid Artifact backup S3 path style error = %v", err)
	}
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

func TestLoadConfigPreservesIndependentComplianceBoundary(t *testing.T) {
	setValidConfigEnvironment(t)
	configuration, err := loadConfig()
	if err != nil {
		t.Fatalf("load Compliance configuration: %v", err)
	}
	if configuration.complianceDatabaseURL != "postgres://compliance.example/vela" ||
		configuration.complianceAddress != "127.0.0.1:9445" ||
		configuration.complianceTLSCertFile != "/run/tls/compliance/tls.crt" ||
		configuration.complianceTLSKeyFile != "/run/tls/compliance/tls.key" ||
		configuration.complianceClientCAFile != "/run/tls/compliance/client-ca.crt" {
		t.Fatalf("Compliance configuration = %#v", configuration)
	}

	for _, address := range []string{"", ":9445", "127.0.0.1", "127.0.0.1:0", "0.0.0.0:9445", "[::]:9445"} {
		t.Run("address="+address, func(t *testing.T) {
			setValidConfigEnvironment(t)
			t.Setenv("VELA_COMPLIANCE_ADDR", address)
			_, err := loadConfig()
			if err == nil || !strings.Contains(err.Error(), "VELA_COMPLIANCE_ADDR") {
				t.Fatalf("Compliance address %q error = %v", address, err)
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

func TestLoadConfigParsesBoundedNonContentExpiryControls(t *testing.T) {
	setValidConfigEnvironment(t)
	configuration, err := loadConfig()
	if err != nil {
		t.Fatalf("load default non-content expiry config: %v", err)
	}
	if configuration.nonContentExpiryTick != time.Minute ||
		configuration.nonContentExpiryClaimTTL != time.Minute ||
		configuration.nonContentExpiryHeldRetry != 5*time.Minute ||
		configuration.nonContentExpiryBatchSize != 100 {
		t.Fatalf("default non-content expiry controls = %#v", configuration)
	}

	t.Setenv("VELA_NON_CONTENT_EXPIRY_TICK", "15s")
	t.Setenv("VELA_NON_CONTENT_EXPIRY_CLAIM_TTL", "45s")
	t.Setenv("VELA_NON_CONTENT_EXPIRY_HELD_RETRY", "10m")
	t.Setenv("VELA_NON_CONTENT_EXPIRY_BATCH_SIZE", "25")
	configuration, err = loadConfig()
	if err != nil {
		t.Fatalf("load explicit non-content expiry config: %v", err)
	}
	if configuration.nonContentExpiryTick != 15*time.Second ||
		configuration.nonContentExpiryClaimTTL != 45*time.Second ||
		configuration.nonContentExpiryHeldRetry != 10*time.Minute ||
		configuration.nonContentExpiryBatchSize != 25 {
		t.Fatalf("explicit non-content expiry controls = %#v", configuration)
	}

	for _, test := range []struct {
		env   string
		value string
	}{
		{env: "VELA_NON_CONTENT_EXPIRY_TICK", value: "0s"},
		{env: "VELA_NON_CONTENT_EXPIRY_TICK", value: "1h1s"},
		{env: "VELA_NON_CONTENT_EXPIRY_CLAIM_TTL", value: "0s"},
		{env: "VELA_NON_CONTENT_EXPIRY_CLAIM_TTL", value: "1500ms"},
		{env: "VELA_NON_CONTENT_EXPIRY_CLAIM_TTL", value: "1h1s"},
		{env: "VELA_NON_CONTENT_EXPIRY_HELD_RETRY", value: "0s"},
		{env: "VELA_NON_CONTENT_EXPIRY_HELD_RETRY", value: "1500ms"},
		{env: "VELA_NON_CONTENT_EXPIRY_HELD_RETRY", value: "24h1s"},
		{env: "VELA_NON_CONTENT_EXPIRY_BATCH_SIZE", value: "0"},
		{env: "VELA_NON_CONTENT_EXPIRY_BATCH_SIZE", value: "1001"},
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

func TestLoadConfigParsesBoundedArtifactReplicationControls(t *testing.T) {
	setValidConfigEnvironment(t)
	configuration, err := loadConfig()
	if err != nil {
		t.Fatalf("load default Artifact replication config: %v", err)
	}
	if configuration.artifactReplicationTick != time.Minute ||
		configuration.artifactReplicationClaimTTL != 20*time.Minute ||
		configuration.artifactReplicationRetryDelay != 5*time.Minute ||
		configuration.artifactReplicationTimeout != 15*time.Minute ||
		configuration.artifactReplicationBatchSize != 10 {
		t.Fatalf("default Artifact replication controls = %#v", configuration)
	}

	t.Setenv("VELA_ARTIFACT_REPLICATION_TICK", "10s")
	t.Setenv("VELA_ARTIFACT_REPLICATION_CLAIM_TTL", "30m")
	t.Setenv("VELA_ARTIFACT_REPLICATION_RETRY_DELAY", "1m")
	t.Setenv("VELA_ARTIFACT_REPLICATION_TIMEOUT", "20m")
	t.Setenv("VELA_ARTIFACT_REPLICATION_BATCH_SIZE", "25")
	configuration, err = loadConfig()
	if err != nil {
		t.Fatalf("load explicit Artifact replication config: %v", err)
	}
	if configuration.artifactReplicationTick != 10*time.Second ||
		configuration.artifactReplicationClaimTTL != 30*time.Minute ||
		configuration.artifactReplicationRetryDelay != time.Minute ||
		configuration.artifactReplicationTimeout != 20*time.Minute ||
		configuration.artifactReplicationBatchSize != 25 {
		t.Fatalf("explicit Artifact replication controls = %#v", configuration)
	}

	for _, test := range []struct {
		env   string
		value string
	}{
		{env: "VELA_ARTIFACT_REPLICATION_TICK", value: "0s"},
		{env: "VELA_ARTIFACT_REPLICATION_TICK", value: "1m1s"},
		{env: "VELA_ARTIFACT_REPLICATION_CLAIM_TTL", value: "1500ms"},
		{env: "VELA_ARTIFACT_REPLICATION_CLAIM_TTL", value: "1h1s"},
		{env: "VELA_ARTIFACT_REPLICATION_RETRY_DELAY", value: "0s"},
		{env: "VELA_ARTIFACT_REPLICATION_RETRY_DELAY", value: "24h1s"},
		{env: "VELA_ARTIFACT_REPLICATION_TIMEOUT", value: "1500ms"},
		{env: "VELA_ARTIFACT_REPLICATION_TIMEOUT", value: "1h1s"},
		{env: "VELA_ARTIFACT_REPLICATION_BATCH_SIZE", value: "0"},
		{env: "VELA_ARTIFACT_REPLICATION_BATCH_SIZE", value: "1001"},
	} {
		t.Run(test.env+"="+test.value, func(t *testing.T) {
			setValidConfigEnvironment(t)
			t.Setenv(test.env, test.value)
			if _, loadErr := loadConfig(); loadErr == nil ||
				!strings.Contains(loadErr.Error(), test.env) {
				t.Fatalf("loadConfig error = %v, want bounded %s rejection", loadErr, test.env)
			}
		})
	}
	t.Run("timeout must precede claim expiry", func(t *testing.T) {
		setValidConfigEnvironment(t)
		t.Setenv("VELA_ARTIFACT_REPLICATION_TIMEOUT", "20m")
		t.Setenv("VELA_ARTIFACT_REPLICATION_CLAIM_TTL", "20m")
		if _, loadErr := loadConfig(); loadErr == nil ||
			!strings.Contains(loadErr.Error(), "VELA_ARTIFACT_REPLICATION_TIMEOUT") {
			t.Fatalf("loadConfig error = %v, want timeout/claim ordering rejection", loadErr)
		}
	})
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
		configuration.schedulerCandidateAttempts != 5 ||
		configuration.natsSchedulerCredentials != "/run/secrets/vela-scheduler.creds" ||
		configuration.natsSchedulerUserPublicKeys !=
			"UAWF3LT7L6HYUQF4YB7QTKGMBMTNHTL23W5EPJSGQ2AEVKXOJMLX5MQR" {
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
		runScheduler(ctx, scheduling, time.Millisecond, nil)
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

func TestSchedulerWakeupRunsCycleWithoutReplacingPeriodicReconciliation(t *testing.T) {
	scheduling := &testHierarchicalScheduler{
		calls:      make(chan struct{}, 2),
		reconciles: make(chan struct{}, 1),
	}
	wakeups := make(chan schedulerCycleRequest, 1)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		runScheduler(ctx, scheduling, time.Hour, wakeups)
	}()

	select {
	case <-scheduling.reconciles:
	case <-time.After(time.Second):
		t.Fatal("Scheduler did not run startup reconciliation")
	}
	select {
	case <-scheduling.calls:
	case <-time.After(time.Second):
		t.Fatal("Scheduler did not run startup cycle")
	}
	cycleResult := make(chan error, 1)
	wakeups <- schedulerCycleRequest{result: cycleResult}
	select {
	case <-scheduling.calls:
	case <-time.After(time.Second):
		t.Fatal("Scheduler wakeup did not trigger RunCycle")
	}
	select {
	case err := <-cycleResult:
		if err != nil {
			t.Fatalf("Scheduler wakeup cycle result = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Scheduler wakeup did not wait for RunCycle completion")
	}
	select {
	case <-scheduling.reconciles:
		t.Fatal("event wakeup replaced the periodic PostgreSQL reconciliation boundary")
	case <-time.After(25 * time.Millisecond):
	}

	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Scheduler wakeup loop did not stop with context")
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

func TestNonContentExpiryReconcilerRetriesTransientFailureAndStopsWithContext(t *testing.T) {
	reconciler := &testNonContentExpiryReconciler{calls: make(chan struct{}, 2)}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		runNonContentExpiryReconciler(ctx, reconciler, time.Millisecond)
	}()

	for range 2 {
		select {
		case <-reconciler.calls:
		case <-time.After(time.Second):
			t.Fatal("non-content expiry Reconciler did not retry")
		}
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("non-content expiry Reconciler did not stop with context")
	}
}

func TestArtifactBackupReplicatorRetriesTransientFailureAndStopsWithContext(t *testing.T) {
	replicator := &testArtifactBackupReplicator{calls: make(chan struct{}, 2)}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		runArtifactBackupReplicator(ctx, replicator, time.Millisecond, time.Second)
	}()

	for range 2 {
		select {
		case <-replicator.calls:
		case <-time.After(time.Second):
			t.Fatal("Artifact backup Replicator did not retry")
		}
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Artifact backup Replicator did not stop with context")
	}
}

func TestReadinessPingsComplianceDatabase(t *testing.T) {
	first := &testDatabasePinger{}
	compliance := &testDatabasePinger{err: errors.New("Compliance database unavailable")}
	artifactStore := &testArtifactBucketValidator{}
	request := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	response := httptest.NewRecorder()

	readinessHandler(artifactStore, first, compliance).ServeHTTP(response, request)

	if response.Code != http.StatusServiceUnavailable || first.invocations.Load() != 1 ||
		compliance.invocations.Load() != 1 || artifactStore.invocations.Load() != 0 {
		t.Fatalf(
			"readiness with failed Compliance ping = status %d pings %d/%d store %d",
			response.Code,
			first.invocations.Load(),
			compliance.invocations.Load(),
			artifactStore.invocations.Load(),
		)
	}

	compliance.err = nil
	response = httptest.NewRecorder()
	readinessHandler(artifactStore, first, compliance).ServeHTTP(response, request)
	if response.Code != http.StatusNoContent || first.invocations.Load() != 2 ||
		compliance.invocations.Load() != 2 || artifactStore.invocations.Load() != 1 {
		t.Fatalf(
			"ready dependencies = status %d pings %d/%d store %d",
			response.Code,
			first.invocations.Load(),
			compliance.invocations.Load(),
			artifactStore.invocations.Load(),
		)
	}
}

func TestControlHTTPHandlersKeepHealthEndpointsOffPublicAPI(t *testing.T) {
	publicCalls := atomic.Int32{}
	publicAPI := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		publicCalls.Add(1)
		w.WriteHeader(http.StatusTeapot)
	})
	readinessCalls := atomic.Int32{}
	readiness := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		readinessCalls.Add(1)
		w.WriteHeader(http.StatusAccepted)
	})
	metrics := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	public, management := controlHTTPHandlers(publicAPI, metrics, readiness)

	response := httptest.NewRecorder()
	public.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if response.Code != http.StatusTeapot || publicCalls.Load() != 1 {
		t.Fatalf("public /healthz = status %d calls %d", response.Code, publicCalls.Load())
	}

	response = httptest.NewRecorder()
	management.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if response.Code != http.StatusNoContent || readinessCalls.Load() != 0 {
		t.Fatalf("management /healthz = status %d readiness calls %d", response.Code, readinessCalls.Load())
	}

	response = httptest.NewRecorder()
	management.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if response.Code != http.StatusAccepted || readinessCalls.Load() != 1 {
		t.Fatalf("management /readyz = status %d readiness calls %d", response.Code, readinessCalls.Load())
	}
	response = httptest.NewRecorder()
	management.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if response.Code != http.StatusOK {
		t.Fatalf("management /metrics = status %d", response.Code)
	}

	response = httptest.NewRecorder()
	management.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/v1/projects", nil))
	if response.Code != http.StatusNotFound || publicCalls.Load() != 1 {
		t.Fatalf("management public route = status %d public calls %d", response.Code, publicCalls.Load())
	}
}

func TestComplianceTLSListenerFailureCancelsRuntime(t *testing.T) {
	listenerFailure := errors.New("Compliance listener failed")
	server := &testTLSServer{serveErr: listenerFailure}
	var started atomic.Bool
	exits := serveTLSHTTPServer(server, nil, func() { started.Store(true) })
	ctx, stop := context.WithCancel(context.Background())
	defer stop()

	var serveErr error
	select {
	case err := <-exits:
		serveErr = handleHTTPServerExit(stop, "Compliance Legal Hold HTTPS", err)
	case <-time.After(time.Second):
		t.Fatal("Compliance TLS listener did not report failure")
	}
	if !started.Load() || !errors.Is(serveErr, listenerFailure) || ctx.Err() != context.Canceled ||
		!strings.Contains(serveErr.Error(), "serve Compliance Legal Hold HTTPS") {
		t.Fatalf("Compliance listener failure = started %t context %v error %v", started.Load(), ctx.Err(), serveErr)
	}
}

func TestComplianceShutdownHonorsBoundedContext(t *testing.T) {
	server := &testTLSServer{shutdown: func(ctx context.Context) error {
		if _, ok := ctx.Deadline(); !ok {
			return errors.New("shutdown context has no deadline")
		}
		<-ctx.Done()
		return ctx.Err()
	}}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	startedAt := time.Now()

	err := shutdownHTTPServer(ctx, server, "Compliance Legal Hold HTTPS")

	if !errors.Is(err, context.DeadlineExceeded) ||
		!strings.Contains(err.Error(), "shut down Compliance Legal Hold HTTPS server") ||
		time.Since(startedAt) > time.Second {
		t.Fatalf("bounded Compliance shutdown after %s error = %v", time.Since(startedAt), err)
	}
}

type testDatabasePinger struct {
	invocations atomic.Int32
	err         error
}

func (p *testDatabasePinger) Ping(context.Context) error {
	p.invocations.Add(1)
	return p.err
}

type testArtifactBucketValidator struct {
	invocations atomic.Int32
	err         error
}

func (v *testArtifactBucketValidator) ValidateBucket(context.Context) error {
	v.invocations.Add(1)
	return v.err
}

type testTLSServer struct {
	serveErr error
	shutdown func(context.Context) error
}

func (s *testTLSServer) ServeTLS(net.Listener, string, string) error {
	return s.serveErr
}

func (s *testTLSServer) Shutdown(ctx context.Context) error {
	if s.shutdown == nil {
		return nil
	}
	return s.shutdown(ctx)
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

type testNonContentExpiryReconciler struct {
	invocations atomic.Int32
	calls       chan struct{}
}

type testArtifactBackupReplicator struct {
	invocations atomic.Int32
	calls       chan struct{}
}

func (r *testArtifactBackupReplicator) ReplicateBatch(
	context.Context,
) (artifactreplication.Result, error) {
	invocation := r.invocations.Add(1)
	r.calls <- struct{}{}
	if invocation == 1 {
		return artifactreplication.Result{Claimed: 1, Failed: 1},
			errors.New("transient Artifact replication failure")
	}
	return artifactreplication.Result{}, nil
}

func (r *testRetentionReconciler) ReconcileBatch(context.Context) (retention.ReconcileResult, error) {
	invocation := r.invocations.Add(1)
	r.calls <- struct{}{}
	if invocation == 1 {
		return retention.ReconcileResult{Claimed: 1, Failed: 1}, errors.New("transient retention failure")
	}
	return retention.ReconcileResult{}, nil
}

func (r *testNonContentExpiryReconciler) ReconcileBatch(
	context.Context,
) (noncontentexpiry.Result, error) {
	invocation := r.invocations.Add(1)
	r.calls <- struct{}{}
	if invocation == 1 {
		return noncontentexpiry.Result{Claimed: 1, Stale: 1},
			errors.New("transient non-content expiry failure")
	}
	return noncontentexpiry.Result{}, nil
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
