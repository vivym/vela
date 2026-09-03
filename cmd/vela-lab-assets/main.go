package main

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"errors"
	"flag"
	"fmt"
	"io/fs"
	"math/big"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/nats-io/jwt/v2"
	"github.com/nats-io/nkeys"
	"github.com/vivym/vela/internal/labv2contract"
	"github.com/vivym/vela/internal/modelruntime"
	"github.com/vivym/vela/internal/stageauthority"
)

var privateRegistryRepositoryPattern = regexp.MustCompile(`^[a-z0-9][a-z0-9._/-]*$`)

const (
	defaultPostgresHost        = "vela-lab-postgres.vela-lab-v2.svc"
	defaultNATSHost            = "vela-lab-nats.vela-lab-v2.svc"
	defaultMinIOHost           = "vela-lab-minio.vela-lab-v2.svc"
	defaultKubernetesNamespace = labv2contract.Namespace
	worker1Name                = labv2contract.Worker1Name
	worker2Name                = labv2contract.Worker2Name
	worker1ID                  = labv2contract.Worker1ID
	worker2ID                  = labv2contract.Worker2ID
	thumbnailWorkerID          = labv2contract.ThumbnailWorkerID
	worker1MemberID            = labv2contract.Worker1MemberID
	worker2MemberID            = labv2contract.Worker2MemberID
	thumbnailWorkerMemberID    = labv2contract.ThumbnailWorkerMemberID
	worker1DeviceID            = labv2contract.Worker1DeviceID
	worker2DeviceID            = labv2contract.Worker2DeviceID
	thumbnailDeviceID          = labv2contract.ThumbnailDeviceID
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
	stageAuthorityKeyID        = "lab-stage-authority-v1"
	smokeCredentialID          = "84000000-0000-0000-0000-00000000000d"
	outboxWorkloadName         = "vela-outbox-dispatcher"
	schedulerWorkloadName      = "vela-scheduler-consumer"
)

type options struct {
	output                string
	postgresHost          string
	natsHost              string
	minioHost             string
	kubernetesNamespace   string
	runtimeImage          string
	thumbnailRuntimeImage string
	validFor              time.Duration
}

type certificateAuthority struct {
	certificate    *x509.Certificate
	privateKey     *ecdsa.PrivateKey
	certificatePEM []byte
	privateKeyPEM  []byte
}

type natsAssets struct {
	configuration       []byte
	outboxCredential    []byte
	schedulerCredential []byte
	bootstrapCredential []byte
	accountPublicKey    string
	accountSignerKey    string
	outboxUserKey       string
	schedulerUserKey    string
}

type issuedNATSUser struct {
	publicKey  string
	credential []byte
}

type loginRole struct {
	environment string
	role        string
}

var loginRoles = []loginRole{
	{"VELA_ARTIFACT_REPLICATION_DATABASE_URL", "vela_artifact_replication"},
	{"VELA_ARTIFACT_REQUEST_DATABASE_URL", "vela_artifact_request"},
	{"VELA_ATTEMPT_COORDINATOR_DATABASE_URL", "vela_attempt_coordinator"},
	{"VELA_AUTH_DATABASE_URL", "vela_auth"},
	{"VELA_BACKUP_RETENTION_DATABASE_URL", "vela_backup_retention"},
	{"VELA_BILLING_DATABASE_URL", "vela_billing"},
	{"VELA_BREAK_GLASS_AUDIT_DATABASE_URL", "vela_break_glass_audit_request"},
	{"VELA_BREAK_GLASS_REQUEST_DATABASE_URL", "vela_break_glass_request"},
	{"VELA_CANCEL_DATABASE_URL", "vela_cancel"},
	{"VELA_COMPLIANCE_DATABASE_URL", "vela_compliance"},
	{"VELA_DEBUG_DUMP_AUDIT_REQUEST_DATABASE_URL", "vela_debug_dump_audit_request"},
	{"VELA_DEBUG_DUMP_REQUEST_DATABASE_URL", "vela_debug_dump_request"},
	{"VELA_FINANCE_RECONCILIATION_DATABASE_URL", "vela_finance_reconciliation"},
	{"VELA_FLEET_DATABASE_URL", "vela_fleet"},
	{"VELA_HUMAN_AUTH_DATABASE_URL", "vela_human_auth"},
	{"VELA_HUMAN_MEMBERSHIP_AUTH_DATABASE_URL", "vela_human_membership_auth"},
	{"VELA_HUMAN_MEMBERSHIP_REQUEST_DATABASE_URL", "vela_human_membership_request"},
	{"VELA_IDENTITY_REQUEST_DATABASE_URL", "vela_identity_request"},
	{"VELA_INTERNAL_DATABASE_URL", "vela_internal"},
	{"VELA_NON_CONTENT_EXPIRY_DATABASE_URL", "vela_non_content_expiry"},
	{"VELA_ORGANIZATION_AUDIT_REQUEST_DATABASE_URL", "vela_organization_audit_request"},
	{"VELA_ORGANIZATION_BILLING_REQUEST_DATABASE_URL", "vela_organization_billing_request"},
	{"VELA_PLATFORM_OPERATOR_AUTH_DATABASE_URL", "vela_platform_operator_auth"},
	{"VELA_REMEDIATION_DATABASE_URL", "vela_remediation"},
	{"VELA_REQUEST_DATABASE_URL", "vela_request"},
	{"VELA_RETENTION_DATABASE_URL", "vela_retention"},
	{"VELA_RETENTION_REQUEST_DATABASE_URL", "vela_retention_request"},
	{"VELA_STAGE_ARTIFACT_DATABASE_URL", "vela_stage_artifact"},
	{"VELA_STAGE_SCHEDULER_DATABASE_URL", "vela_stage_scheduler"},
	{"VELA_STAGE_WORKER_CONTROL_DATABASE_URL", "vela_stage_worker_control"},
	{"VELA_WEBHOOK_DATABASE_URL", "vela_webhook"},
	{"VELA_WEBHOOK_REQUEST_DATABASE_URL", "vela_webhook_request"},
}

func main() {
	if err := run(os.Args[1:]); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "vela lab asset generation failed: %v\n", err)
		os.Exit(1)
	}
}

func run(arguments []string) error {
	flags := flag.NewFlagSet("vela-lab-assets", flag.ContinueOnError)
	flags.SetOutput(os.Stderr)
	configuration := options{}
	flags.StringVar(&configuration.output, "output", "", "new output directory")
	flags.StringVar(&configuration.postgresHost, "postgres-host", defaultPostgresHost, "PostgreSQL service host")
	flags.StringVar(&configuration.natsHost, "nats-host", defaultNATSHost, "NATS service host")
	flags.StringVar(&configuration.minioHost, "minio-host", defaultMinIOHost, "MinIO service host")
	flags.StringVar(
		&configuration.kubernetesNamespace,
		"kubernetes-namespace",
		defaultKubernetesNamespace,
		"Kubernetes namespace used in service and NATS route identities",
	)
	flags.StringVar(
		&configuration.runtimeImage,
		"runtime-image",
		"",
		"immutable private Registry image used by the H3 ModelRuntime launch manifests",
	)
	flags.StringVar(
		&configuration.thumbnailRuntimeImage,
		"thumbnail-runtime-image",
		"",
		"immutable private Registry image containing the lab CPU thumbnail ModelRuntime",
	)
	flags.DurationVar(&configuration.validFor, "valid-for", 30*24*time.Hour, "lab certificate lifetime")
	if err := flags.Parse(arguments); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return errors.New("unexpected positional arguments")
	}
	return generate(configuration)
}

func generate(configuration options) error {
	if configuration.kubernetesNamespace == "" {
		configuration.kubernetesNamespace = defaultKubernetesNamespace
	}
	if configuration.postgresHost == defaultPostgresHost {
		configuration.postgresHost = kubernetesServiceHost("vela-lab-postgres", configuration.kubernetesNamespace)
	}
	if configuration.natsHost == defaultNATSHost {
		configuration.natsHost = kubernetesServiceHost("vela-lab-nats", configuration.kubernetesNamespace)
	}
	if configuration.minioHost == defaultMinIOHost {
		configuration.minioHost = kubernetesServiceHost("vela-lab-minio", configuration.kubernetesNamespace)
	}
	if configuration.output == "" || !filepath.IsAbs(configuration.output) {
		return errors.New("--output must be an absolute path")
	}
	if configuration.validFor < time.Hour || configuration.validFor > 365*24*time.Hour {
		return errors.New("--valid-for must be between 1h and 365d")
	}
	for name, value := range map[string]string{
		"postgres host": configuration.postgresHost,
		"NATS host":     configuration.natsHost,
		"MinIO host":    configuration.minioHost,
	} {
		if !validDNSName(value) {
			return fmt.Errorf("%s is invalid", name)
		}
	}
	if !validKubernetesNamespace(configuration.kubernetesNamespace) {
		return errors.New("kubernetes namespace is invalid")
	}
	runtimeDigest, err := privateRegistryImageDigest(configuration.runtimeImage)
	if err != nil {
		return fmt.Errorf("--runtime-image: %w", err)
	}
	thumbnailRuntimeDigest, err := privateRegistryImageDigest(configuration.thumbnailRuntimeImage)
	if err != nil {
		return fmt.Errorf("--thumbnail-runtime-image: %w", err)
	}
	if thumbnailRuntimeDigest == runtimeDigest {
		return errors.New("H3 and thumbnail runtime images must have distinct digests")
	}
	if _, err := os.Lstat(configuration.output); err == nil {
		return fmt.Errorf("output path %s already exists", configuration.output)
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("inspect output path: %w", err)
	}
	parent := filepath.Dir(configuration.output)
	temporary, err := os.MkdirTemp(parent, ".vela-lab-assets-*")
	if err != nil {
		return fmt.Errorf("create temporary output directory: %w", err)
	}
	if err := os.Chmod(temporary, 0o700); err != nil {
		_ = os.RemoveAll(temporary)
		return fmt.Errorf("protect temporary output directory: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = os.RemoveAll(temporary)
		}
	}()

	ca, err := newCertificateAuthority(configuration.validFor)
	if err != nil {
		return err
	}
	if err := writeFile(temporary, "pki/ca.crt", ca.certificatePEM); err != nil {
		return err
	}
	if err := writeFile(temporary, "pki/ca.key", ca.privateKeyPEM); err != nil {
		return err
	}
	certificates := []struct {
		name       string
		commonName string
		dnsNames   []string
		uris       []string
		usages     []x509.ExtKeyUsage
	}{
		{"control-fleet", "vela-lab-control", []string{"vela-lab-control", "vela-lab-control." + configuration.kubernetesNamespace + ".svc"}, nil, []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}},
		{"control-stage-worker", "vela-lab-control", []string{"vela-lab-control", "vela-lab-control." + configuration.kubernetesNamespace + ".svc"}, nil, []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}},
		{"control-finance", "vela-lab-control", []string{"vela-lab-control", "localhost"}, nil, []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}},
		{"control-compliance", "vela-lab-control", []string{"vela-lab-control", "localhost"}, nil, []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}},
		{"control-remediation", "vela-lab-control", nil, []string{"spiffe://vela.internal/control/remediation-lab"}, []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}},
		{"nats-client", "vela-lab-control", nil, []string{"spiffe://vela.internal/control/nats-lab"}, []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}},
		{"fleet-client", "vela-lab-fleet-controller", nil, []string{"spiffe://vela.internal/fleet-controller/non-production-lab"}, []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}},
		{"fleet-admission", "vela-lab-fleet-controller", []string{"vela-lab-fleet-controller", "vela-lab-fleet-controller." + configuration.kubernetesNamespace + ".svc"}, nil, []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}},
		{"fleet-admission-client", "vela-lab-admission-probe", nil, []string{"spiffe://vela.internal/kube-apiserver/admission"}, []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}},
		{"stage-worker-1", worker1Name, nil, []string{stageWorkerSPIFFEIdentity(worker1MemberID)}, []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}},
		{"stage-worker-2", worker2Name, nil, []string{stageWorkerSPIFFEIdentity(worker2MemberID)}, []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}},
		{"stage-worker-thumbnail", "vela-lab-control-1", nil, []string{stageWorkerSPIFFEIdentity(thumbnailWorkerMemberID)}, []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}},
		{"minio-server", "vela-lab-minio", []string{"vela-lab-minio", configuration.minioHost}, nil, []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}},
	}
	natsDNSNames := []string{
		configuration.natsHost,
		"vela-lab-nats",
		"vela-lab-nats-headless." + configuration.kubernetesNamespace + ".svc",
		"vela-lab-nats-0.vela-lab-nats-headless." + configuration.kubernetesNamespace + ".svc",
		"vela-lab-nats-1.vela-lab-nats-headless." + configuration.kubernetesNamespace + ".svc",
		"vela-lab-nats-2.vela-lab-nats-headless." + configuration.kubernetesNamespace + ".svc",
	}
	certificates = append(certificates, struct {
		name       string
		commonName string
		dnsNames   []string
		uris       []string
		usages     []x509.ExtKeyUsage
	}{"nats-server", "vela-lab-nats", natsDNSNames, nil, []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth}})
	for _, certificate := range certificates {
		certificatePEM, keyPEM, issueErr := ca.issue(
			certificate.commonName,
			certificate.dnsNames,
			certificate.uris,
			certificate.usages,
			configuration.validFor,
		)
		if issueErr != nil {
			return fmt.Errorf("issue %s certificate: %w", certificate.name, issueErr)
		}
		if err := writeFile(temporary, "pki/"+certificate.name+".crt", certificatePEM); err != nil {
			return err
		}
		if err := writeFile(temporary, "pki/"+certificate.name+".key", keyPEM); err != nil {
			return err
		}
	}

	nats, err := generateNATSAssets(configuration.validFor, configuration.kubernetesNamespace)
	if err != nil {
		return err
	}
	for name, content := range map[string][]byte{
		"nats/nats.conf":       nats.configuration,
		"nats/outbox.creds":    nats.outboxCredential,
		"nats/scheduler.creds": nats.schedulerCredential,
		"nats/bootstrap.creds": nats.bootstrapCredential,
	} {
		if err := writeFile(temporary, name, content); err != nil {
			return err
		}
	}

	postgresPassword, err := randomHex(24)
	if err != nil {
		return err
	}
	minioSuffix, err := randomIdentifier(8)
	if err != nil {
		return err
	}
	minioUser := "velalab" + strings.ToLower(minioSuffix)
	minioPassword, err := randomHex(24)
	if err != nil {
		return err
	}
	pepper, err := randomBytes(32)
	if err != nil {
		return err
	}
	smokeSecret, err := randomBytes(32)
	if err != nil {
		return err
	}
	leaseKey, err := randomBytes(32)
	if err != nil {
		return err
	}
	webhookKey, err := randomBytes(32)
	if err != nil {
		return err
	}
	stageWorkerIdentityKey, err := randomBytes(32)
	if err != nil {
		return err
	}
	verifierKeyring, err := stageauthority.DeriveVerifierKeyring(map[string][]byte{
		stageAuthorityKeyID: leaseKey,
	})
	if err != nil {
		return fmt.Errorf("derive ModelRuntime StageAuthority verifier keyring: %w", err)
	}
	defer stageauthority.ClearKeyring(verifierKeyring)

	passwords := make(map[string]string, len(loginRoles))
	databaseValues := make(map[string]string, len(loginRoles))
	for _, role := range loginRoles {
		password, randomErr := randomHex(24)
		if randomErr != nil {
			return randomErr
		}
		login := role.role + "_login"
		passwords[login] = password
		databaseValues[role.environment] = postgresURL(configuration.postgresHost, login, password)
	}
	passwordJSON, err := json.Marshal(passwords)
	if err != nil {
		return fmt.Errorf("encode database login passwords: %w", err)
	}
	if err := writeEnv(temporary, "env/postgres.env", map[string]string{
		"POSTGRES_DB": "vela", "POSTGRES_USER": "postgres", "POSTGRES_PASSWORD": postgresPassword,
	}); err != nil {
		return err
	}
	if err := writeEnv(temporary, "env/minio.env", map[string]string{
		"MINIO_ROOT_USER": minioUser, "MINIO_ROOT_PASSWORD": minioPassword,
	}); err != nil {
		return err
	}
	if err := writeEnv(temporary, "env/database.env", databaseValues); err != nil {
		return err
	}
	if err := writeEnv(temporary, "env/bootstrap.env", map[string]string{
		"VELA_LAB_POSTGRES_ADMIN_URL":             postgresURL(configuration.postgresHost, "postgres", postgresPassword),
		"VELA_LAB_DATABASE_LOGIN_PASSWORDS":       string(passwordJSON),
		"VELA_LAB_MINIO_ENDPOINT":                 "https://" + configuration.minioHost + ":9000",
		"VELA_LAB_MINIO_ACCESS_KEY":               minioUser,
		"VELA_LAB_MINIO_SECRET_KEY":               minioPassword,
		"VELA_LAB_NATS_URL":                       "tls://" + configuration.natsHost + ":4222",
		"VELA_LAB_RUNTIME_IMAGE_DIGEST":           "sha256:" + runtimeDigest,
		"VELA_LAB_THUMBNAIL_RUNTIME_IMAGE_DIGEST": "sha256:" + thumbnailRuntimeDigest,
	}); err != nil {
		return err
	}
	if err := writeEnv(temporary, "env/control-secret.env", map[string]string{
		"VELA_CREDENTIAL_PEPPER_BASE64": base64.StdEncoding.EncodeToString(pepper),
	}); err != nil {
		return err
	}
	if err := writeEnv(temporary, "env/control-public.env", map[string]string{
		"VELA_NATS_OUTBOX_ACCOUNT_PUBLIC_KEY":         nats.accountPublicKey,
		"VELA_NATS_OUTBOX_ACCOUNT_SIGNER_PUBLIC_KEYS": nats.accountSignerKey,
		"VELA_NATS_OUTBOX_USER_PUBLIC_KEYS":           nats.outboxUserKey,
		"VELA_NATS_SCHEDULER_USER_PUBLIC_KEYS":        nats.schedulerUserKey,
	}); err != nil {
		return err
	}
	keyrings := map[string]map[string]string{
		"lease.json":   {stageAuthorityKeyID: base64.StdEncoding.EncodeToString(leaseKey)},
		"webhook.json": {"lab-webhook-v1": base64.StdEncoding.EncodeToString(webhookKey)},
		"model-runtime-verifier.json": {
			stageAuthorityKeyID: base64.StdEncoding.EncodeToString(verifierKeyring[stageAuthorityKeyID]),
		},
	}
	for name, payload := range keyrings {
		content, marshalErr := json.Marshal(payload)
		if marshalErr != nil {
			return marshalErr
		}
		content = append(content, '\n')
		if err := writeFile(temporary, "control/"+name, content); err != nil {
			return err
		}
	}
	launchManifests, err := buildModelRuntimeLaunchManifests(runtimeDigest, thumbnailRuntimeDigest)
	if err != nil {
		return err
	}
	for name, content := range launchManifests {
		if err := writeFile(temporary, "stage/"+name, content); err != nil {
			return err
		}
	}
	for _, worker := range labWorkerRuntimes(runtimeDigest, thumbnailRuntimeDigest) {
		if err := writeEnv(
			temporary,
			"env/stage-worker-"+worker.assetIndex+".env",
			stageWorkerEnvironment(worker),
		); err != nil {
			return err
		}
	}
	invoiceToken, err := randomIdentifier(24)
	if err != nil {
		return err
	}
	for name, content := range map[string][]byte{
		"control/minio-access-key":               []byte(minioUser + "\n"),
		"control/minio-secret-key":               []byte(minioPassword + "\n"),
		"control/invoice-bearer-token":           []byte("lab-disabled-" + invoiceToken + "\n"),
		"control/stage-worker-identity-key":      []byte(base64.StdEncoding.EncodeToString(stageWorkerIdentityKey) + "\n"),
		"bootstrap/smoke-secret":                 smokeSecret,
		"smoke/bearer-credential":                []byte(smokeBearerCredential(smokeSecret) + "\n"),
		"metadata/smoke-credential-id":           []byte(smokeCredentialID + "\n"),
		"metadata/worker-1-id":                   []byte(worker1ID + "\n"),
		"metadata/worker-2-id":                   []byte(worker2ID + "\n"),
		"metadata/worker-thumbnail-id":           []byte(thumbnailWorkerID + "\n"),
		"metadata/worker-1-member-id":            []byte(worker1MemberID + "\n"),
		"metadata/worker-2-member-id":            []byte(worker2MemberID + "\n"),
		"metadata/worker-thumbnail-member-id":    []byte(thumbnailWorkerMemberID + "\n"),
		"metadata/credential-pepper-sha256-note": []byte("secret pepper is stored only in env/control-secret.env\n"),
	} {
		if err := writeFile(temporary, name, content); err != nil {
			return err
		}
	}
	nodeAgents := map[string]map[string]any{
		worker1Name: {
			"address": "vela-lab-node-agent.invalid:9444", "server_name": "vela-lab-node-agent.invalid",
			"worker_id": worker1ID, "worker_epoch": 1,
			"spiffe_identity": nodeAgentSPIFFEIdentity(worker1Name, worker1ID),
		},
	}
	nodeAgentJSON, err := json.MarshalIndent(nodeAgents, "", "  ")
	if err != nil {
		return err
	}
	if err := writeFile(temporary, "control/node-agents.json", append(nodeAgentJSON, '\n')); err != nil {
		return err
	}

	assetFiles, err := digestAssetFiles(temporary)
	if err != nil {
		return err
	}
	manifest := map[string]any{
		"schema_version":                 1,
		"environment":                    "non-production-lab",
		"kubernetes_namespace":           configuration.kubernetesNamespace,
		"generated_at":                   time.Now().UTC().Format(time.RFC3339),
		"postgres_host":                  configuration.postgresHost,
		"nats_host":                      configuration.natsHost,
		"minio_host":                     configuration.minioHost,
		"worker_ids":                     []string{worker1ID, worker2ID, thumbnailWorkerID},
		"worker_member_ids":              []string{worker1MemberID, worker2MemberID, thumbnailWorkerMemberID},
		"runtime_image":                  configuration.runtimeImage,
		"runtime_image_digest":           runtimeDigest,
		"thumbnail_runtime_image":        configuration.thumbnailRuntimeImage,
		"thumbnail_runtime_image_digest": thumbnailRuntimeDigest,
		"production_gate_evidence":       false,
		"files":                          assetFiles,
	}
	manifestJSON, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return err
	}
	if err := writeFile(temporary, "manifest.json", append(manifestJSON, '\n')); err != nil {
		return err
	}
	if err := os.Rename(temporary, configuration.output); err != nil {
		return fmt.Errorf("publish generated assets atomically: %w", err)
	}
	committed = true
	_, _ = fmt.Fprintf(os.Stdout, "LAB_ASSETS=%s\n", configuration.output)
	return nil
}

type labWorkerRuntime struct {
	assetIndex       string
	name             string
	instanceID       string
	memberID         string
	deviceID         string
	workerProfileID  string
	role             string
	sharedSlot       string
	resourceClass    string
	capacityVector   map[string]int64
	gpuUUID          string
	pciBDF           string
	runtimeProcesses []modelruntime.LaunchRuntime
}

func labWorkerRuntimes(runtimeDigest, thumbnailRuntimeDigest string) []labWorkerRuntime {
	launchRuntime := func(
		residencyID, identity, stageProfileID, component, componentRevision, imageDigest, command string,
	) modelruntime.LaunchRuntime {
		return modelruntime.LaunchRuntime{
			ModelResidencyID: residencyID, RuntimeIdentity: identity,
			StageProfileRevisionID: stageProfileID, ModelRuntimeEpochFloor: 1,
			Component: component, ModelComponentRevision: componentRevision,
			RuntimeImageDigest: imageDigest, Command: []string{command},
			ScratchRoot:           "/var/lib/vela/stage-worker/scratch",
			InputRoot:             "/var/lib/vela/stage-worker/scratch/inputs",
			OutputRoot:            "/var/lib/vela/stage-worker/scratch/outputs",
			InitializationTimeout: "2m", ShutdownTimeout: "30s",
		}
	}
	stages := make(map[string]labv2contract.StageDescriptor)
	for _, stage := range labv2contract.StageDescriptors() {
		stages[stage.Key] = stage
	}
	workers := make([]labWorkerRuntime, 0, len(labv2contract.WorkerDescriptors()))
	for _, descriptor := range labv2contract.WorkerDescriptors() {
		runtimes := make([]modelruntime.LaunchRuntime, 0, len(descriptor.StageKeys))
		for _, stageKey := range descriptor.StageKeys {
			stage := stages[stageKey]
			imageDigest := runtimeDigest
			if stage.RuntimeImageClass == labv2contract.BootstrapRuntimeImageClass {
				imageDigest = thumbnailRuntimeDigest
			}
			runtimes = append(runtimes, launchRuntime(
				stage.ResidencyID, stage.RuntimeIdentityPrefix+"@sha256:"+imageDigest,
				stage.ProfileID, stage.Component, stage.ComponentRevision, imageDigest, stage.RuntimeCommand,
			))
		}
		workers = append(workers, labWorkerRuntime{
			assetIndex: descriptor.AssetIndex, name: descriptor.Name, instanceID: descriptor.InstanceID,
			memberID: descriptor.MemberID, deviceID: descriptor.DeviceID,
			workerProfileID: descriptor.WorkerProfileID, role: descriptor.Role,
			sharedSlot: descriptor.SharedSlot, resourceClass: descriptor.ResourceClass,
			capacityVector: descriptor.CapacityVector, gpuUUID: descriptor.GPUUUID, pciBDF: descriptor.PCIBDF,
			runtimeProcesses: runtimes,
		})
	}
	return workers
}

func buildModelRuntimeLaunchManifests(runtimeDigest, thumbnailRuntimeDigest string) (map[string][]byte, error) {
	result := make(map[string][]byte, 3)
	for _, worker := range labWorkerRuntimes(runtimeDigest, thumbnailRuntimeDigest) {
		membershipDigest, _, deviceSetDigest := labWorkerDigests(worker.name)
		manifest := modelruntime.LaunchManifest{
			SchemaVersion: 1, WorkerProfileRevisionID: worker.workerProfileID,
			WorkerRole: worker.role, CapacitySlots: 1, SharedSlotException: worker.sharedSlot,
			WorkerInstanceID: worker.instanceID, WorkerInstanceEpoch: 1,
			WorkerMemberID: worker.memberID, WorkerMemberEpoch: 1,
			DeviceSetDigest:  hex.EncodeToString(deviceSetDigest[:]),
			MembershipDigest: hex.EncodeToString(membershipDigest[:]),
			Devices:          []modelruntime.LaunchDeviceEpoch{{ID: worker.deviceID, Epoch: 1}},
			Members:          []modelruntime.LaunchMemberEpoch{{ID: worker.memberID, Epoch: 1}},
			LocalDevices: []modelruntime.DriverDevice{{
				DeviceID: worker.deviceID, DeviceEpoch: 1, ResourceClass: worker.resourceClass,
				GPUUUID: worker.gpuUUID, PCIBDF: worker.pciBDF,
			}},
			Runtimes: worker.runtimeProcesses,
		}
		encoded, err := modelruntime.EncodeLaunchManifest(manifest)
		if err != nil {
			return nil, fmt.Errorf("encode %s ModelRuntime launch manifest: %w", worker.name, err)
		}
		result["worker-"+worker.assetIndex+"-launch.json"] = append(encoded, '\n')
	}
	return result, nil
}

func stageWorkerEnvironment(worker labWorkerRuntime) map[string]string {
	membershipDigest, _, deviceSetDigest := labWorkerDigests(worker.name)
	identityDigest := sha256.Sum256([]byte(stageWorkerSPIFFEIdentity(worker.memberID)))
	devices, _ := json.Marshal([]map[string]any{{
		"device_id": worker.deviceID, "device_epoch": 1,
	}})
	members, _ := json.Marshal([]map[string]any{{
		"worker_member_id": worker.memberID, "member_epoch": 1,
		"identity_digest": hex.EncodeToString(identityDigest[:]),
	}})
	capacity, _ := json.Marshal(worker.capacityVector)
	return map[string]string{
		"VELA_WORKER_INSTANCE_ID":                worker.instanceID,
		"VELA_WORKER_INSTANCE_EPOCH":             "1",
		"VELA_WORKER_MEMBER_ID":                  worker.memberID,
		"VELA_WORKER_MEMBER_EPOCH":               "1",
		"VELA_STAGE_WORKER_DEVICES_JSON":         string(devices),
		"VELA_STAGE_WORKER_MEMBERS_JSON":         string(members),
		"VELA_STAGE_WORKER_CAPACITY_VECTOR_JSON": string(capacity),
		"VELA_LAB_DEVICE_SET_DIGEST":             hex.EncodeToString(deviceSetDigest[:]),
		"VELA_LAB_MEMBERSHIP_DIGEST":             hex.EncodeToString(membershipDigest[:]),
	}
}

func labWorkerDigests(name string) ([sha256.Size]byte, [sha256.Size]byte, [sha256.Size]byte) {
	membership := sha256.Sum256([]byte("vela/lab-v2/" + name + "/membership/v1"))
	topology := sha256.Sum256([]byte("vela/lab-v2/" + name + "/topology/v1"))
	combined := make([]byte, 0, 2*sha256.Size)
	combined = append(combined, membership[:]...)
	combined = append(combined, topology[:]...)
	return membership, topology, sha256.Sum256(combined)
}

func stageWorkerSPIFFEIdentity(memberID string) string {
	return "spiffe://vela.internal/stage-worker/" + memberID
}

func privateRegistryImageDigest(value string) (string, error) {
	const prefix = "10.1.200.17:5443/"
	if !strings.HasPrefix(value, prefix) || strings.Count(value, "@sha256:") != 1 {
		return "", errors.New("must be an immutable image from 10.1.200.17:5443")
	}
	repository, digest, found := strings.Cut(strings.TrimPrefix(value, prefix), "@sha256:")
	if !found || !privateRegistryRepositoryPattern.MatchString(repository) ||
		strings.Contains(repository, "//") || strings.Contains("/"+repository+"/", "/../") ||
		strings.HasSuffix(repository, "/") {
		return "", errors.New("repository path is invalid")
	}
	decoded, err := hex.DecodeString(digest)
	if err != nil || len(decoded) != sha256.Size || hex.EncodeToString(decoded) != digest {
		return "", errors.New("digest must be a lowercase SHA-256 value")
	}
	return digest, nil
}

func digestAssetFiles(root string) (map[string]string, error) {
	digests := make(map[string]string)
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if path == root || entry.IsDir() {
			return nil
		}
		if entry.Type()&os.ModeSymlink != 0 || !entry.Type().IsRegular() {
			return fmt.Errorf("asset path %s is not a regular file", path)
		}
		relative, err := filepath.Rel(root, path)
		if err != nil {
			return fmt.Errorf("resolve asset path %s: %w", path, err)
		}
		content, err := os.ReadFile(path)
		if err != nil {
			return fmt.Errorf("read asset %s for manifest: %w", relative, err)
		}
		digest := sha256.Sum256(content)
		digests[filepath.ToSlash(relative)] = hex.EncodeToString(digest[:])
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("digest generated asset files: %w", err)
	}
	if len(digests) == 0 {
		return nil, errors.New("generated asset set is empty")
	}
	return digests, nil
}

func nodeAgentSPIFFEIdentity(nodeIdentity, workerID string) string {
	encodedNodeIdentity := base64.RawURLEncoding.EncodeToString([]byte(nodeIdentity))
	return "spiffe://vela.internal/node-agent/" + encodedNodeIdentity + "/" + workerID
}

func newCertificateAuthority(validFor time.Duration) (certificateAuthority, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return certificateAuthority{}, fmt.Errorf("generate lab CA key: %w", err)
	}
	now := time.Now().UTC()
	serial, err := randomSerial()
	if err != nil {
		return certificateAuthority{}, err
	}
	template := &x509.Certificate{
		SerialNumber:          serial,
		Subject:               pkix.Name{CommonName: "Vela non-production lab CA"},
		NotBefore:             now.Add(-5 * time.Minute),
		NotAfter:              now.Add(validFor),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		IsCA:                  true,
		BasicConstraintsValid: true,
		MaxPathLenZero:        true,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		return certificateAuthority{}, fmt.Errorf("create lab CA certificate: %w", err)
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		return certificateAuthority{}, fmt.Errorf("marshal lab CA key: %w", err)
	}
	return certificateAuthority{
		certificate:    template,
		privateKey:     key,
		certificatePEM: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
		privateKeyPEM:  pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER}),
	}, nil
}

func (ca certificateAuthority) issue(
	commonName string,
	dnsNames []string,
	uriStrings []string,
	usages []x509.ExtKeyUsage,
	validFor time.Duration,
) ([]byte, []byte, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, nil, err
	}
	serial, err := randomSerial()
	if err != nil {
		return nil, nil, err
	}
	parsedURIs := make([]*url.URL, 0, len(uriStrings))
	for _, value := range uriStrings {
		identity, parseErr := url.Parse(value)
		if parseErr != nil || identity.Scheme != "spiffe" || identity.Host == "" || identity.User != nil || identity.Fragment != "" {
			return nil, nil, fmt.Errorf("invalid SPIFFE identity %q", value)
		}
		parsedURIs = append(parsedURIs, identity)
	}
	now := time.Now().UTC()
	template := &x509.Certificate{
		SerialNumber: serial,
		Subject:      pkix.Name{CommonName: commonName},
		NotBefore:    now.Add(-5 * time.Minute),
		NotAfter:     now.Add(validFor),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  usages,
		DNSNames:     append([]string(nil), dnsNames...),
		URIs:         parsedURIs,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, ca.certificate, &key.PublicKey, ca.privateKey)
	if err != nil {
		return nil, nil, err
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		return nil, nil, err
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
		pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER}), nil
}

func generateNATSAssets(validFor time.Duration, kubernetesNamespace string) (natsAssets, error) {
	operator, err := nkeys.CreateOperator()
	if err != nil {
		return natsAssets{}, err
	}
	defer operator.Wipe()
	operatorSigner, err := nkeys.CreateOperator()
	if err != nil {
		return natsAssets{}, err
	}
	defer operatorSigner.Wipe()
	account, err := nkeys.CreateAccount()
	if err != nil {
		return natsAssets{}, err
	}
	defer account.Wipe()
	accountSigner, err := nkeys.CreateAccount()
	if err != nil {
		return natsAssets{}, err
	}
	defer accountSigner.Wipe()
	systemAccount, err := nkeys.CreateAccount()
	if err != nil {
		return natsAssets{}, err
	}
	defer systemAccount.Wipe()
	systemSigner, err := nkeys.CreateAccount()
	if err != nil {
		return natsAssets{}, err
	}
	defer systemSigner.Wipe()

	operatorPublic, _ := operator.PublicKey()
	operatorSignerPublic, _ := operatorSigner.PublicKey()
	accountPublic, _ := account.PublicKey()
	accountSignerPublic, _ := accountSigner.PublicKey()
	systemPublic, _ := systemAccount.PublicKey()
	systemSignerPublic, _ := systemSigner.PublicKey()

	operatorClaims := jwt.NewOperatorClaims(operatorPublic)
	operatorClaims.Name = "Vela Lab Operator"
	operatorClaims.SigningKeys.Add(operatorSignerPublic)
	operatorClaims.SystemAccount = systemPublic
	operatorJWT, err := operatorClaims.Encode(operator)
	if err != nil {
		return natsAssets{}, err
	}
	accountClaims := jwt.NewAccountClaims(accountPublic)
	accountClaims.Name = "Vela Lab Workloads"
	accountClaims.SigningKeys.Add(accountSignerPublic)
	accountClaims.Limits.JetStreamLimits = jwt.JetStreamLimits{
		MemoryStorage: jwt.NoLimit, DiskStorage: jwt.NoLimit,
		Streams: jwt.NoLimit, Consumer: jwt.NoLimit,
	}
	accountJWT, err := accountClaims.Encode(operatorSigner)
	if err != nil {
		return natsAssets{}, err
	}
	systemClaims := jwt.NewAccountClaims(systemPublic)
	systemClaims.Name = "Vela Lab System"
	systemClaims.SigningKeys.Add(systemSignerPublic)
	systemJWT, err := systemClaims.Encode(operatorSigner)
	if err != nil {
		return natsAssets{}, err
	}
	expiresAt := time.Now().Add(validFor).Unix()
	outbox, err := issueNATSUser(accountPublic, accountSigner, outboxWorkloadName, expiresAt, func(claims *jwt.UserClaims) {
		claims.Pub.Allow.Add("vela.events.>")
		claims.Pub.Allow.Add("$JS.API.STREAM.INFO.VELA_EVENTS")
		claims.Sub.Allow.Add("_INBOX.>")
	})
	if err != nil {
		return natsAssets{}, err
	}
	scheduler, err := issueNATSUser(accountPublic, accountSigner, schedulerWorkloadName, expiresAt, func(claims *jwt.UserClaims) {
		claims.Pub.Allow.Add("$JS.API.STREAM.INFO.VELA_EVENTS")
		claims.Pub.Allow.Add("$JS.API.CONSUMER.INFO.VELA_EVENTS.VELA_SCHEDULER")
		claims.Pub.Allow.Add("$JS.API.CONSUMER.MSG.NEXT.VELA_EVENTS.VELA_SCHEDULER")
		claims.Pub.Allow.Add("$JS.ACK.VELA_EVENTS.VELA_SCHEDULER.>")
		claims.Sub.Allow.Add("_INBOX.>")
	})
	if err != nil {
		return natsAssets{}, err
	}
	bootstrap, err := issueNATSUser(accountPublic, accountSigner, "vela-lab-bootstrap", expiresAt, func(claims *jwt.UserClaims) {
		claims.Pub.Allow.Add("$JS.API.>")
		claims.Sub.Allow.Add("_INBOX.>")
	})
	if err != nil {
		return natsAssets{}, err
	}
	configuration := fmt.Sprintf(`server_name: $POD_NAME
port: 4222
http: 8222
operator: %s
resolver: MEMORY
resolver_preload: {
  %s: %s
  %s: %s
}
jetstream {
  store_dir: "/data"
  max_file_store: 96GiB
}
tls {
  cert_file: "/etc/nats-tls/tls.crt"
  key_file: "/etc/nats-tls/tls.key"
  ca_file: "/etc/nats-tls/ca.crt"
  verify: true
  timeout: 2
}
cluster {
  name: VELA_LAB
  listen: "0.0.0.0:6222"
  routes: [
    nats://vela-lab-nats-0.vela-lab-nats-headless.%s.svc:6222,
    nats://vela-lab-nats-1.vela-lab-nats-headless.%s.svc:6222,
    nats://vela-lab-nats-2.vela-lab-nats-headless.%s.svc:6222
  ]
  tls {
    cert_file: "/etc/nats-tls/tls.crt"
    key_file: "/etc/nats-tls/tls.key"
    ca_file: "/etc/nats-tls/ca.crt"
    timeout: 2
  }
}
`, operatorJWT, accountPublic, accountJWT, systemPublic, systemJWT,
		kubernetesNamespace, kubernetesNamespace, kubernetesNamespace)
	return natsAssets{
		configuration: []byte(configuration), outboxCredential: outbox.credential,
		schedulerCredential: scheduler.credential, bootstrapCredential: bootstrap.credential,
		accountPublicKey: accountPublic, accountSignerKey: accountSignerPublic,
		outboxUserKey: outbox.publicKey, schedulerUserKey: scheduler.publicKey,
	}, nil
}

func validKubernetesNamespace(value string) bool {
	if len(value) < 1 || len(value) > 63 {
		return false
	}
	for index, character := range value {
		if (character >= 'a' && character <= 'z') || (character >= '0' && character <= '9') {
			continue
		}
		if character != '-' || index == 0 || index == len(value)-1 {
			return false
		}
	}
	return true
}

func kubernetesServiceHost(service, namespace string) string {
	return service + "." + namespace + ".svc"
}

func issueNATSUser(
	accountPublic string,
	signer nkeys.KeyPair,
	name string,
	expiresAt int64,
	configure func(*jwt.UserClaims),
) (issuedNATSUser, error) {
	user, err := nkeys.CreateUser()
	if err != nil {
		return issuedNATSUser{}, err
	}
	defer user.Wipe()
	publicKey, err := user.PublicKey()
	if err != nil {
		return issuedNATSUser{}, err
	}
	claims := jwt.NewUserClaims(publicKey)
	claims.Name = name
	claims.IssuerAccount = accountPublic
	claims.Expires = expiresAt
	claims.AllowedConnectionTypes.Add(jwt.ConnectionTypeStandard)
	configure(claims)
	encoded, err := claims.Encode(signer)
	if err != nil {
		return issuedNATSUser{}, err
	}
	seed, err := user.Seed()
	if err != nil {
		return issuedNATSUser{}, err
	}
	defer clear(seed)
	credential, err := jwt.FormatUserConfig(encoded, seed)
	if err != nil {
		return issuedNATSUser{}, err
	}
	return issuedNATSUser{publicKey: publicKey, credential: credential}, nil
}

func postgresURL(host, username, password string) string {
	value := &url.URL{Scheme: "postgresql", Host: net.JoinHostPort(host, "5432"), Path: "/vela"}
	value.User = url.UserPassword(username, password)
	query := value.Query()
	query.Set("sslmode", "disable")
	value.RawQuery = query.Encode()
	return value.String()
}

func smokeBearerCredential(secret []byte) string {
	return "vla_" + smokeCredentialID + "." + base64.RawURLEncoding.EncodeToString(secret)
}

func writeEnv(root, relative string, values map[string]string) error {
	names := make([]string, 0, len(values))
	for name := range values {
		if name == "" || strings.ContainsAny(name, "=\n\r") {
			return errors.New("invalid environment name")
		}
		names = append(names, name)
	}
	sort.Strings(names)
	var content strings.Builder
	for _, name := range names {
		value := values[name]
		if strings.ContainsAny(value, "\n\r") {
			return fmt.Errorf("environment value %s contains a newline", name)
		}
		_, _ = fmt.Fprintf(&content, "%s=%s\n", name, value)
	}
	return writeFile(root, relative, []byte(content.String()))
}

func writeFile(root, relative string, content []byte) error {
	path := filepath.Join(root, filepath.Clean(relative))
	if !strings.HasPrefix(path, root+string(filepath.Separator)) {
		return errors.New("asset path escapes output root")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return fmt.Errorf("create asset directory: %w", err)
	}
	if err := os.Chmod(filepath.Dir(path), 0o700); err != nil {
		return fmt.Errorf("protect asset directory: %w", err)
	}
	if err := os.WriteFile(path, content, 0o600); err != nil {
		return fmt.Errorf("write asset %s: %w", relative, err)
	}
	return nil
}

func randomBytes(size int) ([]byte, error) {
	value := make([]byte, size)
	if _, err := rand.Read(value); err != nil {
		return nil, fmt.Errorf("read secure random bytes: %w", err)
	}
	return value, nil
}

func randomHex(size int) (string, error) {
	value, err := randomBytes(size)
	if err != nil {
		return "", err
	}
	return hex.EncodeToString(value), nil
}

func randomIdentifier(size int) (string, error) {
	value, err := randomBytes(size)
	if err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(value), nil
}

func randomSerial() (*big.Int, error) {
	limit := new(big.Int).Lsh(big.NewInt(1), 128)
	serial, err := rand.Int(rand.Reader, limit)
	if err != nil {
		return nil, fmt.Errorf("generate certificate serial: %w", err)
	}
	if serial.Sign() == 0 {
		return big.NewInt(1), nil
	}
	return serial, nil
}

func validDNSName(value string) bool {
	if value == "" || len(value) > 253 || strings.TrimSpace(value) != value {
		return false
	}
	for _, label := range strings.Split(value, ".") {
		if label == "" || len(label) > 63 || label[0] == '-' || label[len(label)-1] == '-' {
			return false
		}
		for _, character := range label {
			if (character < 'a' || character > 'z') && (character < '0' || character > '9') && character != '-' {
				return false
			}
		}
	}
	return true
}
