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
	"sort"
	"strings"
	"time"

	"github.com/nats-io/jwt/v2"
	"github.com/nats-io/nkeys"
)

const (
	defaultPostgresHost   = "vela-lab-postgres.vela-lab.svc"
	defaultNATSHost       = "vela-lab-nats.vela-lab.svc"
	defaultMinIOHost      = "vela-lab-minio.vela-lab.svc"
	worker1Name           = "vela-lab-worker-1"
	worker2Name           = "vela-lab-worker-2"
	worker1ID             = "84000000-0000-0000-0000-000000000101"
	worker2ID             = "84000000-0000-0000-0000-000000000102"
	smokeCredentialID     = "84000000-0000-0000-0000-00000000000d"
	outboxWorkloadName    = "vela-outbox-dispatcher"
	schedulerWorkloadName = "vela-scheduler-consumer"
)

type options struct {
	output       string
	postgresHost string
	natsHost     string
	minioHost    string
	validFor     time.Duration
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
	{"VELA_SCHEDULER_DATABASE_URL", "vela_scheduler"},
	{"VELA_SCHEDULER_INBOX_DATABASE_URL", "vela_scheduler_inbox"},
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
		{"control-worker", "vela-lab-control", []string{"vela-lab-control", "vela-lab-control.vela-lab.svc"}, nil, []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}},
		{"control-fleet", "vela-lab-control", []string{"vela-lab-control", "vela-lab-control.vela-lab.svc"}, nil, []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}},
		{"control-finance", "vela-lab-control", []string{"vela-lab-control", "localhost"}, nil, []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}},
		{"control-compliance", "vela-lab-control", []string{"vela-lab-control", "localhost"}, nil, []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}},
		{"control-remediation", "vela-lab-control", nil, []string{"spiffe://vela.internal/control/remediation-lab"}, []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}},
		{"nats-client", "vela-lab-control", nil, []string{"spiffe://vela.internal/control/nats-lab"}, []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}},
		{"worker-1", worker1Name, nil, []string{"spiffe://vela.internal/worker/" + worker1Name}, []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}},
		{"worker-2", worker2Name, nil, []string{"spiffe://vela.internal/worker/" + worker2Name}, []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}},
	}
	natsDNSNames := []string{
		configuration.natsHost,
		"vela-lab-nats",
		"vela-lab-nats-headless.vela-lab.svc",
		"vela-lab-nats-0.vela-lab-nats-headless.vela-lab.svc",
		"vela-lab-nats-1.vela-lab-nats-headless.vela-lab.svc",
		"vela-lab-nats-2.vela-lab-nats-headless.vela-lab.svc",
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

	nats, err := generateNATSAssets(configuration.validFor)
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
		"VELA_LAB_POSTGRES_ADMIN_URL":       postgresURL(configuration.postgresHost, "postgres", postgresPassword),
		"VELA_LAB_DATABASE_LOGIN_PASSWORDS": string(passwordJSON),
		"VELA_LAB_MINIO_ENDPOINT":           "http://" + configuration.minioHost + ":9000",
		"VELA_LAB_MINIO_ACCESS_KEY":         minioUser,
		"VELA_LAB_MINIO_SECRET_KEY":         minioPassword,
		"VELA_LAB_NATS_URL":                 "tls://" + configuration.natsHost + ":4222",
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
		"lease.json":   {"lab-lease-v1": base64.StdEncoding.EncodeToString(leaseKey)},
		"webhook.json": {"lab-webhook-v1": base64.StdEncoding.EncodeToString(webhookKey)},
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
	invoiceToken, err := randomIdentifier(24)
	if err != nil {
		return err
	}
	for name, content := range map[string][]byte{
		"control/minio-access-key":               []byte(minioUser + "\n"),
		"control/minio-secret-key":               []byte(minioPassword + "\n"),
		"control/invoice-bearer-token":           []byte("lab-disabled-" + invoiceToken + "\n"),
		"bootstrap/smoke-secret":                 smokeSecret,
		"smoke/bearer-credential":                []byte(smokeBearerCredential(smokeSecret) + "\n"),
		"metadata/smoke-credential-id":           []byte(smokeCredentialID + "\n"),
		"metadata/worker-1-id":                   []byte(worker1ID + "\n"),
		"metadata/worker-2-id":                   []byte(worker2ID + "\n"),
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
		"schema_version":           1,
		"environment":              "non-production-lab",
		"generated_at":             time.Now().UTC().Format(time.RFC3339),
		"postgres_host":            configuration.postgresHost,
		"nats_host":                configuration.natsHost,
		"minio_host":               configuration.minioHost,
		"worker_ids":               []string{worker1ID, worker2ID},
		"production_gate_evidence": false,
		"files":                    assetFiles,
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

func generateNATSAssets(validFor time.Duration) (natsAssets, error) {
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
    nats://vela-lab-nats-0.vela-lab-nats-headless.vela-lab.svc:6222,
    nats://vela-lab-nats-1.vela-lab-nats-headless.vela-lab.svc:6222,
    nats://vela-lab-nats-2.vela-lab-nats-headless.vela-lab.svc:6222
  ]
  tls {
    cert_file: "/etc/nats-tls/tls.crt"
    key_file: "/etc/nats-tls/tls.key"
    ca_file: "/etc/nats-tls/ca.crt"
    timeout: 2
  }
}
`, operatorJWT, accountPublic, accountJWT, systemPublic, systemJWT)
	return natsAssets{
		configuration: []byte(configuration), outboxCredential: outbox.credential,
		schedulerCredential: scheduler.credential, bootstrapCredential: bootstrap.credential,
		accountPublicKey: accountPublic, accountSignerKey: accountSignerPublic,
		outboxUserKey: outbox.publicKey, schedulerUserKey: scheduler.publicKey,
	}, nil
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
