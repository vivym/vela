package main

import (
	"encoding/base64"
	"encoding/json"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestLoadConfigurationRequiresEverySeparatedDatabaseURL(t *testing.T) {
	setValidEnvironment(t)
	_ = os.Unsetenv("VELA_FLEET_DATABASE_URL")
	_, err := loadConfiguration(validOptions(t))
	if err == nil || !strings.Contains(err.Error(), "VELA_FLEET_DATABASE_URL is required") {
		t.Fatalf("missing Fleet database error = %v", err)
	}
}

func TestLoadConfigurationAcceptsGeneratedShape(t *testing.T) {
	setValidEnvironment(t)
	configuration, err := loadConfiguration(validOptions(t))
	if err != nil {
		t.Fatalf("load configuration: %v", err)
	}
	if len(configuration.databaseURLs) != len(requiredDatabaseEnvironments) ||
		len(configuration.loginPasswords) != len(requiredDatabaseEnvironments) ||
		len(configuration.credentialPepper) != 32 {
		t.Fatalf("configuration is incomplete: %#v", configuration)
	}
}

func TestRequiredControlPrincipalFixturesAreDeterministicAndDistinct(t *testing.T) {
	seenIDs := make(map[string]struct{}, len(requiredControlPrincipals))
	seenRoles := make(map[string]struct{}, len(requiredControlPrincipals))
	for _, fixture := range requiredControlPrincipals {
		if _, duplicate := seenIDs[fixture.principalID]; duplicate {
			t.Fatalf("duplicate control Principal ID %q", fixture.principalID)
		}
		seenIDs[fixture.principalID] = struct{}{}
		if _, duplicate := seenRoles[fixture.databaseRole]; duplicate {
			t.Fatalf("duplicate control database role %q", fixture.databaseRole)
		}
		seenRoles[fixture.databaseRole] = struct{}{}
		identity, err := url.Parse(fixture.tlsURI)
		if err != nil || identity.Scheme != "spiffe" || identity.Host != "vela.internal" ||
			identity.RawQuery != "" || identity.Fragment != "" {
			t.Fatalf("%s SPIFFE identity %q is invalid", fixture.name, fixture.tlsURI)
		}
		if !identifierPattern.MatchString(fixture.databaseRole) ||
			!strings.HasSuffix(fixture.databaseRole, "_login") ||
			!identifierPattern.MatchString(fixture.principalTable) ||
			!identifierPattern.MatchString(fixture.bindingTable) {
			t.Fatalf("%s fixture identifiers are invalid: %#v", fixture.name, fixture)
		}
	}
	if len(requiredControlPrincipals) != 2 {
		t.Fatalf("required control Principal count = %d, want 2", len(requiredControlPrincipals))
	}
}

func TestExecutionLeaseRenewalReceiptIsBounded(t *testing.T) {
	if strings.TrimSpace(executionLeaseRenewalReceipt) != executionLeaseRenewalReceipt ||
		len(executionLeaseRenewalReceipt) < 1 || len(executionLeaseRenewalReceipt) > 1000 {
		t.Fatalf("execution Lease renewal receipt is invalid: %q", executionLeaseRenewalReceipt)
	}
}

func setValidEnvironment(t *testing.T) {
	t.Helper()
	t.Setenv("VELA_LAB_POSTGRES_ADMIN_URL", "postgresql://postgres:secret@postgres:5432/vela?sslmode=disable")
	t.Setenv("VELA_LAB_MINIO_ENDPOINT", "http://minio:9000")
	t.Setenv("VELA_LAB_MINIO_ACCESS_KEY", "velalab")
	t.Setenv("VELA_LAB_MINIO_SECRET_KEY", "secret")
	t.Setenv("VELA_LAB_NATS_URL", "tls://nats:4222")
	t.Setenv("VELA_CREDENTIAL_PEPPER_BASE64", base64.StdEncoding.EncodeToString(make([]byte, 32)))
	passwords := make(map[string]string, len(requiredDatabaseEnvironments))
	for index, name := range requiredDatabaseEnvironments {
		login := "vela_test_" + strings.ToLower(strings.TrimPrefix(name, "VELA_")) + "_login"
		login = strings.ReplaceAll(login, "_database_url", "")
		if index == 0 {
			login = "vela_artifact_replication_login"
		}
		passwords[login] = strings.Repeat("a", 48)
		t.Setenv(name, "postgresql://"+login+":"+strings.Repeat("a", 48)+"@postgres:5432/vela?sslmode=disable")
	}
	encoded, err := json.Marshal(passwords)
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv("VELA_LAB_DATABASE_LOGIN_PASSWORDS", string(encoded))
}

func validOptions(t *testing.T) options {
	t.Helper()
	root := t.TempDir()
	paths := []string{"nats.creds", "ca.crt", "tls.crt", "tls.key", "smoke-secret"}
	for _, name := range paths {
		if err := os.WriteFile(filepath.Join(root, name), []byte("fixture"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	return options{
		databaseRoot: root, natsCredential: filepath.Join(root, "nats.creds"),
		natsRootCA: filepath.Join(root, "ca.crt"), natsCert: filepath.Join(root, "tls.crt"),
		natsKey: filepath.Join(root, "tls.key"), smokeSecret: filepath.Join(root, "smoke-secret"),
		timeout: time.Minute,
	}
}
