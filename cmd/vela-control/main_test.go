package main

import (
	"encoding/base64"
	"strings"
	"testing"
)

func TestLoadConfigRequiresNATSWorkloadCredentialsAndRootCA(t *testing.T) {
	tests := []struct {
		name       string
		missingEnv string
	}{
		{name: "workload credentials", missingEnv: "VELA_NATS_CREDENTIALS_FILE"},
		{name: "root CA", missingEnv: "VELA_NATS_ROOT_CA_FILE"},
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

func setValidConfigEnvironment(t *testing.T) {
	t.Helper()
	t.Setenv("VELA_AUTH_DATABASE_URL", "postgres://auth.example/vela")
	t.Setenv("VELA_REQUEST_DATABASE_URL", "postgres://request.example/vela")
	t.Setenv("VELA_INTERNAL_DATABASE_URL", "postgres://internal.example/vela")
	t.Setenv("VELA_NATS_URL", "nats://nats.example:4222")
	t.Setenv("VELA_NATS_CREDENTIALS_FILE", "/run/secrets/vela-control.creds")
	t.Setenv("VELA_NATS_ROOT_CA_FILE", "/run/secrets/nats-root-ca.pem")
	t.Setenv(
		"VELA_CREDENTIAL_PEPPER_BASE64",
		base64.StdEncoding.EncodeToString(make([]byte, 32)),
	)
	t.Setenv("VELA_NATS_CLIENT_CERT_FILE", "")
	t.Setenv("VELA_NATS_CLIENT_KEY_FILE", "")
	t.Setenv("VELA_OUTBOX_BATCH_SIZE", "")
}
