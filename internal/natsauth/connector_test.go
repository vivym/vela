package natsauth_test

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/jwt/v2"
	"github.com/nats-io/nkeys"
	"github.com/vivym/vela/internal/natsauth"
)

func TestConnectOutboxRejectsInvalidConfigurationBeforeDial(t *testing.T) {
	credential := issueOutboxCredential(t, nil)
	tests := []struct {
		name   string
		mutate func(*natsauth.OutboxConfig)
	}{
		{
			name: "plaintext endpoint",
			mutate: func(config *natsauth.OutboxConfig) {
				config.URL = "nats://nats.internal:4222"
			},
		},
		{
			name: "URL userinfo",
			mutate: func(config *natsauth.OutboxConfig) {
				config.URL = "tls://shared:secret@nats.internal:4222"
			},
		},
		{
			name: "empty server entry",
			mutate: func(config *natsauth.OutboxConfig) {
				config.URL += ","
			},
		},
		{
			name: "missing root CA",
			mutate: func(config *natsauth.OutboxConfig) {
				config.RootCAFile = ""
			},
		},
		{
			name: "missing credential",
			mutate: func(config *natsauth.OutboxConfig) {
				config.CredentialsFile = ""
			},
		},
		{
			name: "missing account identity",
			mutate: func(config *natsauth.OutboxConfig) {
				config.ExpectedAccountPublicKey = ""
			},
		},
		{
			name: "missing user identity",
			mutate: func(config *natsauth.OutboxConfig) {
				config.ExpectedUserPublicKeys = nil
			},
		},
		{
			name: "duplicate overlap user",
			mutate: func(config *natsauth.OutboxConfig) {
				config.ExpectedUserPublicKeys = append(
					config.ExpectedUserPublicKeys,
					config.ExpectedUserPublicKeys[0],
				)
			},
		},
		{
			name: "more than two overlap users",
			mutate: func(config *natsauth.OutboxConfig) {
				for range 2 {
					key := createUserKey(t)
					defer key.Wipe()
					config.ExpectedUserPublicKeys = append(
						config.ExpectedUserPublicKeys,
						publicKey(t, key),
					)
				}
			},
		},
		{
			name: "invalid root CA",
			mutate: func(config *natsauth.OutboxConfig) {
				config.RootCAFile = credential.path
			},
		},
		{
			name: "only client certificate",
			mutate: func(config *natsauth.OutboxConfig) {
				config.ClientCertificateFile = "/run/tls/client.crt"
			},
		},
		{
			name: "only client key",
			mutate: func(config *natsauth.OutboxConfig) {
				config.ClientKeyFile = "/run/tls/client.key"
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			config := validOutboxConfig(credential)
			test.mutate(&config)
			connection, err := natsauth.ConnectOutbox(config, natsauth.Handlers{})
			if connection != nil {
				connection.Close()
				t.Fatal("invalid NATS Outbox configuration returned a connection")
			}
			if !errors.Is(err, natsauth.ErrInvalidOutboxConfig) {
				t.Fatalf("ConnectOutbox error = %v, want invalid configuration", err)
			}
			assertNoCredentialMaterial(t, err, credential)
		})
	}
}

func TestConnectOutboxRejectsCredentialDriftWithoutLeakingSecrets(t *testing.T) {
	tests := []struct {
		name               string
		mutateClaims       func(*jwt.UserClaims)
		mutateCredential   func(*testCredential)
		credentialContents []byte
		credentialMode     os.FileMode
	}{
		{
			name: "wrong account",
			mutateCredential: func(credential *testCredential) {
				key := createAccountKey(t)
				defer key.Wipe()
				credential.expectedAccount = publicKey(t, key)
			},
		},
		{
			name: "wrong user",
			mutateCredential: func(credential *testCredential) {
				key := createUserKey(t)
				defer key.Wipe()
				credential.expectedUsers = []string{publicKey(t, key)}
			},
		},
		{
			name: "wrong workload name",
			mutateClaims: func(claims *jwt.UserClaims) {
				claims.Name = "vela-scheduler"
			},
		},
		{
			name: "publish permission drift",
			mutateClaims: func(claims *jwt.UserClaims) {
				claims.Pub.Allow.Add("vela.commands.>")
			},
		},
		{
			name: "subscribe permission drift",
			mutateClaims: func(claims *jwt.UserClaims) {
				claims.Sub.Allow.Add("vela.events.>")
			},
		},
		{
			name: "deny permission ambiguity",
			mutateClaims: func(claims *jwt.UserClaims) {
				claims.Pub.Deny.Add("vela.events.private.>")
			},
		},
		{
			name: "response permission",
			mutateClaims: func(claims *jwt.UserClaims) {
				claims.Resp = &jwt.ResponsePermission{
					MaxMsgs: 1,
					Expires: time.Second,
				}
			},
		},
		{
			name: "bearer credential",
			mutateClaims: func(claims *jwt.UserClaims) {
				claims.BearerToken = true
			},
		},
		{
			name: "perpetual credential",
			mutateClaims: func(claims *jwt.UserClaims) {
				claims.Expires = 0
			},
		},
		{
			name: "expired credential",
			mutateClaims: func(claims *jwt.UserClaims) {
				claims.Expires = time.Now().Add(-time.Minute).Unix()
			},
		},
		{
			name:               "malformed credential",
			credentialContents: []byte("not-a-nats-credential\nSENSITIVE-SENTINEL\n"),
		},
		{
			name:               "oversized credential",
			credentialContents: []byte(strings.Repeat("x", 64*1024+1)),
		},
		{
			name:           "group readable credential",
			credentialMode: 0o640,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			credential := issueOutboxCredential(t, test.mutateClaims)
			if test.mutateCredential != nil {
				test.mutateCredential(&credential)
			}
			if test.credentialContents != nil {
				credential.contents = test.credentialContents
			}
			mode := test.credentialMode
			if mode == 0 {
				mode = 0o600
			}
			if err := os.WriteFile(credential.path, credential.contents, mode); err != nil {
				t.Fatalf("write mutated NATS credential: %v", err)
			}
			if err := os.Chmod(credential.path, mode); err != nil {
				t.Fatalf("set mutated NATS credential mode: %v", err)
			}

			connection, err := natsauth.ConnectOutbox(
				validOutboxConfig(credential),
				natsauth.Handlers{},
			)
			if connection != nil {
				connection.Close()
				t.Fatal("invalid NATS Outbox credential returned a connection")
			}
			if !errors.Is(err, natsauth.ErrInvalidOutboxCredential) {
				t.Fatalf("ConnectOutbox error = %v, want invalid credential", err)
			}
			assertNoCredentialMaterial(t, err, credential)
		})
	}
}

type testCredential struct {
	path            string
	contents        []byte
	jwt             string
	seed            string
	expectedAccount string
	expectedUsers   []string
}

func issueOutboxCredential(t *testing.T, mutate func(*jwt.UserClaims)) testCredential {
	t.Helper()
	account := createAccountKey(t)
	defer account.Wipe()
	accountPublicKey := publicKey(t, account)
	signer := createAccountKey(t)
	defer signer.Wipe()
	user := createUserKey(t)
	defer user.Wipe()
	userPublicKey := publicKey(t, user)

	claims := jwt.NewUserClaims(userPublicKey)
	claims.Name = "vela-outbox-dispatcher"
	claims.IssuerAccount = accountPublicKey
	claims.Expires = time.Now().Add(time.Hour).Unix()
	claims.AllowedConnectionTypes.Add(jwt.ConnectionTypeStandard)
	claims.Pub.Allow.Add("vela.events.>")
	claims.Sub.Allow.Add("_INBOX.>")
	if mutate != nil {
		mutate(claims)
	}
	userJWT, err := claims.Encode(signer)
	if err != nil {
		t.Fatalf("encode NATS user JWT: %v", err)
	}
	seed, err := user.Seed()
	if err != nil {
		t.Fatalf("read NATS user seed: %v", err)
	}
	defer clear(seed)
	contents, err := jwt.FormatUserConfig(userJWT, seed)
	if err != nil {
		t.Fatalf("format NATS user credential: %v", err)
	}
	path := filepath.Join(t.TempDir(), "outbox.creds")
	if err := os.WriteFile(path, contents, 0o600); err != nil {
		t.Fatalf("write NATS user credential: %v", err)
	}
	return testCredential{
		path:            path,
		contents:        contents,
		jwt:             userJWT,
		seed:            string(seed),
		expectedAccount: accountPublicKey,
		expectedUsers:   []string{userPublicKey},
	}
}

func validOutboxConfig(credential testCredential) natsauth.OutboxConfig {
	return natsauth.OutboxConfig{
		URL:                      "tls://127.0.0.1:1",
		CredentialsFile:          credential.path,
		RootCAFile:               "/run/tls/nats-root-ca.pem",
		ExpectedAccountPublicKey: credential.expectedAccount,
		ExpectedUserPublicKeys:   credential.expectedUsers,
	}
}

func createAccountKey(t *testing.T) nkeys.KeyPair {
	t.Helper()
	key, err := nkeys.CreateAccount()
	if err != nil {
		t.Fatalf("create NATS account key: %v", err)
	}
	return key
}

func createUserKey(t *testing.T) nkeys.KeyPair {
	t.Helper()
	key, err := nkeys.CreateUser()
	if err != nil {
		t.Fatalf("create NATS user key: %v", err)
	}
	return key
}

func publicKey(t *testing.T, key nkeys.KeyPair) string {
	t.Helper()
	publicKey, err := key.PublicKey()
	if err != nil {
		t.Fatalf("read NATS public key: %v", err)
	}
	return publicKey
}

func assertNoCredentialMaterial(t *testing.T, err error, credential testCredential) {
	t.Helper()
	if err == nil {
		t.Fatal("expected an error")
	}
	for _, sensitive := range []string{
		credential.path,
		credential.jwt,
		credential.seed,
		credential.expectedAccount,
		strings.Join(credential.expectedUsers, ","),
		"SENSITIVE-SENTINEL",
	} {
		if sensitive != "" && strings.Contains(err.Error(), sensitive) {
			t.Fatalf("error exposed NATS credential material %q: %v", sensitive, err)
		}
	}
}
