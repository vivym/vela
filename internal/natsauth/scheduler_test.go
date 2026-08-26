package natsauth_test

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/nats-io/jwt/v2"
	"github.com/vivym/vela/internal/natsauth"
)

func TestConnectSchedulerRejectsCredentialDriftBeforeDial(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*jwt.UserClaims)
	}{
		{
			name: "wrong workload name",
			mutate: func(claims *jwt.UserClaims) {
				claims.Name = "vela-outbox-dispatcher"
			},
		},
		{
			name: "missing stream info",
			mutate: func(claims *jwt.UserClaims) {
				claims.Pub.Allow = claims.Pub.Allow[1:]
			},
		},
		{
			name: "wildcard consumer API",
			mutate: func(claims *jwt.UserClaims) {
				claims.Pub.Allow.Add("$JS.API.CONSUMER.>")
			},
		},
		{
			name: "broad ack authority",
			mutate: func(claims *jwt.UserClaims) {
				claims.Pub.Allow[3] = "$JS.ACK.>"
			},
		},
		{
			name: "event subscription",
			mutate: func(claims *jwt.UserClaims) {
				claims.Sub.Allow.Add("vela.events.>")
			},
		},
		{
			name: "response permission",
			mutate: func(claims *jwt.UserClaims) {
				claims.Resp = &jwt.ResponsePermission{MaxMsgs: 1, Expires: time.Second}
			},
		},
		{
			name: "deny ambiguity",
			mutate: func(claims *jwt.UserClaims) {
				claims.Pub.Deny.Add("$JS.ACK.VELA_EVENTS.VELA_SCHEDULER.1.>")
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			credential := issueSchedulerCredential(t, test.mutate)
			connection, err := natsauth.ConnectScheduler(
				validSchedulerConfig(credential),
				natsauth.Handlers{},
			)
			if connection != nil {
				connection.Close()
				t.Fatal("invalid Scheduler credential returned a connection")
			}
			if !errors.Is(err, natsauth.ErrInvalidSchedulerCredential) {
				t.Fatalf("ConnectScheduler error = %v, want invalid credential", err)
			}
			assertNoCredentialMaterial(t, err, credential)
		})
	}
}

func TestConnectSchedulerRejectsInvalidConfigurationBeforeCredentialRead(t *testing.T) {
	credential := issueSchedulerCredential(t, nil)
	config := validSchedulerConfig(credential)
	config.ExpectedUserPublicKeys = nil
	connection, err := natsauth.ConnectScheduler(config, natsauth.Handlers{})
	if connection != nil {
		connection.Close()
		t.Fatal("invalid Scheduler configuration returned a connection")
	}
	if !errors.Is(err, natsauth.ErrInvalidSchedulerConfig) {
		t.Fatalf("ConnectScheduler error = %v, want invalid configuration", err)
	}
	assertNoCredentialMaterial(t, err, credential)
}

func issueSchedulerCredential(t *testing.T, mutate func(*jwt.UserClaims)) testCredential {
	t.Helper()
	account := createAccountKey(t)
	defer account.Wipe()
	accountPublicKey := publicKey(t, account)
	signer := createAccountKey(t)
	defer signer.Wipe()
	signerPublicKey := publicKey(t, signer)
	user := createUserKey(t)
	defer user.Wipe()
	userPublicKey := publicKey(t, user)
	claims := jwt.NewUserClaims(userPublicKey)
	claims.Name = "vela-scheduler-consumer"
	claims.IssuerAccount = accountPublicKey
	claims.Expires = time.Now().Add(time.Hour).Unix()
	claims.AllowedConnectionTypes.Add(jwt.ConnectionTypeStandard)
	for _, subject := range []string{
		"$JS.API.STREAM.INFO.VELA_EVENTS",
		"$JS.API.CONSUMER.INFO.VELA_EVENTS.VELA_SCHEDULER",
		"$JS.API.CONSUMER.MSG.NEXT.VELA_EVENTS.VELA_SCHEDULER",
		"$JS.ACK.VELA_EVENTS.VELA_SCHEDULER.>",
	} {
		claims.Pub.Allow.Add(subject)
	}
	claims.Sub.Allow.Add("_INBOX.>")
	if mutate != nil {
		mutate(claims)
	}
	userJWT, err := claims.Encode(signer)
	if err != nil {
		t.Fatalf("encode Scheduler NATS user JWT: %v", err)
	}
	seed, err := user.Seed()
	if err != nil {
		t.Fatalf("read Scheduler NATS user seed: %v", err)
	}
	defer clear(seed)
	contents, err := jwt.FormatUserConfig(userJWT, seed)
	if err != nil {
		t.Fatalf("format Scheduler NATS user credential: %v", err)
	}
	path := filepath.Join(t.TempDir(), "scheduler.creds")
	if err := os.WriteFile(path, contents, 0o600); err != nil {
		t.Fatalf("write Scheduler NATS user credential: %v", err)
	}
	return testCredential{
		path:            path,
		contents:        contents,
		jwt:             userJWT,
		seed:            string(seed),
		expectedAccount: accountPublicKey,
		expectedSigners: []string{signerPublicKey},
		expectedUsers:   []string{userPublicKey},
	}
}

func validSchedulerConfig(credential testCredential) natsauth.SchedulerConfig {
	return natsauth.SchedulerConfig{
		URL:                             "tls://127.0.0.1:1",
		CredentialsFile:                 credential.path,
		RootCAFile:                      "/run/tls/nats-root-ca.pem",
		ExpectedAccountPublicKey:        credential.expectedAccount,
		ExpectedAccountSignerPublicKeys: credential.expectedSigners,
		ExpectedUserPublicKeys:          credential.expectedUsers,
	}
}
