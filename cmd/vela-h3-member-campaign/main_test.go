package main

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/vivym/vela/internal/h3membercampaign"
)

func TestRunClientEmitsTypedReceiptFromSecretBoundConfiguration(t *testing.T) {
	directory := t.TempDir()
	keyPath := filepath.Join(directory, "authority.key")
	if err := os.WriteFile(keyPath, []byte("0123456789abcdef0123456789abcdef"), 0o600); err != nil {
		t.Fatalf("write authority key: %v", err)
	}
	values := map[string]string{
		campaignAddressEnvironment:           "follower.vela-h3-disposable.svc:7444",
		campaignServerNameEnvironment:        "follower.vela-h3-disposable.svc",
		campaignClientCertificateEnvironment: "/run/campaign/client.crt",
		campaignClientPrivateKeyEnvironment:  "/run/campaign/client.key",
		campaignServerCAEnvironment:          "/run/campaign/ca.crt",
		campaignAuthorityKeyFileEnvironment:  keyPath,
		campaignDialTimeoutEnvironment:       "3s",
		campaignCommandTimeoutEnvironment:    "20s",
	}
	var captured h3membercampaign.ClientConfig
	want := h3membercampaign.Receipt{
		SchemaVersion: h3membercampaign.SchemaVersion,
		Outcome:       h3membercampaign.OutcomePass,
		BarrierPassed: true,
	}
	var stdout, stderr bytes.Buffer
	code := run(
		context.Background(), []string{"run"},
		func(name string) string { return values[name] },
		&stdout, &stderr,
		func(context.Context, h3membercampaign.ServerConfig) error { return nil },
		func(_ context.Context, config h3membercampaign.ClientConfig) (h3membercampaign.Receipt, error) {
			captured = config
			return want, nil
		},
	)
	if code != 0 || stderr.Len() != 0 ||
		captured.Address != values[campaignAddressEnvironment] ||
		captured.ServerName != values[campaignServerNameEnvironment] ||
		captured.DialTimeout != 3*time.Second || captured.CommandTimeout != 20*time.Second ||
		captured.ExpectedStartFailure || captured.RemoteStartDelay != 0 {
		t.Fatalf("run client code=%d config=%#v stderr=%q", code, captured, stderr.String())
	}
	var receipt h3membercampaign.Receipt
	if err := json.Unmarshal(stdout.Bytes(), &receipt); err != nil || receipt != want {
		t.Fatalf("decode client receipt = %#v error=%v stdout=%q", receipt, err, stdout.String())
	}
}

func TestRunFaultRequiresBoundedRemoteStartDelay(t *testing.T) {
	directory := t.TempDir()
	keyPath := filepath.Join(directory, "authority.key")
	if err := os.WriteFile(keyPath, []byte("0123456789abcdef0123456789abcdef"), 0o600); err != nil {
		t.Fatalf("write authority key: %v", err)
	}
	values := map[string]string{
		campaignAddressEnvironment:           "follower.vela-h3-disposable.svc:7444",
		campaignServerNameEnvironment:        "follower.vela-h3-disposable.svc",
		campaignClientCertificateEnvironment: "/run/campaign/client.crt",
		campaignClientPrivateKeyEnvironment:  "/run/campaign/client.key",
		campaignServerCAEnvironment:          "/run/campaign/ca.crt",
		campaignAuthorityKeyFileEnvironment:  keyPath,
		campaignDialTimeoutEnvironment:       "3s",
		campaignCommandTimeoutEnvironment:    "20s",
	}
	var stdout, stderr bytes.Buffer
	called := false
	code := run(
		context.Background(), []string{"run-fault"},
		func(name string) string { return values[name] },
		&stdout, &stderr,
		func(context.Context, h3membercampaign.ServerConfig) error { return nil },
		func(context.Context, h3membercampaign.ClientConfig) (h3membercampaign.Receipt, error) {
			called = true
			return h3membercampaign.Receipt{}, nil
		},
	)
	if code != 2 || called || stdout.Len() != 0 ||
		!bytes.Contains(stderr.Bytes(), []byte(campaignRemoteStartDelayEnvironment)) {
		t.Fatalf("run fault code=%d called=%t stdout=%q stderr=%q", code, called, stdout.String(), stderr.String())
	}
}
