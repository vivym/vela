package main

import (
	"context"
	"encoding/base64"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/vivym/vela/internal/cancellation"
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
	t.Setenv("VELA_CANCEL_DATABASE_URL", "postgres://cancel.example/vela")
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
