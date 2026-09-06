package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/fleet"
)

func TestBootstrapCommandRejectsAmbiguousActionsBeforeTransport(t *testing.T) {
	connection := []string{"--fleet-address", "localhost:1", "--fleet-server-name", "fleet.internal", "--fleet-ca-file", "/missing-ca",
		"--client-cert-file", "/missing-cert", "--client-key-file", "/missing-key"}
	for _, arguments := range [][]string{
		{}, {"--action", "initialize"}, {"--action", "prepare"}, {"--action", "reconcile-pair"}, {"--action", "history"},
		{"--action", "history", "--request-id", uuid.Nil.String()},
		{"--action", "history", "--request-id", "CA000000-0000-0000-0000-000000000001"},
		{"--action", "history", "--request-id", uuid.NewString(), "--scratch-directory", "/unused"},
		{"--action", "history", "--request-id", uuid.NewString(), "--timeout", "0s"},
		{"--action", "history", "--request-id", uuid.NewString(), "--timeout", "6m"},
		{"--action", "history", "--request-id", uuid.NewString(), "extra-argument"},
	} {
		var output bytes.Buffer
		args := append([]string{"bootstrap"}, connection...)
		err := runCommand(t.Context(), append(args, arguments...), &output, io.Discard)
		if err == nil || strings.Contains(err.Error(), "missing-") || output.Len() != 0 {
			t.Fatalf("invalid action reached transport or produced output: %v: %v %s", arguments, err, output.String())
		}
	}
	if err := runCommand(t.Context(), []string{"unknown"}, io.Discard, io.Discard); err == nil {
		t.Fatal("unknown arguments started the daemon")
	}
	if err := runCommand(nil, []string{"bootstrap"}, io.Discard, io.Discard); err == nil { //nolint:staticcheck // Exercise the explicit invalid-context rejection.
		t.Fatal("nil context accepted")
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := runCommand(ctx, []string{"bootstrap"}, io.Discard, io.Discard); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled command: %v", err)
	}
	if err := runCommand(t.Context(), []string{"bootstrap", "--help"}, io.Discard, io.Discard); err != nil {
		t.Fatalf("bootstrap help: %v", err)
	}
}

func TestBootstrapHistoryOutputCarriesIdentityWithoutPermission(t *testing.T) {
	history := fleet.WorkerBootstrapHistory{Claim: fleet.WorkerBootstrapClaim{RequestID: uuid.New(), WorkerInstanceID: uuid.New(),
		WorkerInstanceEpoch: 1, WorkerMemberID: uuid.New(), WorkerMemberEpoch: 2, NodeIdentity: "cpu-node-1",
		BundleDigest: bytes.Repeat([]byte{3}, 32), ClaimedAt: time.Now().UTC(), Fresh: true}, ActorIdentity: "test-node-agent"}
	for _, recorded := range []bool{false, true} {
		if recorded {
			history.Receipt = &fleet.WorkerBootstrapReceipt{WorkerJournalID: uuid.New(), RuntimeJournalID: uuid.New(),
				WorkerScope: bytes.Repeat([]byte{4}, 32), RuntimeScope: bytes.Repeat([]byte{5}, 32)}
			history.RecordedAt = time.Now().UTC()
		}
		value := bootstrapHistoryOutput(history)
		wire, err := json.Marshal(value)
		if err != nil || value.RequestID != history.Claim.RequestID || value.WorkerMemberEpoch != 2 ||
			value.BundleDigest != strings.Repeat("03", 32) || value.Action != "history" || (value.Pair != nil) != recorded {
			t.Fatalf("history projection: %s %v", wire, err)
		}
		for _, denied := range []string{"fresh", "ready", "initialize", "drain"} {
			if bytes.Contains(bytes.ToLower(wire), []byte(denied)) {
				t.Fatalf("history output contains permission: %s", wire)
			}
		}
	}
}
