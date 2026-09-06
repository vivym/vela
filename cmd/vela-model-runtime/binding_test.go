package main

import (
	"bytes"
	"crypto/ed25519"
	"os"
	"path/filepath"
	"testing"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/journalbinding"
	"github.com/vivym/vela/internal/modelruntime"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// This signs fixture metadata; Registry issuance is covered by integration tests.
func configureCommandJournalBinding(t *testing.T, manifest modelruntime.LaunchManifest, journal modelruntime.ExecutionJournalStatus) {
	t.Helper()
	seed := bytes.Repeat([]byte{21}, ed25519.SeedSize)
	signer, err := journalbinding.NewSigner("registry", seed)
	if err != nil {
		t.Fatal(err)
	}
	requestID := uuid.NewString()
	binding, err := signer.Sign(&velav1.WorkerBootstrapBinding{SchemaVersion: journalbinding.SchemaVersion,
		Claim: &velav1.WorkerBootstrapClaim{RequestId: requestID, WorkerInstanceId: manifest.WorkerInstanceID, WorkerInstanceEpoch: manifest.WorkerInstanceEpoch,
			WorkerMemberId: manifest.WorkerMemberID, WorkerMemberEpoch: manifest.WorkerMemberEpoch, NodeIdentity: "cpu-node", ActorIdentity: "node-agent/cpu-node",
			BundleDigest: bytes.Repeat([]byte{1}, 32), ClaimedAt: timestamppb.Now()},
		Pair: &velav1.WorkerBootstrapJournalPair{RequestId: requestID, ActorIdentity: "node-agent/cpu-node", WorkerJournalId: uuid.NewString(), WorkerScope: bytes.Repeat([]byte{2}, 32),
			RuntimeJournalId: journal.JournalID.String(), RuntimeScope: journal.Scope[:], RecordedAt: timestamppb.Now()},
	})
	if err != nil {
		t.Fatal(err)
	}
	wire, err := journalbinding.Encode(binding)
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	path := filepath.Join(root, "binding.json")
	if err := os.WriteFile(path, wire, 0o600); err != nil {
		t.Fatal(err)
	}
	keyPath := filepath.Join(root, "binding-verifiers.json")
	writeCommandJSON(t, keyPath, map[string][]byte{"registry": ed25519.NewKeyFromSeed(seed).Public().(ed25519.PublicKey)})
	t.Setenv("VELA_MODEL_RUNTIME_JOURNAL_BINDING_FILE", path)
	t.Setenv("VELA_MODEL_RUNTIME_JOURNAL_BINDING_VERIFIER_KEYRING_FILE", keyPath)
}
