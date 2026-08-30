//go:build vela_lab_fault_injection

package main

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/workercontrol"
)

func TestLabStaleCompletionProbeValidateOnly(t *testing.T) {
	var output bytes.Buffer
	handled, err := runLabStaleCompletionProbeCommand(
		[]string{labStaleCompletionProbeArg, "--validate-only"}, &output,
	)
	if err != nil || !handled {
		t.Fatalf("stale Completion probe handled = %t, error = %v", handled, err)
	}
	if !regexp.MustCompile(
		`^schema=vela-lab-stale-completion-probe-v1 validation=PASS binary_sha256=[0-9a-f]{64} production_gates=0/9\n$`,
	).MatchString(output.String()) {
		t.Fatalf("stale Completion probe validation output = %q", output.String())
	}
}

func TestLabStaleCompletionProbeAuthorityStatusContainsNoIdentityOrToken(t *testing.T) {
	for _, forbidden := range []string{"job_id", "attempt_id", "worker_id", "fence", "token"} {
		if strings.Contains(labStaleCompletionLoadedStatus, forbidden) {
			t.Fatalf("authority status contains forbidden field %q", forbidden)
		}
	}
}

func TestReadLabStaleCompletionAuthorityUsesExactFinalizationCandidate(t *testing.T) {
	state := testLabStaleFinalizationState()
	encoded, err := json.Marshal(state)
	if err != nil {
		t.Fatalf("marshal Finalization State: %v", err)
	}
	if !bytes.Contains(encoded, []byte(`"lease_token":"private-lease-token"`)) {
		t.Fatalf("Finalization State did not retain its Lease token: %s", encoded)
	}
	path := writeLabStaleCompletionAuthority(t, encoded)

	got, gotEncoded, err := readLabStaleCompletionAuthority(path)
	if err != nil {
		t.Fatalf("read Finalization State: %v", err)
	}
	if !validLabStaleFinalizationState(
		got, state.JobID, state.AttemptID, state.WorkerID, state.WorkerEpoch, state.LeaseFence,
	) || !bytes.Equal(gotEncoded, encoded) {
		t.Fatalf("decoded Finalization State = %#v", got)
	}
	digest, err := labStaleCompletionCandidateSHA256(*got.CompletionCandidate)
	if err != nil || !regexp.MustCompile(`^[0-9a-f]{64}$`).MatchString(digest) {
		t.Fatalf("Finalization candidate digest = %q error %v", digest, err)
	}
}

func TestWaitForLabStaleCompletionAuthorityCapturesTransientCandidate(t *testing.T) {
	state := testLabStaleFinalizationState()
	encoded, err := json.Marshal(state)
	if err != nil {
		t.Fatalf("marshal Finalization State: %v", err)
	}
	path := filepath.Join(t.TempDir(), "upload--finalization.json")
	done := make(chan error, 1)
	go func() {
		time.Sleep(10 * time.Millisecond)
		temporaryPath := path + ".tmp"
		if err := os.WriteFile(temporaryPath, encoded, 0o600); err != nil {
			done <- err
			return
		}
		if err := os.Rename(temporaryPath, path); err != nil {
			done <- err
			return
		}
		time.Sleep(40 * time.Millisecond)
		done <- os.Remove(path)
	}()

	got, gotEncoded, err := waitForLabStaleCompletionAuthority(
		path, state.JobID, state.AttemptID, state.WorkerID, state.WorkerEpoch,
		state.LeaseFence, time.Now().Add(time.Second),
	)
	if err != nil {
		t.Fatalf("wait for transient Finalization State: %v", err)
	}
	if err := <-done; err != nil {
		t.Fatalf("write transient Finalization State: %v", err)
	}
	if got.CompletionCandidate.CompletionID != state.CompletionID ||
		!bytes.Equal(gotEncoded, encoded) {
		t.Fatalf("transient Finalization State = %#v, encoded = %s", got, gotEncoded)
	}
}

func TestWaitForLabStaleCompletionAuthorityWaitsForExactCandidate(t *testing.T) {
	state := testLabStaleFinalizationState()
	incomplete := state
	incomplete.JobVersion = 0
	incomplete.Artifacts = nil
	incomplete.CompletionCandidate = nil
	incompleteEncoded, err := json.Marshal(incomplete)
	if err != nil {
		t.Fatalf("marshal incomplete Finalization State: %v", err)
	}
	completeEncoded, err := json.Marshal(state)
	if err != nil {
		t.Fatalf("marshal complete Finalization State: %v", err)
	}
	path := writeLabStaleCompletionAuthority(t, incompleteEncoded)
	done := make(chan error, 1)
	go func() {
		time.Sleep(20 * time.Millisecond)
		temporaryPath := path + ".tmp"
		if writeErr := os.WriteFile(temporaryPath, completeEncoded, 0o600); writeErr != nil {
			done <- writeErr
			return
		}
		done <- os.Rename(temporaryPath, path)
	}()

	got, gotEncoded, err := waitForLabStaleCompletionAuthority(
		path, state.JobID, state.AttemptID, state.WorkerID, state.WorkerEpoch,
		state.LeaseFence, time.Now().Add(time.Second),
	)
	if err != nil {
		t.Fatalf("wait for exact Finalization candidate: %v", err)
	}
	if err := <-done; err != nil {
		t.Fatalf("complete Finalization State: %v", err)
	}
	if got.CompletionCandidate == nil ||
		got.CompletionCandidate.CompletionID != state.CompletionID ||
		!bytes.Equal(gotEncoded, completeEncoded) {
		t.Fatalf("completed Finalization State = %#v, encoded = %s", got, gotEncoded)
	}
}

func TestWaitForLabStaleCompletionAuthorityRejectsInvalidCandidateWithoutLeakingToken(t *testing.T) {
	state := testLabStaleFinalizationState()
	state.CompletionCandidate.ArtifactIDs = []uuid.UUID{uuid.New()}
	encoded, err := json.Marshal(state)
	if err != nil {
		t.Fatalf("marshal invalid Finalization State: %v", err)
	}
	path := writeLabStaleCompletionAuthority(t, encoded)
	_, _, err = waitForLabStaleCompletionAuthority(
		path, state.JobID, state.AttemptID, state.WorkerID, state.WorkerEpoch,
		state.LeaseFence, time.Now().Add(time.Second),
	)
	if err == nil || !strings.Contains(err.Error(), "identity is invalid") {
		t.Fatalf("invalid Finalization candidate error = %v", err)
	}
	if strings.Contains(err.Error(), state.LeaseToken) {
		t.Fatalf("Finalization candidate error leaked Lease token: %v", err)
	}
}

func TestReadLabStaleCompletionFilesRequireExactPermissionsAndSignalSchema(t *testing.T) {
	state := testLabStaleFinalizationState()
	signal := testLabStaleCompletionSignal(state)
	encoded, err := json.Marshal(signal)
	if err != nil {
		t.Fatalf("marshal signal: %v", err)
	}
	path := filepath.Join(t.TempDir(), "go.json")
	if err := os.WriteFile(path, encoded, 0o444); err != nil {
		t.Fatalf("write signal: %v", err)
	}
	if err := os.Chmod(path, 0o444); err != nil {
		t.Fatalf("chmod signal: %v", err)
	}
	got, err := readLabStaleCompletionSignal(path)
	if err != nil || got != signal {
		t.Fatalf("decoded signal = %#v error %v", got, err)
	}

	if err := os.Chmod(path, 0o600); err != nil {
		t.Fatalf("chmod unsafe signal: %v", err)
	}
	if _, err := readLabStaleCompletionSignal(path); err == nil ||
		!strings.Contains(err.Error(), "unsafe") {
		t.Fatalf("unsafe signal error = %v", err)
	}
	unknown := append(append([]byte(nil), encoded[:len(encoded)-1]...), []byte(`,"unknown":true}`)...)
	if err := os.WriteFile(path, unknown, 0o444); err != nil {
		t.Fatalf("write signal with unknown field: %v", err)
	}
	if err := os.Chmod(path, 0o444); err != nil {
		t.Fatalf("chmod signal with unknown field: %v", err)
	}
	if _, err := readLabStaleCompletionSignal(path); err == nil ||
		!strings.Contains(err.Error(), "unknown field") {
		t.Fatalf("unknown signal field error = %v", err)
	}
}

func TestWaitForLabStaleCompletionSignalBindsBothJobVersions(t *testing.T) {
	state := testLabStaleFinalizationState()
	signal := testLabStaleCompletionSignal(state)
	encoded, err := json.Marshal(signal)
	if err != nil {
		t.Fatalf("marshal signal: %v", err)
	}
	path := filepath.Join(t.TempDir(), "go.json")
	if err := os.WriteFile(path, encoded, 0o444); err != nil {
		t.Fatalf("write signal: %v", err)
	}
	got, err := waitForLabStaleCompletionSignal(path, state, time.Now().Add(time.Second))
	if err != nil || got != signal {
		t.Fatalf("wait for signal = %#v error %v", got, err)
	}
	signal.OriginalJobVersion++
	encoded, err = json.Marshal(signal)
	if err != nil {
		t.Fatalf("marshal mismatched signal: %v", err)
	}
	if err := os.Remove(path); err != nil {
		t.Fatalf("remove original signal: %v", err)
	}
	if err := os.WriteFile(path, encoded, 0o444); err != nil {
		t.Fatalf("write mismatched signal: %v", err)
	}
	if _, err := waitForLabStaleCompletionSignal(
		path, state, time.Now().Add(time.Second),
	); err == nil || !strings.Contains(err.Error(), "identity is invalid") {
		t.Fatalf("mismatched Job version signal error = %v", err)
	}
}

func TestReadLabStaleCompletionSignalAcceptsKubernetesProjectedVolumeLink(t *testing.T) {
	state := testLabStaleFinalizationState()
	encoded, err := json.Marshal(testLabStaleCompletionSignal(state))
	if err != nil {
		t.Fatalf("marshal signal: %v", err)
	}
	root := t.TempDir()
	version := "..2026_08_30_12_56_59.123456789"
	versionRoot := filepath.Join(root, version)
	if err := os.Mkdir(versionRoot, 0o755); err != nil {
		t.Fatalf("create projected volume version: %v", err)
	}
	if err := os.WriteFile(filepath.Join(versionRoot, "go.json"), encoded, 0o444); err != nil {
		t.Fatalf("write projected signal: %v", err)
	}
	if err := os.Symlink(version, filepath.Join(root, "..data")); err != nil {
		t.Fatalf("link projected volume data: %v", err)
	}
	path := filepath.Join(root, "go.json")
	if err := os.Symlink(filepath.Join("..data", "go.json"), path); err != nil {
		t.Fatalf("link projected signal: %v", err)
	}

	signal, err := readLabStaleCompletionSignal(path)
	if err != nil || signal.JobID != state.JobID || signal.OriginalJobVersion != state.JobVersion {
		t.Fatalf("read projected signal = %#v error %v", signal, err)
	}
}

func TestReadLabStaleCompletionSignalRejectsProjectedVolumeEscape(t *testing.T) {
	parent := t.TempDir()
	root := filepath.Join(parent, "signal")
	escape := filepath.Join(parent, "escape")
	if err := os.Mkdir(root, 0o755); err != nil {
		t.Fatalf("create signal root: %v", err)
	}
	if err := os.Mkdir(escape, 0o755); err != nil {
		t.Fatalf("create escape root: %v", err)
	}
	if err := os.WriteFile(filepath.Join(escape, "go.json"), []byte(`{}`), 0o444); err != nil {
		t.Fatalf("write escaped signal: %v", err)
	}
	if err := os.Symlink(filepath.Join("..", "escape"), filepath.Join(root, "..data")); err != nil {
		t.Fatalf("link escaped data root: %v", err)
	}
	path := filepath.Join(root, "go.json")
	if err := os.Symlink(filepath.Join("..data", "go.json"), path); err != nil {
		t.Fatalf("link escaped signal: %v", err)
	}
	if _, err := readLabStaleCompletionSignal(path); err == nil ||
		!strings.Contains(err.Error(), "unsafe") {
		t.Fatalf("escaped projected signal error = %v", err)
	}
}

func testLabStaleFinalizationState() labPendingControlOperation {
	artifacts := []labStaleFinalizationArtifact{
		{
			ArtifactID: uuid.New(), UploadID: uuid.New(), Kind: workercontrol.ArtifactKindVideo,
			Ordinal: 0, ObjectKey: "artifacts/video.mp4", ClaimID: uuid.New(),
			VerificationID: uuid.New(),
		},
		{
			ArtifactID: uuid.New(), UploadID: uuid.New(), Kind: workercontrol.ArtifactKindThumbnail,
			Ordinal: 0, ObjectKey: "artifacts/thumbnail.webp", ClaimID: uuid.New(),
			VerificationID: uuid.New(),
		},
	}
	completionID := uuid.New()
	return labPendingControlOperation{
		AttemptID: uuid.New(), JobID: uuid.New(), WorkerID: uuid.New(),
		WorkerEpoch: 7, LeaseFence: 11, LeaseToken: "private-lease-token",
		CompletionID: completionID, HeartbeatSequence: 9,
		GPUHealthSummary: json.RawMessage(`{"healthy":true}`), JobVersion: 13,
		Outputs: []json.RawMessage{
			json.RawMessage(`{"kind":"VIDEO"}`),
			json.RawMessage(`{"kind":"THUMBNAIL"}`),
		},
		Artifacts: artifacts,
		CompletionCandidate: &workercontrol.VisibleCompletionCandidate{
			CompletionID: completionID, ExpectedJobVersion: 13,
			ArtifactIDs: []uuid.UUID{artifacts[0].ArtifactID, artifacts[1].ArtifactID},
		},
	}
}

func testLabStaleCompletionSignal(state labPendingControlOperation) labStaleCompletionSignal {
	return labStaleCompletionSignal{
		Schema: "vela-lab-stale-completion-signal-v1", JobID: state.JobID,
		OriginalAttemptID: state.AttemptID, OriginalFence: state.LeaseFence,
		ReplacementAttemptID: uuid.New(), ReplacementFence: state.LeaseFence + 2,
		OriginalJobVersion: state.JobVersion, ReplacementJobVersion: state.JobVersion + 3,
	}
}

func writeLabStaleCompletionAuthority(t *testing.T, encoded []byte) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "upload--finalization.json")
	if err := os.WriteFile(path, encoded, 0o600); err != nil {
		t.Fatalf("write recovery authority: %v", err)
	}
	if err := os.Chmod(path, 0o600); err != nil {
		t.Fatalf("chmod recovery authority: %v", err)
	}
	return path
}
