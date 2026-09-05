package stageworkeragent_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/materializationauthority"
	"github.com/vivym/vela/internal/stageartifact"
	"github.com/vivym/vela/internal/stageworkeragent"
)

func TestFilesystemScratchRetirementRejectsUnconfirmedAndEscapingTargets(t *testing.T) {
	for _, mutation := range []string{"unconfirmed", "wrong digest", "other attempt", "escape", "output symlink", "input symlink", "changed output"} {
		t.Run(mutation, func(t *testing.T) {
			inputRoot, outputRoot := t.TempDir(), t.TempDir()
			payload := []byte("exact committed scratch payload")
			manifest := scratchManifest(payload)
			committed := stageartifact.Artifact{ID: uuid.New(), ObjectKey: "artifacts/stage/committed", ObjectVersion: "v1",
				SHA256: manifest.PayloadSHA256, SizeBytes: manifest.SizeBytes, CommittedAt: time.Now()}
			inputPath := filepath.Join(inputRoot, "stage-runs", manifest.Lineage.StageRunID.String(), "inputs", "input.bin")
			outputPath := filepath.Join(outputRoot, filepath.FromSlash(manifest.LocalLocator))
			protectedPath := filepath.Join(t.TempDir(), "must-remain.bin")
			for _, path := range []string{inputPath, outputPath, protectedPath} {
				writeScratchFixture(t, path, payload)
			}
			retirer, err := stageworkeragent.NewFilesystemScratchRetirer(inputRoot, outputRoot)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = retirer.Close() })
			switch mutation {
			case "unconfirmed":
				committed.CommittedAt = time.Time{}
			case "wrong digest":
				committed.SHA256[0] ^= 1
			case "other attempt":
				manifest.LocalLocator = uuid.NewString() + "/output.bin"
			case "escape":
				manifest.LocalLocator = manifest.Lineage.StageAttemptID.String() + "/../../must-remain.bin"
			case "output symlink":
				if err := os.Remove(outputPath); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(protectedPath, outputPath); err != nil {
					t.Fatal(err)
				}
			case "input symlink":
				if err := os.Remove(inputPath); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(protectedPath, inputPath); err != nil {
					t.Fatal(err)
				}
			case "changed output":
				writeScratchFixture(t, outputPath, []byte("different source"))
			}
			if err := retirer.RetireCommitted(context.Background(), manifest, committed); err == nil {
				t.Fatal("unsafe scratch retirement was accepted")
			}
			assertScratchFiles(t, true, inputPath, outputPath, protectedPath)
		})
	}
}

func TestFilesystemScratchRetirementRemainsBoundToOriginalRoots(t *testing.T) {
	inputRoot, outputRoot := t.TempDir(), t.TempDir()
	payload := []byte("committed output")
	manifest := scratchManifest(payload)
	committed := stageartifact.Artifact{ID: uuid.New(), ObjectKey: "artifacts/stage/committed", ObjectVersion: "v1",
		SHA256: manifest.PayloadSHA256, SizeBytes: manifest.SizeBytes, CommittedAt: time.Now()}
	outputPath := filepath.Join(outputRoot, filepath.FromSlash(manifest.LocalLocator))
	writeScratchFixture(t, outputPath, payload)
	retirer, err := stageworkeragent.NewFilesystemScratchRetirer(inputRoot, outputRoot)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = retirer.Close() })
	moved := outputRoot + ".original"
	if err := os.Rename(outputRoot, moved); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(moved) })
	writeScratchFixture(t, outputPath, payload)
	if err := retirer.RetireCommitted(context.Background(), manifest, committed); err != nil {
		t.Fatal(err)
	}
	assertScratchFiles(t, true, outputPath)
	assertScratchFiles(t, false, filepath.Join(moved, filepath.FromSlash(manifest.LocalLocator)))
	link := filepath.Join(t.TempDir(), "output-root-link")
	if err := os.Symlink(outputRoot, link); err != nil {
		t.Fatal(err)
	}
	if invalid, err := stageworkeragent.NewFilesystemScratchRetirer(inputRoot, link); err == nil {
		_ = invalid.Close()
		t.Fatal("symlink retirement root was accepted")
	}
}

func scratchManifest(payload []byte) stageartifact.LocalOutputManifestV1 {
	lineage := stageartifact.LocalOutputLineageV1{AttemptID: uuid.New(), StageRunID: uuid.New(), StageAttemptID: uuid.New(),
		StageLeaseID: uuid.New(), StageProfileRevisionID: uuid.New(), AttemptFence: 1, StageFence: 1}
	return stageartifact.LocalOutputManifestV1{SchemaVersion: 1, OutputPort: "latent", LocalLocator: lineage.StageAttemptID.String() + "/latent.bin",
		ContentType: "application/octet-stream", PayloadSHA256: sha256.Sum256(payload), SizeBytes: int64(len(payload)), Lineage: lineage}
}

func TestStreamMaterializationRetiresScratchOnlyAfterCommitAndReplaysCleanup(t *testing.T) {
	fixture := newSingleMemberMaterializationFixture(t)
	var document map[string]any
	if err := json.Unmarshal(fixture.manifest, &document); err != nil {
		t.Fatal(err)
	}
	locator := fixture.authority.GetStageAttemptId() + "/latent.bin"
	document["local_locator"] = locator
	var err error
	fixture.manifest, err = json.Marshal(document)
	if err != nil {
		t.Fatal(err)
	}
	outputPath := filepath.Join(fixture.localRoot, filepath.FromSlash(locator))
	writeScratchFixture(t, outputPath, fixture.payload)
	if err := os.Remove(filepath.Join(fixture.localRoot, "dit.bin")); err != nil {
		t.Fatal(err)
	}
	inputRoot := t.TempDir()
	inputPath := filepath.Join(inputRoot, "stage-runs", fixture.authority.GetStageRunId(), "inputs", "consumed.bin")
	otherInput := filepath.Join(inputRoot, "stage-runs", "another-active-run", "inputs", "active.bin")
	otherOutput := filepath.Join(fixture.localRoot, "another-materializing-attempt", "output.bin")
	for _, path := range []string{inputPath, otherInput, otherOutput} {
		writeScratchFixture(t, path, fixture.payload)
	}
	retirer, err := stageworkeragent.NewFilesystemScratchRetirer(inputRoot, fixture.localRoot)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = retirer.Close() })
	cleanup := &scratchRetirementResponseLoss{retirer: retirer, failures: 1}
	runtimeAgent, err := stageworkeragent.New(stageworkeragent.Config{Members: []stageworkeragent.RuntimeMember{{ID: fixture.memberID, Client: fixture.client}}})
	if err != nil {
		t.Fatal(err)
	}
	control := newMaterializingStreamControl(t, fixture.authority)
	control.commitFailures = 1
	publisher := &outageOncePublisher{objectVersion: "durable-exact-version"}
	source, err := stageartifact.NewFilesystemLocalOutputSource(fixture.localRoot)
	if err != nil {
		t.Fatal(err)
	}
	journalRoot := t.TempDir()
	journal, err := stageworkeragent.NewFileMaterializationJournal(journalRoot, 4)
	if err != nil {
		t.Fatal(err)
	}
	config := stageworkeragent.MaterializationConfig{Validator: control.validator, Source: source,
		OutputOwnershipContract: stageworkeragent.AttemptOwnedFilesystemScratchV1,
		Publisher:               publisher, Journal: &confirmationFailJournal{MaterializationJournal: journal, failures: 1}, ScratchRetirer: cleanup, SourceLossEvidence: testSourceLossEvidenceProvider()}
	agent, err := stageworkeragent.NewMaterializingStreamAgent(runtimeAgent, control, config)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if _, err := agent.ExecuteAssignment(ctx, fixture.assignment); err != nil {
		t.Fatal(err)
	}
	fixture.backend.MarkOutputReadyWithSize(fixture.manifest, int64(len(fixture.payload)))
	if result, err := agent.SealAndMaterialize(ctx); err == nil || result.Committed || cleanup.calls != 0 {
		t.Fatalf("unconfirmed COMMIT retired scratch: %+v err=%v cleanup=%d", result, err, cleanup.calls)
	}
	assertScratchFiles(t, true, inputPath, outputPath, otherInput, otherOutput)
	if records, err := journal.List(ctx); err != nil || len(records) != 1 {
		t.Fatalf("unconfirmed COMMIT lost recovery record: %d %v", len(records), err)
	}
	if result, err := agent.ResumeMaterializations(ctx); err == nil || result.Committed || cleanup.calls != 0 {
		t.Fatalf("confirmation persistence failure allowed cleanup: %+v err=%v calls=%d", result, err, cleanup.calls)
	}
	assertScratchFiles(t, true, inputPath, outputPath, otherInput, otherOutput)
	if result, err := agent.ResumeMaterializations(ctx); err == nil || result.Committed {
		t.Fatalf("cleanup response-loss was not retained: %+v %v", result, err)
	}
	assertScratchFiles(t, false, inputPath, outputPath)
	assertScratchFiles(t, true, otherInput, otherOutput)
	if records, err := journal.List(ctx); err != nil || len(records) != 1 {
		t.Fatalf("cleanup failure lost recovery record: %d %v", len(records), err)
	} else if records[0].ConfirmedDisposition != stageworkeragent.MaterializationCommitted {
		t.Fatalf("cleanup ran without durable COMMIT confirmation: %+v", records[0])
	}
	expireScratchControl(t, control)
	config.Validator = control.validator
	config.Journal, err = stageworkeragent.NewFileMaterializationJournal(journalRoot, 4)
	if err != nil {
		t.Fatal(err)
	}
	restarted, err := stageworkeragent.NewMaterializingStreamAgent(runtimeAgent, control, config)
	if err != nil {
		t.Fatal(err)
	}
	if result, err := restarted.ResumeMaterializations(ctx); err != nil || !result.Committed {
		t.Fatalf("restart did not retry exact cleanup: %+v %v", result, err)
	}
	if records, err := journal.List(ctx); err != nil || len(records) != 0 {
		t.Fatalf("successful cleanup left recovery record: %d %v", len(records), err)
	}
	if _, err := runtimeAgent.SealOutput(ctx, fixture.authority); err != nil {
		t.Fatalf("old sealed receipt replay failed after scratch retirement: %v", err)
	}
	if fixture.countingBackend.sealCalls != 1 || publisher.calls != 1 || cleanup.calls != 2 || control.commitCalls != 3 {
		t.Fatalf("cleanup reran physical work: seals=%d publishes=%d cleanups=%d",
			fixture.countingBackend.sealCalls, publisher.calls, cleanup.calls)
	}
}

func TestFilesystemScratchOwnershipContractRejectsGenericLocatorBeforeAuthority(t *testing.T) {
	fixture := newSingleMemberMaterializationFixture(t)
	if _, err := stageartifact.ParseLocalOutputManifestV1(fixture.manifest); err != nil {
		t.Fatalf("generic v1 locator must remain valid: %v", err)
	}
	retirer, err := stageworkeragent.NewFilesystemScratchRetirer(t.TempDir(), fixture.localRoot)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = retirer.Close() })
	runtimeAgent, err := stageworkeragent.New(stageworkeragent.Config{Members: []stageworkeragent.RuntimeMember{{ID: fixture.memberID, Client: fixture.client}}})
	if err != nil {
		t.Fatal(err)
	}
	control := newMaterializingStreamControl(t, fixture.authority)
	publisher := &outageOncePublisher{objectVersion: "must-not-publish"}
	source, err := stageartifact.NewFilesystemLocalOutputSource(fixture.localRoot)
	if err != nil {
		t.Fatal(err)
	}
	journal, err := stageworkeragent.NewFileMaterializationJournal(t.TempDir(), 4)
	if err != nil {
		t.Fatal(err)
	}
	config := stageworkeragent.MaterializationConfig{Validator: control.validator, Source: source,
		Publisher: publisher, Journal: journal, ScratchRetirer: retirer, SourceLossEvidence: testSourceLossEvidenceProvider()}
	if _, err := stageworkeragent.NewMaterializingStreamAgent(runtimeAgent, control, config); err == nil {
		t.Fatal("scratch retirement silently adopted a narrower output ownership contract")
	}
	config.OutputOwnershipContract = stageworkeragent.AttemptOwnedFilesystemScratchV1
	agent, err := stageworkeragent.NewMaterializingStreamAgent(runtimeAgent, control, config)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if _, err := agent.ExecuteAssignment(ctx, fixture.assignment); err != nil {
		t.Fatal(err)
	}
	fixture.backend.MarkOutputReadyWithSize(fixture.manifest, int64(len(fixture.payload)))
	if result, err := agent.SealAndMaterialize(ctx); err == nil || result.Committed || result.L2Published {
		t.Fatalf("nonconforming production output advanced: %+v %v", result, err)
	}
	if control.sealCalls != 0 || control.commitCalls != 0 || publisher.calls != 0 {
		t.Fatalf("ownership was checked after authority/publication: seals=%d commits=%d publishes=%d", control.sealCalls, control.commitCalls, publisher.calls)
	}
	assertScratchFiles(t, true, filepath.Join(fixture.localRoot, "dit.bin"))
	if records, err := journal.List(ctx); err != nil || len(records) != 1 || records[0].MaterializationAuthority != nil {
		t.Fatalf("nonconforming sealed output lost its recovery record: %+v %v", records, err)
	}
}

func TestStreamSourceLostRetirementPreservesRetryInputsAndSurvivesExpiredAuthority(t *testing.T) {
	fixture := newSingleMemberMaterializationFixture(t)
	var document map[string]any
	if err := json.Unmarshal(fixture.manifest, &document); err != nil {
		t.Fatal(err)
	}
	locator := fixture.authority.GetStageAttemptId() + "/latent.bin"
	document["local_locator"] = locator
	var err error
	fixture.manifest, err = json.Marshal(document)
	if err != nil {
		t.Fatal(err)
	}
	outputPath := filepath.Join(fixture.localRoot, filepath.FromSlash(locator))
	writeScratchFixture(t, outputPath, []byte("corrupt source"))
	if err := os.Remove(filepath.Join(fixture.localRoot, "dit.bin")); err != nil {
		t.Fatal(err)
	}
	inputRoot := t.TempDir()
	inputPath := filepath.Join(inputRoot, "stage-runs", fixture.authority.GetStageRunId(), "inputs", "retry.bin")
	otherOutput := filepath.Join(fixture.localRoot, "another-active-attempt", "output.bin")
	for _, file := range []string{inputPath, otherOutput} {
		writeScratchFixture(t, file, fixture.payload)
	}
	retirer, err := stageworkeragent.NewFilesystemScratchRetirer(inputRoot, fixture.localRoot)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = retirer.Close() })
	cleanup := &sourceLossRetirementFaults{retirer: retirer, beforeFailures: 1, afterFailures: 1}
	runtimeAgent, err := stageworkeragent.New(stageworkeragent.Config{Members: []stageworkeragent.RuntimeMember{{ID: fixture.memberID, Client: fixture.client}}})
	if err != nil {
		t.Fatal(err)
	}
	control := newMaterializingStreamControl(t, fixture.authority)
	control.sourceLossFailures = 1
	publisher := &outageOncePublisher{objectVersion: "must-not-publish"}
	source, err := stageartifact.NewFilesystemLocalOutputSource(fixture.localRoot)
	if err != nil {
		t.Fatal(err)
	}
	journalRoot := t.TempDir()
	journal, err := stageworkeragent.NewFileMaterializationJournal(journalRoot, 4)
	if err != nil {
		t.Fatal(err)
	}
	config := stageworkeragent.MaterializationConfig{Validator: control.validator, Source: source,
		OutputOwnershipContract: stageworkeragent.AttemptOwnedFilesystemScratchV1,
		Publisher:               publisher, Journal: &confirmationFailJournal{MaterializationJournal: journal, failures: 1}, ScratchRetirer: cleanup, SourceLossEvidence: testSourceLossEvidenceProvider()}
	agent, err := stageworkeragent.NewMaterializingStreamAgent(runtimeAgent, control, config)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if _, err := agent.ExecuteAssignment(ctx, fixture.assignment); err != nil {
		t.Fatal(err)
	}
	fixture.backend.MarkOutputReadyWithSize(fixture.manifest, int64(len(fixture.payload)))
	if result, err := agent.SealAndMaterialize(ctx); err == nil || result.SourceLostReported || cleanup.calls != 0 {
		t.Fatalf("unconfirmed SOURCE_LOST retired scratch: %+v err=%v cleanup=%d", result, err, cleanup.calls)
	}
	assertScratchFiles(t, true, inputPath, outputPath, otherOutput)
	// A source that reappears must not change a durable SOURCE_LOST request back
	// into publication under the old materialization authority.
	writeScratchFixture(t, outputPath, fixture.payload)
	if result, err := agent.ResumeMaterializations(ctx); err == nil || result.SourceLostReported || cleanup.calls != 0 {
		t.Fatalf("source-loss confirmation persistence failure allowed cleanup: %+v err=%v calls=%d", result, err, cleanup.calls)
	}
	assertScratchFiles(t, true, inputPath, outputPath, otherOutput)
	if result, err := agent.ResumeMaterializations(ctx); err == nil || result.SourceLostReported || cleanup.calls != 1 {
		t.Fatalf("retirement failure was not retained: %+v err=%v cleanup=%d", result, err, cleanup.calls)
	}
	assertScratchFiles(t, true, inputPath, outputPath, otherOutput)
	if records, err := journal.List(ctx); err != nil || len(records) != 1 {
		t.Fatalf("retirement failure lost journal: %d %v", len(records), err)
	} else if records[0].ConfirmedDisposition != stageworkeragent.MaterializationSourceLost {
		t.Fatalf("retirement ran without durable source-loss confirmation: %+v", records[0])
	}
	expireScratchControl(t, control)
	config.Validator = control.validator
	for restart := range 2 {
		config.Journal, err = stageworkeragent.NewFileMaterializationJournal(journalRoot, 4)
		if err != nil {
			t.Fatal(err)
		}
		restarted, err := stageworkeragent.NewMaterializingStreamAgent(runtimeAgent, control, config)
		if err != nil {
			t.Fatal(err)
		}
		result, resumeErr := restarted.ResumeMaterializations(ctx)
		if restart == 0 && (resumeErr == nil || result.SourceLostReported) {
			t.Fatalf("retirement response loss cleared journal: %+v %v", result, resumeErr)
		}
		if restart == 1 && (!errors.Is(resumeErr, stageworkeragent.ErrMaterializationSourceLostReported) || !result.SourceLostReported) {
			t.Fatalf("confirmed source loss did not finish after token expiry: %+v %v", result, resumeErr)
		}
		assertScratchFiles(t, false, outputPath)
		assertScratchFiles(t, true, inputPath, otherOutput)
	}
	if records, err := journal.List(ctx); err != nil || len(records) != 0 {
		t.Fatalf("successful source-loss retirement retained journal: %d %v", len(records), err)
	}
	if publisher.calls != 0 || fixture.countingBackend.sealCalls != 1 || control.sourceLossCalls != 3 || control.commitCalls != 0 || cleanup.calls != 3 {
		t.Fatalf("retirement repeated authority or physical work: publishes=%d seals=%d reports=%d commits=%d cleanups=%d",
			publisher.calls, fixture.countingBackend.sealCalls, control.sourceLossCalls, control.commitCalls, cleanup.calls)
	}
}

type confirmationFailJournal struct {
	stageworkeragent.MaterializationJournal
	failures int
}

func (journal *confirmationFailJournal) Put(ctx context.Context, record stageworkeragent.PendingMaterialization) error {
	if record.ConfirmedDisposition != "" && journal.failures > 0 {
		journal.failures--
		return errors.New("injected confirmation persistence failure")
	}
	return journal.MaterializationJournal.Put(ctx, record)
}

func expireScratchControl(t *testing.T, control *materializingStreamControl) {
	t.Helper()
	afterExpiry := control.materialization.GetExpiresAt().AsTime().Add(time.Hour)
	validator, err := materializationauthority.NewValidator(map[string][]byte{"materialization-test-key": bytes.Repeat([]byte{0x8b}, 32)}, func() time.Time { return afterExpiry })
	if err != nil {
		t.Fatal(err)
	}
	if _, err := validator.Validate(control.materialization); !errors.Is(err, materializationauthority.ErrStale) {
		t.Fatalf("restart test requires an expired MaterializationAuthority: %v", err)
	}
	control.validator = validator
}

type sourceLossRetirementFaults struct {
	retirer                       stageworkeragent.ScratchRetirer
	beforeFailures, afterFailures int
	calls                         int
}

func (retirer *sourceLossRetirementFaults) RetireCommitted(ctx context.Context, manifest stageartifact.LocalOutputManifestV1, committed stageartifact.Artifact) error {
	return retirer.retirer.RetireCommitted(ctx, manifest, committed)
}

func (retirer *sourceLossRetirementFaults) RetireSourceLost(ctx context.Context, manifest stageartifact.LocalOutputManifestV1, reported stageworkeragent.MaterializationSourceLossEvidence) error {
	retirer.calls++
	if retirer.beforeFailures > 0 {
		retirer.beforeFailures--
		return errors.New("injected source-loss retirement failure")
	}
	if err := retirer.retirer.RetireSourceLost(ctx, manifest, reported); err != nil {
		return err
	}
	if retirer.afterFailures > 0 {
		retirer.afterFailures--
		return errors.New("injected source-loss retirement response loss")
	}
	return nil
}

type scratchRetirementResponseLoss struct {
	retirer  stageworkeragent.ScratchRetirer
	failures int
	calls    int
}

func (retirer *scratchRetirementResponseLoss) RetireSourceLost(ctx context.Context, manifest stageartifact.LocalOutputManifestV1, reported stageworkeragent.MaterializationSourceLossEvidence) error {
	return retirer.retirer.RetireSourceLost(ctx, manifest, reported)
}

func (retirer *scratchRetirementResponseLoss) RetireCommitted(ctx context.Context, manifest stageartifact.LocalOutputManifestV1, committed stageartifact.Artifact) error {
	retirer.calls++
	if err := retirer.retirer.RetireCommitted(ctx, manifest, committed); err != nil {
		return err
	}
	if retirer.failures > 0 {
		retirer.failures--
		return errors.New("injected scratch cleanup response loss")
	}
	return nil
}

func writeScratchFixture(t *testing.T, path string, payload []byte) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, payload, 0o600); err != nil {
		t.Fatal(err)
	}
}

func assertScratchFiles(t *testing.T, exists bool, paths ...string) {
	t.Helper()
	for _, path := range paths {
		_, err := os.Stat(path)
		if (exists && err != nil) || (!exists && !errors.Is(err, os.ErrNotExist)) {
			t.Fatalf("scratch %s exists=%t: %v", path, exists, err)
		}
	}
}
