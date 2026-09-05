package stageworkeragent_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/materializationauthority"
	"github.com/vivym/vela/internal/stageartifact"
	"github.com/vivym/vela/internal/stageauthority"
	"github.com/vivym/vela/internal/stageworkeragent"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func terminalMaterializationFixture(t *testing.T) *floorCollectorFixture {
	t.Helper()
	f := newAssignmentFloorFixture(t)
	// Materialization is currently single-member. Keep both allocation profiles.
	f.assignment.Authority.Members = f.assignment.Authority.Members[:1]
	f.assignment.RequiredWorkerMemberIds = f.assignment.RequiredWorkerMemberIds[:1]
	f.config.Members = f.config.Members[:1]
	f.config.ExecutionFloor.Bindings = []stageworkeragent.ExecutionFloorBinding{f.config.ExecutionFloor.Bindings[0], f.config.ExecutionFloor.Bindings[2]}
	f.admissionFixture.config.Bindings = nil
	for index := range f.config.ExecutionFloor.Bindings {
		binding := &f.config.ExecutionFloor.Bindings[index]
		binding.Runtime.Members = binding.Runtime.Members[:1]
		f.admissionFixture.config.Bindings = append(f.admissionFixture.config.Bindings, stageworkeragent.AdmissionRuntimeBinding(*binding))
	}
	for _, allocation := range f.disposition.Allocations {
		allocation.Members = allocation.Members[:1]
	}
	f.sign(t, f.assignment)
	digest, err := stageauthority.Digest(f.assignment.Authority)
	if err != nil {
		t.Fatal(err)
	}
	f.disposition.OriginalAuthorityDigest = digest[:]
	f.signDisposition(t)
	return f
}

func terminalMaterializationRecord(t *testing.T, f *floorCollectorFixture, phase string) (stageworkeragent.PendingMaterialization, *materializationauthority.Validator) {
	t.Helper()
	a := f.assignment.Authority
	payload := sha256.Sum256([]byte("owned-scratch"))
	manifest, err := json.Marshal(map[string]any{
		"schema_version": 1, "output_port": "latent", "local_locator": a.StageAttemptId + "/output",
		"content_type": "application/octet-stream", "payload_sha256": hex.EncodeToString(payload[:]), "size_bytes": 13,
		"lineage": stageartifact.LocalOutputLineageV1{
			AttemptID: uuid.MustParse(a.AttemptId), StageRunID: uuid.MustParse(a.StageRunId), StageAttemptID: uuid.MustParse(a.StageAttemptId),
			StageLeaseID: uuid.MustParse(a.StageLeaseId), AttemptFence: a.AttemptFence, StageFence: a.StageFence,
			StageProfileRevisionID: uuid.MustParse(a.StageProfileRevisionId),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(manifest)
	id := uuid.NewString()
	record := stageworkeragent.PendingMaterialization{ID: id, StageAuthority: proto.Clone(a).(*velav1.StageAuthority), LocalReceipt: &velav1.LocalMaterializationReceipt{
		ReceiptId: id, ManifestSha256: digest[:], TotalSizeBytes: 13, SealedAt: a.IssuedAt, OutputManifestJson: manifest,
	}}
	keys := map[string][]byte{"materialization-test-key": bytes.Repeat([]byte{0x8b}, 32)}
	validator, err := materializationauthority.NewValidator(keys, func() time.Time { return time.Unix(0, f.clock.Load()) })
	if err != nil {
		t.Fatal(err)
	}
	if phase == "sealed" || phase == "late-seal" {
		if phase == "late-seal" {
			record.LocalReceipt.SealedAt = timestamppb.New(a.IssuedAt.AsTime().Add(time.Second))
			f.clock.Add(int64(2 * time.Second))
		}
		return record, validator
	}
	signer, err := materializationauthority.NewSigner(keys)
	if err != nil {
		t.Fatal(err)
	}
	stageDigest, err := stageauthority.Digest(a)
	if err != nil {
		t.Fatal(err)
	}
	record.MaterializationAuthority, err = signer.Sign(&velav1.MaterializationAuthority{
		SchemaVersion: 1, StageAuthorityDigest: stageDigest[:], StageMaterializationLeaseId: uuid.NewString(), StageArtifactId: uuid.NewString(),
		ObjectKey: "artifacts/stage/test/latent.bin", ContentType: "application/octet-stream", Sha256: payload[:], SizeBytes: 13,
		LocalReceiptId: id, LocalReceiptDigest: digest[:], SigningKeyId: "materialization-test-key", IssuedAt: a.IssuedAt, ExpiresAt: a.ExpiresAt,
		SourceWorkerInstanceId: a.WorkerInstanceId, SourceWorkerInstanceEpoch: a.WorkerInstanceEpoch,
		SourceWorkerMemberId: a.Members[0].WorkerMemberId, SourceWorkerMemberEpoch: a.Members[0].MemberEpoch,
		SourceSpiffeIdDigest: bytes.Repeat([]byte{0x9b}, 32),
	})
	if err != nil {
		t.Fatal(err)
	}
	switch phase {
	case "published", "commit-pending", "committed", "legacy-commit":
		record.ObjectVersion = "exact-l2-version"
		if phase != "published" {
			record.CommittedAt, record.CommitCommandID = a.IssuedAt.AsTime(), uuid.NewString()
		}
		if phase == "committed" {
			record.ConfirmedDisposition = stageworkeragent.MaterializationCommitted
		}
		if phase == "legacy-commit" {
			record.CommitCommandID = ""
		}
	case "source-lost", "source-lost-pending":
		record.SourceLoss = &stageworkeragent.MaterializationSourceLossEvidence{FailureFingerprint: payload, ConsumedResourceUnits: 1,
			LostAt: a.IssuedAt.AsTime(), RetryAt: a.ExpiresAt.AsTime()}
		record.SourceLossCommandID = uuid.NewString()
		if phase == "source-lost" {
			record.ConfirmedDisposition = stageworkeragent.MaterializationSourceLost
		}
	}
	return record, validator
}

type noTerminalMaterializationIO struct{ calls int }

func (guard *noTerminalMaterializationIO) Exchange(context.Context, *velav1.StageWorkerControlServiceConnectRequest) (*velav1.StageWorkerControlServiceConnectResponse, error) {
	guard.calls++
	return nil, errors.New("terminal recovery reached Control")
}
func (guard *noTerminalMaterializationIO) NextCommand(ctx context.Context) (*velav1.StageWorkerControlServiceConnectResponse, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}
func (guard *noTerminalMaterializationIO) Open(context.Context, stageartifact.LocalOutputManifestV1) (io.ReadCloser, error) {
	guard.calls++
	return nil, errors.New("terminal recovery opened local output")
}
func (guard *noTerminalMaterializationIO) Publish(context.Context, stageartifact.MaterializationLease, io.Reader) (stageartifact.PublishedObject, error) {
	guard.calls++
	return stageartifact.PublishedObject{}, errors.New("terminal recovery published output")
}

func terminalMaterializationStream(t *testing.T, f *floorCollectorFixture, gate *stageworkeragent.FileAssignmentAdmission, journal stageworkeragent.MaterializationJournal, validator *materializationauthority.Validator, guard *noTerminalMaterializationIO) *stageworkeragent.StreamAgent {
	t.Helper()
	stream, err := stageworkeragent.NewDurableStreamAgent(terminalMaterializationConfig(t, f, gate, journal, validator, guard))
	if err != nil {
		t.Fatal(err)
	}
	return stream
}

func terminalMaterializationConfig(t *testing.T, f *floorCollectorFixture, gate *stageworkeragent.FileAssignmentAdmission, journal stageworkeragent.MaterializationJournal, validator *materializationauthority.Validator, guard *noTerminalMaterializationIO) stageworkeragent.DurableStreamConfig {
	t.Helper()
	runtime := f.agent(t)
	retirer, err := stageworkeragent.NewTerminalScratchRetirement(gate, runtime, stageworkeragent.AttemptOwnedFilesystemScratchV1)
	if err != nil {
		t.Fatal(err)
	}
	return stageworkeragent.DurableStreamConfig{
		Runtime: runtime, Control: guard, Admission: gate, TerminalRetirement: retirer,
		Materialization: &stageworkeragent.MaterializationConfig{Validator: validator, Source: guard, Publisher: guard, Journal: journal,
			ScratchRetirer: stageworkeragent.RetainScratchRetirer{}, OutputOwnershipContract: stageworkeragent.AttemptOwnedFilesystemScratchV1,
			SourceLossEvidence: testSourceLossEvidenceProvider()},
	}
}

func TestTerminalMaterializationResumesWithoutInventingCommandResults(t *testing.T) {
	for _, phase := range []string{"sealed", "late-seal", "issued", "published", "commit-pending", "committed", "legacy-commit", "source-lost-pending", "source-lost"} {
		t.Run(phase, func(t *testing.T) {
			f := terminalMaterializationFixture(t)
			if phase == "committed" {
				f.disposition.TerminalState = velav1.StageTerminalState_STAGE_TERMINAL_STATE_SUCCEEDED
				f.signDisposition(t)
			}
			gate := f.open(t)
			group := startFloorCollectorRuntimes(t, f, t.TempDir(), true, false)
			paths := retirementScratch(t, f)
			record, validator := terminalMaterializationRecord(t, f, phase)
			journalRoot := t.TempDir()
			journal, err := stageworkeragent.NewFileMaterializationJournal(journalRoot, 2)
			if err != nil {
				t.Fatal(err)
			}
			if err := journal.Put(t.Context(), record); err != nil {
				t.Fatal(err)
			}
			restore := stageworkeragent.SetTerminalRetirementDirectoryHookForTest(gate, func(int) error { return errors.New("process ended during cleanup") })
			guard := &noTerminalMaterializationIO{}
			stream := terminalMaterializationStream(t, f, gate, journal, validator, guard)
			if result, err := stream.RetireTerminalScratch(t.Context(), f.disposition, nil, drainCollectorTargets(f)); err == nil || result.Phase != stageworkeragent.TerminalRetirementReady {
				t.Fatalf("partial retirement: %+v %v", result, err)
			}
			restore()
			if records, err := journal.List(t.Context()); err != nil || len(records) != 1 || records[0].ConfirmedDisposition != record.ConfirmedDisposition || records[0].CommitCommandID != record.CommitCommandID {
				t.Fatalf("incomplete retirement lost or rewrote recovery: %+v %v", records, err)
			}
			if err := gate.Close(); err != nil {
				t.Fatal(err)
			}
			group.close()
			f.clock.Add(int64(time.Hour))
			gate = f.open(t)
			journal, err = stageworkeragent.NewFileMaterializationJournal(journalRoot, 2)
			if err != nil {
				t.Fatal(err)
			}
			stream = automaticTerminalStream(t, terminalMaterializationConfig(t, f, gate, journal, validator, guard), func(context.Context, *stageauthority.Validator, *velav1.StageAuthority, string, uuid.UUID) (*stageauthority.VerifiedTerminalDisposition, error) {
				t.Fatal("READY recovery requested fresh Control history")
				return nil, errors.New("Control offline")
			})
			result, err := stream.ResumeMaterializations(t.Context())
			if err != nil || result.TerminalRecordsRetired != 1 || result.Committed || result.SourceLostReported || result.L2Published || guard.calls != 0 {
				t.Fatalf("terminal recovery invented command outcome or I/O: %+v %v calls=%d", result, err, guard.calls)
			}
			assertRetirementScratch(t, paths, false)
			if records, err := journal.List(t.Context()); err != nil || len(records) != 0 {
				t.Fatalf("obsolete materialization retained: %+v %v", records, err)
			}
			if state := admissionSnapshot(t, gate); state.Retirements[0].Phase != stageworkeragent.TerminalRetirementRetired || state.Floor != f.disposition.Cutoff {
				t.Fatal("reconciliation cleared durable retirement proof")
			}
			if result, err := stream.ResumeMaterializations(t.Context()); err != nil || result.TerminalRecordsRetired != 0 {
				t.Fatalf("replay: %+v %v", result, err)
			}
		})
	}
}

func TestTerminalMaterializationPreflightsRecordsBeforeScratchDeletion(t *testing.T) {
	for _, fault := range []string{"signature", "lineage", "locator", "size", "sealed-at", "allocation", "materialization-signature", "confirmed-commit"} {
		t.Run(fault, func(t *testing.T) {
			f := terminalMaterializationFixture(t)
			gate := f.open(t)
			startFloorCollectorRuntimes(t, f, t.TempDir(), true, false)
			paths := retirementScratch(t, f)
			record, validator := terminalMaterializationRecord(t, f, "issued")
			switch fault {
			case "signature":
				record.StageAuthority.Signature[0] ^= 1
			case "lineage", "locator":
				var document map[string]any
				if err := json.Unmarshal(record.LocalReceipt.OutputManifestJson, &document); err != nil {
					t.Fatal(err)
				}
				if fault == "lineage" {
					document["lineage"].(map[string]any)["attempt_id"] = uuid.NewString()
				} else {
					document["local_locator"] = "unrelated/keep"
				}
				wire, err := json.Marshal(document)
				if err != nil {
					t.Fatal(err)
				}
				digest := sha256.Sum256(wire)
				record.LocalReceipt.OutputManifestJson, record.LocalReceipt.ManifestSha256 = wire, digest[:]
			case "size":
				record.LocalReceipt.TotalSizeBytes++
			case "sealed-at":
				record.LocalReceipt.SealedAt = record.StageAuthority.ExpiresAt
			case "allocation":
				record.StageAuthority.StageAllocationId = uuid.NewString()
				var err error
				record.StageAuthority, err = f.signer.Sign(record.StageAuthority)
				if err != nil {
					t.Fatal(err)
				}
			case "materialization-signature":
				record.MaterializationAuthority.Token[0] ^= 1
			case "confirmed-commit":
				record, validator = terminalMaterializationRecord(t, f, "committed")
			}
			journal, err := stageworkeragent.NewMemoryMaterializationJournal(2)
			if err != nil {
				t.Fatal(err)
			}
			if err := journal.Put(t.Context(), record); err != nil {
				t.Fatal(err)
			}
			guard := &noTerminalMaterializationIO{}
			stream := terminalMaterializationStream(t, f, gate, journal, validator, guard)
			if _, err := stream.RetireTerminalScratch(t.Context(), f.disposition, nil, drainCollectorTargets(f)); err == nil {
				t.Fatal("invalid materialization enabled retirement")
			}
			if state := admissionSnapshot(t, gate); len(state.Retirements) != 0 || state.Floor != 0 || guard.calls != 0 {
				t.Fatal("invalid materialization crossed retirement preflight")
			}
			assertRetirementScratch(t, paths, true)
		})
	}
}

func TestTerminalMaterializationIntentBlocksProductionDiscovery(t *testing.T) {
	f := terminalMaterializationFixture(t)
	gate := f.open(t)
	group := startFloorCollectorRuntimes(t, f, t.TempDir(), true, false)
	paths := retirementScratch(t, f)
	// An input invocation lost across restart remains unproven.
	beginAdmission(t, gate, f.assignment, f.acquireID).Release()
	record, validator := terminalMaterializationRecord(t, f, "sealed")
	journal, err := stageworkeragent.NewMemoryMaterializationJournal(2)
	if err != nil {
		t.Fatal(err)
	}
	if err := journal.Put(t.Context(), record); err != nil {
		t.Fatal(err)
	}
	guard := &noTerminalMaterializationIO{}
	stream := terminalMaterializationStream(t, f, gate, journal, validator, guard)
	if _, err := stream.RetireTerminalScratch(t.Context(), f.disposition, nil, drainCollectorTargets(f)); !errors.Is(err, stageworkeragent.ErrInputWritersUnproven) {
		t.Fatalf("unknown input writer: %v", err)
	}
	if state := admissionSnapshot(t, gate); state.Retirements[0].Phase != stageworkeragent.TerminalRetirementIntent {
		t.Fatal("missing durable intent")
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	var observed error
	production, err := stageworkeragent.NewProductionAgent(stageworkeragent.ProductionConfig{
		Control: guard, Runtime: group.clients[0], Stream: stream, RuntimeIdentity: runtimeIdentityFromAuthority(f.assignment.Authority),
		Devices: f.assignment.Authority.Devices, Members: f.assignment.Authority.Members,
		CapacityVector: f.assignment.Authority.CapacityVector, CapacityTTL: time.Minute,
		HeartbeatInterval: time.Second, RetryMinimum: time.Millisecond, RetryMaximum: time.Second,
		ObservationSequenceSource: &capacitySequenceSource{values: []int64{1}}, Now: time.Now,
		RetryObserver: func(operation string, cause error) {
			if operation == "resume-materialization" {
				observed = cause
			}
		},
		Wait: func(context.Context, time.Duration) error { cancel(); return nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := production.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if !errors.Is(observed, stageworkeragent.ErrScratchRetirementUnproven) || guard.calls != 0 {
		t.Fatalf("INTENT crossed recovery into publication/discovery: observed=%v calls=%d", observed, guard.calls)
	}
	assertRetirementScratch(t, paths, true)
	if records, err := journal.List(t.Context()); err != nil || len(records) != 1 {
		t.Fatalf("INTENT discarded recovery: %+v %v", records, err)
	}
}

type terminalDeleteFaultJournal struct {
	stageworkeragent.MaterializationJournal
	failure error
}

func (journal *terminalDeleteFaultJournal) Delete(ctx context.Context, id string) error {
	if journal.failure != nil {
		return journal.failure
	}
	return journal.MaterializationJournal.Delete(ctx, id)
}

func TestTerminalMaterializationJournalFailureKeepsRetirementProofAndOtherStage(t *testing.T) {
	f := terminalMaterializationFixture(t)
	gate := f.open(t)
	group := startFloorCollectorRuntimes(t, f, t.TempDir(), true, false)
	paths := retirementScratch(t, f)
	record, validator := terminalMaterializationRecord(t, f, "sealed")
	base, err := stageworkeragent.NewMemoryMaterializationJournal(3)
	if err != nil {
		t.Fatal(err)
	}
	if err := base.Put(t.Context(), record); err != nil {
		t.Fatal(err)
	}
	other := record
	other.StageAuthority = proto.Clone(record.StageAuthority).(*velav1.StageAuthority)
	other.StageAuthority.StageRunId = uuid.NewString()
	other.LocalReceipt = proto.Clone(record.LocalReceipt).(*velav1.LocalMaterializationReceipt)
	other.ID, other.LocalReceipt.ReceiptId = "unrelated-receipt", "unrelated-receipt"
	if err := base.Put(t.Context(), other); err != nil {
		t.Fatal(err)
	}
	deleteErr := errors.New("journal deletion failed")
	journal := &terminalDeleteFaultJournal{MaterializationJournal: base, failure: deleteErr}
	guard := &noTerminalMaterializationIO{}
	stream := terminalMaterializationStream(t, f, gate, journal, validator, guard)
	if snapshot, err := stream.RetireTerminalScratch(t.Context(), f.disposition, nil, drainCollectorTargets(f)); !errors.Is(err, deleteErr) || snapshot.Phase != stageworkeragent.TerminalRetirementRetired {
		t.Fatalf("journal failure lost completed retirement: %+v %v", snapshot, err)
	}
	assertRetirementScratch(t, paths, false)
	if records, err := base.List(t.Context()); err != nil || len(records) != 2 {
		t.Fatalf("failed delete changed journals: %+v %v", records, err)
	}
	group.close()
	journal.failure = nil
	// The unrelated record deliberately cannot proceed through ordinary replay.
	// Recovery must still retire the first record without changing the other one.
	result, err := stream.ResumeMaterializations(t.Context())
	if err == nil || result.TerminalRecordsRetired != 1 || result.Committed || result.SourceLostReported {
		t.Fatalf("partial reconciliation: %+v %v", result, err)
	}
	if records, err := base.List(t.Context()); err != nil || len(records) != 1 || records[0].ID != other.ID ||
		!proto.Equal(records[0].StageAuthority, other.StageAuthority) || !proto.Equal(records[0].LocalReceipt, other.LocalReceipt) {
		t.Fatalf("reconciliation rewrote unrelated recovery: %+v %v", records, err)
	}
	if state := admissionSnapshot(t, gate); state.Retirements[0].Phase != stageworkeragent.TerminalRetirementRetired {
		t.Fatal("reconciliation removed permanent admission restriction")
	}
}

type blockedTerminalOutputSource struct {
	entered chan struct{}
	release chan struct{}
}

func (source blockedTerminalOutputSource) Open(ctx context.Context, _ stageartifact.LocalOutputManifestV1) (io.ReadCloser, error) {
	close(source.entered)
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-source.release:
		return nil, errors.New("output read ended")
	}
}

func TestTerminalMaterializationSerializesWithOutputReader(t *testing.T) {
	f := terminalMaterializationFixture(t)
	gate := f.open(t)
	startFloorCollectorRuntimes(t, f, t.TempDir(), true, false)
	paths := retirementScratch(t, f)
	record, validator := terminalMaterializationRecord(t, f, "issued")
	// Ordinary replay requires retained admission identity before reading output.
	completeAdmissionInputs(t, beginAdmission(t, gate, f.assignment, f.acquireID))
	journal, err := stageworkeragent.NewMemoryMaterializationJournal(2)
	if err != nil {
		t.Fatal(err)
	}
	if err := journal.Put(t.Context(), record); err != nil {
		t.Fatal(err)
	}
	guard := &noTerminalMaterializationIO{}
	config := terminalMaterializationConfig(t, f, gate, journal, validator, guard)
	source := blockedTerminalOutputSource{entered: make(chan struct{}), release: make(chan struct{})}
	config.Materialization.Source = source
	stream, err := stageworkeragent.NewDurableStreamAgent(config)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	reader := make(chan error, 1)
	go func() { _, err := stream.ResumeMaterializations(ctx); reader <- err }()
	select {
	case <-source.entered:
	case err := <-reader:
		t.Fatalf("replay never opened output: %v", err)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	retired := make(chan error, 1)
	go func() {
		_, err := stream.RetireTerminalScratch(ctx, f.disposition, nil, drainCollectorTargets(f))
		retired <- err
	}()
	select {
	case err := <-retired:
		t.Fatalf("retirement raced output reader: %v", err)
	case <-time.After(25 * time.Millisecond):
	}
	assertRetirementScratch(t, paths, true)
	if state := admissionSnapshot(t, gate); len(state.Retirements) != 0 {
		t.Fatal("retirement began while materialization held output")
	}
	close(source.release)
	if err := <-reader; err == nil {
		t.Fatal("injected output error was lost")
	}
	if err := <-retired; err != nil {
		t.Fatal(err)
	}
	assertRetirementScratch(t, paths, false)
	if guard.calls != 0 {
		t.Fatal("retirement issued a new materialization command")
	}
}

func TestTerminalMaterializationRequiresSameStreamOwners(t *testing.T) {
	for _, fault := range []string{"admission", "runtime", "materialization", "output-contract"} {
		t.Run(fault, func(t *testing.T) {
			f := terminalMaterializationFixture(t)
			gate := f.open(t)
			_, validator := terminalMaterializationRecord(t, f, "sealed")
			journal, err := stageworkeragent.NewMemoryMaterializationJournal(2)
			if err != nil {
				t.Fatal(err)
			}
			config := terminalMaterializationConfig(t, f, gate, journal, validator, &noTerminalMaterializationIO{})
			switch fault {
			case "admission":
				other := terminalMaterializationFixture(t)
				config.Admission = other.open(t)
			case "runtime":
				config.Runtime = f.agent(t)
			case "materialization":
				config.Materialization = nil
			case "output-contract":
				config.Materialization.OutputOwnershipContract = ""
			}
			if _, err := stageworkeragent.NewDurableStreamAgent(config); err == nil {
				t.Fatal("retirement accepted different Stream ownership")
			}
		})
	}
}
