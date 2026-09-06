package stageworkeragent_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/stageauthority"
	"github.com/vivym/vela/internal/stageworkeragent"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type terminalHistoryReaderFunc func(context.Context, *stageauthority.Validator, *velav1.StageAuthority, string, uuid.UUID) (*stageauthority.VerifiedTerminalDisposition, error)

func (read terminalHistoryReaderFunc) ReadTerminalDisposition(ctx context.Context, validator *stageauthority.Validator, authority *velav1.StageAuthority, member string, acquire uuid.UUID) (*stageauthority.VerifiedTerminalDisposition, error) {
	return read(ctx, validator, authority, member, acquire)
}

func automaticTerminalStream(t *testing.T, config stageworkeragent.DurableStreamConfig, reader terminalHistoryReaderFunc) *stageworkeragent.StreamAgent {
	t.Helper()
	config.TerminalHistory = reader
	stream, err := stageworkeragent.NewDurableStreamAgent(config)
	if err != nil {
		t.Fatal(err)
	}
	return stream
}

func fixtureTerminalResponse(t *testing.T, f *floorCollectorFixture) *stageauthority.VerifiedTerminalDisposition {
	t.Helper()
	verified, err := f.admissionFixture.config.Validator.ValidateTerminalDispositionEnvelope(f.disposition)
	if err != nil {
		t.Fatal(err)
	}
	return &verified
}

func terminalRecoveryJournal(t *testing.T, records ...stageworkeragent.PendingMaterialization) *stageworkeragent.FileMaterializationJournal {
	t.Helper()
	journal, err := stageworkeragent.NewFileMaterializationJournal(t.TempDir(), 8)
	if err != nil {
		t.Fatal(err)
	}
	for _, record := range records {
		if err := journal.Put(t.Context(), record); err != nil {
			t.Fatal(err)
		}
	}
	return journal
}

func TestTerminalRecoveryCollectsFreshHistoryAndResumesLostProof(t *testing.T) {
	for _, lost := range []bool{false, true} {
		t.Run(map[bool]string{false: "fresh", true: "lost-proof"}[lost], func(t *testing.T) {
			f := terminalMaterializationFixture(t)
			gate := f.open(t)
			completeAdmissionInputs(t, beginAdmission(t, gate, f.assignment, f.acquireID))
			base := t.TempDir()
			group := startFloorCollectorRuntimes(t, f, base, true, false)
			// One allocated retry never received an execution envelope or entered Runtime.
			// The original allocation executed and sealed, so it must supply drain proof.
			a := f.assignment.Authority
			if response, err := group.clients[0].PrepareStage(t.Context(), &velav1.ModelRuntimeServicePrepareStageRequest{Authority: a, ExecutionSpec: f.assignment.ExecutionSpec}); err != nil || response.GetDecision() != velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_ACCEPTED {
				t.Fatalf("prepare: %v %v", response, err)
			}
			if response, err := group.clients[0].StartStage(t.Context(), &velav1.ModelRuntimeServiceStartStageRequest{Authority: a}); err != nil || response.GetDecision() != velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_ACCEPTED {
				t.Fatalf("start: %v %v", response, err)
			}
			group.activeBackends[0].MarkOutputReady([]byte(`{"kind":"LATENT","path":"/local/dit.bin"}`))
			if response, err := group.clients[0].SealOutput(t.Context(), &velav1.ModelRuntimeServiceSealOutputRequest{Authority: a}); err != nil || response.GetReceipt() == nil {
				t.Fatalf("seal: %v %v", response, err)
			}
			paths := retirementScratch(t, f)
			record, validator := terminalMaterializationRecord(t, f, "sealed")
			journal := terminalRecoveryJournal(t, record)
			guard := &noTerminalMaterializationIO{}
			queries := 0
			reader := terminalHistoryReaderFunc(func(ctx context.Context, _ *stageauthority.Validator, anchor *velav1.StageAuthority, member string, acquire uuid.UUID) (*stageauthority.VerifiedTerminalDisposition, error) {
				queries++
				if _, ok := ctx.Deadline(); !ok || !proto.Equal(anchor, a) || member != a.Members[0].WorkerMemberId || acquire != f.acquireID {
					t.Fatal("query lost deadline or exact retained authority/Acquire binding")
				}
				return fixtureTerminalResponse(t, f), nil
			})
			stream := automaticTerminalStream(t, terminalMaterializationConfig(t, f, gate, journal, validator, guard), reader)
			if lost {
				group.dropNonAdmissionResponse.Store(true)
				result, err := stream.ResumeMaterializations(t.Context())
				state := admissionSnapshot(t, gate)
				if err == nil || result.TerminalRecordsRetired != 0 || len(state.Retirements) != 1 || state.Retirements[0].Phase != stageworkeragent.TerminalRetirementIntent {
					t.Fatalf("lost proof did not retain INTENT: %+v %+v %v", state, result, err)
				}
				assertRetirementScratch(t, paths, true)
				group.close()
				if err := gate.Close(); err != nil {
					t.Fatal(err)
				}
				for index := range f.config.ExecutionFloor.Bindings {
					binding := &f.config.ExecutionFloor.Bindings[index]
					binding.Runtime.ModelRuntimeEpoch++
					f.admissionFixture.config.Bindings[index] = stageworkeragent.AdmissionRuntimeBinding(*binding)
				}
				group = startFloorCollectorRuntimes(t, f, base, false, false)
				f.config.ExecutionFloor.CurrentReaders = drainCollectorTargets(f)
				gate = f.open(t)
				// The first signed response has expired. Re-query against the old envelope.
				f.clock.Add(int64(2 * time.Minute))
				f.disposition.ObservedAt = timestamppb.New(time.Unix(0, f.clock.Load()))
				f.disposition.ExpiresAt = timestamppb.New(f.disposition.ObservedAt.AsTime().Add(time.Minute))
				f.signDisposition(t)
				stream = automaticTerminalStream(t, terminalMaterializationConfig(t, f, gate, journal, validator, guard), reader)
				for _, configured := range f.config.ExecutionFloor.CurrentReaders {
					configured.ModelRuntimeEpoch++
				}
			}
			result, err := stream.ResumeMaterializations(t.Context())
			if err != nil || result.TerminalRecordsRetired != 1 || result.Committed || result.SourceLostReported || result.L2Published || guard.calls != 0 {
				t.Fatalf("automatic retirement: %+v %v calls=%d", result, err, guard.calls)
			}
			assertRetirementScratch(t, paths, false)
			if records, err := journal.List(t.Context()); err != nil || len(records) != 0 {
				t.Fatalf("records retained: %+v %v", records, err)
			}
			state := admissionSnapshot(t, gate)
			if state.Retirements[0].Phase != stageworkeragent.TerminalRetirementRetired || state.Latest.AcquireCommandID != f.acquireID {
				t.Fatal("automatic recovery lost retained exclusion or Acquire evidence")
			}
			before := queries
			group.close()
			if _, err := stream.ResumeMaterializations(t.Context()); err != nil || queries != before {
				t.Fatalf("RETIRED performed a new history query: queries=%d %v", queries, err)
			}
		})
	}
}

func TestTerminalRecoveryUsesLatestRenewalAndOriginalAcquire(t *testing.T) {
	f := terminalMaterializationFixture(t)
	gate := f.open(t)
	completeAdmissionInputs(t, beginAdmission(t, gate, f.assignment, f.acquireID))
	record, validator := terminalMaterializationRecord(t, f, "sealed")
	renewal := proto.Clone(f.assignment).(*velav1.StageAssignment)
	renewal.Authority.StageVersion++
	renewal.Authority.IssuedAt = timestamppb.New(renewal.Authority.IssuedAt.AsTime().Add(time.Second))
	renewal.Authority.ExpiresAt = timestamppb.New(renewal.Authority.ExpiresAt.AsTime().Add(time.Second))
	f.clock.Add(int64(time.Second))
	f.sign(t, renewal)
	completeAdmissionInputs(t, beginAdmission(t, gate, renewal, f.acquireID))
	if err := gate.Close(); err != nil {
		t.Fatal(err)
	}
	gate = f.open(t)
	digest, err := stageauthority.Digest(renewal.Authority)
	if err != nil {
		t.Fatal(err)
	}
	f.disposition.OriginalAuthorityDigest = digest[:]
	f.disposition.StageVersion++
	f.disposition.ObservedAt = renewal.Authority.IssuedAt
	f.signDisposition(t)
	startFloorCollectorRuntimes(t, f, t.TempDir(), true, false)
	paths := retirementScratch(t, f)
	second, _ := terminalMaterializationRecord(t, f, "sealed")
	journal := terminalRecoveryJournal(t, record, second)
	queries := 0
	guard := &noTerminalMaterializationIO{}
	stream := automaticTerminalStream(t, terminalMaterializationConfig(t, f, gate, journal, validator, guard), func(_ context.Context, _ *stageauthority.Validator, anchor *velav1.StageAuthority, _ string, acquire uuid.UUID) (*stageauthority.VerifiedTerminalDisposition, error) {
		queries++
		if !proto.Equal(anchor, renewal.Authority) || acquire != f.acquireID {
			t.Fatal("old materialization envelope replaced latest admission renewal")
		}
		return fixtureTerminalResponse(t, f), nil
	})
	if result, err := stream.ResumeMaterializations(t.Context()); err != nil || result.TerminalRecordsRetired != 2 || queries != 1 || guard.calls != 0 {
		t.Fatalf("renewal reconciliation: %+v %v queries=%d", result, err, queries)
	}
	assertRetirementScratch(t, paths, false)
}

func TestTerminalRecoveryRetainAndMissingEnvelopePreserveIntent(t *testing.T) {
	for _, mode := range []string{"retain", "intent-retain", "intent-missing-envelope"} {
		t.Run(mode, func(t *testing.T) {
			f := terminalMaterializationFixture(t)
			gate := f.open(t)
			paths := retirementScratch(t, f)
			_, validator := terminalMaterializationRecord(t, f, "sealed")
			journal := terminalRecoveryJournal(t)
			if mode != "intent-missing-envelope" {
				completeAdmissionInputs(t, beginAdmission(t, gate, f.assignment, f.acquireID))
			}
			startFloorCollectorRuntimes(t, f, t.TempDir(), true, false)
			guard := &noTerminalMaterializationIO{}
			queries := 0
			config := terminalMaterializationConfig(t, f, gate, journal, validator, guard)
			if mode != "retain" {
				// Persist INTENT without execution envelopes, then cancel before RPCs.
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				restore := stageworkeragent.SetAssignmentAdmissionSyncHookForTest(gate, func(sync func() error) error {
					err := sync()
					cancel()
					return err
				})
				if _, err := config.TerminalRetirement.Retire(ctx, f.disposition, nil, drainCollectorTargets(f)); err == nil {
					t.Fatal("canceled collection completed")
				}
				restore()
			}
			stream := automaticTerminalStream(t, config, func(context.Context, *stageauthority.Validator, *velav1.StageAuthority, string, uuid.UUID) (*stageauthority.VerifiedTerminalDisposition, error) {
				queries++
				return nil, nil
			})
			result, err := stream.ResumeMaterializations(t.Context())
			if result.TerminalRecordsRetired != 0 || guard.calls != 0 {
				t.Fatalf("retained history caused materialization side effects: %+v calls=%d", result, guard.calls)
			}
			switch mode {
			case "retain":
				if err != nil || queries != 1 || len(admissionSnapshot(t, gate).Retirements) != 0 {
					t.Fatalf("RETAIN did not permit empty ordinary recovery: %v queries=%d", err, queries)
				}
			case "intent-retain":
				if !errors.Is(err, stageworkeragent.ErrScratchRetirementUnproven) || queries != 1 {
					t.Fatalf("RETAIN unblocked INTENT: %v queries=%d", err, queries)
				}
			case "intent-missing-envelope":
				if !errors.Is(err, stageworkeragent.ErrAdmissionRecoveryRequired) || queries != 0 {
					t.Fatalf("missing envelope was reconstructed: %v queries=%d", err, queries)
				}
			}
			assertRetirementScratch(t, paths, true)
		})
	}
}

func TestTerminalRecoveryRevalidatesReaderBeforeAnyFloorOrDeletion(t *testing.T) {
	for _, fault := range []string{"signature", "digest", "anchor", "member", "history", "binding", "expired", "nil-disposition", "late", "request-mutation"} {
		t.Run(fault, func(t *testing.T) {
			f := terminalMaterializationFixture(t)
			gate := f.open(t)
			paths := retirementScratch(t, f)
			record, validator := terminalMaterializationRecord(t, f, "sealed")
			journal := terminalRecoveryJournal(t, record)
			if fault == "binding" {
				f.config.ExecutionFloor.Bindings = f.config.ExecutionFloor.Bindings[:1]
			}
			guard := &noTerminalMaterializationIO{}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			stream := automaticTerminalStream(t, terminalMaterializationConfig(t, f, gate, journal, validator, guard), func(_ context.Context, _ *stageauthority.Validator, anchor *velav1.StageAuthority, _ string, _ uuid.UUID) (*stageauthority.VerifiedTerminalDisposition, error) {
				switch fault {
				case "anchor":
					f.disposition.OriginalAuthorityDigest[0] ^= 1
				case "member":
					f.disposition.WorkerMemberId = uuid.NewString()
					for _, allocation := range f.disposition.Allocations {
						allocation.Members[0].WorkerMemberId = f.disposition.WorkerMemberId
					}
				case "history":
					f.disposition.Allocations[1].StageProfileRevisionId = uuid.NewString()
				case "request-mutation":
					anchor.StageVersion++
					anchor, _ = f.signer.Sign(anchor)
					digest, err := stageauthority.Digest(anchor)
					if err != nil {
						t.Fatal(err)
					}
					f.disposition.OriginalAuthorityDigest = digest[:]
				}
				f.signDisposition(t)
				response := fixtureTerminalResponse(t, f)
				switch fault {
				case "signature":
					response.Disposition.Signature[0] ^= 1
				case "digest":
					response.Digest[0] ^= 1
				case "expired":
					f.clock.Add(int64(2 * time.Minute))
				case "nil-disposition":
					response.Disposition = nil
				case "late":
					cancel()
				}
				return response, nil
			})
			result, err := stream.ResumeMaterializations(ctx)
			if err == nil || result.TerminalRecordsRetired != 0 || guard.calls != 0 {
				t.Fatalf("bad reader result accepted: %+v %v", result, err)
			}
			for _, client := range f.clients {
				if client.calls.Load() != 0 {
					t.Fatal("bad history reached Runtime")
				}
			}
			if state := admissionSnapshot(t, gate); state.Floor != 0 || len(state.Retirements) != 0 {
				t.Fatal("bad history installed local exclusion")
			}
			assertRetirementScratch(t, paths, true)
		})
	}
}

func TestTerminalRecoveryRejectsConflictingRetainedEnvelopes(t *testing.T) {
	f := terminalMaterializationFixture(t)
	gate := f.open(t)
	completeAdmissionInputs(t, beginAdmission(t, gate, f.assignment, f.acquireID))
	record, validator := terminalMaterializationRecord(t, f, "sealed")
	record.StageAuthority.ExecutionNonce[0] ^= 1
	var err error
	record.StageAuthority, err = f.signer.Sign(record.StageAuthority)
	if err != nil {
		t.Fatal(err)
	}
	journal := terminalRecoveryJournal(t, record)
	guard := &noTerminalMaterializationIO{}
	stream := automaticTerminalStream(t, terminalMaterializationConfig(t, f, gate, journal, validator, guard), func(context.Context, *stageauthority.Validator, *velav1.StageAuthority, string, uuid.UUID) (*stageauthority.VerifiedTerminalDisposition, error) {
		t.Fatal("conflicting execution history reached Control")
		return nil, nil
	})
	if _, err := stream.ResumeMaterializations(t.Context()); err == nil || !strings.Contains(err.Error(), "conflicting execution envelopes") {
		t.Fatalf("conflicting signed envelopes accepted: %v", err)
	}
}

func TestTerminalRecoveryConfigurationRequiresRetirement(t *testing.T) {
	config := stageworkeragent.DurableStreamConfig{Admission: &stageworkeragent.FileAssignmentAdmission{}, TerminalHistory: terminalHistoryReaderFunc(func(context.Context, *stageauthority.Validator, *velav1.StageAuthority, string, uuid.UUID) (*stageauthority.VerifiedTerminalDisposition, error) {
		return nil, nil
	})}
	if _, err := stageworkeragent.NewDurableStreamAgent(config); err == nil || !strings.Contains(err.Error(), "automatic terminal history requires durable retirement") {
		t.Fatalf("history reader accepted without durable retirement: %v", err)
	}
}
