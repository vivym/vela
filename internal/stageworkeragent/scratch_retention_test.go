package stageworkeragent_test

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/vivym/vela/internal/stageartifact"
	"github.com/vivym/vela/internal/stageworkeragent"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
)

func TestUnprovenScratchRetirementRetainsConfirmedHistoryAndBackpressure(t *testing.T) {
	for _, disposition := range []stageworkeragent.MaterializationDisposition{
		stageworkeragent.MaterializationCommitted, stageworkeragent.MaterializationSourceLost,
	} {
		t.Run(string(disposition), func(t *testing.T) {
			fixture := newSingleMemberMaterializationFixture(t)
			var manifest map[string]any
			if err := json.Unmarshal(fixture.manifest, &manifest); err != nil {
				t.Fatal(err)
			}
			locator := fixture.authority.GetStageAttemptId() + "/latent.bin"
			manifest["local_locator"] = locator
			var err error
			fixture.manifest, err = json.Marshal(manifest)
			if err != nil {
				t.Fatal(err)
			}
			outputPath := filepath.Join(fixture.localRoot, filepath.FromSlash(locator))
			inputPath := filepath.Join(t.TempDir(), "stage-runs", fixture.authority.GetStageRunId(), "inputs", "input.bin")
			neighborPath := filepath.Join(fixture.localRoot, "neighbor", "output.bin")
			for _, file := range []string{inputPath, outputPath, neighborPath} {
				writeScratchFixture(t, file, fixture.payload)
			}
			if disposition == stageworkeragent.MaterializationSourceLost {
				writeScratchFixture(t, outputPath, []byte("corrupt local source"))
			}
			runtime, err := stageworkeragent.New(stageworkeragent.Config{
				Members: []stageworkeragent.RuntimeMember{{ID: fixture.memberID, Client: fixture.client}},
			})
			if err != nil {
				t.Fatal(err)
			}
			control := newMaterializingStreamControl(t, fixture.authority)
			if disposition == stageworkeragent.MaterializationCommitted {
				control.commitFailures = 1
			} else {
				control.sourceLossFailures = 1
			}
			publisher := &outageOncePublisher{objectVersion: "retained-exact-version"}
			source, err := stageartifact.NewFilesystemLocalOutputSource(fixture.localRoot)
			if err != nil {
				t.Fatal(err)
			}
			journalRoot := t.TempDir()
			journal, err := stageworkeragent.NewFileMaterializationJournal(journalRoot, 1)
			if err != nil {
				t.Fatal(err)
			}
			config := stageworkeragent.MaterializationConfig{
				Validator: control.validator, Source: source, Publisher: publisher,
				Journal:                 &confirmationFailJournal{MaterializationJournal: journal, failures: 1},
				ScratchRetirer:          stageworkeragent.RetainScratchRetirer{},
				OutputOwnershipContract: stageworkeragent.AttemptOwnedFilesystemScratchV1,
				SourceLossEvidence:      testSourceLossEvidenceProvider(),
			}
			stream, err := stageworkeragent.NewMaterializingStreamAgent(runtime, control, config)
			if err != nil {
				t.Fatal(err)
			}
			ctx := t.Context()
			if _, err := stream.ExecuteAssignment(ctx, fixture.assignment); err != nil {
				t.Fatal(err)
			}
			fixture.backend.MarkOutputReadyWithSize(fixture.manifest, int64(len(fixture.payload)))
			result, err := stream.SealAndMaterialize(ctx)
			if err == nil || errors.Is(err, stageworkeragent.ErrScratchRetirementUnproven) || !result.GPUReleased {
				t.Fatalf("unconfirmed control disposition: %+v %v", result, err)
			}
			if _, err := stream.ResumeMaterializations(ctx); err == nil || errors.Is(err, stageworkeragent.ErrScratchRetirementUnproven) {
				t.Fatalf("confirmation persistence failure was hidden: %v", err)
			}
			if records, err := journal.List(ctx); err != nil || len(records) != 1 || records[0].ConfirmedDisposition != "" {
				t.Fatalf("failed confirmation persistence changed recovery record: %+v %v", records, err)
			}
			assertScratchFiles(t, true, inputPath, outputPath, neighborPath)

			result, err = stream.ResumeMaterializations(ctx)
			if !errors.Is(err, stageworkeragent.ErrScratchRetirementUnproven) || result.Committed || result.SourceLostReported ||
				result.L2Published != (disposition == stageworkeragent.MaterializationCommitted) {
				t.Fatalf("unproven retirement completed: %+v %v", result, err)
			}
			retained, err := journal.List(ctx)
			if err != nil || len(retained) != 1 || retained[0].ConfirmedDisposition != disposition {
				t.Fatalf("confirmed history was discarded: %+v %v", retained, err)
			}
			commitCalls, sourceLossCalls := control.commitCalls, control.sourceLossCalls
			publishCalls, sealCalls := publisher.calls, fixture.countingBackend.sealCalls
			expireScratchControl(t, control)
			config.Validator = control.validator
			for range 2 {
				config.Journal, err = stageworkeragent.NewFileMaterializationJournal(journalRoot, 1)
				if err != nil {
					t.Fatal(err)
				}
				stream, err = stageworkeragent.NewMaterializingStreamAgent(runtime, control, config)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := stream.ResumeMaterializations(ctx); !errors.Is(err, stageworkeragent.ErrScratchRetirementUnproven) {
					t.Fatalf("journal reopen forgot pending retirement: %v", err)
				}
				if records, err := config.Journal.List(ctx); err != nil || !reflect.DeepEqual(records, retained) {
					t.Fatalf("replay changed confirmed identity or evidence: %+v %v", records, err)
				}
				assertScratchFiles(t, true, inputPath, outputPath, neighborPath)
			}
			if result, err := stream.ExecuteAssignment(ctx, fixture.assignment); !errors.Is(err, stageworkeragent.ErrMaterializationJournalFull) ||
				result.PreparedMembers != 0 || result.StartedMembers != 0 || control.startCalls != 1 {
				t.Fatalf("retained journal lost admission backpressure: %+v %v", result, err)
			}

			commands := make(chan *velav1.StageWorkerControlServiceConnectResponse)
			productionControl := &productionExecutionControl{
				materializingStreamControl: control, identity: runtimeIdentityFromAuthority(fixture.authority), commands: commands,
			}
			stream, err = stageworkeragent.NewMaterializingStreamAgent(runtime, productionControl, config)
			if err != nil {
				t.Fatal(err)
			}
			runContext, cancel := context.WithCancel(ctx)
			defer cancel()
			var observed error
			production, err := stageworkeragent.NewProductionAgent(stageworkeragent.ProductionConfig{
				Control: productionControl, Runtime: fixture.client, Stream: stream,
				RuntimeIdentity: productionControl.identity, Devices: fixture.authority.GetDevices(), Members: fixture.authority.GetMembers(),
				CapacityVector: fixture.authority.GetCapacityVector(), CapacityTTL: 2 * time.Minute,
				HeartbeatInterval: time.Second, RetryMinimum: time.Millisecond, RetryMaximum: time.Second,
				ObservationSequenceSource: &capacitySequenceSource{values: []int64{1}},
				RetryObserver: func(operation string, cause error) {
					if operation == "resume-materialization" {
						observed = cause
					}
				},
				Now: time.Now, Wait: func(context.Context, time.Duration) error { cancel(); return nil },
			})
			if err != nil {
				t.Fatal(err)
			}
			if err := production.Run(runContext); err != nil {
				t.Fatal(err)
			}
			close(commands)
			if !errors.Is(observed, stageworkeragent.ErrScratchRetirementUnproven) || productionControl.acquireCalls != 0 {
				t.Fatalf("production resumed discovery before retirement: observed=%v acquires=%d", observed, productionControl.acquireCalls)
			}
			if control.commitCalls != commitCalls || control.sourceLossCalls != sourceLossCalls ||
				publisher.calls != publishCalls || fixture.countingBackend.sealCalls != sealCalls {
				t.Fatal("retention replay repeated control authority, publication or Runtime Seal")
			}
			assertScratchFiles(t, true, inputPath, outputPath, neighborPath)
			if info, err := os.Stat(outputPath); err != nil || !info.Mode().IsRegular() {
				t.Fatalf("retained source changed type: %v", err)
			}
		})
	}
}
