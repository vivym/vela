package stageworkeragent_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/stageartifact"
	"github.com/vivym/vela/internal/stageworkeragent"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/protobuf/proto"
)

func TestMaterializationRecoveryPreservesCommandIdentityAndRejectsUnknownLegacyIDs(t *testing.T) {
	for _, kind := range []string{"COMMIT", "SOURCE_LOST"} {
		for _, legacy := range []bool{false, true} {
			t.Run(kind+" legacy="+map[bool]string{false: "false", true: "true"}[legacy], func(t *testing.T) {
				fixture := newSingleMemberMaterializationFixture(t)
				runtimeAgent, err := stageworkeragent.New(stageworkeragent.Config{Members: []stageworkeragent.RuntimeMember{{ID: fixture.memberID, Client: fixture.client}}})
				if err != nil {
					t.Fatal(err)
				}
				control := newMaterializingStreamControl(t, fixture.authority)
				if kind == "COMMIT" {
					control.commitFailures = 1
				} else {
					control.sourceLossFailures = 1
					if err := os.Remove(filepath.Join(fixture.localRoot, "dit.bin")); err != nil {
						t.Fatal(err)
					}
				}
				transport := &materializationIdentityControl{materializingStreamControl: control, t: t, requests: make(map[string]*velav1.StageWorkerControlServiceConnectRequest)}
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
					Publisher: &outageOncePublisher{objectVersion: "exact-version"}, Journal: journal,
					SourceLossEvidence: testSourceLossEvidenceProvider()}
				agent, err := stageworkeragent.NewMaterializingStreamAgent(runtimeAgent, transport, config)
				if err != nil {
					t.Fatal(err)
				}
				ctx := context.Background()
				if _, err := agent.ExecuteAssignment(ctx, fixture.assignment); err != nil {
					t.Fatal(err)
				}
				fixture.backend.MarkOutputReadyWithSize(fixture.manifest, int64(len(fixture.payload)))
				if _, err := agent.SealAndMaterialize(ctx); err == nil {
					t.Fatal("injected response loss was not observed")
				}
				records, err := journal.List(ctx)
				if err != nil || len(records) != 1 {
					t.Fatalf("pending operation not durable: %+v %v", records, err)
				}
				record := records[0]
				commandID := record.CommitCommandID
				if kind == "SOURCE_LOST" {
					commandID = record.SourceLossCommandID
				}
				if commandID == "" || commandID != transport.requests[kind].GetRequestId() {
					t.Fatalf("journal ID %q does not match wire request %q", commandID, transport.requests[kind].GetRequestId())
				}
				if legacy {
					record.CommitCommandID, record.SourceLossCommandID = "", ""
					if err := journal.Put(ctx, record); err != nil {
						t.Fatal(err)
					}
				}
				config.Journal, err = stageworkeragent.NewFileMaterializationJournal(journalRoot, 4)
				if err != nil {
					t.Fatal(err)
				}
				restarted, err := stageworkeragent.NewMaterializingStreamAgent(runtimeAgent, transport, config)
				if err != nil {
					t.Fatal(err)
				}
				result, resumeErr := restarted.ResumeMaterializations(ctx)
				if legacy {
					if !errors.Is(resumeErr, stageworkeragent.ErrLegacyMaterializationCommandIdentity) || transport.replays != 0 {
						t.Fatalf("legacy command identity was guessed: %+v %v replays=%d", result, resumeErr, transport.replays)
					}
					if records, err := journal.List(ctx); err != nil || len(records) != 1 {
						t.Fatalf("legacy recovery record was discarded: %+v %v", records, err)
					}
					return
				}
				if transport.replays != 1 || (kind == "COMMIT" && (resumeErr != nil || !result.Committed)) ||
					(kind == "SOURCE_LOST" && (!errors.Is(resumeErr, stageworkeragent.ErrMaterializationSourceLostReported) || !result.SourceLostReported)) {
					t.Fatalf("stable command replay failed: %+v %v replays=%d", result, resumeErr, transport.replays)
				}
			})
		}
	}
}

type materializationIdentityControl struct {
	*materializingStreamControl
	t        *testing.T
	requests map[string]*velav1.StageWorkerControlServiceConnectRequest
	replays  int
}

func (control *materializationIdentityControl) Exchange(ctx context.Context, request *velav1.StageWorkerControlServiceConnectRequest) (*velav1.StageWorkerControlServiceConnectResponse, error) {
	kind := ""
	switch {
	case request.GetSealStageOutput() != nil:
		kind = "SEAL"
	case request.GetCommitStageMaterialization() != nil:
		kind = "COMMIT"
	case request.GetReportMaterializationSourceLost() != nil:
		kind = "SOURCE_LOST"
	}
	if kind != "" {
		id, err := uuid.Parse(request.GetRequestId())
		if err != nil || id == uuid.Nil || id.String() != request.GetRequestId() {
			control.t.Fatalf("%s lacks a canonical command ID: %q", kind, request.GetRequestId())
		}
		if previous := control.requests[kind]; previous != nil {
			if !proto.Equal(previous, request) {
				control.t.Fatalf("%s request identity or payload changed on recovery: first=%v resumed=%v", kind, previous, request)
			}
			control.replays++
		} else {
			for previousKind, previous := range control.requests {
				if previous.GetRequestId() == request.GetRequestId() {
					control.t.Fatalf("%s and %s share a command ID", kind, previousKind)
				}
			}
			control.requests[kind] = proto.Clone(request).(*velav1.StageWorkerControlServiceConnectRequest)
		}
	}
	return control.materializingStreamControl.Exchange(ctx, request)
}
