package nodeagent

import (
	"bytes"
	"testing"
	"time"

	"github.com/vivym/vela/internal/modelruntime"
)

func TestJournalReadOnlyEndpoint(t *testing.T) {
	for _, role := range []string{"runtime", "worker"} {
		t.Run(role, func(t *testing.T) {
			f := newJournalEndpointConfiguredFixture(t, 0, nil, true)
			server, done := startJournalTestServer(t, f, 30*time.Second)
			child, ownerRole := f.runtime, modelruntime.JournalRuntimeRole
			valid := modelruntime.JournalCommand{SchemaVersion: 1, Admit: &modelruntime.JournalAuthorityCommand{Authority: f.authority}}
			if role == "worker" {
				child, ownerRole = f.worker, modelruntime.JournalWorkerRole
				valid = modelruntime.JournalCommand{SchemaVersion: 1, Floor: &modelruntime.JournalFloorCommand{Disposition: f.floor}}
			}
			before, err := modelruntime.ReadJournalDocument(t.Context(), f.identity, f.owner.Read)
			if err != nil {
				t.Fatal(err)
			}
			// Same-UID siblings cannot read even though the endpoint is read-only.
			if report := journalServerRequest(t, f.sibling, journalEndpointControl{Identity: f.identity, Read: true}); report.Error == "" || report.ReadCompleted {
				t.Fatal("read-only mode bypassed original-process authentication")
			}
			for _, reader := range []*journalEndpointChild{f.runtime, f.worker} {
				if report := journalServerRequest(t, reader, journalEndpointControl{Identity: f.identity, Read: true}); report.Error != "" || !report.ReadCompleted {
					t.Fatalf("enrolled startup read failed: %+v", report)
				}
			}
			// This command is independently shown valid against the same owner
			// below. Rejection cannot be attributed to invalid authority or role.
			for range 2 {
				report := journalServerRequest(t, child, journalEndpointControl{Identity: f.identity, Command: valid})
				if report.Error != modelruntime.ErrJournalRejected.Error() || report.Receipt != (modelruntime.JournalMutationReceipt{}) {
					t.Fatalf("read-only valid mutation acknowledged: %+v", report)
				}
			}
			families := []struct {
				name    string
				command modelruntime.JournalCommand
			}{
				{"admit", modelruntime.JournalCommand{Admit: &modelruntime.JournalAuthorityCommand{Authority: f.authority}}},
				{"candidates", modelruntime.JournalCommand{Candidates: &modelruntime.JournalCandidatesCommand{Authority: f.authority}}},
				{"seal", modelruntime.JournalCommand{Seal: &modelruntime.JournalSealCommand{Authority: f.authority}}},
				{"drain", modelruntime.JournalCommand{Drain: &modelruntime.JournalDrainCommand{Authority: f.authority}}},
				{"health", modelruntime.JournalCommand{Health: &modelruntime.JournalHealthCommand{Authority: f.authority}}},
				{"floor", modelruntime.JournalCommand{Floor: &modelruntime.JournalFloorCommand{Disposition: f.floor}}},
				{"non-admission", modelruntime.JournalCommand{NonAdmission: &modelruntime.JournalAuthorityCommand{Authority: f.authority}}},
				{"terminal-non-admission", modelruntime.JournalCommand{TerminalNonAdmission: &modelruntime.JournalTerminalNonAdmissionCommand{Disposition: f.floor}}},
			}
			for _, family := range families {
				t.Run(family.name, func(t *testing.T) {
					family.command.SchemaVersion = 1
					report := journalServerRequest(t, child, journalEndpointControl{Identity: f.identity, Command: family.command})
					if report.Error != modelruntime.ErrJournalRejected.Error() || report.Receipt != (modelruntime.JournalMutationReceipt{}) {
						t.Fatalf("mutation family crossed read-only endpoint: %+v", report)
					}
				})
			}
			after, err := modelruntime.ReadJournalDocument(t.Context(), f.identity, f.owner.Read)
			if err != nil || !bytes.Equal(before.Document, after.Document) || !bytes.Equal(before.LockDocument, after.LockDocument) {
				t.Fatalf("read-only requests mutated or poisoned original journal: %v", err)
			}
			if report := journalServerRequest(t, child, journalEndpointControl{Identity: f.identity, Read: true}); report.Error != "" || !report.ReadCompleted {
				t.Fatalf("rejected writes poisoned later reads: %+v", report)
			}
			if err := f.endpoint.Close(); err != nil {
				t.Fatal(err)
			}
			if report := journalServerRequest(t, child, journalEndpointControl{Identity: f.identity, Read: true}); report.Error == "" || report.ReadCompleted {
				t.Fatal("closed read-only endpoint retained route")
			}
			if err := server.Shutdown(t.Context()); err != nil {
				t.Fatal(err)
			}
			assertJournalServerJoined(t, server, done)
			wire, err := modelruntime.EncodeJournalCommand(valid)
			if err != nil {
				t.Fatal(err)
			}
			receipt, err := f.owner.Apply(t.Context(), ownerRole, wire)
			if err != nil || receipt.Replayed || role == "runtime" && receipt.Highest != 1 || role == "worker" && receipt.Floor != 1 {
				t.Fatalf("blocked command was not independently valid: %+v %v", receipt, err)
			}
			t.Log("original peers read; all eight mutation selectors rejected; exact valid command succeeds only through independent trusted owner call after endpoint close")
		})
	}
}
