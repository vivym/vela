package stageworkeragent

import (
	"testing"

	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
)

func TestInputTransferTicketRejectsAmbiguousArtifactVersionIdentity(t *testing.T) {
	assignment := &velav1.StageAssignment{
		ExecutionSpec: &velav1.StageExecutionSpec{
			Inputs: []*velav1.StageInputArtifact{{
				StageArtifactId: "72000000-0000-0000-0000-000000000001",
				ObjectVersion:   "version\x00suffix",
			}},
		},
		InputTransferTickets: []*velav1.StageInputTransferTicket{{
			StageArtifactId: "72000000-0000-0000-0000-000000000001\x00version",
			ObjectVersion:   "suffix",
			TransferTicket:  []byte("ticket"),
		}},
	}
	if err := validateInputTransferTickets(assignment); err == nil {
		t.Fatal("TransferTicket accepted an ambiguous non-UUID StageArtifact identity")
	}
}
