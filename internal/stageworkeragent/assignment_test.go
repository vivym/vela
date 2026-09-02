package stageworkeragent_test

import (
	"testing"

	"github.com/vivym/vela/internal/stageassignment"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
)

func TestInputTransferTicketRejectsAmbiguousArtifactVersionIdentity(t *testing.T) {
	fixture := newBarrierFixture(t, false)
	fixture.assignment.ExecutionSpec.Inputs = []*velav1.StageInputArtifact{{
		StageArtifactId: "72000000-0000-0000-0000-000000000001",
		ObjectVersion:   "version\x00suffix",
	}}
	fixture.assignment.InputTransferTickets = []*velav1.StageInputTransferTicket{{
		StageArtifactId: "72000000-0000-0000-0000-000000000001\x00version",
		ObjectVersion:   "suffix",
		TransferTicket:  []byte("ticket"),
	}}
	if _, err := stageassignment.Validate(fixture.assignment); err == nil {
		t.Fatal("TransferTicket accepted an ambiguous non-UUID StageArtifact identity")
	}
}
