package modelruntime_test

import (
	"testing"
	"time"

	"github.com/vivym/vela/internal/modelruntime"
	"github.com/vivym/vela/internal/stageauthority"
	"google.golang.org/protobuf/proto"
)

func TestJournalLookupPreservesHistoricalIdentityAndRenewal(t *testing.T) {
	for selected, name := range []string{"oldest", "middle", "latest"} {
		t.Run(name, func(t *testing.T) {
			f, directory := transitionFixture(t)
			authorities := make([]stageauthority.Verified, 3)
			for index := range authorities {
				authority := f.authority(t, index%2, int64(10+index))
				verified, err := f.validator.ValidateEnvelope(authority)
				if err != nil {
					t.Fatal(err)
				}
				authorities[index] = verified
				applyJournalMutation(t, f, modelruntime.ExecutionMutationForTest{Kind: "admit", Authority: verified})
			}
			// A valid signature and colliding sequence cannot select an unrelated
			// allocation, regardless of its position in the retained history.
			conflict, err := f.validator.ValidateEnvelope(f.authority(t, selected%2, int64(10+selected)))
			if err != nil {
				t.Fatal(err)
			}
			assertJournalMutationRejected(t, f, directory, modelruntime.ExecutionMutationForTest{
				Kind: "candidates", Authority: conflict, Confirmed: &conflict})
			original := authorities[selected]
			applyJournalMutation(t, f, modelruntime.ExecutionMutationForTest{
				Kind: "candidates", Authority: original, Confirmed: &original})
			f.clock.Advance(time.Second)
			renewed, err := f.validator.ValidateEnvelope(renewWatchdogAuthority(t, f.signer, original.Authority, f.clock.Now()))
			if err != nil {
				t.Fatal(err)
			}
			applyJournalMutation(t, f, modelruntime.ExecutionMutationForTest{
				Kind: "candidates", Authority: renewed, Confirmed: &original})
			for index, authority := range authorities {
				record, err := f.supervisor.InspectRetainedAllocationAuthorities(t.Context(), authority.Authority)
				if err != nil || record == nil || !proto.Equal(record.Original, authority.Authority) {
					t.Fatalf("retained allocation %d: %+v %v", index, record, err)
				}
				if index == selected {
					if !proto.Equal(record.Accepted, renewed.Authority) || !proto.Equal(record.Confirmed, original.Authority) {
						t.Fatal("selected allocation lost exact accepted/confirmed renewal identity")
					}
				} else if !proto.Equal(record.Accepted, authority.Authority) || record.Confirmed != nil {
					t.Fatalf("lookup modified unrelated allocation %d", index)
				}
			}
		})
	}
}
