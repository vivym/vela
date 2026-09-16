package stageworkeragent_test

import (
	"strings"
	"testing"

	"github.com/vivym/vela/internal/stageauthority"
	"github.com/vivym/vela/internal/stageworkeragent"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
)

func TestStatusPreservesRuntimeRejectionDetail(t *testing.T) {
	f := newBarrierFixture(t, false)
	digest, err := stageauthority.Digest(f.authority)
	if err != nil {
		t.Fatal(err)
	}
	id := f.memberIDs[0]
	agent, err := stageworkeragent.New(stageworkeragent.Config{Members: []stageworkeragent.RuntimeMember{{
		ID: id, Client: fixedStatusRuntimeClient{response: &velav1.ModelRuntimeServiceStatusResponse{
			Decision:        velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_REJECTED,
			AuthorityDigest: digest[:], RuntimeIdentity: runtimeIdentityForMember(f.authority, id),
			Detail: "stage identity does not match active execution",
		}},
	}}})
	if err != nil {
		t.Fatal(err)
	}
	_, err = agent.Status(t.Context(), f.authority)
	for _, expected := range []string{"REJECTED", "digest_matches=true", "identity_matches=true", "stage identity does not match active execution"} {
		if err == nil || !strings.Contains(err.Error(), expected) {
			t.Fatalf("missing diagnostic %q: %v", expected, err)
		}
	}
}
