package stageworkercontrol

import (
	"crypto/sha256"
	"testing"

	"github.com/google/uuid"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
)

func TestGangAuthorityMatchesIndependentMemberRuntimeEpochs(t *testing.T) {
	leaderID := uuid.MustParse("49200000-0000-0000-0000-000000000120")
	memberID := uuid.MustParse("49200000-0000-0000-0000-000000000121")
	leaderSPIFFE := "spiffe://vela/worker/member-0"
	leaderDigest := sha256.Sum256([]byte(leaderSPIFFE))
	memberDigest := sha256.Sum256([]byte("spiffe://vela/worker/member-1"))
	durable := []durableMember{
		{id: leaderID, epoch: 1, identityDigest: leaderDigest[:], readiness: "READY", modelRuntimeEpoch: 2},
		{id: memberID, epoch: 1, identityDigest: memberDigest[:], readiness: "READY", modelRuntimeEpoch: 8},
	}
	signed := []*velav1.StageAuthorityMemberEpoch{
		{WorkerMemberId: memberID.String(), MemberEpoch: 1, ModelRuntimeEpoch: 8,
			IdentityDigest: memberDigest[:]},
		{WorkerMemberId: leaderID.String(), MemberEpoch: 1, ModelRuntimeEpoch: 2,
			IdentityDigest: leaderDigest[:]},
	}
	if !matchesMembers(durable, signed, leaderSPIFFE) {
		t.Fatal("durable authorizer rejected independent local ModelRuntime epochs")
	}
	tampered := cloneAuthorityMembers(signed)
	tampered[0].ModelRuntimeEpoch++
	if matchesMembers(durable, tampered, leaderSPIFFE) {
		t.Fatal("durable authorizer accepted a changed member-local ModelRuntime epoch")
	}
	tampered = cloneAuthorityMembers(signed)
	tampered[0].IdentityDigest[0] ^= 0xff
	if matchesMembers(durable, tampered, leaderSPIFFE) {
		t.Fatal("durable authorizer accepted a changed signed member identity digest")
	}
}

func TestGangAuthorityRequiresAggregateBarrierGeneration(t *testing.T) {
	authority := &velav1.StageAuthority{ModelRuntimeBarrierGeneration: 3}
	if !stageAuthorityHasBarrierGeneration(authority, 3) {
		t.Fatal("StageAuthority rejected its aggregate barrier generation")
	}
	if stageAuthorityHasBarrierGeneration(authority, 2) ||
		stageAuthorityHasBarrierGeneration(authority, 8) {
		t.Fatal("StageAuthority accepted a member-local epoch as its aggregate barrier generation")
	}
}

func cloneAuthorityMembers(
	values []*velav1.StageAuthorityMemberEpoch,
) []*velav1.StageAuthorityMemberEpoch {
	cloned := make([]*velav1.StageAuthorityMemberEpoch, len(values))
	for index, value := range values {
		cloned[index] = &velav1.StageAuthorityMemberEpoch{
			WorkerMemberId:    value.GetWorkerMemberId(),
			MemberEpoch:       value.GetMemberEpoch(),
			ModelRuntimeEpoch: value.GetModelRuntimeEpoch(),
			IdentityDigest:    append([]byte(nil), value.GetIdentityDigest()...),
		}
	}
	return cloned
}
