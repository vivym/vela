package stageworkercontrol

import (
	"bytes"
	"crypto/sha256"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/stageworkertransport"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestMatchesDurableStageAuthorityAllowsOnlyExactPostSealReplay(t *testing.T) {
	now := time.Date(2026, 9, 4, 6, 0, 0, 0, time.UTC)
	spiffeID := "spiffe://vela/worker/member-1"
	identityDigest := sha256.Sum256([]byte(spiffeID))
	leaseToken := bytes.Repeat([]byte{0x41}, 32)
	leaseTokenDigest := sha256.Sum256(leaseToken)
	authority := &velav1.StageAuthority{
		JobId: "51000000-0000-0000-0000-000000000001", AttemptId: "51000000-0000-0000-0000-000000000002",
		StageRunId: "51000000-0000-0000-0000-000000000003", StageAttemptId: "51000000-0000-0000-0000-000000000004",
		StageAllocationId: "51000000-0000-0000-0000-000000000005", StageLeaseId: "51000000-0000-0000-0000-000000000006",
		AttemptFence: 1, StageFence: 2, StageVersion: 3,
		WorkerInstanceId: "51000000-0000-0000-0000-000000000007", WorkerInstanceEpoch: 4,
		DeviceSetDigest: bytes.Repeat([]byte{0x42}, 32), MembershipDigest: bytes.Repeat([]byte{0x43}, 32),
		ModelResidencyId: "51000000-0000-0000-0000-000000000008", ModelRuntimeIdentity: "encoder-runtime",
		ModelRuntimeBarrierGeneration: 7, StageProfileRevisionId: "51000000-0000-0000-0000-000000000009",
		CapacityObservationSequence: 5, CapacityVector: map[string]int64{"active_stage_slots": 1},
		LeaseToken: leaseToken, ExecutionNonce: bytes.Repeat([]byte{0x44}, 32), SigningKeyId: "stage-key-v1",
		IssuedAt: timestamppb.New(now), ExpiresAt: timestamppb.New(now.Add(time.Minute)),
		MonotonicValidFor: durationpb.New(time.Minute),
		Members: []*velav1.StageAuthorityMemberEpoch{{
			WorkerMemberId: "51000000-0000-0000-0000-000000000010", MemberEpoch: 6,
			ModelRuntimeEpoch: 7, IdentityDigest: identityDigest[:],
		}},
		Devices: []*velav1.StageAuthorityDeviceEpoch{{
			DeviceId: "51000000-0000-0000-0000-000000000011", DeviceEpoch: 8,
		}},
	}
	snapshot := durableStageAuthority{
		jobID: uuid.MustParse(authority.GetJobId()), attemptID: uuid.MustParse(authority.GetAttemptId()),
		attemptFence: authority.GetAttemptFence(), attemptState: "RUNNING",
		stageRunID: uuid.MustParse(authority.GetStageRunId()), stageFence: authority.GetStageFence(),
		stageVersion: authority.GetStageVersion() + 1, stageRunState: "MATERIALIZING",
		stageAttemptID: uuid.MustParse(authority.GetStageAttemptId()), stageAttemptState: "OUTPUT_SEALED",
		stageProfileID:    uuid.MustParse(authority.GetStageProfileRevisionId()),
		stageAllocationID: uuid.MustParse(authority.GetStageAllocationId()), allocationState: "RELEASED",
		capacityVector: authority.GetCapacityVector(), stageLeaseID: uuid.MustParse(authority.GetStageLeaseId()),
		leaseState: "REVOKED", workerInstanceID: uuid.MustParse(authority.GetWorkerInstanceId()),
		workerInstanceEpoch: authority.GetWorkerInstanceEpoch(), deviceSetDigest: authority.GetDeviceSetDigest(),
		membershipDigest: authority.GetMembershipDigest(), modelResidencyID: uuid.MustParse(authority.GetModelResidencyId()),
		modelRuntimeBarrierGeneration: authority.GetModelRuntimeBarrierGeneration(), tokenDigest: leaseTokenDigest[:],
		signingKeyID: authority.GetSigningKeyId(), executionNonce: authority.GetExecutionNonce(),
		issuedAt: now, expiresAt: now.Add(time.Minute), localDeadlineAt: now.Add(time.Minute),
		controlSessionEpoch: 11, workerLifecycle: "READY", workerReachability: "CONNECTED",
		runtimeIdentity: authority.GetModelRuntimeIdentity(), residencyState: "READY",
		capacityObservationOK: false,
		members: []durableMember{{
			id: uuid.MustParse(authority.GetMembers()[0].GetWorkerMemberId()), epoch: 6,
			identityDigest: identityDigest[:], readiness: "READY", modelRuntimeEpoch: 7,
		}},
		devices: []durableDevice{{
			id: uuid.MustParse(authority.GetDevices()[0].GetDeviceId()), epoch: 8,
		}},
	}
	identity := stageworkertransport.Identity{SPIFFEID: spiffeID}

	if !matchesDurableStageAuthority(snapshot, identity, 11, OperationSealStageOutput, authority) {
		t.Fatal("exact post-seal replay was rejected")
	}
	if matchesDurableStageAuthority(snapshot, identity, 12, OperationSealStageOutput, authority) {
		t.Fatal("post-seal replay accepted a mismatched durable control session")
	}
	if matchesDurableStageAuthority(snapshot, identity, 11, OperationHeartbeatStage, authority) {
		t.Fatal("post-seal authority authorized a non-seal operation")
	}
	snapshot.stageVersion++
	if matchesDurableStageAuthority(snapshot, identity, 11, OperationSealStageOutput, authority) {
		t.Fatal("post-seal replay accepted an unexpected StageRun version")
	}
	snapshot.stageVersion--
	if matchesDurableStageAuthority(
		snapshot,
		stageworkertransport.Identity{SPIFFEID: "spiffe://vela/worker/other"},
		11,
		OperationSealStageOutput,
		authority,
	) {
		t.Fatal("post-seal replay accepted another Worker identity")
	}
}
