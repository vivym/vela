//go:build integration

package integration_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/stageworkercontrol"
	"github.com/vivym/vela/internal/stageworkertransport"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestStageWorkerCapacityAcceptsBoundedLegacyConcurrency(t *testing.T) {
	database, _, coordinator, job, attemptID, encoderRunID, _ := newStageGraphCancellationFixture(t, "legacy-concurrency")
	assignment := assignEncoder(t, database, coordinator, attemptID, encoderRunID, time.Now().Add(time.Hour))
	signedAssignedStageAuthorityWithoutRuntimeBarrier(t, database, job, assignment, 2)
	profile := uuid.New()
	_, err := database.Admin.Exec(`INSERT INTO worker_profile_revisions
 (id,stable_id,revision,state,device_count,member_count,device_set_shape,resident_model_revisions,capacity_limits,readiness_checks,content_digest)
 SELECT $2::uuid,'legacy-capacity-'||$2::uuid::text,1,'CERTIFIED',device_count,member_count,device_set_shape,resident_model_revisions,
 (capacity_limits-'concurrency')||jsonb_build_object('active_stage_slots',(capacity_limits->>'concurrency')::bigint),readiness_checks,sha256($2::uuid::text::bytea)
 FROM worker_profile_revisions WHERE id=(SELECT worker_profile_revision_id FROM worker_instances WHERE id=$1);
 `, assignment.WorkerInstanceID, profile)
	if err != nil {
		t.Fatal(err)
	}
	_, err = database.Admin.Exec(`UPDATE worker_instances SET worker_profile_revision_id=$2 WHERE id=$1`, assignment.WorkerInstanceID, profile)
	if err != nil {
		t.Fatal(err)
	}
	pool := newRolePool(t, database.DSN, "vela_stage_worker_control_login", "vela-stage-worker-control-password")
	backend, err := stageworkercontrol.NewPostgresWorkerEvidenceBackend(pool)
	if err != nil {
		t.Fatal(err)
	}
	command := stageworkercontrol.CommandContext{CommandID: uuid.New(), Identity: stageworkertransport.Identity{SPIFFEID: "spiffe://vela/worker/" + assignment.WorkerInstanceID.String()}, ControlSessionEpoch: 2}
	now := time.Now().UTC().Truncate(time.Microsecond)
	request := &velav1.ReportStageCapacityObservationRequest{WorkerInstanceId: assignment.WorkerInstanceID.String(), WorkerInstanceEpoch: assignment.WorkerInstanceEpoch, ObservationSequence: (assignment.WorkerInstanceEpoch << 32) + 1, CapacityVector: map[string]int64{"concurrency": 1, "active_stage_slots": 1}, ObservedAt: timestamppb.New(now), ExpiresAt: timestamppb.New(now.Add(time.Minute))}
	result, err := backend.ReportCapacityObservation(context.Background(), command, request)
	if err != nil || !result.Ready {
		t.Fatalf("bounded legacy capacity=%+v error=%v", result, err)
	}
	command.CommandID = uuid.New()
	request.ObservationSequence++
	request.CapacityVector["concurrency"] = 2
	_, err = backend.ReportCapacityObservation(context.Background(), command, request)
	assertPostgresConstraint(t, err, "capacity_observation_exceeds_worker_profile")
}
