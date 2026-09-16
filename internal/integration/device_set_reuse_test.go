//go:build integration

package integration_test

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/fleet"
)

func TestWorkerRegistryReusesCanonicalDeviceSet(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	seedAdmissionFixture(t, database.Admin)
	seedStageExecutionCatalog(t, database.Admin)
	seedWorkerRegistryPlan(t, database.Admin)
	pool := newRolePool(t, database.DSN, "vela_fleet_login", "vela-fleet-password")
	service, err := fleet.NewService(pool)
	if err != nil {
		t.Fatal(err)
	}
	workerID := uuid.New()
	seedWorkerInstance(t, database.Admin, workerID, workerRegistryProfileID, 1, 1)
	evidence := workerRegistryEvidenceValue(t, workerID, 0x12)
	canonical := uuid.New()
	_, err = database.Admin.Exec(`INSERT INTO device_sets (id,membership_digest,topology_digest,device_count) VALUES ($1,decode($2,'hex'),decode($3,'hex'),1)`, canonical, evidence.DeviceSet.MembershipDigest, evidence.DeviceSet.TopologyDigest)
	if err != nil {
		t.Fatal(err)
	}
	// A new Worker incarnation proposes a fresh local ID for the same physical set.
	if _, err = service.Observe(context.Background(), evidence); err != nil {
		t.Fatalf("reuse physical DeviceSet: %v", err)
	}
	var stored uuid.UUID
	if err = database.Admin.QueryRow(`SELECT device_set_id FROM worker_instances WHERE id=$1`, workerID).Scan(&stored); err != nil {
		t.Fatal(err)
	}
	if stored != canonical {
		t.Fatalf("device set = %s, want canonical %s", stored, canonical)
	}
	evidence.Capacity.Sequence++
	if _, err = service.Observe(context.Background(), evidence); err != nil {
		t.Fatalf("refresh canonical DeviceSet: %v", err)
	}
	// Reusing a known ID with different digests must still fail, even if the
	// requested digests already belong to another canonical set.
	conflictingID := uuid.New()
	_, err = database.Admin.Exec(`INSERT INTO device_sets (id,membership_digest,topology_digest,device_count) VALUES ($1,sha256('other membership'::bytea),sha256('other topology'::bytea),1)`, conflictingID)
	if err != nil {
		t.Fatal(err)
	}
	evidence.DeviceSet.ID = conflictingID
	evidence.Capacity.Sequence++
	_, err = pool.Exec(context.Background(), `SELECT * FROM vela_observe_worker_instance($1::jsonb)`, mustJSON(t, evidence))
	assertPostgresConstraint(t, err, "device_set_identity_conflict")
}
