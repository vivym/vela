//go:build integration

package integration_test

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/pressly/goose/v3"
	"github.com/vivym/vela/internal/stagescheduler"
)

func TestStageAdmissionQueuesBehindLiveBusyWorker(t *testing.T) {
	for _, mode := range []string{"live", "expired-lease", "revoked-lease", "withdrawn", "disconnected", "full-queue"} {
		t.Run(mode, func(t *testing.T) {
			f := newStageSchedulerFixture(t, "busy-admission-"+mode)
			leaseTTL := time.Minute
			localTTL := 50 * time.Second
			if mode == "expired-lease" {
				leaseTTL = 2 * time.Second
				localTTL = time.Second
			}
			service, err := stagescheduler.NewService(f.repository, f.coordinator, stagescheduler.Config{
				SchedulerID: "stage-scheduler/busy-admission", ClaimTTL: 30 * time.Second,
				LeaseTTL: leaseTTL, LocalDeadlineTTL: localTTL, SigningKeyID: "stage-authority-key-v1",
			})
			if err != nil {
				t.Fatal(err)
			}
			assignment, ok, err := service.Acquire(context.Background(), f.authority, f.observation)
			if err != nil || !ok {
				t.Fatalf("acquire initial assignment: %v %v", ok, err)
			}
			if _, err = f.database.Admin.Exec(`UPDATE capacity_observations SET observed_at=clock_timestamp()-interval '2 minutes',expires_at=clock_timestamp()-interval '1 minute' WHERE worker_instance_id IN (SELECT worker_instance_id FROM model_residencies WHERE model_component_revision IN (SELECT model_component_revision FROM model_residencies WHERE worker_instance_id=$1))`, f.authority.WorkerInstanceID); err != nil {
				t.Fatal(err)
			}
			switch mode {
			case "expired-lease":
				var expiresAt time.Time
				if err := f.database.Admin.QueryRow(`SELECT vela_stage_lease_effective_expires_at($1)`, assignment.StageLeaseID).Scan(&expiresAt); err != nil {
					t.Fatal(err)
				}
				if delay := time.Until(expiresAt.Add(10 * time.Millisecond)); delay > 0 {
					time.Sleep(delay)
				}
			case "revoked-lease":
				tx, err := f.database.Admin.Begin()
				if err != nil {
					t.Fatal(err)
				}
				defer tx.Rollback()
				if _, err = tx.Exec(`SET LOCAL ROLE vela_attempt_coordinator_owner`); err != nil {
					t.Fatal(err)
				}
				if _, err = tx.Exec(`UPDATE stage_leases SET state='REVOKED',revoked_at=clock_timestamp(),revoke_reason='integration' WHERE id=$1`, assignment.StageLeaseID); err != nil {
					t.Fatal(err)
				}
				if err = tx.Commit(); err != nil {
					t.Fatal(err)
				}
			case "withdrawn":
				if _, err = f.database.Admin.Exec(`INSERT INTO capacity_observations (worker_instance_id,worker_instance_epoch,observation_sequence,capacity_vector,observed_at,expires_at,observed_by) SELECT worker_instance_id,worker_instance_epoch,observation_sequence+1,'{"concurrency":0}',clock_timestamp(),clock_timestamp()+interval '1 minute','integration/withdraw' FROM capacity_observations WHERE worker_instance_id=$1`, f.authority.WorkerInstanceID); err != nil {
					t.Fatal(err)
				}
			case "disconnected":
				if _, err = f.database.Admin.Exec(`UPDATE worker_instances SET reachability_state='DISCONNECTED' WHERE id=$1`, f.authority.WorkerInstanceID); err != nil {
					t.Fatal(err)
				}
			case "full-queue":
				if _, err = f.database.Admin.Exec(`UPDATE capacity_pools SET max_ready_queue_depth=1 WHERE id=$1`, f.authority.CapacityPoolID); err != nil {
					t.Fatal(err)
				}
			}
			server := admissionServerForDatabase(t, f.database)
			body := []byte(`{"model":"minimax-h3","generation_preset":"balanced","service_class":"standard","output_spec":"video-1080p-5s-24fps","generation_count":1,"prompt":"queue behind occupied healthy stage"}`)
			before := readStageAdmissionEffectCounts(t, f.database.Admin)
			response := submitJob(t, server.URL, "busy-admission-next", body)
			want := 503
			if mode == "live" || mode == "full-queue" {
				want = 202
			}
			if response.StatusCode != want {
				t.Fatalf("busy admission status=%d body=%s want=%d", response.StatusCode, response.Body, want)
			}
			if want == 503 {
				if after := readStageAdmissionEffectCounts(t, f.database.Admin); after != before {
					t.Fatalf("rejected admission mutated state: %v -> %v", before, after)
				}
			} else {
				// Accepting a queued job must not advertise a free execution slot.
				_, acquired, _ := service.Acquire(context.Background(), f.authority, f.observation)
				if acquired {
					t.Fatal("busy worker acquired a second allocation")
				}
				if mode == "full-queue" {
					rejected := submitJob(t, server.URL, "busy-admission-overflow", body)
					if rejected.StatusCode != 503 {
						t.Fatalf("bounded queue overflow=%d body=%s", rejected.StatusCode, rejected.Body)
					}
				}
			}
		})
	}
}

func TestStageBusyAdmissionMigrationRoundTrip(t *testing.T) {
	database := newPostgres(t)
	applyFoundationTo(t, database.Admin, 107)
	migrations := filepath.Join(repositoryRoot(t), "db", "migrations")
	var original string
	const identitySQL = `SELECT json_build_array(oid,proowner,proacl)::text FROM pg_proc WHERE oid='vela_lock_stage_graph_ready_capacity_path(uuid,uuid)'::regprocedure`
	if err := database.Admin.QueryRow(identitySQL).Scan(&original); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if err := goose.UpTo(database.Admin, migrations, 108); err != nil {
			t.Fatal(err)
		}
		var identity string
		if err := database.Admin.QueryRow(identitySQL).Scan(&identity); err != nil || identity != original {
			t.Fatalf("admission privilege identity changed: %s %v", identity, err)
		}
		var exposed bool
		if err := database.Admin.QueryRow(`SELECT EXISTS(SELECT 1 FROM pg_roles WHERE rolcanlogin AND NOT rolsuper AND has_function_privilege(oid,'vela_worker_has_live_stage_allocation(uuid,bigint,bigint)','EXECUTE'))`).Scan(&exposed); err != nil || exposed {
			t.Fatalf("private allocation reader exposed: %t %v", exposed, err)
		}
		if err := goose.DownTo(database.Admin, migrations, 107); err != nil {
			t.Fatal(err)
		}
		if err := database.Admin.QueryRow(identitySQL).Scan(&identity); err != nil || identity != original {
			t.Fatalf("rollback changed privileges: %s %v", identity, err)
		}
	}
}
