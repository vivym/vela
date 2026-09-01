package modelruntime_test

import (
	"context"
	"testing"
	"time"

	"github.com/vivym/vela/internal/modelruntime"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
)

func TestSupervisorDiscoversAndRoutesIndependentResidentRuntimes(t *testing.T) {
	clock := newManualClock(time.Date(2026, 9, 1, 6, 0, 0, 0, time.UTC))
	_, validator := runtimeAuthorityCrypto(t, clock)
	encoderBinding := runtimeBinding()
	encoderBinding.ModelResidencyID = "51000000-0000-0000-0000-000000000011"
	encoderBinding.ModelRuntimeIdentity = "h3-encoder-runtime-1"
	encoderBinding.StageProfileRevisionID = "61000000-0000-0000-0000-000000000011"
	encoder := newRuntimeService(
		t, clock, validator, encoderBinding, modelruntime.NewFakeEncoderRuntime(),
	)
	vaeBinding := runtimeBinding()
	vaeBinding.ModelResidencyID = "51000000-0000-0000-0000-000000000012"
	vaeBinding.ModelRuntimeIdentity = "h3-vae-runtime-1"
	vaeBinding.StageProfileRevisionID = "61000000-0000-0000-0000-000000000012"
	vae := newRuntimeService(t, clock, validator, vaeBinding, modelruntime.NewFakeVAERuntime())

	supervisor, err := modelruntime.NewSupervisor(encoder, vae)
	if err != nil {
		t.Fatalf("NewSupervisor: %v", err)
	}
	client, _ := serveRuntimeServer(t, supervisor)
	discovered, err := client.DiscoverRuntimeIdentities(
		context.Background(),
		&velav1.ModelRuntimeServiceDiscoverRuntimeIdentitiesRequest{
			WorkerInstanceId:    encoderBinding.WorkerInstanceID,
			WorkerInstanceEpoch: encoderBinding.WorkerInstanceEpoch,
			WorkerMemberId:      encoderBinding.WorkerMemberID,
			WorkerMemberEpoch:   encoderBinding.WorkerMemberEpoch,
		},
	)
	if err != nil || len(discovered.GetIdentities()) != 2 ||
		discovered.GetIdentities()[0].GetRuntimeIdentity() != "h3-encoder-runtime-1" ||
		discovered.GetIdentities()[1].GetRuntimeIdentity() != "h3-vae-runtime-1" {
		t.Fatalf("DiscoverRuntimeIdentities = %#v error=%v", discovered, err)
	}

	readiness, err := client.ProbeReadiness(
		context.Background(),
		&velav1.ModelRuntimeServiceProbeReadinessRequest{
			Identity: discovered.GetIdentities()[1],
			Check:    velav1.ModelRuntimeReadinessCheck_MODEL_RUNTIME_READINESS_CHECK_MODEL_WARMUP,
		},
	)
	if err != nil || !readiness.GetReady() ||
		readiness.GetIdentity().GetRuntimeIdentity() != "h3-vae-runtime-1" {
		t.Fatalf("routed ProbeReadiness = %#v error=%v", readiness, err)
	}
}

func TestSupervisorRejectsAmbiguousOrCrossWorkerRuntimeSets(t *testing.T) {
	clock := newManualClock(time.Date(2026, 9, 1, 6, 0, 0, 0, time.UTC))
	_, validator := runtimeAuthorityCrypto(t, clock)
	first := newRuntimeService(
		t, clock, validator, runtimeBinding(), modelruntime.NewFakeDiTRuntime(),
	)
	changed := runtimeBinding()
	changed.WorkerInstanceID = "21000000-0000-0000-0000-000000000099"
	second := newRuntimeService(
		t, clock, validator, changed, modelruntime.NewFakeDiTRuntime(),
	)
	if _, err := modelruntime.NewSupervisor(first, second); err == nil {
		t.Fatal("NewSupervisor accepted runtimes from different WorkerInstances")
	}
}
