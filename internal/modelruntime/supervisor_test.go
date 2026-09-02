package modelruntime_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/vivym/vela/internal/modelruntime"
	"github.com/vivym/vela/internal/stageauthority"
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

func TestSupervisorAllowsOnlyOneActiveRuntimeInSharedSlot(t *testing.T) {
	clock := newManualClock(time.Date(2026, 9, 1, 6, 30, 0, 0, time.UTC))
	signer, validator := runtimeAuthorityCrypto(t, clock)
	encoderBinding := runtimeBinding()
	encoderBinding.ModelResidencyID = "51000000-0000-0000-0000-000000000011"
	encoderBinding.ModelRuntimeIdentity = "h3-encoder-runtime-1"
	encoderBinding.StageProfileRevisionID = "61000000-0000-0000-0000-000000000011"
	vaeBinding := runtimeBinding()
	vaeBinding.ModelResidencyID = "51000000-0000-0000-0000-000000000012"
	vaeBinding.ModelRuntimeIdentity = "h3-vae-runtime-1"
	vaeBinding.StageProfileRevisionID = "61000000-0000-0000-0000-000000000012"
	supervisor, err := modelruntime.NewSupervisor(
		newRuntimeService(t, clock, validator, encoderBinding, modelruntime.NewFakeEncoderRuntime()),
		newRuntimeService(t, clock, validator, vaeBinding, modelruntime.NewFakeVAERuntime()),
	)
	if err != nil {
		t.Fatalf("NewSupervisor: %v", err)
	}
	client, _ := serveRuntimeServer(t, supervisor)
	authorities := []*velav1.StageAuthority{
		signSupervisorAuthority(t, signer, clock.Now(), encoderBinding, "11"),
		signSupervisorAuthority(t, signer, clock.Now(), vaeBinding, "12"),
	}

	start := make(chan struct{})
	responses := make(chan *velav1.ModelRuntimeServicePrepareStageResponse, len(authorities))
	errorsSeen := make(chan error, len(authorities))
	var requests sync.WaitGroup
	for _, authority := range authorities {
		requests.Add(1)
		go func() {
			defer requests.Done()
			<-start
			response, prepareErr := client.PrepareStage(
				context.Background(),
				&velav1.ModelRuntimeServicePrepareStageRequest{
					Authority: authority, ExecutionSpec: runtimeExecutionSpec(),
				},
			)
			responses <- response
			errorsSeen <- prepareErr
		}()
	}
	close(start)
	requests.Wait()
	close(responses)
	close(errorsSeen)
	for prepareErr := range errorsSeen {
		if prepareErr != nil {
			t.Fatalf("PrepareStage: %v", prepareErr)
		}
	}
	accepted := 0
	rejected := 0
	for response := range responses {
		switch response.GetDecision() {
		case velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_ACCEPTED:
			accepted++
		case velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_REJECTED:
			rejected++
		default:
			t.Fatalf("PrepareStage decision = %s", response.GetDecision())
		}
	}
	if accepted != 1 || rejected != 1 {
		t.Fatalf("shared slot decisions: accepted=%d rejected=%d", accepted, rejected)
	}
}

func signSupervisorAuthority(
	t *testing.T,
	signer *stageauthority.Signer,
	now time.Time,
	binding stageauthority.RuntimeBinding,
	idSuffix string,
) *velav1.StageAuthority {
	t.Helper()
	authority := signRuntimeAuthority(t, signer, now)
	authority.StageAttemptId = "11000000-0000-0000-0000-0000000000" + idSuffix
	authority.StageAllocationId = "12000000-0000-0000-0000-0000000000" + idSuffix
	authority.StageLeaseId = "13000000-0000-0000-0000-0000000000" + idSuffix
	authority.ModelResidencyId = binding.ModelResidencyID
	authority.ModelRuntimeIdentity = binding.ModelRuntimeIdentity
	authority.StageProfileRevisionId = binding.StageProfileRevisionID
	authority.Members[0].ModelRuntimeEpoch = binding.ModelRuntimeEpoch
	signed, err := signer.Sign(authority)
	if err != nil {
		t.Fatalf("sign supervisor StageAuthority: %v", err)
	}
	return signed
}
