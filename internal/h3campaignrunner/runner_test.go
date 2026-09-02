package h3campaignrunner

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	api "github.com/vivym/vela/api/gen"
	"github.com/vivym/vela/internal/h3campaignevidence"
)

func TestRunSubmitsExactJobsSequentiallyAndCapturesSelection(t *testing.T) {
	manifest := validManifest()
	client := &fakeJobClient{
		projectID: manifest.ProjectID,
		states: map[string][]api.JobState{
			manifest.SameNodeIdempotencyKey:  {api.JobStateQUEUED, api.JobStateSUCCEEDED},
			manifest.CrossNodeIdempotencyKey: {api.JobStateRUNNING, api.JobStateSUCCEEDED},
			manifest.CacheIdempotencyKey:     {api.JobStateSUCCEEDED},
		},
	}
	waits := 0
	evidence, err := Run(context.Background(), Runner{
		Client: client,
		Wait: func(context.Context, time.Duration) error {
			waits++
			return nil
		},
		Capture: func(
			_ context.Context,
			selection h3campaignevidence.Selection,
		) (h3campaignevidence.Evidence, error) {
			if selection.SameNodeJobID != client.jobIDs[0] ||
				selection.CrossNodeJobID != client.jobIDs[1] ||
				selection.CacheJobID != client.jobIDs[2] {
				t.Fatalf("campaign selection = %#v, jobs = %#v", selection, client.jobIDs)
			}
			return h3campaignevidence.Evidence{SchemaVersion: h3campaignevidence.SchemaVersion}, nil
		},
	}, manifest)
	if err != nil || evidence.SchemaVersion != h3campaignevidence.SchemaVersion || waits != 2 {
		t.Fatalf("Run evidence = %#v waits=%d error=%v", evidence, waits, err)
	}
	if len(client.requests) != 3 {
		t.Fatalf("submitted requests = %d", len(client.requests))
	}
	for _, request := range client.requests {
		if request != manifest.Request {
			t.Fatalf("campaign changed exact Job request: %#v", request)
		}
	}
}

func TestRunStopsBeforeLaterScenariosWhenJobFails(t *testing.T) {
	manifest := validManifest()
	client := &fakeJobClient{
		projectID: manifest.ProjectID,
		states: map[string][]api.JobState{
			manifest.SameNodeIdempotencyKey: {api.JobStateFAILED},
		},
	}
	captured := false
	_, err := Run(context.Background(), Runner{
		Client: client,
		Capture: func(context.Context, h3campaignevidence.Selection) (h3campaignevidence.Evidence, error) {
			captured = true
			return h3campaignevidence.Evidence{}, nil
		},
	}, manifest)
	if err == nil || captured || len(client.requests) != 1 {
		t.Fatalf("failed campaign error=%v captured=%t submissions=%d", err, captured, len(client.requests))
	}
}

func TestRunHonorsJobTimeout(t *testing.T) {
	manifest := validManifest()
	client := &fakeJobClient{
		projectID: manifest.ProjectID,
		states: map[string][]api.JobState{
			manifest.SameNodeIdempotencyKey: {api.JobStateQUEUED},
		},
	}
	_, err := Run(context.Background(), Runner{
		Client: client,
		Wait:   func(context.Context, time.Duration) error { return context.DeadlineExceeded },
		Capture: func(context.Context, h3campaignevidence.Selection) (h3campaignevidence.Evidence, error) {
			return h3campaignevidence.Evidence{}, errors.New("must not capture")
		},
	}, manifest)
	if !errors.Is(err, context.DeadlineExceeded) || len(client.requests) != 1 {
		t.Fatalf("timeout error=%v submissions=%d", err, len(client.requests))
	}
}

func TestValidateManifestRejectsAmbiguousCampaign(t *testing.T) {
	tests := []func(*Manifest){
		func(manifest *Manifest) { manifest.SchemaVersion = 0 },
		func(manifest *Manifest) { manifest.ProjectID = uuid.Nil },
		func(manifest *Manifest) { manifest.Request.Prompt = "" },
		func(manifest *Manifest) { manifest.CrossNodeIdempotencyKey = manifest.SameNodeIdempotencyKey },
		func(manifest *Manifest) { manifest.PollIntervalMilliseconds = 0 },
		func(manifest *Manifest) { manifest.JobTimeoutSeconds = 0 },
	}
	for index, mutate := range tests {
		manifest := validManifest()
		mutate(&manifest)
		if _, _, err := ValidateManifest(manifest); err == nil {
			t.Fatalf("invalid manifest %d accepted", index)
		}
	}
}

type fakeJobClient struct {
	projectID uuid.UUID
	states    map[string][]api.JobState
	requests  []api.SubmitJobRequest
	jobIDs    []uuid.UUID
	byID      map[uuid.UUID][]api.JobState
}

func (client *fakeJobClient) SubmitJob(
	_ context.Context,
	projectID uuid.UUID,
	idempotencyKey string,
	request api.SubmitJobRequest,
) (api.Job, error) {
	if projectID != client.projectID {
		return api.Job{}, errors.New("wrong Project")
	}
	states, ok := client.states[idempotencyKey]
	if !ok || len(states) == 0 {
		return api.Job{}, errors.New("unexpected idempotency key")
	}
	id := uuid.New()
	client.requests = append(client.requests, request)
	client.jobIDs = append(client.jobIDs, id)
	if client.byID == nil {
		client.byID = make(map[uuid.UUID][]api.JobState)
	}
	client.byID[id] = append([]api.JobState(nil), states[1:]...)
	return api.Job{JobId: id, ProjectId: projectID, State: states[0]}, nil
}

func (client *fakeJobClient) GetJob(
	_ context.Context,
	projectID uuid.UUID,
	jobID uuid.UUID,
) (api.Job, error) {
	states := client.byID[jobID]
	if len(states) == 0 {
		return api.Job{}, errors.New("no next state")
	}
	state := states[0]
	client.byID[jobID] = states[1:]
	return api.Job{JobId: jobID, ProjectId: projectID, State: state}, nil
}

func validManifest() Manifest {
	return Manifest{
		SchemaVersion: SchemaVersion,
		ProjectID:     uuid.MustParse("49350000-0000-0000-0000-000000000101"),
		Request: api.SubmitJobRequest{
			Model: "minimax-h3", GenerationPreset: api.Quality,
			ServiceClass: api.Standard, OutputSpec: "video-1080p-5s-24fps",
			GenerationCount: 1, Prompt: "fixed certified campaign input",
		},
		SameNodeIdempotencyKey:   "h3-campaign-same-node-v1",
		CrossNodeIdempotencyKey:  "h3-campaign-cross-node-v1",
		CacheIdempotencyKey:      "h3-campaign-cache-v1",
		PollIntervalMilliseconds: 100,
		JobTimeoutSeconds:        3600,
	}
}
