package h3campaignrunner

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"time"
	"unicode/utf8"

	"github.com/google/uuid"
	api "github.com/vivym/vela/api/gen"
	"github.com/vivym/vela/internal/h3campaignevidence"
)

const (
	SchemaVersion       = 1
	maximumJobTimeout   = 7 * 24 * time.Hour
	maximumPollInterval = time.Minute
)

var idempotencyKeyPattern = regexp.MustCompile(`^[A-Za-z0-9._:-]+$`)

type Manifest struct {
	SchemaVersion            int                  `json:"schema_version"`
	ProjectID                uuid.UUID            `json:"project_id"`
	Request                  api.SubmitJobRequest `json:"request"`
	SameNodeIdempotencyKey   string               `json:"same_node_idempotency_key"`
	CrossNodeIdempotencyKey  string               `json:"cross_node_idempotency_key"`
	CacheIdempotencyKey      string               `json:"cache_idempotency_key"`
	PollIntervalMilliseconds int64                `json:"poll_interval_milliseconds"`
	JobTimeoutSeconds        int64                `json:"job_timeout_seconds"`
}

type JobClient interface {
	SubmitJob(context.Context, uuid.UUID, string, api.SubmitJobRequest) (api.Job, error)
	GetJob(context.Context, uuid.UUID, uuid.UUID) (api.Job, error)
}

type EvidenceCapture func(
	context.Context,
	h3campaignevidence.Selection,
) (h3campaignevidence.Evidence, error)

type WaitFunc func(context.Context, time.Duration) error

type Runner struct {
	Client  JobClient
	Capture EvidenceCapture
	Wait    WaitFunc
}

func Run(
	ctx context.Context,
	runner Runner,
	manifest Manifest,
) (h3campaignevidence.Evidence, error) {
	if ctx == nil || runner.Client == nil || runner.Capture == nil {
		return h3campaignevidence.Evidence{}, errors.New("h3 campaign runner is not configured")
	}
	pollInterval, jobTimeout, err := ValidateManifest(manifest)
	if err != nil {
		return h3campaignevidence.Evidence{}, err
	}
	wait := runner.Wait
	if wait == nil {
		wait = waitFor
	}

	selection := h3campaignevidence.Selection{}
	jobs := []struct {
		name           string
		idempotencyKey string
		setID          func(uuid.UUID)
	}{
		{name: "same-node", idempotencyKey: manifest.SameNodeIdempotencyKey,
			setID: func(id uuid.UUID) { selection.SameNodeJobID = id }},
		{name: "cross-node", idempotencyKey: manifest.CrossNodeIdempotencyKey,
			setID: func(id uuid.UUID) { selection.CrossNodeJobID = id }},
		{name: "cache", idempotencyKey: manifest.CacheIdempotencyKey,
			setID: func(id uuid.UUID) { selection.CacheJobID = id }},
	}
	for _, candidate := range jobs {
		job, err := runner.Client.SubmitJob(
			ctx, manifest.ProjectID, candidate.idempotencyKey, manifest.Request,
		)
		if err != nil {
			return h3campaignevidence.Evidence{}, fmt.Errorf(
				"submit %s H3 campaign Job: %w", candidate.name, err,
			)
		}
		if err := validateJobIdentity(job, manifest.ProjectID); err != nil {
			return h3campaignevidence.Evidence{}, fmt.Errorf(
				"submit %s H3 campaign Job: %w", candidate.name, err,
			)
		}
		candidate.setID(uuid.UUID(job.JobId))
		jobContext, cancel := context.WithTimeout(ctx, jobTimeout)
		job, err = waitForSuccess(
			jobContext, runner.Client, wait, manifest.ProjectID, job, pollInterval,
		)
		cancel()
		if err != nil {
			return h3campaignevidence.Evidence{}, fmt.Errorf(
				"wait for %s H3 campaign Job %s: %w", candidate.name, job.JobId, err,
			)
		}
	}

	evidence, err := runner.Capture(ctx, selection)
	if err != nil {
		return h3campaignevidence.Evidence{}, fmt.Errorf(
			"capture completed H3 campaign: %w", err,
		)
	}
	return evidence, nil
}

func ValidateManifest(manifest Manifest) (time.Duration, time.Duration, error) {
	if manifest.SchemaVersion != SchemaVersion || manifest.ProjectID == uuid.Nil {
		return 0, 0, errors.New("h3 campaign manifest identity is invalid")
	}
	if err := validateRequest(manifest.Request); err != nil {
		return 0, 0, err
	}
	keys := []string{
		manifest.SameNodeIdempotencyKey,
		manifest.CrossNodeIdempotencyKey,
		manifest.CacheIdempotencyKey,
	}
	seen := make(map[string]struct{}, len(keys))
	for _, key := range keys {
		if len(key) == 0 || len(key) > 128 || !idempotencyKeyPattern.MatchString(key) {
			return 0, 0, errors.New("h3 campaign idempotency key is invalid")
		}
		if _, duplicate := seen[key]; duplicate {
			return 0, 0, errors.New("h3 campaign idempotency keys must be distinct")
		}
		seen[key] = struct{}{}
	}
	if manifest.PollIntervalMilliseconds < 100 {
		return 0, 0, errors.New("h3 campaign poll interval must be at least 100 milliseconds")
	}
	pollInterval := time.Duration(manifest.PollIntervalMilliseconds) * time.Millisecond
	if pollInterval > maximumPollInterval {
		return 0, 0, errors.New("h3 campaign poll interval exceeds one minute")
	}
	if manifest.JobTimeoutSeconds <= 0 {
		return 0, 0, errors.New("h3 campaign Job timeout must be positive")
	}
	jobTimeout := time.Duration(manifest.JobTimeoutSeconds) * time.Second
	if jobTimeout > maximumJobTimeout || pollInterval >= jobTimeout {
		return 0, 0, errors.New("h3 campaign Job timeout is invalid")
	}
	return pollInterval, jobTimeout, nil
}

func validateRequest(request api.SubmitJobRequest) error {
	if request.Model == "" || len(request.Model) > 100 ||
		request.OutputSpec == "" || len(request.OutputSpec) > 100 ||
		!request.GenerationPreset.Valid() || !request.ServiceClass.Valid() ||
		request.GenerationCount < 1 || request.GenerationCount > 16 ||
		utf8.RuneCountInString(request.Prompt) < 1 || utf8.RuneCountInString(request.Prompt) > 20000 {
		return errors.New("h3 campaign Job request is invalid")
	}
	return nil
}

func validateJobIdentity(job api.Job, projectID uuid.UUID) error {
	if uuid.UUID(job.JobId) == uuid.Nil || uuid.UUID(job.ProjectId) != projectID || !job.State.Valid() {
		return errors.New("vela API returned an invalid H3 campaign Job identity")
	}
	return nil
}

func waitForSuccess(
	ctx context.Context,
	client JobClient,
	wait WaitFunc,
	projectID uuid.UUID,
	job api.Job,
	pollInterval time.Duration,
) (api.Job, error) {
	for {
		if err := validateJobIdentity(job, projectID); err != nil {
			return job, err
		}
		switch job.State {
		case api.JobStateSUCCEEDED:
			return job, nil
		case api.JobStateFAILED, api.JobStateCANCELED:
			return job, fmt.Errorf("job reached terminal state %s", job.State)
		}
		if err := wait(ctx, pollInterval); err != nil {
			return job, err
		}
		refreshed, err := client.GetJob(ctx, projectID, uuid.UUID(job.JobId))
		if err != nil {
			return job, fmt.Errorf("read authoritative Job state: %w", err)
		}
		if refreshed.JobId != job.JobId || refreshed.ProjectId != job.ProjectId {
			return job, errors.New("vela API changed H3 campaign Job identity while polling")
		}
		job = refreshed
	}
}

func waitFor(ctx context.Context, duration time.Duration) error {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
