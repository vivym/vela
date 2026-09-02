package nodeagent

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/fleet"
)

const maxWorkerInstanceReportingTemplates = 1024

type WorkerInstanceEvidenceReportRunner interface {
	Report(
		context.Context,
		WorkerInstanceEvidenceTemplate,
	) (fleet.WorkerInstanceDecision, error)
}

type WorkerInstanceReportResult struct {
	WorkerInstanceID uuid.UUID
	Decision         fleet.WorkerInstanceDecision
	Err              error
}

type WorkerInstanceReportingConfig struct {
	Interval       time.Duration
	CallTimeout    time.Duration
	InitialBackoff time.Duration
	MaxBackoff     time.Duration
	ObserveResult  func(WorkerInstanceReportResult)
}

func RunWorkerInstanceEvidenceReporting(
	ctx context.Context,
	reporter WorkerInstanceEvidenceReportRunner,
	templates []WorkerInstanceEvidenceTemplate,
	config WorkerInstanceReportingConfig,
) error {
	if ctx == nil || reporter == nil || len(templates) == 0 ||
		len(templates) > maxWorkerInstanceReportingTemplates ||
		config.Interval <= 0 || config.CallTimeout <= 0 || config.InitialBackoff <= 0 ||
		config.MaxBackoff < config.InitialBackoff || config.ObserveResult == nil {
		return errors.New("WorkerInstance reporting configuration is invalid")
	}
	seen := make(map[uuid.UUID]struct{}, len(templates))
	for _, template := range templates {
		workerInstanceID := template.Evidence.WorkerInstanceID
		if workerInstanceID == uuid.Nil {
			return errors.New("WorkerInstance reporting template identity is invalid")
		}
		if _, duplicate := seen[workerInstanceID]; duplicate {
			return errors.New("WorkerInstance reporting template is duplicated")
		}
		seen[workerInstanceID] = struct{}{}
	}

	var workers sync.WaitGroup
	workers.Add(len(templates))
	for _, template := range templates {
		template := template
		go func() {
			defer workers.Done()
			runWorkerInstanceEvidenceReporting(ctx, reporter, template, config)
		}()
	}
	workers.Wait()
	return nil
}

func runWorkerInstanceEvidenceReporting(
	ctx context.Context,
	reporter WorkerInstanceEvidenceReportRunner,
	template WorkerInstanceEvidenceTemplate,
	config WorkerInstanceReportingConfig,
) {
	delay := time.Duration(0)
	backoff := config.InitialBackoff
	for {
		if !waitWorkerInstanceReportDelay(ctx, delay) {
			return
		}
		callContext, cancel := context.WithTimeout(ctx, config.CallTimeout)
		decision, err := reporter.Report(callContext, template)
		cancel()
		config.ObserveResult(WorkerInstanceReportResult{
			WorkerInstanceID: template.Evidence.WorkerInstanceID,
			Decision:         decision,
			Err:              err,
		})
		if ctx.Err() != nil {
			return
		}
		if err != nil {
			delay = backoff
			backoff = nextWorkerInstanceReportBackoff(backoff, config.MaxBackoff)
			continue
		}
		delay = config.Interval
		backoff = config.InitialBackoff
	}
}

func waitWorkerInstanceReportDelay(ctx context.Context, delay time.Duration) bool {
	if delay <= 0 {
		select {
		case <-ctx.Done():
			return false
		default:
			return true
		}
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func nextWorkerInstanceReportBackoff(current time.Duration, maximum time.Duration) time.Duration {
	if current >= maximum || current > maximum/2 {
		return maximum
	}
	return current * 2
}

var _ WorkerInstanceEvidenceReportRunner = (*WorkerInstanceEvidenceReporter)(nil)
