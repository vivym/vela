package workeragent

import (
	"context"
	"fmt"
	"time"
)

type capacityReportSession struct {
	cancel context.CancelFunc
	done   chan struct{}
}

func (agent *Agent) startCapacityReports(ctx context.Context) *capacityReportSession {
	runContext, cancel := context.WithCancel(ctx)
	session := &capacityReportSession{cancel: cancel, done: make(chan struct{})}
	go func() {
		defer close(session.done)
		for {
			timer := time.NewTimer(agent.capacityReportInterval)
			select {
			case <-runContext.Done():
				if !timer.Stop() {
					select {
					case <-timer.C:
					default:
					}
				}
				return
			case <-timer.C:
			}
			watermark, err := agent.recovery.Watermark(runContext)
			if err == nil {
				_, err = agent.reportCapacity(runContext, watermark)
			}
			if err != nil {
				if runContext.Err() != nil {
					return
				}
				agent.reportCapacityError(fmt.Errorf("refresh BUSY Worker capacity: %w", err))
			}
		}
	}()
	return session
}

func (session *capacityReportSession) Stop() {
	if session == nil {
		return
	}
	session.cancel()
	<-session.done
}
