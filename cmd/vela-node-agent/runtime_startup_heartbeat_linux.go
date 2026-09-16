package main

import (
	"context"
	"time"

	"github.com/vivym/vela/internal/nodeagent"
)

// Reservation and image measurement precede grant activation. Keep custody
// responsive during that interval without granting backend execution authority.
// Stop joins an in-flight check instead of canceling it, since canceling a
// custody exchange must revoke the original process.
func maintainRuntimeStartupCustody(custody *nodeagent.RuntimeObserverCustody) func() {
	stop, done := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				return
			case <-ticker.C:
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				err := custody.Check(ctx)
				cancel()
				if err != nil {
					return
				}
			}
		}
	}()
	return func() { close(stop); <-done }
}
