package nodeagent

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/vivym/vela/internal/modelruntime"
)

func issueJournalTestGrant(t *testing.T, f journalEndpointFixture) *JournalWriteGrant {
	t.Helper()
	grant, err := IssueJournalWriteGrant(t.Context(), f.endpoint, f.runtime.owner, f.worker.owner, time.Minute)
	if err != nil || grant == nil {
		t.Fatalf("issue grant from original owners: %v", err)
	}
	return grant
}

func TestJournalReadOnlyGrantTransition(t *testing.T) {
	f := newJournalEndpointOwnerFixture(t, 0, nil, true, true)
	admit := modelruntime.JournalCommand{Admit: &modelruntime.JournalAuthorityCommand{Authority: f.authority}}
	floor := modelruntime.JournalCommand{Floor: &modelruntime.JournalFloorCommand{Disposition: f.floor}}
	checkWrite := func(child *journalEndpointChild, command modelruntime.JournalCommand, allowed bool) {
		t.Helper()
		report := journalEndpointExchange(t, f.endpoint, f.listener, child, f.identity, command, false)
		if (report.Error == "") != allowed {
			t.Fatalf("write allowed=%v: %+v", allowed, report)
		}
	}
	checkWrite(f.runtime, admit, false)
	checkWrite(f.worker, floor, false)
	grant := issueJournalTestGrant(t, f)
	// Issuance alone cannot open the route.
	checkWrite(f.runtime, admit, false)
	checkWrite(f.worker, floor, false)
	// Activation uses the endpoint's independent original handles even after
	// enrollment observations are closed; it must never reopen a numeric PID.
	if err := f.runtime.owner.Close(); err != nil {
		t.Fatal(err)
	}
	if err := f.worker.owner.Close(); err != nil {
		t.Fatal(err)
	}
	if err := f.endpoint.ActivateJournalWriteGrant(t.Context(), grant); err != nil {
		t.Fatal(err)
	}
	checkWrite(f.worker, admit, false)
	checkWrite(f.sibling, admit, false)
	checkWrite(f.runtime, floor, false)
	checkWrite(f.runtime, admit, true)
	checkWrite(f.worker, floor, true)
	if status, err := f.owner.Status(t.Context()); err != nil || status.Highest != 1 || status.Floor != 1 {
		t.Fatalf("grant did not enable durable role-specific writes: %+v %v", status, err)
	}
	if err := f.endpoint.ActivateJournalWriteGrant(t.Context(), grant); err == nil {
		t.Fatal("single-use grant activated twice")
	}
	if _, err := IssueJournalWriteGrant(t.Context(), f.endpoint, f.runtime.owner, f.worker.owner, time.Minute); err == nil {
		t.Fatal("issued grant after activation")
	}
}

func TestJournalWriteGrantRejectsInvalidIssuance(t *testing.T) {
	f := newJournalEndpointOwnerFixture(t, 0, nil, true, true)
	cancelled, cancel := context.WithCancel(t.Context())
	cancel()
	for _, tc := range []struct {
		name            string
		ctx             context.Context
		runtime, worker *RuntimeNamespaceOwner
		lifetime        time.Duration
	}{
		{"cancelled", cancelled, f.runtime.owner, f.worker.owner, time.Minute},
		{"nil-context", nil, f.runtime.owner, f.worker.owner, time.Minute},
		{"nil-owner", t.Context(), nil, f.worker.owner, time.Minute},
		{"swapped-roles", t.Context(), f.worker.owner, f.runtime.owner, time.Minute},
		{"same-uid-sibling", t.Context(), f.sibling.owner, f.worker.owner, time.Minute},
		{"zero-lifetime", t.Context(), f.runtime.owner, f.worker.owner, 0},
		{"excess-lifetime", t.Context(), f.runtime.owner, f.worker.owner, 5*time.Minute + time.Nanosecond},
	} {
		t.Run(tc.name, func(t *testing.T) {
			grant, err := IssueJournalWriteGrant(tc.ctx, f.endpoint, tc.runtime, tc.worker, tc.lifetime)
			if err == nil || grant != nil {
				t.Fatal("invalid issuance succeeded")
			}
		})
	}
	grant := issueJournalTestGrant(t, f)
	if err := f.endpoint.ActivateJournalWriteGrant(t.Context(), grant); err != nil {
		t.Fatalf("rejected issuance poisoned valid transition: %v", err)
	}
}

func TestJournalWriteGrantRejectsInvalidActivation(t *testing.T) {
	for _, scenario := range []string{"expired", "wrong-endpoint", "cancelled", "nil-context", "closed-endpoint", "runtime-exited", "worker-exited"} {
		t.Run(scenario, func(t *testing.T) {
			f := newJournalEndpointOwnerFixture(t, 0, nil, true, true)
			grant := issueJournalTestGrant(t, f)
			ctx := t.Context()
			switch scenario {
			case "expired":
				grant.expires = time.Now().Add(-time.Second)
			case "wrong-endpoint":
				other, err := NewReadOnlyJournalEndpoint(ctx, f.owner, f.runtime.owner, f.worker.owner)
				if err != nil {
					t.Fatal(err)
				}
				defer func() { _ = other.Close() }()
				if err := other.ActivateJournalWriteGrant(ctx, grant); err == nil {
					t.Fatal("grant activated another endpoint with the same peers and journal")
				}
				if err := f.endpoint.ActivateJournalWriteGrant(ctx, grant); err != nil {
					t.Fatalf("wrong endpoint consumed grant: %v", err)
				}
				return
			case "cancelled":
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			case "nil-context":
				ctx = nil
			case "closed-endpoint":
				if err := f.endpoint.Close(); err != nil {
					t.Fatal(err)
				}
			case "runtime-exited", "worker-exited":
				child := f.runtime
				if scenario == "worker-exited" {
					child = f.worker
				}
				if err := child.process.Kill(); err != nil {
					t.Fatal(err)
				}
				select {
				case <-child.done:
				case <-time.After(5 * time.Second):
					t.Fatal("original process did not exit")
				}
			}
			if err := f.endpoint.ActivateJournalWriteGrant(ctx, grant); err == nil {
				t.Fatal("invalid activation succeeded")
			}
			if !f.endpoint.readOnly || f.endpoint.grantUsed {
				t.Fatal("rejected activation changed route authority")
			}
		})
	}
}

func TestJournalWriteGrantConcurrentActivation(t *testing.T) {
	f := newJournalEndpointOwnerFixture(t, 0, nil, true, true)
	grant := issueJournalTestGrant(t, f)
	var wg sync.WaitGroup
	results := make(chan error, 8)
	for range 8 {
		wg.Go(func() { results <- f.endpoint.ActivateJournalWriteGrant(t.Context(), grant) })
	}
	wg.Wait()
	close(results)
	successes := 0
	for err := range results {
		if err == nil {
			successes++
		}
	}
	if successes != 1 {
		t.Fatalf("activation successes=%d, want 1", successes)
	}
}
