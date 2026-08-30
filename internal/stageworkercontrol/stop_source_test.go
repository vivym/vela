package stageworkercontrol_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/vivym/vela/internal/stageauthority"
	"github.com/vivym/vela/internal/stageworkercontrol"
	"github.com/vivym/vela/internal/stageworkertransport"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
)

func TestAuthorityStopSourcePushesRevokedAuthority(t *testing.T) {
	now := time.Date(2026, 8, 30, 10, 0, 0, 0, time.UTC)
	keyring := map[string][]byte{"control-key": make([]byte, 32)}
	signer, err := stageauthority.NewSigner(keyring)
	if err != nil {
		t.Fatalf("construct StageAuthority signer: %v", err)
	}
	validator, err := stageauthority.NewValidator(keyring, func() time.Time { return now })
	if err != nil {
		t.Fatalf("construct StageAuthority validator: %v", err)
	}
	authorizer := &mutableStopAuthorizer{active: false}
	source, err := stageworkercontrol.NewAuthorityStopSource(
		validator,
		authorizer,
		stageworkercontrol.AuthorityStopSourceConfig{
			PollInterval: time.Millisecond,
			Now:          func() time.Time { return now },
		},
	)
	if err != nil {
		t.Fatalf("construct authority StopSource: %v", err)
	}
	identity := stageworkertransport.Identity{SPIFFEID: "spiffe://vela/worker/member-1"}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stops := source.Stops(ctx, identity, 7)
	authority, err := signer.Sign(controlAuthority(now))
	if err != nil {
		t.Fatalf("sign tracked StageAuthority: %v", err)
	}
	if err := source.ObserveStageAuthority(identity, 7, authority); err != nil {
		t.Fatalf("track StageAuthority: %v", err)
	}
	select {
	case stop := <-stops:
		if stop.GetAuthority().GetStageLeaseId() != authority.GetStageLeaseId() ||
			stop.GetReason() != velav1.StageWorkerStopReason_STAGE_WORKER_STOP_REASON_AUTHORITY_REVOKED ||
			!stop.GetIssuedAt().AsTime().Equal(now) {
			t.Fatalf("revoked StopStage = %#v", stop)
		}
	case <-time.After(time.Second):
		t.Fatal("revoked StageAuthority did not produce StopStage")
	}
}

func TestAuthorityStopSourceClassifiesExpiredLeaseWithoutDatabaseApproval(t *testing.T) {
	clock := &mutableStopClock{now: time.Date(2026, 8, 30, 11, 0, 0, 0, time.UTC)}
	keyring := map[string][]byte{"control-key": make([]byte, 32)}
	signer, _ := stageauthority.NewSigner(keyring)
	validator, _ := stageauthority.NewValidator(keyring, clock.Now)
	authorizer := &mutableStopAuthorizer{active: true}
	source, err := stageworkercontrol.NewAuthorityStopSource(
		validator,
		authorizer,
		stageworkercontrol.AuthorityStopSourceConfig{
			PollInterval: time.Millisecond,
			Now:          clock.Now,
		},
	)
	if err != nil {
		t.Fatalf("construct authority StopSource: %v", err)
	}
	identity := stageworkertransport.Identity{SPIFFEID: "spiffe://vela/worker/member-1"}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stops := source.Stops(ctx, identity, 8)
	authority, err := signer.Sign(controlAuthority(clock.Now()))
	if err != nil {
		t.Fatalf("sign expiring StageAuthority: %v", err)
	}
	if err := source.ObserveStageAuthority(identity, 8, authority); err != nil {
		t.Fatalf("track expiring StageAuthority: %v", err)
	}
	clock.Set(clock.Now().Add(time.Minute))
	select {
	case stop := <-stops:
		if stop.GetReason() != velav1.StageWorkerStopReason_STAGE_WORKER_STOP_REASON_LEASE_EXPIRED {
			t.Fatalf("expired StopStage reason = %s", stop.GetReason())
		}
	case <-time.After(time.Second):
		t.Fatal("expired StageAuthority did not produce StopStage")
	}
	if authorizer.Calls() != 0 {
		t.Fatalf("expired StageAuthority queried durable approval %d times", authorizer.Calls())
	}
}

type mutableStopAuthorizer struct {
	mu     sync.Mutex
	active bool
	calls  int
}

func (authorizer *mutableStopAuthorizer) IsActive(
	_ context.Context,
	_ stageworkertransport.Identity,
	_ int64,
	_ stageworkercontrol.Operation,
	_ stageauthority.Verified,
) (bool, error) {
	authorizer.mu.Lock()
	defer authorizer.mu.Unlock()
	authorizer.calls++
	return authorizer.active, nil
}

func (authorizer *mutableStopAuthorizer) Calls() int {
	authorizer.mu.Lock()
	defer authorizer.mu.Unlock()
	return authorizer.calls
}

type mutableStopClock struct {
	mu  sync.Mutex
	now time.Time
}

func (clock *mutableStopClock) Now() time.Time {
	clock.mu.Lock()
	defer clock.mu.Unlock()
	return clock.now
}

func (clock *mutableStopClock) Set(now time.Time) {
	clock.mu.Lock()
	defer clock.mu.Unlock()
	clock.now = now
}
