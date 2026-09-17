package stageworkeragent_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/stageauthority"
	"github.com/vivym/vela/internal/stageworkeragent"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
)

func TestProductionExpiredReattachRetainsWorkerAndWaitsForTerminalProof(t *testing.T) {
	f := terminalMaterializationFixture(t)
	gate := f.open(t)
	group := startFloorCollectorRuntimes(t, f, t.TempDir(), true, false)
	client := group.clients[0]
	_, validator := terminalMaterializationRecord(t, f, "sealed")
	guard := &noTerminalMaterializationIO{}
	identity := runtimeIdentityFromAuthority(f.assignment.Authority)
	control := &productionExecutionControl{materializingStreamControl: newMaterializingStreamControl(t, f.assignment.Authority),
		identity: identity, assignment: f.assignment, heartbeatFailures: 1}
	config := terminalMaterializationConfig(t, f, gate, terminalRecoveryJournal(t), validator, guard)
	config.Control = control
	queries := 0
	stream := automaticTerminalStream(t, config, func(context.Context, *stageauthority.Validator, *velav1.StageAuthority, string, uuid.UUID) (*stageauthority.VerifiedTerminalDisposition, error) {
		queries++
		return nil, nil // Control has not yet declared the expired execution terminal.
	})
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	waits := 0
	agent, err := stageworkeragent.NewProductionAgent(stageworkeragent.ProductionConfig{
		Control: control, Runtime: client, Stream: stream, RuntimeIdentity: identity,
		Devices: f.assignment.Authority.Devices, Members: f.assignment.Authority.Members,
		CapacityVector: f.assignment.Authority.CapacityVector, CapacityTTL: time.Minute,
		HeartbeatInterval: time.Second, RetryMinimum: time.Millisecond, RetryMaximum: time.Second,
		ObservationSequenceSource: &capacitySequenceSource{values: []int64{1, 2, 3, 4, 5, 6}},
		Now:                       func() time.Time { return time.Unix(0, f.clock.Load()) },
		Wait: func(context.Context, time.Duration) error {
			waits++
			if waits == 1 {
				f.clock.Add(int64(10 * time.Minute))
				return nil
			}
			state := admissionSnapshot(t, gate)
			if state.Latest == nil || state.Latest.Phase != stageworkeragent.AssignmentClosed || queries == 0 || control.acquireCalls != 1 || len(state.Retirements) != 0 {
				t.Fatalf("expired grant escaped durable recovery fence: waits=%d queries=%d acquires=%d state=%+v", waits, queries, control.acquireCalls, state)
			}
			cancel()
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := agent.Run(ctx); err != nil {
		t.Fatalf("expired reattach killed Worker instead of awaiting terminal proof: %v", err)
	}
	if waits != 2 || control.startCalls != 1 || control.reattachCalls != 0 || guard.calls != 0 {
		t.Fatalf("waits=%d starts=%d reattach=%d io=%d", waits, control.startCalls, control.reattachCalls, guard.calls)
	}
}
