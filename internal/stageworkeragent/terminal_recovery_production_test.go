package stageworkeragent_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/stageauthority"
	"github.com/vivym/vela/internal/stageworkeragent"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
)

type terminalRecoverySessionControl struct {
	*productionControl
	available        bool
	registeredEpoch  int64
	failRegistration bool
	registrations    int
}

func (control *terminalRecoverySessionControl) HasActiveControlSession() bool {
	return control.available
}

func (control *terminalRecoverySessionControl) Exchange(ctx context.Context, request *velav1.StageWorkerControlServiceConnectRequest) (*velav1.StageWorkerControlServiceConnectResponse, error) {
	control.available = true
	if request.GetRegisterWorkerEvidence() != nil {
		control.registrations++
		if control.failRegistration {
			control.failRegistration = false
			return nil, errors.New("registration failed after opening the Control stream")
		}
		control.registeredEpoch = control.controlSessionEpoch
	}
	return control.productionControl.Exchange(ctx, request)
}

func TestTerminalRecoveryProductionRegistersCurrentSessionBeforeHistory(t *testing.T) {
	for _, scenario := range []string{"already-open", "registration-failed", "query-reconnected"} {
		t.Run(scenario, func(t *testing.T) {
			f := terminalMaterializationFixture(t)
			gate := f.open(t)
			completeAdmissionInputs(t, beginAdmission(t, gate, f.assignment, f.acquireID))
			startFloorCollectorRuntimes(t, f, t.TempDir(), true, false)
			paths := retirementScratch(t, f)
			record, validator := terminalMaterializationRecord(t, f, "sealed")
			journal := terminalRecoveryJournal(t, record)
			guard := &noTerminalMaterializationIO{}
			identity := runtimeIdentityFromAuthority(f.assignment.Authority)
			control := &terminalRecoverySessionControl{
				productionControl: &productionControl{identity: identity, controlSessionEpoch: 7},
				available:         scenario == "already-open", failRegistration: scenario == "registration-failed",
			}
			queries, unregisteredQueries := 0, 0
			config := terminalMaterializationConfig(t, f, gate, journal, validator, guard)
			config.Control = control
			stream := automaticTerminalStream(t, config, func(context.Context, *stageauthority.Validator, *velav1.StageAuthority, string, uuid.UUID) (*stageauthority.VerifiedTerminalDisposition, error) {
				queries++
				if control.registeredEpoch != control.controlSessionEpoch {
					unregisteredQueries++
					return nil, nil
				}
				if scenario == "query-reconnected" && queries == 1 {
					// A history RPC can replace a failed transport after the preflight.
					control.controlSessionEpoch++
					return nil, errors.New("Control session changed during history query")
				}
				f.disposition.ControlSessionEpoch = control.controlSessionEpoch
				f.signDisposition(t)
				return fixtureTerminalResponse(t, f), nil
			})
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			waits := 0
			production, err := stageworkeragent.NewProductionAgent(stageworkeragent.ProductionConfig{
				Control: control, Runtime: &readinessRuntime{identity: identity}, Stream: stream, RuntimeIdentity: identity,
				Devices: f.assignment.Authority.Devices, Members: f.assignment.Authority.Members,
				CapacityVector: f.assignment.Authority.CapacityVector, CapacityTTL: time.Minute, HeartbeatInterval: time.Second,
				RetryMinimum: time.Millisecond, RetryMaximum: time.Second,
				ObservationSequenceSource: &capacitySequenceSource{values: []int64{1, 2, 3, 4, 5, 6}},
				Now:                       func() time.Time { return time.Unix(0, f.clock.Load()) },
				Wait: func(context.Context, time.Duration) error {
					waits++
					if control.acquire != nil || waits > 4 {
						cancel()
					}
					return nil
				},
			})
			if err != nil {
				t.Fatal(err)
			}
			if err := production.Run(ctx); err != nil {
				t.Fatal(err)
			}
			if unregisteredQueries != 0 || control.acquire == nil || guard.calls != 0 || control.registrations == 0 {
				t.Fatalf("recovery did not establish registered session before history: queries=%d unregistered=%d registrations=%d acquired=%t materialization-io=%d",
					queries, unregisteredQueries, control.registrations, control.acquire != nil, guard.calls)
			}
			if scenario != "already-open" && control.registrations != 2 {
				t.Fatalf("did not retry failed/new session registration: %d", control.registrations)
			}
			state := admissionSnapshot(t, gate)
			if len(state.Retirements) != 1 || state.Retirements[0].Phase != stageworkeragent.TerminalRetirementRetired {
				t.Fatalf("recovery did not complete before discovery: %+v", state)
			}
			assertRetirementScratch(t, paths, false)
		})
	}
}
