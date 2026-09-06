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
	"google.golang.org/grpc"
)

type terminalRecoverySessionControl struct {
	*productionControl
	available         bool
	synchronizedEpoch int64
	failRegistration  bool
	registrations     int
	loseCapacity      bool
	capacityReports   int
}

func (control *terminalRecoverySessionControl) HasActiveControlSession() bool {
	return control.available
}

func (control *terminalRecoverySessionControl) Exchange(ctx context.Context, request *velav1.StageWorkerControlServiceConnectRequest) (*velav1.StageWorkerControlServiceConnectResponse, error) {
	control.available = true
	if request.GetReportCapacityObservation() != nil {
		control.capacityReports++
		control.synchronizedEpoch = control.controlSessionEpoch
		if control.loseCapacity {
			control.loseCapacity = false
			_, _ = control.productionControl.Exchange(ctx, request)
			return nil, errors.New("response lost after capacity synchronization")
		}
	}
	if request.GetRegisterWorkerEvidence() != nil {
		control.registrations++
		if control.failRegistration {
			control.failRegistration = false
			return nil, errors.New("registration failed after opening the Control stream")
		}
	}
	return control.productionControl.Exchange(ctx, request)
}

type terminalRecoveryReadiness struct {
	*readinessRuntime
	gate            *stageworkeragent.FileAssignmentAdmission
	prematureProbes int
}

func (runtime *terminalRecoveryReadiness) ProbeReadiness(ctx context.Context, request *velav1.ModelRuntimeServiceProbeReadinessRequest, options ...grpc.CallOption) (*velav1.ModelRuntimeServiceProbeReadinessResponse, error) {
	state, err := runtime.gate.Snapshot(ctx)
	if err != nil {
		return nil, err
	}
	if len(state.Retirements) == 0 || state.Retirements[0].Phase != stageworkeragent.TerminalRetirementRetired {
		runtime.prematureProbes++
		return nil, errors.New("Runtime readiness requires terminal history recovery")
	}
	return runtime.readinessRuntime.ProbeReadiness(ctx, request, options...)
}

func TestTerminalRecoveryProductionSynchronizesUnavailableSessionBeforeHistory(t *testing.T) {
	for _, scenario := range []string{"already-open", "registration-failed", "query-reconnected", "readiness-blocked", "capacity-lost", "sequence-persistence"} {
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
				loseCapacity: scenario == "capacity-lost",
			}
			queries, unregisteredQueries := 0, 0
			config := terminalMaterializationConfig(t, f, gate, journal, validator, guard)
			config.Control = control
			stream := automaticTerminalStream(t, config, func(context.Context, *stageauthority.Validator, *velav1.StageAuthority, string, uuid.UUID) (*stageauthority.VerifiedTerminalDisposition, error) {
				queries++
				if (scenario == "capacity-lost" || scenario == "sequence-persistence") && control.capacityReports < 2 {
					t.Fatal("history queried before confirming the lost capacity response")
				}
				if control.synchronizedEpoch != control.controlSessionEpoch {
					unregisteredQueries++
					return nil, nil
				}
				if control.capacity == nil {
					t.Fatal("history queried before capacity withdrawal")
				}
				for _, value := range control.capacity.CapacityVector {
					if value != 0 {
						t.Fatal("history queried while advertising ready capacity")
					}
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
			sequenceSource := &capacitySequenceSource{values: []int64{1, 2, 3, 4, 5, 6}}
			if scenario == "sequence-persistence" {
				sequenceSource.observeErrors = map[int64]error{1: errors.New("capacity high-water persistence failed")}
			}
			readiness := &terminalRecoveryReadiness{readinessRuntime: &readinessRuntime{identity: identity}, gate: gate}
			var runtime stageworkeragent.RuntimeReadinessClient = readiness.readinessRuntime
			if scenario == "readiness-blocked" {
				runtime = readiness
			}
			production, err := stageworkeragent.NewProductionAgent(stageworkeragent.ProductionConfig{
				Control: control, Runtime: runtime, Stream: stream, RuntimeIdentity: identity,
				Devices: f.assignment.Authority.Devices, Members: f.assignment.Authority.Members,
				CapacityVector: f.assignment.Authority.CapacityVector, CapacityTTL: time.Minute, HeartbeatInterval: time.Second,
				RetryMinimum: time.Millisecond, RetryMaximum: time.Second,
				ObservationSequenceSource: sequenceSource,
				Now:                       func() time.Time { return time.Unix(0, f.clock.Load()) },
				Wait: func(context.Context, time.Duration) error {
					waits++
					clear(sequenceSource.observeErrors)
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
				t.Fatalf("recovery did not synchronize session before history: queries=%d unsynchronized=%d registrations=%d acquired=%t materialization-io=%d",
					queries, unregisteredQueries, control.registrations, control.acquire != nil, guard.calls)
			}
			if scenario == "registration-failed" && control.registrations != 2 {
				t.Fatalf("did not retry failed/new session registration: %d", control.registrations)
			}
			if scenario == "sequence-persistence" && (len(control.capacities) < 2 ||
				control.capacities[0].ObservationSequence != control.capacities[1].ObservationSequence) {
				t.Fatal("local persistence failure forgot the accepted sequence needed for replay")
			}
			if readiness.prematureProbes != 0 {
				t.Fatalf("recovery required ready Runtime before clearing history: %d", readiness.prematureProbes)
			}
			state := admissionSnapshot(t, gate)
			if len(state.Retirements) != 1 || state.Retirements[0].Phase != stageworkeragent.TerminalRetirementRetired {
				t.Fatalf("recovery did not complete before discovery: %+v", state)
			}
			assertRetirementScratch(t, paths, false)
		})
	}
}

func TestTerminalRecoveryProductionRetainsZeroCapacityDuringIncompleteIntent(t *testing.T) {
	f := terminalMaterializationFixture(t)
	gate := f.open(t)
	beginAdmission(t, gate, f.assignment, f.acquireID).Release()
	startFloorCollectorRuntimes(t, f, t.TempDir(), true, false)
	paths := retirementScratch(t, f)
	record, validator := terminalMaterializationRecord(t, f, "sealed")
	journal := terminalRecoveryJournal(t, record)
	guard := &noTerminalMaterializationIO{}
	identity := runtimeIdentityFromAuthority(f.assignment.Authority)
	control := &terminalRecoverySessionControl{productionControl: &productionControl{identity: identity, controlSessionEpoch: 7}}
	config := terminalMaterializationConfig(t, f, gate, journal, validator, guard)
	config.Control = control
	queries := 0
	stream := automaticTerminalStream(t, config, func(context.Context, *stageauthority.Validator, *velav1.StageAuthority, string, uuid.UUID) (*stageauthority.VerifiedTerminalDisposition, error) {
		queries++
		return fixtureTerminalResponse(t, f), nil
	})
	readiness := &readinessRuntime{identity: identity}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	waits := 0
	production, err := stageworkeragent.NewProductionAgent(stageworkeragent.ProductionConfig{
		Control: control, Runtime: readiness, Stream: stream, RuntimeIdentity: identity,
		Devices: f.assignment.Authority.Devices, Members: f.assignment.Authority.Members,
		CapacityVector: f.assignment.Authority.CapacityVector, CapacityTTL: time.Minute, HeartbeatInterval: time.Second,
		RetryMinimum: time.Millisecond, RetryMaximum: time.Second,
		ObservationSequenceSource: &capacitySequenceSource{values: []int64{1, 2}},
		Now:                       func() time.Time { return time.Unix(0, f.clock.Load()) },
		Wait: func(context.Context, time.Duration) error {
			waits++
			if waits == 2 {
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
	state := admissionSnapshot(t, gate)
	if len(state.Retirements) != 1 || state.Retirements[0].Phase != stageworkeragent.TerminalRetirementIntent ||
		queries != 2 || control.capacityReports != 1 || control.registrations != 0 || control.acquire != nil || len(readiness.checks) != 0 || guard.calls != 0 {
		t.Fatalf("incomplete recovery crossed into readiness/capacity/acquisition: %+v queries=%d reports=%d registrations=%d probes=%d materialization-io=%d",
			state, queries, control.capacityReports, control.registrations, len(readiness.checks), guard.calls)
	}
	for _, quantity := range control.capacity.CapacityVector {
		if quantity != 0 {
			t.Fatal("incomplete recovery advertised ready capacity")
		}
	}
	assertRetirementScratch(t, paths, true)
}
