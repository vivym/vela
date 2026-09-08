package modelruntime_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/vivym/vela/internal/modelruntime"
	"github.com/vivym/vela/internal/modelruntimetransport"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/protobuf/proto"
)

func TestAllocationExecutionDiscoveryRecoversLatestOnlyDrain(t *testing.T) {
	for _, applied := range []bool{false, true} {
		t.Run(map[bool]string{false: "not-applied", true: "applied-response-lost"}[applied], func(t *testing.T) {
			backend := &renewalRecoveryBackend{FakeRuntime: modelruntime.NewFakeDiTRuntime(), applyRenewal: applied,
				failStatus: true, calls: make(chan cancellationAuthorityCall, 2)}
			f := newExecutionDrainFixture(t, privateExecutionStateDirectory(t), backend)
			first := f.authorities[0]
			prepareFloorRuntime(t, f.supervisor, first)
			f.clock.Advance(time.Second)
			latest := renewWatchdogAuthority(t, f.signer, first, f.clock.Now())
			if response, err := f.supervisor.Status(t.Context(), &velav1.ModelRuntimeServiceStatusRequest{Authority: latest}); err != nil || response.GetDecision() != velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_REJECTED {
				t.Fatalf("renewal fault: %v %v", response, err)
			}
			if _, err := f.supervisor.InstallExecutionFloor(t.Context(), f.disposition(t)); err != nil {
				t.Fatal(err)
			}
			client, _ := serveRuntimeServer(t, f.supervisor)
			read, err := client.InspectAllocationExecution(t.Context(), allocationInspectionRequest(latest))
			expected := first
			if applied {
				expected = latest
			}
			if err != nil || !proto.Equal(read.GetObservedAuthority(), expected) || !read.GetInspection().GetKnown() {
				t.Fatalf("latest-only discovery: %v %v", read, err)
			}
			verified, err := f.validator.ValidateEnvelopeForReplay(latest, 0)
			if err != nil || modelruntimetransport.ValidateAllocationExecutionResponse(f.validator, latest, verified.Digest, read.GetRuntimeIdentity(), read) != nil {
				t.Fatalf("invalid discovery response: %v %v", read, err)
			}
			if backend.statusCalls != 1 || len(backend.calls) != 0 {
				t.Fatal("discovery renewed or canceled execution")
			}
			assertExecutionDrainCheckpoint(t, f.supervisor, expected, false)
			// Recovery supplies only the envelope returned over the wire.
			actual := read.GetObservedAuthority()
			if response, err := client.CancelStage(t.Context(), &velav1.ModelRuntimeServiceCancelStageRequest{
				Authority: actual, Reason: velav1.ModelRuntimeCancelReason_MODEL_RUNTIME_CANCEL_REASON_CONTROL_PLANE_STOP,
			}); err != nil || !response.GetCancellationAcknowledged() {
				t.Fatalf("discovered cancellation: %v %v", response, err)
			}
			backend.FinishStop()
			drained, err := client.DrainStageExecution(t.Context(), &velav1.ModelRuntimeServiceDrainStageExecutionRequest{
				Scope: &velav1.ModelRuntimeExecutionDrainScope{SchemaVersion: 1, Identity: read.GetRuntimeIdentity(), Authority: actual},
			})
			if err != nil || !proto.Equal(drained.GetResult().GetCheckpoint().GetAuthority(), actual) {
				t.Fatalf("discovered exact drain: %v %v", drained, err)
			}
		})
	}
}

func TestAllocationExecutionDiscoveryRejectsAmbiguousOrUnavailableObservations(t *testing.T) {
	for index, fault := range []string{"error", "unknown", "both-known", "invalid", "timeout"} {
		t.Run(fault, func(t *testing.T) {
			backend := &renewalRecoveryBackend{FakeRuntime: modelruntime.NewFakeDiTRuntime(), failStatus: true,
				calls: make(chan cancellationAuthorityCall, 2)}
			f := newExecutionDrainFixture(t, privateExecutionStateDirectory(t), backend)
			prepareFloorRuntime(t, f.supervisor, f.authorities[0])
			f.clock.Advance(time.Second)
			latest := renewWatchdogAuthority(t, f.signer, f.authorities[0], f.clock.Now())
			if response, err := f.supervisor.Status(t.Context(), &velav1.ModelRuntimeServiceStatusRequest{Authority: latest}); err != nil || response.GetDecision() != velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_REJECTED {
				t.Fatalf("renewal fault: %v %v", response, err)
			}
			backend.inspectFault.Store(int32(index + 1))
			// Semantic faults must test the returned evidence, not whether a
			// contended race build finishes verification within 30 milliseconds.
			// Keep the short deadline only for the deliberately blocked inspector.
			timeout := 5 * time.Second
			if fault == "timeout" {
				timeout = 30 * time.Millisecond
			}
			ctx, cancel := context.WithTimeout(t.Context(), timeout)
			read, err := f.supervisor.InspectAllocationExecution(ctx, allocationInspectionRequest(latest))
			cancel()
			if read.GetObservedAuthority() != nil || read.GetInspection() != nil {
				t.Fatalf("unproven observation exposed an envelope: %v %v", read, err)
			}
			if fault == "timeout" {
				if !errors.Is(err, context.DeadlineExceeded) {
					t.Fatalf("inspection lost deadline: %v", err)
				}
			} else if err != nil || fault != "unknown" && read.GetDecision() != velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_REJECTED {
				t.Fatalf("inspection fault was accepted: %v %v", read, err)
			}
			backend.inspectFault.Store(0)
			read, err = f.supervisor.InspectAllocationExecution(t.Context(), allocationInspectionRequest(latest))
			if err != nil || !proto.Equal(read.GetObservedAuthority(), f.authorities[0]) || backend.statusCalls != 1 || len(backend.calls) != 0 {
				t.Fatalf("observation fault changed recovery state: %v %v", read, err)
			}
		})
	}
}

func TestAllocationExecutionDiscoveryIsReadOnlyAndMissingHistoryIsUnknown(t *testing.T) {
	clock := newManualClock(time.Date(2026, 9, 6, 0, 0, 0, 0, time.UTC))
	signer, validator := runtimeAuthorityCrypto(t, clock)
	fake := modelruntime.NewFakeDiTRuntime()
	service := newRuntimeService(t, clock, validator, runtimeBinding(), fake)
	client, _ := serveRuntime(t, service)
	first := signRuntimeAuthority(t, signer, clock.Now())
	assertUnknown := func(service *modelruntime.Service, authority *velav1.StageAuthority) {
		t.Helper()
		read, err := service.InspectAllocationExecution(t.Context(), allocationInspectionRequest(authority))
		if err != nil || read.GetObservedAuthority() != nil || read.GetInspection() != nil || read.GetDecision() != velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_ACCEPTED {
			t.Fatalf("missing allocation history invented state: %v %v", read, err)
		}
	}
	assertUnknown(service, first)
	prepareAndStart(t, client, first)
	clock.Advance(time.Second)
	unseen := renewWatchdogAuthority(t, signer, first, clock.Now())
	read, err := service.InspectAllocationExecution(t.Context(), allocationInspectionRequest(unseen))
	if err != nil || !proto.Equal(read.GetObservedAuthority(), first) {
		t.Fatalf("unseen same-allocation query could not discover current identity: %v %v", read, err)
	}
	assertInspection(t, client, unseen, false, velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_UNSPECIFIED)
	clock.mu.Lock()
	timers := len(clock.timers)
	clock.mu.Unlock()
	if timers != 1 {
		t.Fatal("discovery changed the watchdog")
	}
	read.ObservedAuthority.Signature[0] ^= 1
	read, err = service.InspectAllocationExecution(t.Context(), allocationInspectionRequest(unseen))
	if err != nil || !proto.Equal(read.GetObservedAuthority(), first) {
		t.Fatal("caller mutated retained backend authority")
	}
	other := orderedRuntimeAuthority(t, signer, clock.Now(), first.GetExecutionSequence()+1)
	assertUnknown(service, other)
	restarted := newRuntimeService(t, clock, validator, runtimeBinding(), modelruntime.NewFakeDiTRuntime())
	assertUnknown(restarted, unseen)
}

func TestAllocationExecutionDiscoveryDoesNotHoldCancellationOrWatchdog(t *testing.T) {
	for _, watchdog := range []bool{false, true} {
		t.Run(map[bool]string{false: "cancel", true: "watchdog"}[watchdog], func(t *testing.T) {
			clock := newManualClock(time.Date(2026, 9, 6, 0, 0, 0, 0, time.UTC))
			signer, validator := runtimeAuthorityCrypto(t, clock)
			fake := modelruntime.NewFakeDiTRuntime()
			backend := &waitingInspectionBackend{Backend: fake, entered: make(chan struct{})}
			service := newRuntimeService(t, clock, validator, runtimeBinding(), backend)
			client, _ := serveRuntime(t, service)
			authority := signRuntimeAuthority(t, signer, clock.Now())
			prepareAndStart(t, client, authority)
			ctx, cancel := context.WithCancel(t.Context())
			finished := make(chan error, 1)
			go func() {
				_, err := service.InspectAllocationExecution(ctx, allocationInspectionRequest(authority))
				finished <- err
			}()
			t.Cleanup(func() {
				cancel()
				if err := <-finished; !errors.Is(err, context.Canceled) {
					t.Errorf("inspection completion: %v", err)
				}
			})
			select {
			case <-backend.entered:
			case <-time.After(time.Second):
				t.Fatal("inspection did not reach backend")
			}
			if watchdog {
				clock.Advance(time.Minute)
			} else {
				ctx, stop := context.WithTimeout(t.Context(), time.Second)
				defer stop()
				response, err := client.CancelStage(ctx, &velav1.ModelRuntimeServiceCancelStageRequest{
					Authority: authority, Reason: velav1.ModelRuntimeCancelReason_MODEL_RUNTIME_CANCEL_REASON_CONTROL_PLANE_STOP,
				})
				if err != nil || !response.GetCancellationAcknowledged() {
					t.Fatalf("reader held cancellation: %v %v", response, err)
				}
			}
			verified, err := validator.ValidateSignature(authority, runtimeBinding())
			if err != nil {
				t.Fatal(err)
			}
			deadline := time.Now().Add(time.Second)
			for {
				state, err := fake.InspectExecution(t.Context(), verified)
				if err == nil && state.State == velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_CANCELING {
					break
				}
				if time.Now().After(deadline) {
					t.Fatalf("reader held stop: %+v %v", state, err)
				}
				time.Sleep(time.Millisecond)
			}
		})
	}
}

func allocationInspectionRequest(authority *velav1.StageAuthority) *velav1.ModelRuntimeServiceInspectAllocationExecutionRequest {
	return &velav1.ModelRuntimeServiceInspectAllocationExecutionRequest{SchemaVersion: 1, Authority: authority}
}

func TestAllocationExecutionDiscoveryRejectsInvalidRequestsWithoutBackendReads(t *testing.T) {
	for _, mutation := range []string{"nil", "schema", "unknown fields", "signature", "future issue", "runtime epoch", "canceled"} {
		t.Run(mutation, func(t *testing.T) {
			clock := newManualClock(time.Date(2026, 9, 6, 0, 0, 0, 0, time.UTC))
			signer, validator := runtimeAuthorityCrypto(t, clock)
			backend := &fixedInspectionBackend{Backend: modelruntime.NewFakeDiTRuntime()}
			service := newRuntimeService(t, clock, validator, runtimeBinding(), backend)
			client, _ := serveRuntime(t, service)
			authority := signRuntimeAuthority(t, signer, clock.Now())
			prepareAndStart(t, client, authority)
			request := allocationInspectionRequest(proto.Clone(authority).(*velav1.StageAuthority))
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			switch mutation {
			case "nil":
				request = nil
			case "schema":
				request.SchemaVersion++
			case "unknown fields":
				request.ProtoReflect().SetUnknown([]byte{0x78, 1})
			case "signature":
				request.Authority.Signature[0] ^= 1
			case "future issue":
				request.Authority = signRuntimeAuthority(t, signer, clock.Now().Add(time.Minute))
			case "runtime epoch":
				for _, member := range request.Authority.Members {
					member.ModelRuntimeEpoch++
				}
				var err error
				request.Authority, err = signer.Sign(request.Authority)
				if err != nil {
					t.Fatal(err)
				}
			case "canceled":
				cancel()
			}
			response, err := service.InspectAllocationExecution(ctx, request)
			if response.GetObservedAuthority() != nil || response.GetInspection() != nil || backend.calls.Load() != 0 ||
				response.GetDecision() == velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_ACCEPTED {
				t.Fatalf("invalid request reached backend: %v %v", response, err)
			}
		})
	}
}

func TestAllocationExecutionDiscoveryDropsChangedAndLateObservations(t *testing.T) {
	for _, event := range []string{"renewed", "shutdown", "canceled"} {
		t.Run(event, func(t *testing.T) {
			clock := newManualClock(time.Date(2026, 9, 6, 0, 0, 0, 0, time.UTC))
			signer, validator := runtimeAuthorityCrypto(t, clock)
			backend := &fixedInspectionBackend{Backend: modelruntime.NewFakeDiTRuntime(), result: modelruntime.ExecutionInspection{
				Known: true, State: velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_RUNNING,
			}}
			service := newRuntimeService(t, clock, validator, runtimeBinding(), backend)
			client, _ := serveRuntime(t, service)
			authority := signRuntimeAuthority(t, signer, clock.Now())
			prepareAndStart(t, client, authority)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			backend.after = func() {
				switch event {
				case "renewed":
					clock.Advance(time.Second)
					renewed := renewWatchdogAuthority(t, signer, authority, clock.Now())
					response, err := service.Status(t.Context(), &velav1.ModelRuntimeServiceStatusRequest{Authority: renewed})
					if err != nil || response.GetDecision() != velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_ACCEPTED {
						t.Fatalf("concurrent renewal: %v %v", response, err)
					}
				case "shutdown":
					if err := service.Shutdown(); err != nil {
						t.Fatal(err)
					}
				case "canceled":
					cancel()
				}
			}
			response, err := service.InspectAllocationExecution(ctx, allocationInspectionRequest(authority))
			if event == "canceled" && !errors.Is(err, context.Canceled) || response.GetObservedAuthority() != nil || response.GetInspection() != nil ||
				response.GetDecision() == velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_ACCEPTED {
				t.Fatalf("changed or late observation escaped: %v %v", response, err)
			}
		})
	}
}

func TestAllocationExecutionDiscoveryCannotFallBackToStatus(t *testing.T) {
	clock := newManualClock(time.Date(2026, 9, 6, 0, 0, 0, 0, time.UTC))
	signer, validator := runtimeAuthorityCrypto(t, clock)
	backend := &inspectionStatusCounter{Backend: modelruntime.NewFakeDiTRuntime()}
	service := newRuntimeService(t, clock, validator, runtimeBinding(), backend)
	client, _ := serveRuntime(t, service)
	authority := signRuntimeAuthority(t, signer, clock.Now())
	prepareAndStart(t, client, authority)
	response, err := service.InspectAllocationExecution(t.Context(), allocationInspectionRequest(authority))
	if err != nil || response.GetDecision() != velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_REJECTED ||
		response.GetObservedAuthority() != nil || backend.calls.Load() != 0 {
		t.Fatalf("unsupported inspection granted or renewed authority: %v %v", response, err)
	}
}
