package nodeagent

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/vivym/vela/internal/fleet"
	"github.com/vivym/vela/internal/modelruntime"
	"github.com/vivym/vela/internal/stageauthority"
	"golang.org/x/sys/unix"
	"google.golang.org/protobuf/proto"
)

type startupReservationRegistryFixture struct {
	node, actor string
	calls       int
	reserve     func(context.Context, fleet.RuntimeStartupRequest) (fleet.RuntimeStartupReservation, error)
}

// A private PID namespace makes the killed Node helper's descendants die with
// it. This is fixture cleanup, not proof of production Runtime containment.
func TestRuntimeStartupReservationProcessCrashRecovery(t *testing.T) {
	const modeKey = "VELA_STARTUP_RESERVATION_CRASH"
	if fault := os.Getenv(modeKey); fault != "" {
		// The observer must read procfs in this Node's PID namespace. Keep
		// remounts private to the helper's new mount namespace.
		if err := unix.Mount("", "/", "", unix.MS_REC|unix.MS_PRIVATE, ""); err != nil {
			t.Fatal(err)
		}
		if err := unix.Mount("proc", "/proc", "proc", unix.MS_NOSUID|unix.MS_NOEXEC|unix.MS_NODEV, ""); err != nil {
			t.Fatal(err)
		}
		_, config := newRemoteReservationFixture(t, false)
		ledger, err := OpenRuntimeStartupLedger(t.Context(), os.Getenv("VELA_STARTUP_RESERVATION_LEDGER"), "cpu-node", true)
		if err != nil {
			t.Fatal(err)
		}
		control := os.NewFile(3, "crash-boundary")
		defer func() { _ = control.Close() }()
		kill := func() {
			// PID namespace init cannot self-deliver this SIGKILL. The ancestor
			// must kill the exact child after receiving its boundary report.
			if _, err := control.WriteString(fault + "\n"); err != nil {
				t.Fatal(err)
			}
			select {}
		}
		appends := 0
		ledger.boundary = func(phase string) error {
			if phase == "before-append" {
				appends++
			}
			if appends == 1 && fault == "intent-"+phase || appends == 2 && fault == "receipt-"+phase || appends == 3 && fault == "grant-"+phase {
				kill()
			}
			return nil
		}
		config.Registry.(*startupReservationRegistryFixture).reserve = func(_ context.Context, request fleet.RuntimeStartupRequest) (fleet.RuntimeStartupReservation, error) {
			// Persist a fixture call marker outside the association ledger before
			// simulating a committed reply. This is not a PostgreSQL receipt.
			file, err := os.OpenFile(os.Getenv("VELA_STARTUP_RESERVATION_CALL"), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := file.WriteString(request.RequestID.String()); err != nil {
				t.Fatal(err)
			}
			if err := errors.Join(file.Sync(), file.Close()); err != nil {
				t.Fatal(err)
			}
			if fault == "fleet-reply" {
				kill()
			}
			return fleet.RuntimeStartupReservation{RuntimeStartupRequest: request, Fresh: true, ReservedAt: time.Now().UTC()}, nil
		}
		reservation, err := ledger.ReserveRemote(t.Context(), config)
		if err == nil && strings.HasPrefix(fault, "grant-") {
			_, err = ledger.ConsumeJournalGrantAttempt(t.Context(), reservation.JournalID, reservation.OperationID, [32]byte{1})
		}
		t.Fatalf("crash boundary was not reached: %v", err)
	}
	if os.Geteuid() != 0 {
		t.Skip("requires root Node and nested PID namespaces")
	}
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	for _, fault := range []string{"intent-before-append", "intent-after-append", "intent-after-sync", "fleet-reply", "receipt-before-append", "receipt-after-append", "receipt-after-sync", "grant-before-append", "grant-after-append", "grant-after-sync"} {
		t.Run(fault, func(t *testing.T) {
			root, err := os.MkdirTemp("/tmp", "vela-reservation-crash-")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = os.RemoveAll(root) })
			directory, marker := filepath.Join(root, "ledger"), filepath.Join(root, "fleet-call")
			if err := os.Mkdir(directory, 0o700); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
			defer cancel()
			command := exec.CommandContext(ctx, binary, "-test.run=^TestRuntimeStartupReservationProcessCrashRecovery$", "-test.timeout=12s")
			command.Env = []string{modeKey + "=" + fault, "VELA_STARTUP_RESERVATION_LEDGER=" + directory,
				"VELA_STARTUP_RESERVATION_CALL=" + marker, "TMPDIR=" + root}
			command.SysProcAttr = &syscall.SysProcAttr{Cloneflags: unix.CLONE_NEWPID | unix.CLONE_NEWNS}
			reader, writer, err := os.Pipe()
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = reader.Close(); _ = writer.Close() })
			command.ExtraFiles = []*os.File{writer}
			var output bytes.Buffer
			command.Stdout, command.Stderr = &output, &output
			if err := command.Start(); err != nil {
				t.Fatal(err)
			}
			waited := false
			t.Cleanup(func() {
				if !waited {
					_ = command.Process.Kill()
					_ = command.Wait()
				}
				if t.Failed() {
					t.Logf("Node crash helper: %s", output.String())
				}
			})
			if err := writer.Close(); err != nil {
				t.Fatal(err)
			}
			if err := reader.SetReadDeadline(time.Now().Add(8 * time.Second)); err != nil {
				t.Fatal(err)
			}
			point := make([]byte, len(fault)+1)
			if _, err := io.ReadFull(reader, point); err != nil || string(point) != fault+"\n" {
				t.Fatalf("Node did not reach exact crash boundary: %q %v", point, err)
			}
			if err := command.Process.Kill(); err != nil {
				t.Fatal(err)
			}
			err = command.Wait()
			waited = true
			var exit *exec.ExitError
			if !errors.As(err, &exit) || ctx.Err() != nil {
				t.Fatalf("Node did not die at boundary: %v %s", err, output.String())
			}
			status, ok := exit.Sys().(syscall.WaitStatus)
			if !ok || !status.Signaled() || status.Signal() != syscall.SIGKILL {
				t.Fatalf("Node did not receive SIGKILL: %v %s", status, output.String())
			}
			recovered, err := OpenRuntimeStartupLedger(t.Context(), directory, "cpu-node", false)
			if err != nil {
				t.Fatalf("crash history could not reopen: %v", err)
			}
			t.Cleanup(func() { _ = recovered.Close() })
			calls, markerErr := os.ReadFile(marker)
			wantCall := !strings.HasPrefix(fault, "intent-")
			if wantCall && markerErr != nil || !wantCall && !errors.Is(markerErr, os.ErrNotExist) {
				t.Fatalf("Fleet call crossed ordering boundary: %v", markerErr)
			}
			wantStarts := 1
			if fault == "intent-before-append" {
				wantStarts = 0
			}
			wantReceipts := 0
			if fault == "receipt-after-append" || fault == "receipt-after-sync" || strings.HasPrefix(fault, "grant-") {
				wantReceipts = 1
			}
			if len(recovered.starts) != wantStarts || len(recovered.reservations) != wantReceipts || len(recovered.owners) != 0 {
				t.Fatalf("crash changed durable intent/receipt or recreated pidfd: %d/%d/%d", len(recovered.starts), len(recovered.reservations), len(recovered.owners))
			}
			wantGrants := 0
			if fault == "grant-after-append" || fault == "grant-after-sync" {
				wantGrants = 1
			}
			if len(recovered.grantAttempts) != wantGrants {
				t.Fatalf("recovered grants=%d want=%d", len(recovered.grantAttempts), wantGrants)
			}
			for id, record := range recovered.starts {
				if _, err := recovered.ConsumeJournalGrantAttempt(t.Context(), id, record.OperationID, [32]byte{1}); err == nil {
					t.Fatal("crash recovery consumed a new grant attempt")
				}
				if wantCall && record.OperationID.String() != string(calls) {
					t.Fatal("Fleet marker is not bound to the persisted original intent")
				}
				if _, err := recovered.RecordExit(t.Context(), id); !errors.Is(err, ErrRuntimeNamespaceOwnerLost) {
					t.Fatalf("crash recovery inferred original pidfd exit: %v", err)
				}
			}
			t.Logf("Node SIGKILL at %s: intents=%d receipts=%d; original pidfd not reconstructed", fault, wantStarts, wantReceipts)
		})
	}
}

func (registry *startupReservationRegistryFixture) NodeIdentity() string  { return registry.node }
func (registry *startupReservationRegistryFixture) ActorIdentity() string { return registry.actor }
func (registry *startupReservationRegistryFixture) ReserveRuntimeStartup(ctx context.Context, request fleet.RuntimeStartupRequest) (fleet.RuntimeStartupReservation, error) {
	registry.calls++
	if registry.reserve != nil {
		return registry.reserve(ctx, request)
	}
	return fleet.RuntimeStartupReservation{RuntimeStartupRequest: request, Fresh: true, ReservedAt: time.Now().UTC()}, nil
}

func newRemoteReservationFixture(t *testing.T, oldRoutes bool) (runtimeStartupFixture, RuntimeStartupReservationConfig) {
	t.Helper()
	if os.Geteuid() != 0 {
		t.Skip("requires root Node and non-root PID-1 Runtime")
	}
	launch := runtimeLaunchFixture(t)
	validator, err := stageauthority.NewValidator(map[string][]byte{"authority": make([]byte, 32)}, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	routes, err := modelruntime.RemoteStartupBindings(launch.launch)
	if err != nil {
		t.Fatal(err)
	}
	if oldRoutes {
		routes, err = launch.launch.RuntimeBindings()
		if err != nil {
			t.Fatal(err)
		}
		for i := range routes {
			routes[i].ModelRuntimeEpoch = launch.launch.Runtimes[i].ModelRuntimeEpochFloor
		}
	}
	directory := t.TempDir()
	if err := os.Chmod(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	owner, err := modelruntime.OpenExecutionJournalOwner(modelruntime.ExecutionJournalOwnerConfig{Manifest: launch.launch,
		Validator: validator, State: modelruntime.ExecutionFloorStateConfig{Directory: directory, Initialize: true}, Routes: routes, Now: time.Now})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = owner.Close() })
	startup, err := owner.RecordBackendStartupIntent(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	journal, err := owner.Status(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	launch.binding.Pair.RuntimeJournalId, launch.binding.Pair.RuntimeScope = journal.JournalID.String(), journal.Scope[:]
	launch.binding.Signature, launch.binding.SigningKeyId = nil, ""
	launch.binding, err = launch.signer.Sign(launch.binding)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := VerifyRuntimeLaunchPlan("cpu-node", launch.verifier, launch.binding, launch.wire)
	if err != nil {
		t.Fatal(err)
	}
	binding, err := proto.MarshalOptions{Deterministic: true}.Marshal(plan.binding)
	if err != nil {
		t.Fatal(err)
	}
	request := modelruntime.BackendStartupRequest{SchemaVersion: 1, NodeIdentity: "cpu-node", RegistryBindingDigest: sha256.Sum256(binding),
		JournalID: journal.JournalID, JournalScope: journal.Scope, IncarnationID: startup.IncarnationID, LaunchDigest: startup.LaunchDigest}
	wire, err := modelruntime.EncodeBackendStartupRequest(request)
	if err != nil {
		t.Fatal(err)
	}
	credentials := RuntimeCallerCredentials{UID: plan.uid, GID: plan.gid}
	connection, process, _ := runtimeCallerConfiguredConnection(t, "hold-after-disconnect", "unixpacket", wire, credentials, true)
	caller, err := ReceiveRuntimeCaller(t.Context(), connection, credentials)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = caller.Close() })
	observer, pods := startupPlanObserverFixture(t, plan, caller)
	registry := &startupReservationRegistryFixture{node: plan.binding.Claim.NodeIdentity, actor: plan.binding.Claim.ActorIdentity}
	return runtimeStartupFixture{plan: plan, pods: pods, observer: observer, caller: caller, process: process, connection: connection, request: request},
		RuntimeStartupReservationConfig{Plan: plan, Pods: pods, Observer: observer, Caller: caller, Journal: owner, Registry: registry}
}

func TestRuntimeStartupReservationLinksOriginalOwnerAndNodeJournal(t *testing.T) {
	f, config := newRemoteReservationFixture(t, false)
	ledger, directory := startupTestLedger(t)
	registry := config.Registry.(*startupReservationRegistryFixture)
	registry.reserve = func(_ context.Context, request fleet.RuntimeStartupRequest) (fleet.RuntimeStartupReservation, error) {
		wire, err := os.ReadFile(filepath.Join(directory, runtimeStartupLedgerName))
		if err != nil {
			t.Fatal(err)
		}
		lines := bytes.Split(bytes.TrimSuffix(wire, []byte{'\n'}), []byte{'\n'})
		if len(lines) != 2 {
			t.Fatalf("Fleet called before exactly one durable Node intent: %d", len(lines))
		}
		var entry runtimeStartupEntry
		if err := json.Unmarshal(lines[1], &entry); err != nil {
			t.Fatal(err)
		}
		expected, err := remoteFleetRequest(*entry.Startup)
		if err != nil || !reflect.DeepEqual(request, expected) || len(request.Epochs) != 1 || request.Epochs[0].ModelRuntimeEpoch != 2 {
			t.Fatalf("Fleet did not receive original Node-derived identity: %+v %v", request, err)
		}
		if _, err := ledger.owners[f.request.JournalID].ObserveExit(t.Context()); !errors.Is(err, ErrRuntimeNamespaceOwnerLive) {
			t.Fatalf("original handle missing: %v", err)
		}
		return fleet.RuntimeStartupReservation{RuntimeStartupRequest: request, Fresh: true, ReservedAt: time.Now().UTC()}, nil
	}
	result, err := ledger.ReserveRemote(t.Context(), config)
	if err != nil || registry.calls != 1 {
		t.Fatalf("reserve: %+v %v calls=%d", result, err, registry.calls)
	}
	record, err := ledger.Inspect(t.Context(), f.request.JournalID)
	if err != nil || record.Remote == nil || result.OperationID != record.OperationID {
		t.Fatalf("lost remote intent: %v", err)
	}
	record.Remote.Epochs[0].ModelRuntimeEpoch++
	current, err := ledger.Inspect(t.Context(), f.request.JournalID)
	if err != nil || current.Remote.Epochs[0].ModelRuntimeEpoch != 2 {
		t.Fatal("inspection aliases retained epochs")
	}
	if _, err := ledger.ReserveRemote(t.Context(), config); !errors.Is(err, ErrRuntimeStartupRecorded) || registry.calls != 1 {
		t.Fatalf("repeat reserved again: %v", err)
	}
	if err := ledger.Close(); err != nil {
		t.Fatal(err)
	}
	recovered, err := OpenRuntimeStartupLedger(t.Context(), directory, "cpu-node", false)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = recovered.Close() })
	if history, err := recovered.InspectReservation(t.Context(), f.request.JournalID); err != nil || history != result {
		t.Fatalf("restart lost reservation history: %+v %v", history, err)
	}
	if _, err := recovered.ReserveRemote(t.Context(), config); !errors.Is(err, ErrRuntimeStartupRecorded) || registry.calls != 1 {
		t.Fatalf("restart reissued reservation: %v", err)
	}
	if _, err := recovered.RecordExit(t.Context(), f.request.JournalID); !errors.Is(err, ErrRuntimeNamespaceOwnerLost) {
		t.Fatalf("restart recreated original pidfd: %v", err)
	}
	t.Log("root Node journal + actual non-root original PID-1; one fixture Fleet reservation; restart reads history only; no permit response")
}

func TestRuntimeStartupReservationRejectsWrongOwnerRoutesAndLegacyWriter(t *testing.T) {
	for _, mode := range []string{"old-routes", "wrong-registry", "legacy"} {
		t.Run(mode, func(t *testing.T) {
			_, config := newRemoteReservationFixture(t, mode == "old-routes")
			ledger, directory := startupTestLedger(t)
			registry := config.Registry.(*startupReservationRegistryFixture)
			if mode == "wrong-registry" {
				registry.actor = "other-actor"
			}
			if mode == "legacy" {
				if err := ledger.Close(); err != nil {
					t.Fatal(err)
				}
				path := filepath.Join(directory, runtimeStartupLedgerName)
				wire, err := os.ReadFile(path)
				if err != nil {
					t.Fatal(err)
				}
				wire = bytes.Replace(wire, []byte(`"schema_version":3`), []byte(`"schema_version":1`), 1)
				if err := os.WriteFile(path, wire, 0o600); err != nil {
					t.Fatal(err)
				}
				ledger, err = OpenRuntimeStartupLedger(t.Context(), directory, "cpu-node", false)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = ledger.Close() })
			}
			if result, err := ledger.ReserveRemote(t.Context(), config); err == nil || result != (RuntimeStartupReservationRecord{}) || registry.calls != 0 {
				t.Fatalf("invalid custody/principal/version reached Fleet: %+v %v calls=%d", result, err, registry.calls)
			}
			if mode == "legacy" {
				local, err := ledger.Record(t.Context(), config.Plan, config.Pods, config.Observer, config.Caller)
				if err != nil || local.Remote != nil {
					t.Fatalf("schema 1 lost its original local-only writer: %v", err)
				}
				if err := ledger.Close(); err != nil {
					t.Fatal(err)
				}
				recovered, err := OpenRuntimeStartupLedger(t.Context(), directory, "cpu-node", false)
				if err != nil {
					t.Fatal(err)
				}
				defer func() { _ = recovered.Close() }()
				current, err := recovered.Inspect(t.Context(), local.Request.JournalID)
				if err != nil || !reflect.DeepEqual(current, local) || recovered.header.SchemaVersion != 1 {
					t.Fatalf("legacy recovery changed local history or silently upgraded: %v", err)
				}
			}
		})
	}
}

func TestRuntimeStartupReservationFailuresNeverRetryConsumedIntent(t *testing.T) {
	for _, fault := range []string{"intent-before-append", "intent-after-append", "intent-after-sync", "fleet-loss", "fleet-history", "fleet-mismatch", "journal-closed", "pod-changed", "owner-exit", "receipt-before-append", "receipt-after-append", "receipt-after-sync"} {
		t.Run(fault, func(t *testing.T) {
			f, config := newRemoteReservationFixture(t, false)
			ledger, directory := startupTestLedger(t)
			registry := config.Registry.(*startupReservationRegistryFixture)
			appends := 0
			ledger.boundary = func(phase string) error {
				if phase == "before-append" {
					appends++
				}
				if (appends == 1 && fault == "intent-"+phase) || (appends == 2 && fault == "receipt-"+phase) {
					return errors.New("injected append uncertainty")
				}
				return nil
			}
			registry.reserve = func(_ context.Context, request fleet.RuntimeStartupRequest) (fleet.RuntimeStartupReservation, error) {
				result := fleet.RuntimeStartupReservation{RuntimeStartupRequest: request, Fresh: true, ReservedAt: time.Now().UTC()}
				switch fault {
				case "fleet-loss":
					return fleet.RuntimeStartupReservation{}, errors.New("fixture committed response lost")
				case "fleet-history":
					result.Fresh = false
				case "fleet-mismatch":
					result.OwnerObservationDigest[0] ^= 1
				case "journal-closed":
					if err := config.Journal.Close(); err != nil {
						t.Fatal(err)
					}
				case "pod-changed":
					f.pods.mu.Lock()
					f.pods.pod.ResourceVersion = "2"
					f.pods.mu.Unlock()
				case "owner-exit":
					if err := f.process.Kill(); err != nil {
						t.Fatal(err)
					}
					deadline := time.Now().Add(time.Second)
					for {
						if _, err := ledger.owners[f.request.JournalID].ObserveExit(t.Context()); err == nil {
							break
						}
						if time.Now().After(deadline) {
							t.Fatal("original process did not exit")
						}
						time.Sleep(time.Millisecond)
					}
				}
				return result, nil
			}
			if result, err := ledger.ReserveRemote(t.Context(), config); err == nil || result != (RuntimeStartupReservationRecord{}) {
				t.Fatalf("uncertain operation succeeded: %+v %v", result, err)
			}
			calls := registry.calls
			if strings.HasPrefix(fault, "intent-") && calls != 0 || !strings.HasPrefix(fault, "intent-") && calls != 1 {
				t.Fatalf("unexpected Fleet calls: %d", calls)
			}
			if _, err := ledger.ReserveRemote(t.Context(), config); err == nil || registry.calls != calls {
				t.Fatalf("live retry consumed authority: %v calls=%d", err, registry.calls)
			}
			if err := ledger.Close(); err != nil {
				t.Fatal(err)
			}
			recovered, err := OpenRuntimeStartupLedger(t.Context(), directory, "cpu-node", false)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = recovered.Close() })
			if fault == "intent-before-append" {
				if _, err := recovered.Inspect(t.Context(), f.request.JournalID); !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("unpublished intent exists: %v", err)
				}
				if _, err := recovered.ReserveRemote(t.Context(), config); err != nil || registry.calls != 1 {
					t.Fatalf("safe first use could not proceed: %v", err)
				}
				return
			}
			if _, err := recovered.Inspect(t.Context(), f.request.JournalID); err != nil {
				t.Fatalf("lost committed Node intent: %v", err)
			}
			_, err = recovered.InspectReservation(t.Context(), f.request.JournalID)
			wantReceipt := fault == "receipt-after-append" || fault == "receipt-after-sync"
			if wantReceipt && err != nil || !wantReceipt && !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("recovery changed reservation history: %v", err)
			}
			if _, err := recovered.ReserveRemote(t.Context(), config); err == nil || registry.calls != calls {
				t.Fatalf("restart repeated Fleet call: %v", err)
			}
			if _, err := recovered.RecordExit(t.Context(), f.request.JournalID); !errors.Is(err, ErrRuntimeNamespaceOwnerLost) {
				t.Fatalf("restart inferred original exit: %v", err)
			}
		})
	}
}
