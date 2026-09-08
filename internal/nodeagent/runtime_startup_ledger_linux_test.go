package nodeagent

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/fleetcontroller"
	"github.com/vivym/vela/internal/modelruntime"
	"google.golang.org/protobuf/proto"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
)

type runtimeStartupFixture struct {
	plan       *RuntimeLaunchPlan
	pods       *runtimeLaunchPodFixture
	observer   *RuntimeContainerObserver
	caller     *RuntimeCaller
	process    *os.Process
	connection *net.UnixConn
	request    modelruntime.BackendStartupRequest
}

func newRuntimeStartupFixture(t *testing.T, mutate func(*modelruntime.BackendStartupRequest)) runtimeStartupFixture {
	t.Helper()
	launch := runtimeLaunchFixture(t)
	plan, err := VerifyRuntimeLaunchPlan("cpu-node", launch.verifier, launch.binding, launch.wire)
	if err != nil {
		t.Fatal(err)
	}
	binding, err := proto.MarshalOptions{Deterministic: true}.Marshal(plan.binding)
	if err != nil {
		t.Fatal(err)
	}
	request := modelruntime.BackendStartupRequest{SchemaVersion: 1, NodeIdentity: "cpu-node", RegistryBindingDigest: sha256.Sum256(binding),
		JournalID: uuid.MustParse(plan.binding.Pair.RuntimeJournalId), JournalScope: [32]byte(plan.binding.Pair.RuntimeScope),
		IncarnationID: uuid.New(), LaunchDigest: sha256.Sum256(plan.manifest)}
	if mutate != nil {
		mutate(&request)
	}
	encoded, err := modelruntime.EncodeBackendStartupRequest(request)
	if err != nil {
		t.Fatal(err)
	}
	credentials := RuntimeCallerCredentials{UID: plan.uid, GID: plan.gid}
	connection, process, _ := runtimeCallerConfiguredConnection(t, "hold-after-disconnect", "unixpacket", encoded, credentials, true)
	caller, err := ReceiveRuntimeCaller(t.Context(), connection, credentials)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = caller.Close() })
	observer, pods := startupPlanObserverFixture(t, plan, caller)
	return runtimeStartupFixture{plan: plan, pods: pods, observer: observer, caller: caller, process: process, connection: connection, request: request}
}

func startupPlanObserverFixture(t *testing.T, plan *RuntimeLaunchPlan, caller *RuntimeCaller) (*RuntimeContainerObserver, *runtimeLaunchPodFixture) {
	t.Helper()
	process, err := caller.Inspect(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	cri, _, observer := runtimeCallerObserverFixture(t, process)
	pod := plan.ExpectedPod()
	pod.UID, pod.ResourceVersion, pod.Spec.NodeName = types.UID(cri.target.PodUID.String()), "1", "cpu-node"
	pod.Status.ContainerStatuses = []corev1.ContainerStatus{{Name: "model-runtime", ContainerID: "containerd://" + cri.target.ContainerID,
		RestartCount: int32(cri.target.ContainerAttempt), State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}}}
	cri.mu.Lock()
	cri.resolveTarget = true
	cri.target.PodName, cri.target.PodNamespace = pod.Name, pod.Namespace
	cri.sandbox.Status.Metadata.Name, cri.sandbox.Status.Metadata.Namespace = pod.Name, pod.Namespace
	cri.mu.Unlock()
	return observer, &runtimeLaunchPodFixture{pod: *pod, key: fleetcontroller.ResourceKey{Namespace: pod.Namespace, Name: pod.Name}}
}

func startupTestLedger(t *testing.T) (*RuntimeStartupLedger, string) {
	t.Helper()
	if os.Geteuid() != 0 {
		t.Skip("requires root-owned Node state")
	}
	directory := t.TempDir()
	if err := os.Chmod(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	ledger, err := OpenRuntimeStartupLedger(t.Context(), directory, "cpu-node", true)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ledger.Close() })
	return ledger, directory
}

func TestRuntimeStartupLedgerRetainsExactOwnerExit(t *testing.T) {
	f := newRuntimeStartupFixture(t, nil)
	ledger, directory := startupTestLedger(t)
	if other, err := OpenRuntimeStartupLedger(t.Context(), directory, "cpu-node", false); err == nil || other != nil {
		t.Fatal("two Node processes could own the startup ledger")
	}
	var wait sync.WaitGroup
	records := make(chan RuntimeStartupRecord, 8)
	for range 8 {
		wait.Go(func() {
			record, err := ledger.Record(t.Context(), f.plan, f.pods, f.observer, f.caller)
			if err == nil {
				records <- record
			} else if !errors.Is(err, ErrRuntimeStartupRecorded) {
				t.Errorf("record: %v", err)
			}
		})
	}
	wait.Wait()
	close(records)
	if len(records) != 1 {
		t.Fatalf("concurrent registration produced %d records", len(records))
	}
	original := <-records
	expected := cloneRuntimeStartup(original)
	original.RegistryBinding[0] ^= 1
	if current, err := ledger.Inspect(t.Context(), f.request.JournalID); err != nil || !reflect.DeepEqual(current, expected) {
		t.Fatalf("inspection changed retained data: %v", err)
	}
	original = expected
	if err := errors.Join(f.caller.Close(), f.connection.Close(), f.observer.Close()); err != nil {
		t.Fatal(err)
	}
	if _, err := ledger.RecordExit(t.Context(), f.request.JournalID); !errors.Is(err, ErrRuntimeNamespaceOwnerLive) {
		t.Fatalf("disconnect manufactured exit: %v", err)
	}
	if err := f.process.Kill(); err != nil {
		t.Fatal(err)
	}
	var exit RuntimeStartupExit
	deadline := time.Now().Add(3 * time.Second)
	for {
		var err error
		exit, err = ledger.RecordExit(t.Context(), f.request.JournalID)
		if err == nil {
			break
		}
		if !errors.Is(err, ErrRuntimeNamespaceOwnerLive) || time.Now().After(deadline) {
			t.Fatal(err)
		}
		time.Sleep(10 * time.Millisecond)
	}
	if exit.OperationID != original.OperationID || exit.Observation.Owner != original.Owner {
		t.Fatal("exit changed original association")
	}
	if err := ledger.Close(); err != nil {
		t.Fatal(err)
	}
	recovered, err := OpenRuntimeStartupLedger(t.Context(), directory, "cpu-node", false)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = recovered.Close() })
	if observed, err := recovered.RecordExit(t.Context(), f.request.JournalID); err != nil || observed != exit {
		t.Fatalf("durable exact exit lost across reopen: %v", err)
	}
	if current, err := recovered.Inspect(t.Context(), f.request.JournalID); err != nil || !reflect.DeepEqual(current, original) {
		t.Fatalf("recovered startup changed: %v", err)
	}
}

func TestRuntimeStartupLedgerRestartDoesNotReconstructOwner(t *testing.T) {
	f := newRuntimeStartupFixture(t, nil)
	ledger, directory := startupTestLedger(t)
	original, err := ledger.Record(t.Context(), f.plan, f.pods, f.observer, f.caller)
	if err != nil {
		t.Fatal(err)
	}
	if err := ledger.Close(); err != nil {
		t.Fatal(err)
	}
	recovered, err := OpenRuntimeStartupLedger(t.Context(), directory, "cpu-node", false)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = recovered.Close() })
	for _, exited := range []bool{false, true} {
		if exited {
			if err := f.process.Kill(); err != nil {
				t.Fatal(err)
			}
		}
		if _, err := recovered.RecordExit(t.Context(), f.request.JournalID); !errors.Is(err, ErrRuntimeNamespaceOwnerLost) {
			t.Fatalf("reopen inferred prior owner's exit: %v", err)
		}
		if _, err := recovered.Record(t.Context(), f.plan, f.pods, f.observer, f.caller); !errors.Is(err, ErrRuntimeStartupRecorded) {
			t.Fatalf("replay registered a new owner: %v", err)
		}
		if current, err := recovered.Inspect(t.Context(), f.request.JournalID); err != nil || !reflect.DeepEqual(current, original) {
			t.Fatalf("reopen altered original: %v", err)
		}
	}
}

func TestRuntimeStartupLedgerReservesExitCapacity(t *testing.T) {
	f := newRuntimeStartupFixture(t, nil)
	ledger, directory := startupTestLedger(t)
	original, err := ledger.Record(t.Context(), f.plan, f.pods, f.observer, f.caller)
	if err != nil {
		t.Fatal(err)
	}
	// Pause the helper's own test timeout while the race build fills the ledger.
	if err := f.process.Signal(syscall.SIGSTOP); err != nil {
		t.Fatal(err)
	}
	// Synthetic signed history exercises the byte limit without launching hundreds
	// of processes. Escaped, bounded CRI text must count at its JSON wire size.
	for {
		fixture := runtimeLaunchFixture(t)
		record := cloneRuntimeStartup(original)
		record.OperationID, record.Request.JournalID = uuid.New(), uuid.MustParse(fixture.binding.Pair.RuntimeJournalId)
		record.RegistryBinding, err = proto.MarshalOptions{Deterministic: true}.Marshal(fixture.binding)
		if err != nil {
			t.Fatal(err)
		}
		record.Request.RegistryBindingDigest = sha256.Sum256(record.RegistryBinding)
		record.Owner.Container.ImageRef = strings.Repeat("<", 1024)
		entry := runtimeStartupEntry{Startup: &record}
		before := ledger.size
		err = ledger.append(t.Context(), entry)
		if err == nil {
			if len(ledger.starts) >= maxRuntimeStartupRecords {
				t.Fatal("fixture reached the record count limit before the byte limit")
			}
			continue
		}
		if !errors.Is(err, ErrRuntimeStartupLedger) || ledger.size != before || ledger.failed != nil {
			t.Fatalf("capacity rejection changed or poisoned the ledger: %v", err)
		}
		wire, err := json.Marshal(entry)
		if err != nil || before+int64(len(wire))+1 > maxRuntimeStartupLedgerBytes {
			t.Fatal("new history exhausted the byte budget needed for pending exits")
		}
		break
	}
	t.Logf("startup admission rejected at %d records and %d persisted bytes", len(ledger.starts), ledger.size)
	if _, err := ledger.RecordExit(t.Context(), f.request.JournalID); !errors.Is(err, ErrRuntimeNamespaceOwnerLive) {
		t.Fatalf("capacity exhaustion or a paused owner manufactured exit: %v", err)
	}
	if err := f.process.Kill(); err != nil {
		t.Fatal(err)
	}
	var exit RuntimeStartupExit
	deadline := time.Now().Add(3 * time.Second)
	for {
		exit, err = ledger.RecordExit(t.Context(), f.request.JournalID)
		if err == nil {
			break
		}
		if !errors.Is(err, ErrRuntimeNamespaceOwnerLive) || time.Now().After(deadline) {
			t.Fatalf("saturated ledger could not retain original-owner exit: %v", err)
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err := ledger.Close(); err != nil {
		t.Fatal(err)
	}
	recovered, err := OpenRuntimeStartupLedger(t.Context(), directory, "cpu-node", false)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = recovered.Close() })
	if current, err := recovered.RecordExit(t.Context(), f.request.JournalID); err != nil || current != exit {
		t.Fatalf("saturated ledger lost durable exit on reopen: %v", err)
	}
}

func TestRuntimeStartupLedgerRejectsUnboundRequests(t *testing.T) {
	for _, fault := range []string{"node", "binding", "journal", "scope", "launch", "pod", "closed-caller", "canceled"} {
		t.Run(fault, func(t *testing.T) {
			f := newRuntimeStartupFixture(t, func(request *modelruntime.BackendStartupRequest) {
				switch fault {
				case "node":
					request.NodeIdentity = "other-node"
				case "binding":
					request.RegistryBindingDigest[0] ^= 1
				case "journal":
					request.JournalID = uuid.New()
				case "scope":
					request.JournalScope[0] ^= 1
				case "launch":
					request.LaunchDigest[0] ^= 1
				}
			})
			ledger, _ := startupTestLedger(t)
			before := runtimeCallerDescriptorCount(t)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			switch fault {
			case "pod":
				f.pods.pod.Spec.NodeName = "other-node"
			case "closed-caller":
				_ = f.caller.Close()
				before = runtimeCallerDescriptorCount(t)
			case "canceled":
				cancel()
			}
			if record, err := ledger.Record(ctx, f.plan, f.pods, f.observer, f.caller); err == nil || record.OperationID != uuid.Nil {
				t.Fatal("unbound request registered")
			}
			if len(ledger.starts) != 0 || runtimeCallerDescriptorCount(t) != before {
				t.Fatal("rejected request changed records or leaked owner handles")
			}
		})
	}
}

func TestRuntimeStartupLedgerUncertainAppendRemainsConsumed(t *testing.T) {
	for _, phase := range []string{"before-append", "after-append", "after-sync", "cancel-after-sync"} {
		t.Run(phase, func(t *testing.T) {
			f := newRuntimeStartupFixture(t, nil)
			ledger, directory := startupTestLedger(t)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			ledger.boundary = func(current string) error {
				if phase == "cancel-after-sync" && current == "after-sync" {
					cancel()
					return nil
				}
				if current == phase {
					return errors.New("injected persistence interruption")
				}
				return nil
			}
			before := runtimeCallerDescriptorCount(t)
			if record, err := ledger.Record(ctx, f.plan, f.pods, f.observer, f.caller); err == nil || record.OperationID != uuid.Nil {
				t.Fatal("uncertain append returned a usable result")
			}
			if len(ledger.owners) != 0 || runtimeCallerDescriptorCount(t) != before {
				t.Fatal("uncertain append leaked owner")
			}
			if _, err := ledger.Inspect(t.Context(), f.request.JournalID); !errors.Is(err, ErrRuntimeStartupLedger) {
				t.Fatal("failed handle was reused")
			}
			if err := ledger.Close(); err != nil {
				t.Fatal(err)
			}
			recovered, err := OpenRuntimeStartupLedger(t.Context(), directory, "cpu-node", false)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = recovered.Close() })
			if phase == "before-append" {
				if _, err := recovered.Inspect(t.Context(), f.request.JournalID); !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("pre-write fault created history: %v", err)
				}
				return
			}
			if record, err := recovered.Inspect(t.Context(), f.request.JournalID); err != nil || record.Request != f.request {
				t.Fatalf("committed or uncertain append disappeared: %v", err)
			}
			if _, err := recovered.Record(t.Context(), f.plan, f.pods, f.observer, f.caller); !errors.Is(err, ErrRuntimeStartupRecorded) {
				t.Fatal("uncertain append issued another registration")
			}
			if _, err := recovered.RecordExit(t.Context(), f.request.JournalID); !errors.Is(err, ErrRuntimeNamespaceOwnerLost) {
				t.Fatal("uncertain append reconstructed an exit handle")
			}
		})
	}
}

func TestRuntimeStartupLedgerRejectsMissingAndChangedState(t *testing.T) {
	for _, fault := range []string{"missing", "partial", "duplicate-key", "hardlink", "file-mode", "file-owner", "directory-mode", "directory-replaced", "file-replaced"} {
		t.Run(fault, func(t *testing.T) {
			ledger, directory := startupTestLedger(t)
			path := filepath.Join(directory, runtimeStartupLedgerName)
			original, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			var faultErr error
			switch fault {
			case "missing":
				faultErr = os.Remove(path)
			case "partial":
				faultErr = os.WriteFile(path, append(original, []byte(`{"startup":`)...), 0o600)
			case "duplicate-key":
				faultErr = os.WriteFile(path, bytes.Replace(original, []byte(`"schema_version":2`), []byte(`"schema_version":2,"schema_version":2`), 1), 0o600)
			case "hardlink":
				faultErr = os.Link(path, path+".alias")
			case "file-mode":
				faultErr = os.Chmod(path, 0o644)
			case "file-owner":
				faultErr = os.Chown(path, 10001, 10001)
			case "directory-mode":
				faultErr = os.Chmod(directory, 0o755)
			case "directory-replaced":
				faultErr = os.Rename(directory, directory+".original")
				t.Cleanup(func() { _ = os.RemoveAll(directory + ".original") })
				if faultErr == nil {
					faultErr = os.Mkdir(directory, 0o700)
				}
			case "file-replaced":
				faultErr = os.Rename(path, path+".original")
				if faultErr == nil {
					faultErr = os.WriteFile(path, original, 0o600)
				}
			}
			if faultErr != nil {
				t.Fatal(faultErr)
			}
			if _, err := ledger.Inspect(t.Context(), uuid.New()); err == nil {
				t.Fatal("changed state accepted by live ledger")
			}
			if err := ledger.Close(); err != nil {
				t.Fatal(err)
			}
			if recovered, err := OpenRuntimeStartupLedger(t.Context(), directory, "cpu-node", false); err == nil || recovered != nil {
				t.Fatal("recovery accepted missing or untrusted history")
			}
			if fresh, err := OpenRuntimeStartupLedger(t.Context(), directory, "cpu-node", true); fault != "directory-replaced" && fault != "missing" && (err == nil || fresh != nil) {
				t.Fatal("explicit initialization overwrote retained state")
			} else if fresh != nil {
				_ = fresh.Close()
			}
		})
	}
}
