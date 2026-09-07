package workerbootstrap

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/fleet"
	"github.com/vivym/vela/internal/fleetcontroller"
	"github.com/vivym/vela/internal/modelruntime"
	"github.com/vivym/vela/internal/stageauthority"
	"github.com/vivym/vela/internal/stageworkeragent"
)

func TestPrepareRecordsActualPairAndRecoversReceiptResponseLoss(t *testing.T) {
	config, registry := bootstrapFixture(t)
	registry.loseReceipt = true
	if result, err := Prepare(t.Context(), config, registry); err == nil || result != (Result{}) {
		t.Fatalf("lost receipt response exposed success: %+v %v", result, err)
	}
	before := snapshotFiles(t, config.ScratchDirectory)
	registry.loseReceipt = false
	result, err := Prepare(t.Context(), config, registry)
	if err != nil || result.RequestID == uuid.Nil || result.Worker.SchemaVersion != 5 || result.Runtime.SchemaVersion != 6 ||
		result.Worker.Watermark != 0 || result.Runtime.Highest != 0 || registry.claimCalls != 1 || registry.receiptCalls != 2 {
		t.Fatalf("recover complete pair: %+v %v, claims=%d receipts=%d", result, err, registry.claimCalls, registry.receiptCalls)
	}
	if registry.receipt.WorkerJournalID != result.Worker.JournalID || registry.receipt.RuntimeJournalID != result.Runtime.JournalID ||
		!bytes.Equal(registry.receipt.WorkerScope, result.Worker.Scope[:]) || !bytes.Equal(registry.receipt.RuntimeScope, result.Runtime.Scope[:]) {
		t.Fatal("Registry receipt was not derived from actual journals")
	}
	again, err := Prepare(t.Context(), config, registry)
	if err != nil || again != result || !reflect.DeepEqual(before, snapshotFiles(t, config.ScratchDirectory)) {
		t.Fatalf("recovery changed pair, timestamp or files: %+v %v", again, err)
	}
}

func TestPrepareInterruptedBoundariesNeverReinitialize(t *testing.T) {
	for _, stop := range []string{"operation-durable", "claim-committed", "worker-prepared", "runtime-prepared", "origin-durable", "pair-durable", "pair-recovered", "receipt-committed"} {
		t.Run(stop, func(t *testing.T) {
			config, registry := bootstrapFixture(t)
			injected := errors.New("interrupted at " + stop)
			result, err := prepare(t.Context(), config, registry, func(phase string) error {
				if phase == stop {
					return injected
				}
				return nil
			})
			if !errors.Is(err, injected) || result != (Result{}) {
				t.Fatalf("fault boundary: %+v %v", result, err)
			}
			before := snapshotFiles(t, config.ScratchDirectory)
			claims := registry.claimCalls
			result, err = Prepare(t.Context(), config, registry)
			complete := stop == "pair-durable" || stop == "pair-recovered" || stop == "receipt-committed"
			if complete && err != nil || !complete && (!errors.Is(err, ErrIncomplete) || result != (Result{})) {
				t.Fatalf("restart inferred first use or lost complete pair: %+v %v", result, err)
			}
			if registry.claimCalls != claims || !reflect.DeepEqual(before, snapshotFiles(t, config.ScratchDirectory)) {
				t.Fatal("restart retried claim or changed retained journals")
			}
			if !complete && registry.receiptCalls != 0 {
				t.Fatal("partial pair produced a receipt")
			}
		})
	}
}

func TestPrepareRecoversAfterProcessExit(t *testing.T) {
	for _, stop := range []string{"worker-prepared", "runtime-prepared", "origin-durable", "pair-durable"} {
		t.Run(stop, func(t *testing.T) {
			config, registry := bootstrapFixture(t)
			wireConfig := config
			wireConfig.Validator = nil
			wire, err := json.Marshal(wireConfig)
			mustDo(t, err)
			executable, err := os.Executable()
			mustDo(t, err)
			ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
			defer cancel()
			command := exec.CommandContext(ctx, executable, "-test.run=^TestBootstrapProcessExitHelper$")
			command.Env = append(os.Environ(), "VELA_BOOTSTRAP_EXIT_HELPER="+stop)
			command.Stdin = bytes.NewReader(wire)
			output, err := command.CombinedOutput()
			var exit *exec.ExitError
			if !errors.As(err, &exit) || exit.ExitCode() != 37 {
				t.Fatalf("helper did not exit at boundary: %v\n%s", err, output)
			}
			var operation operationRecord
			encoded, err := os.ReadFile(filepath.Join(config.ScratchDirectory, "bootstrap", operationName))
			mustDo(t, err)
			mustDo(t, json.Unmarshal(encoded, &operation))
			registry.claim.RequestID = operation.RequestID
			registry.claim.ClaimedAt = time.Now()
			before := snapshotFiles(t, config.ScratchDirectory)
			result, err := Prepare(t.Context(), config, registry)
			if stop == "pair-durable" {
				if err != nil || result.RequestID != operation.RequestID {
					t.Fatalf("recover complete pair after process death: %+v %v", result, err)
				}
			} else if !errors.Is(err, ErrIncomplete) || result != (Result{}) {
				t.Fatalf("process death resumed initialization: %+v %v", result, err)
			}
			if registry.claimCalls != 0 || !reflect.DeepEqual(before, snapshotFiles(t, config.ScratchDirectory)) {
				t.Fatal("process restart changed retained state or called Claim")
			}
		})
	}
}

func TestBootstrapProcessExitHelper(t *testing.T) {
	stop := os.Getenv("VELA_BOOTSTRAP_EXIT_HELPER")
	if stop == "" {
		return
	}
	var config Config
	mustDo(t, json.NewDecoder(os.Stdin).Decode(&config))
	config.Validator = testValidator(t)
	_, err := prepare(t.Context(), config, &fakeAuthority{}, func(phase string) error {
		if phase == stop {
			os.Exit(37)
		}
		return nil
	})
	t.Fatalf("process exit boundary was not reached: %v", err)
}

func TestPrepareRejectsLostClaimAndEntireLocalStateLoss(t *testing.T) {
	for _, loss := range []string{"claim-response", "all-local-state"} {
		t.Run(loss, func(t *testing.T) {
			config, registry := bootstrapFixture(t)
			registry.loseClaim = loss == "claim-response"
			_, firstErr := Prepare(t.Context(), config, registry)
			if loss == "claim-response" && firstErr == nil || loss == "all-local-state" && firstErr != nil {
				t.Fatalf("initial preparation: %v", firstErr)
			}
			registry.loseClaim = false
			if loss == "all-local-state" {
				if err := os.Rename(config.ScratchDirectory, config.ScratchDirectory+".retained"); err != nil {
					t.Fatal(err)
				}
				createRoots(t, config.ScratchDirectory)
			}
			if result, err := Prepare(t.Context(), config, registry); err == nil || result != (Result{}) {
				t.Fatalf("lost state acquired replacement initialization: %+v %v", result, err)
			}
			for _, name := range []string{"worker-admission", "runtime-admission", "inputs", "outputs"} {
				entries, err := os.ReadDir(filepath.Join(config.ScratchDirectory, name))
				if err != nil || len(entries) != 0 {
					t.Fatalf("lost-state retry initialized %s: %v", name, err)
				}
			}
		})
	}
}

func TestPreparePreflightsBeforeConsumingPermission(t *testing.T) {
	for _, fault := range []string{"launch-command", "launch-member", "launch-root", "node", "limit", "missing-root", "public-root", "symlink-root", "retained-input", "old-epoch", "nil-validator"} {
		t.Run(fault, func(t *testing.T) {
			config, registry := bootstrapFixture(t)
			switch fault {
			case "launch-command":
				config.Launch.Runtimes[0].Command = []string{"/different-backend"}
			case "launch-member":
				config.Launch.Members[0].DeviceSubsetDigest = strings.Repeat("f", 64)
			case "launch-root":
				config.Launch.Runtimes[0].InputRoot += "-other"
			case "node":
				config.NodeIdentity = "other-node"
			case "limit":
				config.MaxRecords = 65
			case "missing-root":
				mustDo(t, os.Remove(filepath.Join(config.ScratchDirectory, "inputs")))
			case "public-root":
				mustDo(t, os.Chmod(filepath.Join(config.ScratchDirectory, "inputs"), 0o755))
			case "symlink-root":
				path := filepath.Join(config.ScratchDirectory, "inputs")
				mustDo(t, os.Rename(path, path+".real"))
				mustDo(t, os.Symlink(path+".real", path))
			case "retained-input":
				mustDo(t, os.WriteFile(filepath.Join(config.ScratchDirectory, "inputs", "old"), []byte("history"), 0o600))
			case "old-epoch":
				mustDo(t, os.WriteFile(filepath.Join(config.ScratchDirectory, "epochs"), []byte("1"), 0o600))
			case "nil-validator":
				config.Validator = nil
			}
			if result, err := Prepare(t.Context(), config, registry); err == nil || result != (Result{}) || registry.claimCalls != 0 {
				t.Fatalf("invalid preflight consumed permission: %+v %v", result, err)
			}
			if _, err := os.Lstat(filepath.Join(config.ScratchDirectory, "bootstrap", operationName)); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("invalid preflight created an operation: %v", err)
			}
		})
	}
}

func TestPrepareConcurrentOperationAndLiveJournalLocks(t *testing.T) {
	config, registry := bootstrapFixture(t)
	entered, release := make(chan struct{}), make(chan struct{})
	done := make(chan error, 1)
	go func() {
		_, err := prepare(t.Context(), config, registry, func(phase string) error {
			if phase == "operation-durable" {
				close(entered)
				<-release
			}
			return nil
		})
		done <- err
	}()
	<-entered
	_, concurrentErr := Prepare(t.Context(), config, registry)
	close(release)
	firstErr := <-done
	if concurrentErr == nil || firstErr != nil || registry.claimCalls != 1 {
		t.Fatalf("concurrent first use: first=%v second=%v claims=%d", firstErr, concurrentErr, registry.claimCalls)
	}
	p, err := bind(config)
	mustDo(t, err)
	gate, err := stageworkeragent.NewFileAssignmentAdmission(p.worker)
	mustDo(t, err)
	t.Cleanup(func() { _ = gate.Close() })
	_, err = Prepare(t.Context(), config, registry)
	if err == nil || registry.receiptCalls != 1 {
		t.Fatal("preparation reported pair while Worker owned its journal")
	}
	mustDo(t, gate.Close())
	lock, err := os.OpenFile(filepath.Join(config.ScratchDirectory, "runtime-admission", "execution-admission.lock"), os.O_RDWR, 0)
	mustDo(t, err)
	t.Cleanup(func() { _ = lock.Close() })
	mustDo(t, syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB))
	_, err = Prepare(t.Context(), config, registry)
	if err == nil || registry.receiptCalls != 1 {
		t.Fatal("preparation reported pair while Runtime owned its journal")
	}
}

func TestPrepareRejectsRetainedStateTampering(t *testing.T) {
	for _, fault := range []string{"operation-missing", "operation-empty", "operation-unknown", "operation-hardlink", "pair-missing", "pair-id", "worker-missing", "runtime-replaced", "input-root-replaced", "limit-changed", "actor-changed"} {
		t.Run(fault, func(t *testing.T) {
			config, registry := bootstrapFixture(t)
			_, err := Prepare(t.Context(), config, registry)
			mustDo(t, err)
			op := filepath.Join(config.ScratchDirectory, "bootstrap", operationName)
			pair := filepath.Join(config.ScratchDirectory, "bootstrap", pairName)
			switch fault {
			case "operation-missing":
				mustDo(t, os.Remove(op))
			case "operation-empty":
				mustDo(t, os.WriteFile(op, nil, 0o600))
			case "operation-unknown":
				mustDo(t, os.WriteFile(op, []byte(`{"schema_version":1,"unknown":true}`), 0o600))
			case "operation-hardlink":
				mustDo(t, os.Link(op, op+".link"))
			case "pair-missing":
				mustDo(t, os.Remove(pair))
			case "pair-id":
				var record journalPair
				data, err := os.ReadFile(pair)
				mustDo(t, err)
				mustDo(t, json.Unmarshal(data, &record))
				record.RuntimeID = uuid.New()
				data, err = json.Marshal(record)
				mustDo(t, err)
				mustDo(t, os.WriteFile(pair, data, 0o600))
			case "worker-missing":
				mustDo(t, os.Remove(filepath.Join(config.ScratchDirectory, "worker-admission", "assignment-admission.json")))
			case "runtime-replaced":
				p, err := bind(config)
				mustDo(t, err)
				dir := filepath.Join(config.ScratchDirectory, "runtime-admission")
				for _, name := range []string{"execution-admission.json", "execution-admission.lock"} {
					mustDo(t, os.Rename(filepath.Join(dir, name), filepath.Join(config.ScratchDirectory, name+".retained")))
				}
				_, err = modelruntime.PrepareExecutionJournal(t.Context(), p.launch, config.Validator, modelruntime.ExecutionFloorStateConfig{Directory: dir, Initialize: true})
				mustDo(t, err)
			case "input-root-replaced":
				path := filepath.Join(config.ScratchDirectory, "inputs")
				mustDo(t, os.Rename(path, path+".retained"))
				mustDo(t, os.Mkdir(path, 0o700))
			case "limit-changed":
				config.MaxRecords++
			case "actor-changed":
				config.ActorIdentity = "node/other"
			}
			if result, err := Prepare(t.Context(), config, registry); err == nil || result != (Result{}) || registry.claimCalls != 1 || registry.receiptCalls != 1 {
				t.Fatalf("tampered recovery accepted or repeated authority: %+v %v", result, err)
			}
		})
	}
}

func TestPrepareDetectsReplacementDuringClaim(t *testing.T) {
	config, registry := bootstrapFixture(t)
	_, err := prepare(t.Context(), config, registry, func(phase string) error {
		if phase == "claim-committed" {
			path := filepath.Join(config.ScratchDirectory, "worker-admission")
			mustDo(t, os.Rename(path, path+".retained"))
			mustDo(t, os.Mkdir(path, 0o700))
		}
		return nil
	})
	if err == nil || registry.claimCalls != 1 || registry.receiptCalls != 0 {
		t.Fatalf("changed roots during claim accepted: %v", err)
	}
	for _, name := range []string{"worker-admission", "runtime-admission"} {
		entries, err := os.ReadDir(filepath.Join(config.ScratchDirectory, name))
		if err != nil || len(entries) != 0 {
			t.Fatalf("changed root initialized: %s %v", name, err)
		}
	}
}

func TestPrepareRequiresFreshExactClaimResponse(t *testing.T) {
	for _, fault := range []string{"replay", "request", "worker", "epoch", "member", "member-epoch", "node", "digest", "timestamp"} {
		t.Run(fault, func(t *testing.T) {
			config, registry := bootstrapFixture(t)
			registry.badClaim = fault
			if result, err := Prepare(t.Context(), config, registry); !errors.Is(err, ErrIncomplete) || result != (Result{}) || registry.receiptCalls != 0 {
				t.Fatalf("invalid grant prepared journals: %+v %v", result, err)
			}
			for _, name := range []string{"worker-admission", "runtime-admission", "inputs", "outputs"} {
				entries, err := os.ReadDir(filepath.Join(config.ScratchDirectory, name))
				if err != nil || len(entries) != 0 {
					t.Fatalf("invalid grant initialized %s: %v", name, err)
				}
			}
		})
	}
}

type fakeAuthority struct {
	mu                       sync.Mutex
	claim                    fleet.WorkerBootstrapClaim
	receipt                  fleet.WorkerBootstrapReceipt
	claimCalls, receiptCalls int
	loseClaim, loseReceipt   bool
	badClaim                 string
}

func (a *fakeAuthority) ClaimWorkerBootstrap(_ context.Context, request fleet.WorkerBootstrapRequest) (fleet.WorkerBootstrapClaim, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.claimCalls++
	if a.claim.RequestID != uuid.Nil {
		if a.claim.RequestID != request.RequestID {
			return fleet.WorkerBootstrapClaim{}, errors.New("member already claimed")
		}
		result := a.claim
		result.Fresh = false
		return result, nil
	}
	digest := sha256.Sum256(request.BundleManifest)
	a.claim = fleet.WorkerBootstrapClaim{RequestID: request.RequestID, Fresh: true,
		WorkerInstanceID: request.WorkerInstanceID, WorkerInstanceEpoch: request.WorkerInstanceEpoch,
		WorkerMemberID: request.WorkerMemberID, WorkerMemberEpoch: 1, NodeIdentity: "cpu-node-1", BundleDigest: digest[:], ClaimedAt: time.Now()}
	if a.loseClaim {
		return fleet.WorkerBootstrapClaim{}, errors.New("claim response lost")
	}
	result := a.claim
	switch a.badClaim {
	case "replay":
		result.Fresh = false
	case "request":
		result.RequestID = uuid.New()
	case "worker":
		result.WorkerInstanceID = uuid.New()
	case "epoch":
		result.WorkerInstanceEpoch++
	case "member":
		result.WorkerMemberID = uuid.New()
	case "member-epoch":
		result.WorkerMemberEpoch++
	case "node":
		result.NodeIdentity = "other-node"
	case "digest":
		result.BundleDigest = bytes.Repeat([]byte{0xff}, 32)
	case "timestamp":
		result.ClaimedAt = time.Time{}
	}
	return result, nil
}

func (a *fakeAuthority) RecordWorkerBootstrapReceipt(_ context.Context, receipt fleet.WorkerBootstrapReceipt) (time.Time, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.receiptCalls++
	if receipt.RequestID != a.claim.RequestID || a.receipt.RequestID != uuid.Nil && !reflect.DeepEqual(receipt, a.receipt) {
		return time.Time{}, errors.New("receipt ownership changed")
	}
	a.receipt = receipt
	if a.loseReceipt {
		return time.Time{}, errors.New("receipt response lost")
	}
	return a.claim.ClaimedAt, nil
}

func bootstrapFixture(t *testing.T) (Config, *fakeAuthority) {
	t.Helper()
	workerID, memberID, poolID := uuid.New(), uuid.New(), uuid.New()
	memberDigest := sha256.Sum256([]byte("spiffe://vela.internal/stage-worker/" + memberID.String()))
	image := "registry.example/vela@sha256:" + strings.Repeat("a", 64)
	bundle := fleetcontroller.WorkerBundleActuation{SchemaVersion: 2, PlanRevisionID: uuid.New(), WorkerBundleID: uuid.New(), Namespace: "vela",
		InitImage: image, StageWorkerAgentImage: image, RuntimeImage: image, StageWorkerConfigMap: "worker-config",
		ModelRuntimeVerifierConfigMap: "verifier", StageWorkerControlTLSSecret: "control-tls", StageWorkerAuthoritySecret: "authority",
		ArtifactStoreCredentialsSecret: "artifact", ArtifactStoreCASecret: "artifact-ca",
		WorkerInstances: []fleetcontroller.WorkerInstanceActuation{{ID: workerID, InstanceEpoch: 1, WorkerProfileRevisionID: uuid.New(),
			CapacityPoolID: poolID, Role: "cpu-thumbnail", CapacitySlots: 1, DeviceSetDigest: strings.Repeat("b", 64), MembershipDigest: strings.Repeat("c", 64),
			ModelRuntimes: []fleetcontroller.ModelRuntimeProcess{{ModelResidencyID: uuid.New(), CapacityPoolID: poolID, StageProfileRevisionID: uuid.New(),
				ModelRuntimeEpochFloor: 1, Component: "CPU_MEDIA", ModelComponentRevision: "cpu-mock-v1", RuntimeIdentity: "cpu-runtime",
				Command: []string{"/nonexistent-mock-backend"}, InitializationTimeout: "1s", ShutdownTimeout: "1s"}},
			Members: []fleetcontroller.WorkerMemberActuation{{ID: memberID, MemberEpoch: 1, Key: "member-0", NodeIdentity: "cpu-node-1",
				ResourceClass: "CPU", DeviceCount: 1, IdentityDigest: hex.EncodeToString(memberDigest[:]), DeviceSubsetDigest: strings.Repeat("d", 64),
				DeviceConstraints: []fleetcontroller.DeviceConstraint{{DeviceID: uuid.New(), DeviceEpoch: 1, ResourceClass: "CPU"}}}},
		}}}
	var err error
	bundle.RevisionDigest, err = fleetcontroller.ComputeWorkerBundleActuationDigest(bundle)
	mustDo(t, err)
	launch, err := fleetcontroller.WorkerMemberLaunchManifest(bundle, workerID, memberID)
	mustDo(t, err)
	root, err := filepath.EvalSymlinks(t.TempDir())
	mustDo(t, err)
	root = filepath.Join(root, "scratch")
	createRoots(t, root)
	return Config{Bundle: bundle, Launch: launch, NodeIdentity: "cpu-node-1", ActorIdentity: "node/bootstrap-test", ScratchDirectory: root,
		MaxRecords: 4, Validator: testValidator(t)}, &fakeAuthority{}
}

func testValidator(t *testing.T) *stageauthority.Validator {
	t.Helper()
	keyring, err := stageauthority.DeriveVerifierKeyring(map[string][]byte{"test-key": bytes.Repeat([]byte{1}, 32)})
	mustDo(t, err)
	validator, err := stageauthority.NewVerifier(keyring, nil)
	mustDo(t, err)
	t.Cleanup(func() { stageauthority.ClearKeyring(keyring) })
	return validator
}

func createRoots(t *testing.T, root string) {
	t.Helper()
	mustDo(t, os.Mkdir(root, 0o700))
	for _, name := range rootNames[1:] {
		mustDo(t, os.Mkdir(filepath.Join(root, name), 0o700))
	}
}

func snapshotFiles(t *testing.T, root string) map[string]string {
	t.Helper()
	result := make(map[string]string)
	mustDo(t, filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil || entry.IsDir() {
			return err
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		result[path] = fmt.Sprintf("%v:%s", identity(info), data)
		return nil
	}))
	return result
}

func mustDo(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}
