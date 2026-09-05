package h3stagemock

import (
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/vivym/vela/internal/driverdrain"
)

func TestDrainJoinsCommandWorkAndFreezesExactTerminalExecution(t *testing.T) {
	const profile = "49000000-0000-0000-0000-000000000005"
	identity := internalStageIdentity("1", strings.Repeat("a", 64), profile)
	identity.ExecutionSequence = 7
	query := driverdrain.Identity{AuthorityDigest: identity.AuthorityDigest, ExecutionSequence: 7}
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	runtime := &session{component: "ENCODER", mode: ModeFailure,
		initialization:  &initializeV1{StageProfileRevisionID: profile},
		active:          &execution{identity: identity, state: statePrepared, sequence: 1},
		highestSequence: 7,
		now:             func() time.Time { close(entered); <-release; return time.Now() },
	}
	for _, state := range []executionState{statePrepared, stateRunning, stateOutputReady} {
		runtime.active.state = state
		if runtime.drainExecution(query) {
			t.Fatalf("drained nonterminal %s", state)
		}
	}
	runtime.active.state = statePrepared
	done := make(chan responseV1, 1)
	go func() {
		response, _ := runtime.handle(requestV1{Operation: operationStart, Stage: &stageRequestV1{Identity: identity}})
		done <- response
	}()
	t.Cleanup(func() { once.Do(func() { close(release) }) })
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("command did not enter")
	}
	for range 100 {
		if runtime.drainExecution(query) {
			t.Fatal("in-flight command was drained")
		}
	}
	once.Do(func() { close(release) })
	if response := <-done; response.Error != "" {
		t.Fatal(response.Error)
	}
	for _, wrong := range []driverdrain.Identity{
		{AuthorityDigest: strings.Repeat("b", 64), ExecutionSequence: 7},
		{AuthorityDigest: query.AuthorityDigest, ExecutionSequence: 8},
		{AuthorityDigest: query.AuthorityDigest},
	} {
		if runtime.drainExecution(wrong) || runtime.active.drained {
			t.Fatal("mismatched query froze execution")
		}
	}
	for range 3 {
		if !runtime.drainExecution(query) {
			t.Fatal("exact terminal drain failed")
		}
	}
	for _, request := range []requestV1{
		{Operation: operationPrepare, Prepare: &prepareRequestV1{Identity: identity}},
		{Operation: operationStart, Stage: &stageRequestV1{Identity: identity}},
		{Operation: operationCancel, Cancel: &cancelRequestV1{Identity: identity, Reason: "MODEL_RUNTIME_CANCEL_REASON_CONTROL_PLANE_STOP"}},
		{Operation: operationSeal, Stage: &stageRequestV1{Identity: identity}},
	} {
		if response, _ := runtime.handle(request); response.Error == "" {
			t.Fatalf("drained failure accepted %s", request.Operation)
		}
	}
	if runtime.active.state != stateFailed || runtime.active.identity != identity || runtime.active.sequence != 3 {
		t.Fatal("frozen execution changed")
	}
	status, _ := runtime.handle(requestV1{Operation: operationStatus, Stage: &stageRequestV1{Identity: identity}})
	if status.Error != "" || status.Status.State != stateFailed {
		t.Fatalf("status changed drained execution: %+v", status)
	}
	renewed := identity
	renewed.AuthorityDigest, renewed.StageVersion = strings.Repeat("b", 64), identity.StageVersion+1
	if _, err := runtime.requireActive(renewed); err == nil {
		t.Fatal("renewal replaced drained identity")
	}
	runtime.active = nil
	if err := runtime.prepare(&prepareRequestV1{Identity: renewed}); err == nil {
		t.Fatal("old sequence reentered after slot replacement")
	}
}

func TestDrainFreezesNamespaceAgainstShutdownCleanup(t *testing.T) {
	for _, state := range []executionState{stateStopped, stateFailed, stateOutputSealed} {
		t.Run(string(state), func(t *testing.T) {
			identity := internalStageIdentity("1", strings.Repeat("a", 64), "49000000-0000-0000-0000-000000000005")
			identity.ExecutionSequence = 7
			rootPath := t.TempDir()
			path := filepath.Join(identity.StageAttemptID, "conditioning.bin")
			if err := os.Mkdir(filepath.Dir(filepath.Join(rootPath, path)), 0o700); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(rootPath, path), []byte("retained"), 0o600); err != nil {
				t.Fatal(err)
			}
			root, err := os.OpenRoot(rootPath)
			if err != nil {
				t.Fatal(err)
			}
			runtime := &session{component: "ENCODER", initialization: &initializeV1{}, outputRoot: root,
				active: &execution{identity: identity, state: state, outputPath: path}}
			t.Cleanup(func() { _ = runtime.close() })
			if !runtime.drainExecution(driverdrain.Identity{AuthorityDigest: identity.AuthorityDigest, ExecutionSequence: 7}) {
				t.Fatal("drain failed")
			}
			if err := runtime.close(); err != nil {
				t.Fatal(err)
			}
			if content, err := os.ReadFile(filepath.Join(rootPath, path)); err != nil || string(content) != "retained" {
				t.Fatalf("shutdown mutated drained namespace: %q %v", content, err)
			}
		})
	}
}
