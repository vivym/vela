package h3stagemock

import (
	"strings"
	"sync"
	"testing"
	"time"
)

func TestInspectionSnapshotIsUnknownDuringCommandAndNeverInstallsIdentity(t *testing.T) {
	const profile = "49000000-0000-0000-0000-000000000005"
	digest := strings.Repeat("a", 64)
	identity := internalStageIdentity("1", digest, profile)
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	runtime := &session{
		component: "ENCODER", mode: ModeFailure,
		initialization: &initializeV1{StageProfileRevisionID: profile},
		active:         &execution{identity: identity, state: statePrepared, sequence: 1},
		now: func() time.Time {
			close(entered)
			<-release
			return time.Date(2026, 9, 6, 0, 0, 0, 0, time.UTC)
		},
	}
	runtime.publishInspection()
	if observation := runtime.inspect(digest); !observation.Known || observation.State != "PREPARED" {
		t.Fatalf("prepared snapshot missing: %+v", observation)
	}
	for range 100 {
		if observation := runtime.inspect(strings.Repeat("b", 64)); observation.Known {
			t.Fatalf("inspection accepted unseen identity: %+v", observation)
		}
	}
	if runtime.active.identity != identity || len(runtime.retiredAuthorities) != 0 {
		t.Fatal("inspection changed current or retired identity")
	}
	completed := make(chan responseV1, 1)
	go func() {
		result, _ := runtime.handle(requestV1{Operation: operationStart, Stage: &stageRequestV1{Identity: identity}})
		completed <- result
	}()
	t.Cleanup(func() { once.Do(func() { close(release) }) })
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("command did not reach blocked backend work")
	}
	for range 1000 {
		if observation := runtime.inspect(digest); observation.Known || observation.State != "" || observation.Sequence != 0 {
			t.Fatalf("in-flight command retained a stale snapshot: %+v", observation)
		}
	}
	once.Do(func() { close(release) })
	select {
	case result := <-completed:
		if !result.Acknowledged || result.Error != "" {
			t.Fatalf("start failed: %+v", result)
		}
	case <-time.After(time.Second):
		t.Fatal("command did not finish")
	}
	if observation := runtime.inspect(digest); !observation.Known || observation.State != "FAILED" || observation.Sequence != 3 {
		t.Fatalf("completed snapshot missing: %+v", observation)
	}
}
