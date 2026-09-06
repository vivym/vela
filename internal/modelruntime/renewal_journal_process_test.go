package modelruntime_test

import (
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/vivym/vela/internal/modelruntime"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/protobuf/proto"
)

func TestRuntimeRenewalJournalSurvivesAbruptProcessExit(t *testing.T) {
	for _, phase := range []string{"dispatch", "confirmation"} {
		t.Run(phase, func(t *testing.T) {
			directory := privateExecutionStateDirectory(t)
			command := exec.CommandContext(t.Context(), os.Args[0], "-test.run=^TestRuntimeRenewalJournalProcessHelper$")
			command.Env = append(os.Environ(), "VELA_TEST_RENEWAL_JOURNAL_DIRECTORY="+directory, "VELA_TEST_RENEWAL_JOURNAL_EXIT="+phase)
			output, err := command.CombinedOutput()
			var exit *exec.ExitError
			if !errors.As(err, &exit) || exit.ExitCode() != 71 {
				t.Fatalf("process did not exit at durable %s boundary: %s %v", phase, output, err)
			}
			state := readDurableExecutionState(t, directory)
			var records []retainedExecutionDocument
			if err := json.Unmarshal(state.Executions, &records); err != nil || len(records) != 1 || records[0].Candidates == nil {
				t.Fatalf("process exit lost pending renewal: records=%d error=%v", len(records), err)
			}
			original, accepted := &velav1.StageAuthority{}, &velav1.StageAuthority{}
			if err := proto.Unmarshal(records[0].Authority, original); err != nil {
				t.Fatal(err)
			}
			if err := proto.Unmarshal(records[0].Candidates.Accepted, accepted); err != nil {
				t.Fatal(err)
			}
			if proto.Equal(original, accepted) {
				t.Fatal("child did not persist a distinct renewal")
			}
			confirmed := original
			if phase == "confirmation" {
				confirmed = accepted
			}
			assertDurableRenewalCandidates(t, directory, original, accepted, confirmed)
			recovered := durableExecutionFixture(t, directory, false, "", 10, accepted.ExpiresAt.AsTime().Add(time.Minute))
			read, err := recovered.supervisor.InspectRetainedAllocationAuthorities(t.Context(), accepted)
			if err != nil || read == nil || !proto.Equal(read.Accepted, accepted) || !proto.Equal(read.Confirmed, confirmed) {
				t.Fatalf("restart after process exit lost authority candidates: %+v %v", read, err)
			}
			assertRecoveryDrainBlocks(t, recovered, recovered.authority(t, 1, 12))
		})
	}
}

func TestRuntimeRenewalJournalProcessHelper(t *testing.T) {
	directory := os.Getenv("VELA_TEST_RENEWAL_JOURNAL_DIRECTORY")
	if directory == "" {
		return
	}
	backend := &renewalRecoveryBackend{FakeRuntime: modelruntime.NewFakeDiTRuntime(), calls: make(chan cancellationAuthorityCall, 2)}
	f := newExecutionDrainFixture(t, directory, backend)
	prepareFloorRuntime(t, f.supervisor, f.authorities[0])
	f.clock.Advance(time.Second)
	latest := renewWatchdogAuthority(t, f.signer, f.authorities[0], f.clock.Now())
	target := 1
	if os.Getenv("VELA_TEST_RENEWAL_JOURNAL_EXIT") == "confirmation" {
		target = 2
	}
	writes := 0
	modelruntime.SetExecutionStateSyncHookForTest(f.supervisor, func(syncDirectory func() error) error {
		if err := syncDirectory(); err != nil {
			return err
		}
		writes++
		if writes == target {
			os.Exit(71)
		}
		return nil
	})
	_, _ = f.supervisor.Status(t.Context(), &velav1.ModelRuntimeServiceStatusRequest{Authority: latest})
	t.Fatal("process missed its durable renewal boundary")
}
