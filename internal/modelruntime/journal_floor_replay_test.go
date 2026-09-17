package modelruntime_test

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/vivym/vela/internal/modelruntime"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Terminal recovery re-queries Control after a lost reply or incomplete drain.
// Control re-signs the same restriction with fresh query timestamps. This must
// pass the Worker -> Node journal seam before the Runtime RPC can resume drain.
func TestJournalOwnerFloorRefreshedRecovery(t *testing.T) {
	for _, restart := range []bool{false, true} {
		name := "live"
		if restart {
			name = "reopened"
		}
		t.Run(name, func(t *testing.T) {
			f, owner, config := journalOwnerFixture(t)
			d := unsignedTerminalAllocation(t, f)
			command := modelruntime.JournalCommand{Floor: &modelruntime.JournalFloorCommand{Disposition: journalProto(t, d)}}
			first := applyOwnerCommand(t, owner, modelruntime.JournalWorkerRole, command)
			path := filepath.Join(config.State.Directory, durableStateFileName)
			before, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			info, err := os.Stat(path)
			if err != nil {
				t.Fatal(err)
			}
			if restart {
				if err := owner.Close(); err != nil {
					t.Fatal(err)
				}
				var err error
				owner, err = modelruntime.OpenExecutionJournalOwner(config)
				if err != nil {
					t.Fatal(err)
				}
				defer owner.Close()
			}
			f.clock.Advance(time.Second)
			refreshed := proto.Clone(d).(*velav1.StageTerminalDisposition)
			refreshed.ExpiresAt = timestamppb.New(f.clock.Now().Add(time.Minute))
			refreshed, err = f.signer.SignTerminalDisposition(refreshed)
			if err != nil {
				t.Fatal(err)
			}
			if bytes.Equal(journalProto(t, d), journalProto(t, refreshed)) {
				t.Fatal("fixture did not refresh signed history")
			}
			command.Floor.Disposition = journalProto(t, refreshed)
			again := applyOwnerCommand(t, owner, modelruntime.JournalWorkerRole, command)
			after, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			afterInfo, err := os.Stat(path)
			if err != nil {
				t.Fatal(err)
			}
			if !again.Replayed || again.Floor != first.Floor || again.StateDigest != first.StateDigest || !bytes.Equal(before, after) || !os.SameFile(info, afterInfo) {
				t.Fatalf("refreshed floor rewrote or lowered restriction: %+v %+v", first, again)
			}
			for _, fault := range []string{"signature", "scope", "expired"} {
				bad := proto.Clone(refreshed).(*velav1.StageTerminalDisposition)
				switch fault {
				case "signature":
					bad.Signature[0] ^= 1
				case "scope":
					bad.WorkerInstanceEpoch++
					bad, err = f.signer.SignTerminalDisposition(bad)
				case "expired":
					bad.ExpiresAt = timestamppb.New(f.clock.Now().Add(90 * time.Second))
					bad, err = f.signer.SignTerminalDisposition(bad)
					f.clock.Advance(time.Hour)
				}
				if err != nil {
					t.Fatal(err)
				}
				wire, err := modelruntime.EncodeJournalCommand(modelruntime.JournalCommand{SchemaVersion: 1, Floor: &modelruntime.JournalFloorCommand{Disposition: journalProto(t, bad)}})
				if err != nil {
					t.Fatal(err)
				}
				if _, err := owner.Apply(t.Context(), modelruntime.JournalWorkerRole, wire); err == nil {
					t.Fatalf("accepted %s refreshed restriction", fault)
				}
			}
		})
	}
}

func TestJournalWorkerFloorRefreshedAndLowerHistory(t *testing.T) {
	f, owner, transport, _ := remoteSupervisorFixture(t)
	client, _ := serveRuntimeServer(t, f.supervisor)
	worker := &directJournalTransport{owner: owner, identity: transport.identity, role: modelruntime.JournalWorkerRole}
	wrapped, err := modelruntime.NewJournalWorkerClient(client, worker)
	if err != nil {
		t.Fatal(err)
	}
	d := unsignedTerminalAllocation(t, f)
	request := &velav1.ModelRuntimeServiceInstallStageExecutionFloorRequest{SchemaVersion: 1, Identity: inspectionIdentity(f.authorities[0]), Disposition: d}
	first, err := wrapped.InstallStageExecutionFloor(t.Context(), request)
	if err != nil || !first.GetDurable() {
		t.Fatalf("initial floor: %v %v", first, err)
	}
	f.clock.Advance(time.Second)
	refreshed := proto.Clone(d).(*velav1.StageTerminalDisposition)
	refreshed.ExpiresAt = timestamppb.New(f.clock.Now().Add(time.Minute))
	refreshed, err = f.signer.SignTerminalDisposition(refreshed)
	if err != nil {
		t.Fatal(err)
	}
	request.Disposition = refreshed
	again, err := wrapped.InstallStageExecutionFloor(t.Context(), request)
	if err != nil || !again.GetDurable() || again.GetInstalledCutoff() != first.GetInstalledCutoff() || bytes.Equal(again.GetDispositionDigest(), first.GetDispositionDigest()) {
		t.Fatalf("refreshed floor did not reach Runtime: %v %v", again, err)
	}
	high := proto.Clone(refreshed).(*velav1.StageTerminalDisposition)
	high.Cutoff++
	high.Allocations[len(high.Allocations)-1].ExecutionSequence++
	high, err = f.signer.SignTerminalDisposition(high)
	if err != nil {
		t.Fatal(err)
	}
	request.Disposition = high
	advanced, err := wrapped.InstallStageExecutionFloor(t.Context(), request)
	if err != nil || !advanced.GetDurable() || advanced.GetInstalledCutoff() != high.Cutoff {
		t.Fatalf("advance floor: %v %v", advanced, err)
	}
	request.Disposition = refreshed
	lower, err := wrapped.InstallStageExecutionFloor(t.Context(), request)
	if err != nil || !lower.GetDurable() || lower.GetInstalledCutoff() != high.Cutoff {
		t.Fatalf("older history lowered restriction or blocked recovery: %v %v", lower, err)
	}
	assertFloorCommandsRejected(t, f.supervisor, f.authorities[0])
	if f.backend.calls.Load() != 0 {
		t.Fatal("floor replay entered backend")
	}
}
