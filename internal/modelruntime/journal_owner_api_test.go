package modelruntime_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/modelruntime"
	"github.com/vivym/vela/internal/stageauthority"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func journalOwnerFixture(t *testing.T) (*executionFloorFixture, *modelruntime.ExecutionJournalOwner, modelruntime.ExecutionJournalOwnerConfig) {
	t.Helper()
	f, directory := transitionFixture(t)
	manifest := recoveredRuntimeServerConfig(t, f, directory).Manifest
	if err := f.supervisor.Shutdown(); err != nil {
		t.Fatal(err)
	}
	config := modelruntime.ExecutionJournalOwnerConfig{Manifest: manifest, Validator: f.validator,
		State: modelruntime.ExecutionFloorStateConfig{Directory: directory}, Routes: f.bindings,
		MaxClockSkew: 30 * time.Second, Now: f.clock.Now}
	owner, err := modelruntime.OpenExecutionJournalOwner(config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = owner.Close() })
	return f, owner, config
}

func journalProto(t *testing.T, value proto.Message) []byte {
	t.Helper()
	wire, err := proto.MarshalOptions{Deterministic: true}.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return wire
}

func applyOwnerCommand(t *testing.T, owner *modelruntime.ExecutionJournalOwner, role modelruntime.JournalCallerRole, command modelruntime.JournalCommand) modelruntime.JournalMutationReceipt {
	t.Helper()
	command.SchemaVersion = 1
	wire, err := modelruntime.EncodeJournalCommand(command)
	if err != nil {
		t.Fatal(err)
	}
	receipt, err := owner.Apply(t.Context(), role, wire)
	if err != nil || receipt.SchemaVersion != 1 || receipt.RequestDigest != sha256.Sum256(wire) || receipt.JournalID == uuid.Nil || receipt.StateDigest == ([32]byte{}) {
		t.Fatalf("command receipt: %+v %v", receipt, err)
	}
	return receipt
}

func TestJournalOwnerAPIWorkflowAndLostReplyReplay(t *testing.T) {
	f, owner, config := journalOwnerFixture(t)
	if _, err := owner.RecordBackendStartupIntent(t.Context()); err != nil {
		t.Fatal(err)
	}
	authority := journalProto(t, f.authorities[0])
	verified, err := f.validator.ValidateEnvelope(f.authorities[0])
	if err != nil {
		t.Fatal(err)
	}
	manifest := []byte(`{"output":"exact-version"}`)
	digest := sha256.Sum256(manifest)
	seal := &velav1.LocalMaterializationReceipt{OutputManifestJson: manifest, ManifestSha256: digest[:],
		ReceiptId: uuid.NewSHA1(uuid.NameSpaceOID, append([]byte(f.authorities[0].GetStageLeaseId()+"\x00"), digest[:]...)).String(),
		SealedAt:  timestamppb.New(f.clock.Now()), TotalSizeBytes: 1}
	commands := []modelruntime.JournalCommand{
		{Admit: &modelruntime.JournalAuthorityCommand{Authority: authority}},
		{Candidates: &modelruntime.JournalCandidatesCommand{Authority: authority, Confirmed: authority}},
		{Seal: &modelruntime.JournalSealCommand{Authority: authority, Receipt: journalProto(t, seal)}},
		{Health: &modelruntime.JournalHealthCommand{Authority: authority, Evidence: workerHealthEvidence(f, true)}},
		{Drain: &modelruntime.JournalDrainCommand{Authority: authority, Drain: modelruntime.BackendDrain{
			Contract: modelruntime.ExecutionDrainContract, AuthorityDigest: verified.Digest, ExecutionSequence: f.authorities[0].ExecutionSequence}}},
	}
	for i, command := range commands {
		first := applyOwnerCommand(t, owner, modelruntime.JournalRuntimeRole, command)
		path := filepath.Join(config.State.Directory, durableStateFileName)
		before, err := os.Stat(path)
		if err != nil {
			t.Fatal(err)
		}
		// The durable operation may complete even when its reply is lost.
		again := applyOwnerCommand(t, owner, modelruntime.JournalRuntimeRole, command)
		after, err := os.Stat(path)
		if err != nil || !again.Replayed || again.StateDigest != first.StateDigest || !os.SameFile(before, after) {
			t.Fatalf("operation %d replay rewrote history: %+v %v", i, again, err)
		}
	}
	if f.backend.calls.Load() != 0 {
		t.Fatal("journal metadata entered backend")
	}
	if err := owner.Close(); err != nil {
		t.Fatal(err)
	}
	recovered, err := modelruntime.OpenExecutionJournalOwner(config)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = recovered.Close() }()
	if _, err := recovered.RecordBackendStartupIntent(t.Context()); !errors.Is(err, modelruntime.ErrBackendIncarnationUnproven) {
		t.Fatalf("unresolved startup became a restart grant: %v", err)
	}
	f.clock.Advance(time.Hour)
	if receipt := applyOwnerCommand(t, recovered, modelruntime.JournalRuntimeRole, commands[0]); !receipt.Replayed {
		t.Fatal("expired exact history was not acknowledged as replay")
	}
}

func TestJournalOwnerRetiresExactBackendIncarnationBeforeRestart(t *testing.T) {
	_, owner, config := journalOwnerFixture(t)
	startup, err := owner.RecordBackendStartupIntent(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	exitDigest := sha256.Sum256([]byte("retained kernel exit observation"))
	proof := modelruntime.BackendRetirementProof{IncarnationID: startup.IncarnationID, LaunchDigest: startup.LaunchDigest, ExitDigest: exitDigest, RetiredAt: startup.RecordedAt.Add(time.Second)}
	retired, err := owner.RetireBackendIncarnation(t.Context(), proof)
	if err != nil || retired.State != modelruntime.BackendLifecycleRetired {
		t.Fatalf("retire backend incarnation: %+v %v", retired, err)
	}
	if _, err := owner.RetireBackendIncarnation(t.Context(), proof); err == nil {
		t.Fatal("duplicate retirement unexpectedly succeeded")
	}
	if err := owner.Close(); err != nil {
		t.Fatal(err)
	}
	config.State.Initialize = false
	reopened, err := modelruntime.OpenExecutionJournalOwner(config)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = reopened.Close() }()
	second, err := reopened.RecordBackendStartupIntent(t.Context())
	if err != nil || second.State != modelruntime.BackendLifecycleUnresolved || second.IncarnationID == startup.IncarnationID {
		t.Fatalf("retired journal did not permit a fresh intent: %+v %v", second, err)
	}
}

func TestJournalOwnerAPIWorkerCheckpoints(t *testing.T) {
	f, owner, _ := journalOwnerFixture(t)
	disposition := unsignedTerminalAllocation(t, f)
	wire := journalProto(t, disposition)
	commands := []modelruntime.JournalCommand{
		{Floor: &modelruntime.JournalFloorCommand{Disposition: wire}},
		{NonAdmission: &modelruntime.JournalAuthorityCommand{Authority: journalProto(t, f.authorities[0])}},
		{TerminalNonAdmission: &modelruntime.JournalTerminalNonAdmissionCommand{Disposition: wire, Allocation: disposition.Allocations[1].StageAllocationId}},
	}
	for _, command := range commands {
		first := applyOwnerCommand(t, owner, modelruntime.JournalWorkerRole, command)
		again := applyOwnerCommand(t, owner, modelruntime.JournalWorkerRole, command)
		if first.Replayed || !again.Replayed || again.StateDigest != first.StateDigest || again.Floor != 11 {
			t.Fatalf("checkpoint/replay changed: %+v %+v", first, again)
		}
	}
	f.clock.Advance(time.Hour)
	applyOwnerCommand(t, owner, modelruntime.JournalWorkerRole, commands[0])
	applyOwnerCommand(t, owner, modelruntime.JournalWorkerRole, commands[1])
	commands[2].SchemaVersion = 1
	expired, _ := modelruntime.EncodeJournalCommand(commands[2])
	if _, err := owner.Apply(t.Context(), modelruntime.JournalWorkerRole, expired); !errors.Is(err, stageauthority.ErrStale) {
		t.Fatalf("terminal checkpoint accepted expired disposition: %v", err)
	}
}

func TestJournalOwnerAPIRejectsWithoutPoisoning(t *testing.T) {
	for _, fault := range []string{"role", "signature", "scope", "epoch", "expired", "protobuf", "no-startup", "cancel"} {
		t.Run(fault, func(t *testing.T) {
			f, owner, config := journalOwnerFixture(t)
			if fault != "no-startup" {
				if _, err := owner.RecordBackendStartupIntent(t.Context()); err != nil {
					t.Fatal(err)
				}
			}
			authority := proto.Clone(f.authorities[0]).(*velav1.StageAuthority)
			role := modelruntime.JournalRuntimeRole
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			switch fault {
			case "role":
				role = modelruntime.JournalWorkerRole
			case "signature":
				authority.Signature[0] ^= 1
			case "scope":
				authority.WorkerInstanceEpoch++
			case "epoch":
				authority.Members[0].ModelRuntimeEpoch++
			case "expired":
				f.clock.Advance(time.Hour)
			case "cancel":
				cancel()
			}
			if fault == "scope" || fault == "epoch" {
				var err error
				authority, err = f.signer.Sign(authority)
				if err != nil {
					t.Fatal(err)
				}
			}
			wire := journalProto(t, authority)
			if fault == "protobuf" {
				wire = append(wire, 0)
			}
			command, _ := modelruntime.EncodeJournalCommand(modelruntime.JournalCommand{SchemaVersion: 1, Admit: &modelruntime.JournalAuthorityCommand{Authority: wire}})
			path := filepath.Join(config.State.Directory, durableStateFileName)
			before, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := owner.Apply(ctx, role, command); err == nil {
				t.Fatal("invalid admission accepted")
			}
			after, err := os.ReadFile(path)
			if err != nil || !bytes.Equal(before, after) {
				t.Fatalf("rejected request published state: %v", err)
			}
			if fault == "no-startup" {
				if _, err := owner.RecordBackendStartupIntent(t.Context()); err != nil {
					t.Fatal(err)
				}
			}
			applyOwnerCommand(t, owner, modelruntime.JournalRuntimeRole, modelruntime.JournalCommand{
				Admit: &modelruntime.JournalAuthorityCommand{Authority: journalProto(t, f.authority(t, 0, 12))}})
		})
	}
}

func TestJournalCommandRejectsWireAuthorityInjection(t *testing.T) {
	for _, wire := range []string{
		`{"schema_version":1}`, `{"schema_version":2,"admit":{"authority":"AQ=="}}`,
		`{"schema_version":1,"admit":{"authority":"AQ=="},"floor":{"disposition":"AQ=="}}`,
		`{"schema_version":1,"schema_version":1,"admit":{"authority":"AQ=="}}`,
		`{"schema_version":1,"admit":{"authority":"AQ==","authority":"AQ=="}}`,
		`{"schema_version":1,"admit":{"authority":"AQ=="}} {}`,
		` {"schema_version":1,"admit":{"authority":"AQ=="}}`,
	} {
		if _, err := modelruntime.ParseJournalCommand([]byte(wire)); err == nil {
			t.Fatalf("accepted malformed command: %s", wire)
		}
	}
	for _, field := range []string{"role", "path", "state", "route", "now", "startup", "initialize"} {
		wire := []byte(`{"schema_version":1,"admit":{"authority":"AQ=="},"` + field + `":{}}`)
		if _, err := modelruntime.ParseJournalCommand(wire); err == nil {
			t.Fatalf("accepted caller-owned %s", field)
		}
	}
	command := modelruntime.JournalCommand{SchemaVersion: 1, Candidates: &modelruntime.JournalCandidatesCommand{
		Authority: bytes.Repeat([]byte{1}, 64<<10), Confirmed: bytes.Repeat([]byte{2}, 64<<10)}}
	wire, err := modelruntime.EncodeJournalCommand(command)
	if err != nil || len(wire) <= 128<<10 {
		t.Fatalf("maximum envelope pair does not encode: %d %v", len(wire), err)
	}
	if _, err := modelruntime.ParseJournalCommand(wire); err != nil {
		t.Fatal(err)
	}
	if _, err := modelruntime.ParseJournalCommand(bytes.Repeat([]byte{' '}, modelruntime.MaximumJournalCommandBytes+1)); err == nil {
		t.Fatal("accepted oversized command")
	}
}

func TestJournalOwnerAPIStorageUncertaintyAndCancellation(t *testing.T) {
	for _, fault := range []string{"directory-sync", "canceled-after-sync", "changed-state"} {
		t.Run(fault, func(t *testing.T) {
			f, owner, config := journalOwnerFixture(t)
			command := modelruntime.JournalCommand{SchemaVersion: 1, Floor: &modelruntime.JournalFloorCommand{Disposition: journalProto(t, f.disposition(t))}}
			wire, err := modelruntime.EncodeJournalCommand(command)
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			if fault == "changed-state" {
				if err := os.Chmod(filepath.Join(config.State.Directory, durableStateFileName), 0o644); err != nil {
					t.Fatal(err)
				}
			} else {
				restore := modelruntime.SetJournalOwnerSyncHookForTest(owner, func(sync func() error) error {
					if fault == "directory-sync" {
						return errors.New("injected post-rename directory sync failure")
					}
					err := sync()
					cancel()
					return err
				})
				defer restore()
			}
			_, err = owner.Apply(ctx, modelruntime.JournalWorkerRole, wire)
			if fault == "canceled-after-sync" {
				if !errors.Is(err, context.Canceled) {
					t.Fatalf("lost cancellation outcome: %v", err)
				}
				if receipt := applyOwnerCommand(t, owner, modelruntime.JournalWorkerRole, command); !receipt.Replayed || receipt.Floor != 11 {
					t.Fatalf("durable canceled write did not replay: %+v", receipt)
				}
			} else {
				if !errors.Is(err, modelruntime.ErrExecutionStateRecovery) {
					t.Fatalf("storage uncertainty was not fenced: %v", err)
				}
				if _, err := owner.Status(t.Context()); !errors.Is(err, modelruntime.ErrExecutionStateRecovery) {
					t.Fatalf("uncertain owner recovered without reconciliation: %v", err)
				}
			}
		})
	}
}

func TestJournalOwnerAPIHistoryBoundAndHealth(t *testing.T) {
	for _, scenario := range []string{"history", "health"} {
		t.Run(scenario, func(t *testing.T) {
			f, owner, _ := journalOwnerFixture(t)
			if _, err := owner.RecordBackendStartupIntent(t.Context()); err != nil {
				t.Fatal(err)
			}
			limit := 32
			if scenario == "health" {
				limit = 1
			}
			for i := range limit {
				authority := journalProto(t, f.authority(t, 0, int64(i+10)))
				applyOwnerCommand(t, owner, modelruntime.JournalRuntimeRole, modelruntime.JournalCommand{Admit: &modelruntime.JournalAuthorityCommand{Authority: authority}})
				if scenario == "health" {
					applyOwnerCommand(t, owner, modelruntime.JournalRuntimeRole, modelruntime.JournalCommand{Candidates: &modelruntime.JournalCandidatesCommand{Authority: authority, Confirmed: authority}})
					applyOwnerCommand(t, owner, modelruntime.JournalRuntimeRole, modelruntime.JournalCommand{Health: &modelruntime.JournalHealthCommand{Authority: authority, Evidence: workerHealthEvidence(f, false)}})
				}
			}
			wire, err := modelruntime.EncodeJournalCommand(modelruntime.JournalCommand{SchemaVersion: 1,
				Admit: &modelruntime.JournalAuthorityCommand{Authority: journalProto(t, f.authority(t, 0, 100))}})
			if err != nil {
				t.Fatal(err)
			}
			before, err := owner.Status(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			if _, err := owner.Apply(t.Context(), modelruntime.JournalRuntimeRole, wire); err == nil || scenario == "history" && !errors.Is(err, modelruntime.ErrExecutionHistoryFull) {
				t.Fatalf("ignored retained history/health restriction: %v", err)
			}
			after, err := owner.Status(t.Context())
			if err != nil || !reflect.DeepEqual(before, after) {
				t.Fatalf("refusal changed or poisoned owner: %v", err)
			}
		})
	}
}

func TestJournalOwnerAPITrustedRouteAndClose(t *testing.T) {
	_, owner, config := journalOwnerFixture(t)
	if err := owner.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := owner.Status(t.Context()); !errors.Is(err, modelruntime.ErrExecutionStateRecovery) {
		t.Fatalf("closed owner still usable: %v", err)
	}
	config.Routes[0].ModelRuntimeEpoch--
	if reopened, err := modelruntime.OpenExecutionJournalOwner(config); err == nil || reopened != nil {
		if reopened != nil {
			_ = reopened.Close()
		}
		t.Fatal("trusted route below launch epoch floor accepted")
	}
}
