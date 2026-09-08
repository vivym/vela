package modelruntime_test

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/modelruntime"
	"github.com/vivym/vela/internal/stageauthority"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestJournalOwnerNonAdmissionRejectsWithoutPublishing(t *testing.T) {
	for _, kind := range []string{"non-admission", "terminal-non-admission"} {
		t.Run(kind, func(t *testing.T) {
			for _, fault := range []string{"signature", "scope", "wrong-route", "old-epoch", "missing-floor", "admitted-intent", "bad-time", "conflicting-proof", "abort"} {
				t.Run(fault, func(t *testing.T) {
					f, directory := transitionFixture(t)
					disposition := unsignedTerminalAllocation(t, f)
					if fault == "admitted-intent" {
						prepareFloorRuntime(t, f.supervisor, f.authorities[1])
					}
					if fault != "missing-floor" {
						if _, err := f.supervisor.InstallExecutionFloor(t.Context(), disposition); err != nil {
							t.Fatal(err)
						}
					}
					id := disposition.GetAllocations()[1].GetStageAllocationId()
					if fault == "conflicting-proof" {
						if kind == "non-admission" {
							if _, err := f.supervisor.CheckpointTerminalNonAdmission(t.Context(), disposition, id); err != nil {
								t.Fatal(err)
							}
						} else if _, err := f.supervisor.CheckpointNonAdmission(t.Context(), f.authorities[1]); err != nil {
							t.Fatal(err)
						}
					}
					authority := proto.Clone(f.authorities[1]).(*velav1.StageAuthority)
					mutation := modelruntime.ExecutionMutationForTest{Kind: kind, RouteIndex: 1, AllocationID: id,
						Authority: stageauthority.Verified{Authority: authority}, Disposition: disposition, Observed: f.clock.Now()}
					switch fault {
					case "signature":
						authority.Signature[0] ^= 1
						disposition.Signature[0] ^= 1
					case "scope":
						authority.WorkerInstanceEpoch++
						disposition.WorkerInstanceEpoch++
					case "old-epoch":
						authority.Members[0].ModelRuntimeEpoch++
						disposition.Allocations[1].Members[0].ModelRuntimeEpoch++
					case "wrong-route":
						mutation.RouteIndex = 0
					case "bad-time":
						mutation.Observed = time.Time{}
					case "abort":
						mutation.Kind = "abort-" + kind
					}
					if fault == "scope" || fault == "old-epoch" {
						var err error
						mutation.Authority.Authority, err = f.signer.Sign(authority)
						if err != nil {
							t.Fatal(err)
						}
						mutation.Disposition = signTerminalNonAdmission(t, f, disposition)
					}
					assertJournalMutationRejected(t, f, directory, mutation)
					if fault == "missing-floor" {
						if _, err := f.supervisor.InstallExecutionFloor(t.Context(), f.disposition(t)); err != nil {
							t.Fatal(err)
						}
					}
					// A distinct, never-admitted current route remains usable after
					// every rejected draft, including conflicts in the other profile.
					if _, err := f.supervisor.CheckpointNonAdmission(t.Context(), f.authorities[0]); err != nil {
						t.Fatalf("rejection poisoned a legal checkpoint: %v", err)
					}
				})
			}
		})
	}
}

func TestJournalOwnerNonAdmissionRecomputesDigestAndReplaysExactProof(t *testing.T) {
	for _, kind := range []string{"non-admission", "terminal-non-admission"} {
		t.Run(kind, func(t *testing.T) {
			f, directory := transitionFixture(t)
			disposition := unsignedTerminalAllocation(t, f)
			if _, err := f.supervisor.InstallExecutionFloor(t.Context(), disposition); err != nil {
				t.Fatal(err)
			}
			mutation := modelruntime.ExecutionMutationForTest{Kind: kind, RouteIndex: 1,
				Authority:   stageauthority.Verified{Authority: f.authorities[1], Digest: [32]byte{0xff}},
				Disposition: disposition, AllocationID: disposition.Allocations[1].StageAllocationId, Observed: f.clock.Now()}
			applyJournalMutation(t, f, mutation)
			path := filepath.Join(directory, durableStateFileName)
			before, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			syncs := 0
			restore := modelruntime.SetExecutionStateSyncHookForTest(f.supervisor, func(sync func() error) error { syncs++; return sync() })
			mutation.Observed = mutation.Observed.Add(time.Second)
			applyJournalMutation(t, f, mutation)
			restore()
			after, err := os.ReadFile(path)
			if err != nil || !bytes.Equal(before, after) || syncs != 0 || f.backend.calls.Load() != 0 {
				t.Fatalf("exact replay rewrote history or entered backend: %v syncs=%d", err, syncs)
			}
			if kind == "non-admission" {
				proof, err := f.supervisor.InspectNonAdmission(t.Context(), f.authorities[1])
				digest, digestErr := stageauthority.Digest(f.authorities[1])
				if err != nil || digestErr != nil || proof == nil || proof.AuthorityDigest != digest {
					t.Fatalf("caller-supplied digest became durable proof: %+v %v %v", proof, err, digestErr)
				}
			}
		})
	}
}

func TestJournalOwnerStartupBindsManifestAndPreservesUnresolvedIntent(t *testing.T) {
	for _, fault := range []string{"scope", "incarnation", "time", "abort", "none"} {
		t.Run(fault, func(t *testing.T) {
			f, directory := transitionFixture(t)
			manifest := recoveredRuntimeServerConfig(t, f, directory).Manifest
			mutation := modelruntime.ExecutionMutationForTest{Kind: "startup", Manifest: manifest, Incarnation: uuid.New(), Observed: f.clock.Now()}
			switch fault {
			case "scope":
				mutation.Manifest.WorkerInstanceEpoch++
			case "incarnation":
				mutation.Incarnation = uuid.Nil
			case "time":
				mutation.Observed = time.Time{}
			case "abort":
				mutation.Kind = "abort-startup"
			}
			if fault != "none" {
				assertJournalMutationRejected(t, f, directory, mutation)
			}
			mutation.Kind, mutation.Manifest, mutation.Incarnation, mutation.Observed = "startup", manifest, uuid.New(), f.clock.Now()
			applyJournalMutation(t, f, mutation)
			state := readDurableExecutionState(t, directory)
			if state.BackendLifecycle == nil || state.BackendLifecycle.IncarnationID != mutation.Incarnation || f.backend.calls.Load() != 0 {
				t.Fatal("startup mutation lost intent or granted backend execution")
			}
			mutation.Incarnation = uuid.New()
			assertJournalMutationRejected(t, f, directory, mutation)
		})
	}
}

func TestJournalOwnerTerminalExpiryDoesNotPoisonSubsequentCheckpoint(t *testing.T) {
	f, directory := transitionFixture(t)
	disposition := unsignedTerminalAllocation(t, f)
	id := disposition.Allocations[1].StageAllocationId
	if _, err := f.supervisor.InstallExecutionFloor(t.Context(), disposition); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(directory, durableStateFileName)
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	calls := 0
	validator, err := stageauthority.NewValidator(map[string][]byte{"stage-key-9": bytes.Repeat([]byte{0x5a}, 32)}, func() time.Time {
		calls++
		now := f.clock.Now()
		if calls == 2 {
			// The ingress read observes validity; the independent Service clock
			// then advances before checkpoint construction and publication.
			f.clock.Advance(time.Hour)
		}
		return now
	})
	if err != nil {
		t.Fatal(err)
	}
	restore := modelruntime.SetExecutionJournalValidatorForTest(f.supervisor, validator)
	proof, err := f.supervisor.CheckpointTerminalNonAdmission(t.Context(), disposition, id)
	restore()
	if proof != nil || !errors.Is(err, stageauthority.ErrStale) || calls != 3 || f.clock.Now().Before(disposition.ExpiresAt.AsTime()) {
		t.Fatalf("expiry at final owner check = %+v %v calls=%d", proof, err, calls)
	}
	after, readErr := os.ReadFile(path)
	if readErr != nil || !bytes.Equal(before, after) {
		t.Fatalf("expired request changed durable history: %v", readErr)
	}
	refreshed := proto.Clone(disposition).(*velav1.StageTerminalDisposition)
	refreshed.ObservedAt, refreshed.ExpiresAt = timestamppb.New(f.clock.Now()), timestamppb.New(f.clock.Now().Add(time.Minute))
	refreshed.ControlSessionEpoch++
	refreshed = signTerminalNonAdmission(t, f, refreshed)
	if proof, err := f.supervisor.CheckpointTerminalNonAdmission(t.Context(), refreshed, id); err != nil || proof == nil {
		t.Fatalf("expiry poisoned legal retry: %+v %v", proof, err)
	}
}
