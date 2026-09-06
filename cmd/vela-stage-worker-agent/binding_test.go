package main

import (
	"bytes"
	"crypto/ed25519"
	"errors"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/journalbinding"
	"github.com/vivym/vela/internal/modelruntime"
	"github.com/vivym/vela/internal/stageworkeragent"
	"github.com/vivym/vela/internal/stageworkermembertransport"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Fixture signing is independent of the execution secret distributed to Workers.
func configureWorkerJournalBinding(t *testing.T, configuration *config, manifest modelruntime.LaunchManifest, journal stageworkeragent.AssignmentJournalStatus, mutate func(*velav1.WorkerBootstrapBinding)) {
	t.Helper()
	seed := bytes.Repeat([]byte{27}, ed25519.SeedSize)
	signer, err := journalbinding.NewSigner("registry", seed)
	if err != nil {
		t.Fatal(err)
	}
	requestID := uuid.NewString()
	value := &velav1.WorkerBootstrapBinding{SchemaVersion: journalbinding.SchemaVersion,
		Claim: &velav1.WorkerBootstrapClaim{RequestId: requestID, WorkerInstanceId: manifest.WorkerInstanceID, WorkerInstanceEpoch: manifest.WorkerInstanceEpoch,
			WorkerMemberId: manifest.WorkerMemberID, WorkerMemberEpoch: manifest.WorkerMemberEpoch, NodeIdentity: "cpu-node", ActorIdentity: "node-agent/cpu-node",
			BundleDigest: bytes.Repeat([]byte{1}, 32), ClaimedAt: timestamppb.Now()},
		Pair: &velav1.WorkerBootstrapJournalPair{RequestId: requestID, ActorIdentity: "node-agent/cpu-node", WorkerJournalId: journal.JournalID.String(), WorkerScope: journal.Scope[:],
			RuntimeJournalId: uuid.NewString(), RuntimeScope: bytes.Repeat([]byte{2}, 32), RecordedAt: timestamppb.Now()},
	}
	if mutate != nil {
		mutate(value)
	}
	binding, err := signer.Sign(value)
	if err != nil {
		t.Fatal(err)
	}
	wire, err := journalbinding.Encode(binding)
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	configuration.journalBindingFile = filepath.Join(root, "binding.json")
	if err := os.WriteFile(configuration.journalBindingFile, wire, 0o600); err != nil {
		t.Fatal(err)
	}
	configuration.journalBindingVerifierFile = filepath.Join(root, "registry-verifiers.json")
	writeJournalJSON(t, configuration.journalBindingVerifierFile, map[string][]byte{"registry": ed25519.NewKeyFromSeed(seed).Public().(ed25519.PublicKey)})
}

func TestDurableWorkerRejectsRegistryMismatchBeforeExternalStartup(t *testing.T) {
	for _, fault := range []string{"journal", "scope", "worker-epoch", "member-epoch", "missing-binding", "missing-verifier", "signature"} {
		t.Run(fault, func(t *testing.T) {
			identity := productionSmokeIdentity("49800000-0000-0000-0000-000000000004", 11)
			configuration := productionSmokeConfig(t, identity)
			enableDurableSmoke(t, &configuration, identity)
			launch := durableLaunchForTest(t, configuration)
			journal, err := stageworkeragent.PrepareAssignmentJournal(t.Context(), launch.admission)
			if err != nil {
				t.Fatal(err)
			}
			configureWorkerJournalBinding(t, &configuration, launch.manifest, journal, func(v *velav1.WorkerBootstrapBinding) {
				switch fault {
				case "journal":
					v.Pair.WorkerJournalId = uuid.NewString()
				case "scope":
					v.Pair.WorkerScope[0] ^= 1
				case "worker-epoch":
					v.Claim.WorkerInstanceEpoch++
				case "member-epoch":
					v.Claim.WorkerMemberEpoch++
				}
			})
			switch fault {
			case "missing-binding":
				configuration.journalBindingFile = ""
			case "missing-verifier":
				configuration.journalBindingVerifierFile = ""
			case "signature":
				wire, err := os.ReadFile(configuration.journalBindingFile)
				if err != nil {
					t.Fatal(err)
				}
				wire = bytes.Replace(wire, []byte("cpu-node"), []byte("other-node"), 1)
				if err := os.WriteFile(configuration.journalBindingFile, wire, 0o600); err != nil {
					t.Fatal(err)
				}
			}
			configuration.artifactS3AccessKeyFile = "/nonexistent-external-secret"
			if runtime, err := newProductionRuntime(t.Context(), configuration); runtime != nil || err == nil || strings.Contains(err.Error(), "nonexistent-external-secret") {
				t.Fatalf("invalid Registry binding crossed external startup boundary: %T %v", runtime, err)
			}
			if recovered, err := stageworkeragent.PrepareAssignmentJournal(t.Context(), launch.admission); err != nil || recovered != journal {
				t.Fatalf("binding rejection changed journal or retained ownership: %+v %v", recovered, err)
			}
		})
	}
}

func TestDurableWorkerOwnsJournalBeforeMemberServerAndRevalidatesPublication(t *testing.T) {
	for _, replace := range []bool{false, true} {
		t.Run(map[bool]string{false: "held", true: "replaced"}[replace], func(t *testing.T) {
			leaderID := productionSmokeIdentity("49800000-0000-0000-0000-000000000003", 9)
			followerID := productionSmokeIdentity("49800000-0000-0000-0000-000000000004", 11)
			leader, follower := productionSmokeConfig(t, leaderID), productionSmokeConfig(t, followerID)
			configureDurablePeerPair(t, &leader, &follower)
			enableDurableSmoke(t, &follower, followerID)
			launch := durableLaunchForTest(t, follower)
			called := false
			consumers := durableSmokeConsumers(stageworkeragent.NewDurableStreamAgent)
			consumers.newMemberServer = func(config stageworkermembertransport.ServerConfig) (*stageworkermembertransport.Server, error) {
				called = true
				if _, err := stageworkeragent.PrepareAssignmentJournal(t.Context(), launch.admission); err == nil {
					t.Fatal("member server construction preceded journal ownership")
				}
				if replace {
					path := filepath.Join(follower.assignmentAdmissionRoot, "assignment-admission.json")
					wire, err := os.ReadFile(path)
					if err != nil {
						t.Fatal(err)
					}
					if err := os.Rename(path, path+".retained"); err != nil {
						t.Fatal(err)
					}
					if err := os.WriteFile(path, wire, 0o600); err != nil {
						t.Fatal(err)
					}
				}
				return stageworkermembertransport.NewServer(config)
			}
			runtime, err := newProductionRuntimeUsing(t.Context(), follower, consumers)
			if !called {
				t.Fatalf("did not reach member construction: %v", err)
			}
			if replace {
				if runtime != nil || err == nil {
					t.Fatal("replaced held journal allowed member publication")
				}
				listener, err := net.Listen("tcp", follower.memberListenAddress)
				if err != nil {
					t.Fatalf("failed startup left member endpoint listening: %v", err)
				}
				if err := listener.Close(); err != nil {
					t.Fatal(err)
				}
			} else {
				if err != nil {
					t.Fatal(err)
				}
				if err := runtime.Close(); err != nil {
					t.Fatal(err)
				}
				if _, err := stageworkeragent.PrepareAssignmentJournal(t.Context(), launch.admission); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}

func TestDurableWorkerEarlyExternalFailureReleasesJournal(t *testing.T) {
	identity := productionSmokeIdentity("49800000-0000-0000-0000-000000000004", 11)
	configuration := productionSmokeConfig(t, identity)
	enableDurableSmoke(t, &configuration, identity)
	configuration.artifactS3AccessKeyFile = "/nonexistent-external-secret"
	if runtime, err := newProductionRuntime(t.Context(), configuration); runtime != nil || !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("unexpected external failure: %v", err)
	}
	if _, err := stageworkeragent.PrepareAssignmentJournal(t.Context(), durableLaunchForTest(t, configuration).admission); err != nil {
		t.Fatalf("early external failure leaked journal lock: %v", err)
	}
}
