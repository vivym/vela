package modelruntime_test

import (
	"context"
	"crypto/sha256"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/modelruntime"
	"google.golang.org/protobuf/proto"
)

func TestJournalOwnerInspectStartupRequiresHeldExactFirstUse(t *testing.T) {
	for _, fault := range []string{"valid", "manifest", "journal", "scope", "incarnation", "launch", "wrong-route-epoch", "changed-state", "closed", "canceled"} {
		t.Run(fault, func(t *testing.T) {
			config, owner, directory := remoteRuntimeServerFixture(t)
			status, err := owner.Status(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			binding, err := proto.MarshalOptions{Deterministic: true}.Marshal(config.RegistryBinding)
			if err != nil {
				t.Fatal(err)
			}
			request := modelruntime.BackendStartupRequest{SchemaVersion: 1, NodeIdentity: config.RegistryBinding.Claim.NodeIdentity,
				RegistryBindingDigest: sha256.Sum256(binding), JournalID: status.JournalID, JournalScope: status.Scope,
				IncarnationID: status.BackendLifecycle.IncarnationID, LaunchDigest: status.BackendLifecycle.LaunchDigest}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			switch fault {
			case "manifest":
				config.Manifest.Runtimes[0].ModelRuntimeEpochFloor++
			case "journal":
				request.JournalID = uuid.New()
			case "scope":
				request.JournalScope[0] ^= 1
			case "incarnation":
				request.IncarnationID = uuid.New()
			case "launch":
				request.LaunchDigest[0] ^= 1
			case "wrong-route-epoch":
				if err := owner.Close(); err != nil {
					t.Fatal(err)
				}
				routes, err := modelruntime.RemoteStartupBindings(config.Manifest)
				if err != nil {
					t.Fatal(err)
				}
				for i := range routes {
					routes[i].ModelRuntimeEpoch++
				}
				owner, err = modelruntime.OpenExecutionJournalOwner(modelruntime.ExecutionJournalOwnerConfig{
					Manifest: config.Manifest, Validator: config.Validator, Routes: routes, Now: time.Now,
					State: modelruntime.ExecutionFloorStateConfig{Directory: directory}})
				if err != nil {
					t.Fatal(err)
				}
				defer func() { _ = owner.Close() }()
			case "changed-state":
				path := filepath.Join(directory, durableStateFileName)
				wire, err := os.ReadFile(path)
				if err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(path, append(wire, ' '), 0o600); err != nil {
					t.Fatal(err)
				}
			case "closed":
				if err := owner.Close(); err != nil {
					t.Fatal(err)
				}
			case "canceled":
				cancel()
			}
			snapshot, err := owner.InspectStartup(ctx, config.Manifest, request)
			if fault != "valid" {
				if err == nil || snapshot.Digest() != ([sha256.Size]byte{}) {
					t.Fatalf("invalid first use returned snapshot: %v", err)
				}
				return
			}
			wire, readErr := os.ReadFile(filepath.Join(directory, durableStateFileName))
			if err != nil || readErr != nil || snapshot.Digest() != sha256.Sum256(wire) || snapshot.MatchStartup(request) != nil {
				t.Fatalf("held startup snapshot differs from durable bytes: %v %v", err, readErr)
			}
			if after, err := owner.Status(ctx); err != nil || after != status {
				t.Fatalf("inspection changed journal: %v", err)
			}
		})
	}
}
