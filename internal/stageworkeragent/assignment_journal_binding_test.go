package stageworkeragent_test

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/vivym/vela/internal/stageworkeragent"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/protobuf/proto"
)

func TestAssignmentJournalBindingObservationRequiresHeldState(t *testing.T) {
	for _, fault := range []string{"deferred", "pending", "unbound", "closed", "state", "lock", "directory", "canceled"} {
		t.Run(fault, func(t *testing.T) {
			fixture := newAdmissionFixture(t)
			journal, err := stageworkeragent.PrepareAssignmentJournal(t.Context(), fixture.config)
			if err != nil {
				t.Fatal(err)
			}
			fixture.config.Initialize = false
			binding, verifier := admissionRegistryBinding(t, fixture.config, journal, nil)
			want := proto.Clone(binding).(*velav1.WorkerBootstrapBinding)
			if fault != "unbound" {
				fixture.config.RegistryBinding, fixture.config.RegistryVerifier = binding, verifier
			}
			fixture.config.DeferRuntimeRoutes = fault == "deferred"
			gate := fixture.open(t)
			if fault == "pending" {
				handle := beginAdmission(t, gate, fixture.assignment, fixture.acquireID)
				handle.Release()
			}
			statePath := filepath.Join(fixture.config.Directory, admissionTestState)
			before, err := os.ReadFile(statePath)
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			var displaced string
			switch fault {
			case "closed":
				if err := gate.Close(); err != nil {
					t.Fatal(err)
				}
			case "canceled":
				cancel()
			case "state", "lock", "directory":
				displaced = fixture.config.Directory
				switch fault {
				case "state":
					displaced = statePath
				case "lock":
					displaced = filepath.Join(displaced, "assignment-admission.lock")
				}
				if err := os.Rename(displaced, displaced+".retained"); err != nil {
					t.Fatal(err)
				}
				if fault != "directory" {
					wire, err := os.ReadFile(displaced + ".retained")
					if err != nil {
						t.Fatal(err)
					}
					if err := os.WriteFile(displaced, wire, 0o600); err != nil {
						t.Fatal(err)
					}
				}
			}
			binding.Signature[0] ^= 1
			for attempt := range 2 {
				observed, err := gate.InspectJournalBinding(ctx)
				if fault == "deferred" || fault == "pending" {
					if err != nil || !proto.Equal(observed, want) {
						t.Fatalf("intact journal lost binding: %v %v", observed, err)
					}
					observed.Signature[0] ^= 1
				} else if err == nil || observed != nil {
					t.Fatalf("unavailable journal returned binding: %v %v", observed, err)
				} else if fault == "canceled" && !errors.Is(err, context.Canceled) {
					t.Fatalf("canceled observation: %v", err)
				}
				if attempt == 0 && displaced != "" {
					if err := os.Rename(displaced+".retained", displaced); err != nil {
						t.Fatal(err)
					}
				}
			}
			after, err := os.ReadFile(statePath)
			if err != nil || !bytes.Equal(before, after) {
				t.Fatalf("observation changed retained history: %v", err)
			}
			if fault == "deferred" || fault == "pending" {
				competitor := fixture.config
				competitor.RegistryBinding = want
				if _, err := stageworkeragent.PrepareAssignmentJournal(t.Context(), competitor); err == nil {
					t.Fatal("observation released journal ownership")
				}
			}
			if fault == "unbound" {
				if _, err := gate.Snapshot(t.Context()); err != nil {
					t.Fatalf("unbound observation poisoned ordinary journal use: %v", err)
				}
			}
		})
	}
}
