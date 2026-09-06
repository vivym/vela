package stageworkeragent_test

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/vivym/vela/internal/stageworkeragent"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/protobuf/proto"
)

func TestAssignmentJournalBindingRetentionOwnsIndependentReferences(t *testing.T) {
	fixture := newAdmissionFixture(t)
	journal, err := stageworkeragent.PrepareAssignmentJournal(t.Context(), fixture.config)
	if err != nil {
		t.Fatal(err)
	}
	fixture.config.Initialize = false
	fixture.config.RegistryBinding, fixture.config.RegistryVerifier = admissionRegistryBinding(t, fixture.config, journal, nil)
	gate := fixture.open(t)
	path := filepath.Join(fixture.config.Directory, admissionTestState)
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	first, releaseFirst, err := gate.RetainJournalBinding(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = releaseFirst() }()
	second, releaseSecond, err := gate.RetainJournalBinding(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = releaseSecond() }()
	first.Signature[0] ^= 1
	if !proto.Equal(second, fixture.config.RegistryBinding) {
		t.Fatal("retained bindings share mutable state")
	}
	cancel()
	if err := gate.Close(); !errors.Is(err, stageworkeragent.ErrStageWorkerBusy) {
		t.Fatalf("cancellation released in-flight references: %v", err)
	}
	var releases sync.WaitGroup
	for range 16 {
		releases.Go(func() {
			if err := releaseFirst(); err != nil {
				t.Errorf("release canceled reference: %v", err)
			}
		})
	}
	releases.Wait()
	if err := gate.Close(); !errors.Is(err, stageworkeragent.ErrStageWorkerBusy) {
		t.Fatalf("repeated release consumed another reference: %v", err)
	}
	if _, err := gate.InspectJournalBinding(t.Context()); err != nil {
		t.Fatalf("retention blocked journal observation: %v", err)
	}
	if _, err := stageworkeragent.PrepareAssignmentJournal(t.Context(), fixture.config); err == nil {
		t.Fatal("retention lost the exclusive journal lock")
	}
	if err := releaseSecond(); err != nil {
		t.Fatal(err)
	}
	if err := gate.Close(); err != nil {
		t.Fatalf("released references leaked journal ownership: %v", err)
	}
	if err := releaseFirst(); err != nil {
		t.Fatalf("idempotent release rechecked a closed journal: %v", err)
	}
	after, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(before, after) {
		t.Fatalf("retention changed durable history: %v", err)
	}
}

func TestAssignmentJournalBindingRetentionRejectsUnavailableState(t *testing.T) {
	for _, fault := range []string{"deferred", "unbound", "closed", "canceled"} {
		t.Run(fault, func(t *testing.T) {
			fixture := newAdmissionFixture(t)
			journal, err := stageworkeragent.PrepareAssignmentJournal(t.Context(), fixture.config)
			if err != nil {
				t.Fatal(err)
			}
			fixture.config.Initialize = false
			if fault != "unbound" {
				fixture.config.RegistryBinding, fixture.config.RegistryVerifier = admissionRegistryBinding(t, fixture.config, journal, nil)
			}
			fixture.config.DeferRuntimeRoutes = fault == "deferred"
			gate := fixture.open(t)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			if fault == "closed" {
				if err := gate.Close(); err != nil {
					t.Fatal(err)
				}
			}
			if fault == "canceled" {
				cancel()
			}
			binding, release, err := gate.RetainJournalBinding(ctx)
			if err == nil || binding != nil || release != nil {
				t.Fatalf("unavailable journal retained: %v %v", binding, err)
			}
			if err := gate.Close(); err != nil {
				t.Fatalf("rejected retention leaked a reference: %v", err)
			}
		})
	}
}

func TestAssignmentJournalBindingReleaseDetectsReplacementAfterCancellation(t *testing.T) {
	fixture := newAdmissionFixture(t)
	journal, err := stageworkeragent.PrepareAssignmentJournal(t.Context(), fixture.config)
	if err != nil {
		t.Fatal(err)
	}
	fixture.config.Initialize = false
	fixture.config.RegistryBinding, fixture.config.RegistryVerifier = admissionRegistryBinding(t, fixture.config, journal, nil)
	gate := fixture.open(t)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	_, release, err := gate.RetainJournalBinding(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = release() }()
	path := filepath.Join(fixture.config.Directory, admissionTestState)
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
	cancel()
	if err := release(); err == nil || errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation skipped final ownership validation: %v", err)
	}
	if err := os.Rename(path+".retained", path); err != nil {
		t.Fatal(err)
	}
	if _, err := gate.InspectJournalBinding(t.Context()); err == nil {
		t.Fatal("restored path erased observed ownership failure")
	}
	if err := release(); err == nil {
		t.Fatal("repeated release erased its failure")
	}
	if err := gate.Close(); err != nil {
		t.Fatalf("failed final observation leaked a reference: %v", err)
	}
}

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
