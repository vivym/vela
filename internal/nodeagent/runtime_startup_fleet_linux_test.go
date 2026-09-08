package nodeagent

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/fleet"
	"github.com/vivym/vela/internal/fleetcontroller"
	"github.com/vivym/vela/internal/fleettransport"
	"github.com/vivym/vela/internal/journalbinding"
	"github.com/vivym/vela/internal/modelruntime"
	"github.com/vivym/vela/internal/stageauthority"
	"github.com/vivym/vela/internal/stageworkeragent"
	"github.com/vivym/vela/internal/workerjournal"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// The host integration test provisions PostgreSQL and TLS. Only temporary test
// credentials cross stdin; no host paths or private keys are printed in reports.
func TestRuntimeStartupFleetProcessHelper(t *testing.T) {
	if os.Getenv("VELA_RUNTIME_STARTUP_FLEET_HELPER") != "1" {
		t.Skip("requires host PostgreSQL/TLS integration orchestrator")
	}
	if os.Geteuid() != 0 {
		t.Fatal("requires root Node")
	}
	var input struct {
		Request                     fleet.WorkerBootstrapRequest
		Address, ServerName, SPIFFE string
		CA, Certificate, Key        []byte
		PublicKeys                  map[string][]byte
		LoseResponse                bool
		MissingPtrace               bool
	}
	if err := json.NewDecoder(io.LimitReader(os.Stdin, 2<<20)).Decode(&input); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	root := t.TempDir()
	write := func(name string, data []byte) string {
		t.Helper()
		path := filepath.Join(root, name)
		if err := os.WriteFile(path, data, 0o600); err != nil {
			t.Fatal(err)
		}
		return path
	}
	credentials, err := fleettransport.NewClientTLSCredentials(write("client.pem", input.Certificate), write("client.key", input.Key), write("ca.pem", input.CA), input.ServerName)
	if err != nil {
		t.Fatal(err)
	}
	client, err := fleettransport.DialClient(ctx, input.Address, credentials)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Close() }()
	registry, err := client.WorkerBootstrap(input.SPIFFE)
	if err != nil {
		t.Fatal(err)
	}
	input.Request.ActorIdentity = registry.ActorIdentity()
	claim, err := registry.ClaimWorkerBootstrap(ctx, input.Request)
	if err != nil || !claim.Fresh {
		t.Fatalf("first Registry claim: %v", err)
	}
	bundle, err := fleetcontroller.ParseWorkerBundleActuationManifest(input.Request.BundleManifest)
	if err != nil {
		t.Fatal(err)
	}
	manifest, err := fleetcontroller.WorkerMemberLaunchManifest(bundle, input.Request.WorkerInstanceID, input.Request.WorkerMemberID)
	if err != nil {
		t.Fatal(err)
	}
	routes, err := modelruntime.RemoteStartupBindings(manifest)
	if err != nil {
		t.Fatal(err)
	}
	validator, err := stageauthority.NewValidator(map[string][]byte{"authority": make([]byte, 32)}, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	journalPath := filepath.Join(root, "journal")
	if err := os.Mkdir(journalPath, 0o700); err != nil {
		t.Fatal(err)
	}
	owner, err := modelruntime.OpenExecutionJournalOwner(modelruntime.ExecutionJournalOwnerConfig{Manifest: manifest, Validator: validator, Routes: routes, Now: time.Now, State: modelruntime.ExecutionFloorStateConfig{Directory: journalPath, Initialize: true}})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = owner.Close() }()
	startup, err := owner.RecordBackendStartupIntent(ctx)
	if err != nil {
		t.Fatal(err)
	}
	journal, err := owner.Status(ctx)
	if err != nil {
		t.Fatal(err)
	}
	workerPath := filepath.Join(root, "worker-journal")
	for _, path := range []string{workerPath, manifest.Runtimes[0].InputRoot, manifest.Runtimes[0].OutputRoot} {
		if err := os.MkdirAll(path, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	workerConfig, err := workerjournal.AssignmentConfig(manifest, stageworkeragent.AssignmentAdmissionConfig{
		Directory: workerPath, Initialize: true, Validator: validator, DeferRuntimeRoutes: true,
		MaxRecords: 32,
	})
	if err != nil {
		t.Fatal(err)
	}
	err = stageworkeragent.WithPreparedAssignmentJournal(ctx, workerConfig, func(worker stageworkeragent.AssignmentJournalStatus) error {
		if !worker.Storage.Valid() || worker.SchemaVersion != 5 || worker.JournalID == uuid.Nil {
			t.Fatal("invalid actual Worker journal identity")
		}
		competing := workerConfig
		competing.Initialize = false
		if _, err := stageworkeragent.PrepareAssignmentJournal(ctx, competing); err == nil {
			t.Fatal("Worker journal lock not held through startup")
		}
		_, err = registry.RecordWorkerBootstrapReceipt(ctx, fleet.WorkerBootstrapReceipt{RequestID: claim.RequestID, ActorIdentity: registry.ActorIdentity(), WorkerJournalID: worker.JournalID, WorkerScope: worker.Scope[:], RuntimeJournalID: journal.JournalID, RuntimeScope: journal.Scope[:]})
		if err != nil {
			t.Fatal(err)
		}
		verifier, err := journalbinding.NewVerifier(input.PublicKeys)
		if err != nil {
			t.Fatal(err)
		}
		binding, err := registry.LookupWorkerBootstrapBinding(ctx, claim.RequestID, verifier)
		if err != nil {
			t.Fatal(err)
		}
		plan, err := VerifyRuntimeLaunchPlan(registry.NodeIdentity(), verifier, binding, input.Request.BundleManifest)
		if err != nil {
			t.Fatal(err)
		}
		bindingWire, err := proto.MarshalOptions{Deterministic: true}.Marshal(binding)
		if err != nil {
			t.Fatal(err)
		}
		request := modelruntime.BackendStartupRequest{SchemaVersion: 1, NodeIdentity: registry.NodeIdentity(), RegistryBindingDigest: sha256.Sum256(bindingWire), JournalID: journal.JournalID, JournalScope: journal.Scope, IncarnationID: startup.IncarnationID, LaunchDigest: startup.LaunchDigest}
		wire, err := modelruntime.EncodeBackendStartupRequest(request)
		if err != nil {
			t.Fatal(err)
		}
		callerCredentials := RuntimeCallerCredentials{UID: plan.uid, GID: plan.gid}
		connection, _, _ := runtimeCallerConfiguredConnection(t, "protected-hold-after-disconnect", "unixpacket", wire, callerCredentials, true)
		caller, err := ReceiveRuntimeCaller(ctx, connection, callerCredentials)
		if input.MissingPtrace {
			if err == nil || caller != nil {
				if caller != nil {
					_ = caller.Close()
				}
				t.Fatal("Node without CAP_SYS_PTRACE accepted protected Runtime")
			}
			fmt.Println("VELA_PROTECTED_CALLER_REJECTED_BEFORE_RESERVATION")
			return nil
		}
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = caller.Close() }()
		observer, pods := startupPlanObserverFixture(t, plan, caller)
		ledgerPath := filepath.Join(root, "ledger")
		if err := os.Mkdir(ledgerPath, 0o700); err != nil {
			t.Fatal(err)
		}
		ledger, err := OpenRuntimeStartupLedger(ctx, ledgerPath, registry.NodeIdentity(), true)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = ledger.Close() }()
		config := RuntimeStartupReservationConfig{Plan: plan, Pods: pods, Observer: observer, Caller: caller, Journal: owner, Registry: registry}
		result, err := ledger.ReserveRemote(ctx, config)
		if input.LoseResponse {
			if status.Code(err) != codes.Unavailable || result != (RuntimeStartupReservationRecord{}) {
				t.Fatalf("lost response became result: %v", err)
			}
		} else if err != nil || result.OperationID == uuid.Nil {
			t.Fatalf("real reservation: %v", err)
		}
		if _, err := ledger.ReserveRemote(ctx, config); !errors.Is(err, ErrRuntimeStartupRecorded) {
			t.Fatalf("live retry: %v", err)
		}
		record, err := ledger.Inspect(ctx, journal.JournalID)
		if err != nil {
			t.Fatal(err)
		}
		expected, err := remoteFleetRequest(record)
		if err != nil {
			t.Fatal(err)
		}
		history, err := registry.LookupRuntimeStartup(ctx, record.OperationID)
		if err != nil || history.Fresh || !reflect.DeepEqual(history.RuntimeStartupRequest, expected) {
			t.Fatalf("database history differs from original Node record: %v", err)
		}
		if err := ledger.Close(); err != nil {
			t.Fatal(err)
		}
		recovered, err := OpenRuntimeStartupLedger(ctx, ledgerPath, registry.NodeIdentity(), false)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = recovered.Close() }()
		if _, err := recovered.ReserveRemote(ctx, config); !errors.Is(err, ErrRuntimeStartupRecorded) {
			t.Fatalf("reopen retry: %v", err)
		}
		if _, err := recovered.RecordExit(ctx, journal.JournalID); !errors.Is(err, ErrRuntimeNamespaceOwnerLost) {
			t.Fatalf("reopen recreated pidfd: %v", err)
		}
		receipt, err := recovered.InspectReservation(ctx, journal.JournalID)
		if input.LoseResponse {
			if !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("lost response created receipt: %v", err)
			}
		} else if err != nil || receipt != result {
			t.Fatalf("lost local receipt: %v", err)
		}
		recordWire, err := json.Marshal(record)
		if err != nil {
			t.Fatal(err)
		}
		report, err := json.Marshal(struct {
			Request       fleet.RuntimeStartupRequest
			Record        json.RawMessage
			HasReceipt    bool
			WorkerJournal stageworkeragent.AssignmentJournalStatus
		}{expected, recordWire, !input.LoseResponse, worker})
		if err != nil {
			t.Fatal(err)
		}
		fmt.Printf("VELA_RESERVATION_REPORT=%s\n", report)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}
