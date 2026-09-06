package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/x509"
	"encoding/hex"
	"errors"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/modelruntime"
	"github.com/vivym/vela/internal/stageauthority"
	"github.com/vivym/vela/internal/stageworkeragent"
	"github.com/vivym/vela/internal/stageworkermembertransport"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestDurableWorkerStartupOwnsJournalBeforeReadinessAndAcquire(t *testing.T) {
	identity := productionSmokeIdentity("49800000-0000-0000-0000-000000000004", 11)
	configuration := productionSmokeConfig(t, identity)
	address, files, control := serveStageWorkerControlSmoke(t, identity)
	configuration.controlAddress, configuration.tlsCertificateFile = address, files.clientCertificate
	configuration.tlsPrivateKeyFile, configuration.controlCAFile = files.clientPrivateKey, files.ca
	enableDurableSmoke(t, &configuration, identity)
	launch := durableLaunchForTest(t, configuration)
	var captured stageworkeragent.DurableStreamConfig
	consumers := durableSmokeConsumers(func(value stageworkeragent.DurableStreamConfig) (*stageworkeragent.StreamAgent, error) {
		captured = value
		return stageworkeragent.NewDurableStreamAgent(value)
	})
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	runtime, err := newProductionRuntimeUsing(ctx, configuration, consumers)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = runtime.Close() })
	if captured.Admission == nil || captured.TerminalRetirement == nil || captured.TerminalHistory == nil || captured.InputResolver == nil {
		t.Fatal("Leader startup omitted durable admission or terminal recovery")
	}
	if _, err := stageworkeragent.PrepareAssignmentJournal(t.Context(), launch.admission); err == nil {
		t.Fatal("serving Worker did not hold its admission lock")
	}
	done := make(chan error, 1)
	go func() { done <- runtime.Run(ctx) }()
	select {
	case <-control.acquired:
		cancel()
	case err := <-done:
		t.Fatalf("durable Worker stopped before Acquire: %v", err)
	case <-ctx.Done():
		t.Fatal("durable Worker did not reach Acquire")
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if err := runtime.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := stageworkeragent.PrepareAssignmentJournal(t.Context(), launch.admission); err != nil {
		t.Fatalf("shutdown did not release admission lock: %v", err)
	}
}

func TestDurableWorkerStartupFailureReleasesJournal(t *testing.T) {
	identity := productionSmokeIdentity("49800000-0000-0000-0000-000000000004", 11)
	configuration := productionSmokeConfig(t, identity)
	enableDurableSmoke(t, &configuration, identity)
	stopped := errors.New("injected Stream construction failure")
	runtime, err := newProductionRuntimeUsing(t.Context(), configuration, durableSmokeConsumers(func(stageworkeragent.DurableStreamConfig) (*stageworkeragent.StreamAgent, error) {
		return nil, stopped
	}))
	if runtime != nil || !errors.Is(err, stopped) {
		t.Fatalf("construction fault: %T %v", runtime, err)
	}
	if _, err := stageworkeragent.PrepareAssignmentJournal(t.Context(), durableLaunchForTest(t, configuration).admission); err != nil {
		t.Fatalf("failed startup retained admission lock: %v", err)
	}
}

func TestDurableWorkerMissingHistoryRejectsBeforeExternalConfiguration(t *testing.T) {
	for _, missing := range []string{"state", "inputs"} {
		t.Run(missing, func(t *testing.T) {
			identity := productionSmokeIdentity("49800000-0000-0000-0000-000000000004", 11)
			configuration := productionSmokeConfig(t, identity)
			enableDurableSmoke(t, &configuration, identity)
			path := filepath.Join(configuration.assignmentAdmissionRoot, "assignment-admission.json")
			if missing == "inputs" {
				path = configuration.inputRoot
			}
			if err := os.Rename(path, path+".retained"); err != nil {
				t.Fatal(err)
			}
			configuration.artifactS3AccessKeyFile = filepath.Join(t.TempDir(), "missing-secret")
			if runtime, err := newProductionRuntime(t.Context(), configuration); runtime != nil || err == nil || !strings.Contains(err.Error(), "recover Worker assignment journal before startup") {
				t.Fatalf("missing recovery state crossed external startup boundary: %T %v", runtime, err)
			}
			if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("service recreated missing recovery state: %v", err)
			}
		})
	}
}

func TestDurableWorkerLaunchRejectsConfigurationDrift(t *testing.T) {
	for _, fault := range []string{"worker", "member-epoch", "identity", "device", "roots", "overlap", "limit", "partial"} {
		t.Run(fault, func(t *testing.T) {
			identity := productionSmokeIdentity("49800000-0000-0000-0000-000000000004", 11)
			configuration := productionSmokeConfig(t, identity)
			enableDurableSmoke(t, &configuration, identity)
			switch fault {
			case "worker":
				configuration.workerInstanceEpoch++
			case "member-epoch":
				configuration.workerMemberEpoch++
			case "identity":
				configuration.members[0].identityDigest[0] ^= 1
			case "device":
				configuration.devices[0].DeviceEpoch++
			case "roots":
				configuration.outputRoot = filepath.Join(configuration.scratchRoot, "different-output")
			case "overlap":
				configuration.assignmentAdmissionRoot = filepath.Join(configuration.materializationJournalRoot, "nested")
			case "limit":
				configuration.assignmentAdmissionLimit = 65
			case "partial":
				configuration.launchManifestFile = ""
			}
			if _, err := loadDurableWorkerLaunch(configuration); err == nil {
				t.Fatal("configuration diverged from trusted launch without rejection")
			}
		})
	}
}

func TestDurableWorkerDiscoveryRejectsUnapprovedOrIncompleteRoutes(t *testing.T) {
	for _, fault := range []string{"valid", "epoch-floor", "profile", "residency", "runtime", "device-set", "unknown", "duplicate", "empty"} {
		t.Run(fault, func(t *testing.T) {
			identity := productionSmokeIdentity("49800000-0000-0000-0000-000000000004", 11)
			configuration := productionSmokeConfig(t, identity)
			enableDurableSmoke(t, &configuration, identity)
			launch := durableLaunchForTest(t, configuration)
			identities := []*velav1.ModelRuntimeIdentity{identity}
			switch fault {
			case "epoch-floor":
				identity.ModelRuntimeEpoch = launch.manifest.Runtimes[0].ModelRuntimeEpochFloor
			case "profile":
				identity.StageProfileRevisionId = uuid.NewString()
			case "residency":
				identity.ModelResidencyId = uuid.NewString()
			case "runtime":
				identity.RuntimeIdentity += "-unapproved"
			case "device-set":
				identity.DeviceSetDigest[0] ^= 1
			case "unknown":
				identity.ProtoReflect().SetUnknown([]byte{0xa0, 0x06, 0x01})
			case "duplicate":
				identities = append(identities, identity)
			case "empty":
				identities = nil
			}
			bindings, err := launch.bindMember(configuration.workerMemberID.String(), identities)
			if fault != "valid" {
				if err == nil {
					t.Fatal("unapproved discovery was bound to execution")
				}
				return
			}
			if err != nil || len(bindings) != 1 || bindings[0].Runtime.ModelRuntimeEpoch != identity.GetModelRuntimeEpoch() {
				t.Fatalf("approved observed epoch: %+v %v", bindings, err)
			}
		})
	}
}

func TestDurableWorkerLeaderDiscoversPinnedFollowerBeforeStreamAssembly(t *testing.T) {
	for _, changed := range []bool{false, true} {
		t.Run(map[bool]string{false: "approved", true: "unapproved-profile"}[changed], func(t *testing.T) {
			leaderIdentity := productionSmokeIdentity("49800000-0000-0000-0000-000000000003", 9)
			followerIdentity := productionSmokeIdentity("49800000-0000-0000-0000-000000000004", 11)
			if changed {
				followerIdentity.StageProfileRevisionId = uuid.NewString()
			}
			leader, follower := productionSmokeConfig(t, leaderIdentity), productionSmokeConfig(t, followerIdentity)
			configureDurablePeerPair(t, &leader, &follower)
			enableDurableSmoke(t, &leader, leaderIdentity)
			enableDurableSmoke(t, &follower, followerIdentity)
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			followerBuilt := false
			followerRuntime, err := newProductionRuntimeUsing(ctx, follower, durableSmokeConsumers(func(config stageworkeragent.DurableStreamConfig) (*stageworkeragent.StreamAgent, error) {
				followerBuilt = true
				if config.TerminalHistory != nil || config.TerminalRetirement != nil || config.Admission == nil {
					t.Fatal("follower assembled Leader retirement or omitted its journal")
				}
				return stageworkeragent.NewDurableStreamAgent(config)
			}))
			if err != nil || !followerBuilt {
				t.Fatalf("follower startup: %v", err)
			}
			defer func() { _ = followerRuntime.Close() }()
			leaderBuilt := false
			leaderRuntime, err := newProductionRuntimeUsing(ctx, leader, durableSmokeConsumers(func(config stageworkeragent.DurableStreamConfig) (*stageworkeragent.StreamAgent, error) {
				leaderBuilt = true
				if config.TerminalHistory == nil || config.TerminalRetirement == nil || config.Admission == nil {
					t.Fatal("Leader recovery assembly incomplete")
				}
				return stageworkeragent.NewDurableStreamAgent(config)
			}))
			if changed {
				if err == nil || leaderBuilt || leaderRuntime != nil || !strings.Contains(err.Error(), "approved launch identity") {
					t.Fatalf("pinned but unapproved peer profile crossed startup: %t %v", leaderBuilt, err)
				}
				return
			}
			if err != nil || !leaderBuilt {
				t.Fatalf("Leader startup: %v", err)
			}
			if err := leaderRuntime.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func durableSmokeConsumers(build func(stageworkeragent.DurableStreamConfig) (*stageworkeragent.StreamAgent, error)) productionAuthorityConsumers {
	return productionAuthorityConsumers{
		newMemberServer: stageworkermembertransport.NewServer, newInputResolver: stageworkeragent.NewAssignmentInputResolver,
		newMaterializingAgent: stageworkeragent.NewInputResolvingMaterializingStreamAgent, newDurableAgent: build,
	}
}

func enableDurableSmoke(t *testing.T, configuration *config, identity *velav1.ModelRuntimeIdentity) {
	t.Helper()
	if len(configuration.members) == 0 {
		configuration.members = []memberConfig{{workerMemberID: configuration.workerMemberID, memberEpoch: configuration.workerMemberEpoch,
			identityDigest: sha256.Sum256([]byte("spiffe://vela.internal/stage-worker/smoke-member"))}}
	}
	manifest := modelruntime.LaunchManifest{
		SchemaVersion: 2, WorkerProfileRevisionID: uuid.NewString(), WorkerRole: "llm", CapacitySlots: 1,
		WorkerInstanceID: configuration.workerInstanceID.String(), WorkerInstanceEpoch: configuration.workerInstanceEpoch,
		WorkerMemberID: configuration.workerMemberID.String(), WorkerMemberEpoch: configuration.workerMemberEpoch,
		DeviceSetDigest: hex.EncodeToString(identity.GetDeviceSetDigest()), MembershipDigest: hex.EncodeToString(identity.GetMembershipDigest()),
		Runtimes: []modelruntime.LaunchRuntime{{
			ModelResidencyID: identity.GetModelResidencyId(), RuntimeIdentity: identity.GetRuntimeIdentity(),
			StageProfileRevisionID: identity.GetStageProfileRevisionId(), ModelRuntimeEpochFloor: identity.GetModelRuntimeEpoch() - 1,
			Component: "LLM", ModelComponentRevision: "cpu-mock-test", RuntimeImageDigest: strings.Repeat("a", 64),
			Command: []string{filepath.Join(configuration.scratchRoot, "nonexistent-backend")}, ScratchRoot: configuration.scratchRoot,
			InputRoot: configuration.inputRoot, OutputRoot: configuration.outputRoot, InitializationTimeout: "1s", ShutdownTimeout: "1s",
		}},
	}
	for _, member := range configuration.members {
		deviceID := uuid.NewSHA1(uuid.NameSpaceOID, []byte(member.workerMemberID.String())).String()
		subset := sha256.Sum256([]byte("subset:" + deviceID))
		manifest.Members = append(manifest.Members, modelruntime.LaunchMemberEpoch{ID: member.workerMemberID.String(), Epoch: member.memberEpoch,
			IdentityDigest: hex.EncodeToString(member.identityDigest[:]), DeviceSubsetDigest: hex.EncodeToString(subset[:])})
		manifest.Devices = append(manifest.Devices, modelruntime.LaunchDeviceEpoch{ID: deviceID, Epoch: 3})
		if member.workerMemberID == configuration.workerMemberID {
			manifest.LocalDevices = []modelruntime.DriverDevice{{DeviceID: deviceID, DeviceEpoch: 3, ResourceClass: "CPU"}}
			configuration.devices = []*velav1.StageAuthorityDeviceEpoch{{DeviceId: deviceID, DeviceEpoch: 3}}
		}
	}
	configuration.assignmentAdmissionRoot = filepath.Join(filepath.Dir(configuration.scratchRoot), "assignment-state")
	configuration.assignmentAdmissionLimit = 4
	configuration.launchManifestFile = filepath.Join(filepath.Dir(configuration.scratchRoot), "launch.json")
	writeJournalJSON(t, configuration.launchManifestFile, manifest)
	if err := ensureStageWorkerDirectories(*configuration); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(configuration.assignmentAdmissionRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	launch := durableLaunchForTest(t, *configuration)
	launch.admission.Initialize = true
	if _, err := stageworkeragent.PrepareAssignmentJournal(t.Context(), launch.admission); err != nil {
		t.Fatal(err)
	}
}

func durableLaunchForTest(t *testing.T, configuration config) *durableWorkerLaunch {
	t.Helper()
	launch, err := loadDurableWorkerLaunch(configuration)
	if err != nil || launch == nil {
		t.Fatalf("load durable launch: %v", err)
	}
	validator, err := stageauthority.NewValidator(map[string][]byte{"stage-authority-smoke-v1": bytes.Repeat([]byte{0x71}, 32)}, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	launch.admission.Validator = validator
	return launch
}

func configureDurablePeerPair(t *testing.T, leader, follower *config) {
	t.Helper()
	ca, key, caPEM := issueSmokeCA(t)
	members := []memberConfig{}
	for i, configuration := range []*config{leader, follower} {
		spiffe, err := url.Parse("spiffe://vela.internal/stage-worker/" + configuration.workerMemberID.String())
		if err != nil {
			t.Fatal(err)
		}
		name := []string{"leader.internal", "follower.internal"}[i]
		server, serverKey := issueSmokeCertificate(t, ca, key, name, []string{name}, []*url.URL{spiffe}, []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth})
		client, clientKey := issueSmokeCertificate(t, ca, key, name, nil, []*url.URL{spiffe}, []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth})
		root := t.TempDir()
		configuration.memberListenAddress = unusedSmokeTCPAddress(t)
		configuration.memberClientCertificateFile = writeSmokeSecret(t, root, "client.crt", client)
		configuration.memberClientPrivateKeyFile = writeSmokeSecret(t, root, "client.key", clientKey)
		configuration.memberServerCertificateFile = writeSmokeSecret(t, root, "server.crt", server)
		configuration.memberServerPrivateKeyFile = writeSmokeSecret(t, root, "server.key", serverKey)
		configuration.memberServerCAFile = writeSmokeSecret(t, root, "ca.crt", caPEM)
		configuration.memberClientCAFile = configuration.memberServerCAFile
		configuration.memberDialTimeout, configuration.memberShutdownTimeout = time.Second, time.Second
		members = append(members, memberConfig{workerMemberID: configuration.workerMemberID, memberEpoch: configuration.workerMemberEpoch,
			identityDigest: sha256.Sum256([]byte(spiffe.String())), address: configuration.memberListenAddress, serverName: name})
	}
	leader.members, follower.members = members, members
}

func TestDurableWorkerRestartQueriesHistoryBeforeReadiness(t *testing.T) {
	identity := productionSmokeIdentity("49800000-0000-0000-0000-000000000004", 11)
	configuration := productionSmokeConfig(t, identity)
	address, files, control := serveStageWorkerControlSmoke(t, identity)
	configuration.controlAddress, configuration.tlsCertificateFile = address, files.clientCertificate
	configuration.tlsPrivateKeyFile, configuration.controlCAFile = files.clientPrivateKey, files.ca
	enableDurableSmoke(t, &configuration, identity)
	launch := durableLaunchForTest(t, configuration)
	bindings, err := launch.bindMember(identity.GetWorkerMemberId(), []*velav1.ModelRuntimeIdentity{identity})
	if err != nil {
		t.Fatal(err)
	}
	launch.admission.Bindings = bindings
	gate, err := stageworkeragent.NewFileAssignmentAdmission(launch.admission)
	if err != nil {
		t.Fatal(err)
	}
	assignment := durableSmokeAssignment(t, configuration, identity)
	acquireID := uuid.New()
	handle, err := gate.Begin(t.Context(), assignment, acquireID)
	if err != nil {
		t.Fatal(err)
	}
	handle.Release()
	if err := gate.Close(); err != nil {
		t.Fatal(err)
	}
	statePath := filepath.Join(configuration.assignmentAdmissionRoot, "assignment-admission.json")
	before, err := os.ReadFile(statePath)
	if err != nil {
		t.Fatal(err)
	}
	observed := make(chan *velav1.ReadStageTerminalDispositionRequest, 1)
	control.terminalQuery = func(query *velav1.ReadStageTerminalDispositionRequest) error {
		select {
		case observed <- proto.Clone(query).(*velav1.ReadStageTerminalDispositionRequest):
		default:
		}
		return errors.New("injected terminal history unavailable")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- runWithContext(ctx, configuration) }()
	select {
	case query := <-observed:
		cancel()
		if query.GetAcquireCommandId() != acquireID.String() || !proto.Equal(query.GetAuthority(), assignment.Authority) {
			t.Error("startup did not preserve original execution/Acquire lookup evidence")
		}
	case err := <-done:
		t.Fatalf("startup stopped before recovery query: %v", err)
	case <-ctx.Done():
		t.Fatal("startup did not query retained history")
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	operations, registration, capacity, acquire := control.snapshot()
	if len(operations) < 2 || operations[0] != "capacity" || operations[1] != "terminal" || registration != nil || acquire != nil || capacity == nil {
		t.Fatalf("recovery did not precede readiness/Acquire: %v", operations)
	}
	for _, value := range capacity.GetCapacityVector() {
		if value != 0 {
			t.Fatal("incomplete history recovery published usable capacity")
		}
	}
	if after, err := os.ReadFile(statePath); err != nil || !bytes.Equal(before, after) {
		t.Fatalf("failed recovery rewrote retained history: %v", err)
	}
}

func durableSmokeAssignment(t *testing.T, configuration config, identity *velav1.ModelRuntimeIdentity) *velav1.StageAssignment {
	t.Helper()
	spec := &velav1.StageExecutionSpec{ParametersJson: []byte(`{"prompt":"CPU recovery test"}`)}
	digest, err := stageauthority.ExecutionSpecDigest(spec)
	if err != nil {
		t.Fatal(err)
	}
	signer, err := stageauthority.NewSigner(map[string][]byte{"stage-authority-smoke-v1": bytes.Repeat([]byte{0x71}, 32)})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	authority, err := signer.Sign(&velav1.StageAuthority{
		SchemaVersion: 2, ExecutionSequence: 1,
		JobId: uuid.NewString(), AttemptId: uuid.NewString(), StageRunId: uuid.NewString(), StageAttemptId: uuid.NewString(),
		StageAllocationId: uuid.NewString(), StageLeaseId: uuid.NewString(), AttemptFence: 1, StageFence: 1, StageVersion: 1,
		WorkerInstanceId: identity.GetWorkerInstanceId(), WorkerInstanceEpoch: identity.GetWorkerInstanceEpoch(),
		DeviceSetDigest: identity.GetDeviceSetDigest(), MembershipDigest: identity.GetMembershipDigest(), Devices: configuration.devices,
		Members: []*velav1.StageAuthorityMemberEpoch{{WorkerMemberId: identity.GetWorkerMemberId(), MemberEpoch: identity.GetWorkerMemberEpoch(),
			ModelRuntimeEpoch: identity.GetModelRuntimeEpoch(), IdentityDigest: configuration.members[0].identityDigest[:]}},
		ModelResidencyId: identity.GetModelResidencyId(), ModelRuntimeIdentity: identity.GetRuntimeIdentity(),
		ModelRuntimeBarrierGeneration: 1, StageProfileRevisionId: identity.GetStageProfileRevisionId(),
		CapacityObservationSequence: 1, CapacityVector: configuration.capacityVector,
		LeaseToken: bytes.Repeat([]byte{0x61}, 32), ExecutionNonce: bytes.Repeat([]byte{0x62}, 32), ExecutionSpecDigest: digest[:],
		SigningKeyId: "stage-authority-smoke-v1", IssuedAt: timestamppb.New(now.Add(-time.Second)),
		ExpiresAt: timestamppb.New(now.Add(time.Minute)), MonotonicValidFor: durationpb.New(time.Minute),
	})
	if err != nil {
		t.Fatal(err)
	}
	return &velav1.StageAssignment{Authority: authority, ExecutionSpec: spec,
		RequiredWorkerMemberIds: []string{identity.GetWorkerMemberId()}, MemberStartTimeout: durationpb.New(time.Second)}
}
