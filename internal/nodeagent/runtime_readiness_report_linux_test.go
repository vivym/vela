package nodeagent

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/fleet"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"golang.org/x/sys/unix"
	"google.golang.org/grpc"
)

type readinessReportRuntime struct {
	velav1.UnimplementedModelRuntimeServiceServer
	Discovery *velav1.ModelRuntimeServiceDiscoverRuntimeIdentitiesResponse
	Fault     string
}

func (server *readinessReportRuntime) DiscoverRuntimeIdentities(context.Context, *velav1.ModelRuntimeServiceDiscoverRuntimeIdentitiesRequest) (*velav1.ModelRuntimeServiceDiscoverRuntimeIdentitiesResponse, error) {
	return server.Discovery, nil
}
func (server *readinessReportRuntime) ProbeReadiness(_ context.Context, request *velav1.ModelRuntimeServiceProbeReadinessRequest) (*velav1.ModelRuntimeServiceProbeReadinessResponse, error) {
	return &velav1.ModelRuntimeServiceProbeReadinessResponse{Identity: request.Identity, Check: request.Check,
		Ready: server.Fault != request.Check.String(), Evidence: []byte("current-process-test-evidence")}, nil
}

type readinessRecordingRegistry struct {
	reports []fleet.WorkerInstanceEvidence
}

func (registry *readinessRecordingRegistry) Observe(_ context.Context, evidence fleet.WorkerInstanceEvidence) (fleet.WorkerInstanceDecision, error) {
	registry.reports = append(registry.reports, evidence)
	return fleet.WorkerInstanceDecision{WorkerInstanceID: evidence.WorkerInstanceID, InstanceEpoch: evidence.InstanceEpoch, ControlSessionEpoch: 2, ModelRuntimeEpoch: evidence.Residencies[0].ModelRuntimeEpoch, Readiness: fleet.WorkerInstanceReady}, nil
}

func TestRuntimeReadinessReporterRequiresCurrentEvidence(t *testing.T) {
	if os.Getenv("VELA_READINESS_REPORT_HELPER") != "" {
		var server readinessReportRuntime
		data, err := os.ReadFile(os.Getenv("VELA_READINESS_REPORT_HELPER"))
		if err != nil {
			panic(err)
		}
		if err = json.Unmarshal(data, &server); err != nil {
			panic(err)
		}
		listener, err := net.Listen("unix", os.Getenv("VELA_READINESS_SOCKET"))
		if err != nil {
			panic(err)
		}
		if err = os.Chmod(os.Getenv("VELA_READINESS_SOCKET"), 0600); err != nil {
			panic(err)
		}
		grpcServer := grpc.NewServer()
		velav1.RegisterModelRuntimeServiceServer(grpcServer, &server)
		fmt.Println("listening")
		if err := grpcServer.Serve(listener); err != nil {
			panic(err)
		}
		return
	}
	for _, fault := range []string{"none", "binding", "runtime-identity", "membership", "image", "MODEL_RUNTIME_READINESS_CHECK_DEVICE", "MODEL_RUNTIME_READINESS_CHECK_BACKEND", "MODEL_RUNTIME_READINESS_CHECK_MODEL_WARMUP", "MODEL_RUNTIME_READINESS_CHECK_CANARY"} {
		t.Run(fault, func(t *testing.T) {
			fixture := runtimeLaunchFixture(t)
			worker := &fixture.bundle.WorkerInstances[0]
			member := &worker.Members[0]
			runtime := worker.ModelRuntimes[0]
			directory, err := os.MkdirTemp("/tmp", "vela-live-report-")
			if err != nil {
				t.Fatal(err)
			}
			defer func(path string) { _ = os.RemoveAll(path) }(directory)
			epochs, err := NewFileWorkerInstanceEpochStore(FileWorkerInstanceEpochStoreConfig{Directory: filepath.Join(directory, "epochs"), NodeIdentity: member.NodeIdentity, BootIDPath: "/proc/sys/kernel/random/boot_id"})
			if err != nil {
				t.Fatal(err)
			}
			defer func(cleanup func() error) { _ = cleanup() }(epochs.Close)
			probe := &CPUDeviceProbe{NodeIdentity: member.NodeIdentity, OnlineCPUsPath: "/sys/devices/system/cpu/online", Epochs: epochs}
			deviceID, nodeID := member.DeviceConstraints[0].DeviceID, uuid.New()
			attested, err := probe.AttestWorkerInstanceDevices(t.Context(), []ExpectedWorkerDevice{{Kind: "CPU", DeviceID: deviceID, ComputeNodeID: nodeID, NodeIdentity: member.NodeIdentity}})
			if err != nil {
				t.Fatal(err)
			}
			device := fleet.WorkerDeviceEvidence{ID: deviceID, ComputeNodeID: nodeID, NodeIdentity: member.NodeIdentity, Region: "test", NetworkDomain: "test", FaultDomain: "test", Kind: "CPU", NodeEpoch: attested[0].NodeEpoch, DeviceEpoch: attested[0].DeviceEpoch}
			evidence := fleet.WorkerInstanceEvidence{SchemaVersion: 1, WorkerInstanceID: worker.ID, InstanceEpoch: 1, ControlSessionEpoch: 1,
				DeviceSet:   fleet.WorkerDeviceSetEvidence{ID: uuid.New(), Devices: []fleet.WorkerDeviceEvidence{device}},
				Members:     []fleet.WorkerMemberEvidence{{ID: member.ID, MemberKey: member.Key, ComputeNodeID: nodeID, MemberEpoch: 1, DeviceIDs: []uuid.UUID{deviceID}, Readiness: "UNOBSERVED"}},
				Residencies: []fleet.ModelResidencyEvidence{{ID: runtime.ModelResidencyID, ModelComponentRevision: runtime.ModelComponentRevision, RuntimeIdentity: runtime.RuntimeIdentity, RuntimeImageDigest: "sha256:" + strings.Repeat("a", 64), State: "UNOBSERVED"}},
				Capacity:    fleet.WorkerCapacityEvidence{Vector: map[string]int64{"concurrency": 1}}}
			membership, topology, err := workerDeviceSetDigests(evidence.DeviceSet.Devices)
			if err != nil {
				t.Fatal(err)
			}
			expectedMemberEvidence := cloneWorkerInstanceEvidence(evidence)
			expectedMemberEvidence.Members[0].Readiness = "READY"
			if err := bindWorkerMemberEvidence(&expectedMemberEvidence); err != nil {
				t.Fatal(err)
			}
			m, _ := hex.DecodeString(membership)
			d, _ := hex.DecodeString(topology)
			sum := sha256.Sum256(append(m, d...))
			worker.MembershipDigest = membership
			worker.DeviceSetDigest = hex.EncodeToString(sum[:])
			member.DeviceSubsetDigest = expectedMemberEvidence.Members[0].DeviceSubsetDigest
			fixture.bind(t, 0, 0)
			plan, err := VerifyRuntimeLaunchPlan(member.NodeIdentity, fixture.verifier, fixture.binding, fixture.wire)
			if err != nil {
				t.Fatal(err)
			}
			identity := &velav1.ModelRuntimeIdentity{WorkerInstanceId: worker.ID.String(), WorkerInstanceEpoch: 1, WorkerMemberId: member.ID.String(), WorkerMemberEpoch: 1, ModelResidencyId: runtime.ModelResidencyID.String(), ModelRuntimeEpoch: 1, RuntimeIdentity: runtime.RuntimeIdentity, StageProfileRevisionId: runtime.StageProfileRevisionID.String(), DeviceSetDigest: sum[:], MembershipDigest: m}
			server := readinessReportRuntime{Discovery: &velav1.ModelRuntimeServiceDiscoverRuntimeIdentitiesResponse{Identities: []*velav1.ModelRuntimeIdentity{identity}, JournalBinding: fixture.binding}, Fault: fault}
			switch fault {
			case "binding":
				server.Discovery.JournalBinding = nil
			case "runtime-identity":
				identity.RuntimeIdentity = "other-runtime"
			case "membership":
				identity.MembershipDigest = make([]byte, 32)
			case "image":
				evidence.Residencies[0].RuntimeImageDigest = "sha256:" + strings.Repeat("e", 64)
			}
			configPath, socketPath := filepath.Join(directory, "reply.json"), filepath.Join(directory, "runtime.sock")
			data, err := json.Marshal(server)
			if err != nil {
				t.Fatal(err)
			}
			if err = os.WriteFile(configPath, data, 0600); err != nil {
				t.Fatal(err)
			}
			command := exec.Command(os.Args[0], "-test.run=^TestRuntimeReadinessReporterRequiresCurrentEvidence$")
			command.Env = append(os.Environ(), "VELA_READINESS_REPORT_HELPER="+configPath, "VELA_READINESS_SOCKET="+socketPath)
			pidfd := -1
			command.SysProcAttr = &syscall.SysProcAttr{PidFD: &pidfd}
			pipe, err := command.StdoutPipe()
			if err != nil {
				t.Fatal(err)
			}
			command.Stderr = os.Stderr
			if err = command.Start(); err != nil {
				t.Fatal(err)
			}
			defer func() {
				_ = command.Process.Kill()
				_ = command.Wait()
				if pidfd >= 0 {
					_ = unix.Close(pidfd)
				}
			}()
			scanner := bufio.NewScanner(pipe)
			if !scanner.Scan() || scanner.Text() != "listening" {
				t.Fatal("Runtime did not start")
			}
			duplicate, err := unix.FcntlInt(uintptr(pidfd), unix.F_DUPFD_CLOEXEC, 0)
			if err != nil {
				t.Fatal(err)
			}
			boot, err := readBootID("/proc/sys/kernel/random/boot_id")
			if err != nil {
				t.Fatal(err)
			}
			owner := &RuntimeNamespaceOwner{pidfd: os.NewFile(uintptr(duplicate), "original"), owner: RuntimeContainerCallerObservation{Process: RuntimeCallerObservation{HostPID: int32(command.Process.Pid), UID: uint32(os.Getuid()), GID: uint32(os.Getgid()), BootID: uuid.MustParse(boot)}}}
			defer func(cleanup func() error) { _ = cleanup() }(owner.Close)
			registry := &readinessRecordingRegistry{}
			reporter, err := NewWorkerInstanceEvidenceReporter(probe, registry, epochs, time.Minute, time.Now)
			if err != nil {
				t.Fatal(err)
			}
			live := &RuntimeWorkerEvidenceReporter{Reporter: reporter, Owner: owner, Plan: plan, RegistryVerifier: fixture.verifier, SocketPath: socketPath}
			template := WorkerInstanceEvidenceTemplate{Evidence: evidence, ObservedBy: "node-agent/test"}
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			decision, err := live.Report(ctx, template)
			if fault != "none" {
				if err == nil || len(registry.reports) != 0 {
					t.Fatalf("published invalid evidence: %v %v", decision, err)
				}
				return
			}
			if err != nil || len(registry.reports) != 1 || decision.ControlSessionEpoch != 2 {
				t.Fatalf("live report: %v %v", decision, err)
			}
			got := registry.reports[0]
			if got.Residencies[0].State != "READY" || got.Members[0].Readiness != "READY" || got.Residencies[0].ModelRuntimeEpoch != 1 || !validDigestHex(got.Residencies[0].CanaryEvidenceDigest) || got.DeviceSet.Devices[0].AttestationDigest == "" {
				t.Fatal("incomplete live evidence")
			}
			if template.Evidence.Residencies[0].State != "UNOBSERVED" || template.Evidence.Residencies[0].ModelRuntimeEpoch != 0 {
				t.Fatal("mutated template")
			}
			if err = command.Process.Kill(); err != nil {
				t.Fatal(err)
			}
			_ = command.Wait()
			if _, err = live.Report(ctx, template); err == nil || len(registry.reports) != 1 {
				t.Fatal("exited Runtime renewed READY")
			}
		})
	}
}
