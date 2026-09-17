//go:build linux

// vela-validation-runtime-launcher is intentionally validation-only. It is a
// real CRI launcher used to exercise the Node RuntimeStartupLauncher wire
// contract on a disposable host. It must never be installed at the production
// launcher path: its policy response is a test evidence source, not Fleet
// authority.
package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	tasksapi "github.com/containerd/containerd/api/services/tasks/v1"
	"github.com/google/uuid"
	"github.com/vivym/vela/internal/nodeagent"
	"github.com/vivym/vela/internal/securefile"
	"github.com/vivym/vela/internal/strictjson"
	"golang.org/x/sys/unix"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	corev1 "k8s.io/api/core/v1"
	runtimev1 "k8s.io/cri-api/pkg/apis/runtime/v1"
)

const (
	protocolVersion         = 1 // policy channel wire version
	launcherProtocolVersion = 2 // launcher handoff wire version
)

type launcherRequest struct {
	Version           int               `json:"version"`
	Manifest          json.RawMessage   `json:"manifest"`
	ExpectedPod       json.RawMessage   `json:"expected_pod"`
	ExpectedPodDigest [sha256.Size]byte `json:"expected_pod_digest"`
	StartupSocket     string            `json:"startup_socket"`
}

type launcherReply struct {
	Version        int                              `json:"version"`
	ValidationOnly bool                             `json:"validation_only"`
	Target         nodeagent.RuntimeContainerTarget `json:"target"`
	WorkerTarget   nodeagent.RuntimeContainerTarget `json:"worker_target"`
	// SCM_RIGHTS order is Runtime pidfd, Worker pidfd, observer pidfd, observer socket.
	FDCount int `json:"fd_count"`
}

type policyRequest struct {
	Version           int               `json:"version"`
	OperationID       uuid.UUID         `json:"operation_id"`
	JournalID         uuid.UUID         `json:"journal_id"`
	RequestDigest     [sha256.Size]byte `json:"request_digest"`
	ReservationDigest [sha256.Size]byte `json:"reservation_digest"`
}

type policyReply struct {
	Version           int               `json:"version"`
	OperationID       uuid.UUID         `json:"operation_id"`
	JournalID         uuid.UUID         `json:"journal_id"`
	RequestDigest     [sha256.Size]byte `json:"request_digest"`
	ReservationDigest [sha256.Size]byte `json:"reservation_digest"`
	EvidenceDigest    [sha256.Size]byte `json:"evidence_digest"`
	IssuedAt          time.Time         `json:"issued_at"`
	ExpiresAt         time.Time         `json:"expires_at"`
}

type validationReceipt struct {
	Version        int                              `json:"version"`
	ValidationOnly bool                             `json:"validation_only"`
	Scenario       string                           `json:"scenario"`
	StartedAt      time.Time                        `json:"started_at"`
	FinishedAt     time.Time                        `json:"finished_at"`
	Target         nodeagent.RuntimeContainerTarget `json:"target"`
	WorkerTarget   nodeagent.RuntimeContainerTarget `json:"worker_target"`
	FDCount        int                              `json:"fd_count"`
	Outcome        string                           `json:"outcome"`
	Error          string                           `json:"error,omitempty"`
	CleanupError   string                           `json:"cleanup_error,omitempty"`
}

type validationWorkload struct {
	connection   *grpc.ClientConn
	runtime      runtimev1.RuntimeServiceClient
	image        runtimev1.ImageServiceClient
	tasks        tasksapi.TasksClient
	target       nodeagent.RuntimeContainerTarget
	workerTarget nodeagent.RuntimeContainerTarget
	workerPIDFD  *os.File
	runtimePIDFD *os.File
	observer     *exec.Cmd
	observerPID  *os.File
	observerEnd  *os.File
	runtimeOffer *pidfdOfferListener
	workerOffer  *pidfdOfferListener
	volumeRoot   string
	receiptPath  string
	scenario     string
	startedAt    time.Time
	handedOff    int
}

func main() {
	if os.Getenv("VELA_VALIDATION_PIDFD_OFFER") == "1" {
		if err := runPIDFDOffer(os.Args[1:]); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		return
	}
	controlFD := 3
	flag.IntVar(&controlFD, "vela-runtime-launcher-control-fd", 3, "inherited launcher control descriptor")
	flag.Parse()
	if os.Getenv("VELA_VALIDATION_ONLY") != "1" {
		fatal("VELA_VALIDATION_ONLY=1 is required")
	}
	if os.Geteuid() != 0 {
		fatal("validation runtime launcher must run as root")
	}
	fd := 3
	if value := os.Getenv("VELA_VALIDATION_LAUNCHER_CONTROL_FD"); value != "" {
		parsed, err := strconv.Atoi(value)
		if err != nil || parsed < 0 {
			fatal("invalid VELA_VALIDATION_LAUNCHER_CONTROL_FD")
		}
		fd = parsed
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if controlFD != 3 {
		fd = controlFD
	}
	if err := run(ctx, fd); err != nil {
		fatal(err.Error())
	}
}

func run(ctx context.Context, fd int) (runErr error) {
	control := os.NewFile(uintptr(fd), "validation-launcher-control")
	if control == nil {
		return errors.New("validation launcher control fd is unavailable")
	}
	defer func(cleanup func() error) { _ = cleanup() }(control.Close)
	stopControl := context.AfterFunc(ctx, func() { _ = control.Close() })
	defer stopControl()
	packet, err := recvFrame(ctx, fd)
	if err != nil {
		if errors.Is(err, context.Canceled) {
			return nil
		}
		return fmt.Errorf("receive launcher request: %w", err)
	}
	var request launcherRequest
	if err := strictJSON(packet, &request); err != nil || request.Version != launcherProtocolVersion {
		return errors.New("validation launcher request schema is invalid")
	}
	if len(request.ExpectedPod) == 0 || sha256.Sum256(request.ExpectedPod) != request.ExpectedPodDigest {
		return errors.New("validation launcher expected Pod digest mismatch")
	}
	if !filepath.IsAbs(request.StartupSocket) || filepath.Clean(request.StartupSocket) != request.StartupSocket {
		return errors.New("validation launcher startup socket is not canonical")
	}
	receiptPath := os.Getenv("VELA_VALIDATION_RECEIPT_PATH")
	if !filepath.IsAbs(receiptPath) || filepath.Clean(receiptPath) != receiptPath || receiptPath == "/" {
		return errors.New("VELA_VALIDATION_RECEIPT_PATH must be a canonical absolute path")
	}
	var pod corev1.Pod
	if err := strictJSON(request.ExpectedPod, &pod); err != nil {
		return fmt.Errorf("decode expected Pod: %w", err)
	}
	workload, err := launchWorkload(ctx, request.StartupSocket, &pod)
	if err != nil {
		if workload == nil {
			if receiptErr := writeStandaloneValidationFailureReceipt(receiptPath, getenvDefault("VELA_VALIDATION_SCENARIO", "unspecified"), err); receiptErr != nil {
				return errors.Join(err, receiptErr)
			}
		}
		return err
	}
	workload.receiptPath = receiptPath
	workload.scenario = getenvDefault("VELA_VALIDATION_SCENARIO", "unspecified")
	defer func() {
		// Cleanup must use an independent context. A canceled control context is
		// exactly the case in which the disposable CRI workload must still be
		// stopped and removed.
		cleanupErr := workload.cleanup(context.Background())
		runErr = errors.Join(runErr, cleanupErr)
		runErr = errors.Join(runErr, writeValidationReceipt(workload, runErr, cleanupErr))
	}()
	// Keep the crash scenario receipt-bearing: a panic in the validation helper
	// is converted into a failed run after the workload cleanup defer observes
	// the captured error.
	defer func() {
		if recovered := recover(); recovered != nil {
			runErr = fmt.Errorf("validation helper crashed: %v", recovered)
		}
	}()

	reply, err := json.Marshal(launcherReply{Version: launcherProtocolVersion, ValidationOnly: true, Target: workload.target, WorkerTarget: workload.workerTarget, FDCount: 4})
	if err != nil {
		return err
	}
	if err := sendFrameWithRights(fd, reply, []int{int(workload.runtimePIDFD.Fd()), int(workload.workerPIDFD.Fd()), int(workload.observerPID.Fd()), int(workload.observerEnd.Fd())}); err != nil {
		return fmt.Errorf("send validation launcher handoff: %w", err)
	}
	workload.handedOff = 4
	if getenvDefault("VELA_VALIDATION_SCENARIO", "") == "observer-channel-loss" {
		if err := unix.Shutdown(int(workload.observerEnd.Fd()), unix.SHUT_RDWR); err != nil {
			return fmt.Errorf("interrupt validation observer channel: %w", err)
		}
		_ = workload.observerEnd.Close()
		workload.observerEnd = nil
	}
	if getenvDefault("VELA_VALIDATION_SCENARIO", "") == "helper-crash" {
		panic("intentional validation helper crash")
	}
	// The Node now owns duplicates of these descriptors. The helper retains its
	// copies only for cleanup and closes its observer socket endpoint after the
	// SCM_RIGHTS transfer, before the observer executable is released.
	if workload.observerEnd != nil {
		_ = workload.observerEnd.Close()
		workload.observerEnd = nil
	}
	for {
		packet, err := recvFrame(ctx, fd)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				// A controlled Node shutdown cancels the helper context. The
				// timeout fault deliberately turns the same transport event into
				// a failed validation receipt so the scenario cannot look like a
				// successful completion.
				scenario := getenvDefault("VELA_VALIDATION_SCENARIO", "")
				if scenario == "helper-timeout" || scenario == "observer-channel-loss" || scenario == "caller-replacement" {
					return fmt.Errorf("validation scenario %s terminated without a successful policy exchange", scenario)
				}
				return nil
			}
			if errors.Is(err, io.EOF) || errors.Is(err, unix.ECONNRESET) || errors.Is(err, unix.ENOTCONN) {
				return fmt.Errorf("validation launcher control channel lost: %w", err)
			}
			return fmt.Errorf("receive validation policy request: %w", err)
		}
		var request policyRequest
		if err := strictJSON(packet, &request); err != nil || request.Version != protocolVersion || request.OperationID == uuid.Nil || request.RequestDigest == ([sha256.Size]byte{}) {
			return errors.New("validation policy request schema is invalid")
		}
		if getenvDefault("VELA_VALIDATION_SCENARIO", "") == "policy-response-loss" {
			return errors.New("validation policy response intentionally suppressed")
		}
		issued := time.Now().UTC()
		evidenceInput := append(append([]byte(nil), request.RequestDigest[:]...), request.OperationID[:]...)
		evidenceInput = append(evidenceInput, request.ReservationDigest[:]...)
		evidence := sha256.Sum256(evidenceInput)
		reply, err := json.Marshal(policyReply{Version: protocolVersion, OperationID: request.OperationID, JournalID: request.JournalID, RequestDigest: request.RequestDigest, ReservationDigest: request.ReservationDigest, EvidenceDigest: evidence, IssuedAt: issued, ExpiresAt: issued.Add(2 * time.Minute)})
		if err != nil {
			return err
		}
		if err := sendFrameWithRights(fd, reply, nil); err != nil {
			return fmt.Errorf("send validation policy response: %w", err)
		}
	}
}

func launchWorkload(ctx context.Context, startupSocket string, pod *corev1.Pod) (workload *validationWorkload, runErr error) {
	if pod == nil {
		return nil, errors.New("expected Pod UID is invalid")
	}
	if pod.UID != "" {
		if parsed, err := uuid.Parse(string(pod.UID)); err != nil || parsed == uuid.Nil {
			return nil, errors.New("expected Pod UID is invalid")
		}
	}
	var container *corev1.Container
	for i := range pod.Spec.Containers {
		if pod.Spec.Containers[i].Name == "model-runtime" {
			if container != nil {
				return nil, errors.New("expected Pod has duplicate model-runtime containers")
			}
			container = &pod.Spec.Containers[i]
		}
	}
	if container == nil || container.Image == "" {
		return nil, errors.New("expected Pod has no model-runtime image")
	}
	socketPath := os.Getenv("VELA_VALIDATION_CONTAINER_STARTUP_SOCKET")
	if socketPath == "" {
		socketPath = "/run/vela-node/startup.sock"
	}
	if !filepath.IsAbs(socketPath) || filepath.Clean(socketPath) != socketPath {
		return nil, errors.New("validation container startup socket is not canonical")
	}
	criSocket := os.Getenv("VELA_VALIDATION_CRI_SOCKET")
	if criSocket == "" {
		return nil, errors.New("VELA_VALIDATION_CRI_SOCKET is required")
	}
	connection, err := dialCRI(ctx, criSocket)
	if err != nil {
		return nil, err
	}
	workload = &validationWorkload{connection: connection, runtime: runtimev1.NewRuntimeServiceClient(connection), image: runtimev1.NewImageServiceClient(connection), tasks: tasksapi.NewTasksClient(connection), receiptPath: os.Getenv("VELA_VALIDATION_RECEIPT_PATH"), scenario: getenvDefault("VELA_VALIDATION_SCENARIO", "unspecified"), startedAt: time.Now().UTC()}
	activeWorkload := workload
	cleanup := true
	defer func() {
		if cleanup {
			workload = activeWorkload // preserve exact partial targets for the failure receipt
			cleanupErr := activeWorkload.cleanup(context.Background())
			runErr = errors.Join(runErr, cleanupErr)
			runErr = errors.Join(runErr, writeValidationReceipt(activeWorkload, runErr, cleanupErr))
		}
	}()

	imagePull := getenvDefault("VELA_VALIDATION_SANDBOX_IMAGE", "registry.k8s.io/pause:3.9")
	workerImage := getenvDefault("VELA_VALIDATION_WORKER_IMAGE", container.Image)
	for _, image := range []string{imagePull, container.Image, workerImage} {
		if err := ensureImage(ctx, workload.image, image); err != nil {
			return nil, fmt.Errorf("prepare validation image %q: %w", image, err)
		}
	}
	uid, gid := containerCredentials(*pod, *container)
	if uid == 0 || gid == 0 {
		return nil, errors.New("validation workload requires non-root runtime credentials")
	}
	podUID := string(pod.UID)
	if podUID == "" {
		podUID = uuid.NewString()
	}
	uidValue, gidValue := uid, gid
	namespaces := &runtimev1.NamespaceOption{Network: runtimev1.NamespaceMode_NODE, Pid: runtimev1.NamespaceMode_CONTAINER, Ipc: runtimev1.NamespaceMode_POD}
	sandboxConfig := &runtimev1.PodSandboxConfig{Metadata: &runtimev1.PodSandboxMetadata{Name: pod.Name, Namespace: pod.Namespace, Uid: podUID, Attempt: 1}, LogDirectory: filepath.Join(os.TempDir(), "vela-validation-logs"), Linux: &runtimev1.LinuxPodSandboxConfig{CgroupParent: getenvDefault("VELA_VALIDATION_CGROUP_PARENT", "kubepods.slice"), SecurityContext: &runtimev1.LinuxSandboxSecurityContext{NamespaceOptions: namespaces, RunAsUser: &runtimev1.Int64Value{Value: int64(uid)}, RunAsGroup: &runtimev1.Int64Value{Value: int64(gid)}}}}
	sandbox, err := workload.runtime.RunPodSandbox(ctx, &runtimev1.RunPodSandboxRequest{Config: sandboxConfig, RuntimeHandler: getenvDefault("VELA_VALIDATION_RUNTIME_HANDLER", "runc")})
	if err != nil {
		return nil, fmt.Errorf("run validation PodSandbox: %w", err)
	}
	workload.target.SandboxID = sandbox.PodSandboxId
	root, err := os.MkdirTemp("/tmp", "vela-validation-launcher-")
	if err != nil {
		return nil, err
	}
	workload.volumeRoot = root
	workload.runtimeOffer, err = newPIDFDOfferListener(root, "runtime", uid, gid)
	if err != nil {
		return nil, err
	}
	workload.workerOffer, err = newPIDFDOfferListener(root, "worker", uid, gid)
	if err != nil {
		return nil, err
	}
	mounts, err := validationMounts(pod, container, root, startupSocket, socketPath)
	if err != nil {
		return nil, err
	}
	envs := make([]*runtimev1.KeyValue, 0, len(container.Env)+1)
	for _, value := range container.Env {
		if value.ValueFrom != nil {
			return nil, errors.New("validation launcher does not resolve downward or secret environment")
		}
		envs = append(envs, &runtimev1.KeyValue{Key: value.Name, Value: value.Value})
	}
	if !containsEnv(envs, "VELA_MODEL_RUNTIME_NODE_STARTUP_SOCKET") {
		envs = append(envs, &runtimev1.KeyValue{Key: "VELA_MODEL_RUNTIME_NODE_STARTUP_SOCKET", Value: socketPath})
	}
	command, arguments := append([]string(nil), container.Command...), append([]string(nil), container.Args...)
	if callerBinary := os.Getenv("VELA_VALIDATION_CALLER_BINARY"); callerBinary != "" {
		if !filepath.IsAbs(callerBinary) || filepath.Clean(callerBinary) != callerBinary {
			return nil, errors.New("VELA_VALIDATION_CALLER_BINARY must be canonical")
		}
		payloadFile := os.Getenv("VELA_VALIDATION_PAYLOAD_FILE")
		if payloadFile == "" || !filepath.IsAbs(payloadFile) || filepath.Clean(payloadFile) != payloadFile {
			return nil, errors.New("VELA_VALIDATION_PAYLOAD_FILE must be canonical when caller override is used")
		}
		mounts = append(mounts,
			&runtimev1.Mount{ContainerPath: "/vela-validation-caller", HostPath: callerBinary, Readonly: true},
			&runtimev1.Mount{ContainerPath: "/vela-validation/payload", HostPath: payloadFile, Readonly: true},
		)
		command, arguments = []string{"/vela-validation-caller"}, nil
		envs = append(envs,
			&runtimev1.KeyValue{Key: "VELA_VALIDATION_STARTUP_SOCKET", Value: socketPath},
			&runtimev1.KeyValue{Key: "VELA_VALIDATION_PAYLOAD_FILE", Value: "/vela-validation/payload"},
		)
	}
	launcherBinary, err := os.Executable()
	if err != nil || !filepath.IsAbs(launcherBinary) || filepath.Clean(launcherBinary) != launcherBinary {
		return nil, errors.New("validation launcher executable path is invalid")
	}
	mounts = append(mounts,
		&runtimev1.Mount{ContainerPath: "/vela-validation/pidfd-offer", HostPath: launcherBinary, Readonly: true},
		&runtimev1.Mount{ContainerPath: "/vela-validation/offer", HostPath: workload.runtimeOffer.directory, Readonly: true},
	)
	if len(command) == 0 {
		return nil, errors.New("validation launcher requires an explicit runtime command")
	}
	runtimeCommand := append([]string(nil), command...)
	runtimeArgs := append([]string(nil), arguments...)
	command = []string{"/vela-validation/pidfd-offer"}
	arguments = append([]string{"--socket", workload.runtimeOffer.containerPath, "--"}, append(runtimeCommand, runtimeArgs...)...)
	envs = append(envs, &runtimev1.KeyValue{Key: "VELA_VALIDATION_PIDFD_OFFER", Value: "1"})
	containerConfig := &runtimev1.ContainerConfig{Metadata: &runtimev1.ContainerMetadata{Name: container.Name, Attempt: 1}, Image: &runtimev1.ImageSpec{Image: container.Image}, Command: command, Args: arguments, Envs: envs, Mounts: mounts, LogPath: "validation-runtime.log", Linux: &runtimev1.LinuxContainerConfig{SecurityContext: &runtimev1.LinuxContainerSecurityContext{NamespaceOptions: namespaces, RunAsUser: &runtimev1.Int64Value{Value: int64(uidValue)}, RunAsGroup: &runtimev1.Int64Value{Value: int64(gidValue)}, ReadonlyRootfs: false, NoNewPrivs: true, Capabilities: &runtimev1.Capability{DropCapabilities: []string{"ALL"}}}}}
	// Create the observer endpoint before any target executable can start. This
	// preserves the creation-time ordering required by the launcher contract;
	// the validation observer is still explicitly separate from production
	// Runtime/Worker ancestry.
	observer, observerPID, observerEnd, err := startObserver(uid, gid)
	if err != nil {
		return nil, err
	}
	workload.observer, workload.observerPID, workload.observerEnd = observer, observerPID, observerEnd
	created, err := workload.runtime.CreateContainer(ctx, &runtimev1.CreateContainerRequest{PodSandboxId: sandbox.PodSandboxId, Config: containerConfig, SandboxConfig: sandboxConfig})
	if err != nil {
		return nil, fmt.Errorf("create validation container: %w", err)
	}
	workload.target.ContainerID = created.ContainerId
	workload.target.PodUID, _ = uuid.Parse(podUID)
	workload.target.PodNamespace, workload.target.PodName, workload.target.ContainerName, workload.target.ContainerAttempt = pod.Namespace, pod.Name, container.Name, 1
	if err := workload.target.Validate(); err != nil {
		return nil, err
	}
	if _, err := workload.runtime.StartContainer(ctx, &runtimev1.StartContainerRequest{ContainerId: created.ContainerId}); err != nil {
		return nil, fmt.Errorf("start validation container: %w", err)
	}
	worker, err := workload.runtimeOffer.accept(ctx, workload.tasks, created.ContainerId)
	if err != nil {
		return nil, err
	}
	workload.runtimePIDFD = worker
	workerCommand := splitValidationCommand(getenvDefault("VELA_VALIDATION_WORKER_COMMAND", "/bin/sleep 3600"))
	if len(workerCommand) == 0 {
		return nil, errors.New("VELA_VALIDATION_WORKER_COMMAND is empty")
	}
	workerRuntimeCommand := append([]string(nil), workerCommand...)
	workerCommand = []string{"/vela-validation/pidfd-offer"}
	workerArguments := append([]string{"--socket", workload.workerOffer.containerPath, "--"}, workerRuntimeCommand...)
	workerJournalSocket := podContainerEnvValue(pod, "stage-worker-agent", "VELA_WORKER_JOURNAL_SOCKET")
	if workerJournalSocket == "" || !filepath.IsAbs(workerJournalSocket) || filepath.Clean(workerJournalSocket) != workerJournalSocket {
		return nil, errors.New("signed Worker Pod does not provide a canonical Worker journal socket")
	}
	workerJournalDirectory := filepath.Dir(workerJournalSocket)
	if workerJournalDirectory == "/" || filepath.Dir(workerJournalDirectory) == "/" {
		return nil, errors.New("worker journal socket parent directory is too broad")
	}
	if err := os.MkdirAll(workerJournalDirectory, 0o755); err != nil {
		return nil, fmt.Errorf("create Worker journal socket directory: %w", err)
	}
	if _, err := securefile.ResolveTrustedDirectory(workerJournalDirectory); err != nil {
		return nil, fmt.Errorf("validate Worker journal socket directory: %w", err)
	}
	brokerSocket := getenvDefault("VELA_VALIDATION_PIDFD_BROKER_SOCKET", "/run/vela/pidfd-broker.sock")
	if !filepath.IsAbs(brokerSocket) || filepath.Clean(brokerSocket) != brokerSocket || brokerSocket == "/" {
		return nil, errors.New("VELA_VALIDATION_PIDFD_BROKER_SOCKET must be canonical")
	}
	if podContainerEnvValue(pod, "stage-worker-agent", "VELA_WORKER_JOURNAL_PIDFD_BROKER_SOCKET") != brokerSocket {
		return nil, errors.New("signed Worker Pod does not bind the configured pidfd broker socket")
	}
	brokerDirectory := filepath.Dir(brokerSocket)
	if brokerDirectory == "/" || filepath.Dir(brokerDirectory) == "/" {
		return nil, errors.New("pidfd broker socket parent directory is too broad")
	}
	if _, err := securefile.ResolveTrustedDirectory(brokerDirectory); err != nil {
		return nil, fmt.Errorf("validate pidfd broker socket directory: %w", err)
	}
	brokerInfo, err := os.Stat(brokerSocket)
	if err != nil || brokerInfo.Mode()&os.ModeSocket == 0 {
		return nil, errors.New("validation pidfd broker socket is not available")
	}
	workerMounts := []*runtimev1.Mount{
		{ContainerPath: "/vela-validation/pidfd-offer", HostPath: launcherBinary, Readonly: true},
		{ContainerPath: "/vela-validation/offer", HostPath: workload.workerOffer.directory, Readonly: true},
		{ContainerPath: workerJournalDirectory, HostPath: workerJournalDirectory, Readonly: true},
		{ContainerPath: brokerDirectory, HostPath: brokerDirectory, Readonly: true},
	}
	workerEnvs := []*runtimev1.KeyValue{
		{Key: "VELA_VALIDATION_PIDFD_OFFER", Value: "1"},
		{Key: "VELA_WORKER_JOURNAL_SOCKET", Value: workerJournalSocket},
		{Key: "VELA_WORKER_JOURNAL_PIDFD_BROKER_SOCKET", Value: brokerSocket},
	}
	workerConfig := &runtimev1.ContainerConfig{Metadata: &runtimev1.ContainerMetadata{Name: "stage-worker-agent", Attempt: 1}, Image: &runtimev1.ImageSpec{Image: workerImage}, Command: workerCommand, Args: workerArguments, Envs: workerEnvs, Mounts: workerMounts, LogPath: "validation-worker.log", Linux: &runtimev1.LinuxContainerConfig{SecurityContext: &runtimev1.LinuxContainerSecurityContext{NamespaceOptions: namespaces, RunAsUser: &runtimev1.Int64Value{Value: int64(uidValue)}, RunAsGroup: &runtimev1.Int64Value{Value: int64(gidValue)}, ReadonlyRootfs: false, NoNewPrivs: true, Capabilities: &runtimev1.Capability{DropCapabilities: []string{"ALL"}}}}}
	workerCreated, err := workload.runtime.CreateContainer(ctx, &runtimev1.CreateContainerRequest{PodSandboxId: sandbox.PodSandboxId, Config: workerConfig, SandboxConfig: sandboxConfig})
	if err != nil {
		return nil, fmt.Errorf("create validation Worker container: %w", err)
	}
	workload.workerTarget = nodeagent.RuntimeContainerTarget{ContainerID: workerCreated.ContainerId, SandboxID: sandbox.PodSandboxId, PodUID: workload.target.PodUID, PodNamespace: pod.Namespace, PodName: pod.Name, ContainerName: "stage-worker-agent", ContainerAttempt: 1}
	if err := workload.workerTarget.Validate(); err != nil {
		return nil, err
	}
	if _, err := workload.runtime.StartContainer(ctx, &runtimev1.StartContainerRequest{ContainerId: workerCreated.ContainerId}); err != nil {
		return nil, fmt.Errorf("start validation Worker container: %w", err)
	}
	worker, err = workload.workerOffer.accept(ctx, workload.tasks, workerCreated.ContainerId)
	if err != nil {
		return nil, err
	}
	workload.workerPIDFD = worker
	if getenvDefault("VELA_VALIDATION_SCENARIO", "") == "caller-replacement" {
		// Deliberately hand Node a live pidfd for the wrong process while
		// retaining the real CRI Worker target. The adapter must reject the
		// replacement by kernel identity and still clean the exact workload.
		fd, err := unix.FcntlInt(observerPID.Fd(), unix.F_DUPFD_CLOEXEC, 0)
		if err != nil {
			return nil, fmt.Errorf("duplicate replacement pidfd: %w", err)
		}
		_ = workload.workerPIDFD.Close()
		workload.workerPIDFD = os.NewFile(uintptr(fd), "validation-replacement-pidfd")
	}
	cleanup = false
	return workload, nil
}

func podContainerEnvValue(pod *corev1.Pod, containerName, key string) string {
	if pod == nil || containerName == "" || key == "" {
		return ""
	}
	for _, container := range pod.Spec.Containers {
		if container.Name != containerName {
			continue
		}
		for _, env := range container.Env {
			if env.Name == key && env.ValueFrom == nil {
				return env.Value
			}
		}
	}
	return ""
}

func ensureImage(ctx context.Context, client runtimev1.ImageServiceClient, reference string) error {
	status, err := client.ImageStatus(ctx, &runtimev1.ImageStatusRequest{Image: &runtimev1.ImageSpec{Image: reference}})
	if err == nil && status.GetImage().GetId() != "" {
		return nil
	}
	_, err = client.PullImage(ctx, &runtimev1.PullImageRequest{Image: &runtimev1.ImageSpec{Image: reference}})
	return err
}

func dialCRI(ctx context.Context, socket string) (*grpc.ClientConn, error) {
	if !filepath.IsAbs(socket) || filepath.Clean(socket) != socket {
		return nil, errors.New("validation CRI socket is not canonical")
	}
	info, err := os.Stat(socket)
	if err != nil || info.Mode()&os.ModeSocket == 0 {
		return nil, errors.New("validation CRI socket is not a socket")
	}
	dialer := func(ctx context.Context, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "unix", socket)
	}
	connection, err := grpc.NewClient("passthrough:///vela-validation-cri", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(dialer), grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(1<<20), grpc.MaxCallSendMsgSize(64<<10)))
	if err != nil {
		return nil, err
	}
	connection.Connect()
	return connection, nil
}

func startObserver(uid, gid uint32) (*exec.Cmd, *os.File, *os.File, error) {
	path := os.Getenv("VELA_VALIDATION_OBSERVER_PATH")
	if path == "" || !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return nil, nil, nil, errors.New("VELA_VALIDATION_OBSERVER_PATH must be canonical")
	}
	pair, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_SEQPACKET|unix.SOCK_CLOEXEC, 0)
	if err != nil {
		return nil, nil, nil, err
	}
	nodeSide, childSide := os.NewFile(uintptr(pair[0]), "validation-observer-node"), os.NewFile(uintptr(pair[1]), "validation-observer-child")
	observerPID := -1
	if uid == 0 || gid == 0 {
		return nil, nil, nil, errors.New("validation observer credentials must be non-root")
	}
	command := exec.Command(path, "--custody-fd3", strconv.FormatUint(uint64(uid), 10), strconv.FormatUint(uint64(gid), 10), "/bin/sleep", "3600")
	command.ExtraFiles = []*os.File{childSide}
	command.SysProcAttr = &syscall.SysProcAttr{PidFD: &observerPID}
	command.Stdout, command.Stderr = io.Discard, io.Discard
	if err := command.Start(); err != nil {
		_ = nodeSide.Close()
		_ = childSide.Close()
		return nil, nil, nil, err
	}
	_ = childSide.Close()
	if observerPID < 0 {
		_ = command.Process.Kill()
		_ = command.Wait()
		_ = nodeSide.Close()
		return nil, nil, nil, errors.New("observer did not provide a pidfd")
	}
	return command, os.NewFile(uintptr(observerPID), "validation-observer-pidfd"), nodeSide, nil
}

func (workload *validationWorkload) cleanup(ctx context.Context) error {
	if workload == nil {
		return nil
	}
	var cleanupErr error
	if workload.observerPID != nil {
		if err := unix.PidfdSendSignal(int(workload.observerPID.Fd()), unix.SIGKILL, nil, 0); err != nil && !errors.Is(err, unix.ESRCH) {
			cleanupErr = errors.Join(cleanupErr, fmt.Errorf("kill validation observer: %w", err))
		}
		cleanupErr = errors.Join(cleanupErr, workload.observerPID.Close())
	}
	if workload.observer != nil {
		if err := workload.observer.Wait(); err != nil {
			// SIGKILL is the expected observer termination path.
			var exitErr *exec.ExitError
			if !errors.As(err, &exitErr) {
				cleanupErr = errors.Join(cleanupErr, fmt.Errorf("wait validation observer: %w", err))
			}
		}
	}
	if workload.workerPIDFD != nil {
		cleanupErr = errors.Join(cleanupErr, workload.workerPIDFD.Close())
	}
	if workload.runtimePIDFD != nil {
		cleanupErr = errors.Join(cleanupErr, workload.runtimePIDFD.Close())
	}
	if workload.runtime != nil && workload.workerTarget.ContainerID != "" {
		stopCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if _, err := workload.runtime.StopContainer(stopCtx, &runtimev1.StopContainerRequest{ContainerId: workload.workerTarget.ContainerID, Timeout: 5}); err != nil {
			cleanupErr = errors.Join(cleanupErr, fmt.Errorf("stop validation Worker container: %w", err))
		}
		if _, err := workload.runtime.RemoveContainer(stopCtx, &runtimev1.RemoveContainerRequest{ContainerId: workload.workerTarget.ContainerID}); err != nil {
			cleanupErr = errors.Join(cleanupErr, fmt.Errorf("remove validation Worker container: %w", err))
		}
	}
	if workload.runtime != nil && workload.target.ContainerID != "" {
		stopCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if _, err := workload.runtime.StopContainer(stopCtx, &runtimev1.StopContainerRequest{ContainerId: workload.target.ContainerID, Timeout: 5}); err != nil {
			cleanupErr = errors.Join(cleanupErr, fmt.Errorf("stop validation container: %w", err))
		}
		if _, err := workload.runtime.RemoveContainer(stopCtx, &runtimev1.RemoveContainerRequest{ContainerId: workload.target.ContainerID}); err != nil {
			cleanupErr = errors.Join(cleanupErr, fmt.Errorf("remove validation container: %w", err))
		}
	}
	if workload.runtime != nil && workload.target.SandboxID != "" {
		stopCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if _, err := workload.runtime.StopPodSandbox(stopCtx, &runtimev1.StopPodSandboxRequest{PodSandboxId: workload.target.SandboxID}); err != nil {
			cleanupErr = errors.Join(cleanupErr, fmt.Errorf("stop validation sandbox: %w", err))
		}
		if _, err := workload.runtime.RemovePodSandbox(stopCtx, &runtimev1.RemovePodSandboxRequest{PodSandboxId: workload.target.SandboxID}); err != nil {
			cleanupErr = errors.Join(cleanupErr, fmt.Errorf("remove validation sandbox: %w", err))
		}
	}
	if workload.observerEnd != nil {
		cleanupErr = errors.Join(cleanupErr, workload.observerEnd.Close())
	}
	for _, offer := range []*pidfdOfferListener{workload.runtimeOffer, workload.workerOffer} {
		cleanupErr = errors.Join(cleanupErr, offer.Close())
	}
	if workload.connection != nil {
		cleanupErr = errors.Join(cleanupErr, workload.connection.Close())
	}
	if workload.volumeRoot != "" {
		cleanupErr = errors.Join(cleanupErr, os.RemoveAll(workload.volumeRoot))
	}
	return cleanupErr
}

func validationMounts(pod *corev1.Pod, container *corev1.Container, root, startupHost, startupContainer string) ([]*runtimev1.Mount, error) {
	volumes := make(map[string]string)
	for _, volume := range pod.Spec.Volumes {
		switch {
		case volume.HostPath != nil:
			volumes[volume.Name] = volume.HostPath.Path
		case volume.EmptyDir != nil:
			path := filepath.Join(root, volume.Name)
			if err := os.MkdirAll(path, 0o755); err != nil {
				return nil, err
			}
			volumes[volume.Name] = path
		default:
			return nil, fmt.Errorf("validation launcher cannot materialize volume %q", volume.Name)
		}
	}
	mounts := make([]*runtimev1.Mount, 0, len(container.VolumeMounts)+1)
	for _, mount := range container.VolumeMounts {
		host, ok := volumes[mount.Name]
		if !ok || filepath.Clean(mount.MountPath) != mount.MountPath {
			return nil, fmt.Errorf("validation launcher cannot resolve volume mount %q", mount.Name)
		}
		mounts = append(mounts, &runtimev1.Mount{ContainerPath: mount.MountPath, HostPath: host, Readonly: mount.ReadOnly})
	}
	mounts = append(mounts, &runtimev1.Mount{ContainerPath: startupContainer, HostPath: startupHost, Readonly: true})
	return mounts, nil
}

func containerCredentials(pod corev1.Pod, container corev1.Container) (uint32, uint32) {
	uid, gid := int64(0), int64(0)
	if pod.Spec.SecurityContext != nil {
		if pod.Spec.SecurityContext.RunAsUser != nil {
			uid = *pod.Spec.SecurityContext.RunAsUser
		}
		if pod.Spec.SecurityContext.RunAsGroup != nil {
			gid = *pod.Spec.SecurityContext.RunAsGroup
		}
	}
	if container.SecurityContext != nil {
		if container.SecurityContext.RunAsUser != nil {
			uid = *container.SecurityContext.RunAsUser
		}
		if container.SecurityContext.RunAsGroup != nil {
			gid = *container.SecurityContext.RunAsGroup
		}
	}
	if uid <= 0 || uid > 1<<32-2 || gid <= 0 || gid > 1<<32-2 {
		return 0, 0
	}
	return uint32(uid), uint32(gid)
}

func containsEnv(values []*runtimev1.KeyValue, name string) bool {
	for _, value := range values {
		if value.Key == name {
			return true
		}
	}
	return false
}

func strictJSON(wire []byte, value any) error {
	if err := strictjson.RejectDuplicateKeys(wire); err != nil {
		return err
	}
	decoder := json.NewDecoder(bytes.NewReader(wire))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(value); err != nil {
		return err
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		return errors.New("trailing JSON data")
	}
	return nil
}

func recvFrame(ctx context.Context, fd int) ([]byte, error) {
	if ctx == nil {
		return nil, context.Canceled
	}
	buffer := make([]byte, 64<<10)
	oob := make([]byte, unix.CmsgSpace(16*4))
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		pollfds := []unix.PollFd{{Fd: int32(fd), Events: unix.POLLIN | unix.POLLHUP}}
		if _, err := unix.Poll(pollfds, 100); err != nil {
			if errors.Is(err, unix.EINTR) {
				continue
			}
			return nil, err
		}
		if pollfds[0].Revents&(unix.POLLIN|unix.POLLHUP) != 0 {
			break
		}
	}
	n, oobn, flags, _, err := unix.Recvmsg(fd, buffer, oob, 0)
	if err != nil {
		return nil, err
	}
	if n == 0 {
		return nil, io.EOF
	}
	if flags&(unix.MSG_TRUNC|unix.MSG_CTRUNC) != 0 || oobn != 0 {
		return nil, errors.New("unexpected launcher ancillary data or truncation")
	}
	return buffer[:n], nil
}

func sendFrameWithRights(fd int, wire []byte, rights []int) error {
	if len(wire) == 0 || len(wire) > 64<<10 {
		return errors.New("launcher frame is too large")
	}
	var oob []byte
	if len(rights) > 0 {
		oob = unix.UnixRights(rights...)
	}
	n, err := unix.SendmsgN(fd, wire, oob, nil, 0)
	if err != nil {
		return err
	}
	if n != len(wire) {
		return io.ErrShortWrite
	}
	return nil
}

func getenvDefault(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}

func writeValidationReceipt(workload *validationWorkload, runErr, cleanupErr error) error {
	if workload == nil || workload.receiptPath == "" {
		return errors.New("validation receipt path is unavailable")
	}
	receipt := validationReceipt{Version: 1, ValidationOnly: true, Scenario: workload.scenario,
		StartedAt: workload.startedAt, FinishedAt: time.Now().UTC(), Target: workload.target, WorkerTarget: workload.workerTarget, FDCount: workload.handedOff,
		Outcome: "completed"}
	if runErr != nil {
		receipt.Outcome = "failed"
		receipt.Error = runErr.Error()
	}
	if cleanupErr != nil {
		receipt.CleanupError = cleanupErr.Error()
	}
	wire, err := json.MarshalIndent(receipt, "", "  ")
	if err != nil {
		return err
	}
	directory := filepath.Dir(workload.receiptPath)
	temporary, err := os.CreateTemp(directory, ".vela-validation-receipt-*")
	if err != nil {
		return err
	}
	temporaryName := temporary.Name()
	defer func() { _ = os.Remove(temporaryName) }()
	if err := temporary.Chmod(0o600); err != nil {
		_ = temporary.Close()
		return err
	}
	if _, err := temporary.Write(wire); err != nil {
		_ = temporary.Close()
		return err
	}
	if err := temporary.Sync(); err != nil {
		_ = temporary.Close()
		return err
	}
	if err := temporary.Close(); err != nil {
		return err
	}
	return os.Rename(temporaryName, workload.receiptPath)
}

func writeStandaloneValidationFailureReceipt(path, scenario string, runErr error) error {
	if !filepath.IsAbs(path) || filepath.Clean(path) != path || path == "/" {
		return errors.New("validation receipt path is unavailable")
	}
	now := time.Now().UTC()
	receipt := validationReceipt{Version: 1, ValidationOnly: true, Scenario: scenario, StartedAt: now, FinishedAt: now, Outcome: "failed", Error: runErr.Error(), FDCount: 0}
	wire, err := json.MarshalIndent(receipt, "", "  ")
	if err != nil {
		return err
	}
	directory := filepath.Dir(path)
	temporary, err := os.CreateTemp(directory, ".vela-validation-receipt-*")
	if err != nil {
		return err
	}
	temporaryName := temporary.Name()
	defer func() { _ = os.Remove(temporaryName) }()
	if err := temporary.Chmod(0o600); err != nil {
		_ = temporary.Close()
		return err
	}
	if _, err := temporary.Write(wire); err != nil {
		_ = temporary.Close()
		return err
	}
	if err := temporary.Sync(); err != nil {
		_ = temporary.Close()
		return err
	}
	if err := temporary.Close(); err != nil {
		return err
	}
	return os.Rename(temporaryName, path)
}

// splitValidationCommand deliberately accepts only a simple argv string. This
// helper is validation-only; shell expansion and pipelines would make the
// process identity ambiguous and cannot be used as startup evidence.
func splitValidationCommand(value string) []string {
	return strings.Fields(value)
}

func fatal(message string) {
	_, _ = fmt.Fprintln(os.Stderr, message)
	os.Exit(1)
}
