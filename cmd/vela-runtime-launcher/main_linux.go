//go:build linux

// vela-runtime-launcher is the production Runtime/Worker launcher. It is
// deliberately a separate executable from vela-validation-runtime-launcher.
// The helper is root-owned, validates the complete signed Pod supplied by
// Node, creates both CRI containers in one sandbox, and obtains the original
// process pidfds from a wrapper running at each container's exec boundary.
package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"

	tasksapi "github.com/containerd/containerd/api/services/tasks/v1"
	"github.com/google/uuid"
	"github.com/vivym/vela/internal/modelruntime"
	"github.com/vivym/vela/internal/nodeagent"
	"github.com/vivym/vela/internal/runtimechannel"
	"github.com/vivym/vela/internal/securefile"
	"github.com/vivym/vela/internal/strictjson"
	"golang.org/x/sys/unix"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
	runtimev1 "k8s.io/cri-api/pkg/apis/runtime/v1"
)

const (
	protocolVersion       = 2
	pidfdOfferFrame       = "vela-runtime-pidfd-v1"
	defaultStartupTimeout = 2 * time.Minute
	maxStartupTimeout     = 15 * time.Minute
)

var errPidfdOfferSocketReplaced = errors.New("pidfd offer socket was replaced")

var pinnedImagePattern = regexp.MustCompile(`@sha256:[0-9a-f]{64}$`)
var runtimeContainerIDPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.:-]{0,199}$`)

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

type productionWorkload struct {
	connection  *grpc.ClientConn
	runtime     runtimev1.RuntimeServiceClient
	image       runtimev1.ImageServiceClient
	tasks       tasksapi.TasksClient
	target      nodeagent.RuntimeContainerTarget
	worker      nodeagent.RuntimeContainerTarget
	runtimeFD   *os.File
	workerFD    *os.File
	observer    *exec.Cmd
	observerFD  *os.File
	observerEnd *os.File
	// observerChild is retained until the observer process is started. The
	// custody socketpair is created before any target executable starts.
	observerChild *os.File
	observerDir   string
	initIDs       []string
	offers        []*pidfdOffer
	volumeRoot    string
	startupPath   string
	kube          kubernetes.Interface
	uid, gid      uint32
}

func main() {
	if len(os.Args) > 1 && os.Args[1] == "--vela-runtime-observer" {
		if err := runAttachedObserver(os.Args[2:]); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(2)
		}
		return
	}
	if len(os.Args) > 1 && os.Args[1] == "--vela-runtime-pidfd-offer" {
		if err := runPIDFDOffer(os.Args[2:]); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		return
	}
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() (resultErr error) {
	if os.Geteuid() != 0 {
		return errors.New("vela-runtime-launcher must run as root")
	}
	fd := 3
	if value := os.Getenv("VELA_RUNTIME_LAUNCHER_CONTROL_FD"); value != "" {
		parsed, err := strconv.Atoi(value)
		if err != nil || parsed < 0 {
			return errors.New("VELA_RUNTIME_LAUNCHER_CONTROL_FD is invalid")
		}
		fd = parsed
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	startupTimeout, err := runtimeLauncherStartupTimeout()
	if err != nil {
		return err
	}
	startupCtx, cancelStartup := context.WithTimeout(ctx, startupTimeout)
	defer cancelStartup()
	control := os.NewFile(uintptr(fd), "runtime-launcher-control")
	if control == nil {
		return errors.New("runtime launcher control fd is unavailable")
	}
	defer control.Close()
	packet, err := recvFrame(startupCtx, fd)
	if err != nil {
		return fmt.Errorf("receive runtime launcher request: %w", err)
	}
	var request launcherRequest
	if err := strictJSON(packet, &request); err != nil || request.Version != protocolVersion {
		return errors.New("runtime launcher request schema is invalid")
	}
	if len(request.Manifest) == 0 || len(request.ExpectedPod) == 0 || sha256.Sum256(request.ExpectedPod) != request.ExpectedPodDigest {
		return errors.New("runtime launcher expected Pod digest mismatch")
	}
	var manifest modelruntime.LaunchManifest
	if err := strictJSON(request.Manifest, &manifest); err != nil {
		return fmt.Errorf("decode runtime launcher manifest: %w", err)
	}
	canonicalManifest, err := modelruntime.EncodeLaunchManifest(manifest)
	if err != nil || !bytes.Equal(canonicalManifest, request.Manifest) {
		return errors.New("runtime launcher manifest is not canonical")
	}
	if !canonicalAbsolute(request.StartupSocket) {
		return errors.New("runtime launcher startup socket is not canonical")
	}
	var pod corev1.Pod
	if err := strictJSON(request.ExpectedPod, &pod); err != nil {
		return fmt.Errorf("decode expected Pod: %w", err)
	}
	workload, err := launchProductionWorkload(startupCtx, request.StartupSocket, &pod)
	if err != nil {
		return err
	}
	defer func() { resultErr = errors.Join(resultErr, workload.Close()) }()
	cancelStartup()
	wire, err := json.Marshal(launcherReply{Version: protocolVersion, ValidationOnly: false, Target: workload.target, WorkerTarget: workload.worker, FDCount: 4})
	if err != nil {
		return err
	}
	if err := sendFrameWithRights(fd, wire, []int{int(workload.runtimeFD.Fd()), int(workload.workerFD.Fd()), int(workload.observerFD.Fd()), int(workload.observerEnd.Fd())}); err != nil {
		return fmt.Errorf("send runtime launcher handoff: %w", err)
	}
	// Node owns duplicated descriptors after the SCM_RIGHTS transfer. The
	// helper closes its observer endpoint and waits for Node to close control;
	// that keeps the CRI workload and observer tied to this invocation.
	_ = workload.observerEnd.Close()
	workload.observerEnd = nil
	for {
		if _, err := recvFrame(ctx, fd); err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, unix.ECONNRESET) || errors.Is(err, unix.ENOTCONN) {
				return nil
			}
			return fmt.Errorf("runtime launcher control channel: %w", err)
		}
		return errors.New("runtime launcher received an unexpected second request")
	}
}

func runtimeLauncherStartupTimeout() (time.Duration, error) {
	value := strings.TrimSpace(os.Getenv("VELA_RUNTIME_LAUNCHER_STARTUP_TIMEOUT"))
	if value == "" {
		return defaultStartupTimeout, nil
	}
	timeout, err := time.ParseDuration(value)
	if err != nil || timeout <= 0 || timeout > maxStartupTimeout {
		return 0, errors.New("VELA_RUNTIME_LAUNCHER_STARTUP_TIMEOUT must be positive and at most 15m")
	}
	return timeout, nil
}

func launchProductionWorkload(ctx context.Context, startupSocket string, pod *corev1.Pod) (*productionWorkload, error) {
	if pod == nil || pod.UID == "" || uuidMust(string(pod.UID)) == uuid.Nil || pod.Namespace == "" || pod.Name == "" {
		return nil, errors.New("signed Pod identity is invalid")
	}
	runtimeContainer, workerContainer, err := requiredContainers(pod)
	if err != nil {
		return nil, err
	}
	// Reject unsupported main-container fields and resource requests before CRI,
	// image pulls, secret materialization, or init execution. In particular, a
	// Fleet DRA Pod cannot be implemented by this direct CRI launcher.
	if len(pod.Spec.ResourceClaims) != 0 {
		return nil, errors.New("signed Pod resource claims are unsupported by direct CRI launch")
	}
	for _, container := range []*corev1.Container{runtimeContainer, workerContainer} {
		if err := validateProductionContainer(container); err != nil {
			return nil, fmt.Errorf("validate signed container %s before launch: %w", container.Name, err)
		}
		if _, err := productionResources(container.Resources); err != nil {
			return nil, fmt.Errorf("validate signed container %s resources before launch: %w", container.Name, err)
		}
	}
	uid, gid, err := productionCredentials(*pod, *runtimeContainer, *workerContainer)
	if err != nil {
		return nil, err
	}
	criSocket := os.Getenv("VELA_RUNTIME_LAUNCHER_CRI_SOCKET")
	if !canonicalAbsolute(criSocket) {
		return nil, errors.New("VELA_RUNTIME_LAUNCHER_CRI_SOCKET is required and canonical")
	}
	connection, err := dialCRI(ctx, criSocket)
	if err != nil {
		return nil, err
	}
	kube, err := loadKubernetesClient()
	if err != nil {
		_ = connection.Close()
		return nil, err
	}
	workload := &productionWorkload{connection: connection, runtime: runtimev1.NewRuntimeServiceClient(connection), image: runtimev1.NewImageServiceClient(connection), tasks: tasksapi.NewTasksClient(connection), startupPath: startupSocket, kube: kube, uid: uid, gid: gid}
	observerNode, observerChild, err := newProductionObserverSocketpair()
	if err != nil {
		_ = workload.Close()
		return nil, fmt.Errorf("create production observer custody socketpair: %w", err)
	}
	workload.observerEnd, workload.observerChild = observerNode, observerChild
	if err := workload.prepareImages(ctx, runtimeContainer.Image); err != nil {
		_ = workload.Close()
		return nil, err
	}
	if err := workload.prepareImages(ctx, workerContainer.Image); err != nil {
		_ = workload.Close()
		return nil, err
	}
	// Init containers are part of the signed Pod contract and execute before
	// Runtime/Worker.  Resolve every digest up front so a late pull or digest
	// mismatch cannot leave a partially-created sandbox behind.
	for _, container := range pod.Spec.InitContainers {
		if err := workload.prepareImages(ctx, container.Image); err != nil {
			_ = workload.Close()
			return nil, err
		}
	}
	sandboxImage := os.Getenv("VELA_RUNTIME_LAUNCHER_SANDBOX_IMAGE")
	if !pinnedImagePattern.MatchString(sandboxImage) {
		_ = workload.Close()
		return nil, errors.New("VELA_RUNTIME_LAUNCHER_SANDBOX_IMAGE must be digest-pinned")
	}
	if err := workload.prepareImages(ctx, sandboxImage); err != nil {
		_ = workload.Close()
		return nil, err
	}
	root, err := os.MkdirTemp("/run", "vela-runtime-launcher-")
	if err != nil {
		_ = workload.Close()
		return nil, err
	}
	workload.volumeRoot = root
	logDirectory := filepath.Join(root, "logs")
	if err := os.Mkdir(logDirectory, 0o750); err != nil {
		return nil, errors.Join(err, workload.Close())
	}
	workload.offers = make([]*pidfdOffer, 0, 2)
	for _, name := range []string{"runtime", "worker"} {
		offer, offerErr := newPIDFDOffer(root, name, uid, gid)
		if offerErr != nil {
			_ = workload.Close()
			return nil, offerErr
		}
		workload.offers = append(workload.offers, offer)
	}
	namespaceOptions := &runtimev1.NamespaceOption{Network: runtimev1.NamespaceMode_NODE, Pid: runtimev1.NamespaceMode_CONTAINER, Ipc: runtimev1.NamespaceMode_POD}
	sandboxSecurity, err := productionSandboxSecurityContext(pod.Spec.SecurityContext, namespaceOptions, uid, gid)
	if err != nil {
		_ = workload.Close()
		return nil, err
	}
	sandboxConfig := &runtimev1.PodSandboxConfig{Metadata: &runtimev1.PodSandboxMetadata{Name: pod.Name, Namespace: pod.Namespace, Uid: string(pod.UID), Attempt: 1}, LogDirectory: logDirectory, Linux: &runtimev1.LinuxPodSandboxConfig{CgroupParent: getenvDefault("VELA_RUNTIME_LAUNCHER_CGROUP_PARENT", "kubepods.slice"), SecurityContext: sandboxSecurity}}
	sandbox, err := workload.runtime.RunPodSandbox(ctx, &runtimev1.RunPodSandboxRequest{Config: sandboxConfig, RuntimeHandler: getenvDefault("VELA_RUNTIME_LAUNCHER_RUNTIME_HANDLER", "runc")})
	if err != nil {
		_ = workload.Close()
		return nil, fmt.Errorf("run runtime startup PodSandbox: %w", err)
	}
	workload.target.SandboxID = sandbox.GetPodSandboxId()
	workload.target.PodUID = uuidMust(string(pod.UID))
	workload.target.PodNamespace, workload.target.PodName = pod.Namespace, pod.Name
	if err := workload.createAndStartInitContainers(ctx, sandboxConfig, namespaceOptions, pod, uid, gid); err != nil {
		_ = workload.Close()
		return nil, err
	}
	if err := workload.createAndStart(ctx, sandboxConfig, namespaceOptions, pod, runtimeContainer, uid, gid, workload.offers[0], true); err != nil {
		_ = workload.Close()
		return nil, err
	}
	observer, observerFD, observerEnd, err := startProductionObserver(uid, gid, workload.runtimeFD, workload.observerEnd, workload.observerChild)
	if err != nil {
		_ = workload.Close()
		return nil, err
	}
	workload.observerEnd, workload.observerChild = nil, nil
	workload.observer, workload.observerFD, workload.observerEnd = observer, observerFD, observerEnd
	if err := workload.createAndStart(ctx, sandboxConfig, namespaceOptions, pod, workerContainer, uid, gid, workload.offers[1], false); err != nil {
		_ = workload.Close()
		return nil, err
	}
	return workload, nil
}

func (workload *productionWorkload) prepareImages(ctx context.Context, reference string) error {
	if !pinnedImagePattern.MatchString(reference) {
		return fmt.Errorf("image %q is not digest-pinned", reference)
	}
	status, err := workload.image.ImageStatus(ctx, &runtimev1.ImageStatusRequest{Image: &runtimev1.ImageSpec{Image: reference}})
	if err != nil || status.GetImage().GetId() == "" {
		if _, pullErr := workload.image.PullImage(ctx, &runtimev1.PullImageRequest{Image: &runtimev1.ImageSpec{Image: reference}}); pullErr != nil {
			return fmt.Errorf("prepare image %q: %w", reference, pullErr)
		}
		status, err = workload.image.ImageStatus(ctx, &runtimev1.ImageStatusRequest{Image: &runtimev1.ImageSpec{Image: reference}})
	}
	if err != nil || status.GetImage().GetId() == "" {
		return fmt.Errorf("inspect image %q: %w", reference, err)
	}
	digest := reference[strings.LastIndex(reference, "@")+1:]
	if !imageDigestMatches(status.GetImage().GetId(), status.GetImage().GetRepoDigests(), digest) {
		return fmt.Errorf("image %q resolved to an unexpected digest", reference)
	}
	return nil
}

func productionResources(requirements corev1.ResourceRequirements) (*runtimev1.LinuxContainerResources, error) {
	if len(requirements.Claims) != 0 {
		return nil, errors.New("signed container resource claims are unsupported by direct CRI launch")
	}
	result := &runtimev1.LinuxContainerResources{}
	if quantity, ok := requirements.Limits[corev1.ResourceCPU]; ok {
		milli := quantity.MilliValue()
		if milli <= 0 {
			return nil, errors.New("signed CPU limit is invalid")
		}
		result.CpuPeriod = 100000
		result.CpuQuota = milli * result.CpuPeriod / 1000
	}
	if quantity, ok := requirements.Requests[corev1.ResourceCPU]; ok {
		milli := quantity.MilliValue()
		if milli <= 0 {
			return nil, errors.New("signed CPU request is invalid")
		}
		result.CpuShares = milli
	}
	if quantity, ok := requirements.Limits[corev1.ResourceMemory]; ok {
		bytes := quantity.Value()
		if bytes <= 0 {
			return nil, errors.New("signed memory limit is invalid")
		}
		result.MemoryLimitInBytes = bytes
	}
	for resourceName := range requirements.Limits {
		if resourceName != corev1.ResourceCPU && resourceName != corev1.ResourceMemory {
			return nil, fmt.Errorf("signed resource %q is unsupported", resourceName)
		}
	}
	for resourceName := range requirements.Requests {
		if resourceName != corev1.ResourceCPU && resourceName != corev1.ResourceMemory {
			return nil, fmt.Errorf("signed resource %q is unsupported", resourceName)
		}
	}
	return result, nil
}

func productionSecurityContext(source *corev1.SecurityContext, namespaceOptions *runtimev1.NamespaceOption, uid, gid uint32, privileged bool) (*runtimev1.LinuxContainerSecurityContext, error) {
	result := &runtimev1.LinuxContainerSecurityContext{NamespaceOptions: namespaceOptions, RunAsUser: &runtimev1.Int64Value{Value: int64(uid)}, RunAsGroup: &runtimev1.Int64Value{Value: int64(gid)}, Privileged: privileged, ReadonlyRootfs: true, NoNewPrivs: true, Capabilities: &runtimev1.Capability{DropCapabilities: []string{"ALL"}}}
	if source == nil {
		return result, nil
	}
	if source.RunAsNonRoot != nil && *source.RunAsNonRoot != (uid != 0) {
		return nil, errors.New("signed container non-root policy does not match credentials")
	}
	if source.RunAsUser != nil && *source.RunAsUser != int64(uid) || source.RunAsGroup != nil && *source.RunAsGroup != int64(gid) {
		return nil, errors.New("signed container credentials do not match Pod identity")
	}
	if source.Privileged != nil {
		if *source.Privileged != privileged {
			return nil, errors.New("signed container privilege does not match launcher policy")
		}
	}
	if source.ReadOnlyRootFilesystem != nil {
		result.ReadonlyRootfs = *source.ReadOnlyRootFilesystem
	}
	if source.AllowPrivilegeEscalation != nil {
		result.NoNewPrivs = !*source.AllowPrivilegeEscalation
	}
	if source.Capabilities != nil {
		result.Capabilities = &runtimev1.Capability{}
		for _, capability := range source.Capabilities.Add {
			result.Capabilities.AddCapabilities = append(result.Capabilities.AddCapabilities, string(capability))
		}
		for _, capability := range source.Capabilities.Drop {
			result.Capabilities.DropCapabilities = append(result.Capabilities.DropCapabilities, string(capability))
		}
	}
	if source.SeccompProfile != nil && source.SeccompProfile.Type != corev1.SeccompProfileTypeRuntimeDefault {
		return nil, errors.New("signed container seccomp profile is unsupported")
	}
	return result, nil
}

func productionSandboxSecurityContext(source *corev1.PodSecurityContext, namespaceOptions *runtimev1.NamespaceOption, uid, gid uint32) (*runtimev1.LinuxSandboxSecurityContext, error) {
	result := &runtimev1.LinuxSandboxSecurityContext{NamespaceOptions: namespaceOptions, RunAsUser: &runtimev1.Int64Value{Value: int64(uid)}, RunAsGroup: &runtimev1.Int64Value{Value: int64(gid)}}
	if source == nil {
		return result, nil
	}
	if source.RunAsUser != nil && *source.RunAsUser != int64(uid) || source.RunAsGroup != nil && *source.RunAsGroup != int64(gid) {
		return nil, errors.New("signed Pod credentials do not match launcher identity")
	}
	if source.RunAsNonRoot != nil && !*source.RunAsNonRoot {
		return nil, errors.New("signed Pod must require non-root containers")
	}
	if source.FSGroup != nil && *source.FSGroup != int64(gid) {
		return nil, errors.New("signed Pod fsGroup does not match launcher group")
	}
	if source.FSGroupChangePolicy != nil && *source.FSGroupChangePolicy != corev1.FSGroupChangeOnRootMismatch {
		return nil, errors.New("signed Pod fsGroupChangePolicy is unsupported")
	}
	if source.SeccompProfile != nil {
		if source.SeccompProfile.Type != corev1.SeccompProfileTypeRuntimeDefault {
			return nil, errors.New("signed Pod seccomp profile is unsupported")
		}
		result.Seccomp = &runtimev1.SecurityProfile{ProfileType: runtimev1.SecurityProfile_RuntimeDefault}
	}
	return result, nil
}

func (workload *productionWorkload) createAndStartInitContainers(ctx context.Context, sandboxConfig *runtimev1.PodSandboxConfig, namespaceOptions *runtimev1.NamespaceOption, pod *corev1.Pod, uid, gid uint32) error {
	for _, container := range pod.Spec.InitContainers {
		if err := workload.createAndWaitInit(ctx, sandboxConfig, namespaceOptions, pod, &container, uid, gid); err != nil {
			return err
		}
	}
	return nil
}

func (workload *productionWorkload) createAndWaitInit(ctx context.Context, sandboxConfig *runtimev1.PodSandboxConfig, namespaceOptions *runtimev1.NamespaceOption, pod *corev1.Pod, container *corev1.Container, uid, gid uint32) error {
	if container == nil {
		return errors.New("signed init container is nil")
	}
	command, args, err := productionInitCommand(container)
	if err != nil {
		return err
	}
	mounts, err := workload.productionMounts(ctx, pod, container, workload.volumeRoot, workload.startupPath, false)
	if err != nil {
		return err
	}
	envs, err := workload.productionEnvs(ctx, pod, container, workload.startupPath, false)
	if err != nil {
		return err
	}
	resources, err := productionResources(container.Resources)
	if err != nil {
		return err
	}
	security, err := productionSecurityContext(container.SecurityContext, namespaceOptions, 0, 0, false)
	if err != nil {
		return err
	}
	config := &runtimev1.ContainerConfig{Metadata: &runtimev1.ContainerMetadata{Name: container.Name, Attempt: 1}, Image: &runtimev1.ImageSpec{Image: container.Image}, Command: command, Args: args, Envs: envs, Mounts: mounts, LogPath: container.Name + ".log", Linux: &runtimev1.LinuxContainerConfig{Resources: resources, SecurityContext: security}}
	created, err := workload.runtime.CreateContainer(ctx, &runtimev1.CreateContainerRequest{PodSandboxId: workload.target.SandboxID, Config: config, SandboxConfig: sandboxConfig})
	if err != nil {
		return fmt.Errorf("create production init container %s: %w", container.Name, err)
	}
	id := created.GetContainerId()
	if !runtimeContainerIDPattern.MatchString(id) {
		return errors.New("CRI returned an invalid init container ID")
	}
	workload.initIDs = append(workload.initIDs, id)
	if _, err := workload.runtime.StartContainer(ctx, &runtimev1.StartContainerRequest{ContainerId: id}); err != nil {
		return fmt.Errorf("start production init container %s: %w", container.Name, err)
	}
	deadline := time.Now().Add(2 * time.Minute)
	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("wait for production init container %s: %w", container.Name, ctx.Err())
		default:
		}
		status, statusErr := workload.runtime.ContainerStatus(ctx, &runtimev1.ContainerStatusRequest{ContainerId: id})
		if statusErr != nil {
			return fmt.Errorf("observe production init container %s: %w", container.Name, statusErr)
		}
		state := status.GetStatus()
		if state != nil && state.State == runtimev1.ContainerState_CONTAINER_EXITED {
			if state.ExitCode != 0 {
				return fmt.Errorf("production init container %s exited with code %d", container.Name, state.ExitCode)
			}
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("production init container %s did not exit before timeout", container.Name)
		}
		timer := time.NewTimer(20 * time.Millisecond)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return fmt.Errorf("wait for production init container %s: %w", container.Name, ctx.Err())
		case <-timer.C:
		}
	}
}

func productionInitCommand(container *corev1.Container) ([]string, []string, error) {
	if container == nil || len(container.Command) == 0 {
		return nil, nil, errors.New("signed init container requires an explicit command")
	}
	return append([]string(nil), container.Command...), append([]string(nil), container.Args...), nil
}

func (workload *productionWorkload) createAndStart(ctx context.Context, sandboxConfig *runtimev1.PodSandboxConfig, namespaceOptions *runtimev1.NamespaceOption, pod *corev1.Pod, container *corev1.Container, uid, gid uint32, offer *pidfdOffer, runtime bool) error {
	if err := validateProductionContainer(container); err != nil {
		return err
	}
	command, args, err := productionCommand(container)
	if err != nil {
		return err
	}
	if len(command) == 0 {
		return fmt.Errorf("production %s container has no explicit command; image entrypoint resolution is required", container.Name)
	}
	mounts, err := workload.productionMounts(ctx, pod, container, workload.volumeRoot, workload.startupPath, runtime)
	if err != nil {
		return err
	}
	envs, err := workload.productionEnvs(ctx, pod, container, workload.startupPath, runtime)
	if err != nil {
		return err
	}
	launcherBinary, err := os.Executable()
	if err != nil || !canonicalAbsolute(launcherBinary) {
		return errors.New("production launcher executable path is invalid")
	}
	mounts = append(mounts,
		&runtimev1.Mount{ContainerPath: "/vela-runtime/pidfd-offer", HostPath: launcherBinary, Readonly: true},
		&runtimev1.Mount{ContainerPath: "/vela-runtime/offer", HostPath: offer.directory, Readonly: true},
	)
	envs = append(envs, &runtimev1.KeyValue{Key: "VELA_RUNTIME_LAUNCHER_PIDFD_OFFER", Value: "1"})
	if runtime {
		envs = append(envs, &runtimev1.KeyValue{Key: "VELA_RUNTIME_LAUNCHER_PIDFD_GATE", Value: "1"})
	}
	wrappedCommand := []string{"/vela-runtime/pidfd-offer"}
	wrappedArgs := append([]string{"--vela-runtime-pidfd-offer", "--socket", offer.containerPath, "--"}, append(command, args...)...)
	resources, err := productionResources(container.Resources)
	if err != nil {
		return err
	}
	security, err := productionSecurityContext(container.SecurityContext, namespaceOptions, uid, gid, false)
	if err != nil {
		return err
	}
	containerConfig := &runtimev1.ContainerConfig{Metadata: &runtimev1.ContainerMetadata{Name: container.Name, Attempt: 1}, Image: &runtimev1.ImageSpec{Image: container.Image}, Command: wrappedCommand, Args: wrappedArgs, Envs: envs, Mounts: mounts, LogPath: container.Name + ".log", Linux: &runtimev1.LinuxContainerConfig{Resources: resources, SecurityContext: security}}
	created, err := workload.runtime.CreateContainer(ctx, &runtimev1.CreateContainerRequest{PodSandboxId: workload.target.SandboxID, Config: containerConfig, SandboxConfig: sandboxConfig})
	if err != nil {
		return fmt.Errorf("create production %s container: %w", container.Name, err)
	}
	target := nodeagent.RuntimeContainerTarget{ContainerID: created.GetContainerId(), SandboxID: workload.target.SandboxID, PodUID: workload.target.PodUID, PodNamespace: pod.Namespace, PodName: pod.Name, ContainerName: container.Name, ContainerAttempt: 1}
	// Retain each successful creation before the next fallible step so a failed
	// StartContainer or pidfd offer still removes this invocation's container.
	if runtime {
		workload.target = target
	} else {
		workload.worker = target
	}
	if err := target.Validate(); err != nil {
		return err
	}
	if _, err := workload.runtime.StartContainer(ctx, &runtimev1.StartContainerRequest{ContainerId: target.ContainerID}); err != nil {
		return fmt.Errorf("start production %s container: %w", container.Name, err)
	}
	pidfd, err := offer.accept(ctx, workload.tasks, target.ContainerID)
	if err != nil {
		return err
	}
	if runtime {
		workload.target = target
		workload.runtimeFD = pidfd
	} else {
		workload.worker = target
		workload.workerFD = pidfd
	}
	return nil
}

func validateProductionContainer(container *corev1.Container) error {
	if container == nil {
		return errors.New("production container is nil")
	}
	if container.WorkingDir != "" || len(container.Ports) != 0 || container.ResizePolicy != nil || container.RestartPolicy != nil || len(container.RestartPolicyRules) != 0 || len(container.VolumeDevices) != 0 ||
		container.LivenessProbe != nil || container.ReadinessProbe != nil || container.StartupProbe != nil || container.Lifecycle != nil ||
		container.Stdin || container.StdinOnce || container.TTY {
		return errors.New("signed Runtime/Worker container uses unsupported fields")
	}
	if len(container.Resources.Claims) != 0 {
		return errors.New("signed Runtime/Worker resource claims are unsupported by direct CRI launch")
	}
	if sc := container.SecurityContext; sc != nil && (sc.SELinuxOptions != nil || sc.WindowsOptions != nil || sc.ProcMount != nil || sc.AppArmorProfile != nil) {
		return errors.New("signed Runtime/Worker security context uses unsupported fields")
	}
	if sc := container.SecurityContext; sc != nil && sc.SeccompProfile != nil && sc.SeccompProfile.Type != corev1.SeccompProfileTypeRuntimeDefault {
		return errors.New("signed Runtime/Worker seccomp profile is unsupported")
	}
	for _, mount := range container.VolumeMounts {
		if mount.SubPath != "" || mount.SubPathExpr != "" || mount.MountPropagation != nil {
			return errors.New("signed Runtime/Worker volume mount uses unsupported fields")
		}
	}
	return nil
}

func (workload *productionWorkload) Close() error {
	if workload == nil {
		return nil
	}
	var result error
	if workload.workerFD != nil {
		result = errors.Join(result, workload.workerFD.Close())
		workload.workerFD = nil
	}
	if workload.runtimeFD != nil {
		result = errors.Join(result, workload.runtimeFD.Close())
		workload.runtimeFD = nil
	}
	if workload.observerFD != nil {
		result = errors.Join(result, unix.PidfdSendSignal(int(workload.observerFD.Fd()), unix.SIGKILL, nil, 0), workload.observerFD.Close())
		workload.observerFD = nil
	}
	if workload.observer != nil {
		if err := workload.observer.Wait(); err != nil {
			var exitErr *exec.ExitError
			if !errors.As(err, &exitErr) {
				result = errors.Join(result, err)
			}
		}
		workload.observer = nil
	}
	if workload.runtime != nil {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		for index := len(workload.initIDs) - 1; index >= 0; index-- {
			id := workload.initIDs[index]
			_, stopErr := workload.runtime.StopContainer(cleanupCtx, &runtimev1.StopContainerRequest{ContainerId: id, Timeout: 5})
			result = errors.Join(result, stopErr)
			_, removeErr := workload.runtime.RemoveContainer(cleanupCtx, &runtimev1.RemoveContainerRequest{ContainerId: id})
			result = errors.Join(result, removeErr)
		}
		workload.initIDs = nil
		if workload.worker.ContainerID != "" {
			_, stopErr := workload.runtime.StopContainer(cleanupCtx, &runtimev1.StopContainerRequest{ContainerId: workload.worker.ContainerID, Timeout: 5})
			result = errors.Join(result, stopErr)
			_, err := workload.runtime.RemoveContainer(cleanupCtx, &runtimev1.RemoveContainerRequest{ContainerId: workload.worker.ContainerID})
			result = errors.Join(result, err)
		}
		if workload.target.ContainerID != "" {
			_, stopErr := workload.runtime.StopContainer(cleanupCtx, &runtimev1.StopContainerRequest{ContainerId: workload.target.ContainerID, Timeout: 5})
			result = errors.Join(result, stopErr)
			_, err := workload.runtime.RemoveContainer(cleanupCtx, &runtimev1.RemoveContainerRequest{ContainerId: workload.target.ContainerID})
			result = errors.Join(result, err)
		}
		if workload.target.SandboxID != "" {
			_, stopErr := workload.runtime.StopPodSandbox(cleanupCtx, &runtimev1.StopPodSandboxRequest{PodSandboxId: workload.target.SandboxID})
			result = errors.Join(result, stopErr)
			_, err := workload.runtime.RemovePodSandbox(cleanupCtx, &runtimev1.RemovePodSandboxRequest{PodSandboxId: workload.target.SandboxID})
			result = errors.Join(result, err)
		}
		cancel()
	}
	if workload.observerEnd != nil {
		result = errors.Join(result, workload.observerEnd.Close())
		workload.observerEnd = nil
	}
	if workload.observerChild != nil {
		result = errors.Join(result, workload.observerChild.Close())
		workload.observerChild = nil
	}
	unsafeVolumeCleanup := false
	for _, offer := range workload.offers {
		offerErr := offer.Close()
		if errors.Is(offerErr, errPidfdOfferSocketReplaced) {
			// A replaced path is outside the launcher's identity boundary. Do not
			// recursively remove the containing mount root after this condition.
			unsafeVolumeCleanup = true
		}
		result = errors.Join(result, offerErr)
	}
	if workload.connection != nil {
		result = errors.Join(result, workload.connection.Close())
		workload.connection = nil
	}
	if workload.volumeRoot != "" && !unsafeVolumeCleanup {
		result = errors.Join(result, os.RemoveAll(workload.volumeRoot))
		workload.volumeRoot = ""
	} else if unsafeVolumeCleanup {
		result = errors.Join(result, errors.New("skip workload volume cleanup after pidfd offer socket replacement"))
	}
	return result
}

func requiredContainers(pod *corev1.Pod) (*corev1.Container, *corev1.Container, error) {
	if pod == nil || len(pod.Spec.EphemeralContainers) != 0 || pod.Spec.HostNetwork || pod.Spec.HostPID || pod.Spec.HostIPC || (pod.Spec.ShareProcessNamespace != nil && *pod.Spec.ShareProcessNamespace) || len(pod.Spec.HostAliases) != 0 || pod.Spec.DNSConfig != nil {
		return nil, nil, errors.New("signed Pod uses unsupported workload fields")
	}
	if security := pod.Spec.SecurityContext; security != nil && (security.SELinuxOptions != nil || len(security.SupplementalGroups) != 0 || security.Sysctls != nil && len(security.Sysctls) != 0 || security.WindowsOptions != nil || security.AppArmorProfile != nil || security.SupplementalGroupsPolicy != nil) {
		return nil, nil, errors.New("signed Pod security context uses unsupported fields")
	}
	if err := validatePodSecurityContext(pod); err != nil {
		return nil, nil, err
	}
	if err := validateInitContainers(pod.Spec.InitContainers); err != nil {
		return nil, nil, err
	}
	var runtimeContainer, workerContainer *corev1.Container
	for index := range pod.Spec.Containers {
		container := &pod.Spec.Containers[index]
		switch container.Name {
		case "model-runtime":
			if runtimeContainer != nil {
				return nil, nil, errors.New("signed Pod has duplicate model-runtime containers")
			}
			runtimeContainer = container
		case "stage-worker-agent":
			if workerContainer != nil {
				return nil, nil, errors.New("signed Pod has duplicate stage-worker-agent containers")
			}
			workerContainer = container
		default:
			return nil, nil, errors.New("signed Pod contains an unsupported container")
		}
	}
	if runtimeContainer == nil || workerContainer == nil || !pinnedImagePattern.MatchString(runtimeContainer.Image) || !pinnedImagePattern.MatchString(workerContainer.Image) {
		return nil, nil, errors.New("signed Pod must contain digest-pinned Runtime and Worker containers")
	}
	return runtimeContainer, workerContainer, nil
}

func validatePodSecurityContext(pod *corev1.Pod) error {
	if pod == nil || pod.Spec.SecurityContext == nil {
		return nil
	}
	security := pod.Spec.SecurityContext
	if security.RunAsNonRoot != nil && !*security.RunAsNonRoot {
		return errors.New("signed Pod must require non-root containers")
	}
	if security.FSGroup != nil && *security.FSGroup <= 0 {
		return errors.New("signed Pod fsGroup is invalid")
	}
	if security.FSGroupChangePolicy != nil && *security.FSGroupChangePolicy != corev1.FSGroupChangeOnRootMismatch {
		return errors.New("signed Pod fsGroupChangePolicy is unsupported")
	}
	if security.SeccompProfile != nil && security.SeccompProfile.Type != corev1.SeccompProfileTypeRuntimeDefault {
		return errors.New("signed Pod seccomp profile is unsupported")
	}
	return nil
}

func validateInitContainers(containers []corev1.Container) error {
	if len(containers) != 0 && len(containers) != 2 {
		return errors.New("signed Pod must contain both approved init containers")
	}
	approved := map[string]struct{}{
		"stage-worker-private-materialization":  {},
		"model-runtime-private-materialization": {},
	}
	seen := make(map[string]struct{}, len(containers))
	for _, container := range containers {
		if container.Name == "" {
			return errors.New("signed init container name is empty")
		}
		if _, ok := seen[container.Name]; ok {
			return errors.New("signed Pod has duplicate init containers")
		}
		if _, ok := approved[container.Name]; !ok {
			return fmt.Errorf("signed init container %q is not approved", container.Name)
		}
		seen[container.Name] = struct{}{}
		if !pinnedImagePattern.MatchString(container.Image) {
			return errors.New("signed init container image is not digest-pinned")
		}
		if err := validateProductionContainer(&container); err != nil {
			return fmt.Errorf("signed init container %q: %w", container.Name, err)
		}
		if len(container.Command) == 0 {
			return fmt.Errorf("signed init container %q has no explicit command", container.Name)
		}
	}
	if len(containers) == 2 {
		for name := range approved {
			if _, ok := seen[name]; !ok {
				return fmt.Errorf("signed Pod is missing init container %q", name)
			}
		}
	}
	return nil
}

func productionCredentials(pod corev1.Pod, runtimeContainer, workerContainer corev1.Container) (uint32, uint32, error) {
	uid, gid := int64(0), int64(0)
	if pod.Spec.SecurityContext != nil {
		if pod.Spec.SecurityContext.RunAsUser != nil {
			uid = *pod.Spec.SecurityContext.RunAsUser
		}
		if pod.Spec.SecurityContext.RunAsGroup != nil {
			gid = *pod.Spec.SecurityContext.RunAsGroup
		}
	}
	for _, container := range []corev1.Container{runtimeContainer, workerContainer} {
		if container.SecurityContext != nil {
			if container.SecurityContext.RunAsUser != nil {
				if uid != 0 && uid != *container.SecurityContext.RunAsUser {
					return 0, 0, errors.New("Runtime and Worker UIDs differ")
				}
				uid = *container.SecurityContext.RunAsUser
			}
			if container.SecurityContext.RunAsGroup != nil {
				if gid != 0 && gid != *container.SecurityContext.RunAsGroup {
					return 0, 0, errors.New("Runtime and Worker GIDs differ")
				}
				gid = *container.SecurityContext.RunAsGroup
			}
		}
	}
	if uid <= 0 || gid <= 0 || uid >= int64(^uint32(0)) || gid >= int64(^uint32(0)) {
		return 0, 0, errors.New("signed Pod Runtime credentials are invalid")
	}
	return uint32(uid), uint32(gid), nil
}

func productionCommand(container *corev1.Container) ([]string, []string, error) {
	if container == nil {
		return nil, nil, errors.New("production container is nil")
	}
	command := append([]string(nil), container.Command...)
	if len(command) == 0 {
		switch container.Name {
		case "model-runtime":
			command = []string{"/usr/local/bin/vela-model-runtime"}
		case "stage-worker-agent":
			command = []string{"/usr/local/bin/vela-stage-worker-agent"}
		default:
			return nil, nil, errors.New("production launcher has no approved OCI entrypoint for container")
		}
	}
	for _, value := range append(append([]string(nil), command...), container.Args...) {
		if value == "" || strings.ContainsRune(value, '\x00') {
			return nil, nil, errors.New("production container command contains invalid text")
		}
	}
	return command, append([]string(nil), container.Args...), nil
}

func (workload *productionWorkload) productionMounts(ctx context.Context, pod *corev1.Pod, container *corev1.Container, root, startupSocket string, runtime bool) ([]*runtimev1.Mount, error) {
	volumes, err := materializeVolumes(ctx, workload.kube, pod, root, workload.uid, workload.gid)
	if err != nil {
		return nil, err
	}
	mounts := make([]*runtimev1.Mount, 0, len(container.VolumeMounts)+1)
	seen := make(map[string]struct{})
	for _, mount := range container.VolumeMounts {
		if mount.Name == "" || !canonicalAbsolute(mount.MountPath) || mount.MountPath == "/" {
			return nil, errors.New("signed volume mount is invalid")
		}
		host, ok := volumes[mount.Name]
		if !ok {
			return nil, fmt.Errorf("signed volume %q is missing", mount.Name)
		}
		if _, duplicate := seen[mount.MountPath]; duplicate {
			return nil, errors.New("signed volume mount paths are duplicated")
		}
		seen[mount.MountPath] = struct{}{}
		mounts = append(mounts, &runtimev1.Mount{ContainerPath: mount.MountPath, HostPath: host, Readonly: mount.ReadOnly})
	}
	if runtime {
		containerPath, err := runtimeStartupContainerSocket(startupSocket)
		if err != nil {
			return nil, errors.New("startup socket path is invalid")
		}
		mounts = append(mounts, &runtimev1.Mount{ContainerPath: filepath.Dir(containerPath), HostPath: filepath.Dir(startupSocket), Readonly: true})
		bootstrapPath, bootstrapErr := productionRuntimeBootstrapPath(container)
		if bootstrapErr != nil {
			return nil, errors.New("signed Runtime bootstrap path is invalid")
		}
		bootstrapDir := filepath.Join(filepath.Dir(startupSocket), "runtime-bootstrap")
		if _, err := securefile.ResolveTrustedDirectory(bootstrapDir); err != nil {
			return nil, fmt.Errorf("validate Runtime bootstrap directory: %w", err)
		}
		mounts = append(mounts, &runtimev1.Mount{ContainerPath: filepath.Dir(bootstrapPath), HostPath: bootstrapDir, Readonly: true})
	}
	return mounts, nil
}

func productionRuntimeBootstrapPath(container *corev1.Container) (string, error) {
	if container == nil {
		return "", errors.New("Runtime container is nil")
	}
	command := append([]string(nil), container.Command...)
	if len(command) == 0 {
		command = []string{"/usr/local/bin/vela-model-runtime"}
	}
	argv := append(command, container.Args...)
	if len(argv) != 4 || argv[0] != "/usr/local/bin/vela-model-runtime" || argv[1] != "serve-remote" || argv[2] != "--bootstrap-file" || !canonicalAbsolute(argv[3]) {
		return "", errors.New("Runtime bootstrap argv is invalid")
	}
	return argv[3], nil
}

func (workload *productionWorkload) productionEnvs(ctx context.Context, pod *corev1.Pod, container *corev1.Container, startupSocket string, runtime bool) ([]*runtimev1.KeyValue, error) {
	values := make([]*runtimev1.KeyValue, 0, len(container.Env)+8)
	seen := make(map[string]struct{})
	add := func(name, value string) error {
		if name == "" || strings.ContainsRune(name, '\x00') {
			return errors.New("signed environment name is invalid")
		}
		if _, duplicate := seen[name]; duplicate {
			return fmt.Errorf("signed environment %q is duplicated", name)
		}
		seen[name] = struct{}{}
		values = append(values, &runtimev1.KeyValue{Key: name, Value: value})
		return nil
	}
	for _, source := range container.EnvFrom {
		if source.Prefix != "" && !validEnvPrefix(source.Prefix) {
			return nil, errors.New("signed EnvFrom prefix is invalid")
		}
		entries, err := workload.resolveEnvFrom(ctx, pod.Namespace, source)
		if err != nil {
			return nil, err
		}
		keys := make([]string, 0, len(entries))
		for key := range entries {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			if err := add(source.Prefix+key, entries[key]); err != nil {
				return nil, err
			}
		}
	}
	for _, value := range container.Env {
		resolved, present, err := workload.resolveEnvValue(ctx, pod, value)
		if err != nil {
			return nil, err
		}
		if !present {
			continue
		}
		if err := add(value.Name, resolved); err != nil {
			return nil, err
		}
	}
	if runtime {
		containerPath, err := runtimeStartupContainerSocket(startupSocket)
		if err != nil {
			return nil, errors.New("startup socket path is invalid")
		}
		if err := add("VELA_MODEL_RUNTIME_NODE_STARTUP_SOCKET", containerPath); err != nil {
			return nil, err
		}
	}
	return values, nil
}

func runtimeStartupContainerSocket(hostPath string) (string, error) {
	if !canonicalAbsolute(hostPath) || !canonicalAbsolute(filepath.Dir(hostPath)) {
		return "", errors.New("startup socket path is invalid")
	}
	base := filepath.Base(hostPath)
	if base == "." || base == ".." || strings.ContainsRune(base, '/') || strings.ContainsRune(base, '\x00') {
		return "", errors.New("startup socket basename is invalid")
	}
	return filepath.Join("/run/vela-node", base), nil
}

func materializeVolumes(ctx context.Context, kube kubernetes.Interface, pod *corev1.Pod, root string, uid, gid uint32) (map[string]string, error) {
	result := make(map[string]string, len(pod.Spec.Volumes))
	for _, volume := range pod.Spec.Volumes {
		if volume.Name == "" || volume.Name == "." || strings.ContainsRune(volume.Name, '/') {
			return nil, errors.New("signed volume name is invalid")
		}
		sources := 0
		var host string
		if volume.HostPath != nil {
			sources++
			host = volume.HostPath.Path
			if !canonicalAbsolute(host) || host == "/" {
				return nil, errors.New("signed HostPath is invalid")
			}
			if _, err := securefile.ResolveTrustedDirectory(filepath.Dir(host)); err != nil {
				return nil, fmt.Errorf("validate HostPath: %w", err)
			}
		}
		if volume.EmptyDir != nil {
			sources++
			host = filepath.Join(root, volume.Name)
			if err := os.MkdirAll(host, 0o700); err != nil {
				return nil, err
			}
		}
		if volume.ConfigMap != nil {
			sources++
			host = filepath.Join(root, volume.Name)
			if err := materializeConfigMap(ctx, kube, pod.Namespace, volume.ConfigMap, host, uid, gid); err != nil {
				return nil, err
			}
		}
		if volume.Secret != nil {
			sources++
			host = filepath.Join(root, volume.Name)
			if err := materializeSecret(ctx, kube, pod.Namespace, volume.Secret, host, uid, gid); err != nil {
				return nil, err
			}
		}
		if volume.Projected != nil {
			sources++
			host = filepath.Join(root, volume.Name)
			if err := materializeProjected(ctx, kube, pod, volume.Projected, host, uid, gid); err != nil {
				return nil, err
			}
		}
		if volume.DownwardAPI != nil {
			sources++
			host = filepath.Join(root, volume.Name)
			if err := materializeDownwardAPI(pod, volume.DownwardAPI, host, uid, gid); err != nil {
				return nil, err
			}
		}
		if sources != 1 {
			return nil, fmt.Errorf("signed volume %q has invalid source count", volume.Name)
		}
		result[volume.Name] = host
	}
	return result, nil
}

func loadKubernetesClient() (kubernetes.Interface, error) {
	path := os.Getenv("VELA_RUNTIME_LAUNCHER_KUBECONFIG")
	if !canonicalAbsolute(path) {
		return nil, errors.New("VELA_RUNTIME_LAUNCHER_KUBECONFIG is required and canonical")
	}
	if err := trustedConfigFile(path); err != nil {
		return nil, fmt.Errorf("validate launcher kubeconfig: %w", err)
	}
	configuration, err := clientcmd.BuildConfigFromFlags("", path)
	if err != nil {
		return nil, fmt.Errorf("load launcher kubeconfig: %w", err)
	}
	client, err := kubernetes.NewForConfig(configuration)
	if err != nil {
		return nil, fmt.Errorf("configure launcher Kubernetes client: %w", err)
	}
	return client, nil
}

func (workload *productionWorkload) resolveEnvFrom(ctx context.Context, namespace string, source corev1.EnvFromSource) (map[string]string, error) {
	if source.ConfigMapRef != nil {
		if source.ConfigMapRef.Name == "" {
			return nil, errors.New("EnvFrom ConfigMap name is empty")
		}
		configMap, err := workload.kube.CoreV1().ConfigMaps(namespace).Get(ctx, source.ConfigMapRef.Name, metav1.GetOptions{})
		if err != nil {
			if source.ConfigMapRef.Optional != nil && *source.ConfigMapRef.Optional {
				return map[string]string{}, nil
			}
			return nil, err
		}
		values := make(map[string]string, len(configMap.Data)+len(configMap.BinaryData))
		for key, value := range configMap.Data {
			values[key] = value
		}
		for key, value := range configMap.BinaryData {
			values[key] = string(value)
		}
		return values, nil
	}
	if source.SecretRef != nil {
		if source.SecretRef.Name == "" {
			return nil, errors.New("EnvFrom Secret name is empty")
		}
		secret, err := workload.kube.CoreV1().Secrets(namespace).Get(ctx, source.SecretRef.Name, metav1.GetOptions{})
		if err != nil {
			if source.SecretRef.Optional != nil && *source.SecretRef.Optional {
				return map[string]string{}, nil
			}
			return nil, err
		}
		values := make(map[string]string, len(secret.Data))
		for key, value := range secret.Data {
			values[key] = string(value)
		}
		return values, nil
	}
	return nil, errors.New("EnvFrom has no supported source")
}

func (workload *productionWorkload) resolveEnvValue(ctx context.Context, pod *corev1.Pod, env corev1.EnvVar) (string, bool, error) {
	if env.ValueFrom == nil {
		return env.Value, true, nil
	}
	source := env.ValueFrom
	if source.FieldRef != nil {
		value, err := fieldValue(pod, source.FieldRef.FieldPath)
		return value, err == nil, err
	}
	if source.ConfigMapKeyRef != nil {
		ref := source.ConfigMapKeyRef
		configMap, err := workload.kube.CoreV1().ConfigMaps(pod.Namespace).Get(ctx, ref.Name, metav1.GetOptions{})
		if err != nil {
			if ref.Optional != nil && *ref.Optional {
				return "", false, nil
			}
			return "", false, err
		}
		value, ok := configMap.Data[ref.Key]
		if !ok {
			if ref.Optional != nil && *ref.Optional {
				return "", false, nil
			}
			return "", false, fmt.Errorf("ConfigMap %q key %q is missing", ref.Name, ref.Key)
		}
		return value, true, nil
	}
	if source.SecretKeyRef != nil {
		ref := source.SecretKeyRef
		secret, err := workload.kube.CoreV1().Secrets(pod.Namespace).Get(ctx, ref.Name, metav1.GetOptions{})
		if err != nil {
			if ref.Optional != nil && *ref.Optional {
				return "", false, nil
			}
			return "", false, err
		}
		value, ok := secret.Data[ref.Key]
		if !ok {
			if ref.Optional != nil && *ref.Optional {
				return "", false, nil
			}
			return "", false, fmt.Errorf("Secret %q key %q is missing", ref.Name, ref.Key)
		}
		return string(value), true, nil
	}
	return "", false, errors.New("environment source is unsupported")
}

func fieldValue(pod *corev1.Pod, path string) (string, error) {
	switch path {
	case "metadata.name":
		return pod.Name, nil
	case "metadata.namespace":
		return pod.Namespace, nil
	case "metadata.uid":
		return string(pod.UID), nil
	case "spec.nodeName":
		value := os.Getenv("VELA_RUNTIME_LAUNCHER_NODE_NAME")
		if value == "" {
			return "", errors.New("VELA_RUNTIME_LAUNCHER_NODE_NAME is required for spec.nodeName")
		}
		return value, nil
	case "status.podIP":
		if pod.Status.PodIP == "" {
			return "", errors.New("status.podIP is unavailable before sandbox startup")
		}
		return pod.Status.PodIP, nil
	default:
		return "", fmt.Errorf("unsupported FieldRef %q", path)
	}
}

func validEnvPrefix(value string) bool {
	for index, character := range value {
		if !(character == '_' || character == '.' || character == '-' || character >= 'A' && character <= 'Z' || character >= 'a' && character <= 'z' || index > 0 && character >= '0' && character <= '9') {
			return false
		}
	}
	return true
}

func materializeConfigMap(ctx context.Context, kube kubernetes.Interface, namespace string, source *corev1.ConfigMapVolumeSource, directory string, uid, gid uint32) error {
	if source == nil || source.Name == "" {
		return errors.New("ConfigMap volume source is invalid")
	}
	configMap, err := kube.CoreV1().ConfigMaps(namespace).Get(ctx, source.Name, metav1.GetOptions{})
	if err != nil {
		return err
	}
	values := make(map[string][]byte, len(configMap.Data)+len(configMap.BinaryData))
	for key, value := range configMap.Data {
		values[key] = []byte(value)
	}
	for key, value := range configMap.BinaryData {
		values[key] = value
	}
	return writeVolumeEntries(directory, values, source.Items, source.DefaultMode, uid, gid)
}

func materializeSecret(ctx context.Context, kube kubernetes.Interface, namespace string, source *corev1.SecretVolumeSource, directory string, uid, gid uint32) error {
	if source == nil || source.SecretName == "" {
		return errors.New("Secret volume source is invalid")
	}
	secret, err := kube.CoreV1().Secrets(namespace).Get(ctx, source.SecretName, metav1.GetOptions{})
	if err != nil {
		return err
	}
	return writeVolumeEntries(directory, secret.Data, source.Items, source.DefaultMode, uid, gid)
}

func materializeProjected(ctx context.Context, kube kubernetes.Interface, pod *corev1.Pod, source *corev1.ProjectedVolumeSource, directory string, uid, gid uint32) error {
	if source == nil {
		return errors.New("projected volume source is invalid")
	}
	if err := os.MkdirAll(directory, 0o700); err != nil {
		return err
	}
	for _, projection := range source.Sources {
		switch {
		case projection.ConfigMap != nil:
			configMap := &corev1.ConfigMapVolumeSource{LocalObjectReference: projection.ConfigMap.LocalObjectReference, Items: projection.ConfigMap.Items, Optional: projection.ConfigMap.Optional}
			if err := materializeConfigMap(ctx, kube, pod.Namespace, configMap, directory, uid, gid); err != nil {
				return err
			}
		case projection.Secret != nil:
			secret := &corev1.SecretVolumeSource{SecretName: projection.Secret.Name, Items: projection.Secret.Items, Optional: projection.Secret.Optional}
			if err := materializeSecret(ctx, kube, pod.Namespace, secret, directory, uid, gid); err != nil {
				return err
			}
		case projection.DownwardAPI != nil:
			downward := &corev1.DownwardAPIVolumeSource{Items: projection.DownwardAPI.Items}
			if err := materializeDownwardAPI(pod, downward, directory, uid, gid); err != nil {
				return err
			}
		default:
			return errors.New("projected volume contains unsupported source")
		}
	}
	return nil
}

func materializeDownwardAPI(pod *corev1.Pod, source *corev1.DownwardAPIVolumeSource, directory string, uid, gid uint32) error {
	if source == nil {
		return errors.New("DownwardAPI volume source is invalid")
	}
	entries := make(map[string][]byte, len(source.Items))
	items := make([]corev1.DownwardAPIVolumeFile, 0, len(source.Items))
	for _, item := range source.Items {
		items = append(items, item)
	}
	for _, item := range items {
		if item.FieldRef == nil {
			return errors.New("DownwardAPI resourceFieldRef is unsupported")
		}
		value, err := fieldValue(pod, item.FieldRef.FieldPath)
		if err != nil {
			return err
		}
		entries[item.Path] = []byte(value)
	}
	return writeVolumeEntries(directory, entries, nil, nil, uid, gid)
}

func writeVolumeEntries(directory string, values map[string][]byte, items []corev1.KeyToPath, defaultMode *int32, uid, gid uint32) error {
	if err := os.MkdirAll(directory, 0o700); err != nil {
		return err
	}
	selected := make(map[string]string)
	if len(items) == 0 {
		for key := range values {
			selected[key] = key
		}
	} else {
		for _, item := range items {
			if !safeRelativePath(item.Path) || item.Key == "" {
				return errors.New("volume item path is invalid")
			}
			if _, exists := selected[item.Key]; exists {
				return fmt.Errorf("volume item key %q is duplicated", item.Key)
			}
			for _, existing := range selected {
				if existing == item.Path {
					return fmt.Errorf("volume item path %q is duplicated", item.Path)
				}
			}
			selected[item.Key] = item.Path
		}
	}
	keys := make([]string, 0, len(selected))
	for key := range selected {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	mode := int32(0o400)
	if defaultMode != nil {
		mode = *defaultMode
	}
	if mode < 0 || mode > 0o777 {
		return errors.New("volume default mode is invalid")
	}
	for _, key := range keys {
		value, ok := values[key]
		if !ok {
			return fmt.Errorf("volume key %q is missing", key)
		}
		path := filepath.Join(directory, selected[key])
		if !safePathUnder(directory, path) {
			return errors.New("volume item escapes root")
		}
		if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
			return err
		}
		if err := os.WriteFile(path, value, os.FileMode(mode)); err != nil {
			return err
		}
		if err := os.Chown(path, int(uid), int(gid)); err != nil {
			return err
		}
	}
	return os.Chown(directory, int(uid), int(gid))
}

func safeRelativePath(path string) bool {
	return path != "" && !filepath.IsAbs(path) && filepath.Clean(path) == path && path != "." && !strings.HasPrefix(path, ".."+string(filepath.Separator)) && path != ".."
}
func safePathUnder(root, path string) bool {
	relative, err := filepath.Rel(root, path)
	return err == nil && relative != ".." && !strings.HasPrefix(relative, ".."+string(filepath.Separator))
}

func newProductionObserverSocketpair() (*os.File, *os.File, error) {
	pair, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_SEQPACKET|unix.SOCK_CLOEXEC, 0)
	if err != nil {
		return nil, nil, err
	}
	return os.NewFile(uintptr(pair[0]), "runtime-observer-node"), os.NewFile(uintptr(pair[1]), "runtime-observer-child"), nil
}

func startProductionObserver(uid, gid uint32, targetPIDFD, nodeSide, childSide *os.File) (*exec.Cmd, *os.File, *os.File, error) {
	path := os.Getenv("VELA_RUNTIME_LAUNCHER_OBSERVER_PATH")
	if !canonicalAbsolute(path) {
		return nil, nil, nil, errors.New("VELA_RUNTIME_LAUNCHER_OBSERVER_PATH is required")
	}
	if err := trustedExecutable(path); err != nil {
		return nil, nil, nil, err
	}
	if uid == 0 || gid == 0 || targetPIDFD == nil || nodeSide == nil || childSide == nil {
		return nil, nil, nil, errors.New("production observer credentials are invalid")
	}
	if err := runtimechannel.ValidatePIDFD(int(targetPIDFD.Fd())); err != nil {
		return nil, nil, nil, fmt.Errorf("production observer target pidfd: %w", err)
	}
	targetCopy, err := unix.FcntlInt(targetPIDFD.Fd(), unix.F_DUPFD_CLOEXEC, 0)
	if err != nil {
		_ = nodeSide.Close()
		_ = childSide.Close()
		return nil, nil, nil, fmt.Errorf("duplicate production observer target pidfd: %w", err)
	}
	targetFile := os.NewFile(uintptr(targetCopy), "runtime-observer-target-pidfd")
	observerPID := -1
	command := exec.Command(path, "--vela-runtime-observer", "--attach-fd4", strconv.FormatUint(uint64(uid), 10), strconv.FormatUint(uint64(gid), 10))
	command.ExtraFiles = []*os.File{childSide, targetFile}
	command.SysProcAttr = &syscall.SysProcAttr{PidFD: &observerPID}
	// Preserve observer diagnostics in the launcher's stderr. The observer is
	// still prevented from writing to the Node control channel; hiding attach
	// failures made production diagnosis indistinguishable from a peer close.
	command.Stdout, command.Stderr = io.Discard, os.Stderr
	if err := command.Start(); err != nil {
		_ = nodeSide.Close()
		_ = childSide.Close()
		_ = targetFile.Close()
		return nil, nil, nil, err
	}
	_ = childSide.Close()
	_ = targetFile.Close()
	if observerPID < 0 {
		_ = command.Process.Kill()
		_ = command.Wait()
		_ = nodeSide.Close()
		return nil, nil, nil, errors.New("production observer did not provide a pidfd")
	}
	return command, os.NewFile(uintptr(observerPID), "runtime-observer-pidfd"), nodeSide, nil
}

func dialCRI(ctx context.Context, socket string) (*grpc.ClientConn, error) {
	info, err := os.Stat(socket)
	if err != nil || info.Mode()&os.ModeSocket == 0 {
		return nil, errors.New("production CRI socket is not a socket")
	}
	dialer := func(ctx context.Context, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "unix", socket)
	}
	connection, err := grpc.NewClient("passthrough:///vela-runtime-cri", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(dialer), grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(1<<20), grpc.MaxCallSendMsgSize(64<<10)))
	if err != nil {
		return nil, err
	}
	connection.Connect()
	return connection, nil
}

func strictJSON(wire []byte, target any) error {
	if err := strictjson.RejectDuplicateKeys(wire); err != nil {
		return err
	}
	decoder := json.NewDecoder(bytes.NewReader(wire))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		return errors.New("trailing JSON data")
	}
	return nil
}

func recvFrame(ctx context.Context, fd int) ([]byte, error) {
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
		return nil, errors.New("unexpected runtime launcher ancillary data or truncation")
	}
	return buffer[:n], nil
}

func sendFrameWithRights(fd int, wire []byte, rights []int) error {
	if len(wire) == 0 || len(wire) > 64<<10 {
		return errors.New("runtime launcher frame is too large")
	}
	n, err := unix.SendmsgN(fd, wire, unix.UnixRights(rights...), nil, 0)
	if err != nil {
		return err
	}
	if n != len(wire) {
		return io.ErrShortWrite
	}
	return nil
}

func runPIDFDOffer(args []string) error {
	if len(args) < 4 || args[0] != "--socket" || args[2] != "--" || !canonicalAbsolute(args[1]) || !canonicalAbsolute(args[3]) {
		return errors.New("pidfd offer requires --socket PATH -- absolute-command [ARGS]")
	}
	var release <-chan os.Signal
	var stopRelease func()
	if os.Getenv("VELA_RUNTIME_LAUNCHER_PIDFD_GATE") == "1" {
		// Register before publishing the pidfd. The observer can acknowledge
		// custody immediately after receiving it; using Pause here would lose
		// that SIGCONT and leave the wrapper waiting forever.
		releases := make(chan os.Signal, 1)
		signal.Notify(releases, syscall.SIGCONT)
		release = releases
		stopRelease = func() {
			signal.Stop(releases)
		}
		defer stopRelease()
	}
	fd, err := unix.PidfdOpen(os.Getpid(), 0)
	if err != nil {
		return fmt.Errorf("open self pidfd before exec: %w", err)
	}
	connection, err := net.DialTimeout("unixpacket", args[1], 15*time.Second)
	if err != nil {
		_ = unix.Close(fd)
		return err
	}
	if err := connection.SetWriteDeadline(time.Now().Add(15 * time.Second)); err != nil {
		_ = unix.Close(fd)
		_ = connection.Close()
		return err
	}
	n, _, err := connection.(*net.UnixConn).WriteMsgUnix([]byte(pidfdOfferFrame), unix.UnixRights(fd), nil)
	_ = unix.Close(fd)
	_ = connection.Close()
	if err != nil || n != len(pidfdOfferFrame) {
		return errors.Join(errors.New("send self pidfd before exec"), err)
	}
	if release != nil {
		// The host observer attaches to this exact pidfd before releasing the
		// wrapper. A buffered signal channel also retains a release that arrived
		// between sendmsg and entering this wait.
		if err := waitForPIDFDOfferRelease(release, defaultStartupTimeout); err != nil {
			return err
		}
	}
	return unix.Exec(args[3], args[3:], os.Environ())
}

func waitForPIDFDOfferRelease(release <-chan os.Signal, timeout time.Duration) error {
	if release == nil || timeout <= 0 {
		return errors.New("observer attach release wait is not configured")
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	for {
		select {
		case signal := <-release:
			if signal == syscall.SIGCONT {
				return nil
			}
		case <-timer.C:
			return errors.New("timed out waiting for observer attach")
		}
	}
}

func canonicalAbsolute(path string) bool {
	return path != "" && filepath.IsAbs(path) && filepath.Clean(path) == path && path != "/"
}
func uuidMust(value string) uuid.UUID { parsed, _ := uuid.Parse(value); return parsed }
func imageDigestMatches(imageID string, repoDigests []string, expected string) bool {
	if imageID == expected {
		return true
	}
	for _, value := range repoDigests {
		if value == expected || strings.HasSuffix(value, "@"+expected) {
			return true
		}
	}
	return false
}
func getenvDefault(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}

func trustedExecutable(path string) error {
	if err := securefile.ValidateExecutable(path); err != nil {
		return errors.New("runtime launcher observer is not a trusted executable")
	}
	return nil
}

func trustedConfigFile(path string) error {
	info, err := os.Lstat(path)
	if err != nil {
		return err
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || stat.Uid != 0 || !info.Mode().IsRegular() || info.Mode().Perm()&0o022 != 0 || info.Mode()&os.ModeSymlink != 0 {
		return errors.New("runtime launcher kubeconfig is not root-owned and non-writable")
	}
	if _, err := securefile.ResolveTrustedDirectory(filepath.Dir(path)); err != nil {
		return fmt.Errorf("kubeconfig parent is not trusted: %w", err)
	}
	return nil
}

type pidfdOffer struct {
	listener      *net.UnixListener
	directory     string
	hostPath      string
	containerPath string
	identity      os.FileInfo
	uid, gid      uint32
}

func newPIDFDOffer(root, name string, uid, gid uint32) (*pidfdOffer, error) {
	directory := filepath.Join(root, name+"-offer")
	if err := os.Mkdir(directory, 0o750); err != nil {
		return nil, err
	}
	if err := os.Chown(directory, 0, int(gid)); err != nil {
		return nil, err
	}
	hostPath := filepath.Join(directory, "pidfd.sock")
	listener, err := net.ListenUnix("unixpacket", &net.UnixAddr{Name: hostPath, Net: "unixpacket"})
	if err != nil {
		return nil, err
	}
	raw, err := listener.SyscallConn()
	if err != nil {
		_ = listener.Close()
		return nil, err
	}
	var setupErr error
	err = raw.Control(func(fd uintptr) {
		setupErr = errors.Join(unix.SetsockoptInt(int(fd), unix.SOL_SOCKET, unix.SO_PASSCRED, 1),
			unix.SetsockoptInt(int(fd), unix.SOL_SOCKET, unix.SO_PASSPIDFD, 1))
	})
	if err != nil || setupErr != nil {
		_ = listener.Close()
		return nil, errors.Join(err, setupErr)
	}
	if err := os.Chown(hostPath, 0, int(gid)); err != nil {
		_ = listener.Close()
		return nil, err
	}
	if err := os.Chmod(hostPath, 0o660); err != nil {
		_ = listener.Close()
		return nil, err
	}
	identity, err := os.Lstat(hostPath)
	if err != nil {
		_ = listener.Close()
		return nil, err
	}
	listener.SetUnlinkOnClose(false)
	return &pidfdOffer{listener: listener, directory: directory, hostPath: hostPath, containerPath: "/vela-runtime/offer/pidfd.sock", identity: identity, uid: uid, gid: gid}, nil
}

func (offer *pidfdOffer) accept(ctx context.Context, tasks tasksapi.TasksClient, containerID string) (*os.File, error) {
	if ctx == nil || offer == nil || offer.listener == nil || tasks == nil || containerID == "" {
		return nil, errors.New("pidfd offer is unavailable")
	}
	ctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	deadline, _ := ctx.Deadline()
	if err := offer.listener.SetDeadline(deadline); err != nil {
		return nil, err
	}
	stopAccept := context.AfterFunc(ctx, func() { _ = offer.listener.SetDeadline(time.Now()) })
	defer stopAccept()
	connection, err := offer.listener.AcceptUnix()
	if err != nil {
		return nil, errors.Join(err, context.Cause(ctx))
	}
	defer connection.Close()
	if err := connection.SetDeadline(deadline); err != nil {
		return nil, err
	}
	stopRead := context.AfterFunc(ctx, func() { _ = connection.SetDeadline(time.Now()) })
	defer stopRead()
	raw, err := connection.SyscallConn()
	if err != nil {
		return nil, err
	}
	peerFD := -1
	var peer *unix.Ucred
	var peerErr error
	err = raw.Control(func(fd uintptr) {
		peer, peerErr = unix.GetsockoptUcred(int(fd), unix.SOL_SOCKET, unix.SO_PEERCRED)
		if peerErr == nil {
			peerFD, peerErr = unix.GetsockoptInt(int(fd), unix.SOL_SOCKET, unix.SO_PEERPIDFD)
		}
	})
	if peerFD >= 0 {
		defer unix.Close(peerFD)
	}
	if err != nil || peerErr != nil || peer == nil || peer.Uid != offer.uid || peer.Gid != offer.gid {
		return nil, runtimechannel.ErrIdentity
	}
	packet, sender, senderFD, offeredFD, err := runtimechannel.ReadProcessOffer(connection, len(pidfdOfferFrame))
	if err != nil {
		return nil, errors.Join(err, context.Cause(ctx))
	}
	defer unix.Close(senderFD)
	file := os.NewFile(uintptr(offeredFD), "runtime-self-pidfd")
	if !bytes.Equal(packet, []byte(pidfdOfferFrame)) || sender != *peer {
		_ = file.Close()
		return nil, runtimechannel.ErrIdentity
	}
	if err := errors.Join(runtimechannel.SameLiveProcess(peerFD, senderFD), runtimechannel.SameLiveProcess(senderFD, offeredFD)); err != nil {
		_ = file.Close()
		return nil, err
	}
	header, _ := metadata.FromOutgoingContext(ctx)
	header = header.Copy()
	header.Set("containerd-namespace", "k8s.io")
	status, err := tasks.Get(metadata.NewOutgoingContext(ctx, header), &tasksapi.GetRequest{ContainerID: containerID})
	if err != nil {
		_ = file.Close()
		return nil, err
	}
	if status.GetProcess().GetPid() != uint32(sender.Pid) || status.GetProcess().GetStatus().String() != "RUNNING" {
		_ = file.Close()
		return nil, errors.New("pidfd offer does not match running CRI task")
	}
	return file, nil
}

func (offer *pidfdOffer) Close() error {
	if offer == nil || offer.listener == nil {
		return nil
	}
	// Inspect before closing: UnixListener.Close may unlink the pathname, and
	// that could erase a replacement before we detect an identity mismatch.
	current, inspectErr := os.Lstat(offer.hostPath)
	identityMismatch := inspectErr != nil || offer.identity == nil || !os.SameFile(current, offer.identity)
	err := offer.listener.Close()
	if errors.Is(inspectErr, os.ErrNotExist) {
		return err
	}
	if identityMismatch {
		return errors.Join(err, errPidfdOfferSocketReplaced)
	}
	// Recheck after close to avoid removing a path that was swapped while the
	// listener was being closed.
	after, afterErr := os.Lstat(offer.hostPath)
	if errors.Is(afterErr, os.ErrNotExist) {
		return err
	}
	if afterErr == nil && (offer.identity == nil || !os.SameFile(after, offer.identity)) {
		return errors.Join(err, errPidfdOfferSocketReplaced)
	}
	return errors.Join(err, afterErr, os.Remove(offer.hostPath))
}

var _ = sort.Strings
