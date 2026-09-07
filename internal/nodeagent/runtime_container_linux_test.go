package nodeagent

import (
	"context"
	"errors"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	runtimev1 "k8s.io/cri-api/pkg/apis/runtime/v1"
)

type containerCRIServer struct {
	runtimev1.UnimplementedRuntimeServiceServer
	mu            sync.Mutex
	version       *runtimev1.VersionResponse
	listed        *runtimev1.ListContainersResponse
	container     *runtimev1.ContainerStatusResponse
	sandbox       *runtimev1.PodSandboxStatusResponse
	calls         map[string]int
	hook          func(string, int) error
	target        RuntimeContainerTarget
	resolveTarget bool
}

func (server *containerCRIServer) call(method string) error {
	server.calls[method]++
	if server.hook != nil {
		return server.hook(method, server.calls[method])
	}
	return nil
}

func (server *containerCRIServer) Version(_ context.Context, request *runtimev1.VersionRequest) (*runtimev1.VersionResponse, error) {
	server.mu.Lock()
	defer server.mu.Unlock()
	if err := server.call("Version"); err != nil {
		return nil, err
	}
	if request.Version != "0.1.0" {
		return nil, status.Error(codes.InvalidArgument, "wrong kubelet version request")
	}
	return proto.CloneOf(server.version), nil
}

func (server *containerCRIServer) ListContainers(_ context.Context, request *runtimev1.ListContainersRequest) (*runtimev1.ListContainersResponse, error) {
	server.mu.Lock()
	defer server.mu.Unlock()
	if err := server.call("ListContainers"); err != nil {
		return nil, err
	}
	if !proto.Equal(request.Filter, &runtimev1.ContainerFilter{Id: server.target.ContainerID, PodSandboxId: server.target.SandboxID}) &&
		(!server.resolveTarget || !proto.Equal(request.Filter, &runtimev1.ContainerFilter{Id: server.target.ContainerID})) {
		return nil, status.Error(codes.InvalidArgument, "container query was not exact")
	}
	return proto.CloneOf(server.listed), nil
}

func (server *containerCRIServer) ContainerStatus(_ context.Context, request *runtimev1.ContainerStatusRequest) (*runtimev1.ContainerStatusResponse, error) {
	server.mu.Lock()
	defer server.mu.Unlock()
	if err := server.call("ContainerStatus"); err != nil {
		return nil, err
	}
	if request.ContainerId != server.target.ContainerID || request.Verbose {
		return nil, status.Error(codes.InvalidArgument, "container query requested unstructured or unrelated data")
	}
	return proto.CloneOf(server.container), nil
}

func (server *containerCRIServer) PodSandboxStatus(_ context.Context, request *runtimev1.PodSandboxStatusRequest) (*runtimev1.PodSandboxStatusResponse, error) {
	server.mu.Lock()
	defer server.mu.Unlock()
	if err := server.call("PodSandboxStatus"); err != nil {
		return nil, err
	}
	if request.PodSandboxId != server.target.SandboxID || request.Verbose {
		return nil, status.Error(codes.InvalidArgument, "sandbox query requested unstructured or unrelated data")
	}
	return proto.CloneOf(server.sandbox), nil
}

func newContainerCRIServer() *containerCRIServer {
	now := time.Now().UTC().Add(-time.Minute)
	target := RuntimeContainerTarget{ContainerID: strings.Repeat("a", 64), SandboxID: strings.Repeat("b", 64),
		PodUID: uuid.New(), PodNamespace: "vela-system", PodName: "vela-member", ContainerName: "model-runtime", ContainerAttempt: 3}
	metadata := &runtimev1.ContainerMetadata{Name: target.ContainerName, Attempt: target.ContainerAttempt}
	return &containerCRIServer{
		target: target, calls: map[string]int{},
		version: &runtimev1.VersionResponse{Version: "0.1.0", RuntimeName: "vela-cri-mock", RuntimeVersion: "0.0.1", RuntimeApiVersion: "v1"},
		listed: &runtimev1.ListContainersResponse{Containers: []*runtimev1.Container{{Id: target.ContainerID, PodSandboxId: target.SandboxID,
			Metadata: proto.CloneOf(metadata), CreatedAt: now.UnixNano(), State: runtimev1.ContainerState_CONTAINER_RUNNING,
			ImageRef: "sha256:" + strings.Repeat("c", 64)}}},
		container: &runtimev1.ContainerStatusResponse{Status: &runtimev1.ContainerStatus{Id: target.ContainerID,
			Metadata: proto.CloneOf(metadata), CreatedAt: now.UnixNano(), StartedAt: now.Add(time.Second).UnixNano(),
			State: runtimev1.ContainerState_CONTAINER_RUNNING, ImageRef: "sha256:" + strings.Repeat("c", 64)}},
		sandbox: &runtimev1.PodSandboxStatusResponse{Status: &runtimev1.PodSandboxStatus{Id: target.SandboxID,
			Metadata:  &runtimev1.PodSandboxMetadata{Uid: target.PodUID.String(), Namespace: target.PodNamespace, Name: target.PodName, Attempt: 2},
			CreatedAt: now.Add(-time.Second).UnixNano(), State: runtimev1.PodSandboxState_SANDBOX_READY,
			Linux: &runtimev1.LinuxPodSandboxStatus{Namespaces: &runtimev1.Namespace{Options: &runtimev1.NamespaceOption{Pid: runtimev1.NamespaceMode_CONTAINER}}}}},
	}
}

func serveContainerCRI(t *testing.T, service *containerCRIServer, register ...func(*grpc.Server)) string {
	t.Helper()
	parent, err := filepath.EvalSymlinks(os.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	directory, err := os.MkdirTemp(parent, "vcri-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(directory) })
	path := filepath.Join(directory, "cri.sock")
	listener, err := net.Listen("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, 0o600); err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer(grpc.UnaryInterceptor(func(ctx context.Context, request any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		switch info.FullMethod {
		case "/runtime.v1.RuntimeService/Version", "/runtime.v1.RuntimeService/ListContainers",
			"/runtime.v1.RuntimeService/ContainerStatus", "/runtime.v1.RuntimeService/PodSandboxStatus":
			return handler(ctx, request)
		case "/containerd.services.tasks.v1.Tasks/Get":
			if len(register) != 0 {
				return handler(ctx, request)
			}
			fallthrough
		default:
			t.Errorf("observer attempted a non-observation RPC: %s", info.FullMethod)
			return nil, status.Error(codes.PermissionDenied, "read only")
		}
	}))
	runtimev1.RegisterRuntimeServiceServer(server, service)
	for _, registration := range register {
		registration(server)
	}
	done := make(chan error, 1)
	go func() { done <- server.Serve(listener) }()
	t.Cleanup(func() {
		server.Stop()
		_ = listener.Close()
		if err := <-done; err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			t.Errorf("CRI fixture shutdown: %v", err)
		}
	})
	return path
}

func dialContainerCRI(t *testing.T, path string) *RuntimeContainerObserver {
	t.Helper()
	observer, err := dialRuntimeContainerObserver(t.Context(), RuntimeContainerObserverConfig{SocketPath: path, NodeIdentity: "node-1"},
		uint32(os.Geteuid()), func() (string, error) { return "12345678-1234-4234-8234-123456789abc", nil })
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = observer.Close() })
	return observer
}

func TestRuntimeContainerObservationKeepsCRIStatesDistinct(t *testing.T) {
	for _, state := range []runtimev1.ContainerState{runtimev1.ContainerState_CONTAINER_CREATED, runtimev1.ContainerState_CONTAINER_RUNNING, runtimev1.ContainerState_CONTAINER_EXITED} {
		for _, namespace := range []runtimev1.NamespaceMode{runtimev1.NamespaceMode_CONTAINER, runtimev1.NamespaceMode_POD, runtimev1.NamespaceMode_NODE, runtimev1.NamespaceMode_TARGET} {
			t.Run(state.String()+"/"+namespace.String(), func(t *testing.T) {
				fixture := newContainerCRIServer()
				fixture.listed.Containers[0].State, fixture.container.Status.State = state, state
				if state == runtimev1.ContainerState_CONTAINER_CREATED {
					fixture.container.Status.StartedAt = 0
				}
				if state == runtimev1.ContainerState_CONTAINER_EXITED {
					fixture.container.Status.FinishedAt = fixture.container.Status.StartedAt + int64(time.Second)
					fixture.container.Status.ExitCode = 72
				}
				fixture.sandbox.Status.Linux.Namespaces.Options.Pid = namespace
				if namespace == runtimev1.NamespaceMode_TARGET {
					fixture.sandbox.Status.Linux.Namespaces.Options.TargetId = strings.Repeat("d", 64)
				}
				observer := dialContainerCRI(t, serveContainerCRI(t, fixture))
				result, err := observer.Inspect(t.Context(), fixture.target)
				if err != nil || result.SchemaVersion != 1 || result.Target != fixture.target || result.ContainerState != state.String() ||
					result.SandboxPIDNamespace != namespace.String() || result.SandboxPIDTargetID != fixture.sandbox.Status.Linux.Namespaces.Options.TargetId ||
					result.NodeIdentity != "node-1" || result.BootID.String() != "12345678-1234-4234-8234-123456789abc" ||
					result.RuntimeName != "vela-cri-mock" || result.RuntimeAPIVersion != "v1" || result.RuntimeVersion != "0.0.1" ||
					result.CreatedAt.UnixNano() != fixture.container.Status.CreatedAt || result.ExitCode != fixture.container.Status.ExitCode ||
					result.SandboxAttempt != 2 || result.ObservedThrough.Before(result.ObservedFrom) {
					t.Fatalf("CRI observation lost exact identity or overstated namespace scope: %+v %v", result, err)
				}
				fixture.mu.Lock()
				defer fixture.mu.Unlock()
				if fixture.calls["Version"] != 1 || fixture.calls["ListContainers"] != 2 || fixture.calls["ContainerStatus"] != 2 || fixture.calls["PodSandboxStatus"] != 2 {
					t.Fatalf("observation omitted consistency reads: %+v", fixture.calls)
				}
			})
		}
	}
}

func TestRuntimeContainerObservationRejectsIncompleteAndMismatchedEvidence(t *testing.T) {
	faults := map[string]func(*containerCRIServer){
		"unsupported-api":   func(f *containerCRIServer) { f.version.RuntimeApiVersion = "v1alpha2" },
		"missing-version":   func(f *containerCRIServer) { f.version.RuntimeVersion = "" },
		"missing-container": func(f *containerCRIServer) { f.listed.Containers = nil },
		"nil-container":     func(f *containerCRIServer) { f.listed.Containers[0] = nil },
		"ambiguous-container": func(f *containerCRIServer) {
			f.listed.Containers = append(f.listed.Containers, proto.CloneOf(f.listed.Containers[0]))
		},
		"prefix-match":         func(f *containerCRIServer) { f.listed.Containers[0].Id += "1" },
		"wrong-sandbox":        func(f *containerCRIServer) { f.listed.Containers[0].PodSandboxId = strings.Repeat("e", 64) },
		"wrong-attempt":        func(f *containerCRIServer) { f.listed.Containers[0].Metadata.Attempt++ },
		"wrong-container-name": func(f *containerCRIServer) { f.listed.Containers[0].Metadata.Name = "worker-agent" },
		"missing-status":       func(f *containerCRIServer) { f.container.Status = nil },
		"wrong-status-id":      func(f *containerCRIServer) { f.container.Status.Id = strings.Repeat("e", 64) },
		"missing-metadata":     func(f *containerCRIServer) { f.container.Status.Metadata = nil },
		"changed-created-at":   func(f *containerCRIServer) { f.container.Status.CreatedAt++ },
		"changed-state":        func(f *containerCRIServer) { f.container.Status.State = runtimev1.ContainerState_CONTAINER_EXITED },
		"unknown-state": func(f *containerCRIServer) {
			f.listed.Containers[0].State, f.container.Status.State = runtimev1.ContainerState_CONTAINER_UNKNOWN, runtimev1.ContainerState_CONTAINER_UNKNOWN
		},
		"missing-image":       func(f *containerCRIServer) { f.listed.Containers[0].ImageRef, f.container.Status.ImageRef = "", "" },
		"missing-start":       func(f *containerCRIServer) { f.container.Status.StartedAt = 0 },
		"running-with-exit":   func(f *containerCRIServer) { f.container.Status.FinishedAt = f.container.Status.StartedAt },
		"future-start":        func(f *containerCRIServer) { f.container.Status.StartedAt = time.Now().Add(time.Hour).UnixNano() },
		"negative-created-at": func(f *containerCRIServer) { f.container.Status.CreatedAt, f.listed.Containers[0].CreatedAt = -1, -1 },
		"missing-pod":         func(f *containerCRIServer) { f.sandbox.Status = nil },
		"wrong-pod-uid":       func(f *containerCRIServer) { f.sandbox.Status.Metadata.Uid = uuid.NewString() },
		"wrong-pod-name":      func(f *containerCRIServer) { f.sandbox.Status.Metadata.Name = "other" },
		"wrong-pod-namespace": func(f *containerCRIServer) { f.sandbox.Status.Metadata.Namespace = "other" },
		"wrong-pod-id":        func(f *containerCRIServer) { f.sandbox.Status.Id = strings.Repeat("e", 64) },
		"newer-sandbox":       func(f *containerCRIServer) { f.sandbox.Status.CreatedAt = f.container.Status.CreatedAt + 1 },
		"missing-namespace":   func(f *containerCRIServer) { f.sandbox.Status.Linux.Namespaces.Options = nil },
		"unknown-namespace": func(f *containerCRIServer) {
			f.sandbox.Status.Linux.Namespaces.Options.Pid = runtimev1.NamespaceMode(99)
		},
		"target-without-identity": func(f *containerCRIServer) {
			f.sandbox.Status.Linux.Namespaces.Options.Pid = runtimev1.NamespaceMode_TARGET
		},
		"unexpected-namespace-target": func(f *containerCRIServer) {
			f.sandbox.Status.Linux.Namespaces.Options.TargetId = strings.Repeat("d", 64)
		},
		"container-disappears": func(f *containerCRIServer) {
			f.hook = func(method string, _ int) error {
				if method == "ContainerStatus" {
					return status.Error(codes.NotFound, "garbage collected")
				}
				return nil
			}
		},
		"sandbox-disappears": func(f *containerCRIServer) {
			f.hook = func(method string, _ int) error {
				if method == "PodSandboxStatus" {
					return status.Error(codes.NotFound, "garbage collected")
				}
				return nil
			}
		},
		"list-unavailable": func(f *containerCRIServer) {
			f.hook = func(method string, _ int) error {
				if method == "ListContainers" {
					return status.Error(codes.Unavailable, "runtime unavailable")
				}
				return nil
			}
		},
		"changing-status": func(f *containerCRIServer) {
			f.hook = func(method string, count int) error {
				if method == "ListContainers" && count == 2 {
					f.container.Status.Labels = map[string]string{"changed": "true"}
				}
				return nil
			}
		},
		"changing-sandbox": func(f *containerCRIServer) {
			f.hook = func(method string, count int) error {
				if method == "PodSandboxStatus" && count == 2 {
					f.sandbox.Status.Metadata.Attempt++
				}
				return nil
			}
		},
	}
	for name, fault := range faults {
		t.Run(name, func(t *testing.T) {
			fixture := newContainerCRIServer()
			fault(fixture)
			observer := dialContainerCRI(t, serveContainerCRI(t, fixture))
			if result, err := observer.Inspect(t.Context(), fixture.target); err == nil || result != (RuntimeContainerObservation{}) {
				t.Fatalf("invalid CRI evidence produced an observation: %+v %v", result, err)
			}
		})
	}
}

func TestRuntimeContainerObservationRejectsLostObserverIdentity(t *testing.T) {
	for _, fault := range []string{"boot-change", "boot-missing", "boot-invalid", "clock-regression", "closed", "closed-during-read", "socket-replaced", "socket-replaced-during-read", "directory-permissions", "canceled", "late-cancel", "oversized-response"} {
		t.Run(fault, func(t *testing.T) {
			fixture := newContainerCRIServer()
			path := serveContainerCRI(t, fixture)
			observer := dialContainerCRI(t, path)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			switch fault {
			case "boot-change":
				reads := 0
				observer.bootID = func() (string, error) {
					reads++
					if reads == 1 {
						return "12345678-1234-4234-8234-123456789abc", nil
					}
					return uuid.NewString(), nil
				}
			case "boot-missing":
				observer.bootID = func() (string, error) { return "", os.ErrNotExist }
			case "boot-invalid":
				observer.bootID = func() (string, error) { return "1234", nil }
			case "clock-regression":
				now, reads := time.Now(), 0
				observer.clock = func() time.Time { reads++; return now.Add(-time.Duration(reads) * time.Second) }
			case "closed":
				if err := observer.Close(); err != nil {
					t.Fatal(err)
				}
			case "directory-permissions":
				if err := os.Chmod(filepath.Dir(path), 0o777); err != nil {
					t.Fatal(err)
				}
			case "closed-during-read":
				fixture.hook = func(method string, count int) error {
					if method == "PodSandboxStatus" && count == 2 {
						return observer.Close()
					}
					return nil
				}
			case "socket-replaced", "socket-replaced-during-read":
				replace := func() error {
					if err := os.Rename(path, path+".old"); err != nil {
						return err
					}
					replacement, err := net.Listen("unix", path)
					if err != nil {
						return err
					}
					t.Cleanup(func() { _ = replacement.Close() })
					return os.Chmod(path, 0o600)
				}
				if fault == "socket-replaced-during-read" {
					fixture.hook = func(method string, count int) error {
						if method == "PodSandboxStatus" && count == 2 {
							if err := replace(); err != nil {
								t.Error(err)
								return err
							}
						}
						return nil
					}
				} else if err := replace(); err != nil {
					t.Fatal(err)
				}
			case "canceled":
				cancel()
			case "late-cancel":
				fixture.hook = func(method string, count int) error {
					if method == "PodSandboxStatus" && count == 2 {
						cancel()
					}
					return nil
				}
			case "oversized-response":
				fixture.container.Info = map[string]string{"ignored": strings.Repeat("x", 2<<20)}
			}
			if result, err := observer.Inspect(ctx, fixture.target); err == nil || result != (RuntimeContainerObservation{}) {
				t.Fatalf("lost observation boundary produced evidence: %+v %v", result, err)
			}
		})
	}
}

func TestRuntimeContainerObserverRejectsUntrustedSocket(t *testing.T) {
	for _, fault := range []string{"wrong-owner", "world-access", "symlink", "regular-file", "relative", "network-address"} {
		t.Run(fault, func(t *testing.T) {
			path := serveContainerCRI(t, newContainerCRIServer())
			owner := uint32(os.Geteuid())
			switch fault {
			case "wrong-owner":
				owner++
			case "world-access":
				if err := os.Chmod(path, 0o666); err != nil {
					t.Fatal(err)
				}
			case "symlink":
				if err := os.Symlink(path, path+".link"); err != nil {
					t.Fatal(err)
				}
				path += ".link"
			case "regular-file":
				path += ".file"
				if err := os.WriteFile(path, []byte("not a socket"), 0o600); err != nil {
					t.Fatal(err)
				}
			case "relative":
				path = "cri.sock"
			case "network-address":
				path = "tcp://127.0.0.1:1234"
			}
			observer, err := dialRuntimeContainerObserver(t.Context(), RuntimeContainerObserverConfig{SocketPath: path, NodeIdentity: "node-1"}, owner,
				func() (string, error) { return uuid.NewString(), nil })
			if err == nil || observer != nil {
				t.Fatalf("untrusted CRI endpoint was accepted: %v", err)
			}
		})
	}
}
