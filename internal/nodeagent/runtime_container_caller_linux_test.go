package nodeagent

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	tasksapi "github.com/containerd/containerd/api/services/tasks/v1"
	tasktypes "github.com/containerd/containerd/api/types/task"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type runtimeCallerTaskServer struct {
	tasksapi.UnimplementedTasksServer
	mu      sync.Mutex
	process *tasktypes.Process
	calls   int
	hook    func(int) error
	target  RuntimeContainerTarget
}

func (server *runtimeCallerTaskServer) Get(ctx context.Context, request *tasksapi.GetRequest) (*tasksapi.GetResponse, error) {
	server.mu.Lock()
	defer server.mu.Unlock()
	headers, _ := metadata.FromIncomingContext(ctx)
	values := headers.Get("containerd-namespace")
	if len(values) != 1 || values[0] != "k8s.io" || request.ContainerID != server.target.ContainerID || request.ExecID != "" {
		return nil, status.Error(codes.InvalidArgument, "task query did not select exact Kubernetes container init")
	}
	server.calls++
	if server.hook != nil {
		if err := server.hook(server.calls); err != nil {
			return nil, err
		}
	}
	return &tasksapi.GetResponse{Process: proto.CloneOf(server.process)}, nil
}

func TestRuntimeContainerCallerCorrelation(t *testing.T) {
	for _, scenario := range []string{"matching", "nil-exit-time", "wrong-task-id", "wrong-pid", "paused", "unknown-state", "exit-time", "malformed-time",
		"missing-task", "lost-task", "changing-task", "changing-cri", "different-boot", "unsupported-runtime", "closed-caller", "closed-observer", "canceled"} {
		t.Run(scenario, func(t *testing.T) {
			connection, _, _ := runtimeCallerConnection(t, "normal", "unixpacket", true)
			caller, err := ReceiveRuntimeCaller(t.Context(), connection, RuntimeCallerCredentials{UID: 65532, GID: 65532})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = caller.Close() })
			process, err := caller.Inspect(t.Context())
			if err != nil || process.NamespacePID != 1 || process.NamespaceDepth != 2 {
				t.Fatalf("fixture is not an actual namespace init: %+v %v", process, err)
			}
			cri, tasks, observer := runtimeCallerObserverFixture(t, process)
			ctx, cancel := context.WithCancel(metadata.NewOutgoingContext(t.Context(), metadata.Pairs("containerd-namespace", "untrusted-context")))
			defer cancel()
			tasks.mu.Lock()
			cri.mu.Lock()
			switch scenario {
			case "matching":
			case "nil-exit-time":
				tasks.process.ExitedAt = nil
			case "wrong-task-id":
				tasks.process.ID = strings.Repeat("e", 64)
			case "wrong-pid":
				tasks.process.Pid++
			case "paused":
				tasks.process.Status = tasktypes.Status_PAUSED
			case "unknown-state":
				tasks.process.Status = tasktypes.Status_UNKNOWN
			case "exit-time":
				tasks.process.ExitedAt = timestamppb.Now()
			case "malformed-time":
				tasks.process.ExitedAt.Nanos = -1
			case "missing-task":
				tasks.process = nil
			case "lost-task":
				tasks.hook = func(int) error { return status.Error(codes.NotFound, "removed") }
			case "changing-task":
				tasks.hook = func(count int) error {
					if count == 2 {
						tasks.process.Pid++
					}
					return nil
				}
			case "changing-cri":
				tasks.hook = func(int) error {
					cri.mu.Lock()
					defer cri.mu.Unlock()
					cri.container.Status.ImageRef = "sha256:" + strings.Repeat("f", 64)
					cri.listed.Containers[0].ImageRef = cri.container.Status.ImageRef
					return nil
				}
			case "different-boot":
				otherBoot := uuid.NewString()
				observer.bootID = func() (string, error) { return otherBoot, nil }
			case "unsupported-runtime":
				cri.version.RuntimeVersion = "v2.3.2"
			case "closed-caller":
				tasks.hook = func(int) error { return caller.Close() }
			case "closed-observer":
				tasks.hook = func(int) error { return observer.Close() }
			case "canceled":
				cancel()
			}
			cri.mu.Unlock()
			tasks.mu.Unlock()
			result, err := observer.ObserveCaller(ctx, cri.target, caller)
			tasks.mu.Lock()
			calls := tasks.calls
			tasks.mu.Unlock()
			if scenario == "matching" || scenario == "nil-exit-time" {
				if err != nil || result.SchemaVersion != 1 || result.Container.Target != cri.target || result.Process.HostPID != process.HostPID ||
					result.ObservedFrom.IsZero() || result.ObservedThrough.Before(result.ObservedFrom) || calls != 2 {
					t.Fatalf("matching namespace owner failed correlation: %+v %v", result, err)
				}
			} else if err == nil || result != (RuntimeContainerCallerObservation{}) {
				t.Fatalf("invalid task/CRI/caller yielded correlation: %+v %v", result, err)
			}
		})
	}
}

func TestRuntimeContainerCallerRejectsNonInit(t *testing.T) {
	connection, _, _ := runtimeCallerConnection(t, "normal", "unixpacket")
	caller, err := ReceiveRuntimeCaller(t.Context(), connection, RuntimeCallerCredentials{UID: 65532, GID: 65532})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = caller.Close() })
	process, err := caller.Inspect(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	cri, _, observer := runtimeCallerObserverFixture(t, process)
	if result, err := observer.ObserveCaller(t.Context(), cri.target, caller); !errors.Is(err, ErrRuntimeContainerCaller) || result != (RuntimeContainerCallerObservation{}) {
		t.Fatal("host-namespace caller was accepted as a container lifetime owner")
	}
}

func runtimeCallerObserverFixture(t *testing.T, process RuntimeCallerObservation) (*containerCRIServer, *runtimeCallerTaskServer, *RuntimeContainerObserver) {
	t.Helper()
	cri := newContainerCRIServer()
	cri.version.RuntimeName, cri.version.RuntimeVersion = "containerd", "v2.3.1"
	tasks := &runtimeCallerTaskServer{target: cri.target, process: &tasktypes.Process{ID: cri.target.ContainerID,
		Pid: uint32(process.HostPID), Status: tasktypes.Status_RUNNING, ExitedAt: timestamppb.New(time.Time{})}}
	socket := serveContainerCRI(t, cri, func(server *grpc.Server) { tasksapi.RegisterTasksServer(server, tasks) })
	observer, err := DialRuntimeContainerObserver(t.Context(), RuntimeContainerObserverConfig{SocketPath: socket, NodeIdentity: "cpu-node"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = observer.Close() })
	return cri, tasks, observer
}
