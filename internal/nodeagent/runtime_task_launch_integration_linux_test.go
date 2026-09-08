//go:build integration && linux

package nodeagent

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"syscall"
	"testing"

	containersapi "github.com/containerd/containerd/api/services/containers/v1"
	"golang.org/x/sys/unix"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

func verifyRuntimeTaskLaunchBundle(t *testing.T, fixture *containerdProcessFixture, observer *RuntimeContainerObserver, target RuntimeContainerTarget, caller *RuntimeCaller) {
	t.Helper()
	state := filepath.Join(fixture.root, "state")
	launch, err := observer.ObserveTaskLaunch(t.Context(), state, target, caller)
	if err != nil {
		t.Fatal(err)
	}
	configuration, err := launch.Configuration()
	if err != nil || configuration.Process.User.UID != launch.Caller.Process.UID || len(configuration.Process.Args) == 0 || configuration.Process.Args[0] != "/probe" {
		t.Fatalf("task launch is not the actual CRI init configuration: %v", err)
	}
	path := filepath.Join(state, "io.containerd.runtime.v2.task", "k8s.io", target.ContainerID, "config.json")
	original, err := os.ReadFile(path)
	if err != nil || launch.ConfigDigest != sha256.Sum256(original) || launch.ConfigBytes != int64(len(original)) {
		t.Fatalf("bundle digest differs from actual daemon file: %v", err)
	}
	configuration.Process.Args[0] = "/caller-mutated-copy"
	fresh, err := launch.Configuration()
	if err != nil || fresh.Process.Args[0] != "/probe" {
		t.Fatal("Configuration exposed mutable retained bytes")
	}

	ctx := metadata.NewOutgoingContext(t.Context(), metadata.Pairs("containerd-namespace", "k8s.io"))
	current, err := fixture.containers.Get(ctx, &containersapi.GetContainerRequest{ID: target.ContainerID})
	if err != nil {
		t.Fatal(err)
	}
	changed := proto.CloneOf(current.Container)
	changed.Spec = encodeContainerdSpec(t, configuration)
	if _, err := fixture.containers.Update(ctx, &containersapi.UpdateContainerRequest{Container: changed, UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"spec"}}}); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if _, err := fixture.containers.Update(ctx, &containersapi.UpdateContainerRequest{Container: current.Container, UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"spec"}}}); err != nil {
			t.Error(err)
		}
	}()
	observed, err := observer.ObserveTaskLaunch(t.Context(), state, target, caller)
	if err != nil || observed.ConfigDigest != launch.ConfigDigest || observed.ConfigInode != launch.ConfigInode {
		t.Fatalf("mutable metadata replaced the actual launch bundle: %v", err)
	}
	actual, err := observed.Configuration()
	if err != nil || !reflect.DeepEqual(actual, fresh) {
		t.Fatal("bundle configuration changed with Containers.Update")
	}
	metadataReply, err := fixture.containers.Get(ctx, &containersapi.GetContainerRequest{ID: target.ContainerID})
	if err != nil || !proto.Equal(metadataReply.GetContainer().GetSpec(), changed.Spec) {
		t.Fatal("metadata counterexample did not take effect")
	}
	// A non-root process sharing the host-side mount view still cannot open
	// or replace the daemon's private bundle, even if it knows its full path.
	probe := exec.CommandContext(t.Context(), fixture.binary, "-test.run=^TestRuntimeTaskBundleWriteHelper$", "-test.v")
	probe.Env = []string{"VELA_TASK_BUNDLE_WRITE_PROBE=" + path}
	probe.SysProcAttr = &syscall.SysProcAttr{Credential: &syscall.Credential{Uid: 65532, Gid: 65532}}
	output, err := probe.CombinedOutput()
	if err != nil || !bytes.Contains(output, []byte("--- PASS: TestRuntimeTaskBundleWriteHelper")) {
		t.Fatalf("untrusted writer exclusion failed: %s %v", output, err)
	}

	if result, err := observer.ObserveTaskLaunch(t.Context(), fixture.dataRoot, target, caller); err == nil || result != nil {
		t.Fatal("another directory was accepted as this task's state root")
	}
	for _, fault := range []string{"writable", "wrong-owner", "missing", "oversize", "symlink", "hardlink", "fifo", "duplicate-key", "unknown-key", "wrong-init-pid"} {
		t.Run("task-bundle-"+fault, func(t *testing.T) {
			pidPath := filepath.Join(filepath.Dir(path), "init.pid")
			pidBytes, err := os.ReadFile(pidPath)
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				_ = os.Remove(path)
				if err := os.WriteFile(path, original, 0o644); err != nil {
					t.Error(err)
				}
				if err := os.WriteFile(pidPath, pidBytes, 0o600); err != nil {
					t.Error(err)
				}
			}()
			switch fault {
			case "writable":
				err = os.Chmod(path, 0o666)
			case "wrong-owner":
				err = os.Chown(path, 65532, 65532)
			case "missing":
				err = os.Remove(path)
			case "oversize":
				err = os.WriteFile(path, make([]byte, (1<<20)+1), 0o644)
			case "symlink", "hardlink", "fifo":
				if err := os.Remove(path); err != nil {
					t.Fatal(err)
				}
				switch fault {
				case "symlink":
					err = os.Symlink(pidPath, path)
				case "hardlink":
					err = os.Link(pidPath, path)
				case "fifo":
					err = unix.Mkfifo(path, 0o600)
				}
			case "duplicate-key":
				var document map[string]json.RawMessage
				if err := json.Unmarshal(original, &document); err != nil {
					t.Fatal(err)
				}
				duplicate := append([]byte(`{"ociVersion":`), document["ociVersion"]...)
				duplicate = append(duplicate, ',')
				duplicate = append(duplicate, original[1:]...)
				err = os.WriteFile(path, duplicate, 0o644)
			case "unknown-key":
				err = os.WriteFile(path, append([]byte(`{"unrecognized":true,`), original[1:]...), 0o644)
			case "wrong-init-pid":
				err = os.WriteFile(pidPath, []byte("1"), 0o600)
			}
			if err != nil {
				t.Fatal(err)
			}
			if result, err := observer.ObserveTaskLaunch(t.Context(), state, target, caller); err == nil || result != nil {
				t.Fatal("untrusted or mismatched task bundle produced a launch observation")
			}
		})
	}
	t.Logf("actual task bundle config sha256=%x; metadata update did not alter launch; non-root writes, wrong state root and 10 invalid bundle cases rejected", launch.ConfigDigest)
}

func TestRuntimeTaskBundleWriteHelper(t *testing.T) {
	path := os.Getenv("VELA_TASK_BUNDLE_WRITE_PROBE")
	if path == "" {
		t.Skip("non-root bundle writer helper")
	}
	if os.Geteuid() == 0 {
		t.Fatal("bundle writer probe must be non-root")
	}
	file, err := os.OpenFile(path, os.O_WRONLY, 0)
	if file != nil {
		_ = file.Close()
	}
	if !errors.Is(err, os.ErrPermission) {
		t.Fatalf("bundle write open was not denied: %v", err)
	}
	if err := os.Rename(path, path+".stolen"); !errors.Is(err, os.ErrPermission) {
		t.Fatalf("bundle replacement was not denied: %v", err)
	}
}
