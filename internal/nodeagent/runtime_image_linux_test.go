package nodeagent

import (
	"context"
	"crypto/sha256"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	contentapi "github.com/containerd/containerd/api/services/content/v1"
	leasesapi "github.com/containerd/containerd/api/services/leases/v1"
	"github.com/opencontainers/go-digest"
	"golang.org/x/sys/unix"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"
)

func TestRuntimeImageTargetBounds(t *testing.T) {
	target := RuntimeImageTarget{ManifestDigest: "sha256:" + strings.Repeat("a", 64), ConfigDigest: "sha256:" + strings.Repeat("b", 64), ExecutablePath: "/bin/probe"}
	if err := target.Validate(); err != nil {
		t.Fatal(err)
	}
	for _, value := range []string{"", "bin/probe", "/", "/bin/../probe", "/bin//probe", "/probe\x00", "/\xff", "/" + strings.Repeat("x", 4096)} {
		candidate := target
		candidate.ExecutablePath = value
		if err := candidate.Validate(); err == nil {
			t.Errorf("accepted invalid executable path %q", value)
		}
	}
	for _, value := range []string{"", "sha256:" + strings.Repeat("A", 64), "sha256:" + strings.Repeat("g", 64), "sha512:" + strings.Repeat("a", 128), "fixture:tag"} {
		candidate := target
		candidate.ManifestDigest = value
		if err := candidate.Validate(); err == nil {
			t.Errorf("accepted invalid manifest digest %q", value)
		}
		candidate = target
		candidate.ConfigDigest = value
		if err := candidate.Validate(); err == nil {
			t.Errorf("accepted invalid config digest %q", value)
		}
	}
	for _, snapshotter := range []string{"", "overlayfs", "native/../overlayfs"} {
		if _, err := DialRuntimeImageObserver(t.Context(), RuntimeImageObserverConfig{Namespace: "k8s.io", Snapshotter: snapshotter}); err == nil {
			t.Errorf("accepted unqualified snapshotter %q", snapshotter)
		}
	}
}

func TestRuntimeImageContentBounds(t *testing.T) {
	for _, scenario := range []string{"valid", "chunked", "wrong-digest", "wrong-info-digest", "wrong-size", "oversized-info", "missing-info",
		"gap", "overlap", "empty-part", "nil-part", "truncated", "oversized-part", "duplicate-json", "trailing-json", "invalid-utf8", "stream-error"} {
		t.Run(scenario, func(t *testing.T) {
			data := []byte(`{"key":"value"}`)
			switch scenario {
			case "duplicate-json":
				data = []byte(`{"key":1,"key":2}`)
			case "trailing-json":
				data = []byte(`{} {}`)
			case "invalid-utf8":
				data = []byte("{\"key\":\"\xff\"}")
			}
			hash := digest.FromBytes(data).String()
			client := &runtimeImageContentFixture{info: &contentapi.Info{Digest: hash, Size: int64(len(data))},
				parts: []*contentapi.ReadContentResponse{{Data: data}}}
			expectedSize := int64(len(data))
			switch scenario {
			case "chunked":
				client.parts = []*contentapi.ReadContentResponse{{Data: data[:3]}, {Offset: 3, Data: data[3:]}}
			case "wrong-digest":
				client.parts[0].Data = []byte(strings.Repeat("x", len(data)))
			case "wrong-info-digest":
				client.info.Digest = digest.FromString("other").String()
			case "wrong-size":
				expectedSize++
			case "oversized-info":
				client.info.Size = maximumRuntimeImageJSONBytes + 1
			case "missing-info":
				client.info = nil
			case "gap":
				client.parts[0].Offset = 1
			case "overlap":
				client.parts = []*contentapi.ReadContentResponse{{Data: data[:3]}, {Offset: 2, Data: data[3:]}}
			case "empty-part":
				client.parts[0].Data = nil
			case "nil-part":
				client.parts[0] = nil
			case "truncated":
				client.parts[0].Data = data[:len(data)-1]
			case "oversized-part":
				client.parts[0].Data = append(data, ' ')
			case "stream-error":
				client.streamErr = errors.New("fixture failed stream")
			}
			observer := RuntimeImageObserver{content: client, leases: &runtimeImageLeaseFixture{}}
			actual, err := observer.readJSON(t.Context(), "private-lease", hash, expectedSize)
			if scenario == "valid" || scenario == "chunked" {
				if err != nil || string(actual) != string(data) {
					t.Fatalf("valid JSON read failed: %q %v", actual, err)
				}
			} else if err == nil || actual != nil {
				t.Fatalf("invalid content returned evidence: %q %v", actual, err)
			}
		})
	}
}

type runtimeImageContentFixture struct {
	contentapi.ContentClient
	info      *contentapi.Info
	parts     []*contentapi.ReadContentResponse
	streamErr error
}

func (client *runtimeImageContentFixture) Info(context.Context, *contentapi.InfoRequest, ...grpc.CallOption) (*contentapi.InfoResponse, error) {
	return &contentapi.InfoResponse{Info: client.info}, nil
}

func (client *runtimeImageContentFixture) Read(context.Context, *contentapi.ReadContentRequest, ...grpc.CallOption) (contentapi.Content_ReadClient, error) {
	return &runtimeImageStreamFixture{parts: client.parts, err: client.streamErr}, nil
}

type runtimeImageStreamFixture struct {
	grpc.ClientStream
	parts []*contentapi.ReadContentResponse
	err   error
}

func (stream *runtimeImageStreamFixture) Recv() (*contentapi.ReadContentResponse, error) {
	if stream.err != nil {
		return nil, stream.err
	}
	if len(stream.parts) == 0 {
		return nil, io.EOF
	}
	part := stream.parts[0]
	stream.parts = stream.parts[1:]
	return part, nil
}

type runtimeImageLeaseFixture struct{ leasesapi.LeasesClient }

func (*runtimeImageLeaseFixture) AddResource(context.Context, *leasesapi.AddResourceRequest, ...grpc.CallOption) (*emptypb.Empty, error) {
	return &emptypb.Empty{}, nil
}

func TestRuntimeImageFileResolution(t *testing.T) {
	if os.Getenv("VELA_TEST_CONTAINERD_SANDBOX") != "1" {
		t.Skip("requires the explicitly enabled disposable CPU mount sandbox")
	}
	directory := t.TempDir()
	source, mounted, nested := filepath.Join(directory, "source"), filepath.Join(directory, "view"), filepath.Join(directory, "outside")
	for _, value := range []string{source, mounted, nested, filepath.Join(source, "nested")} {
		if err := os.Mkdir(value, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	for _, value := range []struct {
		name string
		mode os.FileMode
		data string
	}{
		{"payload", 0o755, "image-payload"}, {"empty", 0o755, ""}, {"nonexec", 0o600, "non-executable"},
	} {
		if err := os.WriteFile(filepath.Join(source, value.name), []byte(value.data), value.mode); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(nested, "payload"), []byte("outside-payload"), 0o755); err != nil {
		t.Fatal(err)
	}
	for name, target := range map[string]string{"absolute": "/payload", "relative": "payload", "host-path": filepath.Join(nested, "payload"), "loop": "loop", "cross-mount": "nested/payload"} {
		if err := os.Symlink(target, filepath.Join(source, name)); err != nil {
			t.Fatal(err)
		}
	}
	if err := unix.Mkfifo(filepath.Join(source, "fifo"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := unix.Mknod(filepath.Join(source, "device"), unix.S_IFCHR|0o755, int(unix.Mkdev(1, 3))); err != nil {
		t.Fatal(err)
	}
	large, err := os.OpenFile(filepath.Join(source, "oversized"), os.O_CREATE|os.O_WRONLY, 0o755)
	if err != nil {
		t.Fatal(err)
	}
	if err := errors.Join(large.Truncate(maximumRuntimeExecutableBytes+1), large.Close()); err != nil {
		t.Fatal(err)
	}
	if err := unix.Mount(nested, filepath.Join(source, "nested"), "", unix.MS_BIND, ""); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := unix.Unmount(filepath.Join(source, "nested"), 0); err != nil {
			t.Error(err)
		}
	})
	if err := unix.Mount(source, mounted, "", unix.MS_BIND|unix.MS_REC, ""); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := unix.Unmount(filepath.Join(mounted, "nested"), 0); err != nil {
			t.Error(err)
		}
		if err := unix.Unmount(mounted, 0); err != nil {
			t.Error(err)
		}
	})
	root, err := os.Open(mounted)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = root.Close() })
	if _, _, err := measureRuntimeImageExecutable(t.Context(), root, "/payload"); err == nil {
		t.Fatal("accepted writable root")
	}
	if err := unix.Mount("", mounted, "", unix.MS_BIND|unix.MS_REMOUNT|unix.MS_RDONLY, ""); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"payload", "absolute", "relative"} {
		actual, file, err := measureRuntimeImageExecutable(t.Context(), root, "/"+name)
		if err != nil || actual != sha256.Sum256([]byte("image-payload")) || file.size != 13 {
			t.Fatalf("image-root symlink semantics failed for %s: %x %+v %v", name, actual, file, err)
		}
	}
	for _, name := range []string{"host-path", "loop", "nested/payload", "cross-mount", "empty", "nonexec", "fifo", "device", "oversized", "missing"} {
		if _, _, err := measureRuntimeImageExecutable(t.Context(), root, "/"+name); err == nil {
			t.Errorf("accepted invalid image executable %s", name)
		}
	}
}
