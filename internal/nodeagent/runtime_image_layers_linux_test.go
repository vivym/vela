//go:build integration && linux

package nodeagent

import (
	"archive/tar"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"testing"
	"time"

	containersapi "github.com/containerd/containerd/api/services/containers/v1"
	imagesapi "github.com/containerd/containerd/api/services/images/v1"
	mountsapi "github.com/containerd/containerd/api/services/mounts/v1"
	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/empty"
	"github.com/google/go-containerregistry/pkg/v1/layout"
	"github.com/google/go-containerregistry/pkg/v1/mutate"
	"github.com/google/go-containerregistry/pkg/v1/tarball"
	specs "github.com/opencontainers/runtime-spec/specs-go"
	"golang.org/x/sys/unix"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

type executableLayerEntry struct {
	name, source, trailer, link string
	kind                        byte
}

// Compare the actual unpacker and running kernel file with a flattened tar.
// These are synthetic images, not approved release or startup-grant evidence.
func TestRuntimeImageLayerExecutableIdentity(t *testing.T) {
	if os.Getenv("VELA_TEST_CONTAINERD_SANDBOX") != "1" {
		t.Skip("requires the explicitly enabled disposable containerd CPU sandbox")
	}
	fixture := startProcessContainerd(t)
	md, _ := metadata.FromOutgoingContext(fixture.ctx)
	observer, err := DialRuntimeImageObserver(t.Context(), RuntimeImageObserverConfig{
		RuntimeContainerObserverConfig: RuntimeContainerObserverConfig{SocketPath: fixture.socket, NodeIdentity: "cpu-image-node"},
		Namespace:                      md.Get("containerd-namespace")[0], Snapshotter: "native",
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = observer.Close() })
	original, originalSize := runtimeExecutableDigest(t, fixture.binary)
	const trailer = "vela-image-upper-layer-v1"
	replacement := runtimeImageDigestWithTrailer(t, fixture.binary, trailer)
	for _, scenario := range []string{"regular-overlay", "whiteout-recreate", "opaque-directory", "hardlink-copy-up", "relative-symlink"} {
		t.Run(scenario, func(t *testing.T) {
			directory := filepath.Join(fixture.root, "image-"+scenario)
			if err := os.Mkdir(directory, 0o700); err != nil {
				t.Fatal(err)
			}
			entrypoint := "probe"
			lower := []executableLayerEntry{{name: "probe", source: fixture.binary}}
			upper := []executableLayerEntry{{name: "probe", source: fixture.binary, trailer: trailer}}
			expected, expectedSize := replacement, originalSize+int64(len(trailer))
			switch scenario {
			case "whiteout-recreate":
				upper = append([]executableLayerEntry{{name: ".wh.probe"}}, upper...)
			case "opaque-directory":
				entrypoint = "bin/probe"
				lower = []executableLayerEntry{{name: "bin", kind: tar.TypeDir},
					{name: entrypoint, source: fixture.binary}, {name: "bin/removed", trailer: "lower-only"}}
				upper = []executableLayerEntry{{name: "bin", kind: tar.TypeDir}, {name: "bin/.wh..wh..opq"},
					{name: entrypoint, source: fixture.binary, trailer: trailer}}
			case "hardlink-copy-up", "relative-symlink":
				kind := byte(tar.TypeLink)
				if scenario == "relative-symlink" {
					kind = tar.TypeSymlink
				} else {
					expected, expectedSize = original, originalSize
				}
				lower = []executableLayerEntry{{name: "payload", source: fixture.binary}, {name: "probe", kind: kind, link: "payload"}}
				upper = []executableLayerEntry{{name: "payload", source: fixture.binary, trailer: trailer}}
			}
			lower = append([]executableLayerEntry{{name: "proc", kind: tar.TypeDir},
				{name: "proof", kind: tar.TypeDir}, {name: "dev", kind: tar.TypeDir}}, lower...)
			image, err := mutate.AppendLayers(empty.Image, runtimeExecutableLayer(t, directory, "lower", lower),
				runtimeExecutableLayer(t, directory, "upper", upper))
			if err != nil {
				t.Fatal(err)
			}
			config, err := image.ConfigFile()
			if err != nil {
				t.Fatal(err)
			}
			config.Architecture, config.OS = runtime.GOARCH, "linux"
			config.Config = v1.Config{Entrypoint: []string{"/" + entrypoint}, User: "65532:65532"}
			image, err = mutate.ConfigFile(image, config)
			if err != nil {
				t.Fatal(err)
			}
			imageDigest, err := image.Digest()
			if err != nil {
				t.Fatal(err)
			}
			mount := fixture.mountExecutableImage(t, directory, scenario, image)
			configDigest, err := image.ConfigName()
			if err != nil {
				t.Fatal(err)
			}
			target := RuntimeImageTarget{ManifestDigest: imageDigest.String(), ConfigDigest: configDigest.String(), ExecutablePath: "/" + entrypoint}
			imageObservation, err := observer.InspectExecutable(t.Context(), target)
			if err != nil || imageObservation.Digest != expected || imageObservation.SizeBytes != expectedSize ||
				imageObservation.Target != target || imageObservation.NodeIdentity != "cpu-image-node" || imageObservation.BootID == [16]byte{} {
				t.Fatalf("production image reader differs from independently expected bytes: %+v %v", imageObservation, err)
			}
			assertRuntimeImageResourcesReleased(t, fixture)
			if scenario == "regular-overlay" {
				testRuntimeImageObserverFailures(t, fixture, observer, target)
				t.Run("concurrent-observations", func(t *testing.T) {
					results := make(chan error, 2)
					for range 2 {
						go func() {
							result, err := observer.InspectExecutable(t.Context(), target)
							if err == nil && (result.Digest != expected || result.SizeBytes != expectedSize) {
								err = fmt.Errorf("concurrent observation returned different image bytes")
							}
							results <- err
						}()
					}
					for range 2 {
						if err := <-results; err != nil {
							t.Error(err)
						}
					}
					assertRuntimeImageResourcesReleased(t, fixture)
				})
			}
			var filesystem unix.Statfs_t
			if err := unix.Statfs(mount, &filesystem); err != nil || filesystem.Flags&unix.ST_RDONLY == 0 {
				t.Fatalf("image view is not read-only: %v", err)
			}
			mounted, mountedSize := runtimeExecutableDigest(t, filepath.Join(mount, entrypoint))
			if mounted != expected || mountedSize != expectedSize {
				t.Fatal("native snapshot differs from the expected layer replacement semantics")
			}
			if scenario == "opaque-directory" {
				if _, err := os.Lstat(filepath.Join(mount, "bin/removed")); !os.IsNotExist(err) {
					t.Fatal("opaque directory retained lower-layer content")
				}
			}

			container, listener := fixture.create(t, "owner", "")
			var spec specs.Spec
			if err := json.Unmarshal(container.Spec.Value, &spec); err != nil {
				t.Fatal(err)
			}
			spec.Root.Path, spec.Process.Args[0] = mount, "/"+entrypoint
			// The executable must come from the image view, never the old
			// fixture's host-binary bind mount.
			spec.Mounts = slices.DeleteFunc(spec.Mounts, func(mount specs.Mount) bool { return mount.Destination == "/probe" })
			spec.Mounts = append(spec.Mounts, specs.Mount{Destination: "/dev", Type: "tmpfs", Source: "tmpfs",
				Options: []string{"nosuid", "noexec", "mode=0755", "size=65536"}})
			container.Spec = encodeContainerdSpec(t, spec)
			if _, err := fixture.containers.Update(fixture.ctx, &containersapi.UpdateContainerRequest{
				Container: container, UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"spec"}},
			}); err != nil {
				t.Fatal(err)
			}
			_, caller := fixture.start(t, container.ID, listener)
			observed, err := caller.proof.InspectExecutable(t.Context())
			if err != nil || observed.Digest != expected || observed.SizeBytes != expectedSize || observed.Process.NamespacePID != 1 {
				t.Fatalf("actual image executable differs from its independently expected bytes: %+v %v", observed, err)
			}
			fixture.stop(t, container.ID, caller)

			flatRoot := runtimeFlattenedImage(t, directory, image)
			flatFile, err := os.Open(filepath.Join(flatRoot, entrypoint))
			if scenario == "whiteout-recreate" {
				if !os.IsNotExist(err) {
					if flatFile != nil {
						_ = flatFile.Close()
					}
					t.Fatalf("reassess the pinned flattener whiteout counterexample: %v", err)
				}
				t.Logf("image=%s kernel=%x; mutate.Extract omitted the recreated executable", imageDigest, observed.Digest)
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			_ = flatFile.Close()
			flatDigest, _ := runtimeExecutableDigest(t, filepath.Join(flatRoot, entrypoint))
			if scenario == "hardlink-copy-up" {
				if flatDigest != replacement || flatDigest == observed.Digest {
					t.Fatal("reassess the pinned flattener hardlink counterexample")
				}
			} else if flatDigest != observed.Digest {
				t.Fatal("flattened positive control differs from the running executable")
			}
			t.Logf("image=%s kernel=%x flattened=%x", imageDigest, observed.Digest, flatDigest)
		})
	}
}

func runtimeExecutableLayer(t *testing.T, directory, label string, entries []executableLayerEntry) v1.Layer {
	t.Helper()
	path := filepath.Join(directory, label+".tar")
	file, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = file.Close() })
	writer := tar.NewWriter(file)
	for _, entry := range entries {
		size := int64(len(entry.trailer))
		var source *os.File
		if entry.source != "" {
			source, err = os.Open(entry.source)
			if err != nil {
				t.Fatal(err)
			}
			info, err := source.Stat()
			if err != nil {
				t.Fatal(err)
			}
			size += info.Size()
		}
		if err := writer.WriteHeader(&tar.Header{Name: entry.name, Typeflag: entry.kind, Linkname: entry.link, Mode: 0o755, Size: size}); err != nil {
			t.Fatal(err)
		}
		if source != nil {
			_, copyErr := io.Copy(writer, source)
			closeErr := source.Close()
			if copyErr != nil || closeErr != nil {
				t.Fatalf("write layer content: %v %v", copyErr, closeErr)
			}
		}
		if _, err := io.WriteString(writer, entry.trailer); err != nil {
			t.Fatal(err)
		}
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	// Keep fixture compression outside race instrumentation; the large test
	// executable is payload, not the implementation of the image compressor.
	compressed, err := os.Create(path + ".gz")
	if err != nil {
		t.Fatal(err)
	}
	var stderr bytes.Buffer
	command := exec.CommandContext(t.Context(), "gzip", "-1", "--stdout", path)
	command.Stdout, command.Stderr = compressed, &stderr
	compressErr := command.Run()
	closeErr := compressed.Close()
	if compressErr != nil || closeErr != nil {
		t.Fatalf("compress CPU fixture layer: %v %v %s", compressErr, closeErr, stderr.Bytes())
	}
	layer, err := tarball.LayerFromFile(compressed.Name())
	if err != nil {
		t.Fatal(err)
	}
	return layer
}

func (fixture *containerdProcessFixture) mountExecutableImage(t *testing.T, directory, scenario string, image v1.Image) string {
	t.Helper()
	repository := "docker.io/vela/executable-" + scenario
	oci, err := layout.Write(filepath.Join(directory, "oci"), empty.Index)
	if err != nil {
		t.Fatal(err)
	}
	if err := oci.AppendImage(image, layout.WithAnnotations(map[string]string{"io.containerd.image.name": repository + ":cpu-fixture"})); err != nil {
		t.Fatal(err)
	}
	digest, err := image.Digest()
	if err != nil {
		t.Fatal(err)
	}
	archive := filepath.Join(directory, "image.tar")
	if output, err := exec.CommandContext(t.Context(), "tar", "--create", "--file", archive, "--directory", string(oci), ".").CombinedOutput(); err != nil {
		t.Fatalf("archive exact OCI layout: %s %v", output, err)
	}
	fixture.imageCommand(t, "import", "--local", "--snapshotter", "native", "--digests", "--base-name", repository, archive)
	reference := repository + "@" + digest.String()
	imported, err := imagesapi.NewImagesClient(fixture.connection).Get(fixture.ctx, &imagesapi.GetImageRequest{Name: reference})
	if err != nil || imported.GetImage().GetTarget().GetDigest() != digest.String() {
		t.Fatalf("import changed the exact image manifest identity: %v", err)
	}
	mount := filepath.Join(directory, "rootfs")
	if err := os.Mkdir(mount, 0o755); err != nil {
		t.Fatal(err)
	}
	fixture.imageCommand(t, "mount", "--snapshotter", "native", reference, mount)
	t.Cleanup(func() {
		fixture.imageCommand(t, "unmount", mount)
		md, _ := metadata.FromOutgoingContext(fixture.ctx)
		ctx, cancel := context.WithTimeout(metadata.NewOutgoingContext(context.Background(), md), 5*time.Second)
		defer cancel()
		if _, err := mountsapi.NewMountsClient(fixture.connection).Deactivate(ctx, &mountsapi.DeactivateRequest{Name: mount}); err != nil {
			t.Errorf("release private image mount activation: %v", err)
		}
		fixture.imageCommand(t, "unmount", "--rm", "--snapshotter", "native", mount)
	})
	return mount
}

func (fixture *containerdProcessFixture) imageCommand(t *testing.T, arguments ...string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	md, _ := metadata.FromOutgoingContext(fixture.ctx)
	namespaces := md.Get("containerd-namespace")
	if len(namespaces) != 1 {
		t.Fatal("image fixture requires one private containerd namespace")
	}
	args := append([]string{"--address", fixture.socket, "--namespace", namespaces[0], "images"}, arguments...)
	if output, err := exec.CommandContext(ctx, "ctr", args...).CombinedOutput(); err != nil {
		t.Fatalf("private containerd image operation: %s %v", output, err)
	}
}

func runtimeFlattenedImage(t *testing.T, directory string, image v1.Image) string {
	t.Helper()
	archive, err := os.Create(filepath.Join(directory, "flattened.tar"))
	if err != nil {
		t.Fatal(err)
	}
	reader := mutate.Extract(image)
	_, copyErr := io.Copy(archive, reader)
	readerErr, fileErr := reader.Close(), archive.Close()
	if copyErr != nil || readerErr != nil || fileErr != nil {
		t.Fatalf("flatten fixture image: %v %v %v", copyErr, readerErr, fileErr)
	}
	root := filepath.Join(directory, "flattened")
	if err := os.Mkdir(root, 0o700); err != nil {
		t.Fatal(err)
	}
	// Only the fixed synthetic entries above reach the host tar utility.
	if output, err := exec.CommandContext(t.Context(), "tar", "--extract", "--file", archive.Name(), "--directory", root).CombinedOutput(); err != nil {
		t.Fatalf("materialize flattened fixture: %s %v", output, err)
	}
	return root
}

func runtimeImageDigestWithTrailer(t *testing.T, path, trailer string) [sha256.Size]byte {
	t.Helper()
	file, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = file.Close() }()
	hash := sha256.New()
	if _, err := io.Copy(hash, io.MultiReader(file, strings.NewReader(trailer))); err != nil {
		t.Fatal(err)
	}
	return [sha256.Size]byte(hash.Sum(nil))
}
