package releaseartifacts

import (
	"context"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/google/go-containerregistry/pkg/authn"
	"github.com/google/go-containerregistry/pkg/name"
	"github.com/google/go-containerregistry/pkg/registry"
	"github.com/google/go-containerregistry/pkg/v1/empty"
	"github.com/google/go-containerregistry/pkg/v1/layout"
	"github.com/google/go-containerregistry/pkg/v1/mutate"
	"github.com/google/go-containerregistry/pkg/v1/random"
	"github.com/google/go-containerregistry/pkg/v1/types"
	"github.com/vivym/vela/internal/releasebundle"
)

func TestPublishVelaImageLayoutsUsesOnlyDigestsAndIsIdempotent(t *testing.T) {
	t.Parallel()

	var manifestIdentifiers []string
	var manifestMu sync.Mutex
	handler := recordManifestWrites(
		registry.New(registry.Logger(log.New(io.Discard, "", 0))),
		func(identifier string) {
			manifestMu.Lock()
			defer manifestMu.Unlock()
			manifestIdentifiers = append(manifestIdentifiers, identifier)
		},
	)
	server := httptest.NewServer(handler)
	defer server.Close()

	layoutRoot, candidate, manifest := writePublicationFixture(t, server.URL)
	client := insecureAnonymousRegistryClient()
	first, err := publishVelaImageLayouts(
		context.Background(), layoutRoot, candidate, manifest, client,
	)
	if err != nil {
		t.Fatalf("publish Vela image layouts: %v", err)
	}
	second, err := publishVelaImageLayouts(
		context.Background(), layoutRoot, candidate, manifest, client,
	)
	if err != nil {
		t.Fatalf("repeat Vela image publication: %v", err)
	}
	if !slices.Equal(first.Images, second.Images) ||
		first.SchemaVersion != velaRegistryPublicationSchemaVersion ||
		first.Revision != manifest.Revision || len(first.Images) != velaImageCount {
		t.Fatalf("idempotent publication receipts = %#v and %#v", first, second)
	}

	manifestMu.Lock()
	defer manifestMu.Unlock()
	if len(manifestIdentifiers) < velaImageCount || len(manifestIdentifiers) > 2*velaImageCount {
		t.Fatalf("manifest PUT identifiers = %v", manifestIdentifiers)
	}
	for _, identifier := range manifestIdentifiers {
		if !strings.HasPrefix(identifier, "sha256:") {
			t.Fatalf("mutable registry identifier was written: %q", identifier)
		}
	}
}

func TestPublishVelaImageLayoutsRetriesAfterPartialFailure(t *testing.T) {
	t.Parallel()

	var reject atomic.Bool
	reject.Store(true)
	registryHandler := registry.New(registry.Logger(log.New(io.Discard, "", 0)))
	handler := http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if reject.Load() && request.Method == http.MethodPut &&
			strings.Contains(request.URL.Path, "/vela-stage-worker-agent/manifests/") {
			http.Error(response, "publication disabled", http.StatusBadRequest)
			return
		}
		registryHandler.ServeHTTP(response, request)
	})
	server := httptest.NewServer(handler)
	defer server.Close()

	layoutRoot, candidate, manifest := writePublicationFixture(t, server.URL)
	client := insecureAnonymousRegistryClient()
	if _, err := publishVelaImageLayouts(
		context.Background(), layoutRoot, candidate, manifest, client,
	); err == nil {
		t.Fatal("partial registry publication succeeded")
	}

	reject.Store(false)
	receipt, err := publishVelaImageLayouts(
		context.Background(), layoutRoot, candidate, manifest, client,
	)
	if err != nil {
		t.Fatalf("retry partial registry publication: %v", err)
	}
	if len(receipt.Images) != velaImageCount {
		t.Fatalf("publication receipt images = %#v", receipt.Images)
	}
}

func TestPublishVelaImageLayoutsRejectsChangedRemoteManifestBytes(t *testing.T) {
	t.Parallel()

	registryHandler := registry.New(registry.Logger(log.New(io.Discard, "", 0)))
	handler := http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodGet ||
			!strings.Contains(request.URL.Path, "/vela-control/manifests/") {
			registryHandler.ServeHTTP(response, request)
			return
		}
		recorded := httptest.NewRecorder()
		registryHandler.ServeHTTP(recorded, request)
		for key, values := range recorded.Header() {
			response.Header()[key] = slices.Clone(values)
		}
		response.WriteHeader(recorded.Code)
		tampered := slices.Clone(recorded.Body.Bytes())
		tampered[0] = '['
		_, _ = response.Write(tampered)
	})
	server := httptest.NewServer(handler)
	defer server.Close()

	layoutRoot, candidate, manifest := writePublicationFixture(t, server.URL)
	if _, err := publishVelaImageLayouts(
		context.Background(), layoutRoot, candidate, manifest,
		insecureAnonymousRegistryClient(),
	); err == nil || !strings.Contains(err.Error(), "does not match requested digest") {
		t.Fatalf("changed remote manifest error = %v", err)
	}
}

func TestPublishVelaImageLayoutsRejectsChangedLocalLayoutBeforeNetwork(t *testing.T) {
	t.Parallel()

	var requests atomic.Int64
	registryHandler := registry.New(registry.Logger(log.New(io.Discard, "", 0)))
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		requests.Add(1)
		registryHandler.ServeHTTP(response, request)
	}))
	defer server.Close()

	layoutRoot, candidate, manifest := writePublicationFixture(t, server.URL)
	digest := strings.Split(manifest.OCIManifests[0].Image, "@sha256:")[1]
	manifestBlob := filepath.Join(
		layoutRoot, "vela-control", "blobs", "sha256", digest,
	)
	encoded, err := os.ReadFile(manifestBlob)
	if err != nil {
		t.Fatalf("read local layout manifest: %v", err)
	}
	if err := os.WriteFile(manifestBlob, append(encoded, ' '), 0o600); err != nil {
		t.Fatalf("change local layout manifest: %v", err)
	}

	if _, err := publishVelaImageLayouts(
		context.Background(), layoutRoot, candidate, manifest,
		insecureAnonymousRegistryClient(),
	); err == nil || !strings.Contains(err.Error(), "does not match local manifest bytes") {
		t.Fatalf("changed local layout error = %v", err)
	}
	if requests.Load() != 0 {
		t.Fatalf("registry requests before local validation = %d", requests.Load())
	}
}

func TestPublishVelaImageLayoutsUsesKeychainWithoutCredentialReceipt(t *testing.T) {
	t.Parallel()

	const username = "release-publisher"
	const password = "publication-secret"
	registryHandler := registry.New(registry.Logger(log.New(io.Discard, "", 0)))
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		actualUsername, actualPassword, authenticated := request.BasicAuth()
		if !authenticated || actualUsername != username || actualPassword != password {
			response.Header().Set("WWW-Authenticate", `Basic realm="vela-test"`)
			http.Error(response, "authentication required", http.StatusUnauthorized)
			return
		}
		registryHandler.ServeHTTP(response, request)
	}))
	defer server.Close()

	layoutRoot, candidate, manifest := writePublicationFixture(t, server.URL)
	keychain := &recordingRegistryKeychain{username: username, password: password}
	receipt, err := publishVelaImageLayouts(
		context.Background(), layoutRoot, candidate, manifest,
		&remoteVelaImageRegistry{
			keychain:    keychain,
			transport:   http.DefaultTransport,
			nameOptions: []name.Option{name.StrictValidation, name.Insecure},
		},
	)
	if err != nil {
		t.Fatalf("publish with registry keychain: %v", err)
	}
	if keychain.resolutions.Load() == 0 {
		t.Fatal("registry keychain was not resolved")
	}
	encoded, err := json.Marshal(receipt)
	if err != nil {
		t.Fatalf("encode registry receipt: %v", err)
	}
	if strings.Contains(string(encoded), username) || strings.Contains(string(encoded), password) {
		t.Fatalf("registry receipt contains credentials: %s", encoded)
	}
}

func writePublicationFixture(
	t *testing.T,
	registryURL string,
) (string, string, velaImageArtifactManifest) {
	t.Helper()
	root := t.TempDir()
	layoutRoot := filepath.Join(root, "layouts")
	candidate := filepath.Join(root, "candidate")
	if err := os.Mkdir(layoutRoot, 0o700); err != nil {
		t.Fatalf("create layout root: %v", err)
	}
	if err := os.Mkdir(candidate, 0o700); err != nil {
		t.Fatalf("create candidate: %v", err)
	}
	manifest := velaImageArtifactManifest{
		SchemaVersion: velaImageArtifactSchemaVersion,
		Revision:      "release-test-r4",
		OCIManifests:  make([]releasebundle.OCIManifestInput, 0, velaImageCount),
	}
	prefix := strings.TrimPrefix(registryURL, "http://") + "/vela"
	for _, specification := range velaImageSpecifications() {
		image, err := random.Image(1024, 1)
		if err != nil {
			t.Fatalf("create random image: %v", err)
		}
		image = mutate.MediaType(image, types.OCIManifestSchema1)
		imageLayout, err := layout.Write(
			filepath.Join(layoutRoot, specification.name), empty.Index,
		)
		if err != nil {
			t.Fatalf("create image layout: %v", err)
		}
		if err := imageLayout.AppendImage(image); err != nil {
			t.Fatalf("append image to layout: %v", err)
		}
		digest, err := image.Digest()
		if err != nil {
			t.Fatalf("digest image: %v", err)
		}
		rawManifest, err := image.RawManifest()
		if err != nil {
			t.Fatalf("read image manifest: %v", err)
		}
		manifestReference := specification.name + ".manifest.json"
		if err := os.WriteFile(
			filepath.Join(candidate, manifestReference), rawManifest, 0o600,
		); err != nil {
			t.Fatalf("write image manifest: %v", err)
		}
		manifest.OCIManifests = append(manifest.OCIManifests, releasebundle.OCIManifestInput{
			Image: prefix + "/" + specification.name + "@" + digest.String(),
			Ref:   manifestReference, ConfigRef: specification.name + ".config.json",
		})
	}
	return layoutRoot, candidate, manifest
}

func insecureAnonymousRegistryClient() velaImageRegistryClient {
	return &remoteVelaImageRegistry{
		authenticator: authn.Anonymous,
		transport:     http.DefaultTransport,
		nameOptions:   []name.Option{name.StrictValidation, name.Insecure},
	}
}

func recordManifestWrites(next http.Handler, record func(string)) http.Handler {
	return http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if request.Method == http.MethodPut {
			if _, identifier, found := strings.Cut(request.URL.Path, "/manifests/"); found {
				record(identifier)
			}
		}
		next.ServeHTTP(response, request)
	})
}

type recordingRegistryKeychain struct {
	username    string
	password    string
	resolutions atomic.Int64
}

func (keychain *recordingRegistryKeychain) Resolve(
	authn.Resource,
) (authn.Authenticator, error) {
	keychain.resolutions.Add(1)
	return authn.FromConfig(authn.AuthConfig{
		Username: keychain.username,
		Password: keychain.password,
	}), nil
}
