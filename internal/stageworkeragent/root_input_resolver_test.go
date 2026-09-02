package stageworkeragent_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/stageauthority"
	"github.com/vivym/vela/internal/stageworkeragent"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/protobuf/proto"
)

func TestHTTPSRootInputResolverMaterializesAndRevalidatesExactContent(t *testing.T) {
	payload := []byte("customer supplied reference frame")
	digest := sha256.Sum256(payload)
	var requests atomic.Int32
	server := httptest.NewTLSServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		requests.Add(1)
		if request.URL.Query().Get("signature") != "worker-only" ||
			request.Header.Get("Accept-Encoding") != "identity" {
			t.Errorf("root input request = %s headers=%v", request.URL, request.Header)
		}
		writer.Header().Set("Content-Length", "33")
		_, _ = writer.Write(payload)
	}))
	defer server.Close()

	fixture := newSingleMemberMaterializationFixture(t)
	assignment := rootInputAssignment(t, fixture.assignment, digest, int64(len(payload)), server.URL+"/material?signature=worker-only")
	inputRoot := t.TempDir()
	resolver, err := stageworkeragent.NewHTTPSRootInputResolver(
		stageworkeragent.HTTPSRootInputResolverConfig{
			InputRoot: inputRoot,
			Client:    server.Client(),
		},
	)
	if err != nil {
		t.Fatalf("NewHTTPSRootInputResolver: %v", err)
	}
	if err := resolver.Resolve(context.Background(), assignment); err != nil {
		t.Fatalf("Resolve root input: %v", err)
	}
	if err := resolver.Resolve(context.Background(), assignment); err != nil {
		t.Fatalf("revalidate root input: %v", err)
	}
	if requests.Load() != 1 {
		t.Fatalf("root input downloads = %d, want 1", requests.Load())
	}
	relative, err := stageworkeragent.RootInputRelativePath(
		uuid.MustParse(assignment.GetAuthority().GetStageRunId()), 0, digest,
	)
	if err != nil {
		t.Fatalf("RootInputRelativePath: %v", err)
	}
	materialized, err := os.ReadFile(filepath.Join(inputRoot, relative))
	if err != nil || !bytes.Equal(materialized, payload) {
		t.Fatalf("materialized root input = %q error=%v", materialized, err)
	}
}

func TestHTTPSRootInputResolverRejectsDigestDriftWithoutPublishingFile(t *testing.T) {
	payload := []byte("changed content")
	expected := sha256.Sum256([]byte("expected content"))
	server := httptest.NewTLSServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		_, _ = writer.Write(payload)
	}))
	defer server.Close()
	fixture := newSingleMemberMaterializationFixture(t)
	assignment := rootInputAssignment(t, fixture.assignment, expected, int64(len(payload)), server.URL+"/material")
	inputRoot := t.TempDir()
	resolver, err := stageworkeragent.NewHTTPSRootInputResolver(
		stageworkeragent.HTTPSRootInputResolverConfig{InputRoot: inputRoot, Client: server.Client()},
	)
	if err != nil {
		t.Fatalf("NewHTTPSRootInputResolver: %v", err)
	}
	if err := resolver.Resolve(context.Background(), assignment); err == nil {
		t.Fatal("Resolve accepted root input digest drift")
	}
	relative, _ := stageworkeragent.RootInputRelativePath(
		uuid.MustParse(assignment.GetAuthority().GetStageRunId()), 0, expected,
	)
	if _, err := os.Stat(filepath.Join(inputRoot, relative)); !os.IsNotExist(err) {
		t.Fatalf("digest-drift material was published: %v", err)
	}
}

func rootInputAssignment(
	t *testing.T,
	base *velav1.StageAssignment,
	digest [sha256.Size]byte,
	sizeBytes int64,
	downloadURL string,
) *velav1.StageAssignment {
	t.Helper()
	assignment := proto.Clone(base).(*velav1.StageAssignment)
	assignment.ExecutionSpec.RootInputs = []*velav1.StageRootInputMaterial{{
		ConditionIndex: 0, Uri: "vela://uploads/reference", Sha256: digest[:], SizeBytes: sizeBytes,
	}}
	assignment.RootInputFetches = []*velav1.StageRootInputFetch{{
		ConditionIndex: 0, Sha256: digest[:], DownloadUrl: downloadURL,
	}}
	executionDigest, err := stageauthority.ExecutionSpecDigest(assignment.ExecutionSpec)
	if err != nil {
		t.Fatalf("ExecutionSpecDigest: %v", err)
	}
	unsigned := proto.Clone(assignment.Authority).(*velav1.StageAuthority)
	unsigned.ExecutionSpecDigest = executionDigest[:]
	unsigned.Signature = nil
	signer, err := stageauthority.NewSigner(map[string][]byte{
		"single-stage-key": bytes.Repeat([]byte{0x7b}, 32),
	})
	if err != nil {
		t.Fatalf("NewSigner: %v", err)
	}
	assignment.Authority, err = signer.Sign(unsigned)
	if err != nil {
		t.Fatalf("sign root-input Assignment: %v", err)
	}
	return assignment
}
