//go:build integration

package integration_test

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/vivym/vela/internal/artifactstore"
)

// Exercise S3's signature verifier behind a TLS gateway. Merely replacing the
// hostname in an already signed internal URL must not pass this test.
func TestArtifactDownloadThroughGatewayPreservesSignedVersion(t *testing.T) {
	ctx := context.Background()
	fixture := newMinIOFixture(t, "vela-artifacts")
	fixture.enableVersioning(t)
	target, err := url.Parse(fixture.endpoint)
	if err != nil {
		t.Fatal(err)
	}
	proxy := httputil.NewSingleHostReverseProxy(target)
	// NewSingleHostReverseProxy preserves the original Host, as APISIX must.
	gateway := httptest.NewTLSServer(proxy)
	t.Cleanup(gateway.Close)
	configuration := fixture.config
	configuration.DownloadEndpoint = gateway.URL
	store, err := artifactstore.NewS3(configuration)
	if err != nil {
		t.Fatal(err)
	}
	const key = "artifacts/org/project/job/attempt/artifact/video.mp4"
	const complete = "complete original video and audio bytes"
	original, err := fixture.admin.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(fixture.bucket), Key: aws.String(key),
		Body: strings.NewReader(complete), ContentType: aws.String("video/mp4"),
	})
	if err != nil {
		t.Fatal(err)
	}
	replacement, err := fixture.admin.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(fixture.bucket), Key: aws.String(key),
		Body: strings.NewReader("later version"), ContentType: aws.String("video/mp4"),
	})
	if err != nil {
		t.Fatal(err)
	}
	signed, err := store.PresignExactVersion(ctx, key, aws.ToString(original.VersionId))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(signed.URL, gateway.URL+"/") {
		t.Fatal("download URL bypasses gateway")
	}
	response, err := gateway.Client().Get(signed.URL)
	if err != nil {
		t.Fatal(err)
	}
	content, err := io.ReadAll(response.Body)
	_ = response.Body.Close()
	if err != nil || response.StatusCode != http.StatusOK || !bytes.Equal(content, []byte(complete)) {
		t.Fatalf("gateway download status=%d content=%q error=%v", response.StatusCode, content, err)
	}
	for _, mutation := range []string{"signature", "version", "unsigned"} {
		t.Run(mutation, func(t *testing.T) {
			u, err := url.Parse(signed.URL)
			if err != nil {
				t.Fatal(err)
			}
			query := u.Query()
			switch mutation {
			case "signature":
				query.Set("X-Amz-Signature", strings.Repeat("0", 64))
			case "version":
				query.Set("versionId", aws.ToString(replacement.VersionId))
			case "unsigned":
				query = url.Values{"versionId": {aws.ToString(original.VersionId)}}
			}
			u.RawQuery = query.Encode()
			response, err := gateway.Client().Get(u.String())
			if err != nil {
				t.Fatal(err)
			}
			_ = response.Body.Close()
			if response.StatusCode != http.StatusForbidden {
				t.Fatalf("unauthorized gateway read status=%d", response.StatusCode)
			}
		})
	}
}
