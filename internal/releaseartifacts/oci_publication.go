package releaseartifacts

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"path/filepath"
	"strings"

	"github.com/google/go-containerregistry/pkg/authn"
	"github.com/google/go-containerregistry/pkg/name"
	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/layout"
	"github.com/google/go-containerregistry/pkg/v1/remote"
	"github.com/google/go-containerregistry/pkg/v1/types"
)

const (
	velaRegistryPublicationSchemaVersion = 1
	velaRegistryPublicationFile          = "vela-registry-publication.json"
)

type velaRegistryPublicationReceipt struct {
	SchemaVersion int                            `json:"schema_version"`
	Revision      string                         `json:"revision"`
	Images        []velaRegistryPublicationImage `json:"images"`
}

type velaRegistryPublicationImage struct {
	Image             string `json:"image"`
	ManifestDigest    string `json:"manifest_digest"`
	ManifestMediaType string `json:"manifest_media_type"`
	ManifestSizeBytes int64  `json:"manifest_size_bytes"`
}

type velaImageRegistryClient interface {
	publishAndVerify(
		ctx context.Context,
		imageReference string,
		layoutDirectory string,
		expectedManifest []byte,
	) (velaRegistryPublicationImage, error)
}

type remoteVelaImageRegistry struct {
	keychain      authn.Keychain
	authenticator authn.Authenticator
	transport     http.RoundTripper
	nameOptions   []name.Option
}

func newRemoteVelaImageRegistry() *remoteVelaImageRegistry {
	return &remoteVelaImageRegistry{
		keychain:    authn.DefaultKeychain,
		transport:   remote.DefaultTransport,
		nameOptions: []name.Option{name.StrictValidation},
	}
}

func publishVelaImageLayouts(
	ctx context.Context,
	layoutRoot string,
	candidate string,
	manifest velaImageArtifactManifest,
	client velaImageRegistryClient,
) (velaRegistryPublicationReceipt, error) {
	if ctx == nil {
		return velaRegistryPublicationReceipt{}, errors.New("registry publication context is required")
	}
	if client == nil {
		return velaRegistryPublicationReceipt{}, errors.New("registry publication client is required")
	}
	if manifest.SchemaVersion != velaImageArtifactSchemaVersion ||
		len(manifest.OCIManifests) != velaImageCount {
		return velaRegistryPublicationReceipt{}, errors.New("vela image artifact manifest is invalid")
	}
	receipt := velaRegistryPublicationReceipt{
		SchemaVersion: velaRegistryPublicationSchemaVersion,
		Revision:      manifest.Revision,
		Images:        make([]velaRegistryPublicationImage, 0, velaImageCount),
	}
	for index, specification := range velaImageSpecifications() {
		input := manifest.OCIManifests[index]
		if input.Ref != specification.name+".manifest.json" ||
			input.ConfigRef != specification.name+".config.json" {
			return velaRegistryPublicationReceipt{}, errors.New("vela image artifact references are not exact")
		}
		manifestEncoded, err := readRegularMetadata(filepath.Join(candidate, input.Ref))
		if err != nil {
			return velaRegistryPublicationReceipt{}, fmt.Errorf("read %s manifest for registry publication: %w", specification.name, err)
		}
		digest := sha256.Sum256(manifestEncoded)
		expectedSuffix := "/" + specification.name + "@sha256:" + hex.EncodeToString(digest[:])
		if !strings.HasSuffix(input.Image, expectedSuffix) {
			return velaRegistryPublicationReceipt{}, fmt.Errorf("%s image reference does not bind the local manifest", specification.name)
		}
		published, err := client.publishAndVerify(
			ctx,
			input.Image,
			filepath.Join(layoutRoot, specification.name),
			manifestEncoded,
		)
		if err != nil {
			return velaRegistryPublicationReceipt{}, fmt.Errorf("publish %s OCI image: %w", specification.name, err)
		}
		if published.Image != input.Image ||
			published.ManifestDigest != "sha256:"+hex.EncodeToString(digest[:]) ||
			published.ManifestMediaType != string(types.OCIManifestSchema1) ||
			published.ManifestSizeBytes != int64(len(manifestEncoded)) {
			return velaRegistryPublicationReceipt{}, fmt.Errorf("%s registry receipt does not bind the local manifest", specification.name)
		}
		receipt.Images = append(receipt.Images, published)
	}
	return receipt, nil
}

func (registry *remoteVelaImageRegistry) publishAndVerify(
	ctx context.Context,
	imageReference string,
	layoutDirectory string,
	expectedManifest []byte,
) (velaRegistryPublicationImage, error) {
	if registry == nil || registry.transport == nil ||
		(registry.keychain == nil && registry.authenticator == nil) {
		return velaRegistryPublicationImage{}, errors.New("registry client is not configured")
	}
	reference, err := name.NewDigest(imageReference, registry.nameOptions...)
	if err != nil || reference.String() != imageReference {
		return velaRegistryPublicationImage{}, errors.New("registry image reference must be a strict digest reference")
	}
	digest, err := v1.NewHash(reference.DigestStr())
	if err != nil {
		return velaRegistryPublicationImage{}, fmt.Errorf("parse registry manifest digest: %w", err)
	}
	image, err := layout.Path(layoutDirectory).Image(digest)
	if err != nil {
		return velaRegistryPublicationImage{}, fmt.Errorf("load validated OCI image layout: %w", err)
	}
	rawManifest, err := image.RawManifest()
	if err != nil {
		return velaRegistryPublicationImage{}, fmt.Errorf("read validated OCI image manifest: %w", err)
	}
	if !bytes.Equal(rawManifest, expectedManifest) {
		return velaRegistryPublicationImage{}, errors.New("validated OCI layout does not match local manifest bytes")
	}
	imageDigest, err := image.Digest()
	if err != nil || imageDigest != digest {
		return velaRegistryPublicationImage{}, errors.New("validated OCI layout does not match the requested manifest digest")
	}

	options := registry.remoteOptions(ctx)
	if err := remote.Write(reference, image, options...); err != nil {
		return velaRegistryPublicationImage{}, fmt.Errorf("upload digest-bound OCI image: %w", err)
	}
	remoteDescriptor, err := remote.Get(reference, options...)
	if err != nil {
		return velaRegistryPublicationImage{}, fmt.Errorf("read published OCI manifest: %w", err)
	}
	if remoteDescriptor.Digest != digest ||
		remoteDescriptor.Size != int64(len(expectedManifest)) ||
		remoteDescriptor.MediaType != types.OCIManifestSchema1 ||
		!bytes.Equal(remoteDescriptor.Manifest, expectedManifest) {
		return velaRegistryPublicationImage{}, errors.New("published OCI manifest does not match local manifest bytes")
	}
	return velaRegistryPublicationImage{
		Image:             imageReference,
		ManifestDigest:    digest.String(),
		ManifestMediaType: string(remoteDescriptor.MediaType),
		ManifestSizeBytes: remoteDescriptor.Size,
	}, nil
}

func (registry *remoteVelaImageRegistry) remoteOptions(ctx context.Context) []remote.Option {
	options := []remote.Option{
		remote.WithContext(ctx),
		remote.WithTransport(registry.transport),
		remote.WithJobs(1),
		remote.WithUserAgent("vela-release-artifacts"),
	}
	if registry.keychain != nil {
		return append(options, remote.WithAuthFromKeychain(registry.keychain))
	}
	return append(options, remote.WithAuth(registry.authenticator))
}
