// seal-runtime-image publishes exact PATH/HOME metadata while preserving all
// layer descriptors. Credentials come from DOCKER_CONFIG; TLS remains verified.
package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"os"
	"reflect"
	"time"

	"github.com/google/go-containerregistry/pkg/authn"
	"github.com/google/go-containerregistry/pkg/name"
	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/mutate"
	"github.com/google/go-containerregistry/pkg/v1/remote"
	registrytransport "github.com/google/go-containerregistry/pkg/v1/remote/transport"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	source := flag.String("source", "", "pinned single-platform source image")
	destination := flag.String("destination", "", "new tag in the same repository")
	ca := flag.String("ca-file", "", "trusted registry CA PEM")
	flag.Parse()
	input, err := name.NewDigest(*source, name.StrictValidation)
	if err != nil {
		return err
	}
	output, err := name.NewTag(*destination, name.StrictValidation)
	if err != nil || flag.NArg() != 0 || input.Context() != output.Context() {
		return errors.New("destination must be a tag in the source repository")
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	if *ca != "" {
		pem, err := os.ReadFile(*ca)
		if err != nil {
			return err
		}
		roots, err := x509.SystemCertPool()
		if err != nil {
			return err
		}
		if !roots.AppendCertsFromPEM(pem) {
			return errors.New("invalid registry CA")
		}
		transport.TLSClientConfig = &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: roots}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	options := []remote.Option{remote.WithContext(ctx), remote.WithTransport(transport), remote.WithAuthFromKeychain(authn.DefaultKeychain)}
	descriptor, err := remote.Get(input, options...)
	if err != nil {
		return err
	}
	if !descriptor.MediaType.IsImage() {
		return errors.New("source must be a single-platform image manifest")
	}
	image, err := descriptor.Image()
	if err != nil {
		return err
	}
	sealed, err := seal(image)
	if err != nil {
		return err
	}
	digest, err := sealed.Digest()
	if err != nil {
		return err
	}
	// Refuse to replace another publication under an existing tag.
	previous, err := remote.Head(output, options...)
	if err == nil && previous.Digest != digest {
		return errors.New("destination tag already names another image")
	}
	if err != nil {
		var failure *registrytransport.Error
		if !errors.As(err, &failure) || failure.StatusCode != http.StatusNotFound {
			return err
		}
	}
	if err := remote.Write(output, sealed, options...); err != nil {
		return err
	}
	verified, err := remote.Get(output.Digest(digest.String()), options...)
	if err != nil {
		return err
	}
	if verified.Digest != digest {
		return errors.New("published image digest mismatch")
	}
	manifest, err := sealed.Manifest()
	if err != nil {
		return err
	}
	return json.NewEncoder(os.Stdout).Encode(map[string]any{"source": input.Name(), "image": output.Digest(digest.String()).Name(), "config_digest": manifest.Config.Digest.String(), "layer_count": len(manifest.Layers), "layers_unchanged": true, "environment": []string{"PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin", "HOME=/"}})
}

func seal(image v1.Image) (v1.Image, error) {
	config, err := image.ConfigFile()
	if err != nil {
		return nil, err
	}
	if config.OS != "linux" || config.Architecture != "amd64" {
		return nil, errors.New("expected linux/amd64")
	}
	config = config.DeepCopy()
	config.Config.Env = []string{"PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin", "HOME=/"}
	sealed, err := mutate.ConfigFile(image, config)
	if err != nil {
		return nil, err
	}
	before, err := image.Manifest()
	if err != nil {
		return nil, err
	}
	after, err := sealed.Manifest()
	if err != nil {
		return nil, err
	}
	if !reflect.DeepEqual(before.Layers, after.Layers) {
		return nil, errors.New("sealing changed image layers")
	}
	return sealed, nil
}
