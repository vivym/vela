package main

import (
	"reflect"
	"testing"

	"github.com/google/go-containerregistry/pkg/v1/mutate"
	"github.com/google/go-containerregistry/pkg/v1/random"
)

func TestSealPreservesFilesystemAndOtherConfiguration(t *testing.T) {
	image, err := random.Image(1024, 2)
	if err != nil {
		t.Fatal(err)
	}
	config, err := image.ConfigFile()
	if err != nil {
		t.Fatal(err)
	}
	config.OS, config.Architecture = "linux", "amd64"
	config.Config.Env = []string{"LD_LIBRARY_PATH=/base", "NVIDIA_VISIBLE_DEVICES=all", "HOME=/old"}
	config.Config.Entrypoint = []string{"/usr/local/bin/vela-runtime-entrypoint", "runtime"}
	config.Config.User = "10001:10001"
	config.Config.Labels = map[string]string{"approved-source": "pinned"}
	image, err = mutate.ConfigFile(image, config)
	if err != nil {
		t.Fatal(err)
	}
	sealed, err := seal(image)
	if err != nil {
		t.Fatal(err)
	}
	actual, err := sealed.ConfigFile()
	if err != nil {
		t.Fatal(err)
	}
	expected := config.DeepCopy()
	expected.Config.Env = []string{"PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin", "HOME=/"}
	if !reflect.DeepEqual(actual, expected) {
		t.Fatal("sealing changed non-environment metadata")
	}
	before, _ := image.Manifest()
	after, _ := sealed.Manifest()
	if !reflect.DeepEqual(before.Layers, after.Layers) || before.Config.Digest == after.Config.Digest {
		t.Fatal("unexpected config or layer identity")
	}
	again, err := seal(sealed)
	if err != nil {
		t.Fatal(err)
	}
	digest, _ := sealed.Digest()
	repeated, _ := again.Digest()
	if digest != repeated {
		t.Fatal("sealing is not idempotent")
	}
}
