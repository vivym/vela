package nodeagent

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
)

func TestRuntimeImageDefaultEntrypoint(t *testing.T) {
	for _, configuration := range []ocispec.ImageConfig{
		{Entrypoint: []string{"/runtime"}, Cmd: []string{"serve", ""}},
		{Cmd: []string{"/runtime", "serve"}},
	} {
		expected := append(append([]string{}, configuration.Entrypoint...), configuration.Cmd...)
		arguments, err := runtimeImageDefaultArguments(configuration)
		if err != nil || !reflect.DeepEqual(arguments, expected) {
			t.Fatalf("image defaults changed: %v", err)
		}
		arguments[0] = "/mutated"
		fresh, err := runtimeImageDefaultArguments(configuration)
		if err != nil || !reflect.DeepEqual(fresh, expected) {
			t.Fatal("returned argv changed image defaults")
		}
	}
	for _, configuration := range []ocispec.ImageConfig{
		{}, {Entrypoint: []string{""}, Cmd: []string{"/fallback"}}, {Entrypoint: []string{"relative"}}, {Cmd: []string{"/"}},
		{Entrypoint: []string{"/bin/../runtime"}}, {Entrypoint: []string{"/runtime"}, Cmd: []string{"bad\x00arg"}},
		{Entrypoint: []string{"/runtime"}, Cmd: make([]string, 256)}, {Entrypoint: []string{"/runtime"}, Cmd: []string{strings.Repeat("x", (64<<10)+1)}},
	} {
		if arguments, err := runtimeImageDefaultArguments(configuration); err == nil || arguments != nil {
			t.Fatal("unsupported image entrypoint accepted")
		}
	}
	encoded, err := json.Marshal(ocispec.Image{Config: ocispec.ImageConfig{Entrypoint: []string{"/runtime"}, Env: []string{"KEY=original"}}})
	if err != nil {
		t.Fatal(err)
	}
	launch := &RuntimeImageLaunch{encoded: encoded}
	configuration, err := launch.Configuration()
	if err != nil {
		t.Fatal(err)
	}
	configuration.Config.Entrypoint[0], configuration.Config.Env[0] = "/changed", "KEY=changed"
	configuration, err = launch.Configuration()
	if err != nil || configuration.Config.Entrypoint[0] != "/runtime" || configuration.Config.Env[0] != "KEY=original" {
		t.Fatal("image launch exposed mutable configuration")
	}
	for _, empty := range []*RuntimeImageLaunch{nil, {}} {
		if _, err := empty.Configuration(); err == nil {
			t.Fatal("unobserved image yielded configuration")
		}
	}
}
