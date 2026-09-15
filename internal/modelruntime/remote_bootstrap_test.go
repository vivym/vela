package modelruntime_test

import (
	"bytes"
	"crypto/ed25519"
	"encoding/json"
	"testing"
	"time"

	"github.com/vivym/vela/internal/modelruntime"
	"github.com/vivym/vela/internal/stageauthority"
	"google.golang.org/protobuf/proto"
)

func TestRemoteRuntimeBootstrapCanonicalBinding(t *testing.T) {
	config, _, _ := remoteRuntimeServerFixture(t)
	keys, err := stageauthority.DeriveVerifierKeyring(map[string][]byte{"authority-v1": make([]byte, 32)})
	if err != nil {
		t.Fatal(err)
	}
	binding, err := proto.MarshalOptions{Deterministic: true}.Marshal(config.RegistryBinding)
	if err != nil {
		t.Fatal(err)
	}
	bootstrap := modelruntime.RemoteRuntimeBootstrap{SchemaVersion: 1, Manifest: config.Manifest, AuthorityKeys: keys, RegistryKeys: map[string][]byte{"registry": ed25519.NewKeyFromSeed(bytes.Repeat([]byte{17}, 32)).Public().(ed25519.PublicKey)},
		RegistryBinding: binding, Identity: config.RemoteStartup.Journal.Identity, Startup: config.RemoteStartup.Journal.Startup,
		JournalSocket: "/run/vela/journal.sock", StartupSocket: "/run/vela/startup.sock", RuntimeSocket: "/run/vela/runtime.sock", JournalTimeout: time.Second, CancelTimeout: time.Second, ShutdownTimeout: time.Second}
	wire, err := modelruntime.EncodeRemoteRuntimeBootstrap(bootstrap)
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := modelruntime.ParseRemoteRuntimeBootstrap(wire)
	if err != nil {
		t.Fatal(err)
	}
	parsed.Manifest.Runtimes[0].Command[0] = "/unapproved"
	parsed.RegistryKeys["registry"][0] ^= 1
	second, err := modelruntime.ParseRemoteRuntimeBootstrap(wire)
	if err != nil || second.Manifest.Runtimes[0].Command[0] != bootstrap.Manifest.Runtimes[0].Command[0] || !bytes.Equal(second.RegistryKeys["registry"], bootstrap.RegistryKeys["registry"]) {
		t.Fatal("parsed bootstrap aliases another snapshot")
	}
	for _, mode := range []string{"schema", "empty-keyring", "private-key", "negative-timeout", "large-timeout", "relative-socket", "same-socket", "manifest", "scope", "unstarted", "zero-time", "extra-json", "trailing-space", "unknown", "duplicate"} {
		t.Run(mode, func(t *testing.T) {
			changed, err := modelruntime.ParseRemoteRuntimeBootstrap(wire)
			if err != nil {
				t.Fatal(err)
			}
			switch mode {
			case "schema":
				changed.SchemaVersion++
			case "empty-keyring":
				changed.AuthorityKeys = nil
			case "private-key":
				changed.AuthorityKeys["authority-v1"] = make([]byte, 64)
			case "negative-timeout":
				changed.JournalTimeout = -time.Second
			case "large-timeout":
				changed.ShutdownTimeout = time.Hour
			case "relative-socket":
				changed.JournalSocket = "relative.sock"
			case "same-socket":
				changed.StartupSocket = changed.JournalSocket
			case "manifest":
				changed.Manifest.Runtimes[0].Command[0] = "/unapproved"
			case "scope":
				changed.Identity.Scope[0] ^= 1
			case "unstarted":
				changed.Startup.State = modelruntime.BackendLifecycleUnstarted
			case "zero-time":
				changed.Startup.RecordedAt = time.Time{}
			}
			candidate, err := json.Marshal(changed)
			if err != nil {
				t.Fatal(err)
			}
			switch mode {
			case "extra-json":
				candidate = append(candidate, []byte("{}")...)
			case "trailing-space":
				candidate = append(candidate, ' ')
			case "unknown":
				candidate = append([]byte(`{"unknown":true,`), candidate[1:]...)
			case "duplicate":
				candidate = append([]byte(`{"schema_version":1,`), candidate[1:]...)
			}
			if _, err := modelruntime.ParseRemoteRuntimeBootstrap(candidate); err == nil {
				t.Fatal("unbound or noncanonical bootstrap accepted")
			}
		})
	}
}

func TestRemoteRuntimeBootstrapPIDFDBrokerSocketRoundTrip(t *testing.T) {
	config, _, _ := remoteRuntimeServerFixture(t)
	keys, err := stageauthority.DeriveVerifierKeyring(map[string][]byte{"authority-v1": make([]byte, 32)})
	if err != nil {
		t.Fatal(err)
	}
	binding, err := proto.MarshalOptions{Deterministic: true}.Marshal(config.RegistryBinding)
	if err != nil {
		t.Fatal(err)
	}
	bootstrap := modelruntime.RemoteRuntimeBootstrap{SchemaVersion: 1, Manifest: config.Manifest, AuthorityKeys: keys,
		RegistryKeys: map[string][]byte{"registry": ed25519.NewKeyFromSeed(bytes.Repeat([]byte{17}, 32)).Public().(ed25519.PublicKey)}, RegistryBinding: binding,
		Identity: config.RemoteStartup.Journal.Identity, Startup: config.RemoteStartup.Journal.Startup,
		JournalSocket: "/run/vela/journal.sock", StartupSocket: "/run/vela/startup.sock", RuntimeSocket: "/run/vela/runtime.sock",
		PIDFDBrokerSocket: "/run/vela/pidfd-broker.sock", JournalTimeout: time.Second, CancelTimeout: time.Second, ShutdownTimeout: time.Second}
	wire, err := modelruntime.EncodeRemoteRuntimeBootstrap(bootstrap)
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := modelruntime.ParseRemoteRuntimeBootstrap(wire)
	if err != nil || parsed.PIDFDBrokerSocket != bootstrap.PIDFDBrokerSocket {
		t.Fatalf("pidfd broker socket did not survive canonical round trip: %q %v", parsed.PIDFDBrokerSocket, err)
	}
	bootstrap.PIDFDBrokerSocket = "relative.sock"
	if _, err := modelruntime.EncodeRemoteRuntimeBootstrap(bootstrap); err == nil {
		t.Fatal("relative pidfd broker socket accepted")
	}
}
