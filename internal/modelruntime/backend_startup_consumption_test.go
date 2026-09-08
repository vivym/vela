package modelruntime_test

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/modelruntime"
)

func TestBackendStartupBootstrapConsumptionEnvelope(t *testing.T) {
	request := modelruntime.BackendStartupRequest{SchemaVersion: 1, NodeIdentity: "cpu-node", JournalID: uuid.New(), IncarnationID: uuid.New(),
		RegistryBindingDigest: sha256.Sum256([]byte("binding")), JournalScope: sha256.Sum256([]byte("scope")), LaunchDigest: sha256.Sum256([]byte("manifest"))}
	legacy, err := modelruntime.EncodeBackendStartupRequest(request)
	if err != nil || bytes.Contains(legacy, []byte("bootstrap_")) {
		t.Fatalf("legacy encoding changed: %s %v", legacy, err)
	}
	request.SchemaVersion, request.BootstrapDigest, request.BootstrapPath = 2, sha256.Sum256([]byte("parsed canonical bootstrap")), "/etc/vela/runtime/bootstrap.json"
	wire, err := modelruntime.EncodeBackendStartupRequest(request)
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := modelruntime.ParseBackendStartupRequest(wire)
	if err != nil || parsed != request {
		t.Fatalf("consumed snapshot declaration changed: %v", err)
	}
	for _, mode := range []string{"legacy-with-digest", "legacy-with-path", "missing-digest", "missing-path", "relative-path", "unclean-path", "root-path", "long-path", "nul-path", "invalid-utf8", "escaped-size", "unsupported-version", "duplicate", "unknown", "trailing", "noncanonical-zero"} {
		t.Run(mode, func(t *testing.T) {
			changed := request
			switch mode {
			case "legacy-with-digest":
				changed.SchemaVersion, changed.BootstrapPath = 1, ""
			case "legacy-with-path":
				changed.SchemaVersion, changed.BootstrapDigest = 1, [sha256.Size]byte{}
			case "missing-digest":
				changed.BootstrapDigest = [sha256.Size]byte{}
			case "missing-path":
				changed.BootstrapPath = ""
			case "relative-path":
				changed.BootstrapPath = "relative.json"
			case "unclean-path":
				changed.BootstrapPath = "/etc/../bootstrap.json"
			case "root-path":
				changed.BootstrapPath = "/"
			case "long-path":
				changed.BootstrapPath = "/" + strings.Repeat("a", 2048)
			case "nul-path":
				changed.BootstrapPath = "/etc/boot\x00strap.json"
			case "invalid-utf8":
				changed.BootstrapPath = "/etc/boot\xffstrap.json"
			case "escaped-size":
				changed.BootstrapPath = "/" + strings.Repeat("\x01", 2047)
			case "unsupported-version":
				changed.SchemaVersion = 3
			}
			if mode == "invalid-utf8" || mode == "escaped-size" {
				if _, err := modelruntime.EncodeBackendStartupRequest(changed); err == nil {
					t.Fatal("invalid or oversized encoded path accepted")
				}
				return
			}
			document, err := json.Marshal(changed)
			if err != nil {
				t.Fatal(err)
			}
			switch mode {
			case "duplicate":
				document = append([]byte(`{"schema_version":2,`), document[1:]...)
			case "unknown":
				document = append([]byte(`{"unknown":true,`), document[1:]...)
			case "trailing":
				document = append(document, ' ')
			case "noncanonical-zero":
				document = bytes.Replace(legacy, []byte(`"schema_version":1`), []byte(`"schema_version":1,"bootstrap_path":""`), 1)
			}
			if got, err := modelruntime.ParseBackendStartupRequest(document); err == nil || got != (modelruntime.BackendStartupRequest{}) {
				t.Fatalf("invalid declaration accepted: %s %v", document, err)
			}
		})
	}
}
