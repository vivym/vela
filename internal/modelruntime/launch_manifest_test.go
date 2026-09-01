package modelruntime_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/vivym/vela/internal/modelruntime"
)

func TestLoadLaunchManifestReturnsCompleteResidentRuntimeBindings(t *testing.T) {
	root := t.TempDir()
	manifestPath := filepath.Join(root, "launch.json")
	manifest := launchManifestFixture(root)
	writeLaunchManifest(t, manifestPath, manifest)

	loaded, err := modelruntime.LoadLaunchManifest(manifestPath)
	if err != nil {
		t.Fatalf("LoadLaunchManifest: %v", err)
	}
	if loaded.WorkerInstanceID != "21000000-0000-0000-0000-000000000001" ||
		loaded.WorkerInstanceEpoch != 7 || loaded.WorkerMemberEpoch != 11 ||
		len(loaded.Runtimes) != 2 || len(loaded.LocalDevices) != 1 {
		t.Fatalf("loaded manifest = %#v", loaded)
	}
	bindings, err := loaded.RuntimeBindings()
	if err != nil {
		t.Fatalf("RuntimeBindings: %v", err)
	}
	if len(bindings) != 2 ||
		bindings[0].ModelResidencyID != "51000000-0000-0000-0000-000000000001" ||
		bindings[1].ModelRuntimeIdentity != "minimax-h3-vae-runtime-v1" ||
		bindings[0].ModelRuntimeEpoch != 0 || len(bindings[0].Devices) != 1 ||
		len(bindings[0].Members) != 1 || bindings[0].WorkerMemberEpoch != 11 {
		t.Fatalf("runtime bindings = %#v", bindings)
	}
}

func TestLoadLaunchManifestRejectsAmbiguousOrIncompleteAuthority(t *testing.T) {
	root := t.TempDir()
	manifest := launchManifestFixture(root)
	tests := []struct {
		name   string
		mutate func(map[string]any)
	}{
		{
			name: "unknown field",
			mutate: func(document map[string]any) {
				document["artifact_store_credentials"] = "forbidden"
			},
		},
		{
			name: "uncertified multi-model shape",
			mutate: func(document map[string]any) {
				document["shared_slot_exception"] = ""
			},
		},
		{
			name: "multi-gpu DiT",
			mutate: func(document map[string]any) {
				document["worker_role"] = "dit"
				document["shared_slot_exception"] = ""
				document["runtimes"] = document["runtimes"].([]any)[:1]
				document["runtimes"].([]any)[0].(map[string]any)["component"] = "DIT"
				local := document["local_devices"].([]any)[0].(map[string]any)
				second := map[string]any{}
				for key, value := range local {
					second[key] = value
				}
				second["device_id"] = "31000000-0000-0000-0000-000000000002"
				second["gpu_uuid"] = "GPU-00000000-0000-0000-0000-000000000002"
				second["pci_bdf"] = "0000:42:00.0"
				document["local_devices"] = append(document["local_devices"].([]any), second)
				document["devices"] = append(document["devices"].([]any), map[string]any{
					"id": second["device_id"], "epoch": second["device_epoch"],
				})
			},
		},
		{
			name: "duplicate runtime route",
			mutate: func(document map[string]any) {
				runtimes := document["runtimes"].([]any)
				runtimes[1] = runtimes[0]
			},
		},
		{
			name: "local gpu not in member device set",
			mutate: func(document map[string]any) {
				local := document["local_devices"].([]any)[0].(map[string]any)
				local["device_id"] = "31000000-0000-0000-0000-000000000099"
			},
		},
		{
			name: "runtime epoch supplied by fleet",
			mutate: func(document map[string]any) {
				document["model_runtime_epoch"] = float64(99)
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			encoded, err := json.Marshal(manifest)
			if err != nil {
				t.Fatalf("encode fixture: %v", err)
			}
			var document map[string]any
			if err := json.Unmarshal(encoded, &document); err != nil {
				t.Fatalf("decode fixture: %v", err)
			}
			test.mutate(document)
			path := filepath.Join(root, strings.ReplaceAll(test.name, " ", "-")+".json")
			writeLaunchManifest(t, path, document)
			if _, err := modelruntime.LoadLaunchManifest(path); err == nil {
				t.Fatal("LoadLaunchManifest accepted invalid authority")
			}
		})
	}
}

func TestLoadLaunchManifestAcceptsCertifiedMultiMemberLLMTopology(t *testing.T) {
	root := t.TempDir()
	manifest := launchManifestFixture(root)
	manifest["worker_profile_revision_id"] = "71000000-0000-0000-0000-000000000002"
	manifest["worker_role"] = "llm"
	manifest["shared_slot_exception"] = ""
	runtime := manifest["runtimes"].([]any)[0].(map[string]any)
	runtime["component"] = "LLM"
	runtime["model_component_revision"] = "future-llm-r1"
	manifest["runtimes"] = []any{runtime}
	manifest["devices"] = append(manifest["devices"].([]any), map[string]any{
		"id": "31000000-0000-0000-0000-000000000002", "epoch": float64(17),
	})
	manifest["members"] = append(manifest["members"].([]any), map[string]any{
		"id": "41000000-0000-0000-0000-000000000002", "epoch": float64(19),
	})
	path := filepath.Join(root, "multi-member-llm.json")
	writeLaunchManifest(t, path, manifest)
	loaded, err := modelruntime.LoadLaunchManifest(path)
	if err != nil {
		t.Fatalf("LoadLaunchManifest: %v", err)
	}
	bindings, err := loaded.RuntimeBindings()
	if err != nil || len(bindings) != 1 || len(bindings[0].Devices) != 2 || len(bindings[0].Members) != 2 {
		t.Fatalf("multi-member LLM bindings = %#v error=%v", bindings, err)
	}
}

func launchManifestFixture(root string) map[string]any {
	return map[string]any{
		"schema_version":             1,
		"worker_profile_revision_id": "71000000-0000-0000-0000-000000000001",
		"worker_role":                "aux",
		"capacity_slots":             float64(1),
		"shared_slot_exception":      "H3_AUX_ENCODER_VAE",
		"worker_instance_id":         "21000000-0000-0000-0000-000000000001",
		"worker_instance_epoch":      float64(7),
		"worker_member_id":           "41000000-0000-0000-0000-000000000001",
		"worker_member_epoch":        float64(11),
		"device_set_digest":          strings.Repeat("a", 64),
		"membership_digest":          strings.Repeat("b", 64),
		"devices": []any{map[string]any{
			"id": "31000000-0000-0000-0000-000000000001", "epoch": float64(13),
		}},
		"members": []any{map[string]any{
			"id": "41000000-0000-0000-0000-000000000001", "epoch": float64(11),
		}},
		"local_devices": []any{map[string]any{
			"device_id": "31000000-0000-0000-0000-000000000001", "device_epoch": float64(13),
			"gpu_uuid": "GPU-00000000-0000-0000-0000-000000000001", "pci_bdf": "0000:41:00.0",
		}},
		"runtimes": []any{
			map[string]any{
				"model_residency_id":        "51000000-0000-0000-0000-000000000001",
				"runtime_identity":          "minimax-h3-encoder-runtime-v1",
				"stage_profile_revision_id": "61000000-0000-0000-0000-000000000001",
				"component":                 "ENCODER", "model_component_revision": "minimax-h3-encoder-r1",
				"runtime_image_digest": strings.Repeat("c", 64),
				"command":              []any{"/usr/local/bin/minimax-h3-driver", "--component", "encoder"},
				"environment":          []any{"CUDA_VISIBLE_DEVICES=GPU-00000000-0000-0000-0000-000000000001"},
				"scratch_root":         root, "output_root": filepath.Join(root, "encoder-outputs"),
				"initialization_timeout": "2h", "shutdown_timeout": "2m",
			},
			map[string]any{
				"model_residency_id":        "51000000-0000-0000-0000-000000000002",
				"runtime_identity":          "minimax-h3-vae-runtime-v1",
				"stage_profile_revision_id": "61000000-0000-0000-0000-000000000002",
				"component":                 "VAE_DECODER", "model_component_revision": "minimax-h3-vae-r1",
				"runtime_image_digest": strings.Repeat("c", 64),
				"command":              []any{"/usr/local/bin/minimax-h3-driver", "--component", "vae"},
				"environment":          []any{"CUDA_VISIBLE_DEVICES=GPU-00000000-0000-0000-0000-000000000001"},
				"scratch_root":         root, "output_root": filepath.Join(root, "vae-outputs"),
				"initialization_timeout": "2h", "shutdown_timeout": "2m",
			},
		},
	}
}

func writeLaunchManifest(t *testing.T, path string, value any) {
	t.Helper()
	encoded, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("encode launch manifest: %v", err)
	}
	if err := os.WriteFile(path, encoded, 0o600); err != nil {
		t.Fatalf("write launch manifest: %v", err)
	}
}
