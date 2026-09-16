//go:build ignore

// Read-only CDI policy inventory using the exact DRA v0.5.0 vendored library.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"github.com/NVIDIA/go-nvml/pkg/nvml"
	"github.com/NVIDIA/nvidia-container-toolkit/pkg/nvcdi"
	"github.com/NVIDIA/nvidia-container-toolkit/pkg/nvcdi/transform/root"
	"github.com/opencontainers/runtime-spec/specs-go"
	"os"
	"tags.cncf.io/container-device-interface/pkg/cdi"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
func run() error {
	gpu := flag.String("gpu-uuid", "", "one independently selected full GPU UUID")
	flag.Parse()
	if *gpu == "" || flag.NArg() != 0 {
		return fmt.Errorf("exact GPU UUID required")
	}
	lib, err := nvcdi.New(nvcdi.WithDriverRoot("/driver-root"), nvcdi.WithDevRoot("/driver-root"), nvcdi.WithNvmlLib(nvml.New(nvml.WithLibraryPath("/driver-root/usr/lib/x86_64-linux-gnu/libnvidia-ml.so.1"))), nvcdi.WithMode("nvml"), nvcdi.WithVendor("k8s.gpu.nvidia.com"), nvcdi.WithClass("claim"), nvcdi.WithNVIDIACDIHookPath("/var/lib/kubelet/plugins/gpu.nvidia.com/nvidia-cdi-hook"), nvcdi.WithFeatureFlags(nvcdi.FeatureDisableNvsandboxUtils))
	if err != nil {
		return err
	}
	generated, err := lib.GetSpec(*gpu)
	if err != nil {
		return err
	}
	raw := generated.Raw()
	if len(raw.Devices) != 1 {
		return fmt.Errorf("expected exactly one GPU")
	}
	if err := root.New(root.WithRoot("/driver-root"), root.WithTargetRoot("/"), root.WithRelativeTo("host")).Transform(raw); err != nil {
		return err
	}
	common := raw.ContainerEdits
	edits := (&cdi.ContainerEdits{ContainerEdits: &common}).Append(&cdi.ContainerEdits{ContainerEdits: &raw.Devices[0].ContainerEdits})
	config := &specs.Spec{Process: &specs.Process{}, Linux: &specs.Linux{}}
	if err := edits.Apply(config); err != nil {
		return err
	}
	return json.NewEncoder(os.Stdout).Encode(map[string]any{"gpu_uuid": *gpu, "hooks": config.Hooks, "environment": config.Process.Env, "cdi_spec": raw})
}
