# DRA launch-policy inventory

This read-only helper discovers common NVIDIA driver edits and the edits of one
operator-selected GPU through NVML. It does not read a workload OCI configuration,
start containers, invoke hooks, or write CDI files. It applies the generated CDI
edits to an in-memory OCI spec solely to check the exact serialization.

Build this source inside the vendor tree of NVIDIA `k8s-dra-driver-gpu` tag
`v0.5.0`, using `CGO_ENABLED=1 go build -mod=vendor main.go`. The explicit source
filename includes this file despite its `ignore` build tag. This uses the same
Toolkit 1.20.0 dependency as the deployed DRA image. The runtime inventory records
the helper binary hash, DRA image digest, hook binary hash, node, and selected GPU.
Run it inside that node's verified DRA `gpus` container:

```sh
/tmp/vela-cdi-inspect --gpu-uuid GPU-<operator-selected-uuid>
```

The helper's `/driver-root` driver/dev roots and NVIDIA library path match the
qualified cluster plugin configuration. Requalify these inputs if that
configuration changes. The output retains separate common and device CDI edits;
applying them must not mutate the original inventory or duplicate device hooks.

Combine this output under the `dra` key of the host inventory consumed by
`hack/build-runtime-host-image-policy.py`. Record `node_identity`, `gpu_uuid`,
the actual containerd state directory, Vela runtime handler, and independently
hashed host binaries. Review the inventory and verify all hashes before installing
the policy. Comparing it with a workload spec is a post-generation check, never
the source of approval. The checked-in test fixtures retain hook/environment
fields only and are not installation inventories.
