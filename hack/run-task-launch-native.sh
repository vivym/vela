#!/usr/bin/env bash
# Rebuild the pinned arm64 CPU containerd/runc fixture and test task launch sources.
set -euo pipefail

launch_repo="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
launch_evidence="${VELA_TASK_LAUNCH_EVIDENCE:-$(mktemp -d /tmp/vela-task-launch.XXXXXX)}"
launch_cache="${VELA_TASK_LAUNCH_BUILD_CACHE:-$launch_evidence/go-cache}"
launch_downloads="${VELA_TASK_LAUNCH_DOWNLOADS:-$launch_evidence/downloads}"
launch_modules="$(go env GOMODCACHE)"
launch_builder='golang@sha256:e30143be198ab04cf7ba25fba83ab3a692ca584c994aad0bf131fa0eb32dd8c1'
[[ "$launch_evidence" = /* && "$launch_cache" = /* && "$launch_downloads" = /* ]] || exit 2
[[ "$(docker version --format '{{.Server.Arch}}')" == arm64 ]] || { echo 'This pinned fixture supports Linux arm64 only' >&2; exit 2; }
mkdir -p "$launch_evidence/image/rootfs/usr/local/bin" "$launch_cache" "$launch_downloads"

launch_fetch() {
  local launch_url="$1" launch_file="$2" launch_digest="$3"
  if [[ ! -f "$launch_file" ]]; then
    curl --http1.1 -fsSL --retry 2 --max-time 60 -H 'Accept: application/octet-stream' "$launch_url" -o "$launch_file"
  fi
  [[ "$(shasum -a 256 "$launch_file" | cut -d ' ' -f 1)" == "$launch_digest" ]] || { echo "Download checksum mismatch: $launch_file" >&2; exit 1; }
}
launch_fetch 'https://github.com/containerd/containerd/releases/download/v2.3.1/containerd-2.3.1-linux-arm64.tar.gz' \
  "$launch_downloads/containerd.tar.gz" '46a83603a850f3916ca7c942310daaf82ed17773a85b1d431e92d4a541e46d0d'
launch_fetch 'https://api.github.com/repos/opencontainers/runc/releases/assets/387432606?download=1' \
  "$launch_downloads/runc.api.arm64" 'ea54032310588e115633aa2f4bba8bf9500257f657e1deca88df5778775138db'
for launch_binary in containerd containerd-shim-runc-v2 ctr; do
  tar -xOf "$launch_downloads/containerd.tar.gz" "bin/$launch_binary" > "$launch_evidence/image/rootfs/usr/local/bin/$launch_binary"
done
cp "$launch_downloads/runc.api.arm64" "$launch_evidence/image/rootfs/usr/local/bin/runc"
chmod 755 "$launch_evidence/image/rootfs/usr/local/bin/"*
shasum -a 256 "$launch_downloads/containerd.tar.gz" "$launch_downloads/runc.api.arm64" > "$launch_evidence/downloads.sha256"
git -C "$launch_repo" rev-parse HEAD > "$launch_evidence/source.txt"
git -C "$launch_repo" status --short >> "$launch_evidence/source.txt"
git -C "$launch_repo" diff HEAD --binary -- internal/nodeagent hack/run-task-launch-native.sh > "$launch_evidence/source.patch"
while IFS= read -r -d '' launch_untracked; do
  launch_diff_status=0
  git -C "$launch_repo" diff --no-index --binary -- /dev/null "$launch_untracked" >> "$launch_evidence/source.patch" || launch_diff_status=$?
  [[ "$launch_diff_status" == 0 || "$launch_diff_status" == 1 ]] || exit "$launch_diff_status"
done < <(git -C "$launch_repo" ls-files -z --others --exclude-standard -- internal/nodeagent hack/run-task-launch-native.sh)
shasum -a 256 "$launch_evidence/source.patch" > "$launch_evidence/source-patch.sha256"
docker version > "$launch_evidence/docker-version.txt"

docker run --rm --network none --cpus 4 --memory 4g --pids-limit 512 \
  -v "$launch_repo:/workspace:ro" -v "$launch_modules:/go/pkg/mod:ro" \
  -v "$launch_evidence:/evidence" -v "$launch_cache:/build-cache" -w /workspace \
  -e GOCACHE=/build-cache -e GOPROXY=off -e CGO_ENABLED=1 \
  "$launch_builder" go test -race -tags=integration -c -ldflags '-linkmode external -extldflags=-static' \
  -o /evidence/image/rootfs/nodeagent.test ./internal/nodeagent > "$launch_evidence/build.log" 2>&1
cat > "$launch_evidence/image/Dockerfile" <<'DOCKERFILE'
FROM golang@sha256:e30143be198ab04cf7ba25fba83ab3a692ca584c994aad0bf131fa0eb32dd8c1
COPY rootfs /
ENTRYPOINT ["/nodeagent.test"]
DOCKERFILE
launch_image="$(docker build --network none -q "$launch_evidence/image")"
echo "$launch_image" > "$launch_evidence/image.txt"
shasum -a 256 "$launch_evidence/image/rootfs/nodeagent.test" "$launch_evidence/image/rootfs/usr/local/bin/"* > "$launch_evidence/binaries.sha256"
# Privileges are confined to this disposable nested-runtime sandbox. No host
# paths/sockets, host PID namespace, network, model weights or GPU are used.
docker run --rm --network none --privileged --cgroupns private --cpus 4 --memory 4g --pids-limit 512 \
  -e VELA_TEST_CONTAINERD_SANDBOX=1 "$launch_image" \
  -test.run='^(TestRuntimeContainerdProcessEvidence|TestRuntimeCallerContainerCRI|TestRuntimeDaemonStateDirectory|TestRuntimeContainerDaemonClosesHandles)$' -test.count=1 -test.v -test.timeout=3m \
  > "$launch_evidence/native.log" 2>&1
for launch_test in TestRuntimeContainerdProcessEvidence TestRuntimeCallerContainerCRI TestRuntimeDaemonStateDirectory TestRuntimeContainerDaemonClosesHandles; do
  rg -q "^--- PASS: $launch_test " "$launch_evidence/native.log" || exit 1
done
if rg -q -- '--- SKIP:|WARNING: DATA RACE' "$launch_evidence/native.log"; then exit 1; fi
echo "Native task launch checks passed: $launch_evidence"
