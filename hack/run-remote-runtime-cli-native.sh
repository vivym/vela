#!/usr/bin/env bash
# Actual non-root Runtime CLI, root journal RPC and CPU process backend.
set -euo pipefail
remote_repo="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
remote_evidence="${VELA_REMOTE_CLI_EVIDENCE:-$(mktemp -d /tmp/vela-remote-cli.XXXXXX)}"
remote_cache="${VELA_REMOTE_CLI_BUILD_CACHE:-$remote_evidence/go-cache}"
remote_modules="$(go env GOMODCACHE)"
remote_builder='golang@sha256:e30143be198ab04cf7ba25fba83ab3a692ca584c994aad0bf131fa0eb32dd8c1'
[[ "$remote_evidence" = /* && "$remote_cache" = /* ]] || exit 2
mkdir -p "$remote_evidence/image/rootfs/run" "$remote_evidence/image/rootfs/tmp" "$remote_cache"
chmod 1777 "$remote_evidence/image/rootfs/tmp"
git -C "$remote_repo" rev-parse HEAD > "$remote_evidence/source.txt"
git -C "$remote_repo" status --short >> "$remote_evidence/source.txt"
git -C "$remote_repo" diff HEAD --binary -- cmd/vela-model-runtime internal/modelruntime internal/nodeagent hack/run-remote-runtime-cli-native.sh > "$remote_evidence/source.patch"
while IFS= read -r -d '' remote_file; do
  remote_diff_status=0
  git -C "$remote_repo" diff --no-index --binary /dev/null "$remote_file" >> "$remote_evidence/source.patch" || remote_diff_status=$?
  [[ "$remote_diff_status" = 0 || "$remote_diff_status" = 1 ]] || exit "$remote_diff_status"
done < <(git -C "$remote_repo" ls-files -z --others --exclude-standard -- cmd/vela-model-runtime internal/modelruntime internal/nodeagent hack/run-remote-runtime-cli-native.sh)
shasum -a 256 "$remote_evidence/source.patch" > "$remote_evidence/source-patch.sha256"
docker version > "$remote_evidence/docker-version.txt"
for remote_target in nodeagent runtime-command vela-model-runtime; do
  remote_args=(test -race -c)
  remote_package='./internal/nodeagent'
  remote_output="$remote_target.test"
  if [[ "$remote_target" != nodeagent ]]; then remote_package='./cmd/vela-model-runtime'; fi
  if [[ "$remote_target" = vela-model-runtime ]]; then remote_args=(build -race); remote_output=vela-model-runtime; fi
  docker run --rm --network none --cpus 4 --memory 4g --pids-limit 512 \
    -v "$remote_repo:/workspace:ro" -v "$remote_modules:/go/pkg/mod:ro" \
    -v "$remote_evidence:/evidence" -v "$remote_cache:/build-cache" -w /workspace \
    -e GOCACHE=/build-cache -e GOPROXY=off -e CGO_ENABLED=1 "$remote_builder" \
    go "${remote_args[@]}" -ldflags '-linkmode external -extldflags=-static' \
    -o "/evidence/image/rootfs/$remote_output" "$remote_package" > "$remote_evidence/build-$remote_target.log" 2>&1
done
cat > "$remote_evidence/image/Dockerfile" <<'DOCKERFILE'
FROM scratch
COPY rootfs /
DOCKERFILE
remote_image="$(docker build --network none -q "$remote_evidence/image")"
echo "$remote_image" > "$remote_evidence/image.txt"
shasum -a 256 "$remote_evidence/image/rootfs/"*.test "$remote_evidence/image/rootfs/vela-model-runtime" > "$remote_evidence/binaries.sha256"
# No host mounts, network or GPU. Root Node creates private PID namespaces;
# Runtime, Worker and the actual backend execute as non-root.
docker run --rm --network none --cap-add SYS_ADMIN --cap-add SYS_PTRACE --security-opt seccomp=unconfined \
  --cpus 4 --memory 4g --pids-limit 256 "$remote_image" /nodeagent.test \
  -test.run '^(TestJournalServerActualRemoteCLI|TestJournalServerStartsRemoteRuntimeBeforeActualWorkerExecution)$' \
  -test.count=1 -test.v -test.timeout=2m > "$remote_evidence/native.log" 2>&1
for remote_test in TestJournalServerActualRemoteCLI TestJournalServerStartsRemoteRuntimeBeforeActualWorkerExecution; do
  rg -q "^--- PASS: $remote_test " "$remote_evidence/native.log" || exit 1
done
if rg -q -- '--- SKIP:|WARNING: DATA RACE' "$remote_evidence/native.log"; then exit 1; fi
echo "Actual remote Runtime CLI checks passed: $remote_evidence"
