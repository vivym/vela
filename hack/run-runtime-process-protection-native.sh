#!/usr/bin/env bash
# CPU-only proof of the actual Runtime entry's same-UID memory boundary.
set -euo pipefail
protection_repo="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
protection_evidence="${VELA_PROTECTION_EVIDENCE:-$(mktemp -d /tmp/vela-runtime-protection.XXXXXX)}"
protection_cache="${VELA_PROTECTION_BUILD_CACHE:-$protection_evidence/go-cache}"
protection_modules="$(go env GOMODCACHE)"
protection_builder='golang@sha256:e30143be198ab04cf7ba25fba83ab3a692ca584c994aad0bf131fa0eb32dd8c1'
[[ "$protection_evidence" = /* && "$protection_cache" = /* ]] || exit 2
mkdir -p "$protection_evidence/image/rootfs/tmp" "$protection_cache"
chmod 1777 "$protection_evidence/image/rootfs/tmp"
git -C "$protection_repo" rev-parse HEAD > "$protection_evidence/source.txt"
git -C "$protection_repo" diff HEAD --binary -- cmd/vela-model-runtime > "$protection_evidence/source.patch"
while IFS= read -r -d '' protection_file; do
  protection_diff_status=0
  git -C "$protection_repo" diff --no-index --binary /dev/null "$protection_file" >> "$protection_evidence/source.patch" || protection_diff_status=$?
  [[ "$protection_diff_status" = 0 || "$protection_diff_status" = 1 ]] || exit "$protection_diff_status"
done < <(git -C "$protection_repo" ls-files -z --others --exclude-standard -- cmd/vela-model-runtime)
docker run --rm --network none --cpus 4 --memory 4g --pids-limit 512 \
  -v "$protection_repo:/workspace:ro" -v "$protection_modules:/go/pkg/mod:ro" \
  -v "$protection_evidence:/evidence" -v "$protection_cache:/build-cache" -w /workspace \
  -e GOCACHE=/build-cache -e GOPROXY=off -e CGO_ENABLED=1 "$protection_builder" \
  go test -race -c -ldflags '-linkmode external -extldflags=-static' \
  -o /evidence/image/rootfs/runtime.test ./cmd/vela-model-runtime > "$protection_evidence/build.log" 2>&1
cat > "$protection_evidence/image/Dockerfile" <<'DOCKERFILE'
FROM scratch
COPY rootfs /
DOCKERFILE
protection_image="$(docker build --network none -q "$protection_evidence/image")"
echo "$protection_image" > "$protection_evidence/image.txt"
shasum -a 256 "$protection_evidence/image/rootfs/runtime.test" > "$protection_evidence/binary.sha256"
# seccomp must allow ptrace in the baseline. The unprivileged process has no
# capabilities; both phases use the same environment and uid/gid.
docker run --rm --network none --user 10001:10001 --cap-drop ALL --security-opt seccomp=unconfined \
  --cpus 4 --memory 1g --pids-limit 128 "$protection_image" /runtime.test \
  -test.run '^(TestRuntimeProcessProtection|TestRuntimeProcessProtectionFailsClosed|TestRunServesResidentRuntimeUntilShutdown)$' \
  -test.count=1 -test.v -test.timeout=45s > "$protection_evidence/native.log" 2>&1
for protection_test in TestRuntimeProcessProtection TestRuntimeProcessProtectionFailsClosed TestRunServesResidentRuntimeUntilShutdown; do
  rg -q -- "^--- PASS: $protection_test " "$protection_evidence/native.log" || exit 1
done
if rg -q -- '--- SKIP:|WARNING: DATA RACE' "$protection_evidence/native.log"; then exit 1; fi
echo "Runtime process protection checks passed: $protection_evidence"
