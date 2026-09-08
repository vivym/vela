#!/usr/bin/env bash
# Native CPU checks for typed journal transitions and the existing Node startup exchange.
set -euo pipefail

owner_repo="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
owner_evidence="${VELA_OWNER_EVIDENCE:-$(mktemp -d /tmp/vela-journal-owner.XXXXXX)}"
owner_cache="${VELA_OWNER_BUILD_CACHE:-$owner_evidence/go-cache}"
owner_modules="$(go env GOMODCACHE)"
owner_builder='golang@sha256:e30143be198ab04cf7ba25fba83ab3a692ca584c994aad0bf131fa0eb32dd8c1'
[[ "$owner_evidence" = /* && "$owner_cache" = /* ]] || { echo 'Evidence and cache paths must be absolute' >&2; exit 2; }
mkdir -p "$owner_evidence/image/rootfs/run" "$owner_evidence/image/rootfs/tmp" "$owner_cache"
chmod 1777 "$owner_evidence/image/rootfs/tmp"
git -C "$owner_repo" rev-parse HEAD > "$owner_evidence/source.txt"
git -C "$owner_repo" status --short >> "$owner_evidence/source.txt"
git -C "$owner_repo" diff HEAD --binary -- internal/modelruntime internal/nodeagent internal/runtimechannel hack/run-journal-owner-native.sh > "$owner_evidence/source.patch"
# Include newly added Go files as well as tracked edits in a dirty worktree.
while IFS= read -r -d '' owner_untracked; do
  owner_diff_status=0
  git -C "$owner_repo" diff --no-index --binary -- /dev/null "$owner_untracked" \
    >> "$owner_evidence/source.patch" || owner_diff_status=$?
  if [[ "$owner_diff_status" != 0 && "$owner_diff_status" != 1 ]]; then
    exit "$owner_diff_status"
  fi
done < <(git -C "$owner_repo" ls-files -z --others --exclude-standard -- internal/modelruntime internal/nodeagent internal/runtimechannel)
shasum -a 256 "$owner_evidence/source.patch" > "$owner_evidence/source-patch.sha256"
docker version > "$owner_evidence/docker-version.txt"

# Source/module mounts exist only in the compiler container. Test containers get
# the static binaries and empty scratch directories, with no host mounts/network.
for owner_package in modelruntime nodeagent; do
  docker run --rm --network none --cpus 4 --memory 4g --pids-limit 512 \
    -v "$owner_repo:/workspace:ro" -v "$owner_modules:/go/pkg/mod:ro" \
    -v "$owner_evidence:/evidence" -v "$owner_cache:/build-cache" -w /workspace \
    -e GOCACHE=/build-cache -e GOPROXY=off -e CGO_ENABLED=1 \
    "$owner_builder" go test -race -c -ldflags '-linkmode external -extldflags=-static' \
    -o "/evidence/image/rootfs/$owner_package.test" "./internal/$owner_package" \
    > "$owner_evidence/build-$owner_package.log" 2>&1
done
cat > "$owner_evidence/image/Dockerfile" <<'DOCKERFILE'
FROM scratch
COPY rootfs /
DOCKERFILE
owner_image="$(docker build --network none -q "$owner_evidence/image")"
echo "$owner_image" > "$owner_evidence/image.txt"
shasum -a 256 "$owner_evidence/image/rootfs/"*.test > "$owner_evidence/binaries.sha256"
owner_selected='^(TestJournalOwner|TestJournalCommand|TestJournalResponse|TestJournalRemote|TestJournalRead|TestExecutionNonAdmission|TestTerminalNonAdmission|TestRuntimeServer(RemoteStartup|RetainsBackendOwnership|FreezesLaunch|DrainDoesNotRetire|BackendStartupPersistence|RejectsMalformedBackendLifecycle))'
docker run --rm --network none --user 10001:10001 --cap-drop ALL --cpus 4 --memory 4g --pids-limit 256 \
  "$owner_image" /modelruntime.test -test.run "$owner_selected" -test.count=1 -test.v -test.timeout=3m \
  > "$owner_evidence/modelruntime.log" 2>&1

# This existing test creates an actual non-root PID-1 child. It needs PID namespace
# creation and process inspection; its permit is explicitly mocked, not a new issuer.
docker run --rm --network none --cap-add SYS_ADMIN --cap-add SYS_PTRACE \
  --security-opt seccomp=unconfined --cpus 4 --memory 4g --pids-limit 256 \
  "$owner_image" /nodeagent.test -test.run '^(TestRuntimeStartupReservation.*|TestRuntimeStartupLedger(BeforeActualFactory|RetainsExactOwnerExit|RestartDoesNotReconstructOwner|ReservesExitCapacity|RejectsUnboundRequests|UncertainAppendRemainsConsumed|RejectsMissingAndChangedState)|TestJournalEndpoint|TestJournalServer.*|TestRuntimeChannel(RoundTrip|LargeRequestBounds|RejectsUntrustedExchange|ReplyRejectsLostLifetime)|TestRuntimeCallerRejectsInvalidMessages)$' \
  -test.count=1 -test.v -test.timeout=3m > "$owner_evidence/node-startup.log" 2>&1
if rg -q -- '--- SKIP:' "$owner_evidence/modelruntime.log" "$owner_evidence/node-startup.log"; then
  echo 'Selected native checks unexpectedly skipped; inspect evidence' >&2
  exit 1
fi
echo "Native journal-owner checks passed: $owner_evidence"
