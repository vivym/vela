#!/usr/bin/env bash
# Run after run-remote-runtime-cli-native.sh, without changing product source.
set -euo pipefail
control_repo="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
control_cli="${1:?absolute CLI evidence directory required}"
control_out="${2:?absolute regression evidence directory required}"
[[ "$control_cli" = /* && "$control_out" = /* ]] || exit 2
mkdir -p "$control_out"
control_builder='golang@sha256:e30143be198ab04cf7ba25fba83ab3a692ca584c994aad0bf131fa0eb32dd8c1'
for control_mode in enabled disabled; do
  control_flags=()
  if [[ "$control_mode" == disabled ]]; then control_flags=(-DDISABLE_EXITKILL); fi
  docker run --rm --network none --cpus 2 --memory 1g --pids-limit 128 \
    -v "$control_repo:/workspace:ro" -v "$control_out:/evidence" -w /workspace "$control_builder" \
    cc -std=c11 -O2 -Wall -Wextra -Werror -static "${control_flags[@]}" \
    hack/experiments/runtime-exec-observer/observer.c -o "/evidence/observer-$control_mode" \
    > "$control_out/build-$control_mode.log" 2>&1
done
# The control must alter exactly the observer option in the measured CLI image.
cmp "$control_out/observer-enabled" "$control_cli/image/rootfs/exec-observer"
control_tag="vela-exec-observer-cli-control:local-$$"
docker tag "$(cat "$control_cli/image.txt")" "$control_tag"
cat > "$control_out/Dockerfile" <<DOCKERFILE
FROM $control_tag
COPY observer-disabled /exec-observer
DOCKERFILE
docker build --network none --progress plain --iidfile "$control_out/image.txt" "$control_out" > "$control_out/build-image.log" 2>&1
control_exit=0
docker run --rm --network none --cap-add SYS_ADMIN --cap-add SYS_PTRACE --security-opt seccomp=unconfined \
  --cpus 4 --memory 4g --pids-limit 256 "$(cat "$control_out/image.txt")" /nodeagent.test \
  -test.run='^TestJournalServerExecObservedRemoteCLI$/^(permit|observer-lost-before-permit|observer-lost-after-permit)$' \
  -test.count=1 -test.v -test.timeout=45s > "$control_out/native.log" 2>&1 || control_exit=$?
echo "$control_exit" > "$control_out/exit-code.txt"
[[ "$control_exit" = 1 ]] || exit 1
rg -q -- '--- PASS: TestJournalServerExecObservedRemoteCLI/permit ' "$control_out/native.log"
for control_case in observer-lost-before-permit observer-lost-after-permit; do
  rg -q -- "--- FAIL: TestJournalServerExecObservedRemoteCLI/$control_case " "$control_out/native.log"
done
if rg -q -- '--- SKIP:|WARNING: DATA RACE' "$control_out/native.log"; then exit 1; fi
shasum -a 256 "$control_out/observer-"* > "$control_out/binaries.sha256"
echo "Disabled EXITKILL produced both expected actual CLI failures; normal CLI control passed: $control_out"
