#!/usr/bin/env bash
set -euo pipefail

shards="${1:-2}"
timeout="${VELA_INTEGRATION_TIMEOUT:-20m}"
concurrency="${VELA_INTEGRATION_CONCURRENCY:-$shards}"
if [[ ! "$shards" =~ ^[1-9][0-9]*$ ]]; then
  echo "usage: $0 [positive-shard-count]" >&2
  exit 2
fi
if [[ ! "$concurrency" =~ ^[1-9][0-9]*$ ]]; then
  echo "VELA_INTEGRATION_CONCURRENCY must be a positive integer" >&2
  exit 2
fi

root_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$root_dir"
work_dir=$(mktemp -d "${TMPDIR:-/tmp}/vela-integration-shards.XXXXXX")
status=0
cleanup() {
  if (( status == 0 )); then
    rm -rf "$work_dir"
  else
    printf 'integration shard logs preserved at %s\n' "$work_dir" >&2
  fi
}
trap cleanup EXIT

mapfile -t tests < <(go test -tags=integration ./internal/integration -list '^Test' | awk '/^Test[A-Za-z0-9_]+$/ { print }')
if (( ${#tests[@]} == 0 )); then
  echo "no integration tests discovered" >&2
  exit 1
fi

if (( shards > ${#tests[@]} )); then
  printf 'requested %d shards but only %d tests were discovered; using %d shards\n' \
    "$shards" "${#tests[@]}" "${#tests[@]}" >&2
  shards=${#tests[@]}
fi
if (( concurrency > shards )); then
  concurrency=$shards
fi

for ((index = 0; index < shards; index++)); do
  : > "$work_dir/tests-$index"
done
for ((index = 0; index < ${#tests[@]}; index++)); do
  shard=$((index % shards))
  printf '%s\n' "${tests[index]}" >> "$work_dir/tests-$shard"
done

if [[ "${VELA_INTEGRATION_SHARDS_DRY_RUN:-0}" == "1" ]]; then
  for ((index = 0; index < shards; index++)); do
    count=$(wc -l < "$work_dir/tests-$index" | tr -d ' ')
    printf 'shard %d: %s tests\n' "$((index + 1))" "$count"
  done
  exit 0
fi

declare -a pids=()
for ((batch = 0; batch < shards; batch += concurrency)); do
  batch_end=$((batch + concurrency))
  if (( batch_end > shards )); then
    batch_end=$shards
  fi
  for ((index = batch; index < batch_end; index++)); do
    pattern=$(paste -sd'|' "$work_dir/tests-$index")
    log="$work_dir/shard-$index.log"
    (
      go test -tags=integration ./internal/integration \
        -run "^(${pattern})$" -count=1 -timeout="$timeout" -v
    ) >"$log" 2>&1 &
    pids[index]=$!
    printf 'started shard %d/%d (concurrency %d; %s)\n' "$((index + 1))" "$shards" "$concurrency" "$log"
  done
  for ((index = batch; index < batch_end; index++)); do
    if ! wait "${pids[index]}"; then
      status=1
      printf 'shard %d failed; output:\n' "$((index + 1))" >&2
      cat "$work_dir/shard-$index.log" >&2
    else
      printf 'shard %d passed\n' "$((index + 1))"
    fi
  done
done
exit "$status"
