#!/bin/sh

set -eu

root=${VELA_LAB_RUNNER_ROOT:-/var/lib/vela-lab/mock-runner}
state_root=$root/scratch/runner-state

fail() {
	printf 'validate-runner-restart-state: %s\n' "$*" >&2
	exit 1
}

[ "$root" = /var/lib/vela-lab/mock-runner ] ||
	fail "VELA_LAB_RUNNER_ROOT must remain /var/lib/vela-lab/mock-runner"
[ -d "$state_root" ] && [ ! -L "$state_root" ] || fail "Runner state root is missing or unsafe"
command -v jq >/dev/null 2>&1 || fail "jq is required"

checked=0
for state_file in "$state_root"/*/state.json; do
	[ -f "$state_file" ] || continue
	[ ! -L "$state_file" ] || fail "Runner state file is a symbolic link"
	attempt_id=$(jq -er '.identity.attempt_id' "$state_file") || fail "Runner state identity is invalid"
	[ "$(basename "$(dirname "$state_file")")" = "$attempt_id" ] ||
		fail "Runner state directory does not match Attempt identity"
	state=$(jq -er '.state' "$state_file") || fail "Runner state enum is invalid"
	case "$state" in 1 | 2 | 3 | 4 | 5 | 6) ;; *) fail "Runner state enum is unsupported" ;; esac
	if [ "$state" -eq 4 ]; then
		outputs=$(jq -er '.outputs | if length > 0 then .[].path else error("successful state has no outputs") end' "$state_file") ||
			fail "successful Runner state $attempt_id has no valid output inventory"
		old_ifs=$IFS
		IFS='
'
		for output in $outputs; do
			case "$output" in
				/var/lib/vela/worker/scratch/outputs/"$attempt_id"/*) ;;
				*) fail "successful Runner state $attempt_id contains an unexpected output path" ;;
			esac
			host_output=$root/scratch/${output#/var/lib/vela/worker/scratch/}
			[ -f "$host_output" ] && [ ! -L "$host_output" ] ||
				fail "successful Runner state $attempt_id references missing output $output"
		done
		IFS=$old_ifs
	fi
	checked=$((checked + 1))
done

printf 'schema=vela-lab-runner-restart-state-v1 result=READY checked_states=%s production_gates=0/9\n' "$checked"
