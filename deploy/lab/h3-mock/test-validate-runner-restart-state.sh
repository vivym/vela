#!/bin/sh

set -eu

script_dir=$(CDPATH='' cd -- "$(dirname -- "$0")" && pwd)
validator=$script_dir/validate-runner-restart-state.sh
temporary=
attempt_id=84000000-0000-0000-0000-000000000001
video=/var/lib/vela/worker/scratch/outputs/$attempt_id/video.mp4
thumbnail=/var/lib/vela/worker/scratch/outputs/$attempt_id/thumbnail.webp

fail() {
	printf 'test-validate-runner-restart-state: %s\n' "$*" >&2
	exit 1
}

cleanup() {
	if [ -n "$temporary" ] && [ -d "$temporary" ]; then
		find "$temporary" -xdev -mindepth 1 -delete
		rmdir "$temporary"
	fi
}

reset_fixture() {
	find "$temporary/root" -xdev -mindepth 1 -delete
	mkdir -p "$temporary/root/scratch/runner-state" "$temporary/root/scratch/outputs"
}

write_success_state() {
	first_path=${1:-$video}
	second_path=${2:-$thumbnail}
	state_dir=$temporary/root/scratch/runner-state/$attempt_id
	mkdir -p "$state_dir"
	jq -n \
		--arg attempt_id "$attempt_id" \
		--arg first_path "$first_path" \
		--arg second_path "$second_path" \
		'{identity:{attempt_id:$attempt_id},state:4,outputs:[{path:$first_path},{path:$second_path}]}' \
		>"$state_dir/state.json"
}

run_validator() {
	env VELA_LAB_RUNNER_TEST_ONLY=1 VELA_LAB_RUNNER_ROOT="$temporary/root" "$validator"
}

expect_ready() {
	expected=${1:-}
	result=$(run_validator) || fail "validator rejected a ready fixture"
	printf '%s\n' "$result" | grep -F "$expected" >/dev/null ||
		fail "ready fixture omitted $expected"
}

expect_failure() {
	expected=${1:-}
	if result=$(run_validator 2>&1); then
		fail "validator accepted an unsafe fixture: $result"
	fi
	printf '%s\n' "$result" | grep -F "$expected" >/dev/null ||
		fail "unsafe fixture did not report $expected: $result"
}

command -v jq >/dev/null 2>&1 || fail "jq is required"
[ -x "$validator" ] || fail "restart-state validator is missing"
trap cleanup EXIT HUP INT TERM
temporary=$(mktemp -d "${TMPDIR:-/tmp}/vela-runner-restart-state-test.XXXXXX")
mkdir -p "$temporary/root"

reset_fixture
expect_ready 'checked_states=0 retired_success_states=0'

reset_fixture
write_success_state
mkdir -p "$temporary/root/scratch/outputs/$attempt_id"
printf 'video' >"$temporary/root/scratch/outputs/$attempt_id/video.mp4"
printf 'thumbnail' >"$temporary/root/scratch/outputs/$attempt_id/thumbnail.webp"
expect_ready 'checked_states=1 retired_success_states=0'

reset_fixture
write_success_state
expect_ready 'checked_states=1 retired_success_states=1'

reset_fixture
write_success_state
mkdir -p "$temporary/root/scratch/outputs/$attempt_id"
printf 'video' >"$temporary/root/scratch/outputs/$attempt_id/video.mp4"
expect_failure "references missing output $thumbnail"

reset_fixture
write_success_state /var/lib/vela/worker/scratch/outputs/another-attempt/video.mp4
expect_failure 'contains an unexpected output path'

reset_fixture
write_success_state
mkdir -p "$temporary/outside-output"
ln -s "$temporary/outside-output" "$temporary/root/scratch/outputs/$attempt_id"
expect_failure 'has an unsafe output directory'

printf 'result=PASS tests=6\n'
