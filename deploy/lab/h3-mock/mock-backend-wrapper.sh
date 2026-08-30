#!/bin/sh

set -eu

mode_file=/etc/vela-runner/mock-mode

[ -f "$mode_file" ] && [ ! -L "$mode_file" ] || {
	printf 'mock-backend-wrapper: mode file is missing or unsafe\n' >&2
	exit 1
}
[ "$(wc -l <"$mode_file" | tr -d ' ')" -eq 1 ] || {
	printf 'mock-backend-wrapper: mode file must contain exactly one line\n' >&2
	exit 1
}
mode=$(sed -n '1p' "$mode_file")
case "$mode" in
	success | hang) ;;
	failure)
		# The deployed backend predates the empty-array receipt fix. Pin one
		# deterministic mock GPU so its strict failure receipt remains valid.
		set -- --mock-failure-gpu-index 0 "$@"
		;;
	*)
		printf 'mock-backend-wrapper: unsupported lab mode %s\n' "$mode" >&2
		exit 1
		;;
esac

exec /opt/vela/bin/h3-backend --mock-mode "$mode" "$@"
