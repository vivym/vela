#!/bin/sh

set -eu

root=${VELA_LAB_RUNNER_ROOT:-/var/lib/vela-lab/mock-runner}
container=${VELA_LAB_RUNNER_CONTAINER:-vela-h3-mock-runner}
purge=${1:-}

fail() {
	printf 'remove-mock-runner: %s\n' "$*" >&2
	exit 1
}

[ "$(id -u)" -eq 0 ] || fail "run as root"
[ "$root" = /var/lib/vela-lab/mock-runner ] ||
	fail "VELA_LAB_RUNNER_ROOT must remain /var/lib/vela-lab/mock-runner"

if docker container inspect "$container" >/dev/null 2>&1; then
	[ "$(docker container inspect "$container" --format '{{index .Config.Labels "vela.ai.component"}}')" = h3-mock-runner ] ||
		fail "refusing to remove an unmanaged container named $container"
	docker stop --time 20 "$container" >/dev/null
	docker rm "$container" >/dev/null
fi

if [ "$purge" = --purge ]; then
	[ -d "$root" ] || fail "$root is not a directory"
	find "$root" -xdev -mindepth 1 -delete
	rmdir "$root"
	printf 'container=%s removed\ndata=%s removed\n' "$container" "$root"
elif [ -n "$purge" ]; then
	fail "only --purge is accepted"
else
	printf 'container=%s removed\ndata=%s preserved\n' "$container" "$root"
fi
