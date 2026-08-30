#!/bin/sh

set -eu

root=${VELA_LAB_RUNNER_ROOT:-/var/lib/vela-lab/mock-runner}
container=${VELA_LAB_RUNNER_CONTAINER:-vela-h3-mock-runner}

fail() {
	printf 'write-mock-runner-container-identity: %s\n' "$*" >&2
	exit 1
}

[ "$(id -u)" -eq 0 ] || fail "run as root"
[ "$root" = /var/lib/vela-lab/mock-runner ] ||
	fail "VELA_LAB_RUNNER_ROOT must remain /var/lib/vela-lab/mock-runner"
[ -d "$root/config" ] && [ ! -L "$root/config" ] || fail "Runner config directory is missing or unsafe"
[ "$(docker container inspect "$container" --format '{{index .Config.Labels "vela.ai.component"}}' 2>/dev/null)" = h3-mock-runner ] ||
	fail "container $container is not managed by this deployment"
[ "$(docker container inspect "$container" --format '{{.State.Status}}')" = running ] ||
	fail "container $container is not running"

container_id=$(docker container inspect "$container" --format '{{.Id}}')
image=$(docker container inspect "$container" --format '{{.Config.Image}}')
printf '%s\n' "$container_id" | grep -Eq '^[0-9a-f]{64}$' || fail "container ID is invalid"
printf '%s\n' "$image" | grep -Eq '^10\.1\.200\.17:5443/vela-lab/vela-h3-runner@sha256:[0-9a-f]{64}$' ||
	fail "container image is not the fixed private Runner repository"

temporary=$(mktemp "$root/config/.container-identity.XXXXXX")
cleanup() { rm -f -- "$temporary"; }
trap cleanup EXIT HUP INT TERM
printf 'schema=vela-lab-runner-container-identity-v1\ncontainer_id=%s\nimage=%s\n' \
	"$container_id" "$image" >"$temporary"
chown 0:0 "$temporary"
chmod 0444 "$temporary"
mv -f -- "$temporary" "$root/config/container-identity"
temporary=
trap - EXIT HUP INT TERM

printf 'schema=vela-lab-runner-container-identity-v1 container_id=%s image=%s\n' "$container_id" "$image"
