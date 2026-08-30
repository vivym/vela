#!/bin/sh

set -eu

expected=${1:-}
requested=${2:-}
apply=${3:-}
root=${VELA_LAB_RUNNER_ROOT:-/var/lib/vela-lab/mock-runner}
container=${VELA_LAB_RUNNER_CONTAINER:-vela-h3-mock-runner}
mode_file=$root/config/mock-mode
wrapper=$root/config/mock-backend-wrapper.sh
temporary=

fail() {
	printf 'set-mock-runner-mode: %s\n' "$*" >&2
	exit 1
}

cleanup() {
	[ -z "$temporary" ] || rm -f -- "$temporary"
}
trap cleanup EXIT HUP INT TERM

[ "$apply" = --apply ] ||
	fail "usage: $0 <expected:success|hang|failure> <requested:success|hang|failure> --apply"
[ "$(id -u)" -eq 0 ] || fail "run as root"
[ "$root" = /var/lib/vela-lab/mock-runner ] ||
	fail "VELA_LAB_RUNNER_ROOT must remain /var/lib/vela-lab/mock-runner"
case "$expected" in success | hang | failure) ;; *) fail "expected mode must be success, hang, or failure" ;; esac
case "$requested" in success | hang | failure) ;; *) fail "requested mode must be success, hang, or failure" ;; esac
[ "$expected" != "$requested" ] || fail "expected and requested modes must differ"
command -v docker >/dev/null 2>&1 || fail "docker is required"
command -v ip >/dev/null 2>&1 || fail "ip is required"
command -v jq >/dev/null 2>&1 || fail "jq is required"

[ "$(hostname)" = ubuntu ] || fail "mode switching is restricted to the two lab Workers"
worker_ip=$(ip -j -4 address show dev eno1 2>/dev/null |
	jq -r '.[0].addr_info[] | select(.family == "inet" and .scope == "global") | .local' |
	head -n 1)
case "$worker_ip" in
	10.1.200.19 | 10.1.200.16) ;;
	*) fail "mode switching is restricted to Worker LAN addresses 10.1.200.19 and 10.1.200.16" ;;
esac
[ -d "$root/config" ] && [ ! -L "$root/config" ] || fail "Runner config directory is missing or unsafe"
[ -f "$mode_file" ] && [ ! -L "$mode_file" ] || fail "installed mode file is missing or unsafe"
[ -f "$wrapper" ] && [ ! -L "$wrapper" ] || fail "installed backend wrapper is missing or unsafe"
[ "$(stat -c '%u:%g:%a' "$mode_file")" = 0:0:444 ] || fail "mode file ownership or permissions are unsafe"
[ "$(stat -c '%u:%g:%a' "$wrapper")" = 0:0:555 ] || fail "backend wrapper ownership or permissions are unsafe"
[ "$(docker container inspect "$container" --format '{{index .Config.Labels "vela.ai.component"}}' 2>/dev/null)" = h3-mock-runner ] ||
	fail "container $container is not managed by this deployment"
[ "$(docker container inspect "$container" --format '{{index .Config.Labels "vela.ai.environment"}}')" = non-production-lab ] ||
	fail "container $container is not marked as non-production"
[ "$(docker container inspect "$container" --format '{{.State.Health.Status}}')" = healthy ] ||
	fail "container $container is not healthy"
[ "$(docker container inspect "$container" --format '{{range .Config.Env}}{{println .}}{{end}}' |
	sed -n 's/^VELA_RUNNER_BACKEND_COMMAND=//p')" = /etc/vela-runner/mock-backend-wrapper.sh ] ||
	fail "container does not use the guarded mock backend wrapper"

[ "$(wc -l <"$mode_file" | tr -d ' ')" -eq 1 ] || fail "mode file must contain exactly one line"
current=$(sed -n '1p' "$mode_file")
[ "$current" = "$expected" ] || fail "current mode is $current, expected $expected"
temporary=$(mktemp "$root/config/.mock-mode.XXXXXX")
printf '%s\n' "$requested" >"$temporary"
chown 0:0 "$temporary"
chmod 0444 "$temporary"
mv -f -- "$temporary" "$mode_file"
temporary=

observed=$(docker exec --user 10001:10001 "$container" /bin/sh -ec \
	'test -f /etc/vela-runner/mock-mode && test ! -L /etc/vela-runner/mock-mode && sed -n "1p" /etc/vela-runner/mock-mode') ||
	fail "container could not observe the updated mode file"
[ "$observed" = "$requested" ] || fail "container observed mode $observed, expected $requested"
mode_sha256=$(sha256sum "$mode_file" | awk '{print $1}')
printf 'schema=vela-lab-mock-runner-mode-v1 host=%s lan_ip=%s before=%s after=%s mode_sha256=%s production_gates=0/9\n' \
	"$(hostname)" "$worker_ip" "$current" "$requested" "$mode_sha256"
