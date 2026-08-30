#!/bin/sh

set -eu

image=${1:-}
apply=${2:-}
root=${VELA_LAB_RUNNER_ROOT:-/var/lib/vela-lab/mock-runner}
container=${VELA_LAB_RUNNER_CONTAINER:-vela-h3-mock-runner}
script_dir=$(CDPATH='' cd -- "$(dirname -- "$0")" && pwd)
upgraded=false
transition_started=false
files_changed=false

fail() {
	printf 'upgrade-mock-runner-mode-control: %s\n' "$*" >&2
	exit 1
}

rollback() {
	result=$?
	trap - EXIT HUP INT TERM
	if [ "$upgraded" != true ]; then
		if [ "$transition_started" = true ] && docker container inspect "$container" >/dev/null 2>&1; then
			if [ "$(docker container inspect "$container" --format '{{index .Config.Labels "vela.ai.component"}}' 2>/dev/null)" = h3-mock-runner ]; then
				docker rm --force "$container" >/dev/null 2>&1 || true
			fi
		fi
		if [ "$files_changed" = true ]; then
			rm -f -- "$root/config/mock-backend-wrapper.sh" "$root/config/mock-mode" \
				"$root/admin/set-mock-runner-mode.sh" "$root/admin/start-mock-runner-container.sh" \
				"$root/admin/upgrade-mock-runner-catalog-profile.sh" \
				"$root/admin/validate-runner-restart-state.sh"
		fi
		if [ "$transition_started" = true ]; then
			if "$script_dir/start-mock-runner-container.sh" "$image" legacy-success >/dev/null 2>&1; then
				printf 'upgrade-mock-runner-mode-control: rollback restored the legacy success-only Runner\n' >&2
			else
				printf 'upgrade-mock-runner-mode-control: rollback could not restore the legacy Runner\n' >&2
			fi
		fi
	fi
	exit "$result"
}
trap rollback EXIT HUP INT TERM

[ "$apply" = --apply ] || fail "usage: $0 <registry/repository@sha256:digest> --apply"
[ "$(id -u)" -eq 0 ] || fail "run as root"
printf '%s\n' "$image" | grep -Eq '@sha256:[0-9a-f]{64}$' || fail "image must use an immutable sha256 digest"
[ "$root" = /var/lib/vela-lab/mock-runner ] || fail "VELA_LAB_RUNNER_ROOT must remain /var/lib/vela-lab/mock-runner"
for source in mock-backend-wrapper.sh set-mock-runner-mode.sh start-mock-runner-container.sh upgrade-mock-runner-catalog-profile.sh validate-runner-restart-state.sh; do
	[ -f "$script_dir/$source" ] || fail "$source is missing"
done
[ "$(docker container inspect "$container" --format '{{index .Config.Labels "vela.ai.component"}}' 2>/dev/null)" = h3-mock-runner ] ||
	fail "container $container is not managed by this deployment"
[ "$(docker container inspect "$container" --format '{{.State.Health.Status}}')" = healthy ] ||
	fail "container $container is not healthy"
[ "$(docker container inspect "$container" --format '{{.Config.Image}}')" = "$image" ] || fail "container image does not match"
[ "$(docker container inspect "$container" --format '{{range .Config.Env}}{{println .}}{{end}}' |
	sed -n 's/^VELA_RUNNER_BACKEND_COMMAND=//p')" = /opt/vela/bin/h3-backend ] ||
	fail "container is not the expected legacy success-only layout"
process_count=$(docker top "$container" -eo pid,ppid,comm 2>/dev/null |
	sed '1d; /^[[:space:]]*$/d' | wc -l | tr -d ' ')
[ "$process_count" -eq 1 ] || fail "Runner has a child process; confirm zero active Attempt before upgrading"
active_gpu_processes=$(nvidia-smi --query-compute-apps=gpu_uuid --format=csv,noheader,nounits 2>/dev/null |
	sed '/^[[:space:]]*$/d' | wc -l | tr -d ' ')
[ "$active_gpu_processes" -eq 0 ] || fail "Worker has active GPU compute processes"
"$script_dir/validate-runner-restart-state.sh" >/dev/null

files_changed=true
install -m 0555 -o 0 -g 0 "$script_dir/mock-backend-wrapper.sh" "$root/config/mock-backend-wrapper.sh"
printf 'success\n' >"$root/config/mock-mode"
chown 0:0 "$root/config/mock-mode"
chmod 0444 "$root/config/mock-mode"
install -m 0550 -o 0 -g 0 "$script_dir/set-mock-runner-mode.sh" "$root/admin/set-mock-runner-mode.sh"
install -m 0550 -o 0 -g 0 "$script_dir/start-mock-runner-container.sh" "$root/admin/start-mock-runner-container.sh"
install -m 0550 -o 0 -g 0 "$script_dir/upgrade-mock-runner-catalog-profile.sh" "$root/admin/upgrade-mock-runner-catalog-profile.sh"
install -m 0550 -o 0 -g 0 "$script_dir/validate-runner-restart-state.sh" "$root/admin/validate-runner-restart-state.sh"
transition_started=true
docker stop --time 20 "$container" >/dev/null
docker rm "$container" >/dev/null
"$script_dir/start-mock-runner-container.sh" "$image" mode-control
upgraded=true
printf 'schema=vela-lab-mock-runner-mode-upgrade-v1 result=PASS mode=success production_gates=0/9\n'
