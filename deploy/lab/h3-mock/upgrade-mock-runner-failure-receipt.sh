#!/bin/sh

set -eu

apply=${1:-}
root=${VELA_LAB_RUNNER_ROOT:-/var/lib/vela-lab/mock-runner}
container=${VELA_LAB_RUNNER_CONTAINER:-vela-h3-mock-runner}
script_dir=$(CDPATH='' cd -- "$(dirname -- "$0")" && pwd)
expected_wrapper_sha256=c5765cd04a7bcd860fa3923c558f562f5254df53cfe8db946f6f275ae42d842d
expected_helper_sha256=37b6b80a1fc1c7b7b56f724ce9bc7bfd29dbea384b5d538d3fe91e284bd8079e
expected_start_sha256=d32907828601f84053882f5c56ca21efcff625052e0efdae66b8320f42f9725e
expected_runner_image=10.1.200.17:5443/vela-lab/vela-h3-runner@sha256:71af1330eefdfff2a33d68e5f8c53c66ebe5b402dc28c35b3ff7516357ec4ca3
backup=
image=
stopped=false
upgraded=false

fail() {
	printf 'upgrade-mock-runner-failure-receipt: %s\n' "$*" >&2
	exit 1
}

install_file() {
	source=$1
	destination=$2
	mode=$3
	temporary=$(mktemp "$(dirname "$destination")/.failure-receipt.XXXXXX")
	install -m "$mode" -o 0 -g 0 "$source" "$temporary"
	mv -f -- "$temporary" "$destination"
}

rollback() {
	result=$?
	trap - EXIT HUP INT TERM
	if [ "$upgraded" != true ] && [ -n "$backup" ] && [ -f "$backup" ]; then
		if [ "$stopped" = true ] && docker container inspect "$container" >/dev/null 2>&1; then
			if [ "$(docker container inspect "$container" --format '{{index .Config.Labels "vela.ai.component"}}' 2>/dev/null)" = h3-mock-runner ]; then
				docker rm --force "$container" >/dev/null 2>&1 || true
			fi
		fi
		install_file "$backup" "$root/config/mock-backend-wrapper.sh" 0555 || true
		if [ "$stopped" = true ] && [ -n "$image" ]; then
			if "$root/admin/start-mock-runner-container.sh" "$image" mode-control >/dev/null 2>&1; then
				printf 'upgrade-mock-runner-failure-receipt: rollback restored the previous Runner\n' >&2
			else
				printf 'upgrade-mock-runner-failure-receipt: rollback could not restart the previous Runner\n' >&2
			fi
		fi
	fi
	[ -z "$backup" ] || rm -f -- "$backup"
	[ "$result" -ne 0 ] || result=1
	exit "$result"
}
trap rollback EXIT HUP INT TERM

[ "$apply" = --apply ] || fail "usage: $0 --apply"
[ "$(id -u)" -eq 0 ] || fail "run as root"
[ "$root" = /var/lib/vela-lab/mock-runner ] ||
	fail "VELA_LAB_RUNNER_ROOT must remain /var/lib/vela-lab/mock-runner"
for command in docker ip jq nvidia-smi sha256sum; do
	command -v "$command" >/dev/null 2>&1 || fail "$command is required"
done
[ -f "$script_dir/mock-backend-wrapper.sh" ] && [ ! -L "$script_dir/mock-backend-wrapper.sh" ] ||
	fail "mock-backend-wrapper.sh is missing or unsafe"
[ "$(hostname)" = ubuntu ] || fail "upgrade is restricted to the two lab Workers"
worker_ip=$(ip -j -4 address show dev eno1 2>/dev/null |
	jq -r '.[0].addr_info[] | select(.family == "inet" and .scope == "global") | .local' |
	head -n 1)
case "$worker_ip" in
	10.1.200.19 | 10.1.200.16) ;;
	*) fail "upgrade is restricted to Worker LAN addresses 10.1.200.19 and 10.1.200.16" ;;
esac

wrapper=$root/config/mock-backend-wrapper.sh
helper=$root/admin/set-mock-runner-mode.sh
start=$root/admin/start-mock-runner-container.sh
mode_file=$root/config/mock-mode
for installed in "$wrapper" "$helper" "$start" "$mode_file"; do
	[ -f "$installed" ] && [ ! -L "$installed" ] || fail "$installed is missing or unsafe"
done
[ "$(sha256sum "$wrapper" | awk '{print $1}')" = "$expected_wrapper_sha256" ] || fail "installed wrapper is not the expected predecessor"
[ "$(sha256sum "$helper" | awk '{print $1}')" = "$expected_helper_sha256" ] || fail "installed helper is not the expected revision"
[ "$(sha256sum "$start" | awk '{print $1}')" = "$expected_start_sha256" ] || fail "installed start helper is not the expected revision"
[ "$(wc -l <"$mode_file" | tr -d ' ')" -eq 1 ] && [ "$(sed -n '1p' "$mode_file")" = success ] ||
	fail "Runner must be in success mode"
[ "$(docker container inspect "$container" --format '{{index .Config.Labels "vela.ai.component"}}' 2>/dev/null)" = h3-mock-runner ] ||
	fail "container $container is not managed by this deployment"
[ "$(docker container inspect "$container" --format '{{.State.Health.Status}}')" = healthy ] || fail "container is not healthy"
image=$(docker container inspect "$container" --format '{{.Config.Image}}')
[ "$image" = "$expected_runner_image" ] || fail "container image is not the pinned lab Runner revision"
[ "$(docker image inspect "$image" --format '{{.Id}}' 2>/dev/null)" != '' ] || fail "container image is not present locally"
process_count=$(docker top "$container" -eo pid,ppid,comm 2>/dev/null |
	sed '1d; /^[[:space:]]*$/d' | wc -l | tr -d ' ')
[ "$process_count" -eq 1 ] || fail "Runner has a child process; confirm zero active Attempt before upgrading"
active_gpu_processes=$(nvidia-smi --query-compute-apps=gpu_uuid --format=csv,noheader,nounits 2>/dev/null |
	sed '/^[[:space:]]*$/d' | wc -l | tr -d ' ')
[ "$active_gpu_processes" -eq 0 ] || fail "Worker has active GPU compute processes"
"$root/admin/validate-runner-restart-state.sh" >/dev/null

backup=$(mktemp "$root/.failure-receipt-wrapper.XXXXXX")
install -m 0555 -o 0 -g 0 "$wrapper" "$backup"
new_wrapper_sha256=$(sha256sum "$script_dir/mock-backend-wrapper.sh" | awk '{print $1}')
[ "$new_wrapper_sha256" != "$expected_wrapper_sha256" ] || fail "new wrapper does not change the predecessor"
install_file "$script_dir/mock-backend-wrapper.sh" "$wrapper" 0555
[ "$(sha256sum "$wrapper" | awk '{print $1}')" = "$new_wrapper_sha256" ] || fail "wrapper installation did not preserve its digest"

stopped=true
docker stop --time 20 "$container" >/dev/null
docker rm "$container" >/dev/null
"$start" "$image" mode-control >/dev/null
[ "$(docker container inspect "$container" --format '{{.State.Health.Status}}')" = healthy ] || fail "upgraded Runner is not healthy"
[ "$(docker container inspect "$container" --format '{{.Config.Image}}')" = "$image" ] || fail "upgraded Runner image changed"
[ "$(sed -n '1p' "$mode_file")" = success ] || fail "upgraded Runner did not retain success mode"
upgraded=true
rm -f -- "$backup"
backup=
trap - EXIT HUP INT TERM
printf 'schema=vela-lab-mock-runner-failure-receipt-upgrade-v1 result=PASS host=%s lan_ip=%s image=%s mode=success wrapper_sha256=%s production_gates=0/9\n' \
	"$(hostname)" "$worker_ip" "$image" "$new_wrapper_sha256"
