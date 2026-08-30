#!/bin/sh

set -eu

role=${1:-}
apply=${2:-}

fail() {
	printf 'prepare-vela-lab-host: %s\n' "$*" >&2
	exit 1
}

case "$role:$apply" in
	control:--apply | worker-1:--apply | worker-2:--apply) ;;
	*) fail "usage: $0 <control|worker-1|worker-2> --apply" ;;
esac
[ "$(id -u)" -eq 0 ] || fail "run as root"

case "$role" in
	control)
		expected_hostname=marslab-server
		expected_ip=10.1.200.17
		expected_service=rke2-server
		;;
	worker-1)
		expected_hostname=ubuntu
		expected_ip=10.1.200.19
		expected_service=rke2-agent
		;;
	worker-2)
		expected_hostname=ubuntu
		expected_ip=10.1.200.16
		expected_service=rke2-agent
		;;
esac

[ "$(hostname)" = "$expected_hostname" ] || fail "hostname does not match role $role"
ip -o -4 addr show scope global | awk '{print $4}' | cut -d/ -f1 | grep -Fx "$expected_ip" >/dev/null ||
	fail "expected address $expected_ip is absent"
systemctl is-active --quiet "$expected_service" || fail "$expected_service is not active"

ensure_directory() {
	path=$1
	owner=$2
	group=$3
	mode=$4
	[ ! -L "$path" ] || fail "$path must not be a symlink"
	if [ -e "$path" ]; then
		[ -d "$path" ] || fail "$path exists and is not a directory"
	else
		install -d -o "$owner" -g "$group" -m "$mode" "$path"
	fi
	chown "$owner:$group" "$path"
	chmod "$mode" "$path"
}

if [ "$role" = control ]; then
	[ "$(docker container inspect vela-registry --format '{{range .Mounts}}{{if eq .Destination "/var/lib/registry"}}{{.Source}}{{end}}{{end}}' 2>/dev/null)" = /srv/vela-registry ] ||
		fail "private Registry data mount does not match /srv/vela-registry"
	[ "$(docker container inspect vela-registry --format '{{.State.Status}}')" = running ] ||
		fail "managed private Registry is not running"
	ensure_directory /var/lib/vela-lab 0 0 0755
	ensure_directory /var/lib/vela-lab/control-plane 0 0 0750
	ensure_directory /var/lib/vela-lab/control-plane/postgres 999 999 0700
	ensure_directory /var/lib/vela-lab/control-plane/nats 0 0 0750
	ensure_directory /var/lib/vela-lab/control-plane/minio 1000 1000 0700
	ensure_directory /var/lib/vela-lab/control-plane/artifact-validation 10001 10001 0700
	ensure_directory /var/lib/vela-lab/control-plane/artifact-validation/sandboxes 10001 10001 0700
	ensure_directory /var/lib/vela-lab/control-plane/artifact-validation/spool 10001 10001 0700
else
	runner_root=/var/lib/vela-lab/mock-runner
	[ ! -L "$runner_root" ] || fail "$runner_root must not be a symlink"
	[ -S "$runner_root/run/runner.sock" ] || fail "mock Runner socket is absent"
	[ "$(stat -c %u "$runner_root/run/runner.sock")" -eq 10001 ] || fail "mock Runner socket owner is not 10001"
	[ "$(stat -c %u "$runner_root/scratch")" -eq 10001 ] || fail "mock Runner scratch owner is not 10001"
	[ "$(docker container inspect vela-h3-mock-runner --format '{{.State.Health.Status}}' 2>/dev/null)" = healthy ] ||
		fail "persistent mock Runner is not healthy"
fi

printf 'schema=vela-lab-host-preparation-v1 role=%s host=%s address=%s result=PASS\n' \
	"$role" "$(hostname)" "$expected_ip"
