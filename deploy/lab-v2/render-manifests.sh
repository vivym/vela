#!/bin/sh

set -eu

images=${1:-}
output=${2:-}
script_dir=$(CDPATH='' cd -- "$(dirname -- "$0")" && pwd)

fail() {
	printf 'render-vela-lab-manifests: %s\n' "$*" >&2
	exit 1
}

[ -f "$images" ] || fail "usage: $0 <images.env> <new-output-directory>"
case "$output" in
	/*) ;;
	*) fail "output directory must be absolute" ;;
esac
[ ! -e "$output" ] && [ ! -L "$output" ] || fail "output path already exists"

read_image() {
	key=$1
	count=$(grep -c "^${key}=" "$images" || true)
	[ "$count" -eq 1 ] || fail "$key must appear exactly once"
	value=$(sed -n "s/^${key}=//p" "$images")
	printf '%s\n' "$value" | grep -Eq '^10\.1\.200\.17:5443/[a-z0-9][a-z0-9._/-]*@sha256:[0-9a-f]{64}$' ||
		fail "$key must be an immutable private Registry digest"
	printf '%s' "$value"
}

postgres_image=$(read_image POSTGRES_IMAGE)
nats_image=$(read_image NATS_IMAGE)
minio_image=$(read_image MINIO_IMAGE)
control_image=$(read_image CONTROL_IMAGE)
fleet_controller_image=$(read_image FLEET_CONTROLLER_IMAGE)
stage_worker_agent_image=$(read_image STAGE_WORKER_AGENT_IMAGE)
runtime_image=$(read_image RUNTIME_IMAGE)
bootstrap_image=$(read_image BOOTSTRAP_IMAGE)
[ "${runtime_image##*@sha256:}" != "${bootstrap_image##*@sha256:}" ] ||
	fail "RUNTIME_IMAGE and BOOTSTRAP_IMAGE must have distinct digests"

temporary=$(mktemp -d "$(dirname "$output")/.vela-lab-render.XXXXXX")
committed=false
cleanup() {
	if [ "$committed" != true ]; then
		rm -rf "$temporary"
	fi
}
trap cleanup EXIT HUP INT TERM
chmod 0750 "$temporary"

install -m 0640 "$script_dir/00-namespace.yaml" "$temporary/00-namespace.yaml"
install -m 0640 "$script_dir/50-network-policies.yaml" "$temporary/50-network-policies.yaml"

render() {
	source_file=$1
	destination=$2
	sed \
		-e "s|__POSTGRES_IMAGE__|$postgres_image|g" \
		-e "s|__NATS_IMAGE__|$nats_image|g" \
		-e "s|__MINIO_IMAGE__|$minio_image|g" \
		-e "s|__CONTROL_IMAGE__|$control_image|g" \
		-e "s|__FLEET_CONTROLLER_IMAGE__|$fleet_controller_image|g" \
		-e "s|__STAGE_WORKER_AGENT_IMAGE__|$stage_worker_agent_image|g" \
		-e "s|__RUNTIME_IMAGE__|$runtime_image|g" \
		-e "s|__BOOTSTRAP_IMAGE__|$bootstrap_image|g" \
		"$source_file" >"$destination"
	chmod 0640 "$destination"
	if grep -Eq '__[A-Z0-9_]+__' "$destination"; then
		fail "unresolved placeholder in $source_file"
	fi
}

render "$script_dir/10-dependencies.yaml.tmpl" "$temporary/10-dependencies.yaml"
render "$script_dir/20-bootstrap.yaml.tmpl" "$temporary/20-bootstrap.yaml"
render "$script_dir/30-control.yaml.tmpl" "$temporary/30-control.yaml"
render "$script_dir/40-workers.yaml.tmpl" "$temporary/40-workers.yaml"
render "$script_dir/60-smoke.yaml.tmpl" "$temporary/60-smoke.yaml"
install -m 0640 "$images" "$temporary/images.env"

mv "$temporary" "$output"
committed=true
printf 'schema=vela-lab-render-v1 output=%s result=PASS\n' "$output"
