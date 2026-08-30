#!/bin/sh

set -eu

image=${1:-}
apply=${2:-}
root=${VELA_LAB_RUNNER_ROOT:-/var/lib/vela-lab/mock-runner}
container=${VELA_LAB_RUNNER_CONTAINER:-vela-h3-mock-runner}
script_dir=$(CDPATH='' cd -- "$(dirname -- "$0")" && pwd)
receipt=
committed=false
writer_existed=false
identity_existed=false
start_predecessor_sha256=d32907828601f84053882f5c56ca21efcff625052e0efdae66b8320f42f9725e
remove_predecessor_sha256=db054203b1987cab3dc5ec2697327f1e0fbece402bf57bf7406615894f82a2d3

fail() {
	printf 'upgrade-mock-runner-container-identity: %s\n' "$*" >&2
	exit 1
}

atomic_install() {
	source=$1
	target=$2
	mode=$3
	temporary=$(mktemp "$root/admin/.container-identity-install.XXXXXX")
	install -m "$mode" -o 0 -g 0 "$source" "$temporary"
	[ "$(sha256sum "$temporary" | awk '{print $1}')" = "$(sha256sum "$source" | awk '{print $1}')" ] || return 1
	mv -f -- "$temporary" "$target"
}

rollback() {
	result=$?
	trap - EXIT HUP INT TERM
	if [ "$committed" != true ] && [ -n "$receipt" ] && [ -d "$receipt" ]; then
		atomic_install "$receipt/start-mock-runner-container.sh.before" "$root/admin/start-mock-runner-container.sh" 0550 || true
		atomic_install "$receipt/remove-mock-runner.sh.before" "$root/admin/remove-mock-runner.sh" 0550 || true
		if [ "$writer_existed" = true ]; then
			atomic_install "$receipt/write-mock-runner-container-identity.sh.before" "$root/admin/write-mock-runner-container-identity.sh" 0550 || true
		else
			rm -f -- "$root/admin/write-mock-runner-container-identity.sh"
		fi
		if [ "$identity_existed" = true ]; then
			temporary=$(mktemp "$root/config/.container-identity-rollback.XXXXXX")
			install -m 0444 -o 0 -g 0 "$receipt/container-identity.before" "$temporary" || true
			mv -f -- "$temporary" "$root/config/container-identity" || true
		else
			rm -f -- "$root/config/container-identity"
		fi
		printf 'status=ROLLED_BACK production_gates=0/9\n' >"$receipt/STATUS"
	fi
	exit "$result"
}
trap rollback EXIT HUP INT TERM

[ "$apply" = --apply ] || fail "usage: $0 <registry/repository@sha256:digest> --apply"
[ "$(id -u)" -eq 0 ] || fail "run as root"
[ "$root" = /var/lib/vela-lab/mock-runner ] ||
	fail "VELA_LAB_RUNNER_ROOT must remain /var/lib/vela-lab/mock-runner"
printf '%s\n' "$image" | grep -Eq '^10\.1\.200\.17:5443/vela-lab/vela-h3-runner@sha256:[0-9a-f]{64}$' ||
	fail "image must use the fixed private Runner repository and an immutable digest"
for source in remove-mock-runner.sh start-mock-runner-container.sh write-mock-runner-container-identity.sh; do
	[ -f "$script_dir/$source" ] && [ ! -L "$script_dir/$source" ] || fail "$source is missing or unsafe"
done
[ -d "$root/admin" ] && [ ! -L "$root/admin" ] || fail "Runner admin directory is missing or unsafe"
[ -d "$root/config" ] && [ ! -L "$root/config" ] || fail "Runner config directory is missing or unsafe"
[ "$(docker container inspect "$container" --format '{{index .Config.Labels "vela.ai.component"}}' 2>/dev/null)" = h3-mock-runner ] ||
	fail "container $container is not managed by this deployment"
[ "$(docker container inspect "$container" --format '{{.State.Health.Status}}')" = healthy ] ||
	fail "container $container is not healthy"
[ "$(docker container inspect "$container" --format '{{.Config.Image}}')" = "$image" ] ||
	fail "container image does not match the requested digest"
[ "$(docker container inspect "$container" --format '{{range .Config.Env}}{{println .}}{{end}}' |
	sed -n 's/^VELA_RUNNER_BACKEND_COMMAND=//p')" = /etc/vela-runner/mock-backend-wrapper.sh ] ||
	fail "container is not the expected mode-controlled Runner"

start_source_sha256=$(sha256sum "$script_dir/start-mock-runner-container.sh" | awk '{print $1}')
remove_source_sha256=$(sha256sum "$script_dir/remove-mock-runner.sh" | awk '{print $1}')
writer_source_sha256=$(sha256sum "$script_dir/write-mock-runner-container-identity.sh" | awk '{print $1}')
for target in start-mock-runner-container.sh remove-mock-runner.sh; do
	[ -f "$root/admin/$target" ] && [ ! -L "$root/admin/$target" ] || fail "installed $target is missing or unsafe"
	[ "$(stat -c %u:%g:%a "$root/admin/$target")" = 0:0:550 ] || fail "installed $target permissions are unsafe"
done
start_installed_sha256=$(sha256sum "$root/admin/start-mock-runner-container.sh" | awk '{print $1}')
remove_installed_sha256=$(sha256sum "$root/admin/remove-mock-runner.sh" | awk '{print $1}')
case "$start_installed_sha256" in "$start_predecessor_sha256" | "$start_source_sha256") ;; *) fail "installed start helper has an unapproved revision" ;; esac
case "$remove_installed_sha256" in "$remove_predecessor_sha256" | "$remove_source_sha256") ;; *) fail "installed remove helper has an unapproved revision" ;; esac
if [ -e "$root/admin/write-mock-runner-container-identity.sh" ] || [ -L "$root/admin/write-mock-runner-container-identity.sh" ]; then
	[ -f "$root/admin/write-mock-runner-container-identity.sh" ] && [ ! -L "$root/admin/write-mock-runner-container-identity.sh" ] ||
		fail "installed identity writer is unsafe"
	[ "$(stat -c %u:%g:%a "$root/admin/write-mock-runner-container-identity.sh")" = 0:0:550 ] ||
		fail "installed identity writer permissions are unsafe"
	[ "$(sha256sum "$root/admin/write-mock-runner-container-identity.sh" | awk '{print $1}')" = "$writer_source_sha256" ] ||
		fail "installed identity writer has an unapproved revision"
	writer_existed=true
fi
if [ -e "$root/config/container-identity" ] || [ -L "$root/config/container-identity" ]; then
	[ -f "$root/config/container-identity" ] && [ ! -L "$root/config/container-identity" ] || fail "installed identity file is unsafe"
	[ "$(stat -c %u:%g:%a "$root/config/container-identity")" = 0:0:444 ] || fail "installed identity file permissions are unsafe"
	identity_existed=true
fi

receipt=$(mktemp -d "$root/admin/container-identity-upgrade.XXXXXX")
chmod 0700 "$receipt"
cp -p -- "$root/admin/start-mock-runner-container.sh" "$receipt/start-mock-runner-container.sh.before"
cp -p -- "$root/admin/remove-mock-runner.sh" "$receipt/remove-mock-runner.sh.before"
[ "$writer_existed" = false ] || cp -p -- "$root/admin/write-mock-runner-container-identity.sh" "$receipt/write-mock-runner-container-identity.sh.before"
[ "$identity_existed" = false ] || cp -p -- "$root/config/container-identity" "$receipt/container-identity.before"
printf 'start_before=%s\nremove_before=%s\nstart_after=%s\nremove_after=%s\nwriter_after=%s\n' \
	"$start_installed_sha256" "$remove_installed_sha256" "$start_source_sha256" "$remove_source_sha256" "$writer_source_sha256" \
	>"$receipt/revisions.txt"

atomic_install "$script_dir/remove-mock-runner.sh" "$root/admin/remove-mock-runner.sh" 0550
atomic_install "$script_dir/start-mock-runner-container.sh" "$root/admin/start-mock-runner-container.sh" 0550
atomic_install "$script_dir/write-mock-runner-container-identity.sh" "$root/admin/write-mock-runner-container-identity.sh" 0550
identity_receipt=$("$root/admin/write-mock-runner-container-identity.sh")

[ "$(sha256sum "$root/admin/start-mock-runner-container.sh" | awk '{print $1}')" = "$start_source_sha256" ] || fail "installed start helper does not match staged source"
[ "$(sha256sum "$root/admin/remove-mock-runner.sh" | awk '{print $1}')" = "$remove_source_sha256" ] || fail "installed remove helper does not match staged source"
[ "$(sha256sum "$root/admin/write-mock-runner-container-identity.sh" | awk '{print $1}')" = "$writer_source_sha256" ] ||
	fail "installed identity writer does not match staged source"
container_id=$(docker container inspect "$container" --format '{{.Id}}')
[ "$(stat -c %u:%g:%a "$root/config/container-identity")" = 0:0:444 ] || fail "identity file permissions are unsafe"
[ "$(sed -n 1p "$root/config/container-identity")" = schema=vela-lab-runner-container-identity-v1 ] ||
	fail "identity file schema is invalid"
[ "$(sed -n 's/^container_id=//p' "$root/config/container-identity")" = "$container_id" ] ||
	fail "identity file does not match the running container"
[ "$(sed -n 's/^image=//p' "$root/config/container-identity")" = "$image" ] ||
	fail "identity file does not match the running image"
printf '%s\n' "$identity_receipt" >"$receipt/identity-writer-receipt.txt"
printf 'status=PASS production_gates=0/9\n' >"$receipt/STATUS"
(
	cd "$receipt"
	# SHA256SUMS is explicitly excluded from find.
	# shellcheck disable=SC2094
	find . -maxdepth 1 -type f ! -name SHA256SUMS -print0 |
		LC_ALL=C sort -z | xargs -0 sha256sum >SHA256SUMS
	sha256sum --check --strict SHA256SUMS >/dev/null
)

committed=true
trap - EXIT HUP INT TERM
printf 'schema=vela-lab-runner-container-identity-upgrade-v1 result=PASS container_id=%s receipt=%s production_gates=0/9\n' \
	"$container_id" "$receipt"
