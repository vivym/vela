#!/bin/sh

set -eu

image=${1:-}
apply=${2:-}
root=${VELA_LAB_RUNNER_ROOT:-/var/lib/vela-lab/mock-runner}
container=${VELA_LAB_RUNNER_CONTAINER:-vela-h3-mock-runner}
old_preset_revision_id=84000000-0000-0000-0000-000000000005
replacement_preset_revision_id=84000000-0000-0000-0000-000000000201
temporary=
backup=
stopped=false
updated=false
complete=false

fail() {
	printf 'upgrade-mock-runner-catalog-profile: %s\n' "$*" >&2
	exit 1
}

rollback() {
	result=$?
	trap - EXIT HUP INT TERM
	if [ "$complete" != true ]; then
		if [ "$stopped" = true ]; then
			if docker container inspect "$container" >/dev/null 2>&1; then
				if [ "$(docker container inspect "$container" --format '{{index .Config.Labels "vela.ai.component"}}' 2>/dev/null)" = h3-mock-runner ]; then
					docker rm --force "$container" >/dev/null 2>&1 || true
				fi
			fi
			if [ "$updated" = true ] && [ -n "$backup" ] && [ -f "$backup" ]; then
				install -m 0444 -o 0 -g 0 "$backup" "$root/config/profiles.json"
			fi
			if "$root/admin/start-mock-runner-container.sh" "$image" mode-control >/dev/null 2>&1; then
				printf 'upgrade-mock-runner-catalog-profile: rollback restored the previous Runner configuration\n' >&2
			else
				printf 'upgrade-mock-runner-catalog-profile: rollback could not restore the previous Runner\n' >&2
			fi
		fi
	fi
	[ -z "$temporary" ] || rm -f -- "$temporary"
	[ -z "$backup" ] || rm -f -- "$backup"
	exit "$result"
}
trap rollback EXIT HUP INT TERM

[ "$apply" = --apply ] || fail "usage: $0 <registry/repository@sha256:digest> --apply"
[ "$(id -u)" -eq 0 ] || fail "run as root"
printf '%s\n' "$image" | grep -Eq '@sha256:[0-9a-f]{64}$' ||
	fail "image must use an immutable sha256 digest"
[ "$root" = /var/lib/vela-lab/mock-runner ] ||
	fail "VELA_LAB_RUNNER_ROOT must remain /var/lib/vela-lab/mock-runner"
for command_name in docker ip jq nvidia-smi sha256sum; do
	command -v "$command_name" >/dev/null 2>&1 || fail "$command_name is required"
done
[ "$(hostname)" = ubuntu ] || fail "catalog profile upgrade is restricted to the two lab Workers"
worker_ip=$(ip -j -4 address show dev eno1 2>/dev/null |
	jq -r '.[0].addr_info[] | select(.family == "inet" and .scope == "global") | .local' |
	head -n 1)
case "$worker_ip" in
	10.1.200.19 | 10.1.200.16) ;;
	*) fail "catalog profile upgrade is restricted to Worker LAN addresses 10.1.200.19 and 10.1.200.16" ;;
esac
[ "$(ip -j -4 route show default | jq -r '.[0].dev // ""')" = eno1 ] ||
	fail "Worker default route must use eno1"

profiles=$root/config/profiles.json
[ -f "$profiles" ] && [ ! -L "$profiles" ] || fail "installed profiles.json is missing or unsafe"
[ "$(stat -c '%u:%g:%a' "$profiles")" = 0:0:444 ] ||
	fail "installed profiles.json must be root:root mode 0444"
[ -x "$root/admin/start-mock-runner-container.sh" ] || fail "installed Runner start helper is missing"
[ -x "$root/admin/validate-runner-restart-state.sh" ] || fail "installed restart-state validator is missing"
[ "$(sed -n '1p' "$root/config/mock-mode" 2>/dev/null)" = success ] ||
	fail "mock Runner must be in success mode"
[ "$(docker container inspect "$container" --format '{{index .Config.Labels "vela.ai.component"}}' 2>/dev/null)" = h3-mock-runner ] ||
	fail "container $container is not managed by this deployment"
[ "$(docker container inspect "$container" --format '{{.State.Health.Status}}')" = healthy ] ||
	fail "container $container is not healthy"
[ "$(docker container inspect "$container" --format '{{.Config.Image}}')" = "$image" ] ||
	fail "container image does not match the requested digest"
[ "$(docker container inspect "$container" --format '{{range .Config.Env}}{{println .}}{{end}}' |
	sed -n 's/^VELA_RUNNER_BACKEND_COMMAND=//p')" = /etc/vela-runner/mock-backend-wrapper.sh ] ||
	fail "container is not the mode-controlled Runner layout"

common_filter='type == "object"
  and keys == ["backend_revision","profiles","schema_version"]
  and .schema_version == 1
  and (.backend_revision | test("^mock-h3-backend@sha256:[0-9a-f]{64}$"))
  and (.profiles | type == "array")
  and all(.profiles[];
    type == "object"
    and keys == ["execution_profile_revision_id","generation_preset_revision_id","model_revision_id","output_spec_id"]
    and .model_revision_id == "84000000-0000-0000-0000-000000000004"
    and .execution_profile_revision_id == "84000000-0000-0000-0000-000000000006"
    and .output_spec_id == "84000000-0000-0000-0000-000000000007")'
if jq -e "$common_filter and (.profiles | length == 1) and .profiles[0].generation_preset_revision_id == \"$old_preset_revision_id\"" "$profiles" >/dev/null; then
	profile_shape=legacy-only
elif jq -e "$common_filter and (.profiles | length == 2)
  and ([.profiles[].generation_preset_revision_id] | sort) == [\"$old_preset_revision_id\",\"$replacement_preset_revision_id\"]" "$profiles" >/dev/null; then
	profile_shape=replacement-ready
else
	fail "installed profiles.json is not the exact supported lab shape"
fi

process_count=$(docker top "$container" -eo pid,ppid,comm 2>/dev/null |
	sed '1d; /^[[:space:]]*$/d' | wc -l | tr -d ' ')
[ "$process_count" -eq 1 ] || fail "Runner has a child process; confirm zero active Attempt before upgrading"
active_gpu_processes=$(nvidia-smi --query-compute-apps=gpu_uuid --format=csv,noheader,nounits 2>/dev/null |
	sed '/^[[:space:]]*$/d' | wc -l | tr -d ' ')
[ "$active_gpu_processes" -eq 0 ] || fail "Worker has active GPU compute processes"
"$root/admin/validate-runner-restart-state.sh" >/dev/null

expected_revision=$(sha256sum \
	"$profiles" "$root/config/gpu-roles.json" "$root/config/mock-backend-wrapper.sh" |
	sha256sum | awk '{print $1}')
observed_revision=$(docker container inspect "$container" --format '{{index .Config.Labels "vela.ai.config-revision"}}')
if [ "$profile_shape" = replacement-ready ] && [ "$observed_revision" = "sha256:$expected_revision" ]; then
	complete=true
	trap - EXIT HUP INT TERM
	printf 'schema=vela-lab-mock-runner-catalog-profile-v1 result=ALREADY_CONFIGURED worker_ip=%s config_revision=sha256:%s production_gates=0/9\n' \
		"$worker_ip" "$expected_revision"
	exit 0
fi

temporary=$(mktemp "$root/config/.profiles.json.replacement.XXXXXX")
backup=$(mktemp "$root/config/.profiles.json.backup.XXXXXX")
install -m 0400 -o 0 -g 0 "$profiles" "$backup"
if [ "$profile_shape" = legacy-only ]; then
	jq --arg replacement "$replacement_preset_revision_id" \
		'.profiles += [(.profiles[0] | .generation_preset_revision_id = $replacement)]' \
		"$profiles" >"$temporary"
else
	install -m 0600 -o 0 -g 0 "$profiles" "$temporary"
fi
jq -e "$common_filter and (.profiles | length == 2)
  and ([.profiles[].generation_preset_revision_id] | sort) == [\"$old_preset_revision_id\",\"$replacement_preset_revision_id\"]" "$temporary" >/dev/null ||
	fail "replacement profiles.json failed validation"
chown 0:0 "$temporary"
chmod 0444 "$temporary"

docker stop --time 20 "$container" >/dev/null
stopped=true
docker rm "$container" >/dev/null
install -m 0444 -o 0 -g 0 "$temporary" "$profiles"
updated=true
rm -f -- "$temporary"
temporary=
"$root/admin/start-mock-runner-container.sh" "$image" mode-control >/dev/null

expected_revision=$(sha256sum \
	"$profiles" "$root/config/gpu-roles.json" "$root/config/mock-backend-wrapper.sh" |
	sha256sum | awk '{print $1}')
observed_revision=$(docker container inspect "$container" --format '{{index .Config.Labels "vela.ai.config-revision"}}')
[ "$observed_revision" = "sha256:$expected_revision" ] || fail "restarted Runner configuration identity is wrong"
[ "$(docker container inspect "$container" --format '{{.State.Health.Status}}')" = healthy ] ||
	fail "restarted Runner is not healthy"

complete=true
rm -f -- "$backup"
backup=
trap - EXIT HUP INT TERM
printf 'schema=vela-lab-mock-runner-catalog-profile-v1 result=PASS worker_ip=%s config_revision=sha256:%s production_gates=0/9\n' \
	"$worker_ip" "$expected_revision"
