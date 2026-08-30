#!/bin/sh

set -eu
umask 077

role=${1:-}
receipt_dir=${2:-}
apply=${3:-}

fail() {
	printf 'capture-rke2-node-state: %s\n' "$*" >&2
	exit 1
}

capture() {
	name=$1
	shift
	"$@" >"$receipt_dir/$name" 2>&1 ||
		fail "capture failed for $name; partial receipt preserved at $receipt_dir"
}

capture_optional() {
	name=$1
	shift
	if command -v "$1" >/dev/null 2>&1; then
		"$@" >"$receipt_dir/$name" 2>&1 || true
	else
		printf 'command-unavailable=%s\n' "$1" >"$receipt_dir/$name"
	fi
}

case "$role" in
	server)
		expected_hostname=marslab-server
		expected_interface=enp34s0f0
		expected_ip=10.1.200.17
		;;
	worker-1)
		expected_hostname=ubuntu
		expected_interface=eno1
		expected_ip=10.1.200.19
		;;
	worker-2)
		expected_hostname=ubuntu
		expected_interface=eno1
		expected_ip=10.1.200.16
		;;
	*) fail "usage: $0 <server|worker-1|worker-2> <new-receipt-directory> --apply" ;;
esac
[ "$apply" = --apply ] || fail "usage: $0 <server|worker-1|worker-2> <new-receipt-directory> --apply"
[ "$(id -u)" -eq 0 ] || fail "run as root"
case "$receipt_dir" in
	/*) ;;
	*) fail "receipt directory must be an absolute path" ;;
esac
[ "$receipt_dir" != / ] || fail "receipt directory must not be /"
[ ! -e "$receipt_dir" ] && [ ! -L "$receipt_dir" ] || fail "receipt directory already exists"
receipt_parent=$(dirname -- "$receipt_dir")
[ -d "$receipt_parent" ] && [ ! -L "$receipt_parent" ] || fail "receipt parent must be an existing directory"
[ "$(stat -c %u "$receipt_parent")" -eq 0 ] || fail "receipt parent must be root-owned"
parent_mode=$(stat -c %a "$receipt_parent")
case "$parent_mode" in
	??? ) ;;
	*) fail "receipt parent has an unexpected mode" ;;
esac
group_digit=$(printf '%s' "$parent_mode" | cut -c2)
other_digit=$(printf '%s' "$parent_mode" | cut -c3)
case "$group_digit$other_digit" in
	*[2367]*) fail "receipt parent must not be group/world writable" ;;
esac

for command_name in df docker find findmnt ip ip6tables-save iptables-save jq nft nvidia-smi sha256sum ss stat swapon sysctl systemctl; do
	command -v "$command_name" >/dev/null 2>&1 || fail "required command is missing: $command_name"
done
[ "$(hostname)" = "$expected_hostname" ] || fail "hostname does not match role $role"
observed_ip=$(ip -j -4 address show dev "$expected_interface" |
	jq -r '.[0].addr_info[] | select(.family == "inet" and .scope == "global") | .local' |
	head -n 1)
[ "$observed_ip" = "$expected_ip" ] || fail "LAN identity does not match role $role"

mkdir "$receipt_dir"
chown 0:0 "$receipt_dir"
chmod 0700 "$receipt_dir"

{
	printf 'schema=vela-rke2-node-state-v1\n'
	printf 'captured_at=%s\n' "$(date -u +%FT%TZ)"
	printf 'role=%s\n' "$role"
	printf 'hostname=%s\n' "$(hostname)"
	printf 'lan_interface=%s\n' "$expected_interface"
	printf 'lan_ip=%s\n' "$observed_ip"
} >"$receipt_dir/metadata.txt"

capture ip-link.json ip -details -json link show
capture ip-address.json ip -details -json address show
capture ip-route-v4.json ip -details -json -4 route show table all
capture ip-route-v6.json ip -details -json -6 route show table all
capture ip-rule-v4.json ip -details -json -4 rule show
capture ip-rule-v6.json ip -details -json -6 rule show
capture listeners.txt ss -H -lntup
capture sysctl-ipv4.txt sysctl net.ipv4
capture sysctl-ipv6.txt sysctl net.ipv6
capture_optional sysctl-bridge.txt sysctl net.bridge
capture iptables-save.txt iptables-save -c
capture ip6tables-save.txt ip6tables-save -c
capture nftables.txt nft list ruleset
capture findmnt.json findmnt --json --real
capture df.txt df -B1 -T
capture swapon.txt swapon --show --bytes
capture proc-swaps.txt sh -c 'cat /proc/swaps'
capture systemd-docker.txt systemctl show docker --no-pager \
	--property=Id,LoadState,ActiveState,SubState,UnitFileState,MainPID,ExecMainPID,ExecMainStartTimestamp,NRestarts,FragmentPath,DropInPaths

docker ps --all --no-trunc \
	--format '{{.ID}}\t{{.Names}}\t{{.Image}}\t{{.Status}}\t{{.Ports}}\t{{.Networks}}' \
	>"$receipt_dir/docker-containers.tsv"
for container in vela-registry vela-h3-mock-runner fchip-4591d89ff18127a74b8a25a0; do
	if docker container inspect "$container" >/dev/null 2>&1; then
		docker container inspect "$container" |
			jq '.[0] | {
			  Id, Name, Created, Image, RestartCount,
			  State: {Status: .State.Status, Running: .State.Running, Restarting: .State.Restarting,
			          Pid: .State.Pid, ExitCode: .State.ExitCode,
			          StartedAt: .State.StartedAt, FinishedAt: .State.FinishedAt,
			          Health: (.State.Health.Status // null)},
			  Config: {Image: .Config.Image, User: .Config.User},
			  HostConfig: {RestartPolicy: .HostConfig.RestartPolicy, NetworkMode: .HostConfig.NetworkMode,
			               ReadonlyRootfs: .HostConfig.ReadonlyRootfs, Privileged: .HostConfig.Privileged,
			               CapAdd: .HostConfig.CapAdd, CapDrop: .HostConfig.CapDrop},
			  Mounts: [.Mounts[]? | {Type, Source, Destination, Mode, RW, Propagation}],
			  Networks: (.NetworkSettings.Networks | with_entries(.value |= {NetworkID, EndpointID,
			             Gateway, IPAddress, IPPrefixLen, IPv6Gateway, GlobalIPv6Address,
			             GlobalIPv6PrefixLen, MacAddress}))
			}' >"$receipt_dir/docker-$container.json"
	fi
done
docker info --format '{{json .}}' |
	jq '{ID, Name, ServerVersion, Driver, DockerRootDir, CgroupDriver, CgroupVersion,
	     DefaultRuntime, Runtimes, KernelVersion, OperatingSystem, OSType, Architecture,
	     NCPU, MemTotal, DockerRootDir, RegistryConfig}' >"$receipt_dir/docker-info-selected.json"
if [ -f /etc/docker/daemon.json ]; then
	stat -c 'path=%n owner=%U:%G mode=%a bytes=%s' /etc/docker/daemon.json >"$receipt_dir/docker-daemon-stat.txt"
	sha256sum /etc/docker/daemon.json >"$receipt_dir/docker-daemon.sha256"
else
	printf 'path=/etc/docker/daemon.json status=absent\n' >"$receipt_dir/docker-daemon-stat.txt"
fi

capture nvidia-smi-query.txt nvidia-smi \
	--query-gpu=index,uuid,name,pci.bus_id,driver_version,memory.total,memory.used,temperature.gpu,power.draw \
	--format=csv,noheader,nounits
capture nvidia-compute-processes.txt nvidia-smi \
	--query-compute-apps=gpu_uuid,pid,process_name,used_memory --format=csv,noheader,nounits
capture nvidia-smi-q.txt nvidia-smi -q

for path in /etc/cni /opt/cni/bin /var/lib/cni /etc/rancher /var/lib/rancher /var/lib/kubelet /run/k3s; do
	label=$(printf '%s' "$path" | sed 's#^/##; s#/#-#g')
	if [ -e "$path" ] || [ -L "$path" ]; then
		find "$path" -xdev -printf '%y\t%u:%g\t%m\t%s\t%p\t%l\n' | sort >"$receipt_dir/tree-$label.tsv"
	else
		printf 'path=%s status=absent\n' "$path" >"$receipt_dir/tree-$label.tsv"
	fi
done

capture_optional zpool-list.txt zpool list -Hp -o name,size,alloc,free,health
capture_optional zpool-status.txt zpool status -P
capture_optional zfs-list.txt zfs list -Hp -o name,used,available,referenced,mountpoint

(
	cd "$receipt_dir"
	# The output paths are explicitly excluded from the inventory being hashed.
	# shellcheck disable=SC2094
	find . -type f ! -name MANIFEST.sha256 ! -name .manifest-files -print0 >.manifest-files
	sort -z .manifest-files | xargs -0 sha256sum >MANIFEST.sha256
	rm .manifest-files
)
chmod 0600 "$receipt_dir"/*
printf 'result=PASS role=%s receipt=%s manifest=%s\n' \
	"$role" "$receipt_dir" "$receipt_dir/MANIFEST.sha256"
