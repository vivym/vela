#!/bin/sh

set -eu

role=${1:-}
if [ "$#" -gt 0 ]; then
	shift
fi
address_policy=unapproved
swap_exception=unapproved
failures=0

fail() {
	printf 'rke2-preflight: %s\n' "$*" >&2
	exit 1
}

pass_check() {
	printf 'check=%s status=PASS detail=%s\n' "$1" "$2"
}

fail_check() {
	printf 'check=%s status=FAIL detail=%s\n' "$1" "$2"
	failures=$((failures + 1))
}

check_equal() {
	if [ "$2" = "$3" ]; then
		pass_check "$1" "$2"
	else
		fail_check "$1" "observed=$2 expected=$3"
	fi
}

check_absent() {
	if [ -e "$2" ] || [ -L "$2" ]; then
		fail_check "$1" "$2-present"
	else
		pass_check "$1" "$2-absent"
	fi
}

check_module_available() {
	module=$1
	if lsmod | awk -v module="$module" '$1 == module { found = 1 } END { exit !found }'; then
		pass_check "kernel-module-$module" loaded
	elif modinfo "$module" >/dev/null 2>&1; then
		pass_check "kernel-module-$module" available-not-loaded
	else
		fail_check "kernel-module-$module" unavailable
	fi
}

check_cidr_free() {
	cidr=$1
	overlaps=$(ip -o -4 route show table all | awk -v target="$cidr" '
		function ip_number(ip, octets) {
			split(ip, octets, ".")
			return (((octets[1] * 256 + octets[2]) * 256 + octets[3]) * 256 + octets[4])
		}
		function cidr_start(cidr, parts, prefix, size, address) {
			split(cidr, parts, "/")
			prefix = (parts[2] == "" ? 32 : parts[2])
			size = 2 ^ (32 - prefix)
			address = ip_number(parts[1])
			return int(address / size) * size
		}
		function cidr_end(cidr, parts, prefix, size) {
			split(cidr, parts, "/")
			prefix = (parts[2] == "" ? 32 : parts[2])
			size = 2 ^ (32 - prefix)
			return cidr_start(cidr) + size - 1
		}
		BEGIN {
			target_start = cidr_start(target)
			target_end = cidr_end(target)
		}
		{
			destination = $1
			if (destination ~ /^(local|broadcast|unreachable|prohibit|blackhole|throw|nat|multicast)$/) {
				destination = $2
			}
			if (destination == "default" || destination !~ /^[0-9][0-9.]*\/?[0-9]*$/) {
				next
			}
			route_start = cidr_start(destination)
			route_end = cidr_end(destination)
			if (route_start <= target_end && target_start <= route_end) {
				print destination
			}
		}
	' | sort -u)
	if [ -z "$overlaps" ]; then
		pass_check "route-overlap-$cidr" none
	else
		fail_check "route-overlap-$cidr" "$(printf '%s\n' "$overlaps" | tr '\n' ',' | sed 's/,$//')"
	fi
}

check_tcp_free() {
	if [ -z "$(ss -H -lnt "sport = :$2")" ]; then
		pass_check "$1" "tcp-$2-free"
	else
		fail_check "$1" "tcp-$2-in-use"
	fi
}

check_udp_free() {
	if [ -z "$(ss -H -lnu "sport = :$2")" ]; then
		pass_check "$1" "udp-$2-free"
	else
		fail_check "$1" "udp-$2-in-use"
	fi
}

hash_command() {
	key=$1
	shift
	value=$("$@" | sha256sum | awk '{print $1}')
	printf 'snapshot=%s sha256=%s\n' "$key" "$value"
}

case "$role" in
	server)
		expected_hostname=marslab-server
		expected_interface=enp34s0f0
		expected_ip=10.1.200.17
		rke2_service=rke2-server
		managed_container=vela-registry
		;;
	worker-1)
		expected_hostname=ubuntu
		expected_interface=eno1
		expected_ip=10.1.200.19
		rke2_service=rke2-agent
		managed_container=vela-h3-mock-runner
		;;
	worker-2)
		expected_hostname=ubuntu
		expected_interface=eno1
		expected_ip=10.1.200.16
		rke2_service=rke2-agent
		managed_container=vela-h3-mock-runner
		;;
	*) fail "usage: $0 <server|worker-1|worker-2> (--dhcp-reservation-proven|--dynamic-ip-risk-approved) [--swap-exception-approved]" ;;
esac

while [ "$#" -gt 0 ]; do
	case "$1" in
		--dhcp-reservation-proven)
			[ "$address_policy" = unapproved ] ||
				fail "address confirmations are mutually exclusive"
			address_policy=dhcp-reservation-proven
			;;
		--dynamic-ip-risk-approved)
			[ "$address_policy" = unapproved ] ||
				fail "address confirmations are mutually exclusive"
			address_policy=dynamic-ip-risk-approved
			;;
		--swap-exception-approved) swap_exception=approved ;;
		*) fail "unknown argument: $1" ;;
	esac
	shift
done

[ "$(id -u)" -eq 0 ] || fail "run as root for authoritative firewall and ownership evidence"
case "$address_policy" in
	dhcp-reservation-proven) pass_check address-authority dhcp-reservation-proven ;;
	dynamic-ip-risk-approved) pass_check address-authority dynamic-ip-risk-approved ;;
	unapproved) fail_check address-authority address-confirmation-required ;;
esac

for command_name in apparmor_parser awk curl df docker find findmnt ip ip6tables-save iptables-save jq lsmod modinfo nft nvidia-container-cli nvidia-container-runtime nvidia-ctk nvidia-smi sha256sum ss stat swapon sysctl systemctl timedatectl ufw; do
	command -v "$command_name" >/dev/null 2>&1 || fail "required command is missing: $command_name"
done
if [ "$role" != server ]; then
	command -v zpool >/dev/null 2>&1 || fail "required command is missing: zpool"
fi

printf 'schema=vela-rke2-node-preflight-v1\n'
printf 'captured_at=%s\n' "$(date -u +%FT%TZ)"
printf 'role=%s\n' "$role"
check_equal hostname "$(hostname)" "$expected_hostname"

address_json=$(ip -j -4 address show dev "$expected_interface")
observed_ip=$(printf '%s' "$address_json" |
	jq -r '.[0].addr_info[] | select(.family == "inet" and .scope == "global") | .local' |
	head -n 1)
check_equal lan-address "$observed_ip" "$expected_ip"
if ip -o -4 address show dev "$expected_interface" | grep -qw dynamic; then
	pass_check lan-address-source dhcp
else
	fail_check lan-address-source expected-dhcp-address
fi
check_equal default-route-interface "$(ip -j -4 route show default | jq -r '.[0].dev // ""')" "$expected_interface"
check_equal default-route-gateway "$(ip -j -4 route show default | jq -r '.[0].gateway // ""')" 10.1.200.1
check_equal ipv4-forwarding "$(sysctl -n net.ipv4.ip_forward)" 1
check_equal cgroup-filesystem "$(stat -fc %T /sys/fs/cgroup)" cgroup2fs
check_equal ntp-synchronized "$(timedatectl show -p NTPSynchronized --value)" yes
check_equal apparmor-enabled "$(cat /sys/module/apparmor/parameters/enabled 2>/dev/null || true)" Y
for module in overlay br_netfilter nf_conntrack vxlan; do
	check_module_available "$module"
done
check_cidr_free 10.42.0.0/16
check_cidr_free 10.43.0.0/16

network_manager_status=$(systemctl is-active NetworkManager 2>/dev/null || true)
check_equal network-manager "${network_manager_status:-inactive}" inactive
ufw_status=$(ufw status | sed -n '1s/^Status: //p')
check_equal ufw "$ufw_status" inactive

for path in /etc/rancher /var/lib/rancher /var/lib/kubelet /var/lib/cni /opt/cni/bin /run/k3s; do
	check_absent "path-${path}" "$path"
done
if [ -d /etc/cni ] && [ -d /etc/cni/net.d ]; then
	check_equal cni-root-mode "$(stat -c '%U:%G:%a' /etc/cni)" root:root:755
	check_equal cni-netd-mode "$(stat -c '%U:%G:%a' /etc/cni/net.d)" root:root:700
	check_equal cni-existing-files "$(find /etc/cni -xdev -type f | wc -l | tr -d '[:space:]')" 0
else
	fail_check cni-baseline /etc/cni/net.d-missing
fi

if command -v rke2 >/dev/null 2>&1; then
	fail_check rke2-binary present
else
	pass_check rke2-binary absent
fi
rke2_status=$(systemctl is-active "$rke2_service" 2>/dev/null || true)
check_equal rke2-service "${rke2_status:-inactive}" inactive
check_absent rke2-systemd-unit "/usr/local/lib/systemd/system/$rke2_service.service"

check_tcp_free rke2-kubelet 10250
check_tcp_free canal-health 9099
check_udp_free canal-vxlan 8472
if [ "$role" = server ]; then
	for port in 2379 2380 2381 6443 9345; do
		check_tcp_free "rke2-server-$port" "$port"
	done
fi

check_equal root-filesystem "$(findmnt -n -o FSTYPE /)" ext4
root_free_bytes=$(df -B1 --output=avail / | tail -n 1 | tr -d '[:space:]')
if [ "$root_free_bytes" -ge 107374182400 ]; then
	pass_check root-free-bytes "$root_free_bytes"
else
	fail_check root-free-bytes "$root_free_bytes-below-100GiB"
fi

swap_rows=$(swapon --show --bytes --noheadings --output NAME,TYPE,SIZE,USED,PRIO)
if [ -z "$swap_rows" ]; then
	pass_check swap-policy no-active-swap
else
	swap_devices=$(printf '%s\n' "$swap_rows" | wc -l | tr -d '[:space:]')
	swap_total_bytes=$(printf '%s\n' "$swap_rows" | awk '{ total += $3 } END { printf "%.0f", total }')
	swap_used_bytes=$(printf '%s\n' "$swap_rows" | awk '{ total += $4 } END { printf "%.0f", total }')
	printf 'observation=active-swap devices=%s total_bytes=%s used_bytes=%s requested_pod_policy=NoSwap\n' \
		"$swap_devices" "$swap_total_bytes" "$swap_used_bytes"
	if [ "$swap_exception" = approved ]; then
		pass_check swap-policy lab-exception-approved
	else
		fail_check swap-policy lab-exception-approval-required
	fi
fi

gpu_count=$(nvidia-smi --query-gpu=uuid --format=csv,noheader,nounits |
	sed '/^[[:space:]]*$/d' | sort -u | wc -l | tr -d '[:space:]')
check_equal physical-gpus "$gpu_count" 8
check_equal nvidia-runtime-version \
	"$(nvidia-container-runtime --version | sed -n '1s/.*version //p')" 1.19.1
check_equal nvidia-toolkit-version \
	"$(nvidia-ctk --version | sed -n '1s/.*version //p')" 1.19.1
check_equal nvidia-runtime-config-sha256 \
	"$(sha256sum /etc/nvidia-container-runtime/config.toml | awk '{print $1}')" \
	65ff485fab3e17754169b066b0a910221b3de4bc8de4089c2c345357316a7982
docker_runtimes=$(docker info --format '{{json .Runtimes}}')
check_equal docker-nvidia-runtime "$(printf '%s' "$docker_runtimes" | jq -r 'has("nvidia")')" true
check_equal docker-default-runtime "$(docker info --format '{{.DefaultRuntime}}')" runc
gpu_processes=$(nvidia-smi --query-compute-apps=pid --format=csv,noheader,nounits 2>/dev/null |
	sed '/^[[:space:]]*$/d' | wc -l | tr -d '[:space:]')
if [ "$role" = server ]; then
	printf 'observation=control-gpu-compute-processes count=%s policy=not-used-by-vela\n' "$gpu_processes"
else
	check_equal worker-gpu-compute-processes "$gpu_processes" 0
	check_equal worker-zfs-data "$(zpool list -H -o name,health | awk '$1 == "data" {print $1 ":" $2}')" data:ONLINE
fi

container_state=$(docker inspect --format '{{.State.Status}}' "$managed_container" 2>/dev/null || true)
check_equal managed-container-state "$container_state" running
if [ "$role" = server ]; then
	check_equal registry-container-image \
		"$(docker inspect --format '{{.Image}}' vela-registry)" \
		sha256:a3d8aaa63ed8681a604f1dea0aa03f100d5895b6a58ace528858a7b332415373
	check_equal shared-container-state \
		"$(docker inspect --format '{{.State.Status}}' fchip-4591d89ff18127a74b8a25a0)" running
	check_equal shared-container-id \
		"$(docker inspect --format '{{.Id}}' fchip-4591d89ff18127a74b8a25a0)" \
		b0a653da3926e90d88a6d3329fab8a927456e23ddfd6acb7d7d40cf6f9db0c94
	registry_ca=/etc/vela-registry/tls/ca.crt
else
	check_equal runner-health \
		"$(docker inspect --format '{{.State.Health.Status}}' vela-h3-mock-runner)" healthy
	check_equal runner-image \
		"$(docker inspect --format '{{.Config.Image}}' vela-h3-mock-runner)" \
		10.1.200.17:5443/vela-lab/vela-h3-runner@sha256:71af1330eefdfff2a33d68e5f8c53c66ebe5b402dc28c35b3ff7516357ec4ca3
	registry_ca=/etc/docker/certs.d/10.1.200.17:5443/ca.crt
fi
check_equal registry-unauthenticated-status \
	"$(curl --silent --show-error --cacert "$registry_ca" --output /dev/null --write-out '%{http_code}' https://10.1.200.17:5443/v2/)" 401

hash_command ipv4-addresses ip -j -4 address show
hash_command ipv4-routes ip -j -4 route show table all
hash_command iptables iptables-save
hash_command ip6tables ip6tables-save
hash_command nftables nft list ruleset

if [ "$failures" -eq 0 ]; then
	printf 'result=PASS failures=0\n'
else
	printf 'result=FAIL failures=%s\n' "$failures"
	exit 1
fi
