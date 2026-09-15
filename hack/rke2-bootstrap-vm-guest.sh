#!/bin/bash
set -euo pipefail
case "$(hostname)" in vela-bootstrap-drill-*) ;; *) exit 80 ;; esac
test ! -e /var/lib/rancher/rke2/agent
install -d -m 0700 /var/lib/vela-bootstrap-validation /etc/rancher/rke2
install -d -m 0755 /usr/local/libexec /etc/rancher/rke2/config.yaml.d /etc/systemd/system/rke2-agent.service.d
printf '{"agent_directory_absent":true,"load_balancer_cache_absent":true}\n' > /var/lib/vela-bootstrap-validation/fresh.json
source=/mnt/vela-bootstrap
install -m 0755 "$source/rke2" /usr/local/bin/rke2
install -m 0755 "$source/rke2-bootstrap-ha.py" /usr/local/libexec/vela-rke2-bootstrap-ha.py
install -m 0644 "$source/rke2-bootstrap-endpoints.json" /etc/rancher/rke2/vela-bootstrap-endpoints.json
install -m 0644 "$source/server-ca.crt" /etc/rancher/rke2/bootstrap-server-ca.crt
install -m 0644 "$source/registry-ca.crt" /etc/rancher/rke2/registry-ca.crt
install -m 0600 "$source/registries.yaml" /etc/rancher/rke2/registries.yaml
install -m 0600 "$source/token" /etc/rancher/rke2/token
install -m 0600 "$source/config.yaml" /etc/rancher/rke2/config.yaml
install -m 0644 "$source/rke2-agent.service" /etc/systemd/system/rke2-agent.service
install -m 0644 "$source/rke2-agent-bootstrap-ha.conf" /etc/systemd/system/rke2-agent.service.d/20-vela-bootstrap-ha.conf
modprobe overlay
modprobe br_netfilter
sysctl -w net.ipv4.ip_forward=1 net.bridge.bridge-nf-call-iptables=1
# This rule exists only inside this disposable VM. The host is untouched.
iptables -w 5 -I OUTPUT 1 -d 10.1.201.70/32 -p tcp -m multiport --dports 6443,9345 \
  -m comment --comment vela-fresh-bootstrap -j REJECT --reject-with tcp-reset
python3 /usr/local/libexec/vela-rke2-bootstrap-ha.py --check-only > /var/lib/vela-bootstrap-validation/selection-before-agent.json
systemctl daemon-reload
systemctl enable --now rke2-agent
touch /var/lib/vela-bootstrap-validation/setup-complete
