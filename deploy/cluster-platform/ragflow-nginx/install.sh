#!/usr/bin/env bash
# Copy this directory to each management host and run as root after Nginx install.
set -euo pipefail
umask 077
src=$(cd -- "$(dirname -- "$0")" && pwd)
backup=/root/vela-backups/nginx-ragflow-sync-$(date +%Y%m%d-%H%M%S)
site=/etc/nginx/conf.d/ragflow.marslab.ic.conf
[[ $EUID == 0 ]] || { echo 'Run as root' >&2; exit 1; }
nginx -t
install -d -m 700 "$backup"
cp -a /etc/nginx "$backup/nginx"
for file in /usr/local/sbin/sync-ragflow-nginx-cert \
    /etc/systemd/system/ragflow-nginx-cert-sync.service \
    /etc/systemd/system/ragflow-nginx-cert-sync.timer; do
  if [[ -f "$file" ]]; then cp -a "$file" "$backup/"; fi
done
install -m 700 "$src/sync-ragflow-nginx-cert" /usr/local/sbin/sync-ragflow-nginx-cert
/usr/local/sbin/sync-ragflow-nginx-cert
install -m 644 "$src/ragflow.marslab.ic.conf" "$site"
if ! nginx -t || ! systemctl reload nginx; then
  if [[ -f "$backup/nginx/conf.d/ragflow.marslab.ic.conf" ]]; then
    cp -a "$backup/nginx/conf.d/ragflow.marslab.ic.conf" "$site"
  else
    rm -f "$site"
  fi
  nginx -t && systemctl reload nginx
  exit 1
fi
install -m 644 "$src/ragflow-nginx-cert-sync.service" /etc/systemd/system/
install -m 644 "$src/ragflow-nginx-cert-sync.timer" /etc/systemd/system/
systemctl daemon-reload
systemctl enable nginx ragflow-nginx-cert-sync.timer
systemctl start ragflow-nginx-cert-sync.timer
systemctl start ragflow-nginx-cert-sync.service
systemctl is-active nginx ragflow-nginx-cert-sync.timer
systemctl is-enabled nginx ragflow-nginx-cert-sync.timer
printf 'backup=%s\n' "$backup"
