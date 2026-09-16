#!/usr/bin/env bash
# Install only the Vela virtual host; reuse existing RAGFlow TLS material.
set -euo pipefail
umask 077
[[ $EUID == 0 ]] || { echo 'Run as root' >&2; exit 1; }
src=$(cd -- "$(dirname -- "$0")" && pwd)
ca=${1:?Provide the public MARSLAB Root CA PEM path}
frontend=/etc/nginx/ssl/ragflow.marslab.ic
site=/etc/nginx/conf.d/vela.marslab.ic.conf
backup=/root/vela-backups/nginx-vela-$(date +%Y%m%d-%H%M%S)
openssl verify -CAfile "$ca" -verify_hostname vela.marslab.ic "$frontend/wildcard.marslab.ic.crt"
openssl x509 -in "$frontend/wildcard.marslab.ic.crt" -checkend 86400 -noout >/dev/null
cmp -s <(openssl x509 -in "$frontend/wildcard.marslab.ic.crt" -pubkey -noout) \
    <(openssl pkey -in "$frontend/wildcard.marslab.ic.key" -pubout 2>/dev/null) || {
    echo 'Certificate/key mismatch' >&2; exit 1;
}
openssl verify -CAfile "$frontend/current/ca.crt" \
    -verify_hostname apisix-gateway.apisix.svc.cluster.local "$frontend/current/tls.crt"
systemctl is-active nginx ragflow-nginx-cert-sync.timer
systemctl is-enabled nginx ragflow-nginx-cert-sync.timer
nginx -t
install -d -m 700 "$backup"
nginx -T >"$backup/nginx-before.txt" 2>&1
if [[ -f "$site" ]]; then cp -a "$site" "$backup/vela.marslab.ic.conf"; fi
changed=false
committed=false
cleanup() {
    rc=$?
    trap - EXIT
    if "$changed" && ! "$committed"; then
        if [[ -f "$backup/vela.marslab.ic.conf" ]]; then
            cp -a "$backup/vela.marslab.ic.conf" "$site"
        else
            rm -f "$site"
        fi
        if nginx -t; then systemctl reload nginx || true; fi
    fi
    exit "$rc"
}
trap cleanup EXIT
trap 'exit 130' INT TERM HUP
changed=true
install -m 644 "$src/vela.marslab.ic.conf" "$site"
nginx -t
systemctl reload nginx
passed=false
for attempt in 1 2 3 4 5; do
    status=$(curl -q --noproxy '*' --cacert "$ca" \
        --resolve vela.marslab.ic:443:127.0.0.1 --max-time 20 -sS -o /dev/null -w '%{http_code}' \
        https://vela.marslab.ic/api/v1/projects/62275ddc-ae83-4ca1-b80c-313161264836/jobs/00000000-0000-4000-8000-000000000001) || status=000
    if [[ "$status" == 401 ]]; then passed=true; break; fi
    sleep 1
done
"$passed" || { echo "Vela probe failed: $status" >&2; exit 1; }
status=$(curl -q --noproxy '*' --cacert "$ca" \
    --resolve ragflow.marslab.ic:443:127.0.0.1 --max-time 30 -sS -o /dev/null -w '%{http_code}' \
    https://ragflow.marslab.ic/)
[[ "$status" == 200 ]] || { echo "RAGFlow regression: $status" >&2; exit 1; }
committed=true
printf 'backup=%s\nVela HTTPS=401 (expected), RAGFlow HTTPS=200\n' "$backup"
