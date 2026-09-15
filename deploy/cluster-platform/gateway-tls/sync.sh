#!/bin/sh
set -eu
umask 077
trap 'rm -f /work/admin.conf /work/ssl.json' EXIT
trap 'exit 130' HUP INT TERM

# Only PEM text enters the JSON string; reject characters needing JSON escaping.
pem_json() {
  awk '
    { sub(/\r$/, "") }
    NF == 0 { next }
    $0 !~ /^-----[A-Z ]+-----$/ && $0 !~ /^[A-Za-z0-9+\/=]+$/ { exit 1 }
    { printf "%s\\n", $0 }
  ' "$@"
}

# curl receives the credential through a private file, not its process args.
printf 'header = "X-API-KEY: %s"\n' "$APISIX_ADMIN" > /work/admin.conf
unset APISIX_ADMIN
# Resolve one projected-Secret generation so rotation cannot mix a key and cert.
tls_directory="/tls/$(readlink /tls/..data)"
{
  printf '{"cert":"'
  pem_json "$tls_directory/tls.crt" "$tls_directory/ca.crt"
  printf '","key":"'
  pem_json "$tls_directory/tls.key"
  printf '","snis":["apisix-gateway","apisix-gateway.apisix.svc","apisix-gateway.apisix.svc.cluster.local","ragflow.marslab.ic","10.1.201.70","10.1.201.71"],"labels":{"managed-by":"vela-gateway-tls-sync"}}'
} > /work/ssl.json
curl -q --config /work/admin.conf --fail --silent --show-error \
  --connect-timeout 5 --max-time 20 --retry 3 --retry-connrefused \
  -H 'Content-Type: application/json' -X PUT \
  --data-binary @/work/ssl.json --output /dev/null \
  http://apisix-admin.apisix.svc.cluster.local:9180/apisix/admin/ssls/vela-gateway-default
# APISIX responses contain the private key; never print response bodies.
echo 'APISIX TLS sync complete'
