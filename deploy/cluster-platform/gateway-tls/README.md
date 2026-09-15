# Private gateway certificate renewal

Apply `kubectl apply -k deploy/cluster-platform/gateway-tls` after cert-manager,
APISIX and its existing admin Secret are ready. Wait for Certificate
`apisix/vela-gateway` to become Ready, then create a Job from CronJob
`vela-gateway-tls-sync` for initial publication. The CronJob subsequently runs
every five minutes, including after cluster or node restarts.

The CA is a self-signed root generated inside this cluster by cert-manager's
`vela-gateway-bootstrap` Issuer. Its common name is `Vela private gateway CA`;
the root certificate and private key live in Secret `apisix/vela-gateway-ca`.
The current root was created on 2026-09-14 at 21:33:34 Asia/Shanghai. This is the
project's own PKI; enterprise-managed PKI has not been integrated.

The private gateway CA is valid for ten years. Leaf certificates last 90 days
and renew 30 days before expiry, rotating their keys. The sync Job mounts only
the leaf Secret, keeps the admin credential and JSON containing the key in
memory-backed files, and updates only SSL object `vela-gateway-default`.
It has no Kubernetes API token and runs only on CPU management nodes.

Distribute only `vela-gateway-ca`'s public `tls.crt` to clients. Use hostname
`apisix-gateway` and port 30443 with a hosts-file/DNS mapping to either CPU
management address. The chart's `ssl.fallbackSNI=apisix-gateway` also supports
raw IP URLs on `.70/.71:30443` without client SNI; both IPs are certificate SANs.
Matched HTTP routes redirect to HTTPS using 308. A private CA does not provide
public browser trust, so install the public CA before following these redirects.
Changing the CA requires a coordinated client trust update. Public DNS/PKI
remains conditional on publication outside the private network.

RAGFlow also uses this CA, with SAN `ragflow.marslab.ic`. Its standard 80/443
host ingress and certificate synchronization are documented in
[`ragflow-nginx`](../ragflow-nginx/README.md).

The previous manual SSL object must be backed up to a root-only file before
the first sync; it contains a private key and must never be committed. If
rollback is needed, suspend the CronJob before restoring that exact object.
Validate served certificate fingerprints on both gateway Pods after renewal,
and compare them with the current `vela-gateway-tls` Secret.
