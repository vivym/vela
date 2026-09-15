# Management-cluster release inputs and evidence

Internal infrastructure has already been provisioned under the user's
instruction to prepare the missing inputs. Do not repeatedly ask the user for
values that can be generated or resolved from the existing cluster. Secrets
remain in the cluster's immutable/versioned Secret objects; this document
records categories and readiness, never credentials.

| Area | Current source and remaining validation |
| --- | --- |
| Images/release | Internal release registry and running image digests exist. Control is pinned to the measured `sha256:7d1acb338a3c8a31cea7393863070ae0e4ef375b414355dc7b03dcfebfcaee51`. Generic base and `deploy/environments/marslab/vela-control` are reconciled; all deployment-contract tests pass, and runtime ConfigMap h is live. The site remains explicitly infrastructure-validation. Assemble and validate one canonical bundle spanning all required application components |
| Customer/platform identity | Internal Keycloak and cluster-local PKI manifests are in `deploy/identity`. Resolve issuer/audience/JWKS and client trust from that deployment, and test actual authentication plus the separation of customer/operator identity |
| API ingress | APISIX is available at `.70/.71:30080`, HTTPS 30443 with cert-manager private PKI and a five-minute certificate sync CronJob. Public DNS and a public-trust certificate are conditional on publication outside the private network; do not invent a VIP |
| Artifacts and backups | Existing MinIO provides internal buckets/credentials. Preserve their object/version/access policies and bind the release references. The user accepts the common physical failure domain; validate in-cluster recovery and MinIO quorum separately |
| PKI/Secrets | Existing cert-manager and materialized transport/NATS/artifact/keyring Secrets supply versioned references. Complete canonical Secret-contract and certificate-role validation rather than committing values |
| Finance/webhooks | Retain the current configured references. Production business integration requires real approved endpoints/identities and end-to-end application evidence; an infrastructure smoke test does not establish delivery |
| Model release | Build digest-pinned runtime packages and bind model checksum/preset revisions to actual model/Stage tests and Launch Receipts |
| Observability | Metrics, logs, gateway traces and internal Grafana alert flow are deployed. Complete business OTLP/SLI/SLO/residency integration and capacity/recovery evidence. External notifications are explicitly deferred |

The exact finite work list and acceptance criteria are maintained only in
`docs/cluster-production-readiness-2026-09-14.md` (R1–R6). External paging and an
independent-site storage purchase are not added requirements for this agreed
cluster deployment. The separate requirements for an actual customer release
remain visible and must not be replaced with placeholder evidence.
