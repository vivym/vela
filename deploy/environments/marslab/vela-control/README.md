# MarsLab Control infrastructure validation

Render with `kubectl kustomize deploy/environments/marslab/vela-control`.
This site overlay records the site configuration separately from the generic
production base. It references existing Secrets without embedding their values.
The image is pinned to the measured digest and both replicas run
on the CPU management nodes with maxSurge 0 / maxUnavailable 1.

As of 2026-09-15 04:12 CST, both Control replicas consume the explicit 33
`secretKeyRef` environment entries and keyed items for nine file Secrets.
The site binds versioned immutable ConfigMaps and Secrets with canonical
`vela.ai/release-revision` annotations. The Secret payloads are provisioned
externally and are not rendered into this repository. See
`docs/release-material-adoption-2026-09-15.md` for exact identities and evidence.

The Pod and ServiceAccount use
`vela-release-pull-v1-r-7c3b2ad47663`, containing read-only credentials for the
three release-registry endpoints. The Control image is
`10.1.201.70:5005/vela-control@sha256:e5132776a18db030cc3530cb6eaa0d6513eee88e349044741bb15c6666060dea`.
At 21:03 CST it was upgraded from archived revision `6619846`, together with
Goose migrations 97–99 and Fleet. Both replicas are Ready. Full Worker startup
and authenticated H3 Job acceptance remain incomplete; see
`docs/h3-formal-deployment-status-2026-09-15.md`.
At 04:27 CST, `tracing.patch.json` enabled OTLP HTTP with 10% root sampling.
Both real Control Pods and both APISIX gateways passed 46 tracing checks through
Collector/Tempo. These used unauthenticated read requests; model execution and
async lineage remain separate acceptance work.

Runtime configuration is
`vela-control-runtime-v-ebd9cc4d0cb4h-r-55f745cacf76`; Node Agent configuration is
`vela-control-node-agents-v-ebd9cc4d0cb4e-r-9680ac7946e6`. The retained Pod release
label `v-ebd9cc4d0cb4` remains a historical infrastructure-validation identity.
Updating original TLS or client Secrets does not update these snapshots;
rotation requires new snapshots and a coordinated consumer rollout.

`node-agent-access.patch.json` replaces the base placeholder policy. At
2026-09-15 07:55–07:58 CST, probes from all 51 online GPU hosts through ClusterIP
to both CPU nodes measured each host's own CNI network address. The policy now
permits those 51 exact `/32` sources on TCP 8444; offline `.19` is excluded.
The policy was applied independently, with 667 checks covering both Control
Pods and the Service, denied 8081/8445/8446/8447 paths, cross-host CPU denial,
and anonymous TLS rejection after pinning the server leaf certificate.
Control and host boot/service identities did not change. See
`docs/worker-control-network-validation-2026-09-15.md` for source bindings and
the temporary-resource cleanup evidence.

This establishes network reachability and refusal of connections without a
client certificate. It does not establish authenticated Node Agent registration
or RPC authorization. The address identifies a CNI host source, not an individual
process. Provision application identities before starting Node Agents, and
re-measure after Node replacement, PodCIDR or Service masquerading changes.
Do not use a whole Pod subnet. Do not apply the full overlay for this network
change: its Fleet trust reference still belongs to an unadopted candidate.

The Pod has no fsGroup: Longhorn remount testing reproduced fsGroup widening
the Artifact sandbox from 0700 to 02770, which the application correctly rejects.
The init materializer sets ownership and modes. Stage Finalizer identity is an
explicit env entry after VELA_POD_UID so Kubernetes can expand it; envFrom
ConfigMap strings do not perform that expansion.

`capacity-status.json` records the current 20Gi disposable scratch claim and
the unmet production storage contract. No dedicated pool or I/O limits are
claimed. Artifact peak space needs further accounting because input spooling
and sandbox staging can each retain a full input copy. The configured invoice
destination at loopback port 9 is also a validation placeholder, not a working
finance integration.

Use `docs/cluster-production-readiness-2026-09-14.md` for the finite remaining
acceptance list. Regenerate this render before applying; older remote renders
may reference superseded immutable ConfigMaps.
See `docs/release-input-validation-2026-09-15.md` for live-policy, external-material, and database-version evidence.
