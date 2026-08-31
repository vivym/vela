# H3 Launch Evidence Capture

`vela-h3-evidence` captures one stable, read-only view of an approved H3
ResidencyPlan from the live Fleet Registry and Kubernetes cluster. Its output
is a candidate `hardware-inventory` input for a real H3 evidence campaign. It
is not a Launch Receipt, does not replace any 72-hour soak or fault exercise,
and does not advance the repository's `0/9 PASS` Production Gate status.

## Bound inputs

The command accepts only:

- a canonical release bundle that passes `releasebundle.Load`; and
- one ResidencyPlan revision UUID already embedded in that bundle's
  digest-bound `fleet-controller` final render.

There is no command-line or file input for an operator-authored Worker,
Kubernetes, DRA, GPU, or ModelResidency snapshot. Expected Pod and
`ResourceClaimTemplate` content is regenerated from the exact approved
`WorkerBundleActuation` used by Fleet.

The collector then reads each authority twice. It emits evidence only if both
reads have identical:

- WorkerInstance, WorkerMember, device, node, DeviceSet, runtime and residency
  epochs and identities from a `REPEATABLE READ READ ONLY` Fleet transaction;
- Kubernetes cluster and namespace UID;
- Pod UID, resource version, scheduled node, container image ID, READY state,
  and restart count;
- generated ResourceClaim UID, reservation to the exact Pod UID, allocation,
  and exact source `ResourceClaimTemplate`; and
- current complete NVIDIA DRA `ResourceSlice` generation, GPU UUID and PCI BDF.

Any missing, duplicate, stale, unhealthy, non-READY, restarted, unpinned, or
mismatched value fails closed without output.

## Capture

Configure a dedicated read-only Fleet login and a Kubernetes identity with
`get` access to the target Namespace, Pods, Nodes, ResourceClaims and
ResourceClaimTemplates, plus `list` access to ResourceSlices. Outside the
cluster, set an explicit kubeconfig path.

```text
export VELA_H3_EVIDENCE_FLEET_DATABASE_URL='postgres://...'
export VELA_H3_EVIDENCE_VALIDATION_ENVIRONMENT='h3-production-cn-north-1'
export VELA_H3_EVIDENCE_COLLECTOR_IDENTITY='spiffe://vela/launch-evidence/collector'
export VELA_H3_EVIDENCE_KUBECONFIG='/secure/path/kubeconfig'

make capture-h3-launch-evidence \
  RELEASE_BUNDLE=/absolute/path/to/release-bundle.json \
  H3_EVIDENCE_PLAN_REVISION=49320000-0000-0000-0000-000000000001 \
  > /absolute/path/to/campaign/h3-launch-evidence.json
```

When the collector runs in-cluster,
`VELA_H3_EVIDENCE_KUBECONFIG` may be omitted and the in-cluster ServiceAccount
configuration is used. Keep the captured bytes immutable and digest-bind them
when assembling the real H3 Production Gate evidence. The full campaign must
still provide mixed-load soak, cross-node StageArtifact lineage, cache and
fencing checks, fault injection, N/N-1 drain, rollback, dashboards, alerts,
runbooks and ownership evidence.

## Stage execution campaign evidence

`internal/h3campaignevidence` implements the repository-side capture and strict
verification boundary for one H3 Stage execution campaign. It accepts a sealed
release binding only after `releasebundle.LoadResidencyPlanRollouts` has
validated the canonical bundle and selected exactly one embedded ResidencyPlan.
It then double-reads PostgreSQL through independent read-only repeatable-read
transactions and rejects any business-authority drift between the two reads.

The accepted campaign contains exactly:

- one successful same-node Encoder -> DiT -> VAE Job;
- one successful three-node Encoder -> DiT -> VAE Job with the same root input,
  graph, ordered StageProfile, StageInterface, connector, and final digest;
- two consumed adjacent-edge TransferTickets per physical Job;
- one successful Job with exact Encoder and DiT cache hits and a physical VAE;
- Project-scoped cache entries bound to source Artifacts, exact object versions,
  equivalence revisions, active references, execution pins, and output bindings;
  and
- one ArtifactSet, Visible Completion, and Charge for every completed campaign
  Job, with every physical source and target Worker bound to the selected
  ResidencyPlan revision.

The resulting JSON records truthful provenance for both database reads. The
integration fixture proves these fail-closed contracts against PostgreSQL, but
it uses synthetic local execution and storage. There is not yet a
`capture-campaign` operator command, dedicated evidence database role, real H3
GPU execution, production network/storage fault exercise, or Launch Receipt.
This repository evidence does not advance `0/9 PASS`.
