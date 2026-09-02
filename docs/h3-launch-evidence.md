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
  digest-bound `fleet-controller` final render. Every external Secret and
  ConfigMap declaration in that bundle is also mandatory capture authority.

There is no command-line or file input for an operator-authored Worker,
Kubernetes, DRA, GPU, or ModelResidency snapshot. Expected Pod and
`ResourceClaimTemplate` content is regenerated from the exact approved
`WorkerBundleActuation` used by Fleet.

The collector then reads each authority twice. It emits evidence only if both
reads have identical:

- WorkerInstance, WorkerMember, device, node, DeviceSet, runtime and residency
  epochs and identities from a `REPEATABLE READ READ ONLY` Fleet transaction;
- Kubernetes cluster and namespace UID;
- every release-declared external Secret/ConfigMap UID, resource version,
  immutability bit, revision annotation, exact Secret key set, and canonical
  content digest;
- Pod UID, resource version, scheduled node, container image ID, READY state,
  and restart count;
- generated ResourceClaim UID, reservation to the exact Pod UID, allocation,
  and exact source `ResourceClaimTemplate`; and
- current complete NVIDIA DRA `ResourceSlice` generation, GPU UUID and PCI BDF.

Any missing, duplicate, stale, unhealthy, non-READY, restarted, unpinned, or
mismatched value fails closed without output.

## Capture

Run `make preflight-h3-real-environment` first with the same release bundle,
ResidencyPlan revision, dedicated database identity, and kubeconfig. Continue
to capture only when its typed report has `ready=true`; the preflight report is
not itself launch evidence.

Configure a login that belongs only to the `vela_h3_campaign_evidence` NOLOGIN
group role. The command rejects admin, Fleet, mixed-role, or over-privileged
database identities before it reads Registry state. The Kubernetes identity
needs `get` access to the target Namespace, Pods, Nodes, ResourceClaims,
ResourceClaimTemplates, and every release-declared Secret and ConfigMap, plus
`list` access to ResourceSlices. Secret payloads are read only inside the
collector to recompute their digest and are never emitted. Outside the
cluster, set an explicit kubeconfig path.

```text
export VELA_H3_EVIDENCE_DATABASE_URL='postgres://...'
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

External-resource `revision` is the `sha256` digest of strict
`ExternalResourceContentV1` JSON: `schema_version`, `kind`, `namespace`, `name`,
optional Secret `secret_type`, ConfigMap `string_data`, and binary data. Empty
optional fields are omitted, object keys are encoded in lexical order, and byte
values use JSON base64 encoding. UID and resource version are excluded from
this content digest and recorded separately. The live object must also carry
`vela.ai/release-revision` equal to the computed digest. A changed payload with
an unchanged annotation therefore fails; deletion and same-name recreation
during the double-read window fails independently on UID drift.

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
it uses synthetic local execution and storage.

Configure a login that is a member of only the `vela_h3_campaign_evidence`
NOLOGIN group role. Migrations `00055_h3_campaign_evidence_reader.sql` and
`00058_legacy_h3_schema_contraction.sql` grant that role `SELECT` only on the
exact campaign and launch-evidence relation set and give it no mutation, owner,
`BYPASSRLS`, or role-escalation authority. The command verifies this exact
privilege boundary before reading any campaign rows; an admin, Fleet,
mixed-role, or over-privileged DSN fails closed.

```text
export VELA_H3_CAMPAIGN_EVIDENCE_DATABASE_URL='postgres://...'
export VELA_H3_EVIDENCE_VALIDATION_ENVIRONMENT='h3-production-cn-north-1'
export VELA_H3_EVIDENCE_COLLECTOR_IDENTITY='spiffe://vela/launch-evidence/campaign-reader'

make capture-h3-campaign-evidence \
  RELEASE_BUNDLE=/absolute/path/to/release-bundle.json \
  H3_EVIDENCE_PLAN_REVISION=49320000-0000-0000-0000-000000000001 \
  H3_SAME_NODE_JOB_ID=49320000-0000-0000-0000-000000000011 \
  H3_CROSS_NODE_JOB_ID=49320000-0000-0000-0000-000000000012 \
  H3_CACHE_JOB_ID=49320000-0000-0000-0000-000000000013 \
  > /absolute/path/to/campaign/h3-stage-campaign-evidence.json
```

The three Job IDs must be valid, non-nil, and pairwise distinct. The command
accepts only the sealed release binding produced by
`h3campaignevidence.LoadEvidenceBinding`, emits one strict JSON document to
stdout, and never accepts an operator-authored database snapshot.

`internal/h3faultevidence` separately verifies digest-bound receipts for the
fixed ten state/event fault scenarios. The stale-fence scenario requires
durable negative-probe records for member epoch, device epoch, ModelRuntime
epoch, and StageLease authority, each with different presented/current
authority digests and an exact rejection reason. It rejects missing scenarios,
nonzero loss/duplicate/stale-acceptance measurements, non-monotonic ledgers,
missing Accepted Job event coverage, campaign-global raw-event identity
duplication, path escape, receipt tamper, and ambiguous JSON. Every scenario
has an exact target kind, action, injection point, and trigger-event contract;
the Assignment crash window is explicitly a Stage Assignment window. A
`MODEL_RUNTIME_PROCESS` target is accepted only with a digest-bound maintenance
approval for that exact target and exercise window.

The command emits the
`vela.production-gates/state-event-fault-injection/v2` contract. Its candidate
`scenario-matrix`, `authority-before-after`, and `raw-event-payloads` artifacts
contain canonical projections of every source receipt rather than PASS-only
summaries. The projections preserve the source manifest digest, receipt
reference and digest, Exercise and Accepted Job identities, fault window,
before/after ledger observations, measurements, stale probes, and actual strict
JSON raw-event payloads. Payload digests are recomputed after whitespace-only
JSON canonicalization, and the Production Gate artifact loader independently
revalidates the three-artifact source and scenario closure.

```text
make build-h3-fault-campaign-evidence \
  H3_FAULT_CAMPAIGN_MANIFEST=/absolute/path/to/fault-campaign/manifest.json \
  H3_FAULT_EVIDENCE_OUTPUT=/absolute/path/to/new-state-event-evidence-directory
```

The output directory must not already exist. The command validates every
digest-bound receipt first, writes the envelope and three artifacts in one
private temporary directory, fsyncs each file, and atomically renames the
directory into place with an OS no-replace primitive. It fsyncs both the
candidate and parent directories and never replaces prior evidence, including
when another publisher creates the destination after the initial existence
check.

`process-kill` targets a stateless control-plane or Worker Agent process by
default. Killing or unloading a healthy resident ModelRuntime is not a normal
scheduling action and requires a separately approved maintenance exercise.
Likewise, N/N-1 drain stops new assignments and waits for accepted work; it does
not opportunistically release a healthy resident model.

There is still no real H3 GPU execution, production network/storage fault
exercise, or Launch Receipt in this repository. Repository fixtures and
candidate artifacts do not advance `0/9 PASS`.
