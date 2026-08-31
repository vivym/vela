# Stage Cutover Zero-backlog Seal Runbook

This runbook records the M5 drain observation and seals the immutable
zero-backlog receipt. It does not unload a healthy resident model, contract the
schema, remove an N-1 deployment, create a Launch Receipt, or advance a
Production Gate.

## Preconditions

- The current cutover revision is `PRODUCTION`, `STAGE_ONLY`, and routes 100% of
  new Jobs to `STAGE_GRAPH`.
- Its release, configuration, graph, profile, connector set, and sealed
  nine-receipt Launch manifest bindings are current.
- Legacy Jobs remain under their original authority until terminal. Never
  translate an in-flight Job between `LEGACY_WORKER` and `STAGE_GRAPH`.
- The operator has the Catalog Promotion database credential and a write-once
  evidence directory with owner-only permissions.
- The external evidence collector can inspect Worker-local recovery state, the
  actual N-1 deployment, scheduler backlog, event backlog, and Artifact backlog.

Build the operator binary from the exact release source:

```bash
umask 077
go build -o ./bin/vela-stage-cutover ./cmd/vela-stage-cutover
export VELA_STAGE_CUTOVER_DATABASE_URL='postgres://...'
evidence_dir="$(mktemp -d)"
```

Do not place the database URL in a request file or shell history. The login must
have only the `vela_catalog_promotion` role surface; the CLI verifies this at
startup.

## Capture the start observation

Create a stable UUID for each request. Retrying the same logical operation must
reuse the same request bytes and UUID.

```bash
start_inventory_id="$(uuidgen | tr '[:upper:]' '[:lower:]')"
cat >"${evidence_dir}/start-inventory-request.json" <<JSON
{
  "snapshot_id": "${start_inventory_id}",
  "observed_by": "stage-cutover-operator"
}
JSON

./bin/vela-stage-cutover capture-inventory \
  --request "${evidence_dir}/start-inventory-request.json" \
  >"${evidence_dir}/start-inventory-result.json"
```

The result reports all nine database inventory categories. Stop if
`total_count` is nonzero; do not delete authority rows to make the count zero.

Collect the five external inventories from their authoritative systems and
store their exact queries, deployment identities, timestamps, outputs, and
collector version in a canonical evidence manifest. The manifest must remain
available independently of Vela. Hash its exact bytes:

```bash
external_manifest="${evidence_dir}/start-external-manifest.json"
external_manifest_digest="$(shasum -a 256 "${external_manifest}" | awk '{print $1}')"
start_external_id="$(uuidgen | tr '[:upper:]' '[:lower:]')"
cat >"${evidence_dir}/start-external-request.json" <<JSON
{
  "evidence_id": "${start_external_id}",
  "backlog": {
    "worker_local_recovery": 0,
    "n_minus_one_deployment": 0,
    "scheduler": 0,
    "event": 0,
    "artifact": 0
  },
  "evidence_manifest_digest": "${external_manifest_digest}",
  "observed_by": "stage-cutover-operator"
}
JSON

./bin/vela-stage-cutover record-external-evidence \
  --request "${evidence_dir}/start-external-request.json" \
  >"${evidence_dir}/start-external-result.json"
```

Never write zero merely because a source is unreachable. Unreachable or stale
inventory is an observation failure and keeps the cutover unsealed.

## Capture the end observation

Wait at least the current cutover revision's `minimum_observation_seconds`.
During the window, keep legacy Admission disabled and continue normal drain,
reconciliation, and monitoring. Do not unload healthy `ModelResidency` to make
the observation easier.

Repeat both commands with new end-observation UUIDs and a new external manifest:

```bash
./bin/vela-stage-cutover capture-inventory \
  --request "${evidence_dir}/end-inventory-request.json" \
  >"${evidence_dir}/end-inventory-result.json"

./bin/vela-stage-cutover record-external-evidence \
  --request "${evidence_dir}/end-external-request.json" \
  >"${evidence_dir}/end-external-result.json"
```

Both end totals must be zero. The database function binds each observation to
the current cutover revision and its release/configuration/graph/profile/
connector/Launch-manifest identities.

## Seal zero backlog

Use the four returned identities, not values copied from an unrelated window:

```json
{
  "receipt_id": "52000000-0000-0000-0000-000000000005",
  "start_inventory_id": "52000000-0000-0000-0000-000000000001",
  "end_inventory_id": "52000000-0000-0000-0000-000000000003",
  "start_external_evidence_id": "52000000-0000-0000-0000-000000000002",
  "end_external_evidence_id": "52000000-0000-0000-0000-000000000004",
  "sealed_by": "stage-cutover-operator"
}
```

```bash
./bin/vela-stage-cutover seal-zero-backlog \
  --request "${evidence_dir}/seal-request.json" \
  >"${evidence_dir}/seal-result.json"
```

The seal transaction rechecks live database inventory while holding the source
relations against concurrent legacy writes. It also verifies the observation
window, both zero inventories, all identity bindings, and the current cutover
revision. A successful receipt permanently fences new legacy Jobs and further
cutover mutation.

## Replay and failure handling

- An interrupted request is retried with the identical UUID and bytes. A
  successful exact replay returns `replayed=true` where the function exposes it.
- A replay mismatch is a fail-closed operator error. Retain both payloads and
  investigate; never choose a new UUID to hide a changed request.
- A genuinely new observation uses a new UUID and a newly retained manifest.
- Nonzero, stale, missing, short-window, revision-mismatch, or live-inventory
  failures leave the cutover unsealed. Drain or repair the authoritative source,
  then start a new evidence window.
- Retain every request, result, external manifest, binary release digest, and
  stderr record under the release evidence policy.
- Do not run schema contraction until its separate M6 guard accepts this receipt
  and independently rechecks live zero. This M5 receipt is not permission to
  invent or bypass any Production Gate evidence.
