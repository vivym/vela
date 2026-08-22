# Invoice Export And Receipt

Status: Implemented

Date: 2026-08-23

Predecessors:

- `docs/specs/0006-customer-cancellation-and-charge.md`
- `docs/specs/0007-artifact-finalization-and-visible-completion.md`
- `docs/specs/0008-hierarchical-scheduler.md`

## Goal

Export every immutable POSTED Charge exactly once as an external Invoice line,
using `charge_id` as the external idempotency key. PostgreSQL remains the
authoritative recovery source before, during, and after JetStream or external
Invoice outages. Export never advances or blocks Job, Visible Completion, or
Artifact access.

## Governing Decisions

- ADR 0001: Contract credit and Charge are Vela facts; Invoice and settlement are
  external financial facts.
- ADR 0003: Visible Completion commits before asynchronous Invoice work.
- ADR 0007: a narrow runtime role cannot read Charge or export tables directly.
- ADR 0013: migration 00009 is additive and the exact `9676083` binary remains a
  valid N-1 writer.
- ADR 0026/0027: export never changes the immutable Charge or its winning billing
  authority.
- ADR 0029: repository evidence advances scenario 20 but is not a Production Gate
  Launch Receipt.

## In Scope

1. A PostgreSQL `invoice_exports` authority for every POSTED Charge, created only
   when the same transaction contains its canonical `invoice.export_requested`
   Outbox event.
2. PENDING, CLAIMED, and EXPORTED states with PostgreSQL time, bounded claims,
   claim expiry, attempts, retry availability, and bounded error evidence.
3. An immutable `invoice_export_receipts` row containing the external Invoice and
   line references returned for one `charge_id`.
4. A narrow `vela_billing` role with no table access and exactly three public
   `SECURITY DEFINER` functions: claim, mark exported, and mark failed. Their
   `vela_billing_owner` definer is NOLOGIN and inherited by no runtime role.
5. `billingexport.Service.ExportBatch` as the production reconciliation seam. It
   claims one Charge immediately before each external call, calls the adapter
   outside a database transaction, and records success or failure through that
   exact claim token.
6. A production HTTPS adapter using bearer authentication, bounded timeouts,
   redirect refusal, strict JSON, and `Idempotency-Key: <charge_id>`.
7. A `vela-control` background loop, independent billing pool and startup role
   verification, bounded configuration, retry logging, and clean shutdown.
8. Backfill, empty/pending Down/Up, durable-evidence Down refusal, exact N-1
   writer, authorization, concurrency, outage, and crash-window evidence.

## Explicitly Deferred

- External payment collection, Invoice lifecycle, credit notes, refunds, and the
  monthly aggregation policy inside the finance system.
- Idempotent settlement and credit-adjustment reconciliation records in Vela.
- BillingAdmin HTTP reads and Human Principal RBAC.
- Production finance credentials, endpoint conformance, dashboards, alerts, and
  Launch Receipts. Repository tests remain `0/9 PASS` for Production Gates.

## PostgreSQL Authority

Migration 00009 installs a deferred Charge constraint trigger. At commit, every
new Charge must have exactly one Outbox event with the same Organization, Project,
Job, posting time, `invoice.export_requested` type, and encoded `charge_id`. Only
then is the PENDING export authority inserted. Migration backfill applies the same
check to existing Charges and fails closed instead of inventing an intent.

The export identity is immutable:

```text
(organization_id, project_id, job_id, charge_id, requested_event_id)
```

The state machine is:

```text
PENDING -> CLAIMED -> EXPORTED
                  \-> PENDING
CLAIMED(expired) -> CLAIMED
```

Claim uses `FOR UPDATE SKIP LOCKED` only inside this dedicated exporter queue.
The function caps batch size at 1000 and claim TTL at five minutes. A failure
clears the claim, records a bounded error, and sets PostgreSQL `available_at` no
more than one hour ahead. The production loop requests one claim at a time so a
slow earlier adapter call cannot leave later work claimed but not yet started.
An expired claim can be reclaimed by another replica.

EXPORTED is valid exactly when one immutable receipt exists. Deferred consistency
triggers reject a partial receipt or a forged EXPORTED transition at transaction
commit. Completed authority and receipt rows cannot be changed or deleted.

## External Adapter Contract

The configured endpoint receives one HTTPS request per claimed Charge:

```http
POST <VELA_INVOICE_EXPORT_ENDPOINT>
Authorization: Bearer <secret from file>
Idempotency-Key: <charge_id>
Content-Type: application/json
Accept: application/json
```

```json
{
  "charge_id": "uuid",
  "organization_id": "uuid",
  "project_id": "uuid",
  "job_id": "uuid",
  "reason": "VISIBLE_COMPLETION | CUSTOMER_CANCELLATION",
  "amount_minor": 1250,
  "currency": "CNY",
  "posted_at": "UTC RFC3339 timestamp"
}
```

The endpoint must return HTTP 200 or 201 with exactly one bounded JSON document:

```json
{
  "invoice_reference": "external immutable Invoice reference",
  "line_reference": "external immutable line reference"
}
```

Redirects, non-JSON responses, unknown fields, missing references, and non-success
status codes are failures. The endpoint must make `charge_id` idempotent: the same
key and line must return the same logical external line after a timeout or crash.

## Failure And Recovery

The adapter call occurs after the claim statement commits and while no PostgreSQL
transaction is open. If the remote call fails, the claim returns to PENDING. If
the remote call succeeds but local receipt recording fails or the claim expires,
another replica retries the same `charge_id`; external idempotency prevents a
duplicate line, and only one current claim token can commit the local receipt.

`ExportBatch` scans PostgreSQL directly on every cycle. JetStream delivery may
wake other consumers but is not required to discover Invoice work. A published,
expired, missing, or temporarily unavailable JetStream consumer therefore cannot
lose a Charge. External Invoice failure changes only export authority; it cannot
change Charge, credit, Job, ArtifactSet, or Artifact access.

`/readyz` checks the narrow billing database pool but deliberately does not probe
the external Invoice endpoint. A finance outage must not remove the control plane
from service for Job and Artifact traffic.

## Runtime Configuration

Required:

- `VELA_BILLING_DATABASE_URL`
- `VELA_INVOICE_EXPORTER_ID`
- `VELA_INVOICE_EXPORT_ENDPOINT`
- `VELA_INVOICE_EXPORT_BEARER_TOKEN_FILE`

Bounded optional controls:

- `VELA_INVOICE_EXPORT_TICK` in `(0, 1m]`, default `500ms`
- `VELA_INVOICE_EXPORT_CLAIM_TTL` in `(0, 5m]`, default `30s`
- `VELA_INVOICE_EXPORT_RETRY_DELAY` in `[0, 1h]`, default `5s`
- `VELA_INVOICE_EXPORT_BATCH_SIZE` in `1..1000`, default `100`
- `VELA_INVOICE_EXPORT_HTTP_TIMEOUT` in `(0, 1m]`, default `15s`

## Compatibility And Migration

Released migrations 00001-00008 remain byte-identical. The exact `9676083`
cancellation writer can create a billable Charge under migration 00009; its
existing canonical Invoice intent causes one PENDING authority at commit. Old
readers ignore the new tables and role.

Down is allowed when all authorities are untouched PENDING rows, because Up can
reconstruct them from immutable Charge and Outbox evidence. Down refuses with
SQLSTATE `55000` after any attempt, failure, live claim, or receipt; deleting that
state could conceal a successful external call. Normal N/N-1 binary rollback keeps
the expanded schema and does not run structural Down.

## Test Seams And Required Evidence

The public seams are:

1. `billingexport.Service.ExportBatch` with a real PostgreSQL database and fake
   external adapter;
2. the production HTTPS adapter through `http.RoundTripper`;
3. migration, constraints, exact role verification, and N/N-1 writer behavior;
4. the `vela-control` reconciliation loop and bounded configuration.

Required evidence includes:

- happy export sends the stable key and persists exactly one receipt;
- remote failure retries the same key and preserves one receipt;
- remote success before local receipt recovers after claim expiry;
- concurrent replicas invoke the adapter once for one live claim;
- missing intent rolls back Charge and all cancellation effects;
- partial or mutable receipt evidence is rejected;
- Invoice outage leaves Visible Completion and exact-version Artifact access live;
- billing and every unrelated runtime role lack direct table/function authority;
- pending Down/Up reconstructs authority and durable evidence refuses Down;
- exact N-1 writer, generated code, unit, lint, integration, and race suites pass.

## Completion Boundary

This slice is complete when every seam above is on the production caller path,
all required evidence passes, generation is stable, and a two-axis review has no
unresolved P0-P2 issue. ADR 0001 remains Partial until settlement and
credit-adjustment reconciliation are implemented. Production remains `0/9 PASS`
until versioned Launch Receipts satisfy the separate hard gates.
