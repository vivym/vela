# Authorized debug-dump lifecycle

## Status

Accepted for Slice 28 implementation.

## Context

ADR 0015 fixes opt-in debug dumps at no more than 72 hours and classifies them
as Customer Content. The Admission snapshot already carries
`retention_debug_hours = 72`, but the repository has no customer authorization,
no Worker upload authority, no isolated read path, and no durable expiry
deletion path. A debug dump must never become an `Artifact`, enter an
`ArtifactSet`, gate Visible Completion, or affect Charge authority.

## Customer authorization

- Only a Human Principal with the exact ProjectAdmin-derived
  `debug_dumps:manage` scope may authorize, inspect, revoke, or read debug dumps.
  Service Principals and all other customer roles are denied.
- Authorization is Project- and Job-scoped, idempotent, immutable except for
  permanent revocation, and auditable with exact Human Principal/session
  attribution.
- A new authorization is accepted only while the Job is `QUEUED` or
  `RETRY_WAIT` and has no active Attempt. This makes opt-in explicit before the
  affected Attempt starts.
- Authorization expires exactly at `authorized_at + retention_debug_hours`,
  using the immutable Job retention snapshot. Launch policy fixes that value at
  72 hours.
- Admission and retry semantics are unchanged. The Scheduler attaches at most
  one active authorization to an Assignment; the Assignment remains valid if no
  authorization exists.

## Runner and Worker contract

- Runner execution input carries the exact authorization id and expiry only
  when the Assignment selected an active authorization.
- The Runner may create one bounded structured debug dump for a failed Attempt
  only. The dump omits credentials, signed URLs, prompt text, and raw input or
  output payloads. It is returned to the Worker Agent as bounded bytes with an
  exact content type and SHA-256 receipt.
- The Worker Agent uploads the dump before reporting the failure while the same
  Attempt Lease remains current. The control plane revalidates Worker identity,
  Worker epoch, Attempt id, Lease fence/token/expiry, Job id, authorization id,
  authorization expiry, size, checksum, content type, and deterministic object
  namespace.
- Multipart permissions bind one exact object key, part number, part size, and
  checksum. Completion records the immutable object version and only succeeds
  when the object-store receipt matches the original claim.
- Debug upload is diagnostic and non-blocking. Rejection, timeout, object-store
  failure, or an expired/revoked authorization cannot delay or replace the
  authoritative failure transition. Incomplete uploads remain private and are
  deleted by retention reconciliation.

## Storage and access isolation

- `debug_dump_authorizations`, `debug_dumps`, and their audit evidence are
  separate from `artifacts`, `artifact_uploads`, `artifact_sets`, and
  `artifact_set_items`.
- Object keys use the private namespace
  `debug-dumps/{organization}/{project}/{job}/{authorization}/{attempt}/{dump}`.
- Customer download uses short-lived exact-version signed URLs. The database
  authorizes each read and the service records delivery success or failure.
  Authorization is revalidated after signing so revocation or expiry wins the
  race.
- Listing or reading requires the same exact ProjectAdmin authority that grants
  opt-in. Cross-Project and cross-Organization access is denied without exposing
  object identity.
- Debug authorization, revocation, read authorization, delivery, upload, and
  deletion evidence are immutable safe metadata and are visible through the
  Organization audit projection without exposing dump content or signed URLs.

## Deletion lifecycle

- Expired authorization creates one durable retention request for its exact
  authorization namespace. Available dumps use exact object-version targets;
  incomplete dumps use object discovery plus multipart-prefix cleanup.
- Customer Content Deletion permanently revokes all debug authorizations for
  the Job and adds the same exact dump and multipart targets to the customer
  deletion request. Charge and required non-content audit records remain.
- Existing claim TTL, retry, exact-version deletion, multipart abort, and
  immutable receipt semantics apply. A dump becomes `DELETED` only after its
  object target completes.
- Retention reconciliation remains compatible with the N-1 control binary on
  the additive schema. Migration down refuses while durable debug evidence
  exists.
- Customer debug-dump operations use a dedicated `vela_debug_dump_request`
  database role and Organization audit v3 uses a dedicated
  `vela_debug_dump_audit_request` role. Migration 00027 does not expand the
  exact function allowlists of `vela_retention_request` or
  `vela_break_glass_audit_request`, so an N-1 control binary still passes its
  startup role preflight on schema 00027.
- Production wiring supplies independently verified
  `VELA_DEBUG_DUMP_REQUEST_DATABASE_URL` and
  `VELA_DEBUG_DUMP_AUDIT_REQUEST_DATABASE_URL` pools.

## Public verification seams

1. Production HTTP: ProjectAdmin authorize/get/revoke/list, idempotency replay,
   stale Human session, service-role denial, and cross-Project isolation.
2. Assignment plus Runner: default-off Assignment, active authorization
   snapshot, bounded failure dump generation, and recovery receipt stability.
3. Worker control gRPC: current Lease upload succeeds; stale epoch/fence/token,
   mismatched authorization/Attempt/checksum/size/content type, revoked/expired
   authorization, and replay conflicts fail closed.
4. Retention: 72-hour enqueue, exact-version deletion, incomplete multipart
   cleanup, retry/claim recovery, Content Deletion coexistence, and immutable
   receipts.
5. Completion isolation: a debug dump never appears in customer Artifact APIs,
   never satisfies an `ArtifactSet` item, and never changes Visible Completion
   or Charge state.
6. Migration and roles: least privilege, forced RLS, empty down/up, refusal with
   durable evidence, generated-code fixed point, and exact N/N-1 compatibility.

## Explicitly deferred

- Production object-store lifecycle and backup deletion replay receipts;
- legal hold and contract-specific debug duration variants;
- routine operator access to debug dumps; Platform Operators still require a
  separately specified dual-controlled support surface;
- production Runner/backend-specific dump enrichment and real failure-injection
  Launch Receipts;
- metadata/financial expiry, live scratch lifecycle, and all nine Production
  Gate Launch Receipts.

## Completion boundary

Slice 28 is complete when the production customer, Assignment, Runner, Worker,
storage, read-audit, and retention caller paths above pass focused and full
verification with no unresolved P0-P2 review finding. This advances ADR 0015
and Scenario 19 but does not make Scenario 19 direct end-to-end evidence or
change Production Gates from `0/9 PASS`.
