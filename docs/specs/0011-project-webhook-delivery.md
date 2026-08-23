# Project Webhook Delivery

Status: Implemented

Date: 2026-08-23

Predecessors:

- `docs/specs/0001-admission-control-plane-foundation.md`
- `docs/specs/0006-customer-cancellation-and-charge.md`
- `docs/specs/0007-artifact-finalization-and-visible-completion.md`
- `docs/specs/0009-invoice-export-and-receipt.md`

## Goal

Deliver every subscribed `job.succeeded`, `job.failed`, and `job.canceled`
terminal event to a Project-owned HTTPS endpoint at least once. PostgreSQL owns
the Subscription, Delivery, retry, dead-letter, replay, and attempt receipts;
the remote endpoint never becomes authoritative for Job, Charge, ArtifactSet,
or Artifact access state.

## Governing Decisions

- ADR 0004/0007: every Subscription and Delivery is bound to one Project and
  Customer Organization through composite identity checks and role isolation.
- ADR 0008: webhook payloads contain no prompt, Artifact metadata, signed URL,
  raw failure evidence, Credential, or other Customer Content.
- ADR 0013: migration 00011 is additive and the exact `53f5d65` N-1 control
  binary can create terminal events safely on the expanded schema.
- ADR 0014: delivery is at-least-once, timestamped and HMAC signed, retries for
  at most 72 hours, then remains visible and manually replayable.
- ADR 0029: repository evidence is not a production webhook Launch Receipt and
  does not advance any Production Gate.

## In Scope

1. Project-scoped create, list, disable, secret-rotation, delivery-list, and
   manual-replay HTTP APIs guarded by explicit Service Principal scopes.
2. An ACTIVE/DISABLED Webhook Subscription with a fixed subset of the three
   terminal Job event types and an immutable HTTPS endpoint.
3. A randomly generated signing secret returned only by create or rotate and
   stored only as authenticated ciphertext bound to its exact Project,
   Subscription, secret revision, and key id.
4. One current signing secret and one time-bounded previous secret during a
   24-hour overlap. Rotation while an earlier overlap is live fails closed.
5. Transactional fanout from each terminal Outbox insert to one durable
   Webhook Delivery per matching ACTIVE Subscription. The Outbox payload is not
   decoded and is never forwarded.
6. PostgreSQL-authoritative PENDING, IN_FLIGHT, DELIVERED, and DEAD_LETTER
   transitions, bounded claims, exponential retry, a 72-hour automatic retry
   window, crash recovery, and immutable per-attempt receipts.
7. Manual replay of a terminal Delivery with the same `event_id`, a fresh
   72-hour retry window, an immutable replay receipt, and no mutation of the
   originating Job or financial and Artifact facts.
8. A production HTTPS adapter that signs the exact request bytes, refuses
   redirects, bounds time and response handling, and blocks local, private,
   link-local, multicast, documentation, and other non-public destinations at
   both registration and dial time.
9. A `vela-control` reconciliation loop and a dedicated least-privilege
   `vela_webhook` database role. JetStream may wake later implementations, but
   PostgreSQL scanning is sufficient for correctness and outage recovery.
10. Migration, role, RLS, replay, concurrency, crash-window, N/N-1,
    generation, lint, integration, and race evidence.

## Explicitly Deferred

- Human Principal, fixed Human RBAC, and administrative membership APIs. This
  slice uses Project Service Principal scopes without claiming ADR 0005/0006.
- Customer-configurable retry schedules, secret-overlap periods, payload
  templates, custom headers, mTLS, or non-HTTPS endpoints.
- More event families than the three terminal Job events.
- A webhook Production Gate receipt, public-DNS deployment validation, or
  external endpoint availability certification.
- Using JetStream as delivery authority. A later consumer may reduce discovery
  latency but must reconcile the same PostgreSQL rows.

## Public Seams

The pre-agreed test seams are:

1. the generated strict HTTP API plus `webhook.Service` for Project-scoped
   Subscription, rotation, visibility, disable, and manual replay;
2. the production terminal Job seams that atomically insert canonical Outbox
   events and therefore create matching Deliveries;
3. `webhook.Dispatcher.DispatchBatch` with PostgreSQL and a fake or production
   HTTPS adapter;
4. the production HTTPS/HMAC adapter through `http.RoundTripper` and the
   public-address dialing policy;
5. PostgreSQL migration, constraints, privileges, N/N-1 writer behavior, and
   immutable attempt/replay receipts.

Tests may inspect PostgreSQL after an operation to prove atomic invariants, but
direct SQL mutation is limited to fixture setup, time/failure injection, and
negative constraint or privilege evidence.

## Subscription And Secret Contract

A Subscription has immutable `organization_id`, `project_id`, `id`, endpoint,
event set, creator, and creation time. It begins ACTIVE and may transition once
to DISABLED. Updating an endpoint or re-enabling a Subscription requires a new
Subscription so old Delivery evidence remains unambiguous.

Create and rotate generate at least 256 bits of entropy. The API returns the
`vwhsec_` secret once. Vela encrypts the exact returned secret with AES-256-GCM;
associated data binds:

```text
organization_id | project_id | subscription_id | secret_id | secret_revision
```

The database stores ciphertext, nonce, key id, and secret revision but never
plaintext. Ciphertext, nonce, key id, and identity are immutable. Rotation
closes the current secret at PostgreSQL time plus 24 hours and inserts the next
current revision atomically. During overlap, Dispatch signs with the new secret
first and the previous secret second. A missing encryption key, authentication
failure, or inconsistent secret set fails the Delivery attempt without sending.

## Delivery And Replay Contract

An `AFTER INSERT` Outbox trigger creates a Delivery only when all of these hold:

- `aggregate_type = 'Job'`;
- event type is `job.succeeded`, `job.failed`, or `job.canceled`;
- the Subscription is ACTIVE, belongs to the same Project and Organization,
  and includes the event type.

The Delivery fixes event id, type, Job id, aggregate version, occurred time,
Subscription, endpoint identity, and a safe schema-version-1 JSON payload. A
composite foreign key and immutable identity trigger prevent cross-Project or
cross-event substitution. Payload construction uses only trusted Outbox columns
and the terminal event-to-state mapping:

```json
{
  "schema_version": 1,
  "event_id": "uuid",
  "event_type": "job.succeeded",
  "occurred_at": "RFC3339 timestamp",
  "organization_id": "uuid",
  "project_id": "uuid",
  "job_id": "uuid",
  "job_version": 7,
  "job_state": "SUCCEEDED"
}
```

Automatic retry ends 72 hours after Delivery creation. A bounded claim changes
PENDING to IN_FLIGHT and inserts a STARTED attempt in the same transaction.
Expired claims become ABANDONED before a new attempt is created. HTTP 2xx marks
the attempt SUCCEEDED and Delivery DELIVERED. Timeout, transport failure,
redirect, invalid acknowledgement, or non-2xx marks the attempt FAILED and uses
bounded exponential backoff while the retry window remains; otherwise the
Delivery becomes DEAD_LETTER. Response bodies are neither trusted nor stored.

Manual replay is allowed only from DELIVERED or DEAD_LETTER on an ACTIVE
Subscription. It preserves all prior attempt evidence, increments the Delivery
generation, records the requesting Principal/Credential in an immutable replay
receipt, resets PENDING availability and a fresh 72-hour retry deadline, and
uses the original event id and payload. Concurrent replay requests serialize;
only one may move a terminal Delivery into a new generation.

Disabling a Subscription prevents new fanout and new claims, moves its PENDING
Deliveries to DEAD_LETTER, and leaves an already executing request free to
record its actual result. An expired IN_FLIGHT claim under a disabled
Subscription becomes DEAD_LETTER rather than being sent again.

## HTTP Signature And Transport

Every request is `POST` with `Content-Type: application/json` and:

```text
Vela-Webhook-Id: <subscription_id>
Vela-Delivery-Id: <delivery_id>
Vela-Event-Id: <event_id>
Vela-Timestamp: <unix seconds from the PostgreSQL claim timestamp>
Vela-Signature: v1=<lowercase hex HMAC>[,v1=<previous-secret HMAC>]
```

Each HMAC-SHA256 signs these exact bytes:

```text
<timestamp>.<event_id>.<raw JSON request body>
```

The adapter accepts only a 2xx response. It never follows redirects, never
sends Authorization or Customer credentials, limits response bytes, and uses a
bounded request timeout. Registration rejects malformed URLs, non-HTTPS URLs,
userinfo, fragments, IP literals in forbidden ranges, localhost names, and
missing explicit hosts. The production dialer resolves every request and fails
closed if any selected address is not public, preventing a later DNS change
from bypassing registration validation.

## Database And Authorization Invariants

- At most one Delivery exists for one Subscription and event id.
- Subscription/event/Project identity and Delivery payload are immutable.
- Secret plaintext is absent from PostgreSQL and logs; ciphertext identity is
  immutable and secret revision numbers are contiguous.
- Only one current secret exists; at most one previous secret is usable during
  overlap.
- A Delivery claim token owns exactly one STARTED attempt; stale tokens cannot
  complete or fail a later attempt.
- DELIVERED and DEAD_LETTER state is consistent with attempt history; attempts
  and replay receipts cannot be updated, deleted, or truncated after terminal.
- Request roles see only safe Subscription and Delivery projections through
  authenticated Project-scoped functions. They cannot read ciphertext, attempt
  error evidence, or dispatcher claims.
- `vela_webhook` has no direct table authority and can execute only claim,
  success, and failure functions. Every unrelated runtime role is denied.
- Delivery failure, replay, disable, and dispatcher crash cannot update Job,
  Charge, CreditReservation, ArtifactSet, Artifact, or Outbox identity.

## Required Evidence

- create returns a secret once, persists only authenticated ciphertext, rejects
  unsafe endpoints/event sets, and enforces Project scope;
- terminal success, failure, and cancellation each fan out one safe Delivery to
  matching active Subscriptions in the same transaction, including an exact
  N-1 terminal writer;
- successful delivery has a verifiable current HMAC and one immutable attempt;
- overlapping rotation emits two independently verifiable signatures, expires
  the previous secret, and rejects a second rotation during overlap;
- timeout, redirect, non-2xx, and dispatcher crash retry the same event id;
  concurrent replicas cannot own the same live claim;
- automatic retry stops at 72 hours and the Delivery remains visible;
- manual replay preserves attempt history and uses the same event id, while
  concurrent/stale replay and disabled-subscription replay fail;
- payload and headers contain no prompt, Artifact facts, signed URL, raw failure
  fingerprint, Credential, or secret;
- unsafe DNS/address targets, oversized responses, stale claim tokens, direct
  table access, cross-Project access, secret/attempt/replay mutation, and
  identity mismatch fail closed;
- pending Down/Up restores the default surface, while any Subscription,
  Delivery, secret, attempt, or replay evidence refuses structural Down;
- `make generate`, lint, unit, integration, race, and two-axis review pass with
  no unresolved P0-P2 finding.

## Completion Boundary

This slice is complete only when all public seams are on the production caller
path, all required evidence passes, generated sources are at a fixed point, and
two-axis review finds no unresolved P0-P2 issue. Human RBAC, deployment DNS
validation, real endpoint receipts, and all nine Production Gates remain
separate and must stay unclaimed.
