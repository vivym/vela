# Non-content Legal Hold conformance

Date: 2026-08-27

Status: Repository conformance implemented; validation and fixed-point review
complete with no unresolved P0-P2 finding; implementation commit pending.

This slice adds the repository authority that can extend the retention of
non-content operational metadata and financial records beyond their ordinary
365-day and 2557-day deadlines. It advances ADRs 0005, 0006, 0007, 0013, and
0015. It does not alter Customer Content retention, delay Content Deletion, or
create a Production Gate Launch Receipt.

## Governing boundary

1. A Legal Hold covers only `METADATA`, `FINANCIAL`, or both. Prompt, input,
   Artifact, incomplete upload, debug dump, Worker scratch, and Local Recovery
   State are Customer Content and cannot be selected or preserved by this
   interface.
2. A hold targets exactly one Customer Organization, Project, or Job. Project
   and Job targets carry their complete ancestor identity and are validated
   against PostgreSQL composite ownership.
3. No Human Principal role, Service Principal scope, Platform Operator
   Break-glass grant, Finance Principal, or existing runtime role can place,
   release, or mutate a hold.
4. An independently provisioned Compliance Principal submits immutable events
   over a dedicated mutual-TLS listener and a dedicated PostgreSQL login.
5. A hold extends retention only while ACTIVE. Release is one-way and never
   deletes evidence or retroactively claims that already-expired records were
   preserved.
6. Slice 34 record-expiry authority must lock and test the applicable active
   hold in the same PostgreSQL transaction that expires a record. Slice 33
   establishes that query and lock contract but does not implement record
   expiry.

## Public seams

Tests observe behavior through:

1. `legalhold.Service.Apply` using the dedicated PostgreSQL runtime login and
   its public security-definer functions;
2. the dedicated Compliance HTTPS handler over real TLS 1.3 mutual
   authentication with exact URI identity matching; and
3. `vela-control` configuration, role verification, listener lifecycle,
   readiness, and bounded shutdown.

Database inspection is limited to evidence, state, privileges, constraints,
locking, and migration compatibility.

## Identity and authorization

Migration 00030 adds provisioned `compliance_principals` and
`compliance_database_bindings`. A binding maps one PostgreSQL LOGIN role to one
ACTIVE Principal with an exact client-certificate URI identity. Provisioning
and permanent disablement remain deployment operations; the runtime cannot
create, enumerate, or modify Principals or bindings.

`vela_compliance` is NOLOGIN, non-superuser, non-BYPASSRLS, owns no object, and
can execute exactly:

```text
vela_get_compliance_identity()
vela_apply_legal_hold_event(...)
```

Both functions resolve the Principal from `session_user`; callers cannot
supply the audit Principal. The independent `vela_compliance_owner` is
NOLOGIN/BYPASSRLS and is inherited by no login or runtime role. Neither role
can read or mutate Customer Content, Jobs, Attempts, Charges, Credentials,
Finance Reconciliation state, or existing retention evidence except through
the declared functions.

## Legal Hold model

Each hold freezes:

- `hold_id`;
- scope and exact Organization/Project/Job identity;
- a canonical non-empty set of `METADATA` and/or `FINANCIAL` record classes;
- bounded external reason code and placement reference;
- external effective timestamp, Compliance Principal, source sequence, and
  PostgreSQL placement time.

An ACTIVE hold may transition once to RELEASED. Release records a distinct
bounded external reason code/reference, external effective timestamp,
Compliance Principal, source sequence, and PostgreSQL release time. Hold
identity, target, classes, placement evidence, and release evidence are
immutable. An ACTIVE hold cannot be deleted; a RELEASED hold cannot be changed
or reactivated.

The scope shape is exact:

| Scope | Required identity | Forbidden identity |
| --- | --- | --- |
| `ORGANIZATION` | `organization_id` | `project_id`, `job_id` |
| `PROJECT` | `organization_id`, `project_id` | `job_id` |
| `JOB` | `organization_id`, `project_id`, `job_id` | none |

An active hold applies to a record when its class matches and its target is the
record's exact Job, ancestor Project, or ancestor Customer Organization.
`vela_private.lock_active_non_content_legal_holds(...)` takes row locks and
returns the matching hold IDs in stable order. Future expiry code must call it
after locking its candidate record and before committing expiry.

## Event contract

The dedicated listener accepts:

```http
POST /internal/v1/compliance/legal-hold-events
Content-Type: application/json
Accept: application/json
```

The body is one strict JSON document no larger than 64 KiB. Placement uses:

```json
{
  "idempotency_key": "compliance-feed immutable key",
  "source_sequence": 1,
  "hold_id": "uuid",
  "kind": "HOLD_PLACED",
  "scope": "JOB",
  "organization_id": "uuid",
  "project_id": "uuid",
  "job_id": "uuid",
  "record_classes": ["METADATA", "FINANCIAL"],
  "reason_code": "LITIGATION",
  "external_reference": "matter-2026-001/place",
  "effective_at": "2026-08-27T12:00:00Z"
}
```

Release uses the same envelope with `kind=HOLD_RELEASED`, the original
`hold_id`, a release reason/reference/effective time, and no scope, target, or
record classes. Null does not substitute for an omitted forbidden field.

Strings are bounded, trimmed, and control-character free. Reason codes match
`[A-Z][A-Z0-9_]{0,99}`. UUIDs are canonical values and timestamps must be
exactly representable at PostgreSQL microsecond precision. Placement classes
are unique and canonicalized to `METADATA`, `FINANCIAL` order.

Each Compliance Principal owns one contiguous source sequence beginning at 1.
Every accepted event freezes the idempotency key, sequence, input, Principal,
and PostgreSQL record time. An exact retry returns the original event with
`replayed=true`; reuse of a key, sequence, hold ID, placement reference, or
release reference for different input fails with no state change. A gap or
late unseen sequence also fails. Concurrent place/release operations serialize
on the Principal cursor and hold row.

Placement requires the exact target to exist. Release requires the hold to be
ACTIVE, its external effective time not precede the placement effective time,
and its record time not precede placement. An already RELEASED hold accepts
only the exact original event replay.

The response contains only event ID, replay flag, hold ID, state, scope,
record classes, and PostgreSQL recorded/released timestamps. It never contains
Customer Content, database identity, TLS material, raw database errors, or
another Principal's cursor.

## HTTP and TLS behavior

A first accepted event returns `201`; an exact replay returns `200`.
Authentication runs before method or body processing. The listener returns
bounded JSON errors:

- `400` malformed or semantically invalid input;
- `401` missing, mismatched, additional, disabled, or unbound identity;
- `404` unknown target or hold;
- `409` ordering, replay, reference, state, or ownership conflict;
- `415` non-JSON media type;
- `500/503` unexpected internal or database failure.

Only POST is accepted. TLS requires version 1.3, the configured client CA, and
exactly one verified leaf URI equal to the database-bound identity.

## Runtime configuration

`vela-control` requires:

- `VELA_COMPLIANCE_DATABASE_URL`;
- `VELA_COMPLIANCE_ADDR`;
- `VELA_COMPLIANCE_SERVER_CERT_FILE`;
- `VELA_COMPLIANCE_SERVER_KEY_FILE`; and
- `VELA_COMPLIANCE_CLIENT_CA_FILE`.

Startup verifies the exact database role and active Principal before binding a
concrete host/port. Main readiness pings the Compliance pool. Listener failure
cancels the process. Shutdown closes the Compliance listener before database
pools and is bounded by the existing shutdown deadline.

## Compatibility

Released migrations 00001-00029 remain byte-identical. Migration 00030 is
additive and grants no new privilege to an existing role. The exact Slice 32
binary at `c08ba84fc5cfc88e9e8de9e0e06a23725c1521e8` remains valid on schema 30
without Compliance configuration. The current binary fails closed on schema 29
because its role and identity functions are absent.

Down is allowed only when Legal Hold events, holds, cursors, Principals, and
bindings are empty. Any durable evidence or provisioning rejects structural
Down with SQLSTATE `55000`. Empty Down/Up restores the same behavior and
privileges.

## Required evidence

- Organization, Project, and Job placements match only their exact descendant
  records and class set;
- release is one-way, exact replay is effect-free, and conflicting replay,
  sequence gap, duplicate reference, invalid target shape, and release-before-
  placement fail with no state change;
- concurrent place/release calls serialize and preserve one ordered event
  stream;
- the active-hold lock blocks a concurrent release until the checking
  transaction commits;
- the Compliance role has only the two public functions, no direct table or
  Customer Content access, no owner inheritance, and no existing role can
  invoke Compliance mutation authority;
- missing, disabled, unbound, and cross-login identities fail closed;
- strict HTTP parsing and real mutual TLS enforce the declared interface with
  secret-safe failures;
- empty migration Down/Up succeeds, durable evidence/provisioning refuses Down,
  exact N-1 control remains compatible, and current control fails closed on
  schema 29;
- generation, focused unit/integration, race, lint, full tests, deployment
  validation, and two-axis review complete without an unresolved P0-P2 issue.

## Explicitly deferred

Slice 33 does not expire metadata or financial records; Slice 34 consumes this
hold contract while implementing those lifecycles. It also does not provision
production Compliance Principals/certificates, define an external case-management
system, prove production network isolation, preserve Customer Content, or create
a Launch Receipt. Production Gates remain `0/9 PASS`.

## Completion boundary

Slice 33 is complete only when every public seam and required invariant above
is implemented, tested, reviewed, and committed. It advances the Legal Hold
portion of ADR 0015 but does not mark ADR 0015 complete before Slice 34 and the
remaining external evidence gates are satisfied.
