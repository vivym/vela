# Settlement And Credit-adjustment Reconciliation

Date: 2026-08-24

Status: Planned

Predecessors:

- `docs/specs/0001-admission-control-plane-foundation.md`
- `docs/specs/0006-customer-cancellation-and-charge.md`
- `docs/specs/0009-invoice-export-and-receipt.md`
- `docs/specs/0015-organization-billing-audit-and-settlement-contacts.md`

## Goal

Accept authenticated external finance facts as immutable, idempotent reconciliation
records and atomically apply their exact effect to a Customer Organization's
Contract Credit Ledger. Settlement, credit adjustment, and Contract Credit Limit
changes may alter available credit, but they never rewrite a Charge, Job,
Artifact, Invoice export authority, or Invoice receipt.

## Governing Decisions

- ADR 0001: Vela owns Contract Credit Limit, Credit Reservation, Charge, and the
  local reconciliation record. Collection, payment, Invoice lifecycle, and the
  commercial reason for a credit note remain external finance facts.
- ADR 0003, 0026, and 0027: reconciliation cannot alter the transaction or winner
  that created a Charge and cannot gate Artifact access.
- ADR 0006: `BillingAdmin` and every other Customer Principal remain unable to
  mutate the ledger. This slice adds no Customer write endpoint or scope.
- ADR 0007: a dedicated finance runtime login has no table privileges and can
  execute only the identity and posting functions defined here.
- ADR 0013: migration 00018 is additive. The exact `afe83d1` N-1 control binary
  continues to use all existing roles and schemas without receiving a new
  privilege. The current binary fails closed against schema 17.
- ADR 0029: repository tests are implementation evidence, not Launch Receipts.
  Production remains `0/9 PASS`.

## Public Seams

Tests observe behavior only through these production boundaries:

1. `financereconciliation.Service.Apply` using the dedicated PostgreSQL runtime
   pool and its public security-definer functions;
2. the dedicated finance HTTPS handler over real mutual TLS, including exact URI
   identity matching and strict request/response behavior;
3. `vela-control` configuration, role verification, listener lifecycle,
   readiness, and clean shutdown wiring.

Database inspection is limited to proving ledger effects, immutable evidence,
least privilege, migration compatibility, and absence of changes to Charge, Job,
Artifact, and Invoice history.

## Finance Principal And Transport Boundary

Migration 00018 creates a provisioned `finance_reconciliation_principals` row
with a stable UUID, bounded stable id, exact client-certificate URI identity,
and ACTIVE or DISABLED status. A separate
`finance_reconciliation_database_bindings` row binds one PostgreSQL LOGIN role
to that Principal. Provisioning and disablement require a privileged deployment
procedure; the Vela runtime cannot create, modify, or list Principals or bindings.

The `vela_finance_reconciliation` runtime role is NOLOGIN, does not bypass RLS,
has no direct table, sequence, or private-schema access, and can execute exactly:

```text
vela_get_finance_reconciliation_identity()
vela_apply_finance_reconciliation(...)
```

Both functions resolve the Principal from PostgreSQL `session_user`; callers
cannot supply or override the audit Principal. A missing, ambiguous, or disabled
binding fails before a record is accepted. The security-definer functions are
owned by the independent NOLOGIN/BYPASSRLS
`vela_finance_reconciliation_owner`, whose role is inherited by no runtime login.

`vela-control` exposes finance writes only on a separate TLS 1.3 listener. The
server requires a client certificate chaining to the configured finance client
CA. The verified leaf must contain exactly the URI identity returned by the
database binding; an absent, additional, or different URI fails authentication.
The Customer HTTP listener, Human/Service authentication, and Customer OpenAPI
do not route this endpoint.

## Reconciliation Contract

The internal finance listener accepts:

```http
POST /internal/v1/finance/reconciliations
Content-Type: application/json
Accept: application/json
```

The body is one strict JSON document no larger than 64 KiB:

```json
{
  "idempotency_key": "finance-feed immutable key",
  "source_sequence": 1,
  "organization_id": "uuid",
  "kind": "SETTLEMENT_POSTED",
  "currency": "CNY",
  "settlement_minor": 1250,
  "external_reference": "payment or credit-system reference",
  "effective_at": "2026-08-24T12:00:00Z"
}
```

The three kinds have disjoint amount fields:

| Kind | Required value | Ledger effect |
| --- | --- | --- |
| `SETTLEMENT_POSTED` | `settlement_minor > 0` | subtract from `unsettled_posted_minor` |
| `CREDIT_ADJUSTMENT_POSTED` | signed, non-zero `credit_adjustment_minor` | positive credit subtracts from unsettled; negative reversal/debit adds to unsettled |
| `CONTRACT_CREDIT_LIMIT_CHANGED` | `contract_credit_limit_minor >= 0` | replace the absolute Contract Credit Limit |

Exactly the field named for the kind is present; the other two are absent. UUID,
currency, time, idempotency key, and external reference are mandatory and
bounded. Timestamps are normalized to UTC. Strings have no surrounding or
control whitespace. Effective timestamps must be exactly representable at
PostgreSQL microsecond precision. JSON numbers must fit signed 64-bit integers.

Every accepted record preserves:

- Finance Principal, source sequence, idempotency key, and external reference;
- Customer Organization, kind, currency, amount, external effective time, and
  PostgreSQL posting time;
- Contract Credit Limit and unsettled posted amount before and after;
- active reservation amount at posting and account versions before and after.

Records are immutable and cannot be deleted. The account row is locked and the
record insert, account update, version increment, and source cursor advance
commit in one transaction.

## Ordering, Replay, And Conflict Rules

Each stable Finance Principal owns one contiguous feed beginning at sequence 1.
For a new record, `source_sequence` must equal the committed cursor plus one.
Gaps and late unseen records fail with conflict and leave no effect. This makes
constraint-sensitive debit reversals and Contract Credit Limit changes
deterministic instead of arrival-order dependent.

An exact retry with the same Principal and idempotency key returns the originally
committed record with `replayed=true`, even after later sequences. Replay compares
every external input, not only the amount. Reusing an idempotency key, source
sequence, or external reference for different input fails with conflict. A
timeout after commit is therefore safe to retry.

Settlement and positive credit adjustment cannot reduce unsettled posted credit
below zero. A negative credit adjustment or lower Contract Credit Limit cannot
make:

```text
reserved_minor + unsettled_posted_minor > contract_credit_limit_minor
```

Currency must equal the Customer Organization account currency. Unknown
Organizations, over-application, numeric overflow, mismatched currency, invalid
shape, conflict, or out-of-order input commits neither a record nor a cursor or
account change.

## HTTP Results

An accepted first application returns HTTP 201; an exact replay returns HTTP
200. Both return the immutable record id, replay flag, kind, Organization,
currency, after-values, account version, and posting time. The handler returns a
bounded JSON error with no database detail, certificate content, DSN, file path,
or Customer Content:

- 400 for malformed or semantically invalid input;
- 401 when mutual-TLS identity is absent or does not match the database-bound
  Finance Principal;
- 404 for an unknown Customer Organization;
- 409 for source ordering, key/reference conflict, or a ledger-state conflict;
- 415 for a non-JSON media type;
- 500/503 for an unexpected internal/database failure.

Only POST is accepted. Redirects are irrelevant because this is an ingress. The
response never contains the client certificate, database login, Contract Credit
Limit before-value, or another Finance Principal's data.

## Runtime Configuration

Required:

- `VELA_FINANCE_RECONCILIATION_DATABASE_URL`
- `VELA_FINANCE_RECONCILIATION_ADDR`
- `VELA_FINANCE_RECONCILIATION_SERVER_CERT_FILE`
- `VELA_FINANCE_RECONCILIATION_SERVER_KEY_FILE`
- `VELA_FINANCE_RECONCILIATION_CLIENT_CA_FILE`

The address must be a concrete host and port. TLS files are absolute, bounded,
regular files. The server key is never logged. Startup verifies the exact
database role boundary and resolves the active Finance Principal before binding
the listener. The main readiness endpoint pings the finance database pool but
does not require a live external client. Listener bind or serve failure cancels
the process; shutdown is bounded and closes the listener before database pools.

## Compatibility And Migration

Released migrations 00001-00017 remain byte-identical. Migration 00018 adds new
roles, types, tables, functions, and grants without changing an existing runtime
role's privileges. Existing Admission, Charge posting, Invoice export,
Organization reporting, and N-1 binaries ignore the new schema while observing
the atomically updated credit-account counters and version.

Down is allowed only when reconciliation records, source cursors, Principals, and
database bindings are empty. Durable finance evidence or provisioning refuses
structural Down. Empty Down/Up restores the same behavior and privileges.

## Required Evidence

- first settlement, positive/negative credit adjustment, and Contract Credit
  Limit change produce the worked ledger effects and exact immutable snapshots;
- exact replay is effect-free, while payload/key/sequence/reference conflict and
  gaps fail with no durable change;
- settlement/credit over-application, debit/limit constraint violation, currency
  mismatch, unknown Organization, overflow, malformed values, and concurrent
  posting fail closed;
- concurrent replicas serialize one sequence and one account effect; a timeout
  after commit can replay the exact record;
- disabled, missing, or cross-login Finance Principal bindings fail; the runtime
  role has only the two declared functions, no direct table access, no owner
  inheritance, and no Customer role can post;
- real TLS 1.3 accepts only the configured CA and exact URI identity and rejects
  anonymous, wrong-CA, wrong-URI, and additional-URI clients before application;
- strict HTTP method/media/body/JSON/error behavior and secret-safe failures;
- reconciliation leaves Charge, Job, Artifact, Invoice authority, and receipt
  history byte-for-byte/logically unchanged except the credit-account effect;
- empty migration Down/Up succeeds, durable evidence/provisioning refuses Down,
  exact `afe83d1` N-1 control and request paths remain compatible on schema 18,
  and current control fails closed on schema 17;
- generation, unit, race, lint, full integration, Protobuf/OpenAPI breaking,
  repeated generated-hash, and two-axis review complete with no P0-P2 finding.

## Explicitly Deferred

This slice does not collect payment, issue an Invoice or credit note, decide why
a commercial adjustment is valid, expose a Customer ledger-mutation API, create
Finance Principals at runtime, supply production certificates, deploy network
policy, or produce a Launch Receipt. Finance Principal provisioning, certificate
issuance/rotation, endpoint conformance, alerting, and deployment isolation need
their own versioned operational evidence.

## Completion Boundary

Slice 19 is complete only when all three public seams are on the production path,
every required ledger/authentication/compatibility behavior passes, and both
Standards and Spec review have no unresolved P0-P2 finding. Completion closes the
repository-verifiable settlement, credit-adjustment, and Contract Credit Limit
portion of ADR 0001. It does not change Production Gates from `0/9 PASS`.
