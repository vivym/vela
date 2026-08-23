# Organization Billing, Audit, And Settlement Contacts

Status: Implemented

Date: 2026-08-24

Fixed point: `1ce496ce06c4ba33038be91a5fd5f7be502bee85`

Predecessors:

- `docs/specs/0009-invoice-export-and-receipt.md`
- `docs/specs/0012-human-oidc-and-fixed-rbac.md`
- `docs/specs/0013-project-service-principal-credential-administration.md`
- `docs/specs/0014-human-membership-and-fixed-role-administration.md`

## Goal

Give the fixed `BillingAdmin` and `OrganizationAuditor` roles their first
positive Organization-scoped capabilities without granting Customer Content
access. A `BillingAdmin` can inspect the exact Customer Organization credit
account, immutable Charges and available external Invoice references, and can
create, list, and permanently disable settlement contacts. An
`OrganizationAuditor` can inspect bounded non-content usage and the immutable
Human/Service identity and settlement-contact administration evidence that the
repository currently records. An `OrganizationOwner` can perform both sets of
operations.

Every request is authorized by an exact short-lived Human Organization session
and revalidated inside the database transaction. Settlement-contact mutations
write immutable evidence with exact Human Principal/session attribution. None of
the responses contain prompts, client metadata, Artifact keys or URLs, OIDC
token material, Credential digests, Webhook secrets, or raw Invoice exporter
errors.

This slice supplies positive fixed-role behavior required by ADR 0006 and
advances Organization-wide billing and audit visibility. It does not claim a
settlement/credit-adjustment reconciliation ledger, a complete all-subsystem
audit stream, Break-glass Access, or any Production Gate.

## Governing Decisions

- ADR 0001/0026: PostgreSQL credit, CreditReservation, Charge, and Invoice export
  receipt state remains authoritative. Reporting cannot mutate the Contract
  Credit Limit, reservation, Charge, export authority, or receipt.
- ADR 0004/0007: Customer Organization is the reporting and isolation boundary.
  Every Project aggregate and Charge is joined through exact composite
  Organization ownership and cross-Organization substitution is invisible.
- ADR 0005/0006: only active OIDC-backed Human Principals with the fixed current
  Organization role may use these interfaces. A Service Principal never
  inherits a Human reporting role.
- ADR 0008: reporting returns non-content facts only. Reading a Job row for
  aggregation never exposes `request_content`, object identity, or signed URLs.
- ADR 0013: migration 00015 is additive and the exact fixed-point control binary
  remains valid on the expanded schema.
- ADR 0029: repository tests are evidence for this slice, not Launch Receipts.

## In Scope

1. Return an exact Organization credit summary: currency, Contract Credit
   Limit, reserved credit, unsettled posted Charges, derived available credit,
   ledger version, and update timestamp.
2. List up to 100 immutable Charges in reverse `(posted_at, charge_id)` order,
   including exact Project, Job, reason, amount, currency, and optional external
   Invoice/line references plus export time. Export claim state, retries, and
   `last_error` are not customer-visible.
3. Create, list, and permanently disable settlement contacts. A contact has a
   server-generated id, trimmed display name, normalized email, creation
   evidence, and optional disablement evidence. Creating the same normalized
   email replays the committed contact and does not duplicate audit history.
4. Return a bounded non-content usage summary for an explicit UTC interval:
   total Jobs and counts by current Job state, total quoted amount, total posted
   Charge amount, and the same aggregates per Project. No Job request or
   Artifact field is selected or returned.
5. List up to 100 Organization audit events in reverse `(created_at, event_id)`
   order across `human_identity_events`, `project_identity_events`, and the new
   settlement-contact events. The safe projection contains source, action,
   optional Project, actor Principal/session, target kind/id, and timestamp; it
   deliberately omits event `details`.
6. Add exact Organization scopes:
   `organization_billing:read`, `organization_billing_contacts:read`,
   `organization_billing_contacts:manage`, `organization_usage:read`, and
   `organization_audit:read`.
7. Grant `BillingAdmin` billing/contact/usage scopes,
   `OrganizationAuditor` audit/usage scopes, and `OrganizationOwner` all five.
   None receives a Project Customer Content scope through an Organization role.
8. Add separate `vela_organization_billing_request` and
   `vela_organization_audit_request` database roles and production pools. The
   former can execute only billing/contact/usage functions; the latter only
   audit/usage functions. Neither role has direct table access.
9. Preserve the exact allowlists of `vela_human_membership_request`,
   `vela_identity_request`, `vela_request`, `vela_artifact_request`, and
   `vela_billing`. Preserve Service/Human authentication, all existing public
   responses, migration Down/Up, and exact N-1 startup/request behavior.

## Explicitly Deferred

- Contract Credit Limit mutation, settlement posting, credit adjustments,
  refunds, payment collection, and external Invoice state beyond immutable
  exporter references.
- Project/Organization creation, rename, suspension, closure, quota, and policy
  administration.
- Artifact access audit, Content Deletion audit, Webhook administration history,
  billing-ledger mutation events, node remediation audit, and one canonical
  stream spanning every subsystem. Each requires its owning later slice to
  persist complete source evidence rather than reconstructing missing history.
- Platform Operator identity and Break-glass request, approval, activation,
  expiry, revocation, content access, and Launch Receipts.
- CSV/PDF statements, tax records, legal entity data, mailing addresses, phone
  numbers, multiple currencies per Customer Organization, and external ERP/CRM
  contact synchronization.
- Production IdP, Invoice endpoint, deployment-isolation, NATS-identity, or any
  of the nine Production Gate receipts.

## Public Seams

The architecture-agreed TDD seams are:

1. generated REST/OpenAPI operations under
   `/v1/organizations/{organization_id}/billing`,
   `/v1/organizations/{organization_id}/usage`, and
   `/v1/organizations/{organization_id}/audit-events`;
2. `identity.Authenticator.Authenticate` and `Principal.ForOrganization` for the
   exact short-lived Human Organization authorization;
3. a dedicated `organizationreporting.Service` for billing, contacts, usage,
   and safe audit projections;
4. `vela_set_organization_identity_admin_context` as the transaction-time
   Human-only scope boundary and narrow reporting functions as the only database
   read/mutation surface;
5. migration 00015 constraints, immutable contact evidence, role allowlists,
   structural Down refusal, and exact fixed-point N-1 compatibility;
6. `vela-control` independent billing/audit request pools, readiness, and current
   production HTTP caller wiring.

Production wiring requires two independently verified pools:

- `VELA_ORGANIZATION_BILLING_REQUEST_DATABASE_URL` connects through a login
  whose only application-role membership is
  `vela_organization_billing_request`;
- `VELA_ORGANIZATION_AUDIT_REQUEST_DATABASE_URL` connects through a login whose
  only application-role membership is `vela_organization_audit_request`.

Tests observe behavior through REST or `organizationreporting.Service`.
Database inspection is limited to proving transaction-time authorization,
isolation, immutable evidence, least privilege, migration, and no-content/no-
mutation invariants.

## REST Contract

The slice adds these operations:

```text
GET  /v1/organizations/{organization_id}/billing/credit
GET  /v1/organizations/{organization_id}/billing/charges
POST /v1/organizations/{organization_id}/billing/settlement-contacts
GET  /v1/organizations/{organization_id}/billing/settlement-contacts
POST /v1/organizations/{organization_id}/billing/settlement-contacts/{contact_id}/disable
GET  /v1/organizations/{organization_id}/usage
GET  /v1/organizations/{organization_id}/audit-events
```

Charge, contact, and audit lists accept `limit` from 1 to 100. Usage requires
`from` and `to` RFC 3339 timestamps with `from < to` and an interval no greater
than 366 days. The interval is half-open `[from, to)` and aggregates Jobs by
`created_at` and Charges by the joined Job population, so one Job contributes at
most one posted Charge.

Settlement-contact creation accepts `display_name` and `email`. The trimmed
display name contains 1 to 200 Unicode characters. Email contains 3 to 320
bytes, has no surrounding or embedded whitespace, contains one non-edge `@`,
and is normalized to lowercase for identity and replay. The API returns only the
normalized address. Disablement is permanent and idempotent.

An authenticated Human with no selected Organization authorization receives
403. An expired, revoked-role, disabled, mismatched-proof, Service, or unknown
session fails at the database boundary without returning data or mutation. An
unknown or cross-Organization resource is not visible and returns 404.

## Authorization Contract

- `OrganizationOwner` may use every operation in this slice but gains no
  Customer Content permission.
- `BillingAdmin` may read credit, Charges/Invoice references, contacts, and
  usage, and may create/disable contacts. It cannot read the Organization audit
  stream unless it independently holds `OrganizationAuditor`.
- `OrganizationAuditor` may read usage and the safe audit stream. It cannot read
  credit, Charge/Invoice references, or settlement contacts unless it
  independently holds `BillingAdmin`.
- Fixed Organization permissions union for a Human Principal. Removing the role
  after authentication but before the transaction makes the operation fail
  closed.
- `ProjectAdmin`, `Developer`, and `ProjectViewer` Project sessions do not
  satisfy any Organization reporting scope. Service Principals, including ones
  with similarly named arbitrary scopes, cannot use these Human interfaces.

## Persistence And Audit Contract

- `organization_settlement_contacts` is Organization-owned. Contact identity,
  email, creator, and creation time are immutable; `disabled_at` and
  `disabled_by_principal_id` transition from NULL exactly once.
- Contact create and disable events commit in the same transaction as the
  mutation and reference the exact Human Principal and Organization session.
  Replays return the committed state without a second event.
- Contact events are immutable and contain no email, token, proof, Credential,
  Invoice exporter error, or Customer Content. The contact id is the target.
- Billing/reporting functions return projections only and cannot update credit,
  Job, Charge, Invoice export, receipt, or identity evidence.
- Available credit is checked in PostgreSQL as
  `contract_credit_limit_minor - reserved_minor - unsettled_posted_minor` and
  cannot be negative under the existing ledger constraint.
- Audit union ordering is deterministic. Source event ids remain canonical;
  this slice does not copy or rewrite predecessor evidence.
- Reporting request roles cannot directly read contacts, Human bindings,
  sessions/proofs, roles, Jobs, Charges, Invoice tables, identity events, or
  private contexts.

## Required Evidence

- each allowed fixed role receives exactly its declared positive capabilities,
  permissions union correctly, and every other Human/Service actor fails closed;
- credit and Charge/Invoice projections match committed ledger facts without
  exposing export errors or mutating any billing authority;
- usage totals and per-Project totals agree over boundary timestamps and mixed
  Job states, without selecting or returning request/Artifact content;
- contact create/replay/list/disable/replay is deterministic, normalized,
  permanently disabled, and writes exactly one immutable event per transition;
- the audit stream returns Human membership/role, Service Principal/Credential,
  and contact events with exact actor/session attribution and no `details` or
  content-bearing field;
- role removal, session expiry, Human disablement, proof substitution,
  cross-Organization ids, malformed email/name/time/limit, and direct SQL
  privilege attempts cause no data disclosure or mutation;
- direct mutation of contact identity/evidence and all reporting attempts to
  mutate credit, Charge, Invoice, Job, or predecessor audit facts are rejected;
- existing Human/Service authentication, identity administration, Admission,
  Artifact, cancellation, billing exporter, and Webhook paths remain unchanged;
- empty migration Down/Up restores the prior role/scope surface, while durable
  settlement-contact evidence refuses structural Down;
- exact fixed-point N-1 startup and Service/Human requests remain valid on
  migration 00015;
- repeated generation, lint, unit, integration, race, Protobuf/OpenAPI breaking
  checks, and two-axis review pass with no unresolved P0-P2 finding.

## Completion Boundary

This slice is complete only when every public seam is on the production caller
path and all required evidence passes. Settlement/credit adjustments, full
cross-subsystem audit, Project/Organization policy, Break-glass, deployment
receipts, and all nine Production Gates remain separate and unclaimed.
