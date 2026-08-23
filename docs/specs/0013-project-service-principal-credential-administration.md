# Project Service Principal And Credential Administration

Status: Implemented

Date: 2026-08-23

Fixed point: `395887177ec9fb5f703eac055b04c02f2086fa8b`

Predecessors:

- `docs/specs/0001-admission-control-plane-foundation.md`
- `docs/specs/0011-project-webhook-delivery.md`
- `docs/specs/0012-human-oidc-and-fixed-rbac.md`

## Goal

Allow a Human `ProjectAdmin` to create, inspect, disable, rotate, and revoke
Project-owned Service Principal credentials without weakening Project or
Organization Isolation. Every successful mutation is attributed to the exact
Human Principal and short-lived Human authorization session, while every
Credential remains attributable to its stable Service Principal after rotation,
revocation, or disablement.

This slice completes the Project Service Principal and Credential management
surface deferred by Slice 12. Human membership and role-assignment
administration, Organization administration, and Platform Operator Break-glass
Access remain later slices, so ADR 0005, ADR 0006, ADR 0007, and Acceptance
Scenario 29 remain partial.

## Governing Decisions

- ADR 0004/0007: every operation is bound to one exact Project and Organization
  by transaction-revalidated Human authorization and a dedicated restricted
  database role.
- ADR 0005: Service Principals are stable identities; their Credentials overlap
  for rotation, expire, and revoke independently. Vela stores only a keyed
  digest and returns newly generated bearer material once.
- ADR 0006: only `ProjectAdmin` receives Service Principal administration
  permissions. Service Principal scopes never grant administrative authority.
- ADR 0013: migration 00013 is additive and the exact fixed-point control binary
  remains valid on the expanded schema.
- ADR 0029: repository tests are not Launch Receipts and do not advance any
  Production Gate.

## In Scope

1. Create and list Project-owned Service Principals through the public REST API.
2. Permanently disable a Service Principal and atomically revoke all active
   Credentials belonging to it.
3. Issue and list Credentials for an active Service Principal. A newly issued
   bearer Credential is generated with operating-system randomness, returned
   once, never logged, and never persisted in plaintext or reversible form.
4. Rotate by issuing an overlapping Credential before revoking the old one.
   Revocation is permanent and idempotent.
5. Restrict Service Credential scopes to the existing Project data-plane API
   scope vocabulary. Administrative scopes cannot be assigned to a Service
   Principal.
6. Revalidate the Human session, current `ProjectAdmin` role, exact Project, and
   required permission inside every database transaction.
7. Record an immutable audit event for each successful create, issue, revoke,
   and disable transition, including the exact Human Principal and Human session
   plus safe transition facts but no Credential digest or bearer material.
8. Use a dedicated `vela_identity_request` database role that can execute only
   the identity-administration context and domain functions and cannot read
   identity tables, request contexts, Customer Content, billing, or credentials.
9. Preserve exact existing Service and Human authentication responses and the
   fixed-point N-1 Service request behavior.
10. Migration 00013 Down/Up, generated-code fixed point, lint, unit,
    integration, race, and two-axis review evidence.

## Explicitly Deferred

- Human invitations, OIDC binding creation/disable APIs, Organization and
  Project role assignment/removal, Project creation, and Organization policy.
- Organization-level Service Principals or customer-defined Service scopes.
- Credential escrow, plaintext recovery, refresh tokens, local Human passwords,
  or a shared Customer Organization master Credential.
- Billing and audit read APIs. The durable identity event table is evidence for
  a later audited projection, not a customer-facing audit endpoint in this
  slice.
- Platform Operator identity and Break-glass request, approval, activation,
  expiry, revocation, content access, and Launch Receipts.

## Public Seams

The architecture-agreed seams used for test-first development are:

1. REST/OpenAPI operations under
   `/v1/projects/{project_id}/service-principals`;
2. `identity.AdministrationService` for Project Service Principal and Credential
   lifecycle commands and safe list projections;
3. `vela_set_identity_admin_context(uuid, bytea, text)` as the Human-only,
   transaction-time authorization boundary;
4. migration 00013 constraints, immutable audit evidence, restricted database
   role, structural Down refusal, and exact fixed-point N-1 compatibility;
5. `vela-control` configuration and production caller wiring.

Tests exercise the public HTTP or service operation. Database inspection is
limited to proving atomic authorization, digest-only storage, audit,
Organization Isolation, privilege, migration, and no-mutation invariants.

## REST Contract

The slice adds these Project-scoped operations:

```text
POST /v1/projects/{project_id}/service-principals
GET  /v1/projects/{project_id}/service-principals
POST /v1/projects/{project_id}/service-principals/{service_principal_id}/disable
POST /v1/projects/{project_id}/service-principals/{service_principal_id}/credentials
GET  /v1/projects/{project_id}/service-principals/{service_principal_id}/credentials
POST /v1/projects/{project_id}/service-principals/{service_principal_id}/credentials/{credential_id}/revoke
```

Create requires a display name. Credential issue requires a non-empty unique
set of allowed scopes and an expiry strictly in the future and no more than 366
days away. Lists are bounded and expose no bearer secret or digest. A disable or
revoke replay returns the already committed safe resource state. Cross-Project
resources are not visible.

Only a Human `ProjectAdmin` may call these operations. A Service Principal,
`Developer`, `ProjectViewer`, or Organization role without an explicit
`ProjectAdmin` binding receives no administration permission.

## Credential Contract

The returned bearer form remains `vla_<credential-id>.<base64url-secret>`, with
32 random secret bytes. Vela stores `HMAC-SHA256(credential_pepper, secret)` and
clears temporary secret material where possible. List, revoke, disable, audit,
log, and error paths expose neither the bearer value nor its digest.

The allowed Service scopes are `jobs:submit`, `jobs:read`, `jobs:cancel`,
`artifacts:read`, `webhooks:manage`, and `webhooks:read`. Duplicates, unknown
scopes, empty sets, and Human administration permissions are rejected in both
the application and PostgreSQL boundary.

## Database And Audit Invariants

- A Service Principal remains permanently bound to one Project and Customer
  Organization.
- `disabled_at` and `revoked_at` are one-way transitions; neither can be cleared
  or moved after it is set.
- A disabled Service Principal cannot authenticate, receive a Credential, or
  regain active state. Disablement and revocation serialize with Credential
  issue.
- Every Credential references its stable Service Principal and exact Project.
- Every successful mutation writes exactly one immutable identity audit event
  in the same transaction. Failed authorization or validation writes nothing.
- Audit actor identity references an attributed Human session; target identity
  references the durable Project Principal attribution.
- `vela_identity_request` has no direct table privilege and cannot use Service
  Credential administration authority.
- Forced RLS remains enabled on every identity, session, attribution, and audit
  relation even though narrow domain functions own the mutation surface.

## Required Evidence

- `ProjectAdmin` can create/list a Service Principal, issue/list overlapping
  Credentials, revoke one without affecting another, and disable the Principal;
- a newly issued bearer authenticates on its granted existing API scopes, while
  wrong, expired, revoked, disabled, or cross-Project Credentials fail closed;
- non-`ProjectAdmin` Human roles and every Service Credential cannot administer
  Service Principals even if a legacy row contains an administrative scope;
- role removal after HTTP authentication but before the administration
  transaction is rejected by PostgreSQL with no mutation;
- raw bearer material and the random secret are absent from all persisted rows
  and safe projections, while the stored digest authenticates the returned
  bearer;
- scope, expiry, immutable transition, Project/Organization ownership, and
  concurrent issue/disable constraints hold under direct negative tests;
- successful events are immutable, exact-session attributed, and contain no
  secret or digest; failed/replayed transitions do not duplicate event history;
- the dedicated role passes an exact privilege audit and cannot directly read
  protected relations or execute unrelated functions;
- empty migration Down/Up restores the previous surface, while durable identity
  administration evidence refuses structural Down;
- exact fixed-point N-1 startup and Service requests remain valid on migration
  00013;
- stable repeated generation, lint, unit, integration, race, and two-axis review
  pass with no unresolved P0-P2 finding.

## Completion Boundary

This slice is complete only when every public seam is on the production caller
path and all required evidence passes. Human membership and role administration,
Break-glass, production IdP/deployment receipts, and all nine Production Gates
remain separate and unclaimed.
