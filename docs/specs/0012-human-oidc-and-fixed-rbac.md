# Human OIDC And Fixed RBAC

Status: Implemented

Date: 2026-08-23

Predecessors:

- `docs/specs/0001-admission-control-plane-foundation.md`
- `docs/specs/0006-customer-cancellation-and-charge.md`
- `docs/specs/0007-artifact-finalization-and-visible-completion.md`
- `docs/specs/0011-project-webhook-delivery.md`

## Goal

Authenticate Human Principals only through one configured enterprise OIDC
issuer and enforce Vela's six fixed launch roles on the existing Project API.
PostgreSQL revalidates the exact Human Principal, Project membership, token
session, and required permission in every request transaction so an application
check, stale token, or removed role cannot bypass Organization Isolation.

This slice establishes the Human authentication and authorization foundation.
It does not create administrative membership or Service Principal lifecycle
APIs and does not implement Platform Operator Break-glass Access; those require
later auditable management slices and therefore ADR 0005, ADR 0006, ADR 0007,
and Acceptance Scenario 29 remain partial after this slice.

## Governing Decisions

- ADR 0004/0007: Customer Organization remains the hard isolation boundary and
  Project remains the operational/content boundary. A Human role never removes
  composite ownership, forced RLS, or transaction-local identity enforcement.
- ADR 0005: Human Principals use external OIDC bindings and never receive a
  local password, shared Organization credential, or Service Principal secret.
- ADR 0006: the only Human roles are `OrganizationOwner`, `BillingAdmin`,
  `OrganizationAuditor`, `ProjectAdmin`, `Developer`, and `ProjectViewer`.
- ADR 0008: Organization billing/audit roles and Project administration do not
  imply prompt or Artifact access.
- ADR 0013: migration 00012 is additive and the exact `cedd0e8` N-1 control
  binary remains valid on the expanded schema.
- ADR 0029: repository tests are not Organization Isolation or identity Launch
  Receipts and do not advance any Production Gate.

## In Scope

1. A production OIDC token verifier backed by the issuer's JWKS endpoint. It
   validates signature, issuer, audience, expiry, and a non-empty subject and
   accepts no unsigned token or caller-provided identity override.
2. An immutable Human OIDC binding from exact `(issuer, subject)` to one HUMAN
   Principal in one Customer Organization. Vela stores the external subject and
   optional display metadata, but no human password or OIDC bearer token.
3. Fixed Organization and Project role assignments with database constraints
   that reject SERVICE Principals, cross-Organization membership, unknown
   roles, and cross-Project substitution.
4. A short-lived per-request Human authorization session stored only as a
   keyed token proof, never the raw OIDC token. Its expiry is bounded by both
   the verified token expiry and a five-minute server ceiling.
5. Human permission evaluation on the existing Project APIs:

   | Role | Project permissions in this slice |
   | --- | --- |
   | `OrganizationOwner` | None without an explicit Project role |
   | `BillingAdmin` | None; no prompt or Artifact access |
   | `OrganizationAuditor` | None; no prompt or Artifact access |
   | `ProjectAdmin` | `webhooks:manage`, `webhooks:read` |
   | `Developer` | `jobs:submit`, `jobs:read`, `jobs:cancel`, `artifacts:read` |
   | `ProjectViewer` | `jobs:read`, `artifacts:read` |

6. A Human Principal may hold multiple fixed roles; effective permissions are
   the union. Organization roles never synthesize Project content permission.
7. HTTP authorization selects one exact Project authorization before invoking
   a domain service. Service Principal scope behavior and Project binding remain
   unchanged.
8. `vela_set_request_context(uuid, bytea, text)` remains the common request-role
   entry point for N-1 compatibility. It accepts either an active Service
   Principal Credential or an active Human authorization session, records the
   actor kind, and refuses identity or permission changes in one transaction.
9. Human membership, OIDC binding, and session validity are rechecked inside
   the same PostgreSQL transaction that accesses customer data. Revocation,
   role removal, expiry, and Organization/Project mismatch therefore fail
   closed after HTTP authentication and before domain mutation or reads.
10. `vela-control` requires explicit OIDC issuer, audience, and JWKS URL
    configuration for the current binary and uses the production verifier on
    the request path.
11. Migration, exact N/N-1 binary, role privilege, RLS, generated-code, lint,
    integration, and race evidence.

## Explicitly Deferred

- Organization member invitations, role assignment/removal APIs, Project
  creation, and other administrative control-plane interfaces.
- Service Principal create/list/disable and Credential issue/rotate/revoke
  APIs. Existing Service Principal authentication remains supported.
- Billing and audit read APIs. `BillingAdmin` and `OrganizationAuditor` have no
  Customer Content permission in this slice and obtain positive capabilities
  only when those interfaces are implemented.
- Platform Operator identity and Break-glass request, approval, activation,
  content access, expiry, revocation, and immutable audit receipts.
- Customer-defined roles, permission composition, shared Organization master
  credentials, local human passwords, token exchange, refresh-token handling,
  browser login UI, or an embedded identity provider.
- Production IdP availability, key-rotation, incident-response, and
  cross-Organization penetration-test Launch Receipts.

## Public Seams

The architecture-agreed TDD seams are:

1. `identity.OIDCTokenVerifier` and the production JWKS-backed implementation;
2. `identity.Authenticator.Authenticate` for Service Principal credentials and
   Human OIDC bearer tokens;
3. `identity.Principal.ForProject` for selecting an exact role-derived Project
   permission set before a domain call;
4. `vela_set_request_context(uuid, bytea, text)` plus forced RLS as the
   transaction-level authorization boundary;
5. the generated submit/get/cancel/Artifact/Webhook HTTP operations;
6. `vela-control` configuration and startup wiring;
7. migration 00012, its structural Down refusal, and exact `cedd0e8` N-1
   startup/request behavior.

Tests may inspect PostgreSQL after a public operation to prove atomic identity,
isolation, and no-mutation invariants. Direct SQL mutation is limited to role
fixture setup, revocation/expiry injection, and negative constraint/privilege
evidence until the deferred administrative APIs exist.

## OIDC Contract

The production verifier is configured with exactly one HTTPS issuer, one
audience, and one HTTPS JWKS URL. It accepts only signed JWT access tokens whose
verified claims satisfy:

- `iss` exactly equals the configured issuer;
- `aud` contains the configured audience;
- `exp` is present and in the future under verifier clock policy;
- `sub` is present, non-empty, and no longer than 500 bytes;
- the JOSE algorithm and key are accepted by the proven OIDC/JWT library and
  resolved from the configured JWKS endpoint.

The raw token is never logged or persisted. Vela computes an HMAC-SHA256 proof
using the configured credential pepper, clears temporary token bytes where
possible, and caps the database authorization session at five minutes even when
the external token lasts longer. A verified but unbound, disabled, ambiguous,
or cross-Organization subject is unauthorized.

## Role And Request Context Contract

Organization role assignments bind `(organization_id, principal_id, role)`.
Project role assignments additionally bind the exact Project. Both require a
HUMAN Principal in the same Customer Organization. Assignment identity and
creation evidence are immutable in this slice; administrative mutation arrives
through a later audited API.

Authentication returns safe in-memory Project authorization projections. Before
an existing Project API is called, `ForProject` selects one Project session and
its effective permission set. It never copies permission from another Project
and never treats an Organization role as Project content authority.

The request-role transaction then calls the existing
`vela_set_request_context` signature. PostgreSQL first attempts the existing
Service Credential proof. If that does not match, it resolves the Human session
and rechecks all of the following at PostgreSQL time:

- session proof and expiry;
- active exact issuer/subject binding;
- HUMAN Principal identity and Organization;
- exact Project ownership;
- a current fixed Project role that grants the requested permission.

The private request context records actor kind plus the selected actor-session
id. A second call in the same transaction is idempotent only for the same actor,
Organization, Project, Principal, and permission. Any attempted identity,
Project, actor-kind, or permission switch fails with SQLSTATE `28000`.

## Database And Authorization Invariants

- One active `(issuer, subject)` binds to at most one Human Principal globally.
- A Human OIDC binding cannot reference a SERVICE Principal.
- Organization and Project role rows cannot cross Customer Organizations.
- Project roles cannot reference a Project outside their Principal's
  Organization.
- Human sessions contain only a keyed token proof, fixed identity, bounded
  expiry, and creation time; the raw bearer token is absent.
- Expired sessions and sessions whose binding or role is no longer active
  cannot establish request context.
- Service Principal credentials continue to require exact digest proof, scope,
  expiry, revocation, and Project identity.
- Request/auth roles cannot directly read Human bindings, memberships, session
  proofs, Service Credential digests, or private request contexts.
- Human and Service actors share RLS policies only after their independently
  validated context resolves to the exact same Organization/Project facts.
- No role decision can update Job, Attempt, Lease, Charge, Artifact, Outbox, or
  Webhook state except through the already-authorized domain operation.

## Required Evidence

- production verifier accepts a valid signed token and rejects wrong issuer,
  audience, signature, expiry, missing subject, insecure configuration, and
  unknown key;
- an exact OIDC subject authenticates to its bound Human Principal without
  storing the raw token;
- unbound/disabled/ambiguous bindings, expired sessions, wrong proofs, removed
  roles, and cross-Organization/Project substitutions fail closed;
- `Developer` can submit/read/cancel and read committed Artifacts only in its
  assigned Project;
- `ProjectViewer` can read Job/Artifact state but cannot submit, cancel, or
  manage Webhooks;
- `ProjectAdmin` can manage/read Webhooks but cannot read prompt or Artifact
  content without another Project role;
- `OrganizationOwner`, `BillingAdmin`, and `OrganizationAuditor` receive no
  Project Customer Content access solely from their Organization role;
- multiple roles union permissions only inside the same Project;
- role removal after OIDC authentication but before the domain transaction is
  rejected by PostgreSQL and causes no mutation;
- Service Principal authentication and scope checks remain byte-for-byte
  compatible at the public API;
- request/auth roles cannot read protected identity/session tables directly;
- empty migration Down/Up restores the prior surface, while any Human binding,
  role, or session evidence refuses structural Down;
- exact `cedd0e8` N-1 startup and Service Principal requests work on the
  expanded schema;
- `make generate`, lint, unit, integration, race, and two-axis review pass with
  no unresolved P0-P2 finding.

## Completion Boundary

This slice is complete only when every public seam is on the current production
caller path, all required evidence passes, generated output reaches a fixed
point, and two-axis review finds no unresolved P0-P2 issue. Administrative
identity lifecycle, Break-glass, production IdP/deployment receipts, and all
nine Production Gates remain separate and must stay unclaimed.
