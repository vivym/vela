# Human Membership And Fixed Role Administration

Status: Implemented

Date: 2026-08-23

Fixed point: `d8537d96cc8aeb7b7d4980e5059cf48efa713d6f`

Predecessors:

- `docs/specs/0012-human-oidc-and-fixed-rbac.md`
- `docs/specs/0013-project-service-principal-credential-administration.md`

## Goal

Allow an active Human `OrganizationOwner` to add, inspect, and permanently
disable Human members in its exact Customer Organization and to assign or revoke
the fixed Organization and Project roles. Allow an active Human `ProjectAdmin`
to inspect and manage fixed Project roles only in its exact Project. Every
successful mutation is attributed to the exact Human Principal and short-lived
authorization session and is revalidated inside the database transaction.

This slice closes the administrative Human membership and role-assignment
surface deferred by Slices 12 and 13. Project creation, Organization policy,
positive billing/audit interfaces, Platform Operator Break-glass Access, and
production identity/deployment receipts remain later work. Repository evidence
does not advance any Production Gate.

## Governing Decisions

- ADR 0004/0007: Customer Organization remains the hard isolation boundary and
  Project remains the operational/content boundary. Organization administration
  cannot substitute a Project identity or bypass composite ownership and forced
  RLS.
- ADR 0005: a Human Principal is bound immutably to one configured enterprise
  OIDC `(issuer, subject)` identity and never receives a local password or
  Service Credential.
- ADR 0006: administration assigns only `OrganizationOwner`, `BillingAdmin`,
  `OrganizationAuditor`, `ProjectAdmin`, `Developer`, and `ProjectViewer`.
  Administrative roles do not synthesize Customer Content access.
- ADR 0013: migration 00014 is additive and the exact fixed-point control binary
  remains valid on the expanded schema.
- ADR 0029: integration tests and review are repository evidence, not Launch
  Receipts.

## In Scope

1. Add and list Human members through the public Organization REST API. The
   server fixes the configured OIDC issuer; the caller supplies the exact OIDC
   subject and optional display name.
2. Permanently disable a Human member. Disablement invalidates all subsequent
   authentication and transaction-time authorization without deleting the
   Principal, OIDC binding, roles, sessions, or audit history.
3. Assign and revoke fixed Organization roles through the public Organization
   REST API.
4. List Project members and assign or revoke fixed Project roles through the
   public Project REST API.
5. `OrganizationOwner` may manage Human members and Organization roles in its
   exact Customer Organization and Project roles in Projects owned by that
   Organization. These administrative permissions grant no prompt, Artifact,
   Job, billing, or audit-read permission.
6. `ProjectAdmin` may list and manage Project roles only in its exact Project.
   It cannot add or disable Organization members or assign Organization roles.
7. The last active `OrganizationOwner` cannot be revoked or disabled. The
   invariant is serialized on the Customer Organization row and holds under
   concurrent requests.
8. Assignment, revocation, member creation, and disablement are idempotent.
   A replay returns the committed safe state and does not duplicate audit
   history.
9. Every successful transition writes one immutable Human identity event in the
   same transaction, attributed to the exact Human Principal and Organization
   or Project authorization session. Events contain no OIDC bearer token,
   session proof, Credential digest, prompt, or Artifact metadata.
10. Add dedicated `vela_human_membership_auth` and
    `vela_human_membership_request` roles for the new Organization-session
    lookup and Human membership administration functions. Do not expand the
    exact function allowlists of the existing `vela_human_auth` or
    `vela_identity_request` roles. Every role retains no direct table access and
    cannot call unrelated request, internal, billing, Webhook, or Artifact
    functions.
11. Preserve Human and Service authentication, Service Principal administration,
    generated-code fixed point, migration Down/Up, and exact N-1 behavior.

## Explicitly Deferred

- Customer Organization and Project creation, rename, deletion, quota, billing,
  and Organization policy administration.
- BillingAdmin billing interfaces and OrganizationAuditor audit/read interfaces.
- Platform Operator identity and Break-glass request, approval, activation,
  expiry, revocation, Customer Content access, and Launch Receipts.
- Customer-defined roles, arbitrary permission composition, role expiry,
  delegated custom administration, group synchronization, SCIM, email delivery,
  browser invitation acceptance, or OIDC subject rebinding.
- Re-enabling a disabled Human member, local Human passwords, refresh-token
  handling, a shared Organization master Credential, or an embedded identity
  provider.
- Production IdP availability/key-rotation receipts, deployment isolation,
  NATS workload identity, and all nine Production Gates.

## Public Seams

The architecture-agreed TDD seams are:

1. generated REST/OpenAPI operations under
   `/v1/organizations/{organization_id}/members` and
   `/v1/projects/{project_id}/members`;
2. `identity.Authenticator.Authenticate`, `Principal.ForOrganization`, and
   `Principal.ForProject` for selecting exact short-lived Human authorization;
3. `identity.AdministrationService` Human member and fixed-role commands and
   safe list projections;
4. `vela_set_organization_identity_admin_context` and
   `vela_set_project_membership_admin_context` as transaction-time Human-only
   authorization boundaries;
5. migration 00014 constraints, immutable event attribution, exact privilege
   surface, structural Down refusal, and fixed-point N-1 compatibility;
6. `vela-control` configured OIDC issuer and production caller wiring.

Production wiring requires two additional independently verified pools:

- `VELA_HUMAN_MEMBERSHIP_AUTH_DATABASE_URL` connects through a login whose only
  application-role membership is `vela_human_membership_auth`;
- `VELA_HUMAN_MEMBERSHIP_REQUEST_DATABASE_URL` connects through a login whose
  only application-role membership is `vela_human_membership_request`.

The existing `VELA_HUMAN_AUTH_DATABASE_URL` and
`VELA_IDENTITY_REQUEST_DATABASE_URL` pools retain their Slice 12 and Slice 13
function allowlists exactly. This role expansion, rather than adding privileges
to the legacy roles, is required because the fixed-point N-1 binary verifies
those legacy allowlists at startup and must remain valid after migration 00014.

Tests exercise the REST or Administration service seam. Database inspection is
limited to proving atomic authorization, immutable audit, fixed-role, last-owner,
privilege, migration, and no-mutation invariants.

## REST Contract

The slice adds these operations:

```text
POST /v1/organizations/{organization_id}/members
GET  /v1/organizations/{organization_id}/members
POST /v1/organizations/{organization_id}/members/{principal_id}/disable
POST /v1/organizations/{organization_id}/members/{principal_id}/roles
POST /v1/organizations/{organization_id}/members/{principal_id}/roles/{role}/revoke

GET  /v1/projects/{project_id}/members
POST /v1/projects/{project_id}/members/{principal_id}/roles
POST /v1/projects/{project_id}/members/{principal_id}/roles/{role}/revoke
```

Member creation accepts `oidc_subject` and optional `display_name`; the issuer is
the server's configured enterprise OIDC issuer and cannot be supplied or changed
by the caller. The opaque subject contains 1 to 500 bytes. A display name, when
present, contains 1 to 200 Unicode characters after trimming. Lists are bounded
to 100 rows and ordered by creation/assignment evidence plus stable ids.

Organization role requests accept only `OrganizationOwner`, `BillingAdmin`, or
`OrganizationAuditor`. Project role requests accept only `ProjectAdmin`,
`Developer`, or `ProjectViewer`. A cross-Organization member or Project is not
visible. Replays expose the already committed resource state without creating a
second event.

## Authorization Contract

Human OIDC authentication creates an additional short-lived Organization
authorization session only when the current fixed Organization roles grant an
Organization administrative scope. The proof is the same keyed token proof used
for Project sessions, expiry is bounded by the external token and five-minute
server ceiling, and the raw OIDC token is never stored.

`Principal.ForOrganization` selects the exact Organization session and only
Organization administrative scopes. It never grants Customer Content access.
`Principal.ForProject` continues to select exact Project role permissions. For a
Project membership operation, `OrganizationOwner` may instead use its exact
Organization session; PostgreSQL verifies that the target Project belongs to
that Organization before establishing the Project administration context.

Every mutation transaction rechecks the active binding, session proof/expiry,
HUMAN Principal kind, current required fixed role, exact Organization, and when
applicable exact Project. Role removal or member disablement after HTTP
authentication therefore fails closed before any mutation.

## Database And Audit Invariants

- `(issuer, subject)` and a Human Principal's Organization ownership remain
  immutable. A disabled binding cannot be re-enabled or move its disable time.
- Organization and Project roles can reference only an active HUMAN member in
  the exact Customer Organization. Project role identity cannot cross Projects.
- The last active `OrganizationOwner` survives concurrent revoke and disable
  attempts. A different active owner can later repair Project administration.
- Project role management does not imply any Job or Artifact permission for an
  `OrganizationOwner`; existing content paths still require an explicit Project
  role and the ordinary request context.
- Assignment/revocation and creation/disable transitions serialize with their
  target identity and write the event atomically. Failed and replayed operations
  write no event.
- Audit actor-session attribution is durable even after the short-lived session
  expires. Events and attribution rows are immutable and retain the target
  Principal after role revocation or member disablement.
- The request, auth, Human-auth, Human-membership-auth, identity-request, and
  Human-membership-request roles cannot directly read Human bindings, role
  bindings, session proofs, private contexts, or identity events.

## Required Evidence

- an `OrganizationOwner` adds/lists a Human member whose exact OIDC subject
  authenticates to the bound identity but receives no Organization or Project
  API authorization until a fixed role is assigned;
- an `OrganizationOwner` assigns/lists/revokes every Organization role and every
  Project role in its Organization without receiving Customer Content access;
- a `ProjectAdmin` lists/assigns/revokes Project roles in its exact Project but
  cannot manage Organization members/roles or another Project;
- `BillingAdmin`, `OrganizationAuditor`, `Developer`, `ProjectViewer`, every
  Service Principal, disabled Humans, and unknown Principal kinds fail closed;
- role removal after authentication but before the transaction causes no
  mutation, and disabled/revoked targets can no longer authenticate/authorize;
- last-owner revoke/disable and concurrent last-owner transitions are rejected
  without losing the final active owner;
- duplicate create/assign/revoke/disable requests are deterministic and do not
  duplicate immutable events;
- every successful event has exact Principal/session attribution and contains no
  OIDC token/proof or Customer Content; event and identity transitions are
  immutable under direct negative tests;
- cross-Organization/Project substitution, malformed subject/display/role,
  inactive target, and direct SQL privilege attempts fail closed;
- Human and Service authentication, Service Principal administration, and all
  existing exact HTTP error responses remain unchanged;
- empty migration Down/Up restores the prior surface, while durable Human
  administration evidence refuses structural Down;
- exact fixed-point N-1 startup and Service/Human requests remain valid on
  migration 00014;
- stable generation, lint, unit, integration, race, breaking checks, and
  two-axis review pass with no unresolved P0-P2 finding.

## Completion Boundary

This slice is complete only when every public seam is on the production caller
path and all required evidence passes. Project/Organization creation and policy,
positive billing/audit interfaces, Break-glass, production IdP/deployment
receipts, NATS identity, and all nine Production Gates remain separate and
unclaimed.
