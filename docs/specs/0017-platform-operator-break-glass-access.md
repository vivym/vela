# Platform Operator Break-glass Access

Date: 2026-08-24

Status: Proposed. This specification defines repository-verifiable identity,
approval, authorization, content-delivery, and audit behavior. It does not by
itself satisfy a Production Gate or prove production IdP, object-store, network,
or deployment isolation.

## Goal

Provide a separate, fail-closed Platform Operator path for exceptional support
access to one Job's Customer Content. Access requires two distinct active
Platform Operators, a closed reason and scope, an exact Organization/Project/Job
target, a maximum one-hour lifetime, and immutable customer-visible evidence.

A Platform Operator never becomes a Customer Organization member, never receives
a customer role, never authenticates as a Customer Principal, and never uses a
shared Organization credential. Ordinary customer credentials and Human OIDC
tokens cannot call the platform path, and Platform Operator tokens cannot call
customer APIs.

## Governing Decisions

- ADR 0004 and ADR 0007: every grant binds one exact `organization_id +
  project_id + job_id`; database functions revalidate the composite ownership
  before every state transition and content access.
- ADR 0005: Platform Operators use an independent OIDC issuer/audience, binding,
  session, proof, authentication role, and HTTP context. They are not added to
  `principals`, `human_oidc_bindings`, membership, or customer-role tables.
- ADR 0006: no fixed customer role receives Break-glass authority. Requester and
  approver are different Platform Operator identities, not merely different
  sessions.
- ADR 0008: support access is exceptional and purpose-bound. Customer Content is
  never copied into request, approval, audit, error, or log fields.
- ADR 0013: migration 00017 is additive on the up path and preserves exact N/N-1
  database/control behavior while the feature is disabled in the N-1 binary.
- ADR 0015: a grant cannot revive expired or deleted content. Existing Content
  Deletion tombstones, revoked grants, exact object-version deletion, and
  retention deadlines remain authoritative.
- ADR 0029: repository evidence does not satisfy a Production Gate. Production
  remains `0/9 PASS` without versioned Launch Receipts.

## Confirmed TDD Seams

Tests exercise behavior only at these public boundaries:

1. independent Platform Operator OIDC verification and database authentication;
2. PostgreSQL request, approval, revocation, and per-access authorization
   functions through the dedicated non-BYPASSRLS roles;
3. production HTTP/OpenAPI contracts for request, approval, revocation, status,
   request-content access, and Artifact access;
4. exact-version Artifact signing whose expiry is bounded by both 15 minutes and
   the active grant expiry.

## In Scope

1. Add immutable Platform Operator OIDC bindings outside all Customer
   Organization identity tables. Bindings are provisioned and disabled through
   an out-of-band platform identity process; no customer-facing API can create,
   list, change, or disable them.
2. Verify Platform Operator tokens with a separately configured HTTPS issuer,
   audience, and JWKS endpoint. A valid proof creates or reuses a proof-bound
   session lasting at most 15 minutes and never past the OIDC token expiry.
3. Route only `/v1/platform/break-glass/**` through the Platform Operator
   authenticator. All other routes retain the customer authenticator. A token
   valid for one surface is invalid on the other even if both IdPs use the same
   subject string.
4. Create an idempotent Break-glass request for one existing Job. The immutable
   request fixes:
   - `organization_id`, `project_id`, and `job_id`;
   - a nonempty, unique subset of `REQUEST_CONTENT_READ` and `ARTIFACT_READ`;
   - one reason code from `CUSTOMER_SUPPORT`, `SECURITY_INVESTIGATION`,
     `SERVICE_RECOVERY`, or `LEGAL_RESPONSE`;
   - an ASCII ticket reference of 3 to 100 characters restricted to letters,
     digits, `.`, `_`, `:`, `/`, and `-`;
   - requested duration from 60 through 3600 seconds;
   - requester identity/session, request hash, PostgreSQL request time, and an
     approval deadline exactly one hour after request time.
5. Request idempotency is scoped to the requesting Platform Operator. Replaying
   the same `Idempotency-Key` and canonical body returns the same request;
   reusing the key with a different body returns `409`.
6. A second active Platform Operator may approve a still-pending request before
   its approval deadline. Self-approval compares immutable operator IDs and is
   rejected even through another session. Approval atomically creates one grant
   whose `expires_at` is PostgreSQL approval time plus the requested duration.
7. A pending request cannot be rejected or rewritten in this slice. It becomes
   `EXPIRED` as a derived state after its approval deadline. An approved request
   is `ACTIVE`, `REVOKED`, or `EXPIRED` according to its immutable approval,
   optional revocation, and PostgreSQL time.
8. Any active Platform Operator may revoke an active grant. Revocation is
   one-way, records exact operator/session attribution, and immediately prevents
   new authorization. Replaying revocation returns the same result without a
   second event.
9. `REQUEST_CONTENT_READ` returns only the immutable Job request-content JSON.
   It is denied if the request snapshot expired, was tombstoned, the Job target
   changed or disappeared, the grant is inactive, or the exact scope is absent.
10. `ARTIFACT_READ` returns only the committed ArtifactSet and its complete
    exact-version Artifact list. It is denied if content deletion was accepted,
    retention elapsed, an Artifact is not `COMMITTED`, the grant is inactive,
    or the exact scope is absent.
11. Artifact URLs bind both object key and exact version. Their expiry is
    `min(authorization_time + 15 minutes, grant_expires_at)`. No URL is issued
    after revocation or expiry, and signing never lists a bucket or resolves a
    current object version.
12. Request, approval, revocation, allowed access, and denied access after a
    valid Platform Operator authentication write immutable events. Evidence
    contains only IDs, closed action/outcome/scope values, ticket reference,
    timestamps, and safe reason codes; it contains no Customer Content, object
    key/version, signed URL, checksum, OIDC token, proof, or arbitrary error.
13. An allowed content access authorization event commits before Customer
    Content or signed URLs leave the service. Artifact delivery additionally
    records a safe completion or signing-failure event so authorization cannot
    be mistaken for successful delivery.
14. Organization audit reporting projects safe Break-glass request, approval,
    revocation, and access-delivery evidence. `OrganizationOwner` and
    `OrganizationAuditor` can see that platform access occurred, the exact Job
    target, scope, outcome, operator IDs, and timestamps, but never content or
    storage identities.

## Public Contracts

All platform routes require a Platform Operator bearer token and reject customer
Human or Service Principal credentials with `401`.

### Request

`POST /v1/platform/break-glass/requests` requires `Idempotency-Key` and:

```json
{
  "organization_id": "uuid",
  "project_id": "uuid",
  "job_id": "uuid",
  "scopes": ["REQUEST_CONTENT_READ", "ARTIFACT_READ"],
  "reason_code": "CUSTOMER_SUPPORT",
  "ticket_reference": "SUPPORT-1234",
  "requested_duration_seconds": 1800
}
```

It returns `201 Created` for a new request and `200 OK` for an exact replay. The
projection contains IDs, scopes, reason code, ticket reference, requester and
approver operator IDs, request/approval/revocation/expiry timestamps, and the
derived state. It contains no Customer Content.

### Approval And Revocation

`POST /v1/platform/break-glass/requests/{request_id}/approval` activates the
grant and returns `200 OK`. It is idempotent for the original approver and fails
with `409` for self-approval, a different later approver, or an expired request.

`POST /v1/platform/break-glass/grants/{grant_id}/revocation` returns `200 OK`.
The first call records revocation; later calls return the same revoked grant.

`GET /v1/platform/break-glass/requests/{request_id}` returns the safe request and
grant projection to any active Platform Operator.

### Content Access

`GET /v1/platform/break-glass/grants/{grant_id}/request-content` returns:

```json
{
  "organization_id": "uuid",
  "project_id": "uuid",
  "job_id": "uuid",
  "request_content": {}
}
```

`GET /v1/platform/break-glass/grants/{grant_id}/artifacts` returns the committed
ArtifactSet metadata and short-lived exact-version URLs. It does not return raw
object keys or version IDs.

### Error Mapping

- `400`: malformed UUID, Idempotency-Key, body, scope, reason, ticket, or
  duration.
- `401`: absent/invalid Platform Operator token, expired/disabled binding, or
  inactive proof-bound session.
- `403`: self-approval, missing exact grant scope, inactive grant, or content
  lifecycle denial after a visible grant was selected.
- `404`: request, grant, Organization/Project/Job tuple, request content, or
  ArtifactSet is absent. Cross-Organization and cross-Project target mismatch is
  indistinguishable from absence.
- `409`: idempotency conflict, approval conflict, or expired approval window.
- `503`: IdP, database, or signing dependency is unavailable. Dependency errors
  are sanitized and never weaken authorization.

## Database And Authorization Invariants

- `platform_operator_oidc_bindings` and sessions have no foreign key to
  Customer Organization identity tables. Issuer/subject and operator ID are
  immutable; disablement is one-way.
- Break-glass requests, their target/scope/reason/ticket/requester identity, and
  all audit events are immutable. Only narrow owner functions can write approval
  and revocation lifecycle columns.
- Every transition and access function reauthenticates the proof-bound session
  inside the transaction using PostgreSQL time. A Go identity value alone is not
  authorization.
- `vela_platform_operator_auth` and `vela_break_glass_request` are NOLOGIN,
  non-superuser, non-BYPASSRLS roles and own no tables. They receive only exact
  function execution privileges.
- `vela_break_glass_owner` is NOLOGIN and BYPASSRLS, owns Break-glass tables and
  security-definer functions, and is never granted to a login or runtime role.
- The Break-glass runtime role cannot directly read or mutate customer,
  identity, Job, Artifact, billing, retention, Webhook, or audit tables and
  cannot execute ordinary customer request-context functions.
- Break-glass tables use forced RLS, composite target foreign keys, immutable
  triggers, and uniqueness constraints for one approval/grant and one revocation.
- Customer roles and Service Principal scopes contain no platform permission;
  customer runtime roles cannot execute any Break-glass function or read its
  tables.

## Compatibility

- Migration 00017 is additive: it creates new roles/functions/tables and replaces
  only the Organization audit projection function with a backward-compatible
  result shape.
- An N-1 control binary starts and serves all pre-00017 customer APIs against
  schema 00017 without access to the new roles or functions.
- The N control binary fails startup against schema 00016 because the dedicated
  Platform Operator auth and Break-glass roles/functions are absent.
- Down migration is allowed only when no Platform Operator binding, session,
  Break-glass request, grant, or event exists. It restores the exact prior
  Organization audit projection.

## Required Evidence

- OIDC unit/integration tests prove the platform issuer/audience is independent,
  HTTPS/JWKS validation remains fail closed, sessions are proof-bound and short,
  disabled/expired operators fail, and platform/customer token confusion fails.
- Database tests prove requester/approver separation, exact tuple ownership,
  idempotency, one-hour limits, approval expiry, revocation, scope closure,
  PostgreSQL-time expiry, and immutable evidence.
- Production HTTP integration proves create/replay/conflict, self-approval
  denial, approval/revocation, request-content read, exact-version Artifact URLs,
  customer credential denial, and safe error mapping.
- Negative integration covers cross-Organization/Project/Job tuples, every fixed
  customer role, Service Principal credentials, expired/revoked grants, deleted
  request content, deleted/expired Artifacts, incomplete ArtifactSets, and
  disabled Platform Operators.
- Organization audit integration proves safe Break-glass projection and absence
  of content, ticket-freeform expansion, object identities, URLs, credentials,
  and proofs.
- Role tests prove runtime non-BYPASSRLS, owner NOLOGIN isolation, exact execute
  grants, direct-table denial, and customer-role denial.
- Migration tests prove 00016 -> 00017 -> 00016 -> 00017, N/N-1 startup, and
  fail-closed down migration with durable identity or access evidence.
- `make generate`, focused tests, full unit/race/integration tests, `go vet`,
  lint, OpenAPI/Protobuf breaking checks, and two-pass generated-output hashes
  are clean before closure.

## Explicitly Deferred

This slice does not provide the out-of-band operator-provisioning product,
production IdP enrollment/disablement receipts, customer notification workflow,
support-ticket system integration, network egress controls, session recording,
deployment manifests, on-call runbooks, or any Production Gate Launch Receipt.

Those boundaries remain separate production work. Repository tests must not be
used to promote a Production Gate.

## Completion Boundary

Slice 17 is complete only when the committed repository implements the separate
Platform Operator identity, dual-control state machine, exact scoped access,
grant-bounded signing, immutable events, customer-visible safe audit projection,
least-privilege roles, N/N-1 behavior, and all required negative evidence with no
P0-P2 review finding.

Completion advances ADR 0005, ADR 0006, ADR 0007, ADR 0008, and acceptance
scenario 29. It does not close production IdP, deployment isolation, content
reuse policy enforcement outside this service, or the Organization Isolation
Production Gate.
