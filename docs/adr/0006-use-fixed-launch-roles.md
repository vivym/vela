# Use fixed roles for launch authorization

Vela launches with fixed Human Principal roles: OrganizationOwner, BillingAdmin, OrganizationAuditor, ProjectAdmin, Developer, and ProjectViewer. Billing and audit roles do not receive prompt or Artifact access by default, Service Principals use explicit API scopes instead of human roles, and Platform Operator access to customer data is a separate time-limited, approved, and fully audited break-glass path.

## Consequences

Launch does not support customer-defined roles or arbitrary permission composition. New permissions must be assigned deliberately to an existing role or introduced through a reviewed role-model revision, and sensitive billing, content, credential, and support-access capabilities remain independently testable.

## Implementation Status

Implemented for the current API. All six Human roles are database-constrained
and auditably administered. BillingAdmin and OrganizationAuditor expose only
their independently tested non-content projections, while no customer role can
request, approve, or use support access. Break-glass Access requires two distinct
Platform Operators, an exact target and scope, a closed reason, expiry of at most
one hour, revocation, and immutable audit evidence. Future permissions and
production identity/deployment receipts must preserve these fixed boundaries.
