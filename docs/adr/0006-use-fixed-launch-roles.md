# Use fixed roles for launch authorization

Vela launches with fixed Human Principal roles: OrganizationOwner, BillingAdmin, OrganizationAuditor, ProjectAdmin, Developer, and ProjectViewer. Billing and audit roles do not receive prompt or Artifact access by default, Service Principals use explicit API scopes instead of human roles, and Platform Operator access to customer data is a separate time-limited, approved, and fully audited break-glass path.

## Consequences

Launch does not support customer-defined roles or arbitrary permission composition. New permissions must be assigned deliberately to an existing role or introduced through a reviewed role-model revision, and sensitive billing, content, credential, and support-access capabilities remain independently testable.
