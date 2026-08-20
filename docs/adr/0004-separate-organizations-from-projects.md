# Separate Customer Organizations from Projects

Customer Organization is Vela's contract, credit, monthly settlement, and hard customer-isolation boundary, while Project is the operational boundary for API credentials, concurrency, rate limits, idempotency, Artifact namespaces, and audit. A Customer Organization can own multiple Projects, whose Charges consume the shared Contract Credit Limit and roll up to one settlement relationship.

## Consequences

External and domain Interfaces use `organization_id` and `project_id` instead of an overloaded `tenant_id`. Authorization always verifies both ownership and Project scope, while billing and customer-wide suspension aggregate across all Projects in the Customer Organization.
