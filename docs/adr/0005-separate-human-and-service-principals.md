# Separate human and service principals

Vela uses OIDC-backed Human Principals for administrative access and Project-owned Service Principals for inference API calls. A Service Principal may hold overlapping hashed credentials for zero-downtime rotation, with explicit scopes, expiry, creator, and revocation records; Vela stores no human password and issues no shared Customer Organization master key.

## Consequences

Every Job and administrative action is attributable to one Principal. Revoking a credential does not erase its Principal or audit history, and replacing a credential does not require changing Project ownership, quotas, or Job attribution.
