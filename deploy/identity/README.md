# Internal identity service

This bundle deploys a two-replica Keycloak instance on the CPU management
nodes, backed by a separate database in the replicated CNPG cluster. The
database and bootstrap-admin credentials are externally generated immutable
Secrets; their values are never committed here.

The initial certificate authority is cluster-local and self-signed. Install
`identity/vela-identity-ca` into the clients that must validate the issuer, and
replace it through the approved PKI workflow before exposing the service beyond
the private management network. The three-node storage and control cluster
does not provide a public DNS name or independent identity-service failure
domain.
