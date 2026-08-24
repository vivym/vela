package httpapi

import "github.com/vivym/vela/api/gen"

// These identifiers predate the Break-glass API and are part of the generated
// Go source contract even when the wire-level OpenAPI diff is additive.
var (
	_ api.ArtifactDownloadKind         = api.VIDEO
	_ api.WebhookSubscriptionState     = api.ACTIVE
	_ api.OrganizationAuditEventAction = api.CREDENTIALISSUED
)
