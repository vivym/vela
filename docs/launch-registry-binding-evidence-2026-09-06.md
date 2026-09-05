# Launch Registry digest binding repair

Local CPU-only repair over `92fb1e8`, schema 90. Production Gates remain **0/9**.

`TestVerifyRejectsRegistryDigestMismatchAgainstApprovedLaunch` reproduced three
false acceptances: changing the Registry Worker DeviceSet digest, membership
digest or member identity digest to a different valid 32-byte hex value still
returned launch evidence for one Worker and no error. The approved rollout and
live Kubernetes objects were unchanged. The previous positive fixture itself
contained different approved and Registry Worker digests.

The verifier now compares both Worker digests and each member identity digest
with the exact approved actuation. Shape checks remain. The positive fixture
now binds its Registry snapshot to those approved values. All three mismatch
regressions reject with `ErrInvalidLaunchEvidence`.

- `go test ./...`: PASS.
- `go test -race ./internal/h3launchevidence ./cmd/vela-h3-evidence`: PASS.
- `make lint`: PASS, 0 issues.

This repairs verification logic, not a live deployment or existing receipt.
Device-subset binding remains a prerequisite: current Worker actuation and
Runtime launch manifests do not carry the Registry's `device_subset_digest`.
It is an opaque authority field and must be forwarded from trusted configuration,
not invented by hashing local device constraints. Default persistent Runtime
admission also needs an explicit bootstrap/recovery lifecycle before activation.
