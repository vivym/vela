# Trusted launch topology propagation

Local CPU-only increment over `e7cc423`, database schema 90. Production Gates
remain **0/9**. No GPU, remote deployment or new load campaign was used.

## Contract

WorkerBundle actuation and H3 bundle specifications now require schema 2 and an
explicit `device_subset_digest` per member. The actuation digest uses the
`vela.worker-bundle-actuation/v2` domain, binding that field into approval.
The launch verifier compares the live Registry subset digest with the approved
value, alongside the Worker DeviceSet, membership and member identity digests
introduced by the preceding repair.

Fleet emits ModelRuntime launch manifest schema 2 with every member's identity
and device-subset digest. Both are required canonical 32-byte hex strings.
RuntimeServer derives the complete execution-floor member configuration from
that manifest when explicitly configured with `ExecutionFloor`. If the caller
also supplies members, every identity, epoch and digest must match exactly;
missing, duplicated or unknown members reject before allocating Runtime epochs
or starting any backend. An omitted floor validator uses the server validator.
Derived member data is copied; it is not learned from a terminal response.

The subset digest is opaque authority, not a local hash of device constraints.
Lab assets preserve the existing bootstrap contract: SHA256 of
`vela/lab-v2/<WorkerDescriptor.Name>/device-subset/v1`, with identity bound to the
member certificate's SPIFFE URI. The Fleet template uses explicit non-approved
subset placeholders and recomputed bundle/plan digests. Those values are not
deployment evidence.

## Compatibility

Schema-1 WorkerBundle actuation and Runtime launch manifests are rejected.
Rebuild v2 configuration from complete approved membership data, recompute the
bundle and plan digests, and bind the resulting resources to a new release.
Do not infer missing subset authority or silently upgrade old documents.
The outer ResidencyPlanRollouts and ApprovedResidencyPlan schemas remain 1;
the PostgreSQL schema remains 90. Existing immutable evidence is historical
and is not rewritten to claim validation by the new verifier.

## Validation

- `go test ./...`: PASS.
- `go test -race ./internal/modelruntime ./internal/fleetcontroller ./internal/h3launchevidence ./cmd/vela-lab-assets ./cmd/vela-fleet-controller ./cmd/vela-model-runtime ./internal/releasebundle`: PASS.
- `go test -tags=integration ./internal/integration -run '^(TestCatalogPromotion|TestH3CampaignEvidence)' -count=1 -timeout=10m`: PASS, 34.011 package seconds.
- `make lint`: PASS, 0 issues.
- `make validate-deployment`: PASS, including the updated template.
- `make test-cross`: PASS for Linux amd64 compilation only.
- `make verify-generated`: PASS, no generated-code drift.
- Linux arm64 execution as UID 65534: PASS for manifest validation, server
  topology rejection and assembled signed-floor persistence/recovery tests.

The assembly test supplies only the explicit journal state configuration for its
first startup and recovery, exercising derived members and validator through a
real private Unix socket. It installs a signed floor, restarts, and rejects the
retired allocation. Its separate 64-member phase retains explicit complete
configuration and verifies the signed-history message bound. Six topology drift
cases assert zero epoch allocations and zero backend starts. Fleet tests cover
legacy schema, absent subset authority and changes retaining the old revision.
The Registry subset mismatch returns `ErrInvalidLaunchEvidence`.

```sh
env GOOS=linux GOARCH=arm64 CGO_ENABLED=0 \
  go test -c -o /tmp/vela-launch-topology-linux.test ./internal/modelruntime
docker run --rm --network none --read-only --cap-drop ALL \
  --user 65534:65534 --tmpfs /tmp:rw,nosuid,nodev,mode=1777 \
  --mount type=bind,src=/tmp/vela-launch-topology-linux.test,dst=/runtime.test,readonly \
  --entrypoint /runtime.test \
  sha256:18cfe3ef5e6815560c98237d6216d1e5119702fb0f3894c8785dd58b8bbe5d73 \
  -test.run '^(TestExecutionFloorServerAssemblyRecoversAndPreservesMessageBounds|TestRuntimeServerRejectsFloorTopologyDriftBeforeStartingBackends|TestLoadLaunchManifest|TestEncodeLaunchManifest)' \
  -test.count=1
```

## Remaining Work

`cmd/vela-model-runtime` still does not enable `ExecutionFloor` by default.
Missing epoch files currently read as zero, and Fleet init containers create
directories on every Pod initialization. Neither a missing file nor an empty
directory proves first use. Default activation requires an independently trusted
bootstrap decision; recovery must reject missing/replaced state. A reusable
static `Initialize=true` permission would reopen admission after state loss.

Worker signed cutoff persistence, default Worker admission, historical read-only
inspection, execution-specific writer drain, the terminal retirement journal,
pending-record reclamation and automatic recovery remain open. Installing all
member floors is not DRAINED and does not permit scratch deletion. Stage drain
must preserve resident models.
