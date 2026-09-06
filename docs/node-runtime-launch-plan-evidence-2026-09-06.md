# Registry-bound Runtime launch plan and Pod correlation

This increment follows `3330824`. Node can now verify the canonical WorkerBundle
behind a signed Registry receipt, derive the exact member's launch manifest and
Pod, and correlate a matching caller declaration with API-observed Pod content
and the actual CRI/native task. This is a library observation path. It does not
authorize backend startup or prove the configuration actually loaded by Runtime.

PostgreSQL 94, Worker journal 5, Runtime journal 6, Registry binding 1 and
Production Gates `0/9` remain unchanged. No database, protocol, deployment,
Runtime factory gate or journal retirement operation changes here.

## Authenticated configuration

`VerifyRuntimeLaunchPlan` verifies the existing dedicated Registry signature,
requires its Node identity to match trusted local configuration, and checks the
SHA-256 of the supplied bundle bytes against the signed `bundle_digest`. It
then uses the existing bounded canonical bundle parser and Fleet's existing
member-manifest and Pod materializers. It does not introduce a second mapping
from WorkerBundle fields to Runtime commands or Pod security configuration.

The selected WorkerInstance/WorkerMember UUIDs, both epochs and assigned Node
must match the signature. Unknown members, a claim for another Node, different
epochs, tampered receipts, mismatching bundle bytes, noncanonical JSON, duplicate
keys, unknown fields and oversized bundles return no verified plan. The original
Registry journal pair remains attached to the plan as immutable historical
identity; verifying that signature does not inspect the local journal lock or
establish its current ownership.

The opaque `RuntimeLaunchPlan` retains its own verified data. `MatchManifest`
compares the complete canonical launch manifest, including commands, environment,
image digest, paths, timeouts, runtime/profile identity and member/device epochs.
`RegistryBinding` and `ExpectedPod` return copies. Changes to input bytes,
the original binding, returned signatures or nested Pod fields cannot alter
later checks. An empty or deserialized zero-value plan yields no trusted data.

These are historical configuration facts. The existing journal-binding signature
has no expiry or activation claim. It must not become a reusable startup grant,
and a later residency/launch configuration needs its own applicable authority.
The interpretation uses the supported schema-v2 Fleet derivation contract;
changing that contract requires deliberate version/compatibility handling.

## Node observation path

`RuntimeContainerObserver.ObservePlannedCaller` requires the verified plan, an
authenticated live `RuntimeCaller`, and a narrow Pod reader supplied by trusted
Node configuration. The existing `fleetcontroller.KubernetesResources` implements
that reader. A Runtime-supplied Pod document is not authenticated inventory.

The current caller payload is exactly the canonical manifest bytes. The existing
32 KiB seqpacket bound still applies; this is not the eventual journal/startup
nonce request protocol. The adapter checks those bytes before querying inventory.

Within one ten-second context bound, the adapter:

1. Queries the exact Pod name/namespace derived from the signed bundle.
2. Requires the current full Pod content to match Fleet's desired Pod using the
   existing strict comparator. Only its documented server-assigned metadata,
   status and exact scheduler Node binding are excluded from desired comparison.
3. Requires a canonical Pod UUID, nonempty resource version, the exact assigned
   Node and no deletion timestamp. It selects exactly one running
   `model-runtime` status with a full `containerd://` ID and nonnegative restart
   count. Readiness is not used as startup authorization.
4. Resolves the sandbox ID through an exact CRI container-ID query, then invokes
   the previous repeated CRI/native-task/pinned-process observer. CRI must match
   the Pod UUID/name/namespace, container name and attempt. The caller's effective
   credentials must match the UID/GID derived from the approved Pod, currently
   `10001:10001`.
5. Re-reads the Pod and rejects any change, including resource-version changes
   or same-name replacement. It copies the first observation before subsequent
   calls so a reader's reused slices/maps cannot conceal a later mutation.
6. Rechecks the retained original process after the last Pod read, then checks
   connection identity, cancellation and the collection interval before output.

The result includes a copy of the signed Registry binding, canonical launch
digest, Pod resource version, combined CRI/process observation and outer time
interval. Sequential reads are not an atomic snapshot or a lifetime lock.
`ObserveCaller` remains available as the lower-level unbound observation.

## Verification

The configuration tests exercise nine structurally valid manifest changes:
command, environment, image, scratch/input/output paths, initialization timeout,
runtime epoch, profile, device epoch and member epoch. All reject against the
original signed bundle. Sixteen malformed/unbound-history cases return no plan.
Separate H3 AUX and two-member LLM metadata cases preserve both AUX runtimes and
the exact local member/device selection; changing the second AUX command or
substituting the sibling member rejects. These are metadata tests, not GPU or
distributed model execution.

The Linux observation suite uses actual non-root processes in private PID
namespaces with protocol-level Pod/CRI/native-task fixtures. Its 26 cases cover
the positive path, invalid manifest declaration, wrong UID/Node, unverified
plans, unavailable readers, mismatching/replaced/modified Pods, changed commands
and volume mounts, deleting Pods, missing/duplicate/waiting container status,
invalid container IDs, wrong restart attempts, retained-object aliasing, caller
closure and early/final cancellation. Every Pod read must target the expected
Registry-derived resource key.

The existing real-containerd CRI experiment adds a `planned-owner` case with
actual UID/GID `10001`, CRI container creation/start/exit, native task lookup and
kernel caller authentication. Registry signatures and Kubernetes Pod reads are
fixtures in this case. The nested image is a synthetic CPU helper, not the
release image named by the synthetic plan. Its successful observation therefore
does not prove actual release-image/configuration conformance. After that real
caller exits, even a fixture Pod still reporting RUNNING cannot yield another
live planned-caller observation.

Full repository unit tests and vet pass. Related Node/command/Fleet race tests
pass. The complete ordinary CPU sandbox campaign passes in `8.71s`. Its wrapper
now checks that all 12 named top-level tests actually pass and rejects any skip,
so a missing/renamed test cannot silently shrink the reported selection.

The Linux static race campaign also executes all 12 top-level tests without
skips or race reports. The final-source planned-caller fault suite takes `0.98s`;
the actual CRI portion, now including `planned-owner`, takes `15.17s`. The pinned environment
remains Linux `6.10.14-linuxkit`, arm64, Docker `28.3.2`, containerd
`v2.3.1 / 64b425cf570b3b8dd1d4cc46da7c1fce65c6651a` and runc `1.4.2`.
Standard full lint and Linux integration-tag Node/command lint pass with
Go `1.26.7` and `golangci-lint v2.13.1`. The changed integration package's vet and
`--new-from-rev=3330824` integration-tag lint also pass; that incremental lint
result does not claim a clean full-repository integration-tag sweep.

The existing `TestWorkerBootstrapBindingCommandUsesCommittedRegistryIdentity`
now feeds its actual PostgreSQL-backed, mTLS-delivered Node command receipt into
the new verifier. The resulting plan preserves the Registry journal pair and
matches Fleet's member manifest; an altered backend command rejects. The
existing replay, unauthorized-principal, response-loss and no-mutation checks
still pass. This integration takes `4.62s` and includes the existing bound CPU
Runtime startup test, but the new plan observation is not installed as its
startup gate. Unlike the real-CRI fixture above, this test exercises the actual
Registry/command path; it does not exercise Kubernetes or CRI.

## Reproduction

```sh
go test ./internal/nodeagent -run '^TestRuntimeLaunchPlan' -count=1
go test -tags=integration ./internal/integration \
  -run '^TestWorkerBootstrapBindingCommandUsesCommittedRegistryIdentity$' -count=1
VELA_TEST_CONTAINERD_IMAGE=sha256:3489c7674813ba5d8b1a9977baea8a6e553784dab7b84759d1014dbd78f7ebd5 \
  go test -tags=integration ./internal/nodeagent \
  -run '^TestRuntimeContainerdSandbox$' -count=1 -v
```

Use the static Linux race build command from the
[caller evidence](node-runtime-caller-evidence-2026-09-06.md#validation), with
the same pinned Go image, resource limits and private containerd sandbox. The
expanded test selection is:

```sh
'-test.run=^Test(RuntimeContainerdProcessEvidence|RuntimeCallerAuthenticatedMessage|RuntimeCallerRejectsInvalidMessages|RuntimeCallerDeadline|RuntimeCallerProcessParser|RuntimeCallerContainerCRI|RuntimeContainerCallerCorrelation|RuntimeContainerCallerRejectsNonInit|RuntimeLaunchPlanAuthenticatesCompleteConfiguration|RuntimeLaunchPlanRejectsUnboundHistory|RuntimeLaunchPlanPreservesMemberAndAUXTopology|RuntimePlannedCallerCorrelatesTrustedPod)$'
```

This campaign needs no network, host runtime socket, model weights or GPU. The
static binary remains necessary for the nested empty rootfs. The existing glibc
NSS linker warnings do not establish DNS/user-database compatibility. Only
task-created binaries/directories and containers are removed; local test logs
remain under `/tmp/vela-runtime-launch-plan-*`.

## Remaining startup chain

Neither signed intent nor a caller's matching manifest declaration proves the
effective executable, mount content, environment, hooks or external-writer
boundary. Kubernetes spec equality and CRI image references do not replace that
proof. The next implementation must authenticate the supported effective launch
path, assemble mutually authenticated Node/Runtime endpoints, and durably bind
the exact process incarnation to actual journal ownership and the retained
schema-6 startup nonce before backend factory dispatch.

Independent exact-owner retirement must also survive lost responses, Node
restart and missing runtime metadata without treating absence as quiescence.
`UNRESOLVED` and `LEGACY_UNKNOWN` still allow only recovery after first backend
startup. Normal durable restart availability, durable unhealthy/receipt recovery,
bounded history reclamation, renewal write cost and Fleet durable activation
remain open. This increment does not complete Vela's overall correctness audit.
