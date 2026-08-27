# Worker process and node-loss recovery conformance

Date: 2026-08-27

Status: Repository conformance implemented at `5a9bad6` (initial behavior at
`864c134`, fixed-point Standards and Spec review closure at `5a9bad6`, complete
integration-fixture closure at `4d2bb7f`).

This slice adds repository-level conformance evidence for ADR 0028 and
Acceptance Scenario 10. It exercises the production Worker Agent, WorkerControl
mTLS transport, PostgreSQL authority, and a versioned MinIO Artifact Store. It
does not exercise a real H3 Worker, XFS project quota, physical NVMe loss, or a
Production Gate Launch Receipt.

## Scope

The conformance exercise covers two distinct recovery contracts:

1. A Worker Agent process fails after the first multipart part reaches the
   Artifact Store. A replacement Agent instance with the same Worker, epoch,
   Attempt, fence, recovery root, and output root reopens the durable local
   finalization state and existing multipart session. It does not call the
   Runner again and uploads only the remaining video and thumbnail parts.
2. A Worker node fails while the first Attempt is running. Its Local Recovery
   State is written to the first Worker root and that root is detached to model
   inaccessible NVMe. Lease-expiry reconciliation marks the first Attempt
   `LOST`; a different registered Worker receives Attempt 2 with a higher fence
   and an empty recovery root, then runs `Prepare` and `Start` and recomputes all
   required outputs.

Both paths must end with one authoritative Visible Completion, ArtifactSet, and
Charge. The node-loss path must retain the first Attempt as `LOST`; detached
Worker-local state must remain inaccessible to and unchanged by the replacement
Worker.

## Public seams

The tests use established production boundaries:

- `workeragent.Agent.RunOnce` for execution, recovery, multipart upload, and
  finalization;
- `workerrecovery.Manager` for exact Worker/epoch/Attempt/fence local state;
- `workertransport.Client` and `workertransport.Server` over mTLS with the exact
  registered Worker SPIFFE ID;
- `workercontrol.Service` for Assignment, Lease, failure reconciliation,
  Artifact, Visible Completion, and Charge authority;
- `fleet.Service` for current capacity authority; and
- `artifactstore.S3Store` with MinIO bucket versioning and real signed multipart
  PUT requests.

Direct database fixture writes seed the replacement Worker and make the expired
Lease eligible for deterministic reconciliation. They do not manufacture the
replacement Assignment, Attempt, fence, finalization, ArtifactSet, Visible
Completion, or Charge.

## Signed upload headers

The real MinIO path exposed that S3 presigning includes `Host` in the signed
header set while Go forbids setting it as an ordinary request header. The
WorkerControl response now validates that a signed `Host` exactly matches the
presigned URL authority and then omits it from transferable headers. Artifact
and debug-dump uploads share this validation. Other required signed headers are
retained, and the Worker uploader continues to reject caller-supplied `Host`,
authorization, proxy-authorization, and hop-by-hop headers.

Unit coverage proves matching `Host` is filtered for Artifact and debug-dump
claims and a mismatched signed `Host` fails closed. The MinIO integration test
proves the resulting signed request remains accepted by the object store.

## Verification

Required repository checks are:

- the two focused Worker recovery integration tests;
- `go test ./internal/workertransport ./internal/workeragent`;
- the complete integration suite;
- `make generate`, `make test`, `make lint`, `make test-cross`, and
  `make validate-deployment`; and
- a fixed-commit Standards and Spec review before this evidence commit.

After the review fixes, the Worker transport and Agent unit packages passed, and
both focused PostgreSQL + MinIO integration tests passed together in 8.901
seconds. Standards review found no hard violation; the shared control fixture,
Agent construction clump, count naming, and SPIFFE identity type findings were
closed. Spec review found no missing, partial, out-of-scope, or
implemented-looking-but-wrong behavior.

The first complete integration run exposed an existing shared startup-fixture
regression and a JetStream leader-election race rather than a Worker recovery
failure. Commit `4d2bb7f` gives current control, adjacent N-1, and older
fixed-point probes distinct configuration contracts, waits for an explicit
one-replica-offline stream quorum before publish, and allows Docker Desktop up
to two minutes to report the mapped PostgreSQL port. All five focused failures
then passed in 50.113 seconds, and the complete integration suite passed in
1477.864 seconds with both Worker recovery tests passing in-suite.

`make generate` completed without generated drift. `make test` passed all Go
packages and 44 Runner tests; `make lint` reported zero Go issues and a clean
Runner Ruff check; `make test-cross` passed the Linux/amd64 CGO-disabled build;
and `make validate-deployment` passed all rendered deployment contracts.

Container images use matching local platform copies when present. The local
registry proxy may be used when an image is missing or a pull fails. Go build
cache may be cleared between large verification phases; the module cache remains
retained unless disk pressure requires a separate decision.

## Evidence boundary

This slice advances Scenario 10 from partial implementation evidence to direct
repository conformance evidence. ADR 0028 remains `Partial`: a real H3 Worker
with host XFS project quota, physical node/NVMe-loss injection, measured
recompute behavior, and a versioned Launch Receipt remain external work.
Production Gates remain `0/9 PASS`.
