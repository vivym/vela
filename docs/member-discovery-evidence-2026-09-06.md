# Authenticated member Runtime discovery before assignment

Local CPU/mock increment over `30b9256`. Worker/Runtime journals remain **4**,
materialization **2**, database **90**, launch/Fleet **2** and floor RPC **1/2**.
The member protocol adds an RPC without changing existing field numbers.
Production Gates remain **0/9**. No GPU or remote deployment.

## Assembly dependency

Durable Worker admission and terminal recovery need independently trusted current
Runtime identities for every member. The local Runtime UDS already supports
discovery without readiness. The remote member client previously always returned
Unimplemented, and its existing operations require an assigned execution or
signed terminal disposition. That prevented startup from collecting peer
identities independently of assignment delivery.

`StageWorkerMemberService.DiscoverRuntimeIdentities` now forwards the exact
WorkerInstance/member IDs and epochs to the member's private Runtime UDS. The
client uses the existing TLS certificate and pinned SPIFFE digest. Before any
Runtime RPC, the server authenticates the peer against the independently
configured deterministic leader digest and validates the requested local scope.
No StageAuthority or terminal disposition is supplied or synthesized.

`MemberBinding.IdentityDigest` carries trusted SPIFFE digests. Discovery requires
them for every configured member. Partial, malformed or zero digests reject
construction. Omitting all digests disables discovery while leaving existing
signed operations available. Leadership is derived from UUID order, independently
of configuration order. Configured digests and local identities are cloned.
The default Worker command now propagates its already configured member identity
digests into this server boundary.

Both transport hops validate bounded, nonempty, unique residency sets, canonical
nonzero UUIDs, positive epochs, nonzero topology digests, consistent
Worker/member/topology scope and absence of unknown fields. The receiving server
also requires the complete current UDS result to equal its pinned startup
identity set, regardless of result ordering. Missing/extra profiles or changed
Runtime epochs/routes reject; an already running Worker must rebuild and verify
its topology after such a change. Late canceled responses are not returned as
successful discovery, and forwarded requests/results are isolated by cloning.

Discovery is an authenticated observation. Callers still need to match it to
approved launch routes and full Worker/member/device topology before building
admission bindings or current recovery readers. It does not establish readiness,
durable journal ownership, execution eligibility or historical writer exclusion.
The server does not initialize state, raise a floor, cancel work or drain a writer.

## Verification

Passed full `go test ./...`, race checks for `internal/stageworkermembertransport`,
`internal/stageworkeragent` and `cmd/vela-stage-worker-agent`, `make lint`
(0 issues), integration `go vet`, `make generate-proto`, protocol compatibility
tests and `buf breaking --against '.git#ref=30b9256'`.

Focused tests cover configured leader authentication, disabled/incomplete trust,
wrong target/Worker/member epochs, malformed or conflicting responses at both
hops, missing/extra/currently changed profiles, bounded payloads, multiple
profiles, canceled calls and mutable request/response/configuration isolation.
The command smoke verifies that the member server receives its configured
SPIFFE digests.

The actual mTLS-to-UDS test discovers identities before assignment, admits a
durable Runtime execution, shuts down and reopens the same journal at the next
Runtime epoch. Discovery succeeds repeatedly while readiness still reports
`ErrExecutionDrainUnproven`. A nonleader with a valid TLS certificate is rejected.
Journal bytes remain unchanged and no backend cancel/close occurs during
discovery. This proves transport availability during recovery, not writer drain.

Linux arm64 passes
`^(TestMemberDiscovery|TestMemberFloor|TestClientWrapsExactTargetAndRejectsUnscopedDiscovery)`
in the member binary and `^TestProductionRuntime` in the Worker command binary.
Both run as UID/GID 65534 with no network, read-only root/binary mounts, all
capabilities dropped, no-new-privileges and a private `/tmp` tmpfs. Image:
`sha256:18cfe3ef5e6815560c98237d6216d1e5119702fb0f3894c8785dd58b8bbe5d73`.

## Remaining integration

Old member binaries return Unimplemented for the new RPC; callers cannot treat
that as an empty trusted identity set. Old configured servers without member
digests reject discovery until trusted topology is supplied. No stored proof
format changes in this increment.

The Worker command still needs to collect these peer observations, match complete
approved launch topology and construct its durable admission/Stream/retirement
path. It also lacks an offline admission-journal operation and independently
authorized first-use provisioning. Runtime replacement while a Worker remains
running requires explicit topology reconstruction. Default scratch retention,
unknown writer/receipt recovery, bounded checkpoint reclamation and external
backend containment remain open. This increment does not enable default
automatic scratch retirement or prove sustained Worker throughput.
