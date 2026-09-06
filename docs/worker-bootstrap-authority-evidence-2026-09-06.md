# One-time Worker journal bootstrap authority

This CPU/PostgreSQL increment follows `29f4cab`. Database schema is now **91**.
Worker journal remains 5, Runtime journal 4, materialization 2, launch/Fleet 2
and floor RPC v1/v2. Production Gates remain `0/9 PASS`.

## Authority boundary

A repeatedly applied ResidencyPlan cannot by itself grant repeated journal
initialization. A missing file or empty hostPath is not evidence of first use.
The new Registry operation consumes first-use authority before the local
provisioner initializes either journal.

`WorkerBundleActuationManifest` returns the existing canonical digest preimage
after validating the bundle. It uses the same encoder as
`ComputeWorkerBundleActuationDigest`; the approved digest format is unchanged.
PostgreSQL recomputes SHA256 over those complete bytes, matches the Registry's
approved bundle layout and plan, and derives the target member epoch/node from
the matched manifest. A caller-supplied digest alone is insufficient. The
manifest is bounded to 4 MiB and stored once per bundle, independently of the
number of member claims.

`fleet.Service.ClaimWorkerBootstrap` requires a stable request UUID, exact
Worker/member target, expected Worker epoch, canonical bundle manifest and
provisioner actor label. PostgreSQL locks the Worker against concurrent
observation/fencing and permits new claims only while it is unobserved
PROVISIONING authority: no old Worker epoch, member, residency, device binding,
observed timestamp or advanced Control session. The target must match the
approved Worker profile, pool, member count and manifest membership.

Only the transaction inserting the claim can return `Fresh=true`. Exact retries
return the original metadata with `Fresh=false`, including after a service
restart. A different request UUID cannot reclaim the same member. The globally
unique member claim also cannot be reused at a later Worker epoch. Changed
request ownership, scope or approved manifest rejects.

This result is a one-time permission for the currently executing provisioner,
not a reusable signed token to put into an ordinary restart configuration. A
lost response or ambiguous failure requires history inspection and local
recovery. If the claim committed but initialization did not complete, preserve
that uncertainty; neither a retry timer nor a missing directory grants another
initialization. Reconciliation/replacement requires separate authority.

## Prepared journal receipts

`RecordWorkerBootstrapReceipt` immutably binds the consumed claim to the reported
Worker and Runtime journal IDs and their scope digests. Exact replay preserves
the original recording time. Changed journal identity, scope or actor rejects;
journal IDs cannot be reused within their respective journal kind.

These are trusted provisioner reports. This database API does not independently
inspect files or validate a local journal's signature/history, and a receipt is
not readiness, writer drain, capacity or a production Launch Receipt. The future
node adapter must obtain the IDs/scopes from successful existing journal
preparation/recovery, then compare them again before serving.

Authorization comes from the dedicated `vela_fleet` database role. The actor
string is an immutable audit label, not independent node authentication. Broad
`vela_internal` cannot call the claim function. All three tables reject update,
delete and truncate; ordinary callers receive function access rather than direct
table writes. Down migration rejects any retained claim.

## Recovery and commit behavior

Claims and receipts use the existing deferred synchronous-quorum guard. When the
deployment requires quorum, a failed commit returns no usable claim or durable
receipt from the Go API. Standalone mock PostgreSQL retains its explicit existing
non-quorum test configuration.

The recovery Admission gate blocks new claims while allowing existing claim
observation and receipt collection. Database quiescence now counts claims without
receipts as pending authority. An unfinished initialization prevents sealing a
quiescence receipt. Once the provisioner reports the prepared pair, collection can
finish with the gate still closed.

Schema-91 recovery receipts require the additional `worker_bootstrap_claims=0`
inventory entry. The reader still accepts the original complete inventories for
schemas 72-90; it rejects a schema-91 receipt that omits the new entry. Old readers
reject the extended inventory, so use the new recovery tool for schema 91.

## Verification

Passed:

- `go test ./...` and `make lint` (`0 issues`).
- `make generate-sql` and full `make generate`: three new persisted record
  types, unchanged OpenAPI/protobuf outputs, identical sqlc output on regeneration.
- Changed-file integration-tag lint across Fleet, integration and recovery
  packages (`0 issues`); this does not erase the previously recorded broader
  integration lint backlog.
- Five new PostgreSQL tests: concurrent first use/receipt immutability; unapproved
  and previously observed scope rejection; empty migration Down/Up; commit-quorum
  failure; and recovery-gate/quiescence coordination.
- A race-enabled regression selection of **25 top-level tests**, completed in
  **103.93 seconds**, covering the new tests, existing Registry behavior, Stage
  quorum handling, recovery gates and a restore into independent PostgreSQL 17.
- `git diff --check`.

Eight concurrent identical requests produced exactly one fresh grant. Discarding
that grant and querying through a replacement service yielded no new permission.
Changing a manifest's node while retaining approved layout authority rejected
without a claim. Quorum failure exercised the actual deferred commit boundary:
the API returned an empty claim and the database retained no claim row. Receipt
commit failure returned no timestamp. The quiescence test closes/reopens the real
gate, proves a pending claim blocks sealing, then records the pair while closed
and seals the extended zero inventory.

The database fixtures use approved GPU-shaped metadata only; they launch no
Runtime, model, Pod or GPU workload. They do not initialize actual journals or
exercise node authentication. Detailed logs are in local temporary files
`/tmp/vela-worker-bootstrap-tests.jsonl` and
`/tmp/vela-worker-bootstrap-regressions.jsonl`.

## Next lifecycle work

Implement the authenticated node-side adapter and durable local operation
record, validate existing roots before consuming first use, prepare both journals
only after a fresh committed claim, and report their actual IDs/scopes. Handle
partial initialization and lost responses without issuing a replacement grant.
Normal startup must recover the recorded pair and reject missing/replaced state.
Only then should Fleet generate durable serving configuration and activate it.

Unknown historical writers, sealed receipt recovery, failed-backend containment,
bounded checkpoint reclamation and command-level successful terminal retirement
remain separate unfinished requirements. No deployment or GPU validation was
performed, and this increment does not complete the overall architecture audit.
