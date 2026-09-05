# Stage publication retention

StageArtifact materialization and final public Artifact publication use immutable
object versions and atomic conditional creation. Each Job owns a separate public
object key, including when its Stage output comes from exact cache. The immutable
`source_stage_artifact_id` retains provenance without sharing the destructive
lifecycle of the public object.

## Deletion protocol

1. PostgreSQL closes publication authority and waits for the original publication
   deadline. Active execution pins continue to protect shared StageArtifact data.
2. The retention worker deletes the authorized exact content version. Orphan
   materialization discovery freezes its resolved version before deletion.
3. The worker conditionally creates a permanent publication fence at the retired
   key. Its fixed payload is `vela-conditional-publication-fenced-v1` plus a newline,
   and its Content-Type is `application/vnd.vela.publication-fence`. The marker
   contains no Customer Content.
4. If an in-flight conditional PUT wins before the marker, the worker verifies its
   size and SHA-256 against the authorized publication, deletes that exact version,
   and retries the fence. Conflicting content or an unconfirmed fence leaves the
   deletion pending. A confirmed marker prevents subsequent conditional PUTs.
5. Only after the fence is confirmed does the worker record completion. Response
   loss is retryable: an existing marker is recognized and retained.

Client cancellation alone does not fence a service-side PUT. `Local.PutIfAbsent`
checks existence under its mutex after reading the payload; `S3.PutIfAbsent` uses
`PutObject` with `If-None-Match: *`. This protocol requires the storage provider to
enforce that condition atomically when committing the object.

The ordinary discovery path, exact-version deletion, and all-version backup purge
preserve publication fences. Backup purge also establishes a fence before reporting
success, preventing a delayed backup PUT from recreating content. Backup replication
reads only the frozen committed source version and rejects a publication marker as
an Artifact source. A completed finalization remains replayable from immutable
metadata after its intermediate content has been deleted.

## Deployment requirements

- Do not expire or remove publication fence objects with bucket lifecycle rules,
  administrative cleanup, replication cleanup, or backup restore. Their continued
  existence is retention authority, and their small storage cost is permanent.
- Keep bucket versioning enabled and use a provider with the required atomic
  conditional-write semantics. Existing provider/versioning conformance remains a
  separate deployment check; CPU and HTTP tests do not establish provider behavior.
- Retention credentials require exact-version metadata reads and deletion plus
  conditional `PutObject` for the fixed fence payload under the authorized Artifact
  prefix. Backup retention therefore needs a narrowly restricted fence-write
  capability in addition to its previous list/delete permissions. Missing permission
  fails closed with the deletion pending.
- Public copies add one source read, one conditional write, and one integrity read
  before final publication. Account for these transfers and retained public bytes in
  capacity/cost validation; customer billing remains one fixed-price Charge per Job.

## Verification

`TestPublicationFenceClosesDelayedConditionalPut` covers marker-first, content-first,
and lost-marker-response races with a store that ignores client cancellation.
`TestS3ExactDeleteAndVersionPurgePreservePublicationFence` checks the HTTP condition,
exact version identities, purge replay, and retained markers.
`TestStageGraphPublicCopyCleanupFencesPutThatIgnoresCancellation` exercises deletion
completion against an actual delayed public-copy workflow and PostgreSQL authority.
The StageArtifact lifecycle suite also checks pinned consumer deletion, orphan
cleanup, exact-version replay, cache quota recycling, and post-lock lease expiry.

These are CPU/mock correctness checks. They do not advance Production Gates.
