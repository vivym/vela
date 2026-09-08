# Durable sealed output receipt and historical replay

The `b8f9964` implementation retained `LocalMaterializationReceipt` only in
Service memory. The execution journal persisted a drain checkpoint but not the
receipt returned to the Worker. After Runtime process loss, the original
receipt, including its original timestamp, could not be recovered from that
journal. Node-private storage alone would not repair this missing evidence.

Runtime journal **schema 7** now records the sealed receipt and its exact signed
authority before attempting writer drain. A successful `SealOutput` response
requires both the receipt and the matching drain checkpoint to be durable.
Worker journal 5, PostgreSQL 94, Registry binding 1 and release bundle 3 are
unchanged. This is a local implementation increment, not a deployment.

## Behavior and ordering

1. After the backend returns sealed output, retain the receipt in the active
   execution so a later failure cannot cause a second backend Seal.
2. Persist the complete receipt with the exact accepted/confirmed authority in
   the existing locked admission journal. A write/fsync uncertainty makes the
   live owner require recovery and withholds the receipt response.
3. Ask the existing backend drainer to close the exact execution's writer scope,
   then persist its matching checkpoint. Pending/failed drain keeps the receipt
   non-replayable; receipt presence alone does not establish writer exclusion.
4. Return the receipt and release the active slot only after successful drain
   persistence. A still-live pending drain may finish on retry without Seal.
5. `Supervisor.SealOutput` first checks for an exact persisted receipt plus
   matching drain. Historical signatures may read that record after expiry,
   a floor advance or Runtime epoch/profile change. This read never enters
   a backend, renews execution, changes journal state or constructs a new receipt.

Historical replay returns `REPLAYED`, the original authority digest, original
Runtime identity and exactly the original receipt bytes/timestamp. The existing
Worker `Agent.SealOutput` adapter consumes this response through public gRPC;
no protobuf wire format changes are required. An unrecorded renewal, another
allocation, invalid signature or canceled call cannot obtain the receipt.
Prepare/Start authority and current residency checks are unchanged.

The journal owner and complete snapshot observer share receipt validation.
It checks bounded canonical protobuf, unknown fields, timestamp validity,
manifest hash, lease-derived receipt UUID and exact accepted/confirmed history.
A seal freezes candidate history; its drain must name the same envelope.
The receipt is immutable, including when a backend drain or directory fsync
previously failed. Zero byte size retains the existing Runtime contract;
the Worker independently requires a positive materialization size.

## Explicit migration

Ordinary recovery rejects schema 6. Offline migration requires the explicit
`vela-model-runtime journal --action upgrade-v6` operation with the same trusted
manifest, verifier and original directory. It preserves original UUID, storage
identity, restrictions, history and backend lifecycle. A schema-6 journal cannot
contain a schema-7 seal field. Lost old receipts remain unknown; migration
does not create them or mark an unresolved backend unstarted.

Existing schema-2/3/4/5 upgrades now target 7 and retain their prior restrictive
migration behavior. Registry-bound normal startup still forbids initialization
and all upgrade flags, including `UpgradeV6`. No normal startup infers first use
or migration from missing state. The CLI's positive schema-6 upgrade and default
rejection both pass executable command tests.

## Verification

- Full `go test ./...`, `go vet ./...`, ordinary golangci-lint 2.13.1 and
  `make test-cross` pass. Related Worker/Runtime/bootstrap tests pass.
- New host race tests cover exact receipt replay through the actual Worker
  gRPC adapter after authority expiry and epoch change, confirmed renewal
  binding, immutable replay copies, unknown request rejection, receipt-before-
  drain ordering, write uncertainty and explicit schema-6 migration.
- Ten corruption cases are rejected by both live recovery and complete snapshot
  validation: manifest, digest, UUID, size, timestamp, unknown protobuf field,
  authority, confirmation, drain digest and a seal injected into legacy schema.
- Five actual subprocess crashes cover receipt rename/sync, drain rename/sync,
  and response construction. Every crash exits without deferred journal cleanup.
  Recovery retains the original journal identity, withholds incomplete outcomes
  and replays only the exact completed receipt without backend entry.
- Native Linux/arm64 static race binaries, built with Go 1.26.7, pass **47
  behavioral main tests plus four helper entrypoints**. The selection includes
  seal, drain, renewal, snapshot, recovery, lifecycle and non-admission histories.
  It runs as UID/GID 10001 in a disposable CPU container; no skips or race reports.
- The root Node/actual non-root PID-1 Runtime startup exchange also passes all
  four scenarios against schema 7: fixture permit, denial, lost response and
  uncertain Node record. These remain mock decisions, not a production issuer.
- The real PostgreSQL/Registry/protected-provisioning command campaign passes
  under host race instrumentation in **69.262 package seconds**, including
  command interruption and original pair/claim recovery. No remote CI ran.

The production implementation was unchanged during the final validation set.
The final added renewal test and positive CLI migration assertion were also run
separately; the final native race binary includes the renewal test.

## Evidence

Logs are in `/tmp/vela-startup-validation.syiSMk/`:

| File | SHA-256 |
| --- | --- |
| `sealed-receipt-unit.log` | `e2f7dcbbc18dfab01d0bb098eb075f6965d187a0e2f15b4958cb7ef9d39582e2` |
| `sealed-receipt-linux-race.log` | `f07e793a50f203acf40e42d3a10ef104205e257ffea67f243e809c7e51cd50f6` |
| `sealed-receipt-node-exchange.log` | `ba40b9afb2c610ae8eda00c51cc178801cd0dfb737a05898b7abe13fa5f96689` |
| `sealed-receipt-postgres.log` | `c562186611023cc395d72d2899f148660d0df7f3b69fb6a206516990d3ed557d` |
| `sealed-receipt-host-race.log` | `1d6407013015d2ddae6e5786acf80c216ad73bfce7e2a477ebb377345c77f68b` |
| `sealed-receipt-renewal-race.log` | `105ed4e5ed2256481e7a6d26777d8415850125ea31ba009a4fe8678d5b51ef53` |
| `sealed-receipt-command.log` | `63cad830d063f51202c31a955b7749f730b5a87a0579c63585c81fdaa3bf1c5d` |

Final Linux image:
`sha256:186746df1223cd823eb2c80891e99445c921602841383ff0f47d4b319a5edadf`.
`/modelruntime.test` and `/nodeagent.test` contain this source. Auxiliary
`/runtimechannel.test` is from the earlier EINTR checkpoint and was not used for
this receipt validation.

## Remaining full-system obligations

A persisted receipt is metadata. It does not prove the current existence,
integrity or accessibility of its local output bytes. Worker materialization
must still validate the source and use the existing Control/StageArtifact
authority; replay does not publish output or charge the customer.

The existing backend drain contract is not independent Node containment or
device-quiescence proof. Process-crash tests retain the host/filesystem instance
and are not power-cut durability tests. Journals still use the existing file
ownership contract; original custody/provenance, production Node startup grants,
effective launch/Fleet activation and safe replacement remain open.

The seal-before-persistence crash window can still leave unknown backend output
without a durable receipt, and a receipt without drain stays withheld after
restart. Neither case may infer fresh execution or writer retirement. Complete
recovery of these uncertain cases needs the independent owner/writer protocol.
Input/backend exclusion, unhealthy Worker persistence, scratch retirement,
capacity release, bounded execution-history reclamation and a latest-source
complete mock/open-loop campaign also remain required. History is still bounded
by admission backpressure, not reclaimed by this increment.

Production Gates remain **0/9**. No GPU work, push or remote deployment occurred.
