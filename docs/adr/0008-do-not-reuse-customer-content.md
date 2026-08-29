# Do not reuse Customer Content by default

Vela processes Customer Content only to execute and deliver Jobs, satisfy audit obligations, or provide explicitly authorized support. It does not use Customer Content for training, benchmarks, shared quality datasets, or routine operator inspection; any future reuse requires a separate contract, explicit authorization, and isolated Project, while non-content operational telemetry remains available for service operation.

## Implementation Status

Partial. Request content and exact-version Artifacts have policy snapshots,
expiry, and irreversible deletion paths. Exceptional support access requires
dual control, an exact purpose/scope/target, short-lived exact-version delivery,
and immutable audit evidence that excludes Customer Content and storage identity;
deletion and retention remain authoritative during signing. Worker scratch,
Local Recovery State, and Runner outputs are private, bounded, exact-identity
bound, independently revalidated, and terminally cleaned. Opt-in failure debug
dumps require exact ProjectAdmin authorization, remain isolated from Artifacts
and Charge authority, use short-lived exact-version reads with safe audit, and
expire or delete under retention and Content Deletion (`6603c36`). Policy
enforcement outside implemented services and production object/scratch
isolation receipts remain unimplemented. Slice 31 adds committed-only
off-cluster backup deletion and post-authority restore replay. Slice 32 adds
committed-only exact-version replication, immutable copy evidence, separate
read/write/delete authorities, response-loss recovery, and serialization with
Content Deletion. Live object-store/network fault and deployment receipts remain
Production Gates.
