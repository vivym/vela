# Do not reuse Customer Content by default

Vela processes Customer Content only to execute and deliver Jobs, satisfy audit obligations, or provide explicitly authorized support. It does not use Customer Content for training, benchmarks, shared quality datasets, or routine operator inspection; any future reuse requires a separate contract, explicit authorization, and isolated Project, while non-content operational telemetry remains available for service operation.

## Implementation Status

Partial. Request content and exact-version Artifacts have policy snapshots,
expiry, and irreversible deletion paths. Exceptional support access requires
dual control, an exact purpose/scope/target, short-lived exact-version delivery,
and immutable audit evidence that excludes Customer Content and storage identity;
deletion and retention remain authoritative during signing. Policy enforcement
outside implemented services plus deletion from Worker scratch, Local Recovery
State, debug paths, and off-cluster backups remain unimplemented.
