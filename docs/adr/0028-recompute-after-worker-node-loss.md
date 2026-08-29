# Recompute after Worker node loss

Launch keeps Encoder, DiT, and VAE Local Recovery State on Worker NVMe for same-node process recovery but does not create cross-Worker Durable Checkpoints. Loss of the Worker node or its NVMe makes the Attempt LOST and causes a compatible Worker to recompute from the beginning within Retry Budget; completed output begins multipart upload immediately, but an incomplete upload never becomes an ArtifactSet.

## Repository conformance

Slice 30 exercises the production Worker Agent, mTLS WorkerControl transport,
PostgreSQL authority, and a versioned MinIO Artifact Store. It proves that an
Agent process loss after the first multipart part resumes the same
Worker/epoch/Attempt/fence and existing multipart session without rerunning the
Runner. It separately detaches the first Worker's local recovery root, reconciles
the expired execution Lease to a `LOST` Attempt, assigns a distinct Worker a
higher-fence Attempt 2, and recomputes all required output from an empty local
root. Both paths produce exactly one Visible Completion, ArtifactSet, and Charge
(`864c134`, review closure at `5a9bad6`).

This is direct repository evidence for Acceptance Scenario 10, not a physical
H3/NVMe/XFS exercise. ADR status remains `Partial` until the live failure
exercise and versioned Launch Receipt exist.

## Consequences

A late node failure may waste almost a full inference run but does not create another customer Charge. Vela adds a Durable Checkpoint only after measured failure-stage distribution, latent size, checkpoint I/O, recovery success, and throughput impact show lower total cost than recomputation.
