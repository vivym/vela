# Recompute after Worker node loss

Launch keeps Encoder, DiT, and VAE Local Recovery State on Worker NVMe for same-node process recovery but does not create cross-Worker Durable Checkpoints. Loss of the Worker node or its NVMe makes the Attempt LOST and causes a compatible Worker to recompute from the beginning within Retry Budget; completed output begins multipart upload immediately, but an incomplete upload never becomes an ArtifactSet.

## Consequences

A late node failure may waste almost a full inference run but does not create another customer Charge. Vela adds a Durable Checkpoint only after measured failure-stage distribution, latent size, checkpoint I/O, recovery success, and throughput impact show lower total cost than recomputation.
