# Post the cancellation Charge when cancel wins

Cancellation before RUNNING atomically produces CANCELED and releases Credit Reservation. Cancellation in RUNNING or FINALIZING atomically increments the fence, produces CANCELING, posts the full quoted Charge, consumes Credit Reservation, and writes a Stop Outbox event without waiting for Worker acknowledgement; acknowledgement or Lease expiry later produces CANCELED.

## Consequences

Visible Completion and Customer Cancellation race through one versioned compare-and-swap. Completion winning first returns `AlreadySucceeded` with the ArtifactSet; cancellation winning first makes late completion stale and unable to publish Artifacts or create another Charge, even if partitioned compute continues physically.
