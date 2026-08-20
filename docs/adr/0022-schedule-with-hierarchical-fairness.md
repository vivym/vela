# Schedule with hierarchical fairness

Vela selects Customer Organization, Service Class, Project, Job, and compatible Worker in that order. Organizations and Projects receive weighted deficit fairness with hard queued and running limits, Project-local Job ordering combines predicted runtime, FIFO, and bounded aging, and a Protected Lane prevents starvation without allowing request-level priority to bypass contracted ServiceClassRevision.

## Consequences

Scheduling is work-conserving and may lend idle capacity without preempting a running long Job or creating a CapacityReservation. Retry retains the original Job's waiting age but uses a separately capped lane, so a failure storm cannot consume the entire Worker pool.
