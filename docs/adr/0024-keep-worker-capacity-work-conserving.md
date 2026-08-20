# Keep Worker capacity work-conserving

Vela does not hold READY Workers idle as a hard failure reserve. Every compatible Worker may run ordinary Jobs, while risk-adjusted Admission, immediate admission tightening after Worker loss, a bounded retry lane, and cross-preset use of all certified compatible ExecutionProfileRevisions provide a Soft Failure Reserve without sacrificing GPU utilization.

## Consequences

When every Worker is busy, a failed long-running Attempt may wait for another Job to finish before retrying; Vela guarantees durable recovery within Retry Budget, not immediate replacement capacity. Preset SLO certification and monthly results include fault-induced queue delay rather than excluding it or hiding it behind nominal capacity.
