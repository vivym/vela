# Runtime launcher pidfd release race evidence (2026-09-13)

The production launcher wrapper now registers a buffered `SIGCONT` handler
before publishing its self pidfd. This preserves an observer release that
arrives before the wrapper reaches the wait, and a bounded two-minute wait
fails closed if the observer never releases custody.

The Linux amd64 regression was run as root on `marslab`:

```text
TestProductionPIDFDOfferBindsKernelSenderToTask/self: PASS
TestPIDFDOfferReleaseWaitFiltersSignalsAndTimesOut: PASS
```

The first test sends `SIGCONT` through the original parent pidfd immediately after
the receiver accepts the offered descriptor, then verifies that the wrapper
execs `/bin/sleep` in place. Temporary test binaries and `/exec-observer`/
`/exec-probe` fixtures were removed after the run.

The second test confirms an unrelated signal does not release the gate and that
the bounded wait returns a timeout instead.

This closes the wrapper lost-wakeup race. It does not constitute a production
Node/Fleet/ModelRuntime composition receipt.
