# PostgreSQL telemetry unavailable

Check the CNPG PodMonitor targets and the `vela-postgres` primary/replica Pods.
Use CNPG status and replication lag before restarting anything. Recovery and
failover follow the CNPG backup/PITR runbook; metrics restoration alone is not
evidence of database availability.

The live PITR procedure is `hack/verify-cnpg-live-pitr.py`; use a new run
directory and inspect any running PID/receipt before starting another drill.
Do not reuse the obsolete `/tmp/pitr-drill.sh`. The procedure uses a unique
schema, a CPU-only isolated restore, committed marker transactions, and a
timestamp target. Allow the CNPG operator to reach the restore's TCP8000 status
endpoint without opening database ingress. See
[`stateful-recovery-validation-2026-09-14.md`](../stateful-recovery-validation-2026-09-14.md)
for the measured 145.8-second restore, on-site policy correction, and cleanup.
