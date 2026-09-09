# ProductionAgent control-session reattach evidence

Date: 2026-09-09  
Branch: `feature/vela-mock-hardening`  
Implementation commit: `791f7a3`  
Evidence class: `LOCAL_CPU_MOCK_POSTGRES_CONTROL_REATTACH`

## Command

```text
VELA_RUN_CPU_MOCK_CAMPAIGN=1 \
VELA_CPU_MOCK_TIMEOUT=60s \
go test -tags=integration ./internal/integration \
  -run '^TestCPUMockExactCacheProductionLoopCampaign$' -count=1
```

The run uses PostgreSQL 17 in Testcontainers, four persistent CPU mock Runtime
processes, the real `ProductionAgent.Run` loop, the durable Worker stream, and
the local exact-version object store.

## Fault and expected invariant

The control fixture drops the response to one accepted
`CommitStageMaterialization` operation. The server has already accepted the
request, so retrying it on the same stream must be rejected as a duplicate.
Recovery must therefore close the old control stream, establish a new session,
rebuild the `StreamAgent`, and replay the same durable request identity from the
materialization journal. A stale command-consumer error from the old stream must
not trigger a second replacement.

## Observed result

The campaign passed. Source and target stages were driven by the same
`ProductionAgent.Run` loops and covered:

- source cache miss/admission and target cache hit/reuse;
- four source physical stages and two target physical stages;
- TransferTicket consumption and materialization publication;
- one committed-response loss per resident Worker and durable replay;
- one visible completion and one Charge per Job;
- zero queued Jobs, running Jobs, active allocations, active leases, scratch
  bytes, and unresolved materialization journal records at the terminal sample.

The implementation uses a stream-tagged command error. Errors from a retired
stream are ignored after replacement; only the current stream can terminate the
ProductionAgent loop or request another recovery.

## Evidence boundary

This proves the ProductionAgent and durable stream recovery semantics in a
PostgreSQL/CPU mock and loopback control assembly. It does not prove production
Node/Fleet network replacement, containerd/CRI custody, power-loss recovery,
remote object storage, sustained arrivals, GPU execution, or any Production
Gate. Those remain separate validation items.
