# Distributed traces

Applications send OTLP to `otel-collector.monitoring.svc` on ports 4317/4318.
Two collectors run on the management nodes and export to Tempo. Tempo keeps
48 hours of traces on a 30Gi Longhorn claim. Current application code supports
OTLP over HTTP using the environment template at
[`application-tracing.env.example`](../../deploy/observability/application-tracing.env.example).
Tracing is disabled unless `VELA_TRACING_ENABLED=true`; enabled tracing requires
an explicit HTTP(S) endpoint. The generic endpoint appends `/v1/traces`; the
optional `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT` is a complete traces URL. Use TLS
and an explicitly trusted CA when exporting outside the protected cluster
network. Do not expose OTLP receiver ports as public application APIs.

MarsLab Control enabled this configuration at 2026-09-15 04:27 CST through
`deploy/environments/marslab/vela-control/tracing.patch.json`. Both Control Pods
and both APISIX gateways were verified through the existing Collector and Tempo
using four unauthenticated read requests. The site image and literal environment
entries are recorded with that rollout; the existing immutable material remains
bound. See [live evidence](../application-tracing-validation-2026-09-15.md).
Other host and isolated application processes still need their own activation.

The Control, Fleet Controller, Node Agent, Stage Worker Agent and ModelRuntime
entrypoints each supply a fixed `service.name`. Names cannot be overridden by
`OTEL_SERVICE_NAME`; arbitrary `OTEL_RESOURCE_ATTRIBUTES` are removed at export.
The collector adds the platform environment. HTTP records only method, route
pattern and status; gRPC records registered protobuf service/method and status
code. Raw URL paths, query strings, payloads, authorization values, panic/error
messages, baggage and vendor tracestate are excluded. Only validated W3C
`traceparent` is propagated; trace IDs are correlation data, never authority.

`OTEL_TRACES_SAMPLER_ARG` defaults to `0.1` and accepts finite ratios 0–1.
Root spans use that ratio; children honor the incoming sampled flag. This is
not a hard per-process trace rate limit. Export uses a queue of 2,048 spans and
batches of up to 256, with a 1s interval and 5s export/shutdown deadlines. Queue
overflow and collector outages can lose traces but do not block requests.
Provider shutdown flushes with an independent context after process cancellation.

HTTP streaming flush behavior is preserved. A gRPC stream produces a single
span when it ends; a persistent control stream does not provide a span per job
or StageRun. The schema-97 candidate persists `jobs.origin_trace_parent` and
connects Outbox publish attempts and Inbox processing/redelivery with separate
producer/consumer spans. It records `messaging.message.id`, `vela.job.id`,
`messaging.nats.message.delivery_count` and `vela.inbox.applied` for diagnosis;
these IDs are not Prometheus labels. Idempotent admission retains the original
request parent, and missing context starts a new trace. See the
[independent three-replica validation](../async-message-tracing-validation-2026-09-15.md).
The schema-98 candidate also carries that parent in StageAssignment and persists
it in the worker admission journal. Per-operation spans cover assignment build,
worker start/status/heartbeat/reattach/stop/failure, output seal/materialization,
and ModelRuntime prepare/start/status/cancel/seal. They carry `vela.job.id`,
`vela.stage_run.id`, `vela.stage_attempt.id`, and `vela.worker_instance.id`;
runtime spans also record their application decision, including rejections that
return a successful gRPC transport status. The watchdog uses the first Prepare
parent with its independent cancellation budget. These spans do not measure
the model driver's internal inference duration. See
[Stage recovery validation](../stage-runtime-tracing-validation-2026-09-15.md).

Readers accept worker admission journal schemas 5 and 6. The first valid traced
admission writes schema 6 atomically; replay and renewal retain its first parent.
Upgrade all journal readers/writers before delivering traced assignments. Old
readers reject schema 6; disabling tracing does not downgrade journals, and
deleting journal history is not a rollback procedure.

Production remains on schema 96 and has not adopted these candidates. Apply the
reviewed migrations 97 and 98 before the new Control image; do not run its queries against
schema 96. Database name must come from active CNPG configuration (currently
`app`), not from the application name. Do not use a transport canary or independent
message test as a model execution or business SLO receipt.

The [2026-09-15 retention inventory](../telemetry-retention-capacity-2026-09-15.md)
found about 2.57MiB used, predominantly from finite trace validation. This is
neither a production traffic estimate nor evidence of a complete 48-hour window.

Database-role logs include the request's trace/span IDs when a valid context
exists, alongside the independently generated request ID. No trace IDs are
added to Prometheus labels. Use Grafana's `Vela Tempo` datasource to inspect a
trace, then search Loki for the trace ID; existing datasource links and the
service map depend on the relevant application log/metric data being present.

For rollout, build and pin the intended release images, include the tracing
variables in new immutable configuration revisions, and verify outbound access
from those workloads to Collector port 4318. The example does not alter the
current deployed images/configuration. Node host services and isolated model
processes need their own approved endpoint and environment; cluster DNS in a
Pod is not evidence that a host or isolated process can resolve it.

The Linux canary is built with
`CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -o /tmp/vela-trace-canary ./hack/trace-canary`.
On a management node, run `hack/verify-application-traces.py` with `--binary`,
the expected `--sha256`, and a new root-private `--output` directory. It starts
two UID 65534 processes on loopback, verifies success/error/unauthorized traces
through the real Collector and Tempo, and removes all probe processes,
listeners and temporary runtime files. It creates no Kubernetes objects,
routes or application credentials. The source and binary checksum must be
archived with its receipt; it is a validation artifact, not a canonical release.
