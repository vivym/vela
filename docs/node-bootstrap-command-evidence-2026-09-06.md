# Node bootstrap command and authenticated history

This increment follows `21a1f13`. Database schema remains **92**, Worker journal
5, Runtime journal 4, local bootstrap operation 1, materialization 2,
launch/Fleet manifest 2 and floor RPC v1/v2. Production Gates remain `0/9`.

## Command boundary

`vela-node-agent bootstrap --action prepare` invokes the existing paired-journal
coordinator through authenticated Fleet RPCs. No arguments still selects the
existing root daemon; unknown command arguments now reject instead of being
ignored. Bootstrap loads no daemon configuration, probes no device, starts no
backend, and requires no remediation or Kubernetes client. It runs as the
intended journal-owner UID against the preprovisioned private layout.

Fleet address, server name, CA, client certificate/key and all preparation files
are explicit arguments. `NewWorkerBootstrapTLSCredentials` derives the expected
Node Agent identity from the same certificate object loaded into TLS. Missing,
multiple, noncanonical or non-Node-Agent URI SANs reject before connecting.
The server independently authenticates and checks registration. This avoids a
separately configured expected actor disagreeing with the actual certificate
after a first-use claim has already committed.

`prepare` uses the strict canonical bundle parser, trusted member launch reader
and public verifier-keyring reader. The coordinator checks exact approved scope,
node and local filesystem ownership before creating an operation or calling
Claim. Complete retained state only recovers and replays the original receipt;
the command has no force, reset or reinitialize action. Operation timeout and
SIGINT/SIGTERM cancellation preserve the same library boundary.

`--action history --request-id <original-uuid>` accepts no preparation settings.
It reads the principal-scoped Registry query without opening local scratch or
calling either mutation RPC. Its dedicated JSON projection contains no fresh
permission, readiness or drain field. A missing or differently owned request
returns an error with no success result. Inspection uses the original request ID
from the retained local operation, including after lost Claim responses.

Both actions print one schema-1 JSON result after success. Prepared journal
status is an inspection receipt; history is a Registry observation. Neither
is serving readiness, writer drain, activation or production acceptance.

## Verification

Passed full `go test ./...`, `make lint` and changed-file integration-tag lint
(zero issues), related command/transport/coordinator race tests and a Linux amd64
cross-build of the actual Node Agent.
The new command uses the existing release-package entrypoint. No generated
contracts, database schema or systemd unit changed in this increment.

The PostgreSQL integration test compiles `cmd/vela-node-agent` and executes
separate processes through actual TLS. It checks normal replay, lost committed
Claim and receipt responses, other-node rejection before local state or RPC,
same-node/different-Agent history isolation, missing history, read-only history
stability, unchanged operation identity and exactly one Claim across retries.
It also removes only the test fixture's pair proof and verifies that another
process cannot recreate it or reclaim initialization authority.

Two more cases withhold an already committed Claim response until the command
deadline or an explicitly delivered SIGTERM. The SIGTERM case verifies normal
error exit rather than abrupt signal termination. Both retain the same operation,
keep journals absent, allow read-only inspection and reject reinitialization on
the next invocation. All five command scenarios passed with the integration
runner's race detector; the child is the ordinary compiled release entrypoint.

The fixture uses approved H3-shaped metadata, but all executable activity is
CPU-only. It invokes no model backend, GPU, hardware probe, Pod or remote
deployment. Process restart and transport interruption do not emulate physical
storage power loss. The Linux cross-build is compilation evidence, not a Linux
runtime or release acceptance receipt.

Logs: `/tmp/vela-node-bootstrap-command.jsonl` and
`/tmp/vela-node-bootstrap-command-race.jsonl` retain the initial three scenarios.
`/tmp/vela-node-bootstrap-command-interruption.jsonl` records all five;
`/tmp/vela-node-bootstrap-command-interruption-final.jsonl` verifies interruption
again after removing a test-only race in when the parent decides to send SIGTERM.

## Remaining lifecycle

Independent reconciliation of incomplete initialization, serving checks against
the Registry-recorded pair while holding journal lifetime locks, and controlled
Fleet provisioning/activation remain open. The node daemon and recurring Pod
initializers do not automatically call this one-shot command. Provisioning the
private layout and delivering credentials to its owner context remain deployment
responsibilities; ordinary serving does not need Node Agent credentials.

Unknown writers, pending Runtime recovery, failed-backend containment, sealed
receipt recovery, bounded checkpoint reclamation and command-level successful
terminal retirement remain separate unfinished work. This increment does not
complete the overall architecture or production audit. Nothing was pushed or
deployed.
