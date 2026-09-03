# Disposable H3 Multi-member Campaign

This runbook exercises the authenticated multi-member transport and complete
start barrier in a new local k3d cluster. It is a repository integration
exercise over fake ModelRuntimes. It is not GPU, DRA, ModelResidency, output
equivalence, performance, or Production Gate evidence.

## Safety boundary

The harness accepts only a new cluster name beginning with
`vela-h3-disposable-`, refuses an existing cluster, creates one server and three
agents without changing the default kubeconfig, and never targets
`k3d-heimdall-staging`. Cleanup deletes only the exact cluster created by the
current invocation. Set `H3_DISPOSABLE_RETAIN_CLUSTER=1` only for bounded manual
inspection; delete that exact disposable cluster when inspection ends.

The campaign image is separate from the production `vela-all` release graph.
Its short-lived CA private key and authority key remain in a private temporary
directory and are removed after the run. The cluster receives only the leaf
certificates, leaf private keys, CA certificate, and campaign authority key.

## Run

Docker, k3d, kubectl, OpenSSL, and the coreutils `timeout` command must be
installed. When image pulls require the local proxy, export it before running:

```sh
export HTTPS_PROXY=http://127.0.0.1:7897
make test-h3-disposable-member-campaign
```

If `proxy.golang.org` is unreachable but an alternate Go module proxy is
available, scope the fallback to this invocation instead of changing the host's
global Go configuration:

```sh
GOPROXY='https://goproxy.cn|https://goproxy.io|https://mirrors.aliyun.com/goproxy|direct' \
  make test-h3-disposable-member-campaign
```

These three proxy endpoints served the Go `1.26.7` toolchain metadata during
the 2026-09-03 lab preflight. Re-probe them before a later campaign; reachability
is an environment observation, not a repository guarantee.

The harness builds and imports `vela-h3-member-campaign:disposable`, pins the
leader and follower to different agent nodes, and runs these phases:

1. Normal prepare/start/status/cancel with a `PASS` receipt.
2. Follower loss after prepare and before delayed remote start, requiring
   `FAULT_REJECTED`, local cancellation, and no claim that the follower stopped.
3. Follower restoration followed by another complete `PASS` receipt.

Use a new absolute evidence directory when an explicit location is required:

```sh
H3_DISPOSABLE_EVIDENCE_DIR=/absolute/new/evidence-directory \
  make test-h3-disposable-member-campaign
```

The printed evidence directory contains all three receipts, Pod UID/node/restart
inventory, follower logs around the injected fault, node inventory, exact image
inspection, tool versions, and a scope-limited `summary.json`. A failed run
retains partial evidence. After `k3d cluster create` succeeds, cleanup deletes
and verifies the disposable cluster by default before writing a `PASS` summary.
If `k3d cluster create` itself fails, the harness retains its log but does not
delete an ambiguously owned cluster; inspect the printed evidence and cluster
inventory before removing any exact `vela-h3-disposable-*` name manually.

## Interpretation

A pass proves that two independent processes on different Kubernetes agent
nodes used the production TCP+mTLS, SPIFFE identity pinning, signed
`StageAuthority`, `stageworkermembertransport`, and `stageworkeragent` paths. It
also proves the tested follower-loss barrier failed closed and recovered.

Do not attach this result to any Launch Receipt. Real H3 or future multi-node LLM
activation still requires certified hardware topology, loaded ModelResidency,
physical GPU/DRA identity, model output equivalence, authorized fault windows,
and the applicable Production Gate receipts.

## Three-host lab evidence

On 2026-09-03 the same campaign command and workload contract was adapted to an
exact temporary namespace in the non-production three-host RKE2 lab. The leader
and follower ran on separate physical Workers, requested no GPU, and passed the
normal, follower-loss, and recovery phases. The temporary namespace, node
labels, and credentials were removed, and the non-campaign Pod inventory was
unchanged. See
[`h3-member-mock-lab-receipt.md`](../h3-member-mock-lab-receipt.md) for the
method, exact image identity, results, cleanup, and evidence boundary.

That one-off lab adaptation is evidence, not a second supported harness. The
command above remains the reproducible repository entry point. Repeating the
shared-cluster procedure requires a fresh exact-target preflight and an
independent cleanup plan.
