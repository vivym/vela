# NATS Workload Identity And Subject Authorization

Date: 2026-08-24

Status: Implemented. This specification defines repository-verifiable NATS
identity, TLS, and subject-authorization behavior for the current Outbox
Dispatcher connection. It does not by itself prove production network isolation,
three-replica placement, credential distribution, or a Production Gate.

## Goal

Make the current Outbox Dispatcher fail closed unless it connects over TLS with
its exact, independently issued NKey user credential in the Vela workload
account. The credential can publish canonical Outbox events and receive only its
JetStream request replies. It cannot subscribe to business events, use JetStream
administration APIs, access monitoring subjects, or act as another workload.

The authenticated NATS server uses operator/account JWT mode. A second workload
credential proves that NATS enforces subject separation rather than merely
accepting any credential that can reach the internal listener.

## Governing Decisions

- ADR 0007: NATS is part of Organization Isolation on shared infrastructure.
  Workload identity and server-side subject authorization are security
  boundaries; application naming conventions are not.
- ADR 0003 and ADR 0010: PostgreSQL remains authoritative. NATS wakeups cannot
  mutate Job state without the existing transaction and fencing checks.
- ADR 0013: the change is configuration-additive for the current binary and does
  not change event subject or payload schemas.
- ADR 0029: authenticated repository integration tests are not a Launch Receipt.
  Production remains `0/9 PASS`.

## Confirmed TDD Seams

Tests exercise behavior only at these public boundaries:

1. the production Outbox NATS connector loading and authenticating an exact
   workload credential over TLS;
2. the production `JetStreamBroker.Publish` path receiving a durable PubAck;
3. the NATS server authorization boundary for cross-workload publish/subscribe,
   JetStream administration, and system/monitoring subjects.

## In Scope

1. Require the Outbox connector to use `tls://` endpoints, a configured root CA,
   a decorated NATS user credential file, an expected workload-account public
   key, one or two expected account-signer public keys, and one or two expected
   Outbox user public keys. Two keys form the bounded overlap window for account
   signer or user NKey rotation. URL userinfo and non-TLS server entries are
   rejected before dialing.
2. Decode and cryptographically verify the user JWT before every initial or
   reconnect authentication. It must be:
   - signed by an explicitly trusted signer for the exact expected workload
     account;
   - bound to the exact expected Outbox user NKey;
   - named `vela-outbox-dispatcher`;
   - time-valid and explicitly expiring;
   - limited to standard NATS connections;
   - free of bearer-token and response-permission behavior.
3. Require the exact Outbox permission set:
   - publish allow: `vela.events.>` and
     `$JS.API.STREAM.INFO.VELA_EVENTS`; the API subject exists only so the
     Publisher can fail closed on live release-stream contract drift;
   - subscribe allow: `_INBOX.>` only, for request/reply PubAck delivery;
   - no deny entries, whose overlap could make the effective policy ambiguous.
4. Keep the credential file reloadable on reconnect so an atomically replaced,
   overlapping credential can rotate without retaining the old seed forever.
   Revalidation applies to every reload. A locally valid credential may enter
   degraded reconnect when the transport is unavailable so PostgreSQL Outbox
   remains authoritative; server authentication rejection must never produce a
   connected state or PubAck. When every configured endpoint is unavailable,
   the connector returns a dormant reconnect handle with no real endpoint in
   its server pool. The caller starts the Publisher before explicitly activating
   the configured pool, so no real authentication attempt is hidden between the
   outage decision and Publisher startup.
5. Bound credential-file size, require a regular file without group/world
   permissions, wipe temporary credential bytes and NKey material where the
   library permits, and never include a credential path, JWT, seed, or public key
   in returned or logged errors.
6. Run a real TLS NATS JetStream fixture in operator/account JWT mode with:
   - a dedicated system account;
   - a Vela workload account;
   - independent Outbox and Scheduler user NKeys;
   - an account bootstrap identity used only by the test harness;
   - server-side user JWT publish/subscribe permissions.
7. Prove the Outbox credential can publish `vela.events.job.ready` through the
   production JetStream broker and receive a durable stream/sequence receipt.
8. Prove the Outbox credential cannot subscribe to business events, publish to
   `$SYS.>` or `$JS.API.>`, or use a credential from the system account.
9. Prove the Scheduler credential can subscribe only to
   `vela.events.job.ready`, cannot publish that subject, cannot subscribe to an
   unrelated billing event, and cannot access `$SYS.>`.
10. Prove anonymous connections and an account-revoked prior Outbox credential
    are rejected while a separately keyed replacement Outbox credential works.

## Runtime Contract

Slice 18 established one NATS connection used only by the Outbox Dispatcher. Its
runtime variables were:

```text
VELA_NATS_URL=tls://nats.internal:4222
VELA_NATS_CREDENTIALS_FILE=/run/secrets/nats/outbox.creds
VELA_NATS_ROOT_CA_FILE=/run/tls/nats/ca.crt
VELA_NATS_OUTBOX_ACCOUNT_PUBLIC_KEY=<account NKey public key>
VELA_NATS_OUTBOX_ACCOUNT_SIGNER_PUBLIC_KEYS=<current[,overlap] account signer NKey public keys>
VELA_NATS_OUTBOX_USER_PUBLIC_KEYS=<current[,overlap] user NKey public keys>
```

Slice 25 extends the current modular monolith with a second, independently keyed
Scheduler pull-consumer connection. Its exact current variables and permissions
are defined by `0025-jetstream-quorum-and-consumer-crash-recovery.md`; the
Slice 18 Outbox credential remains independent and cannot consume events.

The existing generic credential-file variable is retained for N/N-1 deployment
compatibility, but its contents are now contractually the Outbox Dispatcher
credential. No Scheduler, Billing, Fleet, Reconciler, system-account, or account
bootstrap credential is accepted by this connector.

Client-certificate authentication remains optional and additive. When enabled,
both `VELA_NATS_CLIENT_CERT_FILE` and `VELA_NATS_CLIENT_KEY_FILE` are required;
the NKey user JWT remains mandatory.

`ConnectOutbox` synchronously authenticates every reachable configured endpoint
and returns one of those exact authenticated connections. Any authentication or
TLS rejection fails the whole startup, even if another endpoint is offline or
accepts the credential. Only an all-endpoint transport outage returns a dormant
connection. `OutboxConnection.Activate` is idempotent and installs the real
endpoint pool after the Publisher has started; later authentication rejection is
a runtime degraded state and still cannot produce a connected state or PubAck.

## Subject Contract

`vela.events.<event_type>` remains the canonical Outbox subject. Event types are
validated by the existing publisher before reaching NATS. The Outbox credential
has no business-event subscription permission. `_INBOX.>` exists only so the
JetStream client can receive the PubAck for its own request.

The Outbox stream-info permission is read-only JetStream control-plane access;
it does not grant stream mutation or consumer administration. The Slice 18
Scheduler fixture's direct `job.ready` subscription remains historical evidence
for the then-defined wakeup boundary; Slice 18 itself did not activate that
consumer. Slice 25 now activates the production durable pull consumer with a
different credential that may use only the exact stream-info, consumer-info,
pull-next, ack, and `_INBOX.>` subjects. Billing, Fleet, and Reconciler consumers
remain deferred and each requires an independent user NKey and reviewed exact
subject policy.

## Error And Secret Handling

Configuration, local credential validation, and every authentication result from
a reachable startup endpoint fail before the Outbox Publisher starts. The sole
degraded exception is an explicit all-endpoint transport-outage decision followed
by post-start activation. Caller-visible errors identify only the failed class:
invalid NATS Outbox configuration, invalid NATS Outbox workload credential, or
failed authenticated transport. They never echo environment values, filesystem
paths, JWT claims, NKeys, seeds, nonces, or credential-file contents.

Asynchronous permission errors may name the denied subject because subjects are
non-secret routing metadata, but they must not contain credential material.

## Required Evidence

- unit tests reject insecure/userinfo URLs, missing expected identity, unsafe or
  oversized credential files, malformed/unverifiable JWTs, wrong account,
  untrusted account signer, wrong user, wrong workload name, expired/perpetual
  claims, and permission drift;
- a TLS operator/account JWT integration fixture proves anonymous, revoked,
  cross-account, cross-workload, publish, subscribe, JetStream API, and system
  subject denials at the real server boundary;
- the production Outbox connector plus `JetStreamBroker` obtains a durable
  PubAck with the replacement credential;
- all-offline startup exposes no real endpoint until explicit post-Publisher
  activation; a later valid credential reconnects while a revoked credential
  remains fail-closed and never reaches connected state;
- error assertions prove credential paths and contents are absent;
- existing Outbox retry/deduplication, N/N-1, unit, race, and full integration
  suites remain clean;
- `make generate` is stable, lint is clean, and Protobuf/OpenAPI breaking checks
  remain unchanged.

## Explicitly Deferred

Slice 18 did not provide three-replica NATS deployment manifests, PVC and
anti-affinity topology, internal-network policy, monitoring endpoint exposure,
external secret distribution, operator/account signing ceremonies, production
rotation/revocation receipts, durable consumers, JetStream rebuild,
retained-backlog rollout, or any Production Gate Launch Receipt. Slice 25 now
provides the repository-rendered R3 topology/contract and durable Scheduler
consumer; live deployment, secret ceremonies, retained-backlog rollout,
rebuild/restore evidence, Billing/Fleet/Reconciler consumers, and every Launch
Receipt remain deferred.

Those are separate deployment, release, DR, and future consumer slices. The test
operator, accounts, JWTs, NKeys, certificates, and credentials are generated at
test runtime and are never production identities.

## Completion Boundary

Slice 18 is complete only when the production Outbox connector verifies exact
workload identity and permissions on every authentication, authenticates over
TLS to a real operator/account JWT server, receives a durable PubAck, and all
required cross-workload/system/anonymous/revoked negative evidence passes with
no P0-P2 review finding.

Completion advances the NATS portion of ADR 0007 and acceptance scenario 27. It
does not close deployment isolation, the Organization Isolation Production Gate,
or any of the nine Production Gates.
