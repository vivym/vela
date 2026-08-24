# NATS Workload Identity And Subject Authorization

Date: 2026-08-24

Status: In progress. This specification defines repository-verifiable NATS
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
   key, and one or two expected Outbox user public keys. Two keys form the
   bounded overlap window for NKey rotation. URL userinfo and non-TLS server
   entries are rejected before dialing.
2. Decode and cryptographically verify the user JWT before every initial or
   reconnect authentication. It must be:
   - issued through the exact expected workload account;
   - bound to the exact expected Outbox user NKey;
   - named `vela-outbox-dispatcher`;
   - time-valid and explicitly expiring;
   - limited to standard NATS connections;
   - free of bearer-token and response-permission behavior.
3. Require the exact Outbox permission set:
   - publish allow: `vela.events.>`;
   - subscribe allow: `_INBOX.>` only, for request/reply PubAck delivery;
   - no deny entries, whose overlap could make the effective policy ambiguous.
4. Keep the credential file reloadable on reconnect so an atomically replaced,
   overlapping credential can rotate without retaining the old seed forever.
   Revalidation applies to every reload. A locally valid credential may enter
   degraded reconnect when the transport is unavailable so PostgreSQL Outbox
   remains authoritative; server authentication rejection must never produce a
   connected state or PubAck.
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

The current modular monolith opens exactly one NATS connection, used only by the
Outbox Dispatcher. It requires:

```text
VELA_NATS_URL=tls://nats.internal:4222
VELA_NATS_CREDENTIALS_FILE=/run/secrets/nats/outbox.creds
VELA_NATS_ROOT_CA_FILE=/run/tls/nats/ca.crt
VELA_NATS_OUTBOX_ACCOUNT_PUBLIC_KEY=<account NKey public key>
VELA_NATS_OUTBOX_USER_PUBLIC_KEYS=<current[,overlap] user NKey public keys>
```

The existing generic credential-file variable is retained for N/N-1 deployment
compatibility, but its contents are now contractually the Outbox Dispatcher
credential. No Scheduler, Billing, Fleet, Reconciler, system-account, or account
bootstrap credential is accepted by this connector.

Client-certificate authentication remains optional and additive. When enabled,
both `VELA_NATS_CLIENT_CERT_FILE` and `VELA_NATS_CLIENT_KEY_FILE` are required;
the NKey user JWT remains mandatory.

## Subject Contract

`vela.events.<event_type>` remains the canonical Outbox subject. Event types are
validated by the existing publisher before reaching NATS. The Outbox credential
has no business-event subscription permission. `_INBOX.>` exists only so the
JetStream client can receive the PubAck for its own request.

The Scheduler fixture permission is intentionally narrow evidence for the
already-defined `job.ready` wakeup. This slice does not activate a NATS Scheduler
consumer or grant speculative subjects to Billing, Fleet, or Reconciler. Each
future consumer must obtain an independent user NKey and a separately reviewed
exact subject policy before its connection is implemented.

## Error And Secret Handling

Configuration and authentication fail before the Outbox publisher starts.
Caller-visible errors identify only the failed class: invalid NATS Outbox
configuration, invalid NATS Outbox workload credential, or failed authenticated
transport. They never echo environment values, filesystem paths, JWT claims,
NKeys, seeds, nonces, or credential-file contents.

Asynchronous permission errors may name the denied subject because subjects are
non-secret routing metadata, but they must not contain credential material.

## Required Evidence

- unit tests reject insecure/userinfo URLs, missing expected identity, unsafe or
  oversized credential files, malformed/unverifiable JWTs, wrong account/user,
  wrong workload name, expired/perpetual claims, and permission drift;
- a TLS operator/account JWT integration fixture proves anonymous, revoked,
  cross-account, cross-workload, publish, subscribe, JetStream API, and system
  subject denials at the real server boundary;
- the production Outbox connector plus `JetStreamBroker` obtains a durable
  PubAck with the replacement credential;
- error assertions prove credential paths and contents are absent;
- existing Outbox retry/deduplication, N/N-1, unit, race, and full integration
  suites remain clean;
- `make generate` is stable, lint is clean, and Protobuf/OpenAPI breaking checks
  remain unchanged.

## Explicitly Deferred

This slice does not provide three-replica NATS deployment manifests, PVC and
anti-affinity topology, internal-network policy, monitoring endpoint exposure,
external secret distribution, operator/account signing ceremonies, production
rotation/revocation receipts, durable Scheduler/Billing/Fleet/Reconciler
consumers, JetStream rebuild, retained-backlog rollout, or any Production Gate
Launch Receipt.

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
