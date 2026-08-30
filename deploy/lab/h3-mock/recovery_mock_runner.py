from __future__ import annotations

import json
import os
import sys

import grpc
from smoke_mock_runner import (
    SOCKET,
    attempt_identity,
    execution_spec,
    require,
    start_attempt,
    wait_for_outputs,
)

from vela.v1 import runner_pb2, runner_pb2_grpc

RECOVERY_PROMPT = "non-production abrupt runner restart recovery"


def identity_payload(identity: runner_pb2.RunnerAttemptIdentity) -> dict[str, object]:
    return {
        "attempt_id": identity.attempt_id,
        "job_id": identity.job_id,
        "worker_id": identity.worker_id,
        "worker_epoch": identity.worker_epoch,
        "lease_fence": identity.lease_fence,
    }


def decode_identity(raw: str, worker_id: str) -> runner_pb2.RunnerAttemptIdentity:
    payload = json.loads(raw)
    require(
        isinstance(payload, dict)
        and set(payload) == {"schema_version", "phase", "identity"}
        and payload["schema_version"] == 1
        and payload["phase"] == "started"
        and isinstance(payload["identity"], dict)
        and set(payload["identity"])
        == {"attempt_id", "job_id", "worker_id", "worker_epoch", "lease_fence"},
        "recovery identity payload is invalid",
    )
    identity = runner_pb2.RunnerAttemptIdentity(**payload["identity"])
    require(identity.worker_id == worker_id, "recovery Worker identity changed")
    return identity


def main() -> None:
    require(len(sys.argv) == 2, "usage: recovery_mock_runner.py start|resume|settle")
    mode = sys.argv[1]
    worker_id = os.environ["VELA_LAB_WORKER_ID"]
    channel = grpc.insecure_channel(f"unix://{SOCKET}")
    grpc.channel_ready_future(channel).result(timeout=5)
    stub = runner_pb2_grpc.RunnerServiceStub(channel)
    spec = execution_spec(RECOVERY_PROMPT)

    if mode == "start":
        identity = attempt_identity(worker_id)
        resumed = start_attempt(stub, identity, spec)
        require(not resumed, "fresh recovery attempt unexpectedly resumed state")
        result = {
            "schema_version": 1,
            "phase": "started",
            "identity": identity_payload(identity),
        }
    elif mode == "resume":
        identity = decode_identity(os.environ["VELA_LAB_RECOVERY_IDENTITY_JSON"], worker_id)
        resumed = start_attempt(
            stub, identity, spec, same_authority_local_recovery=True
        )
        require(resumed, "runner did not acknowledge same-authority local recovery")
        result = {
            "schema_version": 1,
            "phase": "recovered",
            "identity": identity_payload(identity),
            "resumed_local_state": resumed,
            "execution_state": "SUCCEEDED",
            "outputs": wait_for_outputs(stub, identity),
        }
    elif mode == "settle":
        identity = decode_identity(os.environ["VELA_LAB_RECOVERY_IDENTITY_JSON"], worker_id)
        result = {
            "schema_version": 1,
            "phase": "settled",
            "identity": identity_payload(identity),
            "execution_state": "SUCCEEDED",
            "outputs": wait_for_outputs(stub, identity),
        }
    else:
        raise RuntimeError("usage: recovery_mock_runner.py start|resume|settle")
    channel.close()
    print(json.dumps(result, separators=(",", ":"), sort_keys=True))


if __name__ == "__main__":
    main()
