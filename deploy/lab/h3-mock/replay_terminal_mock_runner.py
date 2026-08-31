from __future__ import annotations

import json
import os
from pathlib import Path

import grpc

from vela.v1 import runner_pb2, runner_pb2_grpc

SOCKET = "/run/vela-runner/runner.sock"
STATE_ROOT = Path("/var/lib/vela/worker/scratch/runner-state")


def require(condition: bool, detail: str) -> None:
    if not condition:
        raise RuntimeError(detail)


def main() -> None:
    attempt_id = os.environ["VELA_LAB_TERMINAL_ATTEMPT_ID"]
    state_file = STATE_ROOT / attempt_id / "state.json"
    payload = json.loads(state_file.read_text(encoding="utf-8"))
    identity_payload = payload.get("identity")
    require(isinstance(identity_payload, dict), "terminal state identity is missing")
    identity = runner_pb2.RunnerAttemptIdentity(**identity_payload)
    require(identity.attempt_id == attempt_id, "terminal state Attempt identity changed")

    channel = grpc.insecure_channel(f"unix://{SOCKET}")
    grpc.channel_ready_future(channel).result(timeout=5)
    stub = runner_pb2_grpc.RunnerServiceStub(channel)
    status = stub.Status(runner_pb2.StatusRequest(identity=identity), timeout=5)
    collected = stub.CollectOutputs(
        runner_pb2.CollectOutputsRequest(identity=identity), timeout=5
    )
    require(
        status.state == runner_pb2.RUNNER_EXECUTION_STATE_SUCCEEDED,
        "retained terminal state was not restored as SUCCEEDED",
    )
    require(
        collected.decision == runner_pb2.RUNNER_COMMAND_DECISION_REJECTED,
        "cleaned terminal outputs unexpectedly remained collectable",
    )
    require(collected.detail == "outputs are not complete", "output rejection detail changed")
    require(len(collected.outputs) == 0, "cleaned terminal replay exposed outputs")
    channel.close()
    print(
        json.dumps(
            {
                "schema_version": 1,
                "environment": "non-production-lab",
                "attempt_id": attempt_id,
                "execution_state": "SUCCEEDED",
                "collect_outputs": "REJECTED",
                "collect_detail": collected.detail,
                "production_gates": "0/9",
            },
            separators=(",", ":"),
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
