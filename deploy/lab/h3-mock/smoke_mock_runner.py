from __future__ import annotations

import hashlib
import json
import os
import stat
import time
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path

import grpc

from vela.v1 import runner_pb2, runner_pb2_grpc

SOCKET = "/run/vela-runner/runner.sock"
MODEL_ID = "84000000-0000-0000-0000-000000000004"
PRESET_ID = "84000000-0000-0000-0000-000000000005"
PROFILE_ID = "84000000-0000-0000-0000-000000000006"
OUTPUT_SPEC_ID = "84000000-0000-0000-0000-000000000007"
BACKEND_REVISION = (
    "mock-h3-backend@sha256:"
    "765077057011f16f852886601235f066dff7a89d3127719a5ae3c38206c7aee6"
)
OUTPUT_ROOT = Path("/var/lib/vela/worker/scratch/outputs")


def canonical_uuid() -> str:
    return str(uuid.uuid4())


def require(condition: bool, detail: str) -> None:
    if not condition:
        raise RuntimeError(detail)


def attempt_identity(worker_id: str) -> runner_pb2.RunnerAttemptIdentity:
    return runner_pb2.RunnerAttemptIdentity(
        attempt_id=canonical_uuid(),
        job_id=canonical_uuid(),
        worker_id=worker_id,
        worker_epoch=1,
        lease_fence=1,
    )


def execution_spec(prompt: str) -> runner_pb2.RunnerExecutionSpec:
    return runner_pb2.RunnerExecutionSpec(
        model_revision_id=MODEL_ID,
        generation_preset_revision_id=PRESET_ID,
        execution_profile_revision_id=PROFILE_ID,
        output_spec_id=OUTPUT_SPEC_ID,
        request_content_json=json.dumps(
            {"prompt": prompt}, separators=(",", ":"), sort_keys=True
        ).encode(),
    )


def start_attempt(
    stub: runner_pb2_grpc.RunnerServiceStub,
    identity: runner_pb2.RunnerAttemptIdentity,
    spec: runner_pb2.RunnerExecutionSpec,
    *,
    same_authority_local_recovery: bool = False,
) -> bool:
    prepared = stub.Prepare(
        runner_pb2.PrepareRequest(
            identity=identity,
            execution_spec=spec,
            same_authority_local_recovery=same_authority_local_recovery,
        ),
        timeout=5,
    )
    require(
        prepared.decision == runner_pb2.RUNNER_COMMAND_DECISION_ACCEPTED,
        f"prepare was rejected: {prepared.detail}",
    )
    started = stub.Start(runner_pb2.StartRequest(identity=identity), timeout=5)
    require(
        started.decision == runner_pb2.RUNNER_COMMAND_DECISION_ACCEPTED,
        f"start was rejected: {started.detail}",
    )
    return prepared.resumed_local_state


def wait_for_outputs(
    stub: runner_pb2_grpc.RunnerServiceStub,
    identity: runner_pb2.RunnerAttemptIdentity,
) -> list[dict[str, object]]:
    deadline = time.monotonic() + 30
    status = None
    while time.monotonic() < deadline:
        status = stub.Status(runner_pb2.StatusRequest(identity=identity), timeout=5)
        if status.state in (
            runner_pb2.RUNNER_EXECUTION_STATE_SUCCEEDED,
            runner_pb2.RUNNER_EXECUTION_STATE_FAILED,
            runner_pb2.RUNNER_EXECUTION_STATE_CANCELED,
        ):
            break
        time.sleep(0.1)
    require(status is not None, "runner returned no status")
    require(
        status.state == runner_pb2.RUNNER_EXECUTION_STATE_SUCCEEDED,
        "runner did not reach SUCCEEDED: "
        + runner_pb2.RunnerExecutionState.Name(status.state),
    )

    collected = stub.CollectOutputs(
        runner_pb2.CollectOutputsRequest(identity=identity), timeout=5
    )
    require(
        collected.decision == runner_pb2.RUNNER_COMMAND_DECISION_ACCEPTED,
        f"collect was rejected: {collected.detail}",
    )
    require(len(collected.outputs) == 2, "runner did not return exactly two outputs")
    outputs: list[dict[str, object]] = []
    for output in collected.outputs:
        path = Path(output.path)
        require(path.is_relative_to(OUTPUT_ROOT), "output escaped the output root")
        info = path.lstat()
        require(stat.S_ISREG(info.st_mode), "output is not a regular file")
        require(info.st_size == output.size_bytes, "output byte count mismatch")
        digest = hashlib.sha256(path.read_bytes()).digest()
        require(digest == output.sha256, "output SHA-256 mismatch")
        outputs.append(
            {
                "kind": output.kind,
                "ordinal": output.ordinal,
                "size_bytes": output.size_bytes,
                "sha256": digest.hex(),
                "content_type": output.content_type,
            }
        )
    require(
        sorted(output["kind"] for output in outputs) == ["THUMBNAIL", "VIDEO"],
        "output kinds are incomplete",
    )
    return outputs


def main() -> None:
    worker_id = os.environ["VELA_LAB_WORKER_ID"]
    node_identity = os.environ["VELA_LAB_NODE_IDENTITY"]
    channel = grpc.insecure_channel(f"unix://{SOCKET}")
    grpc.channel_ready_future(channel).result(timeout=5)
    stub = runner_pb2_grpc.RunnerServiceStub(channel)

    readiness: dict[str, dict[str, object]] = {}
    checks = (
        runner_pb2.RUNNER_READINESS_CHECK_DEVICE,
        runner_pb2.RUNNER_READINESS_CHECK_INFERENCE_BACKEND,
        runner_pb2.RUNNER_READINESS_CHECK_MODEL_WARMUP,
        runner_pb2.RUNNER_READINESS_CHECK_CANARY,
    )
    for check in checks:
        identity = runner_pb2.RunnerReadinessIdentity(
            cycle_id=canonical_uuid(),
            worker_id=worker_id,
            worker_epoch=1,
            node_identity=node_identity,
            execution_profile_revision_id=PROFILE_ID,
            inference_backend_revision=BACKEND_REVISION,
        )
        identity.deadline.FromDatetime(datetime.now(UTC) + timedelta(seconds=30))
        response = stub.ProbeReadiness(
            runner_pb2.ProbeReadinessRequest(identity=identity, check=check), timeout=5
        )
        require(
            response.decision == runner_pb2.RUNNER_COMMAND_DECISION_ACCEPTED,
            f"readiness check {check} was rejected: {response.detail}",
        )
        require(response.passed, f"readiness check {check} did not pass")
        evidence = json.loads(response.evidence_json)
        name = runner_pb2.RunnerReadinessCheck.Name(check).removeprefix(
            "RUNNER_READINESS_CHECK_"
        )
        readiness[name] = {"detail": response.detail, "evidence": evidence}

    identity = attempt_identity(worker_id)
    resumed = start_attempt(
        stub, identity, execution_spec("non-production persistent mock smoke")
    )
    require(not resumed, "fresh smoke unexpectedly resumed prior local state")
    outputs = wait_for_outputs(stub, identity)
    channel.close()
    print(
        json.dumps(
            {
                "schema_version": 1,
                "environment": "non-production-lab",
                "node_identity": node_identity,
                "worker_id": worker_id,
                "readiness": readiness,
                "execution_state": "SUCCEEDED",
                "outputs": outputs,
            },
            separators=(",", ":"),
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
