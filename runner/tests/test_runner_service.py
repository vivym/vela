from __future__ import annotations

import hashlib
import json
import sys
import time
from pathlib import Path

import pytest

from vela.v1 import runner_pb2
from vela_h3_runner.runtime import RunnerRuntime, RuntimeConfig

ATTEMPT_ID = "84000000-0000-0000-0000-000000000001"
JOB_ID = "84000000-0000-0000-0000-000000000002"
WORKER_ID = "84000000-0000-0000-0000-000000000003"
MODEL_ID = "84000000-0000-0000-0000-000000000004"
PRESET_ID = "84000000-0000-0000-0000-000000000005"
PROFILE_ID = "84000000-0000-0000-0000-000000000006"
OUTPUT_SPEC_ID = "84000000-0000-0000-0000-000000000007"


def test_runner_executes_exact_profile_and_collects_verified_outputs(tmp_path: Path) -> None:
    runtime = RunnerRuntime(runtime_config(tmp_path, successful_backend(tmp_path)))
    identity = attempt_identity()
    prepared = runtime.Prepare(
        runner_pb2.PrepareRequest(
            identity=identity,
            execution_spec=execution_spec(),
            same_authority_local_recovery=False,
        ),
        None,
    )
    assert prepared.identity == identity
    assert prepared.decision == runner_pb2.RUNNER_COMMAND_DECISION_ACCEPTED
    assert prepared.resumed_local_state is False

    started = runtime.Start(runner_pb2.StartRequest(identity=identity), None)
    assert started.decision == runner_pb2.RUNNER_COMMAND_DECISION_ACCEPTED

    deadline = time.monotonic() + 3
    while True:
        status = runtime.Status(runner_pb2.StatusRequest(identity=identity), None)
        if status.state == runner_pb2.RUNNER_EXECUTION_STATE_SUCCEEDED:
            break
        assert status.state in {
            runner_pb2.RUNNER_EXECUTION_STATE_PREPARING,
            runner_pb2.RUNNER_EXECUTION_STATE_RUNNING,
        }
        assert time.monotonic() < deadline
        time.sleep(0.01)

    collected = runtime.CollectOutputs(
        runner_pb2.CollectOutputsRequest(identity=identity),
        None,
    )
    assert collected.decision == runner_pb2.RUNNER_COMMAND_DECISION_ACCEPTED
    assert [(output.kind, output.ordinal) for output in collected.outputs] == [
        ("THUMBNAIL", 0),
        ("VIDEO", 0),
    ]
    for output in collected.outputs:
        content = Path(output.path).read_bytes()
        assert output.size_bytes == len(content)
        assert output.sha256 == hashlib.sha256(content).digest()
    runtime.close()


def test_runner_rejects_aggregate_outputs_larger_than_attempt_quota(
    tmp_path: Path,
) -> None:
    config = runtime_config(
        tmp_path,
        successful_backend(tmp_path),
        max_output_bytes=len(b"verified-video") + len(b"verified-thumbnail") - 1,
    )
    runtime = RunnerRuntime(config)
    identity = attempt_identity()
    runtime.Prepare(
        runner_pb2.PrepareRequest(identity=identity, execution_spec=execution_spec()), None
    )
    runtime.Start(runner_pb2.StartRequest(identity=identity), None)

    status = wait_for_terminal(runtime, identity)

    assert status.state == runner_pb2.RUNNER_EXECUTION_STATE_FAILED
    assert status.failure.failure_fingerprint == "backend/output-receipt"
    assert (
        runtime.CollectOutputs(runner_pb2.CollectOutputsRequest(identity=identity), None).decision
        == runner_pb2.RUNNER_COMMAND_DECISION_REJECTED
    )
    assert not (config.output_root / ATTEMPT_ID).exists()
    runtime.close()


def test_runner_rejects_sparse_output_larger_than_attempt_quota(tmp_path: Path) -> None:
    config = runtime_config(
        tmp_path,
        sparse_output_backend(tmp_path),
        max_output_bytes=1024,
    )
    runtime = RunnerRuntime(config)
    identity = attempt_identity()
    runtime.Prepare(
        runner_pb2.PrepareRequest(identity=identity, execution_spec=execution_spec()), None
    )
    runtime.Start(runner_pb2.StartRequest(identity=identity), None)

    status = wait_for_terminal(runtime, identity)

    assert status.state == runner_pb2.RUNNER_EXECUTION_STATE_FAILED
    assert status.failure.failure_fingerprint == "backend/output-receipt"
    assert not (config.output_root / ATTEMPT_ID).exists()
    runtime.close()


def test_agent_shutdown_preserves_only_exact_authority_for_resume(tmp_path: Path) -> None:
    backend = resumable_backend(tmp_path)
    config = runtime_config(tmp_path, backend)
    identity = attempt_identity()
    runtime = RunnerRuntime(config)
    prepared = runtime.Prepare(
        runner_pb2.PrepareRequest(identity=identity, execution_spec=execution_spec()), None
    )
    assert prepared.decision == runner_pb2.RUNNER_COMMAND_DECISION_ACCEPTED
    assert runtime.Start(runner_pb2.StartRequest(identity=identity), None).decision == (
        runner_pb2.RUNNER_COMMAND_DECISION_ACCEPTED
    )
    assert runtime.Status(runner_pb2.StatusRequest(identity=identity), None).state == (
        runner_pb2.RUNNER_EXECUTION_STATE_RUNNING
    )
    canceled = runtime.Cancel(
        runner_pb2.CancelRequest(
            identity=identity,
            reason=runner_pb2.RUNNER_CANCEL_REASON_AGENT_SHUTDOWN,
        ),
        None,
    )
    assert canceled.decision == runner_pb2.RUNNER_COMMAND_DECISION_ACCEPTED
    runtime.close()

    resumed_runtime = RunnerRuntime(config)
    resumed = resumed_runtime.Prepare(
        runner_pb2.PrepareRequest(
            identity=identity,
            execution_spec=execution_spec(),
            same_authority_local_recovery=True,
        ),
        None,
    )
    assert resumed.decision == runner_pb2.RUNNER_COMMAND_DECISION_ACCEPTED
    assert resumed.resumed_local_state is True
    assert resumed_runtime.Start(runner_pb2.StartRequest(identity=identity), None).decision == (
        runner_pb2.RUNNER_COMMAND_DECISION_ACCEPTED
    )
    deadline = time.monotonic() + 3
    while (
        resumed_runtime.Status(runner_pb2.StatusRequest(identity=identity), None).state
        != runner_pb2.RUNNER_EXECUTION_STATE_SUCCEEDED
    ):
        assert time.monotonic() < deadline
        time.sleep(0.01)
    assert (tmp_path / "resume-observed").read_text(encoding="utf-8") == "true"
    resumed_runtime.close()


def test_noneligible_local_recovery_restarts_same_authority_from_scratch(tmp_path: Path) -> None:
    backend = resumable_backend(tmp_path)
    config = runtime_config(tmp_path, backend)
    identity = attempt_identity()
    runtime = RunnerRuntime(config)
    runtime.Prepare(
        runner_pb2.PrepareRequest(identity=identity, execution_spec=execution_spec()), None
    )
    runtime.Start(runner_pb2.StartRequest(identity=identity), None)
    runtime.Cancel(
        runner_pb2.CancelRequest(
            identity=identity,
            reason=runner_pb2.RUNNER_CANCEL_REASON_AGENT_SHUTDOWN,
        ),
        None,
    )
    runtime.close()

    marker = tmp_path / "resume-observed"
    marker.unlink(missing_ok=True)
    restarted_runtime = RunnerRuntime(config)
    prepared = restarted_runtime.Prepare(
        runner_pb2.PrepareRequest(
            identity=identity,
            execution_spec=execution_spec(),
            same_authority_local_recovery=False,
        ),
        None,
    )
    assert prepared.decision == runner_pb2.RUNNER_COMMAND_DECISION_ACCEPTED
    assert prepared.resumed_local_state is False
    restarted_runtime.Start(runner_pb2.StartRequest(identity=identity), None)
    deadline = time.monotonic() + 3
    while not marker.exists():
        assert time.monotonic() < deadline
        time.sleep(0.01)
    assert marker.read_text(encoding="utf-8") == "false"
    restarted_runtime.Cancel(
        runner_pb2.CancelRequest(
            identity=identity,
            reason=runner_pb2.RUNNER_CANCEL_REASON_CONTROL_PLANE_STOP,
        ),
        None,
    )
    restarted_runtime.close()


def test_runner_rejects_profile_outside_exact_allowlist(tmp_path: Path) -> None:
    runtime = RunnerRuntime(runtime_config(tmp_path, successful_backend(tmp_path)))
    spec = execution_spec()
    spec.execution_profile_revision_id = "84000000-0000-0000-0000-000000000009"

    prepared = runtime.Prepare(
        runner_pb2.PrepareRequest(identity=attempt_identity(), execution_spec=spec), None
    )

    assert prepared.decision == runner_pb2.RUNNER_COMMAND_DECISION_REJECTED
    assert "certified profile allowlist" in prepared.detail
    runtime.close()


def test_runner_accepts_certified_profile_independent_of_json_key_order(tmp_path: Path) -> None:
    config = runtime_config(tmp_path, successful_backend(tmp_path))
    config.profiles_file.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "backend_revision": "sglang@sha256:test",
                "profiles": [
                    {
                        "output_spec_id": OUTPUT_SPEC_ID,
                        "execution_profile_revision_id": PROFILE_ID,
                        "model_revision_id": MODEL_ID,
                        "generation_preset_revision_id": PRESET_ID,
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    runtime = RunnerRuntime(config)

    prepared = runtime.Prepare(
        runner_pb2.PrepareRequest(
            identity=attempt_identity(),
            execution_spec=execution_spec(),
        ),
        None,
    )

    assert prepared.decision == runner_pb2.RUNNER_COMMAND_DECISION_ACCEPTED
    runtime.close()


def test_runner_rejects_conflicting_active_attempt_authority(tmp_path: Path) -> None:
    runtime = RunnerRuntime(runtime_config(tmp_path, successful_backend(tmp_path)))
    identity = attempt_identity()
    runtime.Prepare(
        runner_pb2.PrepareRequest(identity=identity, execution_spec=execution_spec()), None
    )
    conflicting = attempt_identity()
    conflicting.attempt_id = "84000000-0000-0000-0000-000000000009"

    prepared = runtime.Prepare(
        runner_pb2.PrepareRequest(identity=conflicting, execution_spec=execution_spec()), None
    )

    assert prepared.decision == runner_pb2.RUNNER_COMMAND_DECISION_REJECTED
    assert "different active Attempt" in prepared.detail
    assert runtime.Status(runner_pb2.StatusRequest(identity=identity), None).state == (
        runner_pb2.RUNNER_EXECUTION_STATE_PREPARING
    )
    runtime.close()


def test_backend_failure_returns_bounded_receipt_and_removes_partial_outputs(
    tmp_path: Path,
) -> None:
    config = runtime_config(tmp_path, failing_backend(tmp_path))
    runtime = RunnerRuntime(config)
    identity = attempt_identity()
    runtime.Prepare(
        runner_pb2.PrepareRequest(identity=identity, execution_spec=execution_spec()), None
    )
    runtime.Start(runner_pb2.StartRequest(identity=identity), None)
    status = wait_for_terminal(runtime, identity)
    assert status.state == runner_pb2.RUNNER_EXECUTION_STATE_FAILED
    assert status.failure.failure_class == "CUDA_OOM"
    assert status.failure.failure_fingerprint == "cuda/oom/dit"
    assert status.failure.inference_backend_revision == "sglang@sha256:test"
    assert status.failure.gpu_uuids == ["GPU-00000000-0000-0000-0000-000000000002"]
    assert not (config.output_root / ATTEMPT_ID).exists()
    runtime.close()


def test_runner_restart_restores_failed_terminal_receipt(tmp_path: Path) -> None:
    config = runtime_config(tmp_path, failing_backend(tmp_path))
    identity = attempt_identity()
    runtime = RunnerRuntime(config)
    runtime.Prepare(
        runner_pb2.PrepareRequest(identity=identity, execution_spec=execution_spec()), None
    )
    runtime.Start(runner_pb2.StartRequest(identity=identity), None)
    original = wait_for_terminal(runtime, identity)
    runtime.close()

    recovered = RunnerRuntime(config)
    replayed = recovered.Prepare(
        runner_pb2.PrepareRequest(
            identity=identity,
            execution_spec=execution_spec(),
            same_authority_local_recovery=True,
        ),
        None,
    )
    started = recovered.Start(runner_pb2.StartRequest(identity=identity), None)
    status = recovered.Status(runner_pb2.StatusRequest(identity=identity), None)

    assert replayed.decision == runner_pb2.RUNNER_COMMAND_DECISION_ACCEPTED
    assert replayed.resumed_local_state is False
    assert started.decision == runner_pb2.RUNNER_COMMAND_DECISION_ACCEPTED
    assert status.state == runner_pb2.RUNNER_EXECUTION_STATE_FAILED
    assert status.failure == original.failure
    recovered.close()


def test_runner_restart_restores_canceled_terminal_state(tmp_path: Path) -> None:
    config = runtime_config(tmp_path, resumable_backend(tmp_path))
    identity = attempt_identity()
    runtime = RunnerRuntime(config)
    runtime.Prepare(
        runner_pb2.PrepareRequest(identity=identity, execution_spec=execution_spec()), None
    )
    runtime.Start(runner_pb2.StartRequest(identity=identity), None)
    canceled = runtime.Cancel(
        runner_pb2.CancelRequest(
            identity=identity,
            reason=runner_pb2.RUNNER_CANCEL_REASON_CONTROL_PLANE_STOP,
        ),
        None,
    )
    assert canceled.decision == runner_pb2.RUNNER_COMMAND_DECISION_ACCEPTED
    runtime.close()

    recovered = RunnerRuntime(config)
    replayed = recovered.Prepare(
        runner_pb2.PrepareRequest(
            identity=identity,
            execution_spec=execution_spec(),
            same_authority_local_recovery=True,
        ),
        None,
    )
    started = recovered.Start(runner_pb2.StartRequest(identity=identity), None)

    assert replayed.decision == runner_pb2.RUNNER_COMMAND_DECISION_ACCEPTED
    assert replayed.resumed_local_state is False
    assert started.decision == runner_pb2.RUNNER_COMMAND_DECISION_ACCEPTED
    assert recovered.Status(runner_pb2.StatusRequest(identity=identity), None).state == (
        runner_pb2.RUNNER_EXECUTION_STATE_CANCELED
    )
    recovered.close()


def test_cancel_replay_retries_failed_output_cleanup_before_accepting(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = runtime_config(tmp_path, resumable_backend(tmp_path))
    identity = attempt_identity()
    runtime = RunnerRuntime(config)
    runtime.Prepare(
        runner_pb2.PrepareRequest(identity=identity, execution_spec=execution_spec()), None
    )
    runtime.Start(runner_pb2.StartRequest(identity=identity), None)
    output = config.output_root / identity.attempt_id / "partial-customer-content.bin"
    output.write_bytes(b"must be removed before Cancel is accepted")
    output.chmod(0o600)
    remove_outputs = runtime._remove_attempt_outputs
    cleanup_calls = 0

    def fail_cleanup_once() -> None:
        nonlocal cleanup_calls
        cleanup_calls += 1
        if cleanup_calls == 1:
            raise OSError("injected output cleanup failure")
        remove_outputs()

    monkeypatch.setattr(runtime, "_remove_attempt_outputs", fail_cleanup_once)
    request = runner_pb2.CancelRequest(
        identity=identity,
        reason=runner_pb2.RUNNER_CANCEL_REASON_CONTROL_PLANE_STOP,
    )

    with pytest.raises(OSError, match="injected output cleanup failure"):
        runtime.Cancel(request, None)
    assert output.exists()

    replayed = runtime.Cancel(request, None)

    assert replayed.decision == runner_pb2.RUNNER_COMMAND_DECISION_ACCEPTED
    assert cleanup_calls == 2
    assert not output.exists()
    runtime.close()

    recovered = RunnerRuntime(config)
    assert recovered.Status(runner_pb2.StatusRequest(identity=identity), None).state == (
        runner_pb2.RUNNER_EXECUTION_STATE_CANCELED
    )
    recovered.close()


def test_malformed_backend_failure_receipt_fails_closed(tmp_path: Path) -> None:
    config = runtime_config(tmp_path, malformed_failure_backend(tmp_path))
    runtime = RunnerRuntime(config)
    identity = attempt_identity()
    runtime.Prepare(
        runner_pb2.PrepareRequest(identity=identity, execution_spec=execution_spec()), None
    )
    runtime.Start(runner_pb2.StartRequest(identity=identity), None)

    status = wait_for_terminal(runtime, identity)

    assert status.state == runner_pb2.RUNNER_EXECUTION_STATE_FAILED
    assert status.failure.failure_class == "BACKEND_PROCESS_FAILED"
    assert status.failure.failure_fingerprint == "backend/process-exit"
    assert status.failure.retry_recommended is True
    assert status.failure.worker_reusable is False
    runtime.close()


def test_runner_rejects_symlink_output_from_successful_backend(tmp_path: Path) -> None:
    config = runtime_config(tmp_path, symlink_backend(tmp_path))
    runtime = RunnerRuntime(config)
    identity = attempt_identity()
    runtime.Prepare(
        runner_pb2.PrepareRequest(identity=identity, execution_spec=execution_spec()), None
    )
    runtime.Start(runner_pb2.StartRequest(identity=identity), None)
    status = wait_for_terminal(runtime, identity)
    assert status.state == runner_pb2.RUNNER_EXECUTION_STATE_FAILED
    assert status.failure.failure_fingerprint == "backend/output-receipt"
    collected = runtime.CollectOutputs(runner_pb2.CollectOutputsRequest(identity=identity), None)
    assert collected.decision == runner_pb2.RUNNER_COMMAND_DECISION_REJECTED
    assert not (config.output_root / ATTEMPT_ID).exists()
    runtime.close()


def test_runner_rejects_cross_root_hardlink_output(tmp_path: Path) -> None:
    config = runtime_config(tmp_path, hardlink_backend(tmp_path))
    runtime = RunnerRuntime(config)
    identity = attempt_identity()
    runtime.Prepare(
        runner_pb2.PrepareRequest(identity=identity, execution_spec=execution_spec()), None
    )
    runtime.Start(runner_pb2.StartRequest(identity=identity), None)

    status = wait_for_terminal(runtime, identity)

    assert status.state == runner_pb2.RUNNER_EXECUTION_STATE_FAILED
    assert status.failure.failure_fingerprint == "backend/output-receipt"
    assert (
        runtime.CollectOutputs(runner_pb2.CollectOutputsRequest(identity=identity), None).decision
        == runner_pb2.RUNNER_COMMAND_DECISION_REJECTED
    )
    runtime.close()


def test_runner_converts_malformed_output_receipt_to_structured_failure(
    tmp_path: Path,
) -> None:
    config = runtime_config(tmp_path, malformed_output_backend(tmp_path))
    runtime = RunnerRuntime(config)
    identity = attempt_identity()
    runtime.Prepare(
        runner_pb2.PrepareRequest(identity=identity, execution_spec=execution_spec()), None
    )
    runtime.Start(runner_pb2.StartRequest(identity=identity), None)

    status = wait_for_terminal(runtime, identity)

    assert status.state == runner_pb2.RUNNER_EXECUTION_STATE_FAILED
    assert status.failure.failure_fingerprint == "backend/output-receipt"
    assert (
        runtime.CollectOutputs(runner_pb2.CollectOutputsRequest(identity=identity), None).decision
        == runner_pb2.RUNNER_COMMAND_DECISION_REJECTED
    )
    assert not (config.output_root / ATTEMPT_ID).exists()
    runtime.close()


def test_runner_restart_after_success_reuses_outputs_without_reexecution(tmp_path: Path) -> None:
    config = runtime_config(tmp_path, single_execution_backend(tmp_path))
    identity = attempt_identity()
    runtime = RunnerRuntime(config)
    runtime.Prepare(
        runner_pb2.PrepareRequest(identity=identity, execution_spec=execution_spec()), None
    )
    runtime.Start(runner_pb2.StartRequest(identity=identity), None)
    assert wait_for_terminal(runtime, identity).state == (
        runner_pb2.RUNNER_EXECUTION_STATE_SUCCEEDED
    )
    runtime.close()

    recovered = RunnerRuntime(config)
    replayed = recovered.Prepare(
        runner_pb2.PrepareRequest(
            identity=identity,
            execution_spec=execution_spec(),
            same_authority_local_recovery=True,
        ),
        None,
    )
    assert replayed.decision == runner_pb2.RUNNER_COMMAND_DECISION_ACCEPTED
    assert recovered.Start(runner_pb2.StartRequest(identity=identity), None).decision == (
        runner_pb2.RUNNER_COMMAND_DECISION_ACCEPTED
    )
    assert recovered.Status(runner_pb2.StatusRequest(identity=identity), None).state == (
        runner_pb2.RUNNER_EXECUTION_STATE_SUCCEEDED
    )
    assert (
        recovered.CollectOutputs(runner_pb2.CollectOutputsRequest(identity=identity), None).decision
        == runner_pb2.RUNNER_COMMAND_DECISION_ACCEPTED
    )
    assert (tmp_path / "backend-runs").read_text(encoding="utf-8") == "1"
    recovered.close()


def test_runner_restart_rejects_changed_successful_output(tmp_path: Path) -> None:
    config = runtime_config(tmp_path, successful_backend(tmp_path))
    identity = attempt_identity()
    runtime = RunnerRuntime(config)
    runtime.Prepare(
        runner_pb2.PrepareRequest(identity=identity, execution_spec=execution_spec()), None
    )
    runtime.Start(runner_pb2.StartRequest(identity=identity), None)
    assert wait_for_terminal(runtime, identity).state == (
        runner_pb2.RUNNER_EXECUTION_STATE_SUCCEEDED
    )
    outputs = runtime.CollectOutputs(
        runner_pb2.CollectOutputsRequest(identity=identity), None
    ).outputs
    runtime.close()

    Path(outputs[0].path).write_bytes(b"changed-after-success")

    with pytest.raises(ValueError, match="successful output receipt changed"):
        RunnerRuntime(config)


def test_agent_shutdown_cancel_does_not_reopen_succeeded_attempt(tmp_path: Path) -> None:
    runtime = RunnerRuntime(runtime_config(tmp_path, successful_backend(tmp_path)))
    identity = attempt_identity()
    runtime.Prepare(
        runner_pb2.PrepareRequest(identity=identity, execution_spec=execution_spec()), None
    )
    runtime.Start(runner_pb2.StartRequest(identity=identity), None)
    assert wait_for_terminal(runtime, identity).state == (
        runner_pb2.RUNNER_EXECUTION_STATE_SUCCEEDED
    )

    canceled = runtime.Cancel(
        runner_pb2.CancelRequest(
            identity=identity,
            reason=runner_pb2.RUNNER_CANCEL_REASON_AGENT_SHUTDOWN,
        ),
        None,
    )

    assert canceled.decision == runner_pb2.RUNNER_COMMAND_DECISION_ACCEPTED
    assert runtime.Status(runner_pb2.StatusRequest(identity=identity), None).state == (
        runner_pb2.RUNNER_EXECUTION_STATE_SUCCEEDED
    )
    assert (
        runtime.CollectOutputs(runner_pb2.CollectOutputsRequest(identity=identity), None).decision
        == runner_pb2.RUNNER_COMMAND_DECISION_ACCEPTED
    )
    runtime.close()


def test_preparing_next_attempt_removes_previous_terminal_outputs(tmp_path: Path) -> None:
    config = runtime_config(tmp_path, successful_backend(tmp_path))
    runtime = RunnerRuntime(config)
    first_identity = attempt_identity()
    runtime.Prepare(
        runner_pb2.PrepareRequest(
            identity=first_identity,
            execution_spec=execution_spec(),
        ),
        None,
    )
    runtime.Start(runner_pb2.StartRequest(identity=first_identity), None)
    assert wait_for_terminal(runtime, first_identity).state == (
        runner_pb2.RUNNER_EXECUTION_STATE_SUCCEEDED
    )
    first_output_root = config.output_root / first_identity.attempt_id
    assert first_output_root.exists()
    next_identity = attempt_identity()
    next_identity.attempt_id = "84000000-0000-0000-0000-000000000009"

    prepared = runtime.Prepare(
        runner_pb2.PrepareRequest(
            identity=next_identity,
            execution_spec=execution_spec(),
        ),
        None,
    )

    assert prepared.decision == runner_pb2.RUNNER_COMMAND_DECISION_ACCEPTED
    assert not first_output_root.exists()
    runtime.close()


def test_terminal_attempt_rejects_conflicting_spec_reuse(tmp_path: Path) -> None:
    runtime = RunnerRuntime(runtime_config(tmp_path, successful_backend(tmp_path)))
    identity = attempt_identity()
    original_spec = execution_spec()
    runtime.Prepare(
        runner_pb2.PrepareRequest(identity=identity, execution_spec=original_spec), None
    )
    runtime.Start(runner_pb2.StartRequest(identity=identity), None)
    assert wait_for_terminal(runtime, identity).state == (
        runner_pb2.RUNNER_EXECUTION_STATE_SUCCEEDED
    )
    conflicting_spec = execution_spec()
    conflicting_spec.request_content_json = json.dumps({"prompt": "different"}).encode()

    replay = runtime.Prepare(
        runner_pb2.PrepareRequest(
            identity=identity,
            execution_spec=conflicting_spec,
            same_authority_local_recovery=True,
        ),
        None,
    )

    assert replay.decision == runner_pb2.RUNNER_COMMAND_DECISION_REJECTED
    assert runtime.Status(runner_pb2.StatusRequest(identity=identity), None).state == (
        runner_pb2.RUNNER_EXECUTION_STATE_SUCCEEDED
    )
    assert (
        runtime.CollectOutputs(runner_pb2.CollectOutputsRequest(identity=identity), None).decision
        == runner_pb2.RUNNER_COMMAND_DECISION_ACCEPTED
    )
    runtime.close()


def test_runner_recovery_rejects_failure_for_unbound_gpu(tmp_path: Path) -> None:
    config = runtime_config(tmp_path, failing_backend(tmp_path))
    identity = attempt_identity()
    runtime = RunnerRuntime(config)
    runtime.Prepare(
        runner_pb2.PrepareRequest(identity=identity, execution_spec=execution_spec()), None
    )
    runtime.Start(runner_pb2.StartRequest(identity=identity), None)
    assert wait_for_terminal(runtime, identity).state == (runner_pb2.RUNNER_EXECUTION_STATE_FAILED)
    runtime.close()
    state_path = config.state_root / identity.attempt_id / "state.json"
    state = json.loads(state_path.read_text(encoding="utf-8"))
    state["failure"]["gpu_uuids"] = ["GPU-10000000-0000-0000-0000-000000000001"]
    state_path.write_text(json.dumps(state), encoding="utf-8")
    state_path.chmod(0o600)

    with pytest.raises(ValueError, match="failure evidence is invalid"):
        RunnerRuntime(config)


def test_running_status_reports_bounded_backend_progress(tmp_path: Path) -> None:
    runtime = RunnerRuntime(runtime_config(tmp_path, progress_backend(tmp_path)))
    identity = attempt_identity()
    runtime.Prepare(
        runner_pb2.PrepareRequest(identity=identity, execution_spec=execution_spec()), None
    )
    runtime.Start(runner_pb2.StartRequest(identity=identity), None)
    deadline = time.monotonic() + 3
    while True:
        status = runtime.Status(runner_pb2.StatusRequest(identity=identity), None)
        if status.backend_stage == "dit":
            break
        assert time.monotonic() < deadline
        time.sleep(0.01)

    assert status.state == runner_pb2.RUNNER_EXECUTION_STATE_RUNNING
    assert status.sequence == 3
    assert status.HasField("backend_stage_progress")
    assert status.backend_stage_progress == 0.25
    assert status.HasField("estimated_remaining_seconds")
    assert status.estimated_remaining_seconds == 90
    runtime.Cancel(
        runner_pb2.CancelRequest(
            identity=identity,
            reason=runner_pb2.RUNNER_CANCEL_REASON_CONTROL_PLANE_STOP,
        ),
        None,
    )
    runtime.close()


@pytest.mark.parametrize(
    ("progress", "remaining"),
    [
        (1.0, 90),
        (0.25, ((1 << 63) - 1) // 1_000_000_000 + 1),
    ],
)
def test_runner_discards_backend_progress_outside_control_plane_contract(
    tmp_path: Path,
    progress: float,
    remaining: int,
) -> None:
    config = runtime_config(
        tmp_path,
        progress_backend(tmp_path, progress=progress, remaining=remaining),
    )
    runtime = RunnerRuntime(config)
    identity = attempt_identity()
    runtime.Prepare(
        runner_pb2.PrepareRequest(identity=identity, execution_spec=execution_spec()), None
    )
    runtime.Start(runner_pb2.StartRequest(identity=identity), None)
    status_path = config.state_root / ATTEMPT_ID / "backend-status.json"
    deadline = time.monotonic() + 3
    while not status_path.exists():
        assert time.monotonic() < deadline
        time.sleep(0.01)

    status = runtime.Status(runner_pb2.StatusRequest(identity=identity), None)

    assert status.backend_stage != "dit"
    assert not status.HasField("backend_stage_progress")
    assert not status.HasField("estimated_remaining_seconds")
    runtime.Cancel(
        runner_pb2.CancelRequest(
            identity=identity,
            reason=runner_pb2.RUNNER_CANCEL_REASON_CONTROL_PLANE_STOP,
        ),
        None,
    )
    runtime.close()


def test_runner_recovery_rejects_nonterminal_sequence_at_int64_limit(
    tmp_path: Path,
) -> None:
    config = runtime_config(tmp_path, successful_backend(tmp_path))
    identity = attempt_identity()
    runtime = RunnerRuntime(config)
    runtime.Prepare(
        runner_pb2.PrepareRequest(identity=identity, execution_spec=execution_spec()), None
    )
    runtime.close()
    state_path = config.state_root / identity.attempt_id / "state.json"
    state = json.loads(state_path.read_text(encoding="utf-8"))
    state["sequence"] = (1 << 63) - 1
    state_path.write_text(json.dumps(state), encoding="utf-8")
    state_path.chmod(0o600)

    with pytest.raises(ValueError, match="state values are invalid"):
        RunnerRuntime(config)


def test_runner_rejects_profile_allowlist_with_duplicate_json_keys(tmp_path: Path) -> None:
    config = runtime_config(tmp_path, successful_backend(tmp_path))
    original = config.profiles_file.read_text(encoding="utf-8")
    config.profiles_file.write_text(
        original.replace('"schema_version": 1', '"schema_version": 1, "schema_version": 1'),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="duplicate key"):
        RunnerRuntime(config)


def runtime_config(
    tmp_path: Path,
    backend: Path,
    *,
    max_output_bytes: int = 1 << 30,
) -> RuntimeConfig:
    scratch_root = tmp_path / "scratch"
    output_root = scratch_root / "outputs"
    state_root = scratch_root / "runner-state"
    output_root.mkdir(parents=True, mode=0o700)
    scratch_root.chmod(0o700)
    output_root.chmod(0o700)
    state_root.mkdir(mode=0o700)
    profiles = tmp_path / "profiles.json"
    profiles.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "backend_revision": "sglang@sha256:test",
                "profiles": [
                    {
                        "model_revision_id": MODEL_ID,
                        "generation_preset_revision_id": PRESET_ID,
                        "execution_profile_revision_id": PROFILE_ID,
                        "output_spec_id": OUTPUT_SPEC_ID,
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    gpu_roles = tmp_path / "gpu-roles.json"
    gpu_roles.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "encoder_vae": "GPU-00000000-0000-0000-0000-000000000001",
                "dit": [f"GPU-00000000-0000-0000-0000-{index:012d}" for index in range(2, 9)],
            }
        ),
        encoding="utf-8",
    )
    return RuntimeConfig(
        socket_path=tmp_path / "run" / "runner.sock",
        scratch_root=scratch_root,
        state_root=state_root,
        output_root=output_root,
        backend_revision="sglang@sha256:test",
        backend_command=(sys.executable, str(backend)),
        profiles_file=profiles,
        gpu_roles_file=gpu_roles,
        stop_timeout_seconds=0.5,
        max_output_bytes=max_output_bytes,
    )


def attempt_identity() -> runner_pb2.RunnerAttemptIdentity:
    return runner_pb2.RunnerAttemptIdentity(
        attempt_id=ATTEMPT_ID,
        job_id=JOB_ID,
        worker_id=WORKER_ID,
        worker_epoch=7,
        lease_fence=11,
    )


def execution_spec() -> runner_pb2.RunnerExecutionSpec:
    return runner_pb2.RunnerExecutionSpec(
        model_revision_id=MODEL_ID,
        generation_preset_revision_id=PRESET_ID,
        execution_profile_revision_id=PROFILE_ID,
        output_spec_id=OUTPUT_SPEC_ID,
        request_content_json=json.dumps({"prompt": "test prompt"}).encode(),
    )


def wait_for_terminal(
    runtime: RunnerRuntime, identity: runner_pb2.RunnerAttemptIdentity
) -> runner_pb2.StatusResponse:
    deadline = time.monotonic() + 3
    while True:
        status = runtime.Status(runner_pb2.StatusRequest(identity=identity), None)
        if status.state in {
            runner_pb2.RUNNER_EXECUTION_STATE_SUCCEEDED,
            runner_pb2.RUNNER_EXECUTION_STATE_FAILED,
            runner_pb2.RUNNER_EXECUTION_STATE_CANCELED,
        }:
            return status
        assert time.monotonic() < deadline
        time.sleep(0.01)


def successful_backend(tmp_path: Path) -> Path:
    script = tmp_path / "successful_backend.py"
    script.write_text(
        """
import argparse
import hashlib
import json
from pathlib import Path

parser = argparse.ArgumentParser()
parser.add_argument("--vela-request", required=True)
parser.add_argument("--vela-output-dir", required=True)
parser.add_argument("--vela-status", required=True)
parser.add_argument("--vela-output-manifest", required=True)
parser.add_argument("--vela-failure", required=True)
parser.add_argument("--vela-resume", required=True)
args = parser.parse_args()
request = json.loads(Path(args.vela_request).read_text(encoding="utf-8"))
assert request["identity"]["attempt_id"]
output_dir = Path(args.vela_output_dir)
output_dir.mkdir(parents=True, exist_ok=True)
video = output_dir / "video.mp4"
thumbnail = output_dir / "thumbnail.webp"
video.write_bytes(b"verified-video")
thumbnail.write_bytes(b"verified-thumbnail")
manifest = {
    "schema_version": 1,
    "outputs": [
        {"kind": "VIDEO", "ordinal": 0, "path": str(video), "content_type": "video/mp4"},
        {"kind": "THUMBNAIL", "ordinal": 0, "path": str(thumbnail), "content_type": "image/webp"},
    ],
}
Path(args.vela_output_manifest).write_text(json.dumps(manifest), encoding="utf-8")
Path(args.vela_status).write_text(
    json.dumps({"schema_version": 1, "backend_stage": "vae", "sequence": 3}),
    encoding="utf-8",
)
""".strip(),
        encoding="utf-8",
    )
    return script


def sparse_output_backend(tmp_path: Path) -> Path:
    script = tmp_path / "sparse_output_backend.py"
    script.write_text(
        """
import argparse
import json
from pathlib import Path

parser = argparse.ArgumentParser()
parser.add_argument("--vela-request", required=True)
parser.add_argument("--vela-output-dir", required=True)
parser.add_argument("--vela-status", required=True)
parser.add_argument("--vela-output-manifest", required=True)
parser.add_argument("--vela-failure", required=True)
parser.add_argument("--vela-resume", required=True)
args = parser.parse_args()
output_dir = Path(args.vela_output_dir)
output_dir.mkdir(parents=True, exist_ok=True)
video = output_dir / "video.mp4"
with video.open("wb") as destination:
    destination.truncate(32 << 20)
Path(args.vela_output_manifest).write_text(json.dumps({
    "schema_version": 1,
    "outputs": [{
        "kind": "VIDEO", "ordinal": 0, "path": str(video), "content_type": "video/mp4"
    }],
}), encoding="utf-8")
""".strip(),
        encoding="utf-8",
    )
    return script


def resumable_backend(tmp_path: Path) -> Path:
    script = tmp_path / "resumable_backend.py"
    script.write_text(
        f"""
import argparse
import json
import time
from pathlib import Path

parser = argparse.ArgumentParser()
parser.add_argument("--vela-request", required=True)
parser.add_argument("--vela-output-dir", required=True)
parser.add_argument("--vela-status", required=True)
parser.add_argument("--vela-output-manifest", required=True)
parser.add_argument("--vela-failure", required=True)
parser.add_argument("--vela-resume", required=True)
args = parser.parse_args()
if args.vela_resume == "false":
    Path({str(tmp_path / "resume-observed")!r}).write_text(args.vela_resume, encoding="utf-8")
    time.sleep(30)
else:
    Path({str(tmp_path / "resume-observed")!r}).write_text(args.vela_resume, encoding="utf-8")
    output_dir = Path(args.vela_output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    video = output_dir / "video.mp4"
    video.write_bytes(b"resumed-video")
    Path(args.vela_output_manifest).write_text(json.dumps({{
        "schema_version": 1,
        "outputs": [{{
            "kind": "VIDEO", "ordinal": 0, "path": str(video), "content_type": "video/mp4"
        }}],
    }}), encoding="utf-8")
""".strip(),
        encoding="utf-8",
    )
    return script


def failing_backend(tmp_path: Path) -> Path:
    script = tmp_path / "failing_backend.py"
    script.write_text(
        """
import argparse
import json
from pathlib import Path

parser = argparse.ArgumentParser()
parser.add_argument("--vela-request", required=True)
parser.add_argument("--vela-output-dir", required=True)
parser.add_argument("--vela-status", required=True)
parser.add_argument("--vela-output-manifest", required=True)
parser.add_argument("--vela-failure", required=True)
parser.add_argument("--vela-resume", required=True)
args = parser.parse_args()
output_dir = Path(args.vela_output_dir)
output_dir.mkdir(parents=True, exist_ok=True)
(output_dir / "partial.bin").write_bytes(b"partial-customer-content")
Path(args.vela_failure).write_text(json.dumps({
    "schema_version": 1,
    "failure_class": "CUDA_OOM",
    "failure_fingerprint": "cuda/oom/dit",
    "error_summary": "certified DiT process exhausted device memory",
    "backend_stage": "dit",
    "gpu_uuids": ["GPU-00000000-0000-0000-0000-000000000002"],
    "retry_recommended": True,
    "worker_reusable": False,
}), encoding="utf-8")
raise SystemExit(7)
""".strip(),
        encoding="utf-8",
    )
    return script


def malformed_failure_backend(tmp_path: Path) -> Path:
    script = tmp_path / "malformed_failure_backend.py"
    script.write_text(
        """
import argparse
import json
from pathlib import Path

parser = argparse.ArgumentParser()
parser.add_argument("--vela-request", required=True)
parser.add_argument("--vela-output-dir", required=True)
parser.add_argument("--vela-status", required=True)
parser.add_argument("--vela-output-manifest", required=True)
parser.add_argument("--vela-failure", required=True)
parser.add_argument("--vela-resume", required=True)
args = parser.parse_args()
Path(args.vela_failure).write_text(json.dumps({
    "schema_version": 1,
    "failure_class": "CUDA_OOM",
    "failure_fingerprint": "cuda/oom/dit",
    "error_summary": "receipt contains invalid boolean fields",
    "backend_stage": "dit",
    "gpu_uuids": [],
    "retry_recommended": "false",
    "worker_reusable": "false",
}), encoding="utf-8")
raise SystemExit(7)
""".strip(),
        encoding="utf-8",
    )
    return script


def symlink_backend(tmp_path: Path) -> Path:
    target = tmp_path / "outside-video.mp4"
    target.write_bytes(b"outside")
    script = tmp_path / "symlink_backend.py"
    script.write_text(
        f"""
import argparse
import json
from pathlib import Path

parser = argparse.ArgumentParser()
parser.add_argument("--vela-request", required=True)
parser.add_argument("--vela-output-dir", required=True)
parser.add_argument("--vela-status", required=True)
parser.add_argument("--vela-output-manifest", required=True)
parser.add_argument("--vela-failure", required=True)
parser.add_argument("--vela-resume", required=True)
args = parser.parse_args()
output_dir = Path(args.vela_output_dir)
output_dir.mkdir(parents=True, exist_ok=True)
output = output_dir / "video.mp4"
output.symlink_to(Path({str(target)!r}))
Path(args.vela_output_manifest).write_text(json.dumps({{
    "schema_version": 1,
    "outputs": [{{
        "kind": "VIDEO", "ordinal": 0, "path": str(output), "content_type": "video/mp4"
    }}],
}}), encoding="utf-8")
""".strip(),
        encoding="utf-8",
    )
    return script


def hardlink_backend(tmp_path: Path) -> Path:
    outside = tmp_path / "outside-hardlink-video.mp4"
    outside.write_bytes(b"outside-hardlink")
    script = tmp_path / "hardlink_backend.py"
    script.write_text(
        f"""
import argparse
import json
import os
from pathlib import Path

parser = argparse.ArgumentParser()
parser.add_argument("--vela-request", required=True)
parser.add_argument("--vela-output-dir", required=True)
parser.add_argument("--vela-status", required=True)
parser.add_argument("--vela-output-manifest", required=True)
parser.add_argument("--vela-failure", required=True)
parser.add_argument("--vela-resume", required=True)
args = parser.parse_args()
output_dir = Path(args.vela_output_dir)
output_dir.mkdir(parents=True, exist_ok=True)
output = output_dir / "video.mp4"
os.link({str(outside)!r}, output)
Path(args.vela_output_manifest).write_text(json.dumps({{
    "schema_version": 1,
    "outputs": [{{
        "kind": "VIDEO", "ordinal": 0, "path": str(output), "content_type": "video/mp4"
    }}],
}}), encoding="utf-8")
""".strip(),
        encoding="utf-8",
    )
    return script


def malformed_output_backend(tmp_path: Path) -> Path:
    script = tmp_path / "malformed_output_backend.py"
    script.write_text(
        """
import argparse
import json
from pathlib import Path

parser = argparse.ArgumentParser()
parser.add_argument("--vela-request", required=True)
parser.add_argument("--vela-output-dir", required=True)
parser.add_argument("--vela-status", required=True)
parser.add_argument("--vela-output-manifest", required=True)
parser.add_argument("--vela-failure", required=True)
parser.add_argument("--vela-resume", required=True)
args = parser.parse_args()
Path(args.vela_output_manifest).write_text(json.dumps({
    "schema_version": 1,
    "outputs": [{
        "kind": "VIDEO",
        "ordinal": 0,
        "path": 42,
        "content_type": "video/mp4",
    }],
}), encoding="utf-8")
""".strip(),
        encoding="utf-8",
    )
    return script


def progress_backend(
    tmp_path: Path,
    *,
    progress: float = 0.25,
    remaining: int = 90,
) -> Path:
    script = tmp_path / "progress_backend.py"
    script.write_text(
        f"""
import argparse
import json
import time
from pathlib import Path

parser = argparse.ArgumentParser()
parser.add_argument("--vela-request", required=True)
parser.add_argument("--vela-output-dir", required=True)
parser.add_argument("--vela-status", required=True)
parser.add_argument("--vela-output-manifest", required=True)
parser.add_argument("--vela-failure", required=True)
parser.add_argument("--vela-resume", required=True)
args = parser.parse_args()
Path(args.vela_status).write_text(json.dumps({{
    "schema_version": 1,
    "backend_stage": "dit",
    "sequence": 3,
    "backend_stage_progress": {progress!r},
    "estimated_remaining_seconds": {remaining!r},
}}), encoding="utf-8")
time.sleep(30)
""".strip(),
        encoding="utf-8",
    )
    return script


def single_execution_backend(tmp_path: Path) -> Path:
    counter = tmp_path / "backend-runs"
    script = tmp_path / "single_execution_backend.py"
    script.write_text(
        f"""
import argparse
import json
import time
from pathlib import Path

parser = argparse.ArgumentParser()
parser.add_argument("--vela-request", required=True)
parser.add_argument("--vela-output-dir", required=True)
parser.add_argument("--vela-status", required=True)
parser.add_argument("--vela-output-manifest", required=True)
parser.add_argument("--vela-failure", required=True)
parser.add_argument("--vela-resume", required=True)
args = parser.parse_args()
counter = Path({str(counter)!r})
runs = int(counter.read_text(encoding="utf-8")) + 1 if counter.exists() else 1
counter.write_text(str(runs), encoding="utf-8")
if runs > 1:
    time.sleep(30)
output_dir = Path(args.vela_output_dir)
output_dir.mkdir(parents=True, exist_ok=True)
video = output_dir / "video.mp4"
video.write_bytes(b"single-execution-video")
Path(args.vela_output_manifest).write_text(json.dumps({{
    "schema_version": 1,
    "outputs": [{{
        "kind": "VIDEO", "ordinal": 0, "path": str(video), "content_type": "video/mp4"
    }}],
}}), encoding="utf-8")
""".strip(),
        encoding="utf-8",
    )
    return script
