from __future__ import annotations

import base64
import hashlib
import json
import math
import os
import re
import signal
import stat
import subprocess
import threading
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import grpc

from vela.v1 import runner_pb2, runner_pb2_grpc

_MAX_JSON_BYTES = 1 << 20
_MAX_REQUEST_BYTES = 64 << 10
_MAX_OUTPUTS = 32
_MAX_SEQUENCE = (1 << 63) - 1
_MAX_ESTIMATED_REMAINING_SECONDS = _MAX_SEQUENCE // 1_000_000_000
_MAX_READINESS_SECONDS = 2 * 60 * 60
_KIND_PATTERN = re.compile(r"^[A-Z][A-Z0-9_]{0,31}$")
_CONTENT_TYPE_PATTERN = re.compile(r"^[a-z0-9][a-z0-9.+-]*/[a-z0-9][a-z0-9.+-]*$")
_GPU_PATTERN = re.compile(
    r"^GPU-[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"
)
_SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")
_READINESS_DETAILS = {
    "DEVICE": ("device roles verified", "device roles did not match"),
    "INFERENCE_BACKEND": (
        "inference backend verified",
        "inference backend did not load",
    ),
    "MODEL_WARMUP": ("model warm-up verified", "model warm-up did not complete"),
    "CANARY": ("canary output verified", "canary output did not pass"),
}


@dataclass(frozen=True)
class RuntimeConfig:
    socket_path: Path
    scratch_root: Path
    state_root: Path
    output_root: Path
    backend_revision: str
    backend_command: tuple[str, ...]
    profiles_file: Path
    gpu_roles_file: Path
    stop_timeout_seconds: float
    max_output_bytes: int


@dataclass(frozen=True)
class _Profile:
    model_revision_id: str
    generation_preset_revision_id: str
    execution_profile_revision_id: str
    output_spec_id: str


@dataclass(frozen=True)
class _Output:
    kind: str
    ordinal: int
    path: Path
    size_bytes: int
    sha256: bytes
    content_type: str


class RunnerRuntime(runner_pb2_grpc.RunnerServiceServicer):
    def __init__(self, config: RuntimeConfig) -> None:
        self._config = _validate_config(config)
        self._profiles, profile_backend = _load_profiles(self._config.profiles_file)
        if profile_backend != self._config.backend_revision:
            raise ValueError("profile allowlist does not match the configured backend revision")
        self._gpu_ids = _load_gpu_roles(self._config.gpu_roles_file)
        self._lock = threading.RLock()
        self._identity: runner_pb2.RunnerAttemptIdentity | None = None
        self._spec: runner_pb2.RunnerExecutionSpec | None = None
        self._state = runner_pb2.RUNNER_EXECUTION_STATE_UNSPECIFIED
        self._sequence = 0
        self._backend_stage = ""
        self._backend_stage_progress: float | None = None
        self._estimated_remaining_seconds: int | None = None
        self._process: subprocess.Popen[bytes] | None = None
        self._readiness_process: subprocess.Popen[bytes] | None = None
        self._process_logs: tuple[Any, Any] | None = None
        self._outputs: tuple[_Output, ...] = ()
        self._failure: runner_pb2.RunnerFailure | None = None
        self._resume = False
        self._load_recoverable_state()

    def ProbeReadiness(self, request: Any, context: grpc.ServicerContext | None) -> Any:
        work_dir: Path | None = None
        process: subprocess.Popen[bytes] | None = None
        try:
            with self._lock:
                identity, deadline, check_name = self._validated_readiness_request(request)
                self._refresh_process()
                terminal_states = {
                    runner_pb2.RUNNER_EXECUTION_STATE_SUCCEEDED,
                    runner_pb2.RUNNER_EXECUTION_STATE_FAILED,
                    runner_pb2.RUNNER_EXECUTION_STATE_CANCELED,
                }
                if self._readiness_process is not None:
                    return _readiness_rejected(request, "runner owns an active readiness probe")
                if self._process is not None or (
                    self._identity is not None and self._state not in terminal_states
                ):
                    return _readiness_rejected(request, "runner owns an active Attempt")
                if context is not None:
                    context_remaining = context.time_remaining()
                    if context_remaining is not None and context_remaining <= 0:
                        return _readiness_rejected(request, "readiness deadline elapsed")

                work_dir = self._config.state_root / f".readiness-{uuid.uuid4()}"
                work_dir.mkdir(mode=0o700)
                request_path = work_dir / "request.json"
                result_path = work_dir / "result.json"
                _write_json_atomic(
                    request_path,
                    {
                        "schema_version": 1,
                        "cycle_id": identity.cycle_id,
                        "worker_id": identity.worker_id,
                        "worker_epoch": identity.worker_epoch,
                        "node_identity": identity.node_identity,
                        "execution_profile_revision_id": identity.execution_profile_revision_id,
                        "inference_backend_revision": identity.inference_backend_revision,
                        "deadline": _timestamp_rfc3339(
                            identity.deadline.seconds, identity.deadline.nanos
                        ),
                        "check": check_name,
                    },
                )
                command = [
                    *self._config.backend_command,
                    "--vela-readiness-check",
                    check_name,
                    "--vela-readiness-request",
                    str(request_path),
                    "--vela-readiness-result",
                    str(result_path),
                ]
                environment = {
                    **os.environ,
                    "CUDA_VISIBLE_DEVICES": ",".join(self._gpu_ids),
                    "VELA_RUNNER_BACKEND_REVISION": self._config.backend_revision,
                }
                process = subprocess.Popen(
                    command,
                    stdin=subprocess.DEVNULL,
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                    env=environment,
                    close_fds=True,
                    start_new_session=True,
                )
                self._readiness_process = process

            return_code, stopped = self._wait_readiness_process(process, deadline, context)
            if stopped == "canceled":
                return _readiness_rejected(request, "readiness probe canceled")
            if stopped == "timed_out":
                return _readiness_rejected(request, "readiness probe timed out")
            if return_code != 0:
                return _readiness_rejected(request, "readiness backend rejected the probe")
            result = _read_private_json(result_path)
            evidence, passed = self._readiness_evidence(identity, check_name, result)
            passed_detail, failed_detail = _READINESS_DETAILS[check_name]
            return runner_pb2.ProbeReadinessResponse(
                identity=identity,
                check=request.check,
                decision=runner_pb2.RUNNER_COMMAND_DECISION_ACCEPTED,
                passed=passed,
                evidence_json=json.dumps(
                    evidence, separators=(",", ":"), sort_keys=True
                ).encode(),
                detail=passed_detail if passed else failed_detail,
            )
        except (OSError, TypeError, ValueError, json.JSONDecodeError) as error:
            if isinstance(error, (TypeError, ValueError)) and work_dir is None:
                return _readiness_rejected(request, str(error))
            return _readiness_rejected(request, "readiness evidence is invalid")
        finally:
            if process is not None:
                with self._lock:
                    if self._readiness_process is process:
                        self._readiness_process = None
            if work_dir is not None:
                _remove_flat_directory(work_dir)

    def _wait_readiness_process(
        self,
        process: subprocess.Popen[bytes],
        deadline: datetime,
        context: grpc.ServicerContext | None,
    ) -> tuple[int | None, str]:
        while True:
            return_code = process.poll()
            if return_code is not None:
                return return_code, ""
            if context is not None and not context.is_active():
                _terminate_process_group(process, self._config.stop_timeout_seconds)
                return None, "canceled"
            remaining = (deadline - datetime.now(UTC)).total_seconds()
            if context is not None:
                context_remaining = context.time_remaining()
                if context_remaining is not None:
                    remaining = min(remaining, context_remaining)
            if remaining <= 0:
                _terminate_process_group(process, self._config.stop_timeout_seconds)
                return None, "timed_out"
            try:
                return process.wait(timeout=min(remaining, 0.05)), ""
            except subprocess.TimeoutExpired:
                continue

    def _validated_readiness_request(
        self, request: Any
    ) -> tuple[runner_pb2.RunnerReadinessIdentity, datetime, str]:
        identity = runner_pb2.RunnerReadinessIdentity()
        identity.CopyFrom(request.identity)
        if (
            _canonical_uuid(identity.cycle_id) != identity.cycle_id
            or _canonical_uuid(identity.worker_id) != identity.worker_id
            or identity.worker_epoch <= 0
            or not _valid_text(identity.node_identity, 500)
            or _canonical_uuid(identity.execution_profile_revision_id)
            != identity.execution_profile_revision_id
            or identity.inference_backend_revision != self._config.backend_revision
            or not any(
                profile.execution_profile_revision_id
                == identity.execution_profile_revision_id
                for profile in self._profiles
            )
        ):
            raise ValueError("readiness authority is invalid")
        try:
            deadline = identity.deadline.ToDatetime(tzinfo=UTC)
        except (OverflowError, ValueError) as error:
            raise ValueError("readiness deadline is invalid") from error
        remaining = deadline - datetime.now(UTC)
        if remaining <= timedelta(0) or remaining > timedelta(seconds=_MAX_READINESS_SECONDS):
            raise ValueError("readiness deadline is invalid")
        checks = {
            runner_pb2.RUNNER_READINESS_CHECK_DEVICE: "DEVICE",
            runner_pb2.RUNNER_READINESS_CHECK_INFERENCE_BACKEND: "INFERENCE_BACKEND",
            runner_pb2.RUNNER_READINESS_CHECK_MODEL_WARMUP: "MODEL_WARMUP",
            runner_pb2.RUNNER_READINESS_CHECK_CANARY: "CANARY",
        }
        check_name = checks.get(request.check)
        if check_name is None:
            raise ValueError("readiness check is invalid")
        return identity, deadline, check_name

    def _readiness_evidence(
        self,
        identity: runner_pb2.RunnerReadinessIdentity,
        check_name: str,
        result: dict[str, Any],
    ) -> tuple[dict[str, Any], bool]:
        if check_name == "DEVICE":
            return self._device_readiness_evidence(identity, result)
        if check_name == "INFERENCE_BACKEND":
            return self._inference_backend_readiness_evidence(identity, result)
        if check_name == "MODEL_WARMUP":
            return self._model_warmup_readiness_evidence(identity, result)
        if check_name == "CANARY":
            return self._canary_readiness_evidence(identity, result)
        raise ValueError("readiness check is invalid")

    def _device_readiness_evidence(
        self, identity: runner_pb2.RunnerReadinessIdentity, result: dict[str, Any]
    ) -> tuple[dict[str, Any], bool]:
        if set(result) != {
            "schema_version",
            "check",
            "passed",
            "encoder_vae_gpu_uuid",
            "dit_gpu_uuids",
        }:
            raise ValueError("DEVICE readiness evidence schema is invalid")
        encoder = result["encoder_vae_gpu_uuid"]
        dit = result["dit_gpu_uuids"]
        if (
            result["schema_version"] != 1
            or result["check"] != "DEVICE"
            or type(result["passed"]) is not bool
            or not isinstance(encoder, str)
            or not _valid_gpu_uuid(encoder)
            or not isinstance(dit, list)
            or len(dit) != 7
            or len({encoder, *dit}) != 8
            or any(not _valid_gpu_uuid(gpu) for gpu in dit)
        ):
            raise ValueError("DEVICE readiness evidence is invalid")
        passed = bool(result["passed"] and (encoder, *dit) == self._gpu_ids)
        return self._base_readiness_evidence(identity, "DEVICE", passed) | {
            "encoder_vae_gpu_uuid": encoder,
            "dit_gpu_uuids": dit,
        }, passed

    def _inference_backend_readiness_evidence(
        self, identity: runner_pb2.RunnerReadinessIdentity, result: dict[str, Any]
    ) -> tuple[dict[str, Any], bool]:
        if set(result) != {
            "schema_version",
            "check",
            "passed",
            "inference_backend_revision",
            "loaded",
        } or (
            result["schema_version"] != 1
            or result["check"] != "INFERENCE_BACKEND"
            or type(result["passed"]) is not bool
            or result["inference_backend_revision"] != identity.inference_backend_revision
            or type(result["loaded"]) is not bool
        ):
            raise ValueError("INFERENCE_BACKEND readiness evidence is invalid")
        passed = bool(result["passed"] and result["loaded"])
        return self._base_readiness_evidence(identity, "INFERENCE_BACKEND", passed) | {
            "loaded": result["loaded"]
        }, passed

    def _model_warmup_readiness_evidence(
        self, identity: runner_pb2.RunnerReadinessIdentity, result: dict[str, Any]
    ) -> tuple[dict[str, Any], bool]:
        if set(result) != {
            "schema_version",
            "check",
            "passed",
            "execution_profile_revision_id",
            "warmed",
        } or (
            result["schema_version"] != 1
            or result["check"] != "MODEL_WARMUP"
            or type(result["passed"]) is not bool
            or result["execution_profile_revision_id"]
            != identity.execution_profile_revision_id
            or type(result["warmed"]) is not bool
        ):
            raise ValueError("MODEL_WARMUP readiness evidence is invalid")
        passed = bool(result["passed"] and result["warmed"])
        return self._base_readiness_evidence(identity, "MODEL_WARMUP", passed) | {
            "warmed": result["warmed"]
        }, passed

    def _canary_readiness_evidence(
        self, identity: runner_pb2.RunnerReadinessIdentity, result: dict[str, Any]
    ) -> tuple[dict[str, Any], bool]:
        if set(result) != {
            "schema_version",
            "check",
            "passed",
            "output_sha256",
        } or (
            result["schema_version"] != 1
            or result["check"] != "CANARY"
            or type(result["passed"]) is not bool
            or not isinstance(result["output_sha256"], str)
            or _SHA256_PATTERN.fullmatch(result["output_sha256"]) is None
        ):
            raise ValueError("CANARY readiness evidence is invalid")
        return self._base_readiness_evidence(identity, "CANARY", result["passed"]) | {
            "output_sha256": result["output_sha256"]
        }, result["passed"]

    def _base_readiness_evidence(
        self,
        identity: runner_pb2.RunnerReadinessIdentity,
        check_name: str,
        passed: bool,
    ) -> dict[str, Any]:
        return {
            "schema_version": 1,
            "cycle_id": identity.cycle_id,
            "worker_id": identity.worker_id,
            "worker_epoch": identity.worker_epoch,
            "node_identity": identity.node_identity,
            "execution_profile_revision_id": identity.execution_profile_revision_id,
            "inference_backend_revision": identity.inference_backend_revision,
            "deadline": _timestamp_rfc3339(identity.deadline.seconds, identity.deadline.nanos),
            "check": check_name,
            "passed": passed,
        }

    def Prepare(self, request: Any, context: grpc.ServicerContext | None) -> Any:
        del context
        with self._lock:
            try:
                identity = _validated_identity(request.identity)
                spec = _validated_spec(request.execution_spec, self._profiles)
            except ValueError as error:
                return runner_pb2.PrepareResponse(
                    identity=request.identity,
                    decision=runner_pb2.RUNNER_COMMAND_DECISION_REJECTED,
                    detail=str(error),
                )
            if self._readiness_process is not None:
                return runner_pb2.PrepareResponse(
                    identity=identity,
                    decision=runner_pb2.RUNNER_COMMAND_DECISION_REJECTED,
                    detail="runner owns an active readiness probe",
                )
            if self._identity is not None:
                if not _same_message(self._identity, identity) or not _same_message(
                    self._spec, spec
                ):
                    if self._identity.attempt_id == identity.attempt_id:
                        return runner_pb2.PrepareResponse(
                            identity=identity,
                            decision=runner_pb2.RUNNER_COMMAND_DECISION_REJECTED,
                            detail=(
                                "Attempt ID is already bound to different authority "
                                "or specification"
                            ),
                        )
                    if self._state not in {
                        runner_pb2.RUNNER_EXECUTION_STATE_SUCCEEDED,
                        runner_pb2.RUNNER_EXECUTION_STATE_FAILED,
                        runner_pb2.RUNNER_EXECUTION_STATE_CANCELED,
                    }:
                        return runner_pb2.PrepareResponse(
                            identity=identity,
                            decision=runner_pb2.RUNNER_COMMAND_DECISION_REJECTED,
                            detail="runner already owns a different active Attempt",
                        )
                    self._discard_terminal_state()
                else:
                    if (
                        self._resume
                        and not request.same_authority_local_recovery
                        and self._process is None
                        and self._state == runner_pb2.RUNNER_EXECUTION_STATE_PREPARING
                    ):
                        _remove_flat_directory(self._attempt_state_dir())
                        self._remove_attempt_outputs()
                        self._sequence = 0
                        self._backend_stage = "prepare"
                        self._backend_stage_progress = None
                        self._estimated_remaining_seconds = None
                        self._resume = False
                        self._outputs = ()
                        self._failure = None
                        self._prepare_directories()
                        self._persist_request()
                        self._persist_state()
                    resumed = bool(request.same_authority_local_recovery and self._resume)
                    return runner_pb2.PrepareResponse(
                        identity=identity,
                        decision=runner_pb2.RUNNER_COMMAND_DECISION_ACCEPTED,
                        resumed_local_state=resumed,
                        detail="same Attempt preparation replayed",
                    )
            self._identity = identity
            self._spec = spec
            self._state = runner_pb2.RUNNER_EXECUTION_STATE_PREPARING
            self._sequence = 0
            self._backend_stage = "prepare"
            self._backend_stage_progress = None
            self._estimated_remaining_seconds = None
            self._outputs = ()
            self._failure = None
            self._resume = False
            self._prepare_directories()
            self._persist_request()
            self._persist_state()
            return runner_pb2.PrepareResponse(
                identity=identity,
                decision=runner_pb2.RUNNER_COMMAND_DECISION_ACCEPTED,
                resumed_local_state=False,
                detail="exact certified profile prepared",
            )

    def Start(self, request: Any, context: grpc.ServicerContext | None) -> Any:
        del context
        with self._lock:
            if not self._matches_current(request.identity):
                return runner_pb2.StartResponse(
                    identity=request.identity,
                    decision=runner_pb2.RUNNER_COMMAND_DECISION_REJECTED,
                    detail="Start does not match a prepared Attempt",
                )
            if self._state in {
                runner_pb2.RUNNER_EXECUTION_STATE_SUCCEEDED,
                runner_pb2.RUNNER_EXECUTION_STATE_FAILED,
                runner_pb2.RUNNER_EXECUTION_STATE_CANCELED,
            }:
                return runner_pb2.StartResponse(
                    identity=request.identity,
                    decision=runner_pb2.RUNNER_COMMAND_DECISION_ACCEPTED,
                    detail="same Attempt terminal Start replayed",
                )
            if self._state not in {
                runner_pb2.RUNNER_EXECUTION_STATE_PREPARING,
                runner_pb2.RUNNER_EXECUTION_STATE_READY,
            }:
                return runner_pb2.StartResponse(
                    identity=request.identity,
                    decision=runner_pb2.RUNNER_COMMAND_DECISION_REJECTED,
                    detail="Start does not match a prepared Attempt",
                )
            if self._process is not None:
                return runner_pb2.StartResponse(
                    identity=request.identity,
                    decision=runner_pb2.RUNNER_COMMAND_DECISION_ACCEPTED,
                    detail="same Attempt Start replayed",
                )
            attempt_dir = self._attempt_state_dir()
            output_dir = self._attempt_output_dir()
            stdout = open(attempt_dir / "backend.stdout", "ab", buffering=0)
            stderr = open(attempt_dir / "backend.stderr", "ab", buffering=0)
            os.chmod(stdout.name, 0o600)
            os.chmod(stderr.name, 0o600)
            command = [
                *self._config.backend_command,
                "--vela-request",
                str(attempt_dir / "request.json"),
                "--vela-output-dir",
                str(output_dir),
                "--vela-status",
                str(attempt_dir / "backend-status.json"),
                "--vela-output-manifest",
                str(attempt_dir / "outputs.json"),
                "--vela-failure",
                str(attempt_dir / "failure.json"),
                "--vela-resume",
                "true" if self._resume else "false",
            ]
            environment = {
                **os.environ,
                "CUDA_VISIBLE_DEVICES": ",".join(self._gpu_ids),
                "VELA_RUNNER_BACKEND_REVISION": self._config.backend_revision,
            }
            try:
                self._process = subprocess.Popen(
                    command,
                    stdin=subprocess.DEVNULL,
                    stdout=stdout,
                    stderr=stderr,
                    env=environment,
                    close_fds=True,
                    start_new_session=True,
                )
            except OSError:
                stdout.close()
                stderr.close()
                self._failure = _default_failure(self._config.backend_revision, "backend/start")
                self._state = runner_pb2.RUNNER_EXECUTION_STATE_FAILED
                self._sequence += 1
                self._persist_state()
                return runner_pb2.StartResponse(
                    identity=request.identity,
                    decision=runner_pb2.RUNNER_COMMAND_DECISION_REJECTED,
                    detail="backend process could not start",
                )
            self._process_logs = (stdout, stderr)
            self._state = runner_pb2.RUNNER_EXECUTION_STATE_RUNNING
            self._sequence += 1
            self._backend_stage = "backend-start"
            self._resume = True
            self._persist_state()
            return runner_pb2.StartResponse(
                identity=request.identity,
                decision=runner_pb2.RUNNER_COMMAND_DECISION_ACCEPTED,
                detail="backend process started",
            )

    def Cancel(self, request: Any, context: grpc.ServicerContext | None) -> Any:
        del context
        with self._lock:
            if not self._matches_current(request.identity) or request.reason not in {
                runner_pb2.RUNNER_CANCEL_REASON_CONTROL_PLANE_STOP,
                runner_pb2.RUNNER_CANCEL_REASON_LEASE_DEADLINE,
                runner_pb2.RUNNER_CANCEL_REASON_AGENT_SHUTDOWN,
            }:
                return runner_pb2.CancelResponse(
                    identity=request.identity,
                    decision=runner_pb2.RUNNER_COMMAND_DECISION_REJECTED,
                    detail="Cancel does not match the active Attempt authority",
                )
            self._refresh_process()
            if self._state in {
                runner_pb2.RUNNER_EXECUTION_STATE_SUCCEEDED,
                runner_pb2.RUNNER_EXECUTION_STATE_FAILED,
                runner_pb2.RUNNER_EXECUTION_STATE_CANCELED,
            }:
                if self._state in {
                    runner_pb2.RUNNER_EXECUTION_STATE_FAILED,
                    runner_pb2.RUNNER_EXECUTION_STATE_CANCELED,
                }:
                    self._remove_attempt_outputs()
                    self._persist_state()
                return runner_pb2.CancelResponse(
                    identity=request.identity,
                    decision=runner_pb2.RUNNER_COMMAND_DECISION_ACCEPTED,
                    detail="same Attempt terminal Cancel replayed",
                )
            self._stop_process()
            self._backend_stage_progress = None
            self._estimated_remaining_seconds = None
            preserve = request.reason == runner_pb2.RUNNER_CANCEL_REASON_AGENT_SHUTDOWN
            if preserve:
                self._resume = True
                self._state = runner_pb2.RUNNER_EXECUTION_STATE_PREPARING
                self._backend_stage = "shutdown-recovery"
                self._sequence += 1
                self._persist_state()
            elif self._state != runner_pb2.RUNNER_EXECUTION_STATE_SUCCEEDED:
                self._resume = False
                self._state = runner_pb2.RUNNER_EXECUTION_STATE_CANCELED
                self._sequence += 1
                self._remove_attempt_outputs()
                self._persist_state()
            return runner_pb2.CancelResponse(
                identity=request.identity,
                decision=runner_pb2.RUNNER_COMMAND_DECISION_ACCEPTED,
                detail="backend process stopped",
            )

    def Status(self, request: Any, context: grpc.ServicerContext | None) -> Any:
        del context
        with self._lock:
            if not self._matches_current(request.identity):
                return runner_pb2.StatusResponse(identity=request.identity)
            self._refresh_process()
            response = runner_pb2.StatusResponse(
                identity=self._identity,
                state=self._state,
                sequence=self._sequence,
                backend_stage=self._backend_stage,
                gpu_health_json=json.dumps(
                    {
                        "healthy": self._state != runner_pb2.RUNNER_EXECUTION_STATE_FAILED,
                        "gpu_uuids": self._gpu_ids,
                    },
                    separators=(",", ":"),
                ).encode(),
                local_artifact_state_json=json.dumps(
                    {"output_count": len(self._outputs)}, separators=(",", ":")
                ).encode(),
            )
            if self._backend_stage_progress is not None:
                response.backend_stage_progress = self._backend_stage_progress
            if self._estimated_remaining_seconds is not None:
                response.estimated_remaining_seconds = self._estimated_remaining_seconds
            if self._failure is not None:
                response.failure.CopyFrom(self._failure)
            return response

    def CollectOutputs(self, request: Any, context: grpc.ServicerContext | None) -> Any:
        del context
        with self._lock:
            if not self._matches_current(request.identity):
                return runner_pb2.CollectOutputsResponse(
                    identity=request.identity,
                    decision=runner_pb2.RUNNER_COMMAND_DECISION_REJECTED,
                    detail="output request does not match the active Attempt",
                )
            self._refresh_process()
            if self._state != runner_pb2.RUNNER_EXECUTION_STATE_SUCCEEDED or not self._outputs:
                return runner_pb2.CollectOutputsResponse(
                    identity=request.identity,
                    decision=runner_pb2.RUNNER_COMMAND_DECISION_REJECTED,
                    detail="outputs are not complete",
                )
            return runner_pb2.CollectOutputsResponse(
                identity=self._identity,
                decision=runner_pb2.RUNNER_COMMAND_DECISION_ACCEPTED,
                outputs=[
                    runner_pb2.RunnerOutput(
                        kind=output.kind,
                        ordinal=output.ordinal,
                        path=str(output.path),
                        size_bytes=output.size_bytes,
                        sha256=output.sha256,
                        content_type=output.content_type,
                    )
                    for output in self._outputs
                ],
                detail="verified output receipt",
            )

    def close(self) -> None:
        with self._lock:
            if self._readiness_process is not None:
                _terminate_process_group(
                    self._readiness_process, self._config.stop_timeout_seconds
                )
                self._readiness_process = None
            self._stop_process()

    def _refresh_process(self) -> None:
        if self._process is None:
            return
        result = self._process.poll()
        if result is None:
            self._load_backend_status()
            return
        self._close_process_logs()
        self._process = None
        if result == 0:
            try:
                self._outputs = self._load_outputs()
            except (OSError, TypeError, ValueError):
                self._failure = _default_failure(
                    self._config.backend_revision, "backend/output-receipt"
                )
                self._state = runner_pb2.RUNNER_EXECUTION_STATE_FAILED
                self._backend_stage = "output-receipt"
                try:
                    self._remove_attempt_outputs()
                except (OSError, ValueError):
                    self._failure = _default_failure(
                        self._config.backend_revision, "backend/output-cleanup"
                    )
                    self._backend_stage = "output-cleanup"
            else:
                self._state = runner_pb2.RUNNER_EXECUTION_STATE_SUCCEEDED
                self._backend_stage = "complete"
        else:
            self._failure = self._load_failure()
            self._state = runner_pb2.RUNNER_EXECUTION_STATE_FAILED
            self._backend_stage = "backend-failed"
            self._remove_attempt_outputs()
        self._backend_stage_progress = None
        self._estimated_remaining_seconds = None
        self._sequence += 1
        self._resume = False
        self._persist_state()

    def _load_backend_status(self) -> None:
        path = self._attempt_state_dir() / "backend-status.json"
        if not path.exists():
            return
        try:
            payload = _read_json(path)
            if set(payload) - {
                "schema_version",
                "backend_stage",
                "sequence",
                "backend_stage_progress",
                "estimated_remaining_seconds",
            }:
                return
            if payload.get("schema_version") != 1:
                return
            sequence = payload.get("sequence")
            stage = payload.get("backend_stage")
            progress = payload.get("backend_stage_progress")
            remaining = payload.get("estimated_remaining_seconds")
            if (
                type(sequence) is not int
                or not self._sequence <= sequence < _MAX_SEQUENCE
                or not _valid_text(stage, 100)
                or (
                    progress is not None
                    and (
                        type(progress) not in {int, float}
                        or not math.isfinite(progress)
                        or not 0 <= progress < 1
                    )
                )
                or (
                    remaining is not None
                    and (
                        type(remaining) is not int
                        or not 0 <= remaining <= _MAX_ESTIMATED_REMAINING_SECONDS
                    )
                )
            ):
                return
            self._sequence = sequence
            self._backend_stage = stage
            self._backend_stage_progress = float(progress) if progress is not None else None
            self._estimated_remaining_seconds = remaining
        except (OSError, ValueError, json.JSONDecodeError):
            return

    def _load_outputs(self) -> tuple[_Output, ...]:
        payload = _read_json(self._attempt_state_dir() / "outputs.json")
        if set(payload) != {"schema_version", "outputs"} or payload["schema_version"] != 1:
            raise ValueError("backend output manifest schema is invalid")
        candidates = payload["outputs"]
        if not isinstance(candidates, list) or not 0 < len(candidates) <= _MAX_OUTPUTS:
            raise ValueError("backend output manifest count is invalid")
        outputs: list[_Output] = []
        identities: set[tuple[str, int]] = set()
        paths: set[Path] = set()
        total_size = 0
        attempt_root = self._attempt_output_dir().resolve(strict=True)
        directory_descriptor = os.open(
            attempt_root,
            os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC,
        )
        try:
            for candidate in candidates:
                if not isinstance(candidate, dict) or set(candidate) != {
                    "kind",
                    "ordinal",
                    "path",
                    "content_type",
                }:
                    raise ValueError("backend output entry schema is invalid")
                kind = candidate["kind"]
                ordinal = candidate["ordinal"]
                content_type = candidate["content_type"]
                path_value = candidate["path"]
                if (
                    not isinstance(kind, str)
                    or _KIND_PATTERN.fullmatch(kind) is None
                    or type(ordinal) is not int
                    or ordinal < 0
                    or not isinstance(content_type, str)
                    or _CONTENT_TYPE_PATTERN.fullmatch(content_type) is None
                    or not isinstance(path_value, str)
                ):
                    raise ValueError("backend output entry is invalid")
                path = Path(path_value)
                if not path.is_absolute() or path.parent != attempt_root:
                    raise ValueError("backend output entry is invalid")
                descriptor = os.open(
                    path.name,
                    os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC,
                    dir_fd=directory_descriptor,
                )
                try:
                    info = os.fstat(descriptor)
                    if (
                        not stat.S_ISREG(info.st_mode)
                        or info.st_uid != os.geteuid()
                        or info.st_size <= 0
                        or info.st_nlink != 1
                    ):
                        raise ValueError("backend output file identity is invalid")
                    os.fchmod(descriptor, 0o600)
                    identity = (kind, ordinal)
                    if identity in identities or path in paths:
                        raise ValueError("backend output manifest contains a duplicate")
                    identities.add(identity)
                    paths.add(path)
                    if info.st_size > self._config.max_output_bytes - total_size:
                        raise ValueError("backend output manifest exceeds its byte limit")
                    total_size += info.st_size
                    digest = hashlib.sha256()
                    size = 0
                    while chunk := os.read(descriptor, 1 << 20):
                        size += len(chunk)
                        digest.update(chunk)
                    after = os.fstat(descriptor)
                    if (
                        size != info.st_size
                        or after.st_dev != info.st_dev
                        or after.st_ino != info.st_ino
                        or after.st_size != info.st_size
                        or after.st_nlink != 1
                    ):
                        raise ValueError("backend output changed while it was read")
                    outputs.append(
                        _Output(kind, ordinal, path, size, digest.digest(), content_type)
                    )
                finally:
                    os.close(descriptor)
        finally:
            os.close(directory_descriptor)
        return tuple(sorted(outputs, key=lambda output: (output.kind, output.ordinal)))

    def _load_failure(self) -> runner_pb2.RunnerFailure:
        path = self._attempt_state_dir() / "failure.json"
        try:
            payload = _read_json(path)
            expected = {
                "schema_version",
                "failure_class",
                "failure_fingerprint",
                "error_summary",
                "backend_stage",
                "gpu_uuids",
                "retry_recommended",
                "worker_reusable",
            }
            if set(payload) != expected or payload["schema_version"] != 1:
                raise ValueError
            failure = _failure_from_dict(
                {key: value for key, value in payload.items() if key != "schema_version"}
                | {"inference_backend_revision": self._config.backend_revision},
                self._config.backend_revision,
            )
            if failure is None or any(gpu not in self._gpu_ids for gpu in failure.gpu_uuids):
                raise ValueError
            return failure
        except (OSError, ValueError, KeyError, TypeError, json.JSONDecodeError):
            return _default_failure(self._config.backend_revision, "backend/process-exit")

    def _stop_process(self) -> None:
        process = self._process
        if process is None:
            return
        _terminate_process_group(process, self._config.stop_timeout_seconds)
        self._process = None
        self._close_process_logs()

    def _close_process_logs(self) -> None:
        if self._process_logs is not None:
            for stream in self._process_logs:
                stream.close()
            self._process_logs = None

    def _matches_current(self, identity: Any) -> bool:
        try:
            candidate = _validated_identity(identity)
        except ValueError:
            return False
        return self._identity is not None and _same_message(self._identity, candidate)

    def _prepare_directories(self) -> None:
        for path in (self._attempt_state_dir(), self._attempt_output_dir()):
            path.mkdir(mode=0o700, parents=False, exist_ok=True)
            _validate_directory(path)

    def _persist_request(self) -> None:
        assert self._identity is not None and self._spec is not None
        _write_json_atomic(
            self._attempt_state_dir() / "request.json",
            {
                "schema_version": 1,
                "identity": _identity_dict(self._identity),
                "execution_spec": {
                    "model_revision_id": self._spec.model_revision_id,
                    "generation_preset_revision_id": self._spec.generation_preset_revision_id,
                    "execution_profile_revision_id": self._spec.execution_profile_revision_id,
                    "output_spec_id": self._spec.output_spec_id,
                    "request_content_base64": base64.b64encode(
                        self._spec.request_content_json
                    ).decode("ascii"),
                },
            },
        )

    def _persist_state(self) -> None:
        if self._identity is None or self._spec is None:
            return
        _write_json_atomic(
            self._attempt_state_dir() / "state.json",
            {
                "schema_version": 1,
                "identity": _identity_dict(self._identity),
                "spec_digest": hashlib.sha256(self._spec.SerializeToString()).hexdigest(),
                "state": int(self._state),
                "sequence": self._sequence,
                "backend_stage": self._backend_stage,
                "resume": self._resume,
                "failure": _failure_dict(self._failure),
                "outputs": [_output_dict(output) for output in self._outputs],
            },
        )

    def _load_recoverable_state(self) -> None:
        entries = list(self._config.state_root.iterdir())
        if not entries:
            return
        if len(entries) != 1 or not entries[0].is_dir() or entries[0].is_symlink():
            raise ValueError("runner state root contains unsafe or ambiguous recovery state")
        attempt_dir = entries[0]
        _validate_directory(attempt_dir)
        state_payload = _read_private_json(attempt_dir / "state.json")
        request_payload = _read_private_json(attempt_dir / "request.json")
        if (
            set(state_payload)
            != {
                "schema_version",
                "identity",
                "spec_digest",
                "state",
                "sequence",
                "backend_stage",
                "resume",
                "failure",
                "outputs",
            }
            or state_payload["schema_version"] != 1
        ):
            raise ValueError("runner recovery state schema is invalid")
        if (
            set(request_payload)
            != {
                "schema_version",
                "identity",
                "execution_spec",
            }
            or request_payload["schema_version"] != 1
        ):
            raise ValueError("runner recovery request schema is invalid")
        identity = _identity_from_dict(state_payload["identity"])
        request_identity = _identity_from_dict(request_payload["identity"])
        if not _same_message(identity, request_identity) or attempt_dir.name != identity.attempt_id:
            raise ValueError("runner recovery state Attempt identity is invalid")
        spec = _spec_from_dict(request_payload["execution_spec"])
        identity = _validated_identity(identity)
        spec = _validated_spec(spec, self._profiles)
        if state_payload["spec_digest"] != hashlib.sha256(spec.SerializeToString()).hexdigest():
            raise ValueError("runner recovery state specification digest is invalid")
        state = state_payload["state"]
        sequence = state_payload["sequence"]
        backend_stage = state_payload["backend_stage"]
        resume = state_payload["resume"]
        terminal_states = {
            runner_pb2.RUNNER_EXECUTION_STATE_SUCCEEDED,
            runner_pb2.RUNNER_EXECUTION_STATE_FAILED,
            runner_pb2.RUNNER_EXECUTION_STATE_CANCELED,
        }
        valid_states = terminal_states | {
            runner_pb2.RUNNER_EXECUTION_STATE_PREPARING,
            runner_pb2.RUNNER_EXECUTION_STATE_READY,
            runner_pb2.RUNNER_EXECUTION_STATE_RUNNING,
        }
        if (
            type(state) is not int
            or state not in valid_states
            or type(sequence) is not int
            or not 0 <= sequence <= _MAX_SEQUENCE
            or (state not in terminal_states and sequence == _MAX_SEQUENCE)
            or not _valid_text(backend_stage, 100)
            or type(resume) is not bool
        ):
            raise ValueError("runner recovery state values are invalid")
        failure = _failure_from_dict(state_payload["failure"], self._config.backend_revision)
        if failure is not None and any(gpu not in self._gpu_ids for gpu in failure.gpu_uuids):
            raise ValueError("runner recovery failure evidence is invalid")
        if (state == runner_pb2.RUNNER_EXECUTION_STATE_FAILED) != (failure is not None):
            raise ValueError("runner recovery failure evidence does not match its state")
        if state in terminal_states and resume:
            raise ValueError("terminal runner recovery state cannot be resumable")
        self._identity = identity
        self._spec = spec
        self._state = state
        self._sequence = sequence
        self._backend_stage = backend_stage
        self._backend_stage_progress = None
        self._estimated_remaining_seconds = None
        self._resume = resume
        self._failure = failure
        if state == runner_pb2.RUNNER_EXECUTION_STATE_SUCCEEDED:
            self._outputs = self._load_outputs()
            persisted_outputs = _outputs_from_dicts(
                state_payload["outputs"], self._attempt_output_dir()
            )
            if self._outputs != persisted_outputs:
                raise ValueError("runner successful output receipt changed")
        elif state not in terminal_states:
            if state_payload["outputs"] != []:
                raise ValueError("non-successful runner state contains output receipts")
            self._state = runner_pb2.RUNNER_EXECUTION_STATE_PREPARING
            self._backend_stage = "process-recovery"
            self._resume = True
        elif state_payload["outputs"] != []:
            raise ValueError("non-successful runner state contains output receipts")

    def _discard_terminal_state(self) -> None:
        self._stop_process()
        if self._identity is not None:
            self._remove_attempt_outputs()
            _remove_flat_directory(self._attempt_state_dir())
        self._identity = None
        self._spec = None
        self._outputs = ()
        self._failure = None
        self._backend_stage_progress = None
        self._estimated_remaining_seconds = None
        self._resume = False

    def _remove_attempt_outputs(self) -> None:
        path = self._attempt_output_dir()
        if path.exists():
            _remove_output_directory(path)

    def _attempt_state_dir(self) -> Path:
        assert self._identity is not None
        return self._config.state_root / self._identity.attempt_id

    def _attempt_output_dir(self) -> Path:
        assert self._identity is not None
        return self._config.output_root / self._identity.attempt_id


def _validate_config(config: RuntimeConfig) -> RuntimeConfig:
    paths = (
        config.socket_path,
        config.scratch_root,
        config.state_root,
        config.output_root,
        config.profiles_file,
        config.gpu_roles_file,
    )
    if any(not path.is_absolute() for path in paths):
        raise ValueError("runner paths must be absolute")
    if (
        config.state_root.parent != config.scratch_root
        or config.output_root.parent != config.scratch_root
        or config.state_root == config.output_root
    ):
        raise ValueError("runner state and output roots must be separate scratch children")
    if (
        not config.backend_revision
        or not config.backend_command
        or not Path(config.backend_command[0]).is_absolute()
        or any(not argument or "\x00" in argument for argument in config.backend_command)
        or not 0.1 <= config.stop_timeout_seconds <= 30
        or type(config.max_output_bytes) is not int
        or not 0 < config.max_output_bytes <= _MAX_SEQUENCE
    ):
        raise ValueError("runner backend configuration is invalid")
    for directory in (config.scratch_root, config.state_root, config.output_root):
        _validate_directory(directory)
    return config


def _validate_directory(path: Path) -> None:
    info = path.lstat()
    if (
        not stat.S_ISDIR(info.st_mode)
        or path.is_symlink()
        or info.st_uid != os.geteuid()
        or stat.S_IMODE(info.st_mode) & 0o077
    ):
        raise ValueError(f"runner directory is unsafe: {path}")


def _load_profiles(path: Path) -> tuple[frozenset[_Profile], str]:
    payload = _read_secure_json(path)
    if set(payload) != {"schema_version", "backend_revision", "profiles"}:
        raise ValueError("profile allowlist schema is invalid")
    if payload["schema_version"] != 1 or not _valid_text(payload["backend_revision"], 200):
        raise ValueError("profile allowlist header is invalid")
    profiles: set[_Profile] = set()
    for candidate in payload["profiles"]:
        if not isinstance(candidate, dict) or set(candidate) != {
            "model_revision_id",
            "generation_preset_revision_id",
            "execution_profile_revision_id",
            "output_spec_id",
        }:
            raise ValueError("profile allowlist entry is invalid")
        profiles.add(
            _Profile(
                model_revision_id=_canonical_uuid(candidate["model_revision_id"]),
                generation_preset_revision_id=_canonical_uuid(
                    candidate["generation_preset_revision_id"]
                ),
                execution_profile_revision_id=_canonical_uuid(
                    candidate["execution_profile_revision_id"]
                ),
                output_spec_id=_canonical_uuid(candidate["output_spec_id"]),
            )
        )
    if not profiles or len(profiles) > 1024:
        raise ValueError("profile allowlist count is invalid")
    return frozenset(profiles), payload["backend_revision"]


def _load_gpu_roles(path: Path) -> tuple[str, ...]:
    payload = _read_secure_json(path)
    if set(payload) != {"schema_version", "encoder_vae", "dit"} or payload["schema_version"] != 1:
        raise ValueError("GPU role map schema is invalid")
    gpu_ids = (payload["encoder_vae"], *payload["dit"])
    if (
        len(gpu_ids) != 8
        or len(set(gpu_ids)) != 8
        or any(not _valid_gpu_uuid(gpu) for gpu in gpu_ids)
    ):
        raise ValueError("GPU role map must bind one Encoder/VAE and seven unique DiT GPUs")
    return gpu_ids


def _validated_identity(identity: Any) -> Any:
    if identity is None:
        raise ValueError("Attempt identity is required")
    for value in (identity.attempt_id, identity.job_id, identity.worker_id):
        _canonical_uuid(value)
    if identity.worker_epoch <= 0 or identity.lease_fence <= 0:
        raise ValueError("Attempt epoch and fence must be positive")
    return runner_pb2.RunnerAttemptIdentity().FromString(identity.SerializeToString())


def _validated_spec(spec: Any, profiles: frozenset[_Profile]) -> Any:
    if spec is None or not 0 < len(spec.request_content_json) <= _MAX_REQUEST_BYTES:
        raise ValueError("execution specification is incomplete")
    profile = _Profile(
        _canonical_uuid(spec.model_revision_id),
        _canonical_uuid(spec.generation_preset_revision_id),
        _canonical_uuid(spec.execution_profile_revision_id),
        _canonical_uuid(spec.output_spec_id),
    )
    if profile not in profiles:
        raise ValueError("execution specification is not in the certified profile allowlist")
    try:
        request_content = json.loads(spec.request_content_json)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError("request content must be one JSON object") from error
    if not isinstance(request_content, dict):
        raise ValueError("request content must be one JSON object")
    return runner_pb2.RunnerExecutionSpec().FromString(spec.SerializeToString())


def _canonical_uuid(value: Any) -> str:
    if not isinstance(value, str):
        raise ValueError("identity field must be a UUID")
    try:
        parsed = uuid.UUID(value)
    except (ValueError, AttributeError) as error:
        raise ValueError("identity field must be a UUID") from error
    if str(parsed) != value or parsed.int == 0:
        raise ValueError("identity field must be a canonical non-zero UUID")
    return value


def _valid_gpu_uuid(value: Any) -> bool:
    if not isinstance(value, str) or _GPU_PATTERN.fullmatch(value) is None:
        return False
    parsed = uuid.UUID(value.removeprefix("GPU-"))
    return parsed.int != 0 and "GPU-" + str(parsed) == value


def _same_message(left: Any, right: Any) -> bool:
    return (
        left is not None
        and right is not None
        and left.SerializeToString() == right.SerializeToString()
    )


def _readiness_rejected(request: Any, detail: str) -> runner_pb2.ProbeReadinessResponse:
    bounded_detail = detail if _valid_text(detail, 1000) else "readiness request rejected"
    return runner_pb2.ProbeReadinessResponse(
        identity=request.identity,
        check=request.check,
        decision=runner_pb2.RUNNER_COMMAND_DECISION_REJECTED,
        detail=bounded_detail,
    )


def _terminate_process_group(process: subprocess.Popen[bytes], timeout: float) -> None:
    if process.poll() is not None:
        return
    try:
        os.killpg(process.pid, signal.SIGTERM)
    except ProcessLookupError:
        return
    try:
        process.wait(timeout=timeout)
    except subprocess.TimeoutExpired:
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            return
        process.wait(timeout=timeout)


def _timestamp_rfc3339(seconds: int, nanos: int) -> str:
    value = datetime.fromtimestamp(seconds, UTC).strftime("%Y-%m-%dT%H:%M:%S")
    if nanos:
        value += f".{nanos:09d}".rstrip("0")
    return value + "Z"


def _identity_dict(identity: Any) -> dict[str, Any]:
    return {
        "attempt_id": identity.attempt_id,
        "job_id": identity.job_id,
        "worker_id": identity.worker_id,
        "worker_epoch": identity.worker_epoch,
        "lease_fence": identity.lease_fence,
    }


def _identity_from_dict(payload: Any) -> Any:
    expected = {"attempt_id", "job_id", "worker_id", "worker_epoch", "lease_fence"}
    if not isinstance(payload, dict) or set(payload) != expected:
        raise ValueError("runner recovery identity schema is invalid")
    try:
        return runner_pb2.RunnerAttemptIdentity(**payload)
    except (TypeError, ValueError) as error:
        raise ValueError("runner recovery identity is invalid") from error


def _spec_from_dict(payload: Any) -> Any:
    expected = {
        "model_revision_id",
        "generation_preset_revision_id",
        "execution_profile_revision_id",
        "output_spec_id",
        "request_content_base64",
    }
    if not isinstance(payload, dict) or set(payload) != expected:
        raise ValueError("runner recovery execution specification schema is invalid")
    encoded = payload["request_content_base64"]
    if not isinstance(encoded, str):
        raise ValueError("runner recovery request content is invalid")
    try:
        request_content = base64.b64decode(encoded, validate=True)
        return runner_pb2.RunnerExecutionSpec(
            model_revision_id=payload["model_revision_id"],
            generation_preset_revision_id=payload["generation_preset_revision_id"],
            execution_profile_revision_id=payload["execution_profile_revision_id"],
            output_spec_id=payload["output_spec_id"],
            request_content_json=request_content,
        )
    except (TypeError, ValueError) as error:
        raise ValueError("runner recovery execution specification is invalid") from error


def _failure_dict(failure: Any) -> dict[str, Any] | None:
    if failure is None:
        return None
    return {
        "failure_class": failure.failure_class,
        "failure_fingerprint": failure.failure_fingerprint,
        "error_summary": failure.error_summary,
        "backend_stage": failure.backend_stage,
        "gpu_uuids": list(failure.gpu_uuids),
        "inference_backend_revision": failure.inference_backend_revision,
        "retry_recommended": failure.retry_recommended,
        "worker_reusable": failure.worker_reusable,
    }


def _output_dict(output: _Output) -> dict[str, Any]:
    return {
        "kind": output.kind,
        "ordinal": output.ordinal,
        "path": str(output.path),
        "size_bytes": output.size_bytes,
        "sha256": output.sha256.hex(),
        "content_type": output.content_type,
    }


def _outputs_from_dicts(payload: Any, attempt_root: Path) -> tuple[_Output, ...]:
    if not isinstance(payload, list) or not 0 < len(payload) <= _MAX_OUTPUTS:
        raise ValueError("runner successful output receipt count is invalid")
    outputs: list[_Output] = []
    identities: set[tuple[str, int]] = set()
    paths: set[Path] = set()
    for candidate in payload:
        expected = {"kind", "ordinal", "path", "size_bytes", "sha256", "content_type"}
        if not isinstance(candidate, dict) or set(candidate) != expected:
            raise ValueError("runner successful output receipt schema is invalid")
        kind = candidate["kind"]
        ordinal = candidate["ordinal"]
        path_value = candidate["path"]
        size_bytes = candidate["size_bytes"]
        digest_value = candidate["sha256"]
        content_type = candidate["content_type"]
        path = Path(path_value) if isinstance(path_value, str) else Path()
        try:
            digest = bytes.fromhex(digest_value)
        except (TypeError, ValueError):
            digest = b""
        if (
            not isinstance(kind, str)
            or _KIND_PATTERN.fullmatch(kind) is None
            or type(ordinal) is not int
            or ordinal < 0
            or not isinstance(path_value, str)
            or not path.is_absolute()
            or path.parent != attempt_root
            or type(size_bytes) is not int
            or size_bytes <= 0
            or len(digest) != hashlib.sha256().digest_size
            or not isinstance(content_type, str)
            or _CONTENT_TYPE_PATTERN.fullmatch(content_type) is None
        ):
            raise ValueError("runner successful output receipt is invalid")
        identity = (kind, ordinal)
        if identity in identities or path in paths:
            raise ValueError("runner successful output receipt contains a duplicate")
        identities.add(identity)
        paths.add(path)
        outputs.append(_Output(kind, ordinal, path, size_bytes, digest, content_type))
    return tuple(sorted(outputs, key=lambda output: (output.kind, output.ordinal)))


def _failure_from_dict(payload: Any, backend_revision: str) -> Any:
    if payload is None:
        return None
    expected = {
        "failure_class",
        "failure_fingerprint",
        "error_summary",
        "backend_stage",
        "gpu_uuids",
        "inference_backend_revision",
        "retry_recommended",
        "worker_reusable",
    }
    if not isinstance(payload, dict) or set(payload) != expected:
        raise ValueError("runner recovery failure schema is invalid")
    if (
        not _valid_text(payload["failure_class"], 100)
        or not _valid_text(payload["failure_fingerprint"], 200)
        or not _valid_text(payload["error_summary"], 1000)
        or not _valid_text(payload["backend_stage"], 100)
        or not isinstance(payload["gpu_uuids"], list)
        or len(payload["gpu_uuids"]) > 8
        or any(
            not _valid_gpu_uuid(gpu)
            for gpu in payload["gpu_uuids"]
        )
        or len(set(payload["gpu_uuids"])) != len(payload["gpu_uuids"])
        or payload["inference_backend_revision"] != backend_revision
        or type(payload["retry_recommended"]) is not bool
        or type(payload["worker_reusable"]) is not bool
    ):
        raise ValueError("runner recovery failure evidence is invalid")
    return runner_pb2.RunnerFailure(**payload)


def _default_failure(backend_revision: str, fingerprint: str) -> Any:
    return runner_pb2.RunnerFailure(
        failure_class="BACKEND_PROCESS_FAILED",
        failure_fingerprint=fingerprint,
        error_summary="inference backend process failed without a valid bounded failure receipt",
        backend_stage="backend",
        inference_backend_revision=backend_revision,
        retry_recommended=True,
        worker_reusable=False,
    )


def _valid_text(value: Any, maximum: int) -> bool:
    return (
        isinstance(value, str)
        and 0 < len(value) <= maximum
        and value == value.strip()
        and value.isprintable()
    )


def _read_secure_json(path: Path) -> dict[str, Any]:
    return _read_json(
        path,
        forbidden_mode=0o022,
        allowed_uids=frozenset({0, os.geteuid()}),
        unsafe_detail="runner configuration file is unsafe",
    )


def _read_private_json(path: Path) -> dict[str, Any]:
    return _read_json(
        path,
        forbidden_mode=0o077,
        allowed_uids=frozenset({os.geteuid()}),
        unsafe_detail="runner state file is unsafe",
    )


def _read_json(
    path: Path,
    *,
    forbidden_mode: int = 0,
    allowed_uids: frozenset[int] | None = None,
    unsafe_detail: str = "runner JSON document is unsafe",
) -> dict[str, Any]:
    if allowed_uids is None:
        allowed_uids = frozenset({os.geteuid()})
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC)
    with os.fdopen(descriptor, "rb") as source:
        info = os.fstat(source.fileno())
        if (
            not stat.S_ISREG(info.st_mode)
            or info.st_uid not in allowed_uids
            or stat.S_IMODE(info.st_mode) & forbidden_mode
            or info.st_size <= 0
            or info.st_size > _MAX_JSON_BYTES
        ):
            raise ValueError(f"{unsafe_detail}: {path}")
        content = source.read(_MAX_JSON_BYTES + 1)
    if not content or len(content) > _MAX_JSON_BYTES:
        raise ValueError("runner JSON document is empty or oversized")
    payload = json.loads(content, object_pairs_hook=_unique_json_object)
    if not isinstance(payload, dict):
        raise ValueError("runner JSON document must be one object")
    return payload


def _unique_json_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    payload: dict[str, Any] = {}
    for key, value in pairs:
        if key in payload:
            raise ValueError(f"runner JSON document contains duplicate key: {key}")
        payload[key] = value
    return payload


def _write_json_atomic(path: Path, payload: dict[str, Any]) -> None:
    content = json.dumps(payload, separators=(",", ":"), sort_keys=True).encode()
    if not content or len(content) > _MAX_JSON_BYTES:
        raise ValueError("runner state document is oversized")
    temporary = path.with_name(f".{path.name}.{uuid.uuid4()}.tmp")
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        with os.fdopen(descriptor, "wb", closefd=False) as destination:
            destination.write(content)
            destination.flush()
            os.fsync(destination.fileno())
    finally:
        os.close(descriptor)
    os.replace(temporary, path)
    _sync_directory(path.parent)


def _sync_directory(path: Path) -> None:
    directory = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(directory)
    finally:
        os.close(directory)


def _remove_flat_directory(path: Path) -> None:
    info = path.lstat()
    if not stat.S_ISDIR(info.st_mode) or path.is_symlink() or info.st_uid != os.geteuid():
        raise ValueError("runner cleanup directory is unsafe")
    entries = list(path.iterdir())
    if len(entries) > 64:
        raise ValueError("runner cleanup directory is unexpectedly large")
    for entry in entries:
        child = entry.lstat()
        if not stat.S_ISREG(child.st_mode) or entry.is_symlink() or child.st_uid != os.geteuid():
            raise ValueError("runner cleanup entry is unsafe")
        entry.unlink()
    path.rmdir()
    _sync_directory(path.parent)


def _remove_output_directory(path: Path) -> None:
    info = path.lstat()
    if not stat.S_ISDIR(info.st_mode) or path.is_symlink() or info.st_uid != os.geteuid():
        raise ValueError("runner output cleanup directory is unsafe")
    entries = list(path.iterdir())
    if len(entries) > 64:
        raise ValueError("runner output cleanup directory is unexpectedly large")
    for entry in entries:
        child = entry.lstat()
        if child.st_uid != os.geteuid() or not (
            stat.S_ISREG(child.st_mode) or stat.S_ISLNK(child.st_mode)
        ):
            raise ValueError("runner output cleanup entry is unsafe")
    for entry in entries:
        entry.unlink()
    path.rmdir()
    _sync_directory(path.parent)
