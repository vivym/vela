from __future__ import annotations

import json
import math
import os
import signal
import sys
import threading
from collections.abc import Mapping
from pathlib import Path

from vela_h3_runner.runtime import RuntimeConfig
from vela_h3_runner.server import RunnerServer

_REQUIRED = (
    "VELA_RUNNER_SOCKET",
    "VELA_RUNNER_SCRATCH_ROOT",
    "VELA_RUNNER_STATE_ROOT",
    "VELA_RUNNER_OUTPUT_ROOT",
    "VELA_RUNNER_BACKEND_REVISION",
    "VELA_RUNNER_BACKEND_COMMAND",
    "VELA_RUNNER_BACKEND_ARGS_JSON",
    "VELA_RUNNER_PROFILES_FILE",
    "VELA_RUNNER_GPU_ROLES_FILE",
    "VELA_RUNNER_STOP_TIMEOUT",
    "VELA_RUNNER_MAX_OUTPUT_BYTES",
)


def load_config(environment: Mapping[str, str] | None = None) -> RuntimeConfig:
    values = os.environ if environment is None else environment
    for name in _REQUIRED:
        value = values.get(name, "")
        if not value or value != value.strip() or "\x00" in value:
            raise ValueError(f"{name} is required without surrounding whitespace")
    path_names = (
        "VELA_RUNNER_SOCKET",
        "VELA_RUNNER_SCRATCH_ROOT",
        "VELA_RUNNER_STATE_ROOT",
        "VELA_RUNNER_OUTPUT_ROOT",
        "VELA_RUNNER_BACKEND_COMMAND",
        "VELA_RUNNER_PROFILES_FILE",
        "VELA_RUNNER_GPU_ROLES_FILE",
    )
    paths: dict[str, Path] = {}
    for name in path_names:
        raw = values[name]
        path = Path(raw)
        if not path.is_absolute() or os.path.normpath(raw) != raw:
            raise ValueError(f"{name} must be a canonical absolute path")
        paths[name] = path
    try:
        arguments = json.loads(values["VELA_RUNNER_BACKEND_ARGS_JSON"])
    except json.JSONDecodeError as error:
        raise ValueError("VELA_RUNNER_BACKEND_ARGS_JSON must be a JSON string array") from error
    if (
        not isinstance(arguments, list)
        or len(arguments) > 128
        or any(
            not isinstance(argument, str)
            or not argument
            or argument != argument.strip()
            or "\x00" in argument
            for argument in arguments
        )
    ):
        raise ValueError("VELA_RUNNER_BACKEND_ARGS_JSON must contain bounded fixed arguments")
    try:
        stop_timeout = float(values["VELA_RUNNER_STOP_TIMEOUT"])
    except ValueError as error:
        raise ValueError("VELA_RUNNER_STOP_TIMEOUT must be a bounded number") from error
    if not math.isfinite(stop_timeout) or not 0.1 <= stop_timeout <= 30:
        raise ValueError("VELA_RUNNER_STOP_TIMEOUT must be in [0.1, 30]")
    max_output_bytes_raw = values["VELA_RUNNER_MAX_OUTPUT_BYTES"]
    if not max_output_bytes_raw.isascii() or not max_output_bytes_raw.isdigit():
        raise ValueError("VELA_RUNNER_MAX_OUTPUT_BYTES must be a positive integer")
    max_output_bytes = int(max_output_bytes_raw)
    if not 0 < max_output_bytes <= (1 << 63) - 1:
        raise ValueError("VELA_RUNNER_MAX_OUTPUT_BYTES must be a positive int64")
    backend_revision = values["VELA_RUNNER_BACKEND_REVISION"]
    if len(backend_revision) > 200 or not backend_revision.isprintable():
        raise ValueError("VELA_RUNNER_BACKEND_REVISION is invalid")
    return RuntimeConfig(
        socket_path=paths["VELA_RUNNER_SOCKET"],
        scratch_root=paths["VELA_RUNNER_SCRATCH_ROOT"],
        state_root=paths["VELA_RUNNER_STATE_ROOT"],
        output_root=paths["VELA_RUNNER_OUTPUT_ROOT"],
        backend_revision=backend_revision,
        backend_command=(str(paths["VELA_RUNNER_BACKEND_COMMAND"]), *arguments),
        profiles_file=paths["VELA_RUNNER_PROFILES_FILE"],
        gpu_roles_file=paths["VELA_RUNNER_GPU_ROLES_FILE"],
        stop_timeout_seconds=stop_timeout,
        max_output_bytes=max_output_bytes,
    )


def run() -> None:
    server = RunnerServer(load_config())
    stopping = threading.Event()

    def request_stop(_signum: int, _frame: object) -> None:
        stopping.set()

    signal.signal(signal.SIGTERM, request_stop)
    signal.signal(signal.SIGINT, request_stop)
    server.start()
    try:
        stopping.wait()
    finally:
        server.stop()


def main() -> None:
    try:
        run()
    except (OSError, RuntimeError, ValueError) as error:
        print(f"vela-h3-runner stopped: {error}", file=sys.stderr)
        raise SystemExit(1) from error


if __name__ == "__main__":
    main()
