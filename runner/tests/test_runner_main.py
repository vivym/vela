from __future__ import annotations

import json
from pathlib import Path

import pytest

from vela_h3_runner.main import load_config


def test_load_config_requires_complete_explicit_runtime_boundary(tmp_path: Path) -> None:
    environment = valid_environment(tmp_path)
    environment.pop("VELA_RUNNER_GPU_ROLES_FILE")
    with pytest.raises(ValueError, match="VELA_RUNNER_GPU_ROLES_FILE"):
        load_config(environment)


def test_load_config_parses_fixed_backend_command_without_shell(tmp_path: Path) -> None:
    environment = valid_environment(tmp_path)
    config = load_config(environment)
    assert config.backend_command == ("/opt/vela/bin/h3-backend", "--fixed", "value")
    assert config.stop_timeout_seconds == 7.5
    assert config.state_root == tmp_path / "scratch" / "runner-state"
    assert config.max_output_bytes == 1_073_741_824


def test_load_config_rejects_relative_or_malformed_backend_arguments(tmp_path: Path) -> None:
    environment = valid_environment(tmp_path)
    environment["VELA_RUNNER_BACKEND_COMMAND"] = "h3-backend"
    with pytest.raises(ValueError, match="VELA_RUNNER_BACKEND_COMMAND"):
        load_config(environment)
    environment = valid_environment(tmp_path)
    environment["VELA_RUNNER_BACKEND_ARGS_JSON"] = json.dumps([" ok"])
    with pytest.raises(ValueError, match="VELA_RUNNER_BACKEND_ARGS_JSON"):
        load_config(environment)


def test_load_config_rejects_nonpositive_output_limit(tmp_path: Path) -> None:
    environment = valid_environment(tmp_path)
    environment["VELA_RUNNER_MAX_OUTPUT_BYTES"] = "0"
    with pytest.raises(ValueError, match="VELA_RUNNER_MAX_OUTPUT_BYTES"):
        load_config(environment)


def valid_environment(tmp_path: Path) -> dict[str, str]:
    scratch = tmp_path / "scratch"
    return {
        "VELA_RUNNER_SOCKET": str(tmp_path / "run" / "runner.sock"),
        "VELA_RUNNER_SCRATCH_ROOT": str(scratch),
        "VELA_RUNNER_STATE_ROOT": str(scratch / "runner-state"),
        "VELA_RUNNER_OUTPUT_ROOT": str(scratch / "outputs"),
        "VELA_RUNNER_BACKEND_REVISION": "sglang@sha256:test",
        "VELA_RUNNER_BACKEND_COMMAND": "/opt/vela/bin/h3-backend",
        "VELA_RUNNER_BACKEND_ARGS_JSON": json.dumps(["--fixed", "value"]),
        "VELA_RUNNER_PROFILES_FILE": str(tmp_path / "profiles.json"),
        "VELA_RUNNER_GPU_ROLES_FILE": str(tmp_path / "gpu-roles.json"),
        "VELA_RUNNER_STOP_TIMEOUT": "7.5",
        "VELA_RUNNER_MAX_OUTPUT_BYTES": "1073741824",
    }
