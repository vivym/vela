from __future__ import annotations

import stat
import tempfile
from dataclasses import replace
from pathlib import Path

import grpc

from vela.v1 import runner_pb2, runner_pb2_grpc
from vela_h3_runner.server import RunnerServer

from .test_runner_service import (
    attempt_identity,
    execution_spec,
    runtime_config,
    successful_backend,
)


def test_runner_serves_contract_on_owner_only_unix_socket(tmp_path: Path) -> None:
    with tempfile.TemporaryDirectory(prefix="vela-runner-", dir="/tmp") as run_directory:
        config = replace(
            runtime_config(tmp_path, successful_backend(tmp_path)),
            socket_path=Path(run_directory) / "runner.sock",
        )
        server = RunnerServer(config)
        server.start()
        try:
            socket_info = config.socket_path.lstat()
            assert stat.S_ISSOCK(socket_info.st_mode)
            assert stat.S_IMODE(socket_info.st_mode) == 0o600
            channel = grpc.insecure_channel(f"unix://{config.socket_path}")
            grpc.channel_ready_future(channel).result(timeout=2)
            stub = runner_pb2_grpc.RunnerServiceStub(channel)
            response = stub.Prepare(
                runner_pb2.PrepareRequest(
                    identity=attempt_identity(),
                    execution_spec=execution_spec(),
                ),
                timeout=2,
            )
            assert response.decision == runner_pb2.RUNNER_COMMAND_DECISION_ACCEPTED
            channel.close()
        finally:
            server.stop()
        assert not config.socket_path.exists()
