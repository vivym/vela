from __future__ import annotations

import os
import stat
import threading
from concurrent import futures
from pathlib import Path

import grpc

from vela.v1 import runner_pb2_grpc
from vela_h3_runner.runtime import RunnerRuntime, RuntimeConfig


class RunnerServer:
    def __init__(self, config: RuntimeConfig) -> None:
        self._config = config
        self._runtime = RunnerRuntime(config)
        self._server = grpc.server(
            futures.ThreadPoolExecutor(max_workers=8, thread_name_prefix="vela-runner"),
            options=(
                ("grpc.max_receive_message_length", 1 << 20),
                ("grpc.max_send_message_length", 1 << 20),
            ),
        )
        runner_pb2_grpc.add_RunnerServiceServicer_to_server(self._runtime, self._server)
        self._lock = threading.Lock()
        self._started = False

    def start(self) -> None:
        with self._lock:
            if self._started:
                return
            _prepare_socket_path(self._config.socket_path)
            address = f"unix://{self._config.socket_path}"
            if self._server.add_insecure_port(address) == 0:
                raise RuntimeError("runner gRPC server could not bind its Unix socket")
            self._server.start()
            try:
                os.chmod(self._config.socket_path, 0o600, follow_symlinks=False)
                _validate_socket(self._config.socket_path)
            except BaseException:
                self._server.stop(0).wait(timeout=5)
                raise
            self._started = True

    def wait(self) -> None:
        self._server.wait_for_termination()

    def stop(self, grace_seconds: float = 5) -> None:
        with self._lock:
            if not self._started:
                self._runtime.close()
                return
            stopped = self._server.stop(grace_seconds)
            if not stopped.wait(timeout=grace_seconds + 5):
                self._server.stop(0).wait(timeout=5)
            self._runtime.close()
            _remove_socket(self._config.socket_path)
            self._started = False


def _prepare_socket_path(path: Path) -> None:
    if not path.is_absolute() or len(os.fsencode(path)) > 100:
        raise ValueError("runner socket path must be a bounded absolute path")
    parent = path.parent
    if not parent.exists():
        parent.mkdir(mode=0o700, parents=False)
    _validate_private_directory(parent)
    if path.exists() or path.is_symlink():
        _validate_socket(path)
        path.unlink()


def _validate_private_directory(path: Path) -> None:
    info = path.lstat()
    if (
        not stat.S_ISDIR(info.st_mode)
        or path.is_symlink()
        or info.st_uid != os.geteuid()
        or stat.S_IMODE(info.st_mode) != 0o700
    ):
        raise ValueError("runner socket directory is unsafe")


def _validate_socket(path: Path) -> None:
    info = path.lstat()
    if (
        not stat.S_ISSOCK(info.st_mode)
        or path.is_symlink()
        or info.st_uid != os.geteuid()
        or stat.S_IMODE(info.st_mode) != 0o600
    ):
        raise ValueError("runner socket owner, type, or mode is unsafe")


def _remove_socket(path: Path) -> None:
    try:
        _validate_socket(path)
    except FileNotFoundError:
        return
    path.unlink()
