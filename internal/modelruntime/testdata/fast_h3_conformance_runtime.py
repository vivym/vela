from __future__ import annotations

import os
from pathlib import Path
from threading import Event

from fast_h3.vela.runtime import (
    ProbeResult,
    RuntimeInitialization,
    RuntimeOutput,
    StageExecution,
)


def _record(event: str) -> None:
    path = Path(os.environ["FAST_H3_CONFORMANCE_EVENT_LOG"])
    with path.open("a", encoding="utf-8") as handle:
        handle.write(event + "\n")


class ConformanceRuntime:
    def __init__(self, component: str) -> None:
        self.component = component
        self.initialized = False

    def initialize(self, request: RuntimeInitialization) -> None:
        if request.component != self.component or self.initialized:
            raise RuntimeError("conformance runtime initialization is invalid")
        self.initialized = True
        _record(f"initialize:{self.component}")

    def probe(self, check: str) -> ProbeResult:
        return ProbeResult(
            ready=self.initialized,
            evidence=f"fast-h3-conformance:{self.component}:{check}".encode(),
            detail="conformance runtime ready",
        )

    def prepare(self, execution: StageExecution) -> StageExecution:
        if not self.initialized:
            raise RuntimeError("conformance runtime is not initialized")
        _record(f"prepare:{self.component}")
        return execution

    def execute(
        self,
        prepared: StageExecution,
        cancellation: Event,
    ) -> RuntimeOutput:
        if cancellation.is_set():
            raise RuntimeError("conformance runtime was canceled before execution")
        prepared.staging_path.parent.mkdir(mode=0o700, parents=True, exist_ok=False)
        prepared.staging_path.write_bytes((self.component + "\n").encode())
        _record(f"execute:{self.component}")
        return RuntimeOutput(content_type="application/x-fast-h3-conformance")

    def cancel(self, _prepared: object, _reason: str) -> None:
        _record(f"cancel:{self.component}")

    def shutdown(self) -> None:
        _record(f"shutdown:{self.component}")


def create_runtime(component: str) -> ConformanceRuntime:
    return ConformanceRuntime(component)
