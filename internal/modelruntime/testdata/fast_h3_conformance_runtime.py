from __future__ import annotations

import hashlib
import os
from pathlib import Path
from threading import Event

import torch

from fast_h3.vela.artifact_codec import (
    DecodedMediaArtifact,
    DecodedMediaShape,
    DiTArtifact,
    EncoderArtifact,
    read_dit_artifact,
    read_encoder_artifact,
)
from fast_h3.vela.h3_runtime import H3ComponentRuntime, H3StageParameters
from fast_h3.vela.runtime import ProbeResult, RuntimeInitialization, StageExecution


def _record(event: str) -> None:
    path = Path(os.environ["FAST_H3_CONFORMANCE_EVENT_LOG"])
    with path.open("a", encoding="utf-8") as handle:
        handle.write(event + "\n")


class ConformanceBackend:
    """Deterministic CPU math behind the real H3 component contract."""

    def __init__(self) -> None:
        self.component: str | None = None
        self.initialized = False

    def initialize(self, component: str, request: RuntimeInitialization) -> None:
        if request.component != component or self.initialized:
            raise RuntimeError("conformance backend initialization is invalid")
        self.component = component
        self.initialized = True
        _record(f"initialize:{component}")

    def probe(self, check: str) -> ProbeResult:
        return ProbeResult(
            ready=self.initialized,
            evidence=f"fast-h3-conformance:{self.component}:{check}".encode(),
            detail="conformance backend ready",
        )

    def prepare(
        self,
        component: str,
        execution: StageExecution,
        parameters: H3StageParameters,
    ) -> tuple[StageExecution, H3StageParameters, object | None]:
        if not self.initialized or component != self.component:
            raise RuntimeError("conformance backend is not initialized")
        canonical = parameters.canonical_request
        if canonical["schema"] != "minimax_h3.request/v1" or canonical["seed"] != 17:
            raise RuntimeError("conformance parameters are not the frozen H3 request")
        source: object | None = None
        if component == "ENCODER":
            for material in execution.spec.root_inputs:
                payload = material.local_path.read_bytes()
                if (
                    len(payload) != material.size_bytes
                    or hashlib.sha256(payload).hexdigest() != material.sha256_hex
                    or material.uri
                    != canonical["conditions"][material.condition_index]["uri"]
                ):
                    raise RuntimeError("Encoder root input material is not exact")
        elif component == "DIT":
            stage_input = execution.spec.inputs[0]
            source = read_encoder_artifact(
                stage_input.local_path, max_bytes=stage_input.size_bytes
            )
        elif component == "VAE_DECODER":
            stage_input = execution.spec.inputs[0]
            source = read_dit_artifact(
                stage_input.local_path, max_bytes=stage_input.size_bytes
            )
        _record(f"prepare:{component}")
        return execution, parameters, source

    def execute(
        self,
        component: str,
        prepared: tuple[StageExecution, H3StageParameters, object | None],
        cancellation: Event,
    ) -> EncoderArtifact | DiTArtifact | DecodedMediaArtifact:
        if cancellation.is_set():
            raise RuntimeError("conformance backend was canceled before execution")
        _execution, parameters, source = prepared
        seed = int(parameters.canonical_request["seed"])
        if component == "ENCODER":
            artifact: EncoderArtifact | DiTArtifact | DecodedMediaArtifact = (
                EncoderArtifact(
                    request_state={"seed": seed, "stage": "encoder"},
                    conditioning={"hidden": torch.tensor([seed], dtype=torch.int64)},
                )
            )
        elif component == "DIT":
            if not isinstance(source, EncoderArtifact):
                raise RuntimeError("DiT did not receive an Encoder artifact")
            artifact = DiTArtifact(
                request_state={**source.request_state, "stage": "dit"},
                video_latents=source.conditioning["hidden"].to(torch.float32).reshape(
                    1, 1, 1, 1, 1
                ),
                audio_latents=torch.tensor([[[float(seed)]]], dtype=torch.float32),
            )
        else:
            if not isinstance(source, DiTArtifact):
                raise RuntimeError("VAE did not receive a DiT artifact")
            value = float(source.video_latents.reshape(-1)[0])
            frames = torch.full((1, 3, 2, 2, 2), value, dtype=torch.float32)
            audio = torch.full((1, 1, 16), value, dtype=torch.float32)
            artifact = DecodedMediaArtifact(
                rgb_frames=frames,
                audio_waveform=audio,
                sample_rate=32_000,
                fps=24.0,
                shape=DecodedMediaShape(
                    batch_size=1,
                    rgb_channels=3,
                    frame_count=2,
                    height=2,
                    width=2,
                    audio_channels=1,
                    audio_samples=16,
                ),
            )
        _record(f"execute:{component}")
        return artifact

    def cancel(self, _prepared: object, _reason: str) -> None:
        _record(f"cancel:{self.component}")

    def shutdown(self) -> None:
        _record(f"shutdown:{self.component}")


def create_runtime(component: str) -> H3ComponentRuntime:
    return H3ComponentRuntime(component, backend=ConformanceBackend())
