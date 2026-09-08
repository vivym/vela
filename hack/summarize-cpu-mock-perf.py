#!/usr/bin/env python3
"""Summarize same-source, same-shape production-loop CPU campaign logs."""

import argparse
import hashlib
import json
import math
from pathlib import Path
import statistics


def read_run(path):
    raw = path.read_bytes()
    lines = raw.decode().splitlines()
    marker = "CPU_MOCK_LOAD_RECEIPT "
    receipts = [json.loads(line.split(marker, 1)[1]) for line in lines if marker in line]
    if len(receipts) != 1 or not any(
        line.startswith("--- PASS: TestCPUMockProductionLoopJobCampaign") for line in lines
    ) or any(line.startswith(("FAIL", "--- FAIL:")) for line in lines):
        raise ValueError(f"{path}: requires one completed successful production-loop campaign")
    receipt = receipts[0]
    if not receipt.get("production_loop") or receipt.get("production_gate"):
        raise ValueError(f"{path}: not local production-loop CPU evidence")
    values = {
        "wave_seconds": receipt["elapsed_seconds"],
        "mean_job_latency_seconds": receipt["mean_latency_seconds"],
        "mean_queue_seconds": receipt["mean_queue_seconds"],
        "wave_jobs_per_second": receipt["jobs_per_second"],
    }
    if any(not isinstance(v, (int, float)) or not math.isfinite(v) or v < 0 for v in values.values()):
        raise ValueError(f"{path}: invalid numeric metrics")
    if values["wave_seconds"] <= 0 or not math.isclose(
        values["wave_jobs_per_second"], receipt["jobs"] / values["wave_seconds"], rel_tol=1e-9
    ):
        raise ValueError(f"{path}: throughput and elapsed time disagree")
    return {"path": str(path.resolve()), "sha256": hashlib.sha256(raw).hexdigest(),
            "metrics": values, "receipt": receipt}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--label", required=True)
    parser.add_argument("--expected-source", required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("logs", type=Path, nargs="+")
    args = parser.parse_args()
    if len({path.resolve() for path in args.logs}) != len(args.logs):
        parser.error("duplicate log paths are not independent runs")
    try:
        runs = [read_run(path) for path in args.logs]
        if len({run["sha256"] for run in runs}) != len(runs):
            raise ValueError("duplicate log contents are not independent runs")
        shape_keys = ("jobs", "waves", "concurrent_arrivals_per_wave", "persistent_workers",
                      "project_running_limit", "go_version", "ffprobe_version", "runtime_binary_sha256")
        first = runs[0]["receipt"]
        for run in runs:
            receipt = run["receipt"]
            if receipt["source_tree_sha256"] != args.expected_source or any(
                receipt[key] != first[key] for key in shape_keys
            ):
                raise ValueError("mixed source, workload, runtime binaries or toolchain")
        metrics = {"median_" + key: statistics.median(run["metrics"][key] for run in runs)
                   for key in runs[0]["metrics"]}
        result = {"schema_version": 1, "label": args.label, "source_tree_sha256": args.expected_source,
                  "run_mode": "oneshot", "run_count": len(runs), "aggregate": "median",
                  "workload": {key: first[key] for key in shape_keys}, "metrics": metrics,
                  "runs": runs, "production_gate": False,
                  "limitations": ["medians of run metrics, not pooled Job latency percentiles",
                                  "logs do not prove exclusive host resources, randomized order or absence of profiling",
                                  "finite drained CPU mock waves, not sustained production throughput"]}
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(result, indent=2, allow_nan=False) + "\n")
        print("PERF_METRICS_START")
        print(json.dumps(metrics, allow_nan=False))
        print("PERF_METRICS_END")
    except (OSError, ValueError, KeyError, TypeError) as error:
        parser.error(str(error))


if __name__ == "__main__":
    main()
