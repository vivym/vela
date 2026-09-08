"""Package a completed run: python3 package-evidence.py EVIDENCE_DIRECTORY.

Requires the upstream v6.10.14 base.c and fork.c sources in EVIDENCE_DIRECTORY's
parent kernel directory. Sources are hashed; the index pins downloadable URLs.
"""

import hashlib
import json
from pathlib import Path
import shutil
import subprocess
import sys


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def main():
    repo = Path(__file__).resolve().parents[3]
    scratch = Path(sys.argv[1]).resolve()
    target = repo / "docs/evidence/runtime-execution-continuity-2026-09-09"
    target.mkdir(parents=True, exist_ok=True)
    for name in ("baseline.txt", "docker-version.txt", "image.txt", "build.log",
                 "results.json", "native.stderr", "files.sha256", "no-ptrace.log",
                 "no-ptrace-exit-code.txt"):
        shutil.copyfile(scratch / name, target / name)
    shutil.copyfile(scratch / "image/Dockerfile", target / "Dockerfile")
    for name in ("probe.c", "check.py"):
        source = repo / "hack/experiments/runtime-mm-witness" / name
        require(source.read_bytes() == (scratch / "image" / name).read_bytes(),
                name + " changed since native build")
        shutil.copyfile(source, target / name)
    for source in (repo / "hack/run-runtime-mm-witness-experiment.sh", Path(__file__)):
        shutil.copyfile(source, target / source.name)

    results = json.loads((target / "results.json").read_text())
    expected_cases = ["no-exec", "same-executable-exec", "aba-exec", "ordinary-fork-exec",
                      "shared-vm-exec", "shared-vm-aba", "shared-vm-owner-exit"]
    require([row["case"] for row in results["results"]] == expected_cases, "case coverage differs")
    require(all(row["result"] == "PASS" for row in results["results"]), "experiment failed")
    require(results["fd_delta"] == 0 and not results["retained_mem_is_exec_authority"], "wrong conclusion")
    require((target / "no-ptrace-exit-code.txt").read_text().strip() == "1", "capability control did not fail")
    require("PermissionError:" in (target / "no-ptrace.log").read_text(), "wrong capability failure")
    require(not (target / "native.stderr").read_bytes(), "unexpected native stderr")
    file_hashes = dict(line.split()[::-1] for line in (target / "files.sha256").read_text().splitlines())
    for name in ("probe.c", "check.py"):
        require(file_hashes["/" + name] == digest(target / name), "container source digest differs")

    revision = "47c2f92131c47a37ea0e3d8e1a4e4c82a9b473d4"
    sources = []
    for path, local in (("fs/proc/base.c", "base-v6.10.14.c"), ("kernel/fork.c", "fork-v6.10.14.c")):
        source = scratch.parent / "kernel" / local
        sources.append({"url": "https://raw.githubusercontent.com/gregkh/linux/" + revision + "/" + path,
                        "sha256": digest(source), "bytes": source.stat().st_size})
    baseline = (target / "baseline.txt").read_text().strip()
    require(baseline == subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=repo, text=True).strip(),
            "HEAD changed since experiment")
    report = {
        "schema_version": 1,
        "baseline_commit": baseline,
        "production_source_changed": False,
        "experiment_command": "bash hack/run-runtime-mm-witness-experiment.sh",
        "platform": {"kernel": results["kernel"], "architecture": results["architecture"]},
        "image": (target / "image.txt").read_text().strip(),
        "probe_binaries_sha256": {name: file_hashes[name] for name in ("/probe-a", "/probe-b")},
        "cases": expected_cases,
        "case_count": len(expected_cases),
        "fd_delta": 0,
        "missing_ptrace_control_exit_code": 1,
        "kernel_source_reference": {"upstream_stable_commit": revision, "tag": "v6.10.14", "files": sources,
                                    "linuxkit_build_identity_verified": False},
        "conclusion": "Retained proc mem readability does not prove the original process has not executed another image. CLONE_VM can retain old mm across same-image exec, ABA and original-process exit.",
        "boundary": "Standalone Linux arm64 kernel experiment with protected non-root C targets. No Vela RuntimeCaller, actual CLI, CRI, Fleet, grant, complete Job, amd64-native or GPU campaign. No production startup behavior changed.",
        "production_gates": "0/9",
        "artifacts": [{"path": str(path.relative_to(repo)), "bytes": path.stat().st_size, "sha256": digest(path)}
                      for path in sorted(target.iterdir()) if path.is_file()],
    }
    output = repo / "docs/runtime-execution-continuity-evidence-2026-09-09.json"
    output.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps({"cases": len(expected_cases), "artifacts": len(report["artifacts"]), "image": report["image"]}))


if __name__ == "__main__":
    main()
