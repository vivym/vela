"""Package the completed prototype, CLI, and EXITKILL-control campaign.

Usage: python3 package-evidence.py /absolute/campaign-directory
Expected subdirectories: final, cli-final, regression, kernel.
"""
import hashlib
import json
from pathlib import Path
import re
import shutil
import subprocess
import sys


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def main():
    repo = Path(__file__).resolve().parents[3]
    scratch = Path(sys.argv[1]).resolve()
    output = repo / "docs/evidence/runtime-exec-observer-2026-09-09"
    for directory in (output, output / "prototype", output / "cli", output / "regression"):
        directory.mkdir(parents=True, exist_ok=True)
    sets = {
        "prototype": ("final", ["baseline.txt", "docker-version.txt", "image.txt", "build.log", "results.json", "native.stderr", "files.sha256"]),
        "cli": ("cli-final", ["source.txt", "source.patch", "source-patch.sha256", "docker-version.txt", "image.txt", "native.log", "binaries.sha256", "build-nodeagent.log", "build-runtime-command.log", "build-vela-model-runtime.log", "build-exec-observer.log"]),
        "regression": ("regression", ["native.log", "exit-code.txt", "image.txt", "binaries.sha256", "build-enabled.log", "build-disabled.log", "build-image.log", "Dockerfile"]),
    }
    for label, (source, files) in sets.items():
        for name in files:
            shutil.copyfile(scratch / source / name, output / label / name)
    for name in ("observer.c", "probe.c", "check.py", "go-probe.go.txt"):
        built_name = name.removesuffix(".txt")
        current = repo / "hack/experiments/runtime-exec-observer" / name
        built = scratch / "final/image" / built_name
        require(current.read_bytes() == built.read_bytes(), name + " changed after build")
        shutil.copyfile(current, output / "prototype" / name)
    shutil.copyfile(scratch / "final/image/Dockerfile", output / "prototype/Dockerfile")
    shutil.copyfile(repo / "hack/run-runtime-exec-observer-experiment.sh", output / "prototype/run-experiment.sh")
    shutil.copyfile(repo / "hack/run-remote-runtime-cli-native.sh", output / "cli/run-cli.sh")
    shutil.copyfile(repo / "hack/experiments/runtime-exec-observer/check-cli-exitkill.sh", output / "regression/check-cli-exitkill.sh")
    shutil.copyfile(scratch / "cli-final-runner.log", output / "cli/runner.log")
    for name in ("linux-lint.log", "linux-vet.log", "repository-test.log", "regression-runner.log"):
        shutil.copyfile(scratch / name, output / name)
    shutil.copyfile(__file__, output / "package-evidence.py")
    patch = output / "cli/source.patch"
    (output / "cli/source-patch.sha256").write_text(sha(patch) + "  source.patch\n")
    subprocess.run(["git", "apply", "--reverse", "--check", str(patch)], cwd=repo, check=True)

    prototype = json.loads((output / "prototype/results.json").read_text())
    require(len(prototype["results"]) == 10 and prototype["fd_delta"] == 0, "wrong prototype coverage")
    require(all(row["result"] == "PASS" for row in prototype["results"]), "prototype failure")
    require(not (output / "prototype/native.stderr").read_bytes(), "prototype emitted stderr")
    cli = (output / "cli/native.log").read_text()
    cli_tests = re.findall(r"^--- PASS: (\S+) ", cli, re.M)
    cli_cases = re.findall(r"--- PASS: TestJournalServerExecObservedRemoteCLI/(\S+) ", cli)
    require(len(cli_tests) == 12 and len(cli_cases) == 4 and cli.endswith("PASS\n"), "wrong CLI coverage")
    require(not any(marker in cli for marker in ("--- SKIP:", "--- FAIL:", "WARNING: DATA RACE")), "CLI failure/skip/race")
    red = (output / "regression/native.log").read_text()
    require((output / "regression/exit-code.txt").read_text().strip() == "1", "control did not fail")
    require(not any(marker in red for marker in ("--- SKIP:", "WARNING: DATA RACE")), "control skipped or raced")
    require("--- PASS: TestJournalServerExecObservedRemoteCLI/permit " in red, "missing healthy control")
    for case in ("observer-lost-before-permit", "observer-lost-after-permit"):
        require("--- FAIL: TestJournalServerExecObservedRemoteCLI/" + case + " " in red, "missing expected failure")
    require(red.count("original namespace owner did not exit") == 2, "wrong control failure cause")
    for label, source in (("cli", "cli-final"), ("regression", "regression")):
        for line in (output / label / "binaries.sha256").read_text().splitlines():
            digest, path = line.split(maxsplit=1)
            require(sha(Path(path)) == digest, source + " binary digest changed")
    require((scratch / "regression/observer-enabled").read_bytes() == (scratch / "cli-final/image/rootfs/exec-observer").read_bytes(), "control baseline differs")
    prototype_hashes = dict(line.split()[::-1] for line in (output / "prototype/files.sha256").read_text().splitlines())
    require(prototype_hashes["/exec-observer"] == sha(scratch / "cli-final/image/rootfs/exec-observer"), "CLI/prototype observers differ")
    for name in ("observer.c", "probe.c", "check.py", "go-probe.go.txt"):
        require(prototype_hashes["/" + name.removesuffix(".txt")] == sha(output / "prototype" / name), "prototype source digest differs")

    tracked = subprocess.check_output(["git", "ls-files", "-z", "--cached", "--others", "--exclude-standard"], cwd=repo).decode().split("\0")
    sources = [{"path": name, "sha256": sha(repo / name)} for name in sorted(set(tracked)) if name and (
        Path(name).suffix in (".go", ".sql", ".proto") or name in ("go.mod", "go.sum", "buf.yaml", "buf.gen.yaml") or
        name.startswith("hack/experiments/runtime-exec-observer/") or name in ("hack/run-remote-runtime-cli-native.sh", "hack/run-runtime-exec-observer-experiment.sh"))]
    aggregate = hashlib.sha256(b"".join((item["path"] + "\0" + item["sha256"] + "\n").encode() for item in sources)).hexdigest()
    (output / "source-files.json").write_text(json.dumps({"source_digest": aggregate, "file_count": len(sources), "files": sources}, indent=2) + "\n")
    baseline = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=repo, text=True).strip()
    require((output / "prototype/baseline.txt").read_text().strip() == baseline, "HEAD changed")
    require((output / "cli/source.txt").read_text().splitlines()[0] == baseline, "CLI baseline differs")
    kernel_revision = "47c2f92131c47a37ea0e3d8e1a4e4c82a9b473d4"
    kernel_files = [("kernel/fork.c", scratch.parent / "execution-continuity/kernel/fork-v6.10.14.c"), ("kernel/ptrace.c", scratch / "kernel/ptrace.c")]
    report = {
        "schema_version": 1, "baseline_commit": baseline,
        "source_file_count": len(sources), "source_digest": aggregate,
        "source_digest_method": "SHA256 sorted path + NUL + hex file SHA256 + newline; Go/SQL/proto/module files plus prototype sources and both runners",
        "platform": {"kernel": prototype["kernel"], "architecture": prototype["architecture"]},
        "prototype_cases": [row["case"] for row in prototype["results"]], "prototype_fd_delta": 0,
        "cli_main_tests": cli_tests, "observed_cli_cases": cli_cases,
        "go_race_instrumented_cli": True, "c_race_detector": False, "cli_skips": 0, "cli_races": 0,
        "images": {name: (output / name / "image.txt").read_text().strip() for name in sets},
        "regression": {"change": "PTRACE_O_EXITKILL disabled only", "test_exit_code": 1,
                       "expected_failures": ["observer-lost-before-permit", "observer-lost-after-permit"], "healthy_control": "permit"},
        "kernel_source": {"upstream_stable_commit": kernel_revision, "linuxkit_build_identity_verified": False,
                          "files": [{"url": "https://raw.githubusercontent.com/gregkh/linux/" + kernel_revision + "/" + path, "sha256": sha(local)} for path, local in kernel_files]},
        "checks": ["10 standalone kernel scenarios", "12 native CLI main tests including 4 observer cases", "2 expected actual CLI EXITKILL regression failures plus healthy permit control", "go test ./...", "GOOS=linux GOARCH=arm64 go vet ./internal/nodeagent", "GOOS=linux GOARCH=arm64 golangci-lint v2.13.1 run ./internal/nodeagent"],
        "production_gates": "0/9", "production_startup_changed": False,
        "boundary": "Creation-time syscall observer prototype and actual non-root Runtime CLI compatibility/failure fixture. Original Runtime and backend exit observations use retained pidfds. Pod/CRI creation relationship and Permit remain fixtures. No production observer custody/IPC, hanging-observer recovery, real Fleet/TLS/PostgreSQL same-launch integration, once-only grant, write route, complete Job, universal descendant/device containment, sustained capacity, native amd64 or GPU evidence.",
        "artifacts": [{"path": str(path.relative_to(repo)), "bytes": path.stat().st_size, "sha256": sha(path)} for path in sorted(output.rglob("*")) if path.is_file()],
    }
    (repo / "docs/runtime-exec-observer-evidence-2026-09-09.json").write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps({"source_files": len(sources), "source_digest": aggregate, "artifacts": len(report["artifacts"]), "prototype_cases": 10, "cli_main_tests": 12, "cli_cases": 4, "expected_regression_failures": 2}))


if __name__ == "__main__":
    main()
