#!/usr/bin/env python3
"""Render application-owned monitoring for release contract kubernetes-v2.

The existing cluster observability deployment supplies Prometheus, Grafana,
Collector, Loki and Tempo. An application release binds and reconciles its SLO
rules, dashboard/contract ConfigMaps and Control PodMonitor. Source content is
shared with deploy/observability; no copied rule or dashboard definitions live
in the repository. This command does not access or modify a Kubernetes cluster.
"""
import argparse
import json
from pathlib import Path
import shutil
import subprocess
import tempfile


def render(repository, kubectl="kubectl"):
    source = repository / "deploy" / "observability"
    definition = {
        "apiVersion": "kustomize.config.k8s.io/v1beta1", "kind": "Kustomization",
        "resources": ["pod-monitor.yaml", "prometheus-rule.yaml"],
        "configMapGenerator": [
            {"name": "vela-slo-alert-rules", "namespace": "monitoring", "files": ["rules.yaml", "rule-tests.yaml"]},
            {"name": "vela-slo-dashboard", "namespace": "monitoring", "files": ["dashboard.json"]},
            {"name": "vela-slo-contract", "namespace": "monitoring", "files": ["observability-contract.json"]},
        ],
        "generatorOptions": {"disableNameSuffixHash": False, "labels": {
            "app.kubernetes.io/part-of": "vela", "app.kubernetes.io/component": "observability"}},
    }
    files = definition["resources"] + [name for generator in definition["configMapGenerator"] for name in generator["files"]]
    with tempfile.TemporaryDirectory(prefix="vela-release-observability-") as temporary:
        root = Path(temporary)
        for name in files:
            shutil.copyfile(source / name, root / name)
        (root / "kustomization.yaml").write_text(json.dumps(definition))
        return subprocess.check_output([kubectl, "kustomize", str(root)], timeout=30)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repository", type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument("--kubectl", default="kubectl")
    parser.add_argument("--output", type=Path, required=True, help="New output file; existing files are never overwritten")
    args = parser.parse_args()
    content = render(args.repository, args.kubectl)
    with args.output.open("xb") as output:
        output.write(content)


if __name__ == "__main__":
    main()
