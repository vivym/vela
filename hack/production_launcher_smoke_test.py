"""Regression checks for the production launcher smoke driver contract."""

import hashlib
import importlib.util
import json
import pathlib
import types
import unittest


spec = importlib.util.spec_from_file_location(
    "production_launcher_smoke", pathlib.Path(__file__).with_name("run-production-launcher-smoke.py")
)
smoke = importlib.util.module_from_spec(spec)
spec.loader.exec_module(smoke)


class ProductionLauncherSmokeContractTest(unittest.TestCase):
    def setUp(self):
        self.args = types.SimpleNamespace(
            runtime_image="registry.example/runtime@sha256:" + "a" * 64,
            worker_image="registry.example/worker@sha256:" + "b" * 64,
            uid=65532,
            gid=65532,
        )

    def test_request_and_reply_use_launcher_protocol_v2(self):
        request = smoke.request_for(self.args)
        self.assertEqual(request["version"], 2)
        pod = request["expected_pod"]
        self.assertEqual(pod["spec"]["containers"][0]["args"][1:], ["serve-remote", "--bootstrap-file", "/tmp/runtime-bootstrap.json"])
        pod_wire = json.dumps(pod, separators=(",", ":")).encode()
        self.assertEqual(request["expected_pod_digest"], list(hashlib.sha256(pod_wire).digest()))

    def test_runtime_command_stays_alive_while_satisfying_bootstrap_shape(self):
        runtime = smoke.request_for(self.args)["expected_pod"]["spec"]["containers"][0]
        self.assertEqual(runtime["command"][:2], ["/bin/sh", "-c"])
        self.assertIn("exec /bin/sleep 3600", runtime["command"][2])
        self.assertEqual(len(runtime["args"]), 4)


if __name__ == "__main__":
    unittest.main()
