"""Counterexamples for the validation driver's evidence verdict."""

import copy
import importlib.util
import pathlib
import unittest


spec = importlib.util.spec_from_file_location(
    "runtime_startup_matrix", pathlib.Path(__file__).with_name("run-runtime-startup-validation-matrix.py")
)
matrix = importlib.util.module_from_spec(spec)
spec.loader.exec_module(matrix)


def result_fixture(scenario="normal"):
    pod = {"pod_uid": "00000000-0000-0000-0000-000000000001", "pod_namespace": "default", "pod_name": "validation"}
    target = dict(pod, container_id="a" * 64, sandbox_id="b" * 64, container_name="model-runtime")
    worker = dict(pod, container_id="c" * 64, sandbox_id="b" * 64, container_name="stage-worker-agent")
    reply = {"version": 2, "validation_only": True, "fd_count": 4, "target": target, "worker_target": worker}
    receipt = dict(reply, version=1, scenario=scenario, outcome="completed" if scenario == "normal" else "failed")
    if scenario != "normal":
        receipt["error"] = "injected helper fault"
    policy = {"version": 1, "operation_id": "00000000-0000-0000-0000-000000000002", "request_digest": [1] * 32}
    return {
        "scenario": scenario, "expected_pod": pod, "reply": reply, "receipt": receipt,
        "fd_count": 4, "reply_flags": 0, "cleanup_verified": True, "cleanup_remaining_ids": [],
        "launcher_exit": 0 if scenario == "normal" else 1,
        "policy_request": policy, "policy_reply": dict(policy, evidence_digest=[2] * 32),
        "policy_fd_count": 0, "policy_flags": 0,
    }


class ValidationVerdictTest(unittest.TestCase):
    def test_launcher_and_policy_wire_versions_are_distinct(self):
        request, _, _ = matrix.request_payload("validation", "image", 65532, 65532, "/run/vela/pidfd-broker.sock")
        self.assertEqual(request["version"], 2)
        result = result_fixture()
        self.assertEqual(result["policy_request"]["version"], 1)

    def test_each_scenario_requires_its_own_outcome(self):
        for scenario in matrix.SCENARIOS:
            with self.subTest(scenario=scenario):
                result = result_fixture(scenario)
                self.assertEqual(matrix.validation_errors(result), [])
                result["receipt"]["outcome"] = "failed" if scenario == "normal" else "completed"
                self.assertTrue(matrix.validation_errors(result))

    def test_missing_or_mismatched_evidence_cannot_pass(self):
        mutations = {
            "driver failure": lambda r: r.update(driver_error="timeout"),
            "missing handoff": lambda r: r.pop("reply"),
            "unmarked helper": lambda r: r["reply"].pop("validation_only"),
            "missing descriptor": lambda r: r.update(fd_count=2),
            "truncated rights": lambda r: r.update(reply_flags=matrix.socket.MSG_CTRUNC),
            "missing cleanup IDs": lambda r: r["reply"]["worker_target"].pop("container_id"),
            "wrong Pod": lambda r: r["reply"]["target"].update(pod_uid="different"),
            "leak": lambda r: r.update(cleanup_remaining_ids=["a" * 64]),
            "wrong receipt": lambda r: r["receipt"].update(scenario="helper-crash"),
            "changed receipt target": lambda r: r["receipt"]["target"].update(container_id="d" * 64),
            "failed normal exit": lambda r: r.update(launcher_exit=1),
            "unbound policy": lambda r: r["policy_reply"].update(operation_id="different"),
            "missing policy": lambda r: r.pop("policy_reply"),
            "policy rights": lambda r: r.update(policy_fd_count=1),
        }
        for name, mutate in mutations.items():
            with self.subTest(name=name):
                result = result_fixture()
                # Receipts and replies were serialized independently in real runs.
                result["receipt"] = copy.deepcopy(result["receipt"])
                mutate(result)
                self.assertTrue(matrix.validation_errors(result), name)


if __name__ == "__main__":
    unittest.main()
