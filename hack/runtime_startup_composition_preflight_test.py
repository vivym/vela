import importlib.util
import json
import os
import pathlib
import tempfile
import unittest


SPEC = importlib.util.spec_from_file_location(
    "runtime_startup_composition_preflight",
    pathlib.Path(__file__).with_name("runtime_startup_composition_preflight.py"),
)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(MODULE)


class CompositionPreflightTest(unittest.TestCase):
    def test_missing_inputs_fail_closed_without_exposing_values(self):
        code, report = MODULE.run({})
        self.assertNotEqual(code, 0)
        self.assertFalse(report["ready"])
        self.assertIn("VELA_NODE_AGENT_RUNTIME_CRI_SOCKET", report["failed_checks"])
        for check in report["checks"]:
            self.assertNotIn("value", check)

    def test_deferred_runtime_paths_are_not_reported_as_ready_failures(self):
        pathlib.Path("/tmp/vela-preflight").mkdir(mode=0o700, exist_ok=True)
        environment = {name: "/tmp/vela-preflight/" + name.lower() for name in MODULE.REQUIRED_PATH_ENV}
        try:
            code, report = MODULE.run(environment)
            self.assertNotEqual(code, 0)
            statuses = {item["name"]: item["status"] for item in report["checks"]}
            expected = "deferred" if os.geteuid() == 0 else "parent_untrusted"
            self.assertEqual(statuses["VELA_NODE_AGENT_RUNTIME_STARTUP_SOCKET"], expected)
            self.assertEqual(statuses["VELA_NODE_AGENT_RUNTIME_STARTUP_LEDGER_DIRECTORY"], expected)
        finally:
            pathlib.Path("/tmp/vela-preflight").rmdir()

    def test_present_socket_must_be_a_socket(self):
        environment = {name: "/tmp/vela-preflight/" + name.lower() for name in MODULE.REQUIRED_PATH_ENV}
        environment["VELA_NODE_AGENT_RUNTIME_CRI_SOCKET"] = __file__
        _, report = MODULE.run(environment)
        check = next(item for item in report["checks"] if item["name"] == "VELA_NODE_AGENT_RUNTIME_CRI_SOCKET")
        self.assertEqual(check["status"], "wrong_type")

    def test_deferred_path_requires_existing_trusted_parent(self):
        environment = {name: "/tmp/vela-preflight/" + name.lower() for name in MODULE.REQUIRED_PATH_ENV}
        environment["VELA_NODE_AGENT_RUNTIME_STARTUP_SOCKET"] = "/tmp/vela-preflight/missing-parent/startup.sock"
        _, report = MODULE.run(environment)
        check = next(item for item in report["checks"] if item["name"] == "VELA_NODE_AGENT_RUNTIME_STARTUP_SOCKET")
        self.assertEqual(check["status"], "parent_missing")
        self.assertIn("VELA_NODE_AGENT_RUNTIME_STARTUP_SOCKET", report["failed_checks"])

    def test_deferred_path_rejects_group_or_other_writable_parent(self):
        parent = pathlib.Path("/tmp/vela-preflight-writable-parent")
        parent.mkdir(mode=0o777, exist_ok=True)
        try:
            environment = {name: "/tmp/vela-preflight/" + name.lower() for name in MODULE.REQUIRED_PATH_ENV}
            environment["VELA_NODE_AGENT_RUNTIME_STARTUP_SOCKET"] = str(parent / "startup.sock")
            _, report = MODULE.run(environment)
            check = next(item for item in report["checks"] if item["name"] == "VELA_NODE_AGENT_RUNTIME_STARTUP_SOCKET")
            self.assertEqual(check["status"], "parent_untrusted")
        finally:
            parent.rmdir()

    def test_deferred_path_rejects_symlink_parent(self):
        base = pathlib.Path("/tmp/vela-preflight-symlink")
        outside = pathlib.Path("/tmp/vela-preflight-symlink-target")
        outside.mkdir(mode=0o700, exist_ok=True)
        base.unlink(missing_ok=True)
        base.symlink_to(outside, target_is_directory=True)
        try:
            environment = {name: "/tmp/vela-preflight-symlink/startup.sock" for name in MODULE.REQUIRED_PATH_ENV}
            _, report = MODULE.run(environment)
            check = next(item for item in report["checks"] if item["name"] == "VELA_NODE_AGENT_RUNTIME_STARTUP_SOCKET")
            self.assertEqual(check["status"], "parent_wrong_type")
        finally:
            base.unlink(missing_ok=True)
            outside.rmdir()

    def test_broken_socket_symlink_is_not_deferred(self):
        path = pathlib.Path("/tmp/vela-preflight-broken.sock")
        path.unlink(missing_ok=True)
        path.symlink_to("/tmp/vela-preflight-no-such-target.sock")
        try:
            environment = {name: str(path) for name in MODULE.REQUIRED_PATH_ENV}
            _, report = MODULE.run(environment)
            check = next(item for item in report["checks"] if item["name"] == "VELA_NODE_AGENT_RUNTIME_STARTUP_SOCKET")
            self.assertEqual(check["status"], "symlink")
        finally:
            path.unlink(missing_ok=True)

    def test_existing_state_directory_requires_trusted_mode(self):
        directory = pathlib.Path("/tmp/vela-preflight-state")
        directory.mkdir(mode=0o777, exist_ok=True)
        directory.chmod(0o777)
        try:
            environment = {name: "/tmp/vela-preflight-state" for name in MODULE.REQUIRED_PATH_ENV}
            _, report = MODULE.run(environment)
            check = next(item for item in report["checks"] if item["name"] == "VELA_NODE_AGENT_RUNTIME_JOURNAL_STATE_DIRECTORY")
            self.assertEqual(check["status"], "untrusted")
        finally:
            directory.chmod(0o700)
            directory.rmdir()

    def test_unresolved_runtime_state_requires_reprovision(self):
        root = pathlib.Path("/tmp/vela-preflight-unresolved")
        root.mkdir(mode=0o700, exist_ok=True)
        for child in root.iterdir():
            if child.is_file():
                child.unlink()
        (root / "execution-admission.json").write_text(json.dumps({"backend_lifecycle": {"state": "UNRESOLVED"}}), encoding="utf-8")
        try:
            environment = {name: str(root / name.lower()) for name in MODULE.REQUIRED_PATH_ENV}
            environment["VELA_NODE_AGENT_RUNTIME_JOURNAL_STATE_DIRECTORY"] = str(root)
            _, report = MODULE.run(environment)
            check = next(item for item in report["checks"] if item["name"] == "runtime_journal_backend_lifecycle")
            self.assertEqual(check["status"], "requires-reprovision")
            self.assertIn("runtime_journal_backend_lifecycle", report["failed_checks"])
        finally:
            (root / "execution-admission.json").unlink(missing_ok=True)
            root.rmdir()

    def test_corrupt_or_consumed_ledger_never_passes(self):
        header = {"schema_version": 3, "ledger_id": "ledger", "node_identity": "cpu-node"}
        startup = {"startup": {"operation_id": "operation", "request": {"journal_id": "journal"}, "owner": {"pid": 1}}}
        exited = {"exit": {"operation_id": "operation", "journal_id": "journal", "observation": {"owner": {"pid": 1}}}}
        cases = {
            "empty": "",
            "bad-header": "{}\n",
            "wrong-json-type": "[]\n",
            "unknown-record": json.dumps(header) + "\n{}\n",
            "active": "\n".join(map(json.dumps, [header, startup])) + "\n",
            "orphan-exit": "\n".join(map(json.dumps, [header, exited])) + "\n",
            "truncated": json.dumps(header) + "\n{",
        }
        with tempfile.TemporaryDirectory() as directory:
            path = pathlib.Path(directory) / "runtime-startups.jsonl"
            for name, wire in cases.items():
                with self.subTest(name=name):
                    path.write_text(wire, encoding="utf-8")
                    checks = MODULE.state_machine_checks({"VELA_NODE_AGENT_RUNTIME_STARTUP_LEDGER_DIRECTORY": directory})
                    check = next(item for item in checks if item["name"] == "runtime_startup_ledger")
                    self.assertNotEqual(check["status"], "ready")

            path.write_text("\n".join(map(json.dumps, [header, startup, exited])) + "\n", encoding="utf-8")
            checks = MODULE.state_machine_checks({"VELA_NODE_AGENT_RUNTIME_STARTUP_LEDGER_DIRECTORY": directory})
            check = next(item for item in checks if item["name"] == "runtime_startup_ledger")
            self.assertEqual(check["status"], "ready")

    def test_initialized_header_only_ledger_is_ready(self):
        with tempfile.TemporaryDirectory() as directory:
            pathlib.Path(directory, "runtime-startups.jsonl").write_text(
                json.dumps({"schema_version": 3, "ledger_id": "ledger", "node_identity": "cpu-node"}) + "\n",
                encoding="utf-8",
            )
            checks = MODULE.state_machine_checks({"VELA_NODE_AGENT_RUNTIME_STARTUP_LEDGER_DIRECTORY": directory})
            check = next(item for item in checks if item["name"] == "runtime_startup_ledger")
            self.assertEqual(check, {"name": "runtime_startup_ledger", "status": "ready", "active_startups": 0})

    def test_nonobject_journal_is_rejected_without_crashing(self):
        with tempfile.TemporaryDirectory() as directory:
            path = pathlib.Path(directory) / "execution-admission.json"
            for value in ([], None, {"backend_lifecycle": []}):
                path.write_text(json.dumps(value), encoding="utf-8")
                checks = MODULE.state_machine_checks({"VELA_NODE_AGENT_RUNTIME_JOURNAL_STATE_DIRECTORY": directory})
                self.assertEqual(checks[0]["status"], "requires-reprovision")


if __name__ == "__main__":
    unittest.main()
