import argparse
import contextlib
import importlib.util
import io
import json
import os
import pathlib
import stat
import tempfile
import unittest


MODULE_PATH = pathlib.Path(__file__).with_name("install_runtime_startup_packages.py")
spec = importlib.util.spec_from_file_location("installer", MODULE_PATH)
installer = importlib.util.module_from_spec(spec)
spec.loader.exec_module(installer)


class InstallerTest(unittest.TestCase):
    def package(self, directory):
        for source, _, _ in installer.FILES.values():
            path = directory / source
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes((source + "\n").encode())

    def verifier(self, directory):
        path = directory / "verifier"
        path.write_text("#!/bin/sh\nexit 0\n")
        path.chmod(0o755)
        return path

    def args(self, package, root, verifier, **kwargs):
        values = dict(
            package_dir=str(package), root=str(root), revision="test-revision",
            verifier=str(verifier), apply=True, enable_services=False, reload_node=False,
        )
        values.update(kwargs)
        return argparse.Namespace(**values)

    def test_systemd_failure_writes_failed_receipt_and_restores_files(self):
        with tempfile.TemporaryDirectory() as temp:
            base = pathlib.Path(temp)
            package, root, bin_dir = base / "package", base / "root", base / "bin"
            package.mkdir(); root.mkdir(); bin_dir.mkdir()
            self.package(package)
            verifier = self.verifier(bin_dir)
            systemctl = bin_dir / "systemctl"
            systemctl.write_text("#!/bin/sh\nexit 7\n")
            systemctl.chmod(0o755)
            old = os.environ.get("PATH")
            os.environ["PATH"] = str(bin_dir) + os.pathsep + (old or "")
            try:
                with self.assertRaises(Exception):
                    installer.install(self.args(package, root, verifier, enable_services=True))
            finally:
                if old is None:
                    os.environ.pop("PATH", None)
                else:
                    os.environ["PATH"] = old
            receipts = list((root / "var/lib/vela/runtime-startup/receipts").glob("*.json"))
            self.assertEqual(len(receipts), 1)
            receipt = json.loads(receipts[0].read_text())
            self.assertEqual(receipt["status"], "failed")
            self.assertFalse(receipt["applied"])
            for _, relative, _ in installer.FILES.values():
                self.assertFalse((root / relative).exists())

    def test_rollback_receipt_is_single_use(self):
        with tempfile.TemporaryDirectory() as temp:
            base = pathlib.Path(temp)
            package, root, bin_dir = base / "package", base / "root", base / "bin"
            package.mkdir(); root.mkdir(); bin_dir.mkdir()
            self.package(package)
            verifier = self.verifier(bin_dir)
            with contextlib.redirect_stdout(io.StringIO()):
                self.assertEqual(installer.install(self.args(package, root, verifier)), 0)
            receipt_path = next((root / "var/lib/vela/runtime-startup/receipts").glob("*.json"))
            receipt = json.loads(receipt_path.read_text())
            self.assertEqual(receipt["status"], "installed")
            rollback_args = argparse.Namespace(receipt=str(receipt_path), root=str(root), apply=True, enable_services=False)
            with contextlib.redirect_stdout(io.StringIO()):
                self.assertEqual(installer.rollback(rollback_args), 0)
            updated = json.loads(receipt_path.read_text())
            self.assertEqual(updated["status"], "rolled-back")
            with self.assertRaises(RuntimeError):
                installer.rollback(rollback_args)

    def test_rollback_restores_recorded_systemd_state(self):
        with tempfile.TemporaryDirectory() as temp:
            base = pathlib.Path(temp)
            package, root, bin_dir = base / "package", base / "root", base / "bin"
            package.mkdir(); root.mkdir(); bin_dir.mkdir()
            self.package(package)
            verifier = self.verifier(bin_dir)
            log = base / "systemctl.log"
            systemctl = bin_dir / "systemctl"
            systemctl.write_text(
                "#!/bin/sh\n"
                f"echo \"$@\" >> {log}\n"
                "if [ \"$1\" = is-enabled ]; then exit 1; fi\n"
                "exit 0\n"
            )
            systemctl.chmod(0o755)
            old = os.environ.get("PATH")
            os.environ["PATH"] = str(bin_dir) + os.pathsep + (old or "")
            try:
                with contextlib.redirect_stdout(io.StringIO()):
                    installer.install(self.args(package, root, verifier, enable_services=True))
                receipt_path = next((root / "var/lib/vela/runtime-startup/receipts").glob("*.json"))
                with contextlib.redirect_stdout(io.StringIO()):
                    installer.rollback(argparse.Namespace(receipt=str(receipt_path), root=str(root), apply=True, enable_services=False))
            finally:
                if old is None:
                    os.environ.pop("PATH", None)
                else:
                    os.environ["PATH"] = old
            calls = log.read_text().splitlines()
            self.assertTrue(any(line.startswith("enable vela-pidfd-broker.service") for line in calls))
            self.assertTrue(any(line.startswith("disable vela-pidfd-broker.service") for line in calls))

    def test_symlink_parent_is_rejected(self):
        with tempfile.TemporaryDirectory() as temp:
            base = pathlib.Path(temp)
            package, root, bin_dir, outside = base / "package", base / "root", base / "bin", base / "outside"
            package.mkdir(); root.mkdir(); bin_dir.mkdir(); outside.mkdir()
            self.package(package)
            verifier = self.verifier(bin_dir)
            (root / "usr").symlink_to(outside, target_is_directory=True)
            with self.assertRaises(RuntimeError):
                installer.install(self.args(package, root, verifier))

    def test_broken_symlink_parent_is_rejected_before_mkdir(self):
        with tempfile.TemporaryDirectory() as temp:
            base = pathlib.Path(temp)
            package, root, bin_dir = base / "package", base / "root", base / "bin"
            package.mkdir(); root.mkdir(); bin_dir.mkdir()
            self.package(package)
            verifier = self.verifier(bin_dir)
            (root / "usr").symlink_to(base / "missing-target", target_is_directory=True)
            with self.assertRaises(RuntimeError):
                installer.install(self.args(package, root, verifier))
            self.assertFalse((base / "missing-target").exists())


if __name__ == "__main__":
    unittest.main()
