import hashlib
import importlib.util
from pathlib import Path
import tempfile
import subprocess
import sys
import stat
import time
import unittest
from unittest import mock


spec = importlib.util.spec_from_file_location("distribute", Path(__file__).with_name("distribute-h3-model-cache.py"))
distribute = importlib.util.module_from_spec(spec)
spec.loader.exec_module(distribute)


class DistributionScopeTests(unittest.TestCase):
    def target(self, address="10.1.201.13", tier="256gb"):
        return {"address": address, "node": "server-31", "memory_tier": tier}

    def test_rejects_outside_scope_and_protected_hosts(self):
        for address, tier in [("192.0.2.1", "256gb"), ("10.1.201.66", "256gb"),
                              ("10.1.201.13", "512gb"), ("10.1.201.44", "256gb")]:
            with self.subTest(address=address, tier=tier), self.assertRaises(ValueError):
                distribute.validate_targets([self.target(address, tier)])

    def test_requires_nonempty_unique_targets(self):
        distribute.validate_targets([self.target()])
        for targets in [[], [self.target(), self.target()],
                        [self.target(), self.target("10.1.201.14")]]:
            with self.assertRaises(ValueError):
                distribute.validate_targets(targets)

    def test_refuses_changed_prefetch_implementation(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "prefetch.py").write_bytes(b"reviewed implementation")
            digest = hashlib.sha256(b"reviewed implementation").hexdigest()
            self.assertEqual(distribute.load_assets(root, {"prefetch.py": digest})["prefetch.py"]["sha256"], digest)
            (root / "prefetch.py").write_bytes(b"unexpected replacement")
            with self.assertRaises(ValueError):
                distribute.load_assets(root, {"prefetch.py": digest})
            with self.assertRaises(ValueError):
                distribute.load_assets(root, {"../escape.py": digest})

    def test_remote_publication_resumes_after_interrupted_temporary_write(self):
        functions = {}
        exec(distribute.REMOTE_FILES, functions)
        with tempfile.TemporaryDirectory(dir=Path.home()) as directory:
            root = Path(directory)
            (root / ".prefetch.py-interrupted").write_bytes(b"partial")
            functions["atomic_file"](root / "prefetch.py", b"complete", immutable=True)
            self.assertEqual((root / "prefetch.py").read_bytes(), b"complete")
            functions["atomic_file"](root / "prefetch.py", b"complete", immutable=True)

    def test_remote_publication_rejects_file_and_parent_symlinks(self):
        functions = {}
        exec(distribute.REMOTE_FILES, functions)
        with tempfile.TemporaryDirectory(dir=Path.home()) as directory:
            root = Path(directory)
            target = root / "preserve"
            target.write_bytes(b"original")
            (root / "receipt.json").symlink_to(target)
            with self.assertRaises(ValueError):
                functions["atomic_file"](root / "receipt.json", b"replacement")
            (root / "bin").symlink_to(root, target_is_directory=True)
            with self.assertRaises(ValueError):
                functions["atomic_file"](root / "bin" / "rsync", b"replacement")
            self.assertEqual(target.read_bytes(), b"original")

    def test_cache_ancestors_allow_runtime_traversal_without_opening_worker_data(self):
        functions = {}
        exec(distribute.REMOTE_FILES, functions)
        with tempfile.TemporaryDirectory(dir=Path.home()) as directory:
            root = Path(directory)
            vela = root / "vela"
            vela.mkdir(mode=0o700)
            private = vela / "worker-private"
            private.mkdir(mode=0o700)
            cache = vela / "models"
            cache.mkdir(mode=0o700)
            functions["cache_directory"](cache)
            self.assertEqual(stat.S_IMODE(vela.stat().st_mode), 0o711)
            self.assertEqual(stat.S_IMODE(cache.stat().st_mode), 0o711)
            self.assertEqual(stat.S_IMODE(private.stat().st_mode), 0o700)
            functions["directory"](vela / "new-shared")
            functions["directory"](vela / "tools", mode=0o700)
            self.assertEqual(stat.S_IMODE((vela / "new-shared").stat().st_mode), 0o755)
            self.assertEqual(stat.S_IMODE((vela / "tools").stat().st_mode), 0o700)
            (vela / "tools").chmod(0o755)
            functions["directory"](vela / "tools", mode=0o700)
            self.assertEqual(stat.S_IMODE((vela / "tools").stat().st_mode), 0o700)

    def test_timeout_stops_the_transfer_descendant_before_retry(self):
        functions = {}
        exec(distribute.REMOTE_FILES, functions)
        with tempfile.TemporaryDirectory() as directory:
            destination = Path(directory) / "late-write"
            ready = Path(directory) / "ready"
            child = "import time,pathlib; time.sleep(0.6); pathlib.Path(" + repr(str(destination)) + ").write_text('bad')"
            parent = "import subprocess,sys,time,pathlib; subprocess.Popen([sys.executable,'-c'," + repr(child) + "]); pathlib.Path(" + repr(str(ready)) + ").touch(); time.sleep(60)"
            with self.assertRaises(subprocess.TimeoutExpired):
                functions["run_group"]([sys.executable, "-c", parent], None, 0.3)
            self.assertTrue(ready.exists())
            time.sleep(0.7)
            self.assertFalse(destination.exists())

    def test_followup_waits_for_predecessor_cleanup_and_requires_verified_result(self):
        with tempfile.TemporaryDirectory() as directory:
            progress = Path(directory) / "progress.json"
            predecessor = {"unit": "copy.service", "progress_file": str(progress)}
            progress.write_text('{"passed": true}')
            running = subprocess.CompletedProcess([], 0, 'LoadState=loaded\nActiveState=active\nResult=success\n')
            done = subprocess.CompletedProcess([], 0, 'LoadState=loaded\nActiveState=inactive\nResult=success\n')
            with mock.patch.object(distribute.subprocess, "run", side_effect=[running, done]), mock.patch.object(distribute.time, "sleep") as sleep:
                distribute.wait_for_predecessor(predecessor)
                sleep.assert_called_once_with(30)
            progress.write_text('{"passed": false}')
            with mock.patch.object(distribute.subprocess, "run", return_value=done), self.assertRaises(SystemExit):
                distribute.wait_for_predecessor(predecessor)

    def test_password_sudo_is_scoped_and_redacted(self):
        with tempfile.TemporaryDirectory() as directory:
            secret = Path(directory) / "sudo-password"
            secret.write_text("test-sudo-secret\n")
            target = dict(self.target(), sudo_password_file=str(secret))
            config = dict(remote_tools_root="/opt/vela/test", source="rsync://source/h3/",
                          bandwidth_kib=64, manifest_sha256="a" * 64,
                          content_sha256="b" * 64, expected_bytes=10)
            failed = subprocess.CompletedProcess([], 1, "", "test-sudo-secret test-rsync-secret")
            with mock.patch.object(distribute.subprocess, "run", return_value=failed) as run:
                with self.assertRaisesRegex(RuntimeError, "<redacted> <redacted>"):
                    distribute.execute_target(config, target, {}, "test-rsync-secret")
            args, kwargs = run.call_args
            self.assertEqual(args[0][-1], "sudo -k -S -p '' python3 -")
            self.assertTrue(kwargs["input"].startswith("test-sudo-secret\nPAYLOAD = "))
            self.assertEqual(distribute.ssh_command(target["address"])[-1], "sudo -n python3 -")


if __name__ == "__main__":
    unittest.main()
