import copy
import importlib.util
import json
import pathlib
import unittest
from unittest.mock import patch

s = importlib.util.spec_from_file_location("storage", pathlib.Path(__file__).with_name("initialize-worker-model-storage.py"))
m = importlib.util.module_from_spec(s)
s.loader.exec_module(m)


class DiskInitializationSafety(unittest.TestCase):
    def node(self):
        return {"hostname": "server-120", "address": "10.1.201.58", "devices": [
            {"role": role, "mountpoint": mount, "filesystem": "xfs", "serial": role,
             "device_by_id": "/dev/disk/by-id/nvme-" + role} for role, mount in m.MOUNTS.items()]}

    def test_protected_and_management_hosts_are_rejected(self):
        for suffix in (44, 56, 57, 66, 70, 71):
            n = self.node(); n["address"] = "10.1.201." + str(suffix)
            with patch.object(m.socket, "gethostname", return_value=n["hostname"]):
                with self.assertRaises(RuntimeError):
                    m.select_node({"nodes": [n]})

    def test_duplicate_disk_and_root_mount_are_rejected(self):
        for mutation in ("duplicate", "root"):
            n = self.node()
            if mutation == "duplicate":
                n["devices"][1]["serial"] = n["devices"][0]["serial"]
            else:
                n["devices"][0]["mountpoint"] = "/"
            with patch.object(m.socket, "gethostname", return_value=n["hostname"]):
                with self.assertRaises(RuntimeError):
                    m.select_node({"nodes": [n]})

    def test_second_disk_preflight_failure_prevents_first_disk_format(self):
        with patch.object(m.os, "geteuid", return_value=0), patch.object(m.shutil, "which", return_value="/usr/sbin/mkfs.xfs"), \
                patch.object(m.pathlib.Path, "exists", return_value=False), \
                patch.object(m, "inspect", side_effect=[None, RuntimeError("second disk in use")]), \
                patch.object(m, "run") as run:
            with self.assertRaises(RuntimeError):
                m.apply(self.node(), "reviewed")
            run.assert_not_called()

    def test_partial_initialization_cannot_be_reformatted_by_retry(self):
        with patch.object(m.os, "geteuid", return_value=0), patch.object(m.shutil, "which", return_value="/usr/sbin/mkfs.xfs"), \
                patch.object(m.pathlib.Path, "exists", return_value=True), patch.object(m, "run") as run:
            with self.assertRaises(RuntimeError):
                m.apply(self.node(), "reviewed")
            run.assert_not_called()


if __name__ == "__main__":
    unittest.main()
