#!/usr/bin/env python3
"""Safety boundaries for the native, non-versioned MinIO migration lane."""
import importlib.util
import io
import pathlib
import unittest
from unittest.mock import patch
import zipfile

spec = importlib.util.spec_from_file_location("migration", pathlib.Path(__file__).with_name("migrate-minio-ha.py"))
migration = importlib.util.module_from_spec(spec)
spec.loader.exec_module(migration)


class MigrationSafety(unittest.TestCase):
    def test_live_append_is_reported_but_overwrite_and_delete_are_rejected(self):
        original = {"bucket": [{"key": "object", "size": 4, "etag": "original"}]}
        appended = {"bucket": original["bucket"] + [{"key": "new", "size": 1}]}
        self.assertEqual(migration.snapshot_changes(original, appended), 1)
        for changed in ({"bucket": []}, {"bucket": [{"key": "object", "size": 4, "etag": "changed"}]}):
            with self.assertRaises(RuntimeError):
                migration.snapshot_changes(original, changed)

    def test_versions_and_delete_markers_never_enter_ordinary_mirror(self):
        for entry in ({"versionId": "retained-version"}, {"isDeleteMarker": True}, {"type": "folder"}):
            row = {"status": "success", "type": "file", "key": "object", **entry}
            with patch.object(migration, "mc", side_effect=[[{"key": "bucket/"}], [row], [{"versioning": {}}]]):
                with self.assertRaises(RuntimeError):
                    migration.copyable_inventory()

    def test_enabled_versioning_allows_only_verified_empty_buckets(self):
        for entries, allowed in (([], True), ([{"status": "success", "type": "file", "key": "object"}], False)):
            with patch.object(migration, "mc", side_effect=[[{"key": "bucket/"}], entries,
                                                         [{"versioning": {"status": "Enabled"}}]]):
                if allowed:
                    self.assertEqual(migration.copyable_inventory(), {"bucket": []})
                else:
                    with self.assertRaises(RuntimeError):
                        migration.copyable_inventory()

    def test_iam_order_does_not_hide_permission_changes(self):
        left = b'{"p":{"Statement":[{"Effect":"Allow","Action":["s3:GetObject","s3:PutObject"]}]}}'
        reordered = b'{"p":{"Statement":[{"Action":["s3:PutObject","s3:GetObject"],"Effect":"Allow"}]}}'
        changed = reordered.replace(b'Allow', b'Deny')
        self.assertTrue(migration.metadata_equal("iam-assets/policies.json", left, reordered))
        self.assertFalse(migration.metadata_equal("iam-assets/policies.json", left, changed))
        self.assertFalse(migration.metadata_equal("bucket-targets.json", b'opaque-1', b'opaque-2'))

    def test_export_frame_is_bounded_and_no_trailing_bytes_are_accepted(self):
        for raw in (b'16777217\n', b'4\nabc', b'1\na1\nbtrailer'):
            with patch.object(migration, "pod_script", return_value=raw):
                with self.assertRaises((RuntimeError, ValueError)):
                    migration.export_metadata("src")

    def test_client_cutover_fences_subsequent_mirrors(self):
        with patch.object(migration, "run", return_value=b'llmpool01\n'), patch.object(
                migration, "kube", return_value={"spec": {"selector": {"app": "minio-ha"}}}):
            with self.assertRaises(RuntimeError):
                migration.preflight()


if __name__ == "__main__":
    unittest.main()
