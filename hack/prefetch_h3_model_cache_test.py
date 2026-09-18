import hashlib
import importlib.util
import json
from pathlib import Path
import sys
import tempfile
import types
import unittest
from unittest import mock

# Filesystem transport verification is exercised by the installed fast-h3
# module; these tests exercise publication and reuse without requiring Linux.
cache_module = types.ModuleType('fast_h3.vela.model_cache')
cache_module.local_filesystem = lambda *args: None
cache_module.require_local_model_cache = lambda *args: None
with mock.patch.dict(sys.modules, {'fast_h3.vela.model_cache': cache_module}):
    spec = importlib.util.spec_from_file_location('prefetch', Path(__file__).with_name('prefetch-h3-model-cache.py'))
    prefetch = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(prefetch)


def manifest(identity, files):
    entries = [{'path': name, 'size_bytes': len(data),
                'sha256': 'sha256:' + hashlib.sha256(data).hexdigest()}
               for name, data in files.items()]
    return {'schema': 'h3-production-model-manifest-v1',
            'content_sha256': 'sha256:' + identity, 'file_count': len(entries),
            'size_bytes': sum(e['size_bytes'] for e in entries),
            'artifacts': {'weights': {'files': entries}},
            'required_runtime_directories': ['empty-runtime-dir']}


class CacheReuseTests(unittest.TestCase):
    def fixture(self, root):
        old = root / ('a' * 64)
        old.mkdir()
        original = old / 'old-shared.bin'
        original.write_bytes(b'shared weights')
        original.chmod(0o444)
        original_info = original.stat()
        old_manifest = manifest(old.name, {'old-shared.bin': b'shared weights'})
        new_manifest = manifest('b' * 64, {'new/shared.bin': b'shared weights', 'new/dit.bin': b'new dit'})
        old_path = root / 'reuse.json'
        old_path.write_text(json.dumps(old_manifest))
        new_path = root / 'manifest.json'
        new_path.write_text(json.dumps(new_manifest))
        return original, original_info, old_manifest, old_path, new_manifest, new_path

    def test_publication_downloads_only_delta_and_preserves_original_inode(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d).resolve()
            original, before, _, reuse, new, source = self.fixture(root)
            def transfer(command, check):
                listing = next(a.split('=', 1)[1] for a in command if a.startswith('--files-from='))
                self.assertEqual(Path(listing).read_text().splitlines(), ['new/dit.bin'])
                destination = Path(command[-1])
                (destination / 'new/dit.bin').write_bytes(b'new dit')
            read_text = Path.read_text
            with mock.patch.object(Path, 'read_text', autospec=True,
                                   side_effect=lambda p, *a, **kw: '' if str(p) == '/proc/self/mountinfo' else read_text(p, *a, **kw)), \
                 mock.patch.object(prefetch.os, 'statvfs', return_value=types.SimpleNamespace(
                     f_bavail=100 * 1024**3, f_frsize=1, f_blocks=200 * 1024**3)), \
                 mock.patch.object(prefetch.subprocess, 'run', side_effect=transfer) as transfer_call:
                result = prefetch.prefetch(source, hashlib.sha256(source.read_bytes()).hexdigest(),
                                           'rsync://source/models', root, reuse)
            published = Path(result['model_root'])
            self.assertEqual((published / 'new/shared.bin').stat().st_ino, original.stat().st_ino)
            self.assertEqual(original.stat().st_mtime_ns, before.st_mtime_ns)
            self.assertEqual(original.read_bytes(), b'shared weights')
            self.assertEqual(original.stat().st_mode & 0o777, 0o444)
            self.assertEqual(result['verified_bytes'], new['size_bytes'])
            self.assertTrue((published / 'empty-runtime-dir').is_dir())
            transfer_call.assert_called_once()
            self.addCleanup(lambda: self.make_writable(root))

    def test_reuse_rejects_corruption_and_symlinks(self):
        for fault in ['corruption', 'symlink', 'writable']:
            with self.subTest(fault=fault), tempfile.TemporaryDirectory() as d:
                root = Path(d).resolve()
                original, _, old, _, new, _ = self.fixture(root)
                candidate = root / 'partial'
                candidate.mkdir()
                if fault == 'symlink':
                    original.unlink()
                    target = root / 'outside'
                    target.write_bytes(b'shared weights')
                    original.symlink_to(target)
                elif fault == 'writable':
                    original.chmod(0o644)
                else:
                    original.chmod(0o644)
                    original.write_bytes(b'broken weights')
                    original.chmod(0o444)
                with self.assertRaises(ValueError):
                    prefetch.reuse_verified_files(candidate, prefetch.manifest_entries(new), original.parent, old)
                self.assertFalse((candidate / 'new/shared.bin').exists())

    @staticmethod
    def make_writable(root):
        if not root.exists():
            return
        for p in [root, *root.rglob('*')]:
            if p.is_dir(): p.chmod(0o755)


if __name__ == '__main__':
    unittest.main()
