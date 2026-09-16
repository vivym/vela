import copy
import importlib.util
from pathlib import Path
import unittest
import uuid

spec = importlib.util.spec_from_file_location('h3_api_flow', Path(__file__).with_name('verify-h3-api-flow.py'))
flow = importlib.util.module_from_spec(spec)
spec.loader.exec_module(flow)


class AcceptanceAssertions(unittest.TestCase):
    def setUp(self):
        self.job = {'job_id': str(uuid.uuid4()), 'project_id': str(uuid.uuid4()), 'state': 'SUCCEEDED',
                    'pricing': {'quantity': 1, 'quoted_amount_minor': 100, 'unit_amount_minor': 100, 'currency': 'CNY'}}
        self.artifact_set = str(uuid.uuid4())
        self.snapshot = {'job_id': self.job['job_id'], 'project_id': self.job['project_id'], 'state': 'SUCCEEDED',
                         'billable_started_at': '2026-09-15T12:00:00Z', 'charge_count': 1,
                         'visible_completion_count': 1, 'artifact_set_count': 1, 'artifact_set_id': self.artifact_set,
                         'idempotency_count': 1, 'reservation_state': 'CONSUMED', 'reservation_amount_minor': 100,
                         'account_reserved_minor': 0, 'sum_reserved_minor': 0,
                         'charge': {'reason': 'VISIBLE_COMPLETION', 'state': 'POSTED', 'amount_minor': 100, 'currency': 'CNY'}}

    def test_consistent_completion(self):
        flow.validate_job(self.job, self.job['project_id'])
        flow.validate_billing(self.snapshot, self.job, self.artifact_set)

    def test_false_billing_success_is_rejected(self):
        for field, value in [('charge_count', 0), ('charge_count', 2), ('billable_started_at', None),
                             ('visible_completion_count', 0), ('artifact_set_count', 0),
                             ('reservation_state', 'RESERVED'), ('reservation_amount_minor', 99),
                             ('account_reserved_minor', 100), ('state', 'FAILED'),
                             ('idempotency_count', 2)]:
            with self.subTest(field=field, value=value), self.assertRaises(ValueError):
                snapshot = copy.deepcopy(self.snapshot)
                snapshot[field] = value
                flow.validate_billing(snapshot, self.job, self.artifact_set)
        for field, value in [('reason', 'CUSTOMER_CANCELLATION'), ('amount_minor', 99), ('currency', 'USD')]:
            with self.subTest(field=field), self.assertRaises(ValueError):
                snapshot = copy.deepcopy(self.snapshot)
                snapshot['charge'][field] = value
                flow.validate_billing(snapshot, self.job, self.artifact_set)

    def test_replay_cannot_create_another_job_or_quote(self):
        with self.assertRaises(ValueError):
            flow.validate_job(self.job, self.job['project_id'], str(uuid.uuid4()))
        with self.assertRaises(ValueError):
            flow.validate_job(self.job, self.job['project_id'], self.job['job_id'], {})

    def test_unsafe_origins_rejected(self):
        for value in ['http://gateway', 'https://user:secret@gateway', 'https://gateway?key=secret', 'https://gateway/#fragment']:
            with self.subTest(value=value), self.assertRaises(ValueError):
                flow.origin(value)

    def media_fixture(self, audio_ms=5175):
        media = {'streams': [
            {'codec_type': 'video', 'width': 1344, 'height': 768, 'codec_name': 'h264',
             'nb_read_frames': '124', 'avg_frame_rate': '24/1', 'r_frame_rate': '24/1',
             'duration': '5.166667', 'start_time': '0.000000'},
            {'codec_type': 'audio', 'codec_name': 'aac', 'sample_rate': '32000', 'channels': 2,
             'duration': str(audio_ms / 1000), 'start_time': '0.000000'}],
            'format': {'duration': str(audio_ms / 1000), 'format_name': 'mov,mp4,m4a,3gp,3g2,mj2'}}
        metadata = {'frame_count': 124, 'frame_rate_milli': 24000, 'duration_milliseconds': 5167,
                    'requested_duration_milliseconds': 5000, 'container_duration_milliseconds': audio_ms,
                    'audio': {'codec': 'aac', 'sample_rate': 32000, 'channels': 2,
                              'duration_milliseconds': audio_ms}}
        return media, metadata

    def test_complete_audio_and_terminal_aac_padding(self):
        for duration in [5175, 5200, 5207]:
            with self.subTest(duration=duration):
                flow.validate_media(*self.media_fixture(duration))
        for duration in [5160, 5174, 5208]:
            with self.subTest(duration=duration), self.assertRaises(ValueError):
                flow.validate_media(*self.media_fixture(duration))

    def test_wrong_video_geometry_timing_and_missing_tail_are_rejected(self):
        for field, value in [('width', 16), ('height', 16), ('avg_frame_rate', '1/1'),
                             ('r_frame_rate', '1/1'), ('duration', '124'),
                             ('start_time', '0.5'), ('nb_read_frames', '120')]:
            with self.subTest(field=field), self.assertRaises(ValueError):
                media, metadata = self.media_fixture()
                media['streams'][0][field] = value
                flow.validate_media(media, metadata)
        with self.assertRaises(ValueError):
            media, metadata = self.media_fixture()
            media['streams'][1]['start_time'] = '0.5'
            flow.validate_media(media, metadata)

    def test_inconsistent_api_media_and_extra_streams_are_rejected(self):
        for field, value in [('frame_rate_milli', 1000), ('duration_milliseconds', 5000),
                             ('container_duration_milliseconds', 5167),
                             ('requested_duration_milliseconds', 4000)]:
            with self.subTest(field=field), self.assertRaises(ValueError):
                media, metadata = self.media_fixture()
                metadata[field] = value
                flow.validate_media(media, metadata)
        with self.assertRaises(ValueError):
            media, metadata = self.media_fixture()
            metadata['audio']['duration_milliseconds'] = 5200
            flow.validate_media(media, metadata)
        with self.assertRaises(ValueError):
            media, metadata = self.media_fixture()
            media['streams'].append({'codec_type': 'subtitle'})
            flow.validate_media(media, metadata)

    def test_download_origin_explicit_allowlist(self):
        client = flow.Client('https://gateway-71/api', 'unused', None, ['https://gateway-70:30443'])
        self.assertEqual(client.allowed_origins, {'https://gateway-71', 'https://gateway-70:30443'})
        with self.assertRaisesRegex(ValueError, 'bypasses'):
            client.download({'download_url': 'https://untrusted/video'}, Path('/unused'))
        with self.assertRaises(ValueError):
            flow.Client('https://gateway-71/api', 'unused', None, ['https://gateway-70/path'])


if __name__ == '__main__':
    unittest.main()
