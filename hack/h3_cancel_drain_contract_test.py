#!/usr/bin/env python3
"""Run against a pinned fast-h3 source via PYTHONPATH; no GPU or weights needed.

Exercises the actual Python driver cancellation/drain boundary without a status
request. An executor barrier makes completion deterministic, including cleanup.
"""
import io
import json
import tempfile
import unittest
from pathlib import Path
from threading import Event, Thread
from types import SimpleNamespace

from fast_h3.vela.driver import _ActiveExecution, _DriverSession, _serve_session
from fast_h3.vela.runtime import StageExecutionCancelled, StageExecutionError


class CancelDrainContract(unittest.TestCase):
    def run_case(self, failure=None, finished_before_cancel=False, defer_completion=False):
        identity = SimpleNamespace(authority_digest='a' * 64, execution_sequence=7)
        started, finish = Event(), Event()
        class Runtime:
            def execute(self, prepared, cancellation):
                started.set()
                if not finish.wait(2):
                    raise RuntimeError('test writer did not finish')
                if failure:
                    raise failure
                raise StageExecutionCancelled('joined cancellation')
            def cancel(self, prepared, reason):
                pass
            def shutdown(self):
                pass
        session = _DriverSession('DIT', Runtime())
        self.addCleanup(session.shutdown)
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        staging = Path(temporary.name) / 'staging'
        staging.write_bytes(b'partial output')
        active = _ActiveExecution(SimpleNamespace(identity=identity, staging_path=staging), b'', None, Event())
        session.active = active
        session.start(identity)
        self.assertTrue(started.wait(2))
        if finished_before_cancel:
            finish.set()
            session.executor.submit(lambda: None).result(timeout=2)
        with session.gate:
            session.cancel(identity, 'MODEL_RUNTIME_CANCEL_REASON_CONTROL_PLANE_STOP')
            session.publish_snapshot()
        query = {'authority_digest': identity.authority_digest, 'execution_sequence': 7}
        if defer_completion:
            return session, finish
        if not finished_before_cancel:
            self.assertFalse(session.drain(query), 'running writer was released')
            finish.set()
        # Queue behind both the compute and cancellation-finalization work.
        session.executor.submit(lambda: None).result(timeout=2)
        return session, active, query, staging

    def test_cancel_drains_without_status_after_real_writer_completion(self):
        for finished in [False, True]:
            with self.subTest(finished_before_cancel=finished):
                session, active, query, staging = self.run_case(finished_before_cancel=finished)
                self.assertTrue(session.drain(query), 'completed cancellation still requires an execution status call')
                self.assertEqual(active.state, 'STOPPED')
                self.assertFalse(staging.exists())
                self.assertEqual(session.inspect(query['authority_digest'])['state'], 'STOPPED')
                self.assertFalse(session.drain(dict(query, execution_sequence=8)))
                self.assertFalse(session.drain(dict(query, authority_digest='b' * 64)))
                self.assertTrue(session.drain(query), 'drain replay failed')
                with self.assertRaises(RuntimeError):
                    session.start(active.execution.identity)

    def test_shutdown_joins_pending_cancel_without_command_gate_deadlock(self):
        session, finish = self.run_case(defer_completion=True)
        stopped = Event()
        failures = []
        def shutdown_command():
            try:
                _serve_session(session, SimpleNamespace(protocols={}), session.runtime,
                               io.StringIO(json.dumps({'schema_version': 1, 'request_id': 1, 'operation': 'shutdown'}) + '\n'), io.StringIO())
            except Exception as error:
                failures.append(error)
            finally:
                stopped.set()
        thread = Thread(target=shutdown_command, daemon=True)
        thread.start()
        finish.set()
        self.assertTrue(stopped.wait(2), 'shutdown deadlocked on cancellation finalization')
        thread.join(timeout=1)
        self.assertEqual(failures, [])

    def test_cancel_writer_failure_does_not_claim_drain(self):
        error = StageExecutionError('writer join failed', failure_class='WRITER_JOIN_FAILED', worker_reusable=False, consumed_resource_units=0, writers_joined=False)
        session, active, query, _ = self.run_case(failure=error)
        self.assertEqual(active.state, 'FAILED')
        self.assertFalse(session.drain(query))
        self.assertFalse(active.writers_joined)


if __name__ == '__main__':
    unittest.main()
