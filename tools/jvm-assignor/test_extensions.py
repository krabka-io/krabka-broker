"""Persistent released-API extension lifecycle and conversion integration tests."""
import json
import os
import subprocess
import unittest
from pathlib import Path

ROOT = Path(__file__).parent

class Extensions(unittest.TestCase):
    def start(self, kind, plugin):
        cp = f"{ROOT}/build/worker:{os.environ['KAFKA44_LIBS']}/*:{ROOT}/build/fixture.jar"
        p = subprocess.Popen(['java', '-Xmx128m', '-cp', cp, 'ExtensionWorker', kind, plugin], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        self.addCleanup(self.cleanup, p)
        return p

    def cleanup(self, p):
        if p.poll() is None:
            p.kill()
        p.communicate(timeout=5)

    def call(self, p, op, **kw):
        p.stdin.write(json.dumps(dict(version=1, op=op, **kw)) + '\n')
        p.stdin.flush()
        return json.loads(p.stdout.readline())

    def test_storage_persistent_roundtrip_epoch_delete_and_close(self):
        p = self.start('storage', 'org.apache.kafka.server.streams.InMemoryTopologyDescriptionPlugin')
        self.assertTrue(self.call(p, 'configure', config={})['ok'])
        def node(name, kind, successors=[]):
            return dict(name=name, kind=kind, successors=successors)
        description = dict(subtopologies=[dict(id='sub', nodes=[node('source', {'Source': {'topics': ['a', 'b']}}, ['processor']), node('processor', {'Processor': {'stores': ['store']}}, ['sink']), node('sink', {'Sink': {'topic': None}})])], global_stores=[dict(source=node('global-source', {'Source': {'topics': ['global']}}, ['global-processor']), processor=node('global-processor', {'Processor': {'stores': ['global-store']}}))])
        self.assertIsNone(self.call(p, 'get', group='g', epoch=2)['description'])
        self.assertTrue(self.call(p, 'set', group='g', epoch=2, description=description)['ok'])
        self.assertEqual(self.call(p, 'get', group='g', epoch=2)['description'], description)
        self.assertIsNone(self.call(p, 'get', group='g', epoch=3)['description'])
        self.assertTrue(self.call(p, 'delete', group='g')['ok'])
        self.assertTrue(self.call(p, 'delete', group='g')['ok'])
        self.assertIsNone(self.call(p, 'get', group='g', epoch=2)['description'])
        self.assertTrue(self.call(p, 'stop')['ok'])
        self.assertEqual(p.wait(timeout=5), 0)

    def test_dlq_exact_long_uuid_causes_state_and_stop(self):
        p = self.start('dlq', 'ExtensionFixture')
        args = dict(group='g', topic_id='00112233-4455-6677-8899-aabbccddeeff', partition=3, first=9007199254740993, last=9223372036854775807, delivery_count=7)
        for cause in ['rejected', 'exceeded']:
            self.assertTrue(self.call(p, 'enqueue', cause=cause, **args)['ok'])
        self.assertTrue(self.call(p, 'stop')['ok'])
        self.assertEqual(p.wait(timeout=5), 0)

    def test_async_exception_and_protocol_rejection(self):
        p = self.start('dlq', 'ExtensionFixture')
        self.assertFalse(self.call(p, 'enqueue', group='fail', topic_id='00112233-4455-6677-8899-aabbccddeeff', partition=3, first=0, last=1, delivery_count=1, cause='rejected')['ok'])
        self.assertFalse(self.call(p, 'nonsense')['ok'])

    def test_permanent_storage_failure(self):
        p = self.start('storage', 'ExtensionFixture$PermanentStorageFixture')
        self.assertTrue(self.call(p, 'configure', config={})['ok'])
        response = self.call(p, 'set', group='g', epoch=1, description=dict(subtopologies=[], global_stores=[]))
        self.assertFalse(response['ok'])
        self.assertTrue(response['permanent'])
        self.assertTrue(self.call(p, 'stop')['ok'])
        self.assertEqual(p.wait(timeout=5), 0)

    def test_caused_permanent_and_transient_storage_failures(self):
        p = self.start('storage', 'ExtensionFixture$PermanentStorageFixture')
        for group, permanent in [('caused', True), ('transient', False)]:
            with self.subTest(group=group):
                response = self.call(p, 'set', group=group, epoch=1, description=dict(subtopologies=[], global_stores=[]))
                self.assertFalse(response['ok'])
                self.assertEqual(response['permanent'], permanent)
