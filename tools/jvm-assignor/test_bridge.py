import asyncio
import os
from pathlib import Path
import unittest
from unittest.mock import patch
from bridge import assign, BridgeError

ROOT = Path(__file__).parent
CP = str(ROOT / 'build/worker') + ':' + os.environ['KAFKA44_LIBS'] + '/*:' + str(ROOT / 'build/fixture.jar')
REQUEST = {'version': 1, 'members': [
    {'member_id': 'a', 'process_id': 'p1', 'task_offsets': {'sub': {'0': 9223372036854775807}}},
    {'member_id': 'z', 'process_id': 'p2'}],
    'input': {'tasks': {'sub': [0, 1]}, 'stateful': ['sub'], 'num_standby_replicas': 0}}

class BridgeTests(unittest.IsolatedAsyncioTestCase):
    async def test_first_last_jar_and_conversion(self):
        for mode, owner in [('first', 'a'), ('last', 'z')]:
            os.environ['JVM_FIXTURE_MODE'] = mode
            result = await assign(CP, 'Fixture', REQUEST)
            self.assertEqual(result['active'], {owner: {'sub': [0, 1]}})

    async def test_exception_death_invalid_output(self):
        for mode in ['throw', 'exit', 'unknown', 'flood', 'closefail']:
            os.environ['JVM_FIXTURE_MODE'] = mode
            with self.assertRaises(BridgeError):
                await assign(CP, 'Fixture', REQUEST, limit=16384)

    async def test_released_sticky_assignor(self):
        result = await assign(CP, 'org.apache.kafka.coordinator.group.streams.assignor.StickyTaskAssignor', REQUEST)
        self.assertEqual(result['name'], 'sticky')
        self.assertEqual(sum(len(ps) for tasks in result['active'].values() for ps in tasks.values()), 2)

    async def test_deadline(self):
        os.environ['JVM_FIXTURE_MODE'] = 'hang'
        with self.assertRaises(TimeoutError):
            await assign(CP, 'Fixture', REQUEST, timeout=.2)

    async def test_cancellation_reaps(self):
        os.environ['JVM_FIXTURE_MODE'] = 'hang'
        processes = []
        spawn = asyncio.create_subprocess_exec
        async def capture(*args, **kwargs):
            process = await spawn(*args, **kwargs)
            processes.append(process)
            return process
        with patch('bridge.asyncio.create_subprocess_exec', capture):
            task = asyncio.create_task(assign(CP, 'Fixture', REQUEST))
            await asyncio.sleep(.2)
            task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await task
        self.assertEqual(len(processes), 1)
        self.assertIsNotNone(processes[0].returncode)

    async def test_bad_version(self):
        with self.assertRaises(BridgeError):
            await assign(CP, 'Fixture', dict(REQUEST, version=99))

if __name__ == '__main__':
    unittest.main()
