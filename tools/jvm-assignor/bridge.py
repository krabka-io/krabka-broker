"""Opt-in async, single-request Kafka 4.4 JAR assignor worker prototype."""
import asyncio
import json


class BridgeError(Exception):
    pass


def validate(result, request):
    """Never admit malformed worker output as a coordinator target."""
    if result.get('version') != 1:
        raise BridgeError('unsupported response version')
    members = {m['member_id']: m['process_id'] for m in request['members']}
    inp = request['input']
    universe = {(s, p) for s, ps in inp['tasks'].items() for p in ps}
    active, placements, counts = set(), set(), {}
    for role in ('active', 'standby'):
        for member, tasks in result[role].items():
            if member not in members:
                raise BridgeError('unknown assignment owner')
            for sub, ps in tasks.items():
                for p in ps:
                    task = (sub, p)
                    if type(p) is not int or task not in universe:
                        raise BridgeError('invalid task')
                    placement = (task, members[member])
                    if placement in placements:
                        raise BridgeError('duplicate task on process')
                    placements.add(placement)
                    if role == 'active':
                        if task in active:
                            raise BridgeError('duplicate active task')
                        active.add(task)
                    else:
                        counts[task] = counts.get(task, 0) + 1
                        if sub not in inp.get('stateful', []) or counts[task] > inp.get('num_standby_replicas', 0):
                            raise BridgeError('invalid standby task')
    if active != universe:
        raise BridgeError('missing active tasks')
    return result


async def assign(classpath, plugin, request, *, timeout=10, java='java', limit=1048576):
    """Cancellation/deadline kills and reaps this request's worker; no retries.

    classpath must include compiled AssignorWorker, Kafka 4.4 libraries and plugin
    JAR. JVM receives a 128 MiB heap cap. This is not a security sandbox.
    """
    payload = json.dumps(request).encode()
    if len(payload) > limit:
        raise BridgeError('request exceeds byte limit')
    process = await asyncio.create_subprocess_exec(
        java, '-Xmx128m', '-cp', str(classpath), 'AssignorWorker', plugin,
        stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE)

    async def read_bounded(stream):
        chunks, total = [], 0
        while chunk := await stream.read(16384):
            total += len(chunk)
            if total > limit:
                raise BridgeError('worker output exceeds byte limit')
            chunks.append(chunk)
        return b''.join(chunks)

    async def exchange():
        async def write():
            process.stdin.write(payload)
            await process.stdin.drain()
            process.stdin.close()
        _, stdout, stderr, _ = await asyncio.gather(
            write(), read_bounded(process.stdout), read_bounded(process.stderr), process.wait())
        if process.returncode:
            raise BridgeError(f'worker failed ({process.returncode}): {stderr.decode(errors="replace")[-4096:]}')
        try:
            return validate(json.loads(stdout), request)
        except (ValueError, KeyError, TypeError, AttributeError) as exc:
            raise BridgeError('malformed assignment response') from exc

    task = asyncio.create_task(exchange())
    try:
        return await asyncio.wait_for(task, timeout)
    finally:
        if process.returncode is None:
            process.kill()
        await process.wait()
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
