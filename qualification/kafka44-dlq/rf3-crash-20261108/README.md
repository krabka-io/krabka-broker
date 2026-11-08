# Paired RF3 append-confirmation-loss source crash

Both retained native and pinned released Kafka 4.4 brokers pass the same fresh
three-node schedule. Source, destination and share-state partitions each have
three assigned replicas and minISR2. This is one record/partition per topic,
not exhaustive qualification.

1. Produce and acquire one source record, then explicitly reject it.
2. Hold the first actual DLQ Produce response at the loopback wire proxy.
3. Read exactly one intact destination copy. Decode the actual state log and
   require its latest group row to contain ARCHIVING(3).
4. In the same orchestration process, SIGKILL source broker1 and restart its
   existing disk. The response remains unconfirmed on that old connection.
5. Recovery share polls yield no source records during 15 seconds. Destination
   verification finds exactly two intact copies and the Admin query reports lag0.

Offline integration verification of all six final log dumps confirms their
latest group rows decode ARCHIVED(4), startOffset=0, deliveryCompleteCount=1.
The historical runner dumps these rows but does not assert them before PASS;
`results.json` records orchestration PASS, not this separate offline check. See [state summary](state-summary.json),
[receipt](receipt.json), and peer count/state/timeline/resume/verify files.
The native source inspection-to-kill-completion interval was about 4.3ms;
the oracle uses Docker SIGKILL with its timing recorded separately.

Supplemental `Rf3Inspect` Admin observations report replicas3/ISR3/minISR2 for
source, destination and share state on both peers. Native inspection followed
an additional clean whole-cluster restart; oracle inspection followed source
crash/recovery. These are **post-recovery** observations, not a pre-crash ISR3
assertion. All new owned processes and containers were stopped; data and stopped
containers are retained privately.

## Reproduction

The exact historical orchestration harness is retained privately at
`/home/matt/.openclaw/workspace/coordinator/.private/dlq-rf3-crash-20261108/run.py`
(SHA-256 `3c29570162b437ece132125610eee7eb2a569ca066876243c1717452f91e994c`).
It is evidence, not a tracked repository task. The command below describes its
historical invocation: the unchanged harness infers the repository from its own
location, so the private copy is not directly runnable without adapting that path.
No reproducible tracked task is claimed. With the original harness layout, use a
fresh nonexistent private RUN directory, the
released JDK25 jars, Docker access, and the exact retained binaries identified
in the receipt. Never use production endpoints or data.

```sh
python3 "$PRIVATE_HARNESS" \
  --run "$RUN" --libs "$LIBS" --broker "$BROKER" --formatter "$FORMATTER"
```

The runner creates fresh clusters on loopback ports64601–64620 and64701–64720;
it refuses occupied ports. It compiles the checked-in shared boundary/proxy
fixtures and generates `ShareFaultRf3.java` from the historical `ShareFault`
fixture, replacing its RF1/RF2 topology with RF3/minISR2 and explicitly creating
share state on replicas[3,1,2]. The exact generated source used is retained here.
It formats native dynamic initial voters and oracle static voters, creates the
response gate, executes both crash schedules and decodes each final replica.
It stops owned processes on either success or failure and retains all data.

`Rf3Inspect.java` is the supplemental topology probe; compile it against
`ShareBoundary` and released jars, then run `Rf3Inspect BOOTSTRAP` while the
owned cluster is running. The first run collected these queries separately
from `run.py`; primary PASS does not silently imply the supplemental query ran.
Private format/start/broker logs and generated configs are retained under the
receipt's private artifact directory.

## Limits

This qualifies **append succeeded, confirmation lost, source process killed**
with RF3/minISR2, demonstrating duplicates rather than exactly-once archival.
It does not inject a real phase-2 persister failure, power loss, fsync fault,
concurrent ACL denial, arbitrary partitions or arbitrary failure schedules.
The separate RF3 ACL-denial clean-restart and bounded phase-2 model receipts
remain separate evidence; they are not combined into an unexecuted schedule.
The candidate binary is the immutable retained RF3-denial build, not a rebuild
from the combined follow-up tree or a claim about current main. Full qualification
remains false.
