# Persisted unavailable custom assignor after clean restart

The paired [receipt](receipt.json) records **11 matching scenario rows per broker**
(four prepare, seven verify) from real broker-wire execution. The native binary
was built from `c51d444b3` with no runtime source modifications. The oracle reports
Kafka 4.4.0, commit `1156b2752aaa28ea`; its exact local image digest and the released
client codec JAR hash are pinned in the receipt. This is a fresh run, not reuse
of the Nov 5 wire receipt.

## What was measured

1. Register `first,last,failure,sticky`; create a two-partition source topic.
2. Persist group override `streams.assignor.name=last`; execute the callback,
   assign both tasks to member `b`, and complete cooperative reconciliation.
3. Explicitly leave both members, retaining the empty group, topology and override.
4. Stop cleanly and reopen **the same data**, registering **only sticky**.
5. DescribeConfigs still returns the persisted override `last` before and after
   member `c` joins. Describe reports selected assignor `sticky`; both task IDs
   are targeted and actually assigned to `c`. The plugin is no longer registered.
6. Explicitly repair the override to `sticky`; another member joins successfully.

[Raw native logs](native-verify.log) and [raw oracle logs](oracle-verify.log)
retain every wire assertion. The [oracle startup/callback excerpt](oracle-server-excerpt.log)
shows registry removal across restart and the actual pre-restart callback.
Native pre/post server logs record startup and callback; the command below
selects its opt-in registry before restart and omits it afterward. Broker IDs,
epochs, timestamps and unrelated fields are not asserted byte-identical.

This closes the **empty-member, clean single-broker unavailable-name fallback**
lane. It does not establish active-member restart, RF3 coordinator movement,
invalid-output wire behavior, ACL failures, crash durability, or JVM plugin
loading in Krabka. No runtime fix was needed. An empty group is deliberate:
member expiry/recovery and plugin removal are separate scheduling dimensions.

## Reproduction

From repository root, choose a new caller-owned private absolute `RUN`, `LIBS`
containing the released Kafka 4.4 JARs, and a caller-owned Cargo target directory.
Ports 64940/64941 and 64944/64945 must be free; change paired properties together
if needed. Do not reuse production data. All build/temp directories are caller-owned.

```sh
mkdir -p "$RUN/classes" "$RUN/oracle" "$RUN/tmp"
TMPDIR="$RUN/tmp" CARGO_TARGET_DIR="$TARGET" cargo build -p krabka-broker --example streams_fault_broker
BIN="$TARGET/debug/examples/streams_fault_broker"
javac -cp "$LIBS/*" -d "$RUN/classes" tools/kafka44/ProtocolScenarios.java tools/kafka44/StreamsPositive.java tools/kafka44/StreamsAssignorWire.java tools/kafka44/AssignorPlugin.java tools/kafka44/StreamsAssignorRestart.java
cp qualification/streams-assignor-restart-20261107/broker-before.properties "$RUN/oracle/broker.properties"
```

The measured oracle was a host-network Docker container with mounts
`$RUN/oracle:/evidence` (writable) and `$RUN/classes:/classes:ro`, user matching the
host owner, environment `LOG_DIR=/evidence/logs`, image
`apache/kafka@sha256:833b867a9585a141da455dc2b57431c65cbccb27f11c91f694444cca9f777e0f`,
and `/bin/bash -c` entrypoint command (format **only on the initial start**):

```sh
export CLASSPATH=/classes
/opt/kafka/bin/kafka-storage.sh format --standalone --cluster-id MkU3OEVBNTcwNTJENDM2Qk --release-version 4.4-IV2 --feature streams.version=1 --config /evidence/broker.properties
exec /opt/kafka/bin/kafka-server-start.sh /evidence/broker.properties
```

Start the native broker separately, then run the prepare probes:

```sh
STREAMS_FIXTURE_ASSIGNORS=1 "$BIN" "$RUN/native-data" "$RUN/native-flags" 127.0.0.1:64944
java -Djava.io.tmpdir="$RUN/tmp" -cp "$LIBS/*:$RUN/classes" StreamsAssignorRestart 127.0.0.1:64940 fresh prepare
java -Djava.io.tmpdir="$RUN/tmp" -cp "$LIBS/*:$RUN/classes" StreamsAssignorRestart 127.0.0.1:64944 fresh prepare
```

Stop the native process with SIGINT and wait for exit (measured exit zero). Stop
Kafka cleanly (measured Docker stop; server log records shutdown completion).
Replace oracle properties with `broker-after.properties`; restart the same
container/data using only the server-start command, **not storage format**.
Restart the native broker with `STREAMS_FIXTURE_ASSIGNORS` absent:

```sh
env -u STREAMS_FIXTURE_ASSIGNORS "$BIN" "$RUN/native-data" "$RUN/native-flags" 127.0.0.1:64944 rejoin
java -Djava.io.tmpdir="$RUN/tmp" -cp "$LIBS/*:$RUN/classes" StreamsAssignorRestart 127.0.0.1:64940 fresh verify
java -Djava.io.tmpdir="$RUN/tmp" -cp "$LIBS/*:$RUN/classes" StreamsAssignorRestart 127.0.0.1:64944 fresh verify
```

All four probe invocations must exit zero. Compare their `SCENARIO` rows against
the receipt (prepare four and verify seven each); raw override `last` and selected
assignor `sticky` are intentionally different after restart. Fixture data and
client groups remain caller-owned; no automatic destructive cleanup occurs.
