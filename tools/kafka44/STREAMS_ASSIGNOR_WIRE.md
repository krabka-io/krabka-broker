# Custom Streams assignor broker-wire differential

`StreamsAssignorWire` drives the same released Kafka 4.4 codec against a real
Kafka broker loading `AssignorPlugin` and a real Krabka broker embedding the
matching Rust fixture. It checks the actual callback's two-task placement, not
just KIP-1357's reported name. The Java implementations use Kafka's JVM SPI;
the native equivalents use Rust's embedding SPI. Neither loads the other's ABI.

The Nov 5 bounded [receipt](../../qualification/streams-assignor-wire-20261105/receipt.json)
records seven matching scenario results on each broker:

- First configured custom implementation assigns both tasks to member `a`.
- Group configuration selects `last`; its different callback targets member `b`.
- Members acknowledge revocations and acquire both tasks cooperatively at `b`.
- The selected `failure` implementation throws/returns an error on joining `c`:
  the wire response is `UNKNOWN_SERVER_ERROR`, and describe contains no `c`.
- Selecting healthy `first` lets the same `c` join successfully; target and actual
  assignments return both tasks to `a` through cooperative reconciliation.

The extra target-first row records the target before that repaired reconciliation.
Raw wire logs and actual broker callback traces are beside the receipt. Broker
host/client strings, group IDs, timing, member ordering and irrelevant response
fields are not asserted byte-identical. The test does assert exact task sets,
selected assignor, error code and failed-member absence. It does not directly
inspect the metadata log or replicate a callback failure between brokers.

## Reproduction

Use a caller-owned fixture directory on the workspace filesystem. `LIBS` must
contain the JARs extracted from the pinned image in the receipt. `RUN` is an
absolute new directory; `BIN` is the built example path. Do not point the fixture
at production data or an existing cluster. Example commands from repository root:

```sh
cargo build -p krabka-broker --example streams_fault_broker
mkdir -p "$RUN/classes" "$RUN/oracle"
javac -cp "$LIBS/*" -d "$RUN/classes" tools/kafka44/ProtocolScenarios.java tools/kafka44/StreamsPositive.java tools/kafka44/StreamsAssignorWire.java tools/kafka44/AssignorPlugin.java
cp qualification/streams-assignor-wire-20261105/broker.properties "$RUN/oracle/broker.properties"
```

Start the pinned Kafka image using host networking, mounting `$RUN/oracle` at
`/evidence` writable and `$RUN/classes` at `/classes` read-only. Its entrypoint is
`/bin/bash -c` with this exact command (the checked-in properties use ports
64820/64821; change them together if occupied):

```sh
export CLASSPATH=/classes
/opt/kafka/bin/kafka-storage.sh format --standalone --cluster-id MkU3OEVBNTcwNTJENDM2Qk --release-version 4.4-IV2 --feature streams.version=1 --config /evidence/broker.properties && exec /opt/kafka/bin/kafka-server-start.sh /evidence/broker.properties
```

In another terminal start the native broker. The opt-in environment flag installs
`first,last,failure` followed by the native registry's built-in sticky. The same
ordered registry is explicitly configured on the released broker. The existing
fault-backend harness still installs its healthy filesystem topology backend,
but this scenario never stores a topology description. The harness enables
unstable versions; this is callback qualification, not a default-policy check.

```sh
STREAMS_FIXTURE_ASSIGNORS=1 "$BIN" "$RUN/native-data" "$RUN/native-flags" 127.0.0.1:64832
```

After both brokers are ready, run with a fresh suffix each time:

```sh
java -cp "$LIBS/*:$RUN/classes" StreamsAssignorWire 127.0.0.1:64820 fresh-oracle
java -cp "$LIBS/*:$RUN/classes" StreamsAssignorWire 127.0.0.1:64832 fresh-native
```

Each must exit zero with the same seven `SCENARIO` records. The probe retains
its caller-owned groups/topics for inspection. No fixture cleanup is automatic.

## Remaining qualification

This closes the previously missing single-broker custom-callback wire,
selection, cooperative reassignment, callback-error and repair/retry lane.
It does **not** close the full custom-assignor gate: RF3 coordinator movement,
ACL failures combined with these callbacks, invalid-output wire behavior and
active-member persisted-unavailable-name fallback after restart remain unmeasured here.
The [Nov 7 paired restart receipt](../../qualification/streams-assignor-restart-20261107/README.md)
now measures the narrower empty-member clean-restart case: persisted `last`
remains visible while `sticky` runs after plugin removal, followed by explicit
override repair (11 matching scenario rows per broker).
Native actor/model coverage and the released component probe remain separate
supporting evidence, not substitutes for those executions.

## Unknown-member output follow-up (2026-11-08)

The [invalid-owner characterization](../../qualification/streams-assignor-invalid-20261108/README.md)
adds an actual malformed-output wire case. Kafka 4.4 accepts a joining member
when the custom callback returns tasks owned by a nonexistent member; native
validation rejects that output. This is a retained **divergence**, not an
invalid-output parity pass. Both modes exercise repair with a healthy callback.
RF3 movement, combined ACL failures and other invalid-output classes remain open.
