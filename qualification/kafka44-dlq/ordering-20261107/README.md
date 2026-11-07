# Fresh paired DLQ terminal ordering (2026-11-07)

The [receipt](receipt.json) and raw decoded state logs establish the subsequent
ordering fix against the pinned released Kafka 4.4 broker. Historical receipts
outside this directory are unchanged.

Both brokers pass eight identical real `ShareBoundary` cases: copied and
headers-only records with generous, one-byte-below, exact and one-byte-above
batch limits. Payloads and six provenance headers are asserted by the fixture.
The two below-limit cases yield no destination record in the fixture's
four-second observation window, but archive their source offsets, matching
released terminal-error semantics.

For **all eight groups on each broker**, released `DumpLogSegments` decodes
actual `__share_group_state` records as ARCHIVING(3), then ARCHIVED(4), retaining
startOffset=0 and deliveryCompleteCount=1. This representation survives a clean
whole-process restart. The real Admin probe reports startOffset=0 and zero lag
for all eight groups before and after restart on both brokers. Boundary and
Admin outputs are identical peer-to-peer. This supersedes the earlier measured
terminal-prefix representation difference for these executions only.

## Reproduction

Use fresh caller-owned private RUN directories and unoccupied ports; do not use
production data. Set `LIBS` to the directory containing the released 4.4 client
jar and its dependencies; the client jar hash is in the receipt. Set `RUN` to
the new private directory and create `$RUN/classes` before compiling. The run used the checked-in `oracle.properties` and
`candidate.toml` with paths rewritten under RUN. The candidate is a fresh locked
build from the integration worktree identified in the receipt, including current
uncommitted catalog/vendor formatting work; it is not described as a pristine
commit build. Neither endpoint uses unstable switches.

```sh
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo build --locked \
  -p krabka-broker --bin krabka-broker -p krabka-format --bin krabka-format
javac -cp "$LIBS/*" -d "$RUN/classes" qualification/kafka44-dlq/ShareBoundary.java
```

Run the pinned image in the receipt with host networking, RUN/oracle mounted
writable at `/evidence`, the current user's UID/GID, and `LOG_DIR=/evidence/logs`.
Format once with `/opt/kafka/bin/kafka-storage.sh format --standalone --cluster-id
MkU3OEVBNTcwNTJENDM2Qk --release-version 4.4-IV2 --config
/evidence/broker.properties`; start with `kafka-server-start.sh` on that config.

```sh
target/debug/krabka-format --log-dir "$RUN/candidate-data" --node-id 1 \
  --standalone --controller-listener 127.0.0.1:64903 --release-version 4.4-IV2
target/debug/krabka-broker --config-file "$RUN/candidate.toml" \
  --controller-listen-addr 127.0.0.1:64903 \
  --metrics-listen-addr none --health-listen-addr none
java -cp "$RUN/classes:$LIBS/*" ShareBoundary 127.0.0.1:64900 ord07
java -cp "$RUN/classes:$LIBS/*" ShareBoundary 127.0.0.1:64902 ord07
```

Use a fresh equal-length suffix for each rerun (names affect encoded size).
Run each peer again with the trailing `verify` argument. Decode each peer's
`__share_group_state-0/00000000000000000000.log` with:

```sh
java -cp "$LIBS/*" org.apache.kafka.tools.DumpLogSegments \
  --files "$SEGMENT" --share-group-state-decoder
```

Cleanly stop only the owned oracle container and candidate PID, restart on the
same data/configuration, then repeat `verify` and decoding. The retained logs
provide both before/after state rows, not merely low lag or source inference.

## Bounds

Single-node RF1/minISR1, completed-write clean restart, no injected crash,
concurrent-owner or destination ACL failure. No arbitrary schedule, power-loss,
fsync, exactly-once or JVM plugin equivalence claim. The older lost-confirmation
duplicate witnesses remain valid. Private build/setup logs and source inventory
are at the receipt's private artifact directory. Owned brokers were stopped
after collection; data and stopped containers are retained for inspection.
