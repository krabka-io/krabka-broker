# Isolated Java storage and custom DLQ extension qualification

Implements the previously absent persistent Java topology storage and custom
no-argument Java DLQ-manager embedding adapters. The [support contract](../../docs/compatibility/kafka-4.4-jvm-extensions.md)
is explicit; this is not a universal Kafka/JVM compatibility claim.

## Reproduce

Use the pinned released Kafka 4.4 libraries and JDK 21+:

```sh
export KAFKA44_LIBS=/absolute/path/to/kafka-4.4/libs
bash tools/jvm-assignor/check.sh
export KAFKA44_JVM_CLASSPATH="$PWD/tools/jvm-assignor/build/worker:$KAFKA44_LIBS/*:$PWD/tools/jvm-assignor/build/fixture.jar"
cargo test -p krabka-broker --lib jvm_extensions -- --include-ignored
cargo clippy -p krabka-broker --lib --no-deps
```

[Released signatures](released-signatures.txt) are obtained from the local
released distribution with javap, not inferred from config names.

## Evidence

- [Java checks](java-output.txt): 10 test methods, including the existing 6
  assignor checks and 4 persistent-extension checks. The released in-memory
  topology plugin roundtrips source/processor/sink/global stores, nullable sink
  topic, successors and epoch mismatch; delete is idempotent. Real custom
  manager JAR checks exact signed-64-bit offsets above 2^53, UUID and partition,
  both released cause constants, retained state and stop. Async/permanent
  exceptions and invalid operations are exercised.
- [Rust checks](rust-output.txt): **5 passed, 0 failed**, real process RPC through native storage and DLQ
  traits, startup class failure, permanent rejection, close exception,
  deadline/cancellation fencing, and stop awaiting /proc-observed child reaping.
- [Combined broker suite](broker-suite-output.txt): **9,157 passed, 0 failed, 7 ignored** in 115.42s.
- [Streams including released JAR checks](streams-output.txt): **301 passed, 0 failed, 0 ignored** in 4.42s. Together with the 5 new JVM checks, all 9,164 listed broker-library tests were exercised (overlapping rows are not double-counted).
- [Clippy](clippy-output.txt): **passed**, broker library, not whole-workspace/all-targets.
  Prior assignor lint failures were fixed without relaxing repository lints.
- [Initial Rust failure](rust-initial-output.txt): 4/5 passed; the permanent
  rejection regression detected a transient mapping in the initially compiled
  version. It was corrected, not ignored or weakened.

No live distributed/restart qualification or stock Java DLQ service wiring is
claimed by these tests. The native topic-name omission is explicit. Storage
plugins must supply their own cluster-wide durability; local in-memory state
cannot survive process loss. Distributed ownership fencing, physical power loss,
full assignor input fidelity and automatic deployment remain separate scope.

No publication, push, merge or production change was performed.
