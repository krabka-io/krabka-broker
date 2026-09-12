# krabka-oracle

A JVM differential-test oracle. It answers Kafka wire questions with Apache
Kafka's own `kafka-clients` code.

Part of [Krabka](../../README.md), a Rust implementation of Apache
Kafka-compatible infrastructure and clients.

## Overview

The oracle is a small Java program. It reads one JSON request for each line of
standard input, and it writes one JSON response for each line of standard
output. Each request asks the `kafka-clients` jar to encode or to decode a
value. The Rust side then compares its own bytes against the answer.

A hand-written expectation can be wrong about Kafka. The oracle cannot: it
calls the same classes a real Kafka client calls.

The program supports these operations:

| Operation | What it does |
| :--- | :--- |
| `encode`, `decode` | A request or response message, through Kafka's generated `*JsonConverter` classes. |
| `header_encode`, `header_decode` | A `RequestHeader` or a `ResponseHeader`. |
| `record_batch_encode`, `record_batch_decode` | A v2 record batch, through `MemoryRecords`. |
| `compress`, `decompress` | One buffer, with the `gzip`, `snappy`, `lz4`, or `zstd` codec. |

The `kafka-clients` version in [`build.gradle.kts`](build.gradle.kts) must
equal the Kafka release that the repository tests against. That release is
`apache/kafka:4.3.1`. `MODULE.bazel` and
[`bazel/images/BUILD.bazel`](../../bazel/images/BUILD.bazel) pin the image, and
[`docs/KIP_MATRIX.md`](../../docs/KIP_MATRIX.md) names it.

## Build

You need a JDK 17. Set `JAVA_HOME` if the default JDK is a different release.
You do not need a system Gradle, because the wrapper is in this directory.

```bash
(cd tools/oracle && ./gradlew installDist)
```

Gradle installs the program under `tools/oracle/build/install/krabka-oracle/`.
The start scripts are `bin/krabka-oracle` and `bin/krabka-oracle.bat`. Gradle
writes both scripts on every platform. The jars are in `lib/`.

Gradle writes `build/` and `.gradle/` in this directory. They are build
output. Do not commit them.

## Who Runs It

The wire-level differential suites run the oracle. Those suites are in
[`krabka-protocol`](https://github.com/krabka-io/krabka-protocol), which owns
the protocol codec and the compression codecs. They start the program as a
child process, and they keep it alive for the whole suite.

This repository holds the broker. It has no consumer of this oracle today. Its
own JVM differential tests use a different oracle: a stock `apache/kafka:4.3.1`
broker in a container, driven through the release's own admin tools. See
[`crates/broker/tests/jvm_acceptance_cli/oracle.rs`](../../crates/broker/tests/jvm_acceptance_cli/oracle.rs).

## License

Apache-2.0. Derivative work of [Apache Kafka](https://kafka.apache.org); see
[NOTICE](../../NOTICE).
