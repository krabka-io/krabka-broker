# Kafka 4.4 bounded qualification closeout

Tracking issue [#1968](https://github.com/krabka-io/krabka-broker/issues/1968)
permits implemented-and-verified capabilities **or explicitly recorded remaining
limitations**. Completion here is that bounded outcome, not full Kafka parity,
a release publication, or a production deployment.

## Completion evidence

| Obligation | Landed evidence |
| --- | --- |
| Released differential/interoperability suite (#1969) | [Acceptance receipt](../../qualification/kafka44-acceptance-20261105.json), [six-surface ledger](COVERAGE.md); #1994. Historical runtime source/binary identities remain pinned. |
| Released defaults (#1970) | [Default-policy receipt](../../qualification/kafka44-defaults-20261105.json), [ceilings and rollback boundary](GRADUATION.md); #1995. Fresh metadata33/share2; existing finalized levels unchanged until explicit operator action. |
| Native Streams assignors (#1971) | #1978; [native evidence and released-component probe](../../qualification/streams-assignor-native.md), [embedding contract](../../docs/streams-task-assignors.md). Distinct callbacks, selection, reassignment, invalid output and failure recovery are covered by native behavioral tests/models. The released registry probe executes actual 4.4 JAR callbacks; the [bounded wire follow-up](STREAMS_ASSIGNOR_WIRE.md) additionally records seven matching custom-callback selection/reassignment/error/retry results per broker. |
| Topology storage/lifecycle (#1972) | #1977 and #1989; [native evidence](../../qualification/streams-topology-native.md), [durable backend receipt](../../qualification/kafka44-streams-durable-20261104.json), [minimum feature](../../qualification/kafka44-streams-minimum-20261104.json), [RF3 owner movement](../../qualification/kafka44-streams-movement-20261104.json). |
| DLQ discovery/validation (#1973) | #1976; [released config receipt](../../qualification/kafka44-dlq-config-20261105.json). |
| DLQ boundaries, crash semantics and native manager (#1974/#1982) | #1979; [boundary/metrics/state/crash evidence](../../qualification/kafka44-dlq/README.md), [native manager contract and lifecycle tests](../../docs/compatibility/kafka-4.4-native-dlq-manager.md). |
| Support classification | Generated [KIP matrix](../../docs/KIP_MATRIX.md): KIP-1191 and KIP-1331 remain Partial. KIP-1357's Implemented status covers name reporting, not arbitrary plugin compatibility. |

## Retained limitations

- Native extensions are Rust embedding APIs, not JVM class/JAR loading, a stable
  shared-library ABI, or executable plugin hot-loading. Callback IO, cancellation
  cooperation and panic isolation are not proved by pure kernels or actor models.
- Custom-assignor single-broker callback wire selection, reassignment, error and
  retry now have a paired receipt. RF3 movement, combined ACL failures, invalid
  output at the wire boundary and unavailable-name restart remain unmeasured.
- Topology storage movement covers the measured shared-backend empty-member case,
  not every active-member/concurrent-write schedule. [Native callback telemetry and bounded periodic cleanup/handoff tests](../../qualification/streams-storage-native-followup-20261105.md)
  now cover local behavior, not distributed scheduling or arbitrary owner overlap. A local filesystem is not automatically
  shared across hosts; fsync/power-loss guarantees are not established.
- Historical DLQ receipts measured a terminal-prefix persistence difference.
  The [ordering fix](../../qualification/kafka44-dlq/README.md#terminal-prefix-ordering-follow-up-2026-11-05) now persists ARCHIVED before live-cache pruning, with
  state-machine and actual-manager regressions. A [fresh paired run](../../qualification/kafka44-dlq/ordering-20261107/README.md) measures eight boundary cases per broker with matching persisted state 3 then 4, startOffset 0 and deliveryCompleteCount 1 before and after clean restart; this is not a crash-window or RF3 state-parity claim. Terminal errors can archive without a DLQ
  copy; lost confirmations can create duplicates. No exactly-once archive claim.
- RF3 destination authorization denial and every Cartesian-product fault/ACL
  permutation remain unmeasured. Bounded model/proof results do not establish
  arbitrary network schedules or plugin IO durability.
- Historical failures are preserved. The later scheduled Jepsen36/36 valid run
  is subsequent regression evidence, not an exact replay of every earlier fault.
- Upgrade/restart/mixed-version evidence retains its actual revisions. Prior
  binaries can reopen unchanged metadata30 data, but refuse explicitly finalized
  metadata33; no blanket downgrade reversibility is promised.

Java-client-only, Streams-library-only, Connect and shell-tool changes are not
broker obligations. The original documentation closeout implied no new runtime execution; subsequent
implementation follow-ups retain their separate verification scope. Follow each linked receipt for its commands, identities and bounds.
