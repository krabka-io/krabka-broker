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
| Native Streams assignors (#1971) | #1978; [native evidence and released-component probe](../../qualification/streams-assignor-native.md), [embedding contract](../../docs/streams-task-assignors.md). Distinct callbacks, selection, reassignment, invalid output and failure recovery are covered by native behavioral tests/models. The released registry probe executes actual 4.4 JAR callbacks, not a full custom-plugin broker-wire differential. |
| Topology storage/lifecycle (#1972) | #1977 and #1989; [native evidence](../../qualification/streams-topology-native.md), [durable backend receipt](../../qualification/kafka44-streams-durable-20261104.json), [minimum feature](../../qualification/kafka44-streams-minimum-20261104.json), [RF3 owner movement](../../qualification/kafka44-streams-movement-20261104.json). |
| DLQ discovery/validation (#1973) | #1976; [released config receipt](../../qualification/kafka44-dlq-config-20261105.json). |
| DLQ boundaries, crash semantics and native manager (#1974/#1982) | #1979; [boundary/metrics/state/crash evidence](../../qualification/kafka44-dlq/README.md), [native manager contract and lifecycle tests](../../docs/compatibility/kafka-4.4-native-dlq-manager.md). |
| Support classification | Generated [KIP matrix](../../docs/KIP_MATRIX.md): KIP-1191 and KIP-1331 remain Partial. KIP-1357's Implemented status covers name reporting, not arbitrary plugin compatibility. |

## Retained limitations

- Native extensions are Rust embedding APIs, not JVM class/JAR loading, a stable
  shared-library ABI, or executable plugin hot-loading. Callback IO, cancellation
  cooperation and panic isolation are not proved by pure kernels or actor models.
- A full released-broker custom-assignor wire/reassignment/error/authorization and
  coordinator-movement differential is not retained. Released-component execution
  and native actor/model coverage must not be described as that missing run.
- Topology storage movement covers the measured shared-backend empty-member case,
  not every active-member/concurrent-write schedule. Cleanup scheduling and native
  plugin metrics remain unqualified. A local filesystem is not automatically
  shared across hosts; fsync/power-loss guarantees are not established.
- DLQ terminal-prefix persistence differs: Kafka retains state4/start0/count1,
  while Krabka prunes to start1/count0. Terminal errors can archive without a DLQ
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
broker obligations. No new runtime execution is implied by this documentation
reconciliation; follow each linked receipt for its commands, identities and bounds.
