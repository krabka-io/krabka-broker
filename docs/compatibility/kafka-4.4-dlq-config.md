# Kafka 4.4 share-group DLQ broker settings

Scope: issue [#1973](https://github.com/krabka-io/krabka-broker/issues/1973).
This qualifies the configuration surface, not custom JVM DLQ-manager loading
or oversized-record behavior.

## Released definitions

The reference is Apache Kafka **4.4.0**, source tag commit
`1156b2752aaa28eafc0b4c4d387e63356793268d`.
[GroupCoordinatorConfig](https://github.com/apache/kafka/blob/4.4.0/group-coordinator/src/main/java/org/apache/kafka/coordinator/group/GroupCoordinatorConfig.java#L330)
defines these two public, non-sensitive keys and includes both in
`RECONFIGURABLE_CONFIGS`:

| Key | Type | Default | Additional validator |
| --- | --- | --- | --- |
| `errors.deadletterqueue.auto.create.topics.enable` | BOOLEAN | `false` | none beyond boolean parsing |
| `errors.deadletterqueue.topic.name.prefix` | STRING | `dlq.` | none; empty string removes the prefix restriction |

Both support cluster-default and per-broker dynamic values. A named broker
reports its own override, then the cluster-default override, then the built-in
default in its synonym chain. The cluster-default resource reports only its
stored dynamic entries. These settings have no statically configurable Krabka
runtime field; the DLQ writer resolves the metadata-backed layers on each write.

Krabka retains its existing compatibility gates: these keys are typed,
discoverable and validated only with `unstable.api.versions.enable=true`.
Without that switch they remain unknown broker keys, matching the 4.3.1
baseline (unknown broker keys may be stored but have withheld describe values).
Actual DLQ delivery still requires `share.version=2`, and group/topic DLQ keys
still require the unstable API switch. This change does not graduate those gates.

## Live released-broker checks

On 2026-11-01, a fresh `apache/kafka:4.4.0` container was checked using its stock
`kafka-configs.sh`, talking to its own broker over loopback. Image digest:
`sha256:833b867a9585a141da455dc2b57431c65cbccb27f11c91f694444cca9f777e0f`.

The following were observed:

- `--entity-type brokers --entity-name 1 --describe --all` lists both defaults,
  `sensitive=false`, with `DEFAULT_CONFIG` synonyms.
- Setting cluster defaults to `true` / `cluster.` and broker overrides to
  `false` / empty string succeeds. Describe reports the broker values with
  broker, cluster-default and built-in synonyms in that order.
- Setting auto-create to `yes` fails with `InvalidRequestException` and
  `Expected value to be either true or false`.
- Deleting both per-broker overrides reveals `true` / `cluster.` and the
  two remaining synonym layers.
- Restarting the same container preserves the cluster defaults and synonyms.
- Deleting the cluster-default overrides restores `false` / `dlq.`.

Useful reproduction commands (substitute the broker address and id):

```sh
kafka-configs.sh --bootstrap-server localhost:9092 --entity-type brokers \
  --entity-default --alter --add-config \
  errors.deadletterqueue.auto.create.topics.enable=true,errors.deadletterqueue.topic.name.prefix=cluster.
kafka-configs.sh --bootstrap-server localhost:9092 --entity-type brokers \
  --entity-name 1 --alter --add-config \
  errors.deadletterqueue.auto.create.topics.enable=false,errors.deadletterqueue.topic.name.prefix=
kafka-configs.sh --bootstrap-server localhost:9092 --entity-type brokers \
  --entity-name 1 --describe --all
```

The regression suite checks typed discovery and synonyms, scalar-only alters,
invalid booleans, both precedence layers and resets, metadata replay, and the
existing DLQ prefix, automatic-creation and feature-gate paths. Metadata replay
is not a full Kafka-to-Krabka migration or a full released-4.4 interoperability
suite.
