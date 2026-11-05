# Released4.4 graduation inputs (not activation)

`ReleasedCeilings.java` executes the immutable4.4 `ApiKeys`, `Feature` and
`MetadataVersion` bytecode with unstable versions disabled. The
[retained output](../../qualification/kafka44-released-ceilings-20261105.json)
derives91 request schemas and these seven changes from the existing4.3 table:

| API | Existing maximum | Released4.4 maximum |
| --- | --- | --- |
| ApiVersions18 | 4 | 5 |
| TxnOffsetCommit28 | 5 | 6 |
| DeleteGroups42 | 2 | 3 |
| StreamsGroupHeartbeat88 | 0 | 1 |
| StreamsGroupDescribe89 | 0 | 1 |
| StreamsGroupTopologyDescriptionUpdate93 | absent | 0 |
| UnregisterController94 | absent | 0 |

`InitProducerId22` remains maximum5. Enabling experimental6 is not part of
release graduation. Preserve intentional legacy request acceptance and the
Produce schema minimum3 versus advertised broker minimum0 distinction. Schema
listener scope is not proof of live client-listener advertisement: listener
role, authentication and finalized-feature policy also matter.

The released registry's production metadata ceiling is4.4-IV2/33. Registry
feature ceilings/fresh defaults at metadata33 are kraft1, transaction2, group1,
eligible-leader-replicas1, share2, streams1. Their lower levels and dependencies
are retained raw in the receipt; these are supported/default policy inputs,
**not instructions to rewrite existing finalized features**.

## Coordinated activation boundary

Issue[1970](https://github.com/krabka-io/krabka-broker/issues/1970) explicitly
requires accepted qualification before activating new production defaults.
This derivation does not satisfy that prerequisite. No runtime gate changes
are included here. After acceptance, implementation must keep these surfaces
consistent:

| Surface | Existing source owner |
| --- | --- |
| Released API ceilings and request gating | `crates/raft/src/config/kafka_release.rs`, `routing.rs` |
| Broker/controller advertisements | `crates/broker/src/api_catalog.rs`, both `api_versions.rs` handlers |
| Supported feature ceilings and registrations | raft `routing.rs`, broker `broker/registration.rs` |
| Feature update admission and startup | broker `handlers/update_features`, raft `controller/feature_check.rs` and startup |
| Fresh format versus persisted existing levels | `crates/format/src/format/features.rs`, controller activation/replay |
| Released configuration discovery/validation | broker `config_keys`, `handlers/describe_configs` |

Retest fresh defaults without unstable flags, previous-release rolling
upgrade/mixed-node intersection, explicit finalization, same-data recovery and
rollback refusal limits. Existing enabled-policy upgrade receipts are valuable
inputs, not execution of a changed default policy. Keep genuinely unreleased
APIs/configuration gated. Do not silently finalize persisted levels.

## Reproduce the release extraction

```text
javac --release 17 -cp "PINNED_LIBS/*" -d CLASSES tools/kafka44/ReleasedCeilings.java
java -cp "CLASSES:PINNED_LIBS/*" ReleasedCeilings
```

Use libraries copied from the immutable image recorded in the receipt, not a
newer installed Kafka distribution. Source/jar hashes and complete output are
retained. These library-only observations are separated from broker runtime
qualification and never set `qualification_pass=true`.
