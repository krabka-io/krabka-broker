# Released4.4 supported defaults

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

The [bounded qualification suite](../../qualification/kafka44-acceptance-20261105.json)
accepts issue1969's pinned executions and explicitly bounded scenarios. This
policy uses its released ceilings, not the maximum implemented trunk schemas.
The [default-policy receipt](../../qualification/kafka44-defaults-20261105.json)
records fresh operation without unstable switches, preserved finalized features
on existing data, explicit finalization, restart and rollback boundaries.
These surfaces share the released policy:

| Surface | Existing source owner |
| --- | --- |
| Released API ceilings and request gating | `crates/raft/src/config/kafka_release.rs`, `routing.rs` |
| Broker/controller advertisements | `crates/broker/src/api_catalog.rs`, both `api_versions.rs` handlers |
| Supported feature ceilings and registrations | raft `routing.rs`, broker `broker/registration.rs` |
| Feature update admission and startup | broker `handlers/update_features`, raft `controller/feature_check.rs` and startup |
| Fresh format versus persisted existing levels | `crates/format/src/format/features.rs`, controller activation/replay |
| Released configuration discovery/validation | broker `config_keys`, `handlers/describe_configs` |

Fresh formatting chooses metadata33 (`4.4-IV2`), share2 and the other released
feature defaults above. Supporting these levels does not rewrite a running
cluster's finalized feature image: prior-release data retains metadata30/share1
until an operator explicitly upgrades it. Mixed nodes must all support a
requested level before the controller admits that update.

The prior binary can reopen unchanged metadata30 data after a new binary has
read and written it. Once metadata33 is explicitly finalized, that prior binary
refuses startup because its supported ceiling is30. This is an enforced rollback
limit, not evidence that every feature downgrade is reversible. Metadata34 and
`InitProducerId`6 remain unstable. Intentional legacy-version acceptance keeps
its existing independent setting.

The retained fixture's former metadata33-refusal expectation is historical:
33 is now released and supported. `UpgradeProbe` rejects genuinely unreleased34
without mutation, then checks the safe unchanged-feature rollback. The first
old-fixture run's successful explicit33 finalization and old-binary refusal are
retained separately, rather than erased or counted as a passing whole task.

## Reproduce the release extraction

```text
javac --release 17 -cp "PINNED_LIBS/*" -d CLASSES tools/kafka44/ReleasedCeilings.java
java -cp "CLASSES:PINNED_LIBS/*" ReleasedCeilings
```

Use libraries copied from the immutable image recorded in the receipt, not a
newer installed Kafka distribution. Source/jar hashes and complete output are
retained. These library-only observations are separated from broker runtime
qualification and never set `qualification_pass=true`.
