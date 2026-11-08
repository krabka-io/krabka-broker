# Changelog

Krabka versions and tags the whole workspace as one unit, so this crate has no
release of its own. The [root changelog](../../CHANGELOG.md) records every
release that contains `krabka-broker-macros`.

## [Unreleased]

### Added

- The crate, with the macros that moved out of `krabka-macros` because their
  expansions name `krabka-broker` modules: the dispatch, coordinator,
  share-state, handler and fixture macros that only the broker invokes. Each
  expands to the same code as before.

### Changed

- `single_partition_produce_fixture!` no longer generates
  `SinglePartitionProduceSetup`: the generated function is a `bon` builder
  over the same fields and defaults, and `.acknowledgements(AllReplicas)`
  replaces `SinglePartitionProduceSetup::replicated()`.
- `create_topic_fixture!` no longer generates `CreateTopicSetup` or
  `configured_topic_request_from`. Call the generated builder.
- `wire_test_driver!(socket ..)` generates a `bon` builder (`stream`,
  optional `api_key`, `version`, `correlation` and `header`, then `body`)
  that forwards to `kafka_wire::round_trip`, instead of a function taking a
  `SocketExchangeSetup`. The `flexible` mode keeps its signature.
- `fixture_constructors!`'s `temporary_partition` and `open_partition`
  domains default their builder members directly (`STANDALONE_TOPIC`, the
  partition index's and storage mode's `Default`) instead of reading
  `StandalonePartitionSetup::default()`.

### Removed

- The unused `token_refusal_result!` and `share_refusal_matrix!` macros, and
  the single-use `keyed_record_batch_fixture!`, whose one caller now holds the
  function as plain Rust.

- `wire_codes!` rows in the `release` and `trunk` sections now name Kafka's
  default message, as `NAME = 3 => "message";`, and `private` rows name none.
  The caller's `NamedCode` gains a `message: Option<&'static str>` field, the
  caller defines `KafkaError { name, message }`, and the declaration emits a
  private `declared_kafka_error` lookup over the rows that carry a message.
