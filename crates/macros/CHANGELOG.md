# Changelog

Krabka versions and tags the whole workspace as one unit, so this crate has no
release of its own. The [root changelog](../../CHANGELOG.md) records every
release that contains `krabka-macros`.

## [Unreleased]

### Changed (breaking)

- `wire_test_driver!(socket ..)` generates a `bon` builder (`stream`,
  optional `api_key`, `version`, `correlation` and `header`, then `body`)
  that forwards to `kafka_wire::round_trip`, instead of a function taking a
  `SocketExchangeSetup`. The caller needs `bon` and `krabka_ids`. The
  `flexible` mode keeps its signature.
- `fixture_constructors!`'s `temporary_partition` and `open_partition`
  domains default their builder members directly (`STANDALONE_TOPIC`, the
  partition index's and storage mode's `Default`) instead of reading
  `StandalonePartitionSetup::default()`. A caller needs `STANDALONE_TOPIC` in
  scope instead of `StandalonePartitionSetup`.
- `single_partition_produce_fixture!` no longer generates
  `SinglePartitionProduceSetup`: the generated function is a `bon` builder
  over the same fields and defaults, and `.acknowledgements(AllReplicas)`
  replaces `SinglePartitionProduceSetup::replicated()`.
- `create_topic_fixture!` no longer generates `CreateTopicSetup` or
  `configured_topic_request_from`. Call the generated builder.
- `parsed_cli_entrypoint!` parses with `Cli::try_parse_from`, prints clap's
  message on a parse error, and returns the caller's `EXIT_USAGE`, which must
  be in scope. `--help` and `--version` still return `0`. It used to exit the
  process with clap's code `2`.
- `quorum_epoch_encoder!` now requires the declared function to return
  `Option<Bytes>`, not `Bytes`, and the generated body returns what the
  caller's `encode_body` returns. An encoder that fails no longer yields a
  truncated body.
- `guard_response!(client, request, err)` takes the stderr writer that a
  refusal is reported to, and passes it to the caller's `report_error` as its
  first argument.

### Removed

- The unused `duration_cases_fixture!`, `token_refusal_result!` and
  `share_refusal_matrix!` macros. No workspace crate invoked them. Removing
  public macros breaks the crate's API, which the 1.2.0 minor version below
  already covers.
- The single-use test fixtures `keyed_record_batch_fixture!`,
  `compaction_record_fixture!` and `format_directories_fixture!`. The one
  caller of each now holds the function as plain Rust.
- The `#[bootstrap_cli_fields]` attribute and the `connect_cli_client!` macro. The operator CLIs, their only users, now flatten the `ConnectionArgs` flag group of the workspace-private `krabka-cli-connection` crate. Removing public macros breaks the crate's API, so the release that carries this is at least a minor version, 1.2.0.
