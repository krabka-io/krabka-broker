# Changelog

Krabka versions and tags the whole workspace as one unit, so this crate has no
release of its own. The [root changelog](../../CHANGELOG.md) records every
release that contains `krabka-macros`.

## [Unreleased]

### Changed (breaking)

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

- The unused `duration_cases_fixture!` macro. No workspace crate invoked it.
  Removing a public macro breaks the crate's API, which the 1.2.0 minor version
  below already covers.
- The single-use test fixtures `compaction_record_fixture!` and
  `format_directories_fixture!`. The one caller of each now holds the
  function as plain Rust.
- The macros whose expansions name `krabka-broker` modules, and the fixtures
  only the broker's tests call, are now in the unpublished
  `krabka-broker-macros` crate and expand to the same code there. Removing them
  from this crate breaks its API. The 115 macros are `acknowledgement_batches!`,
  `assignment_dirs_fixture!`, `async_write_delegate!`, `BackgroundControllers`,
  `bound_start_fixture!`, `broker_config!`, `broker_test_setup!`,
  `config_alter_handler!`, `#[config_table]`, `consumer_fetch_fixture!`,
  `controller_request!`, `create_topic_fixture!`, `#[decode_dispatch]`,
  `decoded_dispatch!`, `#[delegate_authorizer]`, `delete_topic_request!`,
  `described_group_rows!`, `dispatch_table!`, `empty_match_acl_filter!`,
  `epoch_checkpoint_failure!`, `evicted_local_segments!`, `#[extension_wire]`,
  `fixture_constructors!`, `#[flexible_value]`, `FlexibleArrayValue`,
  `FlexibleEpochValue`, `#[flush_group_records]`, `forwardable_requests!`,
  `frame_prefix_fixture!`, `GroupLifecycle`, `GroupRecordKey`,
  `GroupTimingConfig`, `heartbeat_status!`, `#[human_units]`,
  `hydrate_member_epochs!`, `java_string_producer_source!`,
  `jvm_principal_golden!`, `keyed_record_batch_fixture!`,
  `member_identity_fixtures!`, `member_key_boundaries!`,
  `#[membership_defaults]`, `metric_registry_fixture!`,
  `#[missing_remote_reads]`, `model_properties!`, `model_runner!`,
  `ModelEpochLedger`, `named_acl_gates!`, `NodeEndpoints`, `offset_fetch_rows!`,
  `partition_gate!`, `#[partition_spawn_parameters]`,
  `patterned_bytes_fixture!`, `private_dispatch_table!`, `projection_identity!`,
  `#[reaper_task]`, `reassignment_fields!`, `rebuild_reconciliation_group!`,
  `reconciliation_actions!`, `#[reconciliation_fields]`,
  `reconciliation_next_state!`, `ReconciliationAction`, `ReconciliationGroup`,
  `ReconciliationMember`, `record_header_text!`, `record_strings!`,
  `records_payload_match!`, `refused_fetch_partition_fixture!`,
  `RegisterMetrics`, `ReplayMemberMaps`, `replication_target_guard!`,
  `request_frame_fixture!`, `request_test_identity!`, `required_topics!`,
  `response_errors!`, `#[runtime_policy_fields]`, `RuntimeOverlay`,
  `schedule_commit_changes!`, `ScheduledLoads`, `scram_client_first!`,
  `scram_client_proof_fixture!`, `scram_users_fixture!`, `sendfile_match!`,
  `sendfile_platform!`, `set_runtime!`, `share_consumption_driver!`,
  `share_dlq_meters!`, `share_first_fetch!`, `share_fixture_schemas!`,
  `share_group_denial!`, `share_partition_results!`,
  `#[share_produce_parameters]`, `share_refusal_matrix!`,
  `#[share_request_identity]`, `share_state_handler!`,
  `#[share_state_operation]`, `share_state_request!`, `share_state_results!`,
  `ShareStateValue`, `single_partition_produce_fixture!`,
  `snapshot_member_maps!`, `string_mutations!`, `sweep_loop!`, `tagged_wire!`,
  `test_authorizer!`, `throttle_probes!`, `#[token_dispatch]`,
  `token_refusal_result!`, `#[topic_filter]`, `UnitSchema`, `unthrottled_wire!`,
  `#[voter_admission]`, `wire_codec_helpers!`, `wire_codes!`,
  `wire_test_driver!`, `WireStateBatch`.
- The `#[bootstrap_cli_fields]` attribute and the `connect_cli_client!` macro. The operator CLIs, their only users, now flatten the `ConnectionArgs` flag group of the workspace-private `krabka-cli-connection` crate. Removing public macros breaks the crate's API, so the release that carries this is at least a minor version, 1.2.0.
