# Changelog

Krabka versions and tags the whole workspace as one unit, so this crate has no
release of its own. The [root changelog](../../CHANGELOG.md) records every
release that contains `krabka-audit`.

## [Unreleased]

### Added

- `TrustedKeys` holds more than one key. `TrustedKeys::insert`, `TrustedKeys::paired` and `FromIterator<(String, Vec<u8>)>` build a set keyed by `key_id`, so a partition whose checkpoints span a rotation of the audit key verifies under the keys of both epochs.
- `OcsfVersion` and `OcsfVersionSwitch` choose the OCSF record version the audit writer serializes with, and `AuditWriterParams::ocsf_version` hands the writer the switch. From `krabka.version` 3 a broker writes version 2 records: Application Lifecycle start 3 and stop 4, configuration change and TLS reload 99 (Other), with the broker named in `app`; ACL denial 99 (Other); admin operation API Activity 1 (Create), 3 (Update) or 4 (Delete) by Kafka api; and `"log_version": "2"` in every record's `metadata`. Below that level the broker writes the krabka 1.0.0 record, byte for byte.

### Changed

- `krabka-audit verify` takes `--key-id` and `--public-key` once per trusted key, paired by position, and exits `1` when their counts differ. It took exactly one key, so a partition signed across a key rotation could not verify.
- `to_ocsf` and `AuditRecord::from_event` take the `OcsfVersion` to write.

The history of this crate before krabka-broker became its own repository is in
[robot-head/crabka](https://github.com/robot-head/crabka).
