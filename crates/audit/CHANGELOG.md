# Changelog

Krabka versions and tags the whole workspace as one unit, so this crate has no
release of its own. The [root changelog](../../CHANGELOG.md) records every
release that contains `krabka-audit`.

## [Unreleased]

### Added

- `TrustedKeys` holds more than one key. `TrustedKeys::insert`, `TrustedKeys::paired` and `FromIterator<(String, Vec<u8>)>` build a set keyed by `key_id`, so a partition whose checkpoints span a rotation of the audit key verifies under the keys of both epochs.

### Changed

- `krabka-audit verify` takes `--key-id` and `--public-key` once per trusted key, paired by position, and exits `1` when their counts differ. It took exactly one key, so a partition signed across a key rotation could not verify.

The history of this crate before krabka-broker became its own repository is in
[robot-head/crabka](https://github.com/robot-head/crabka).
