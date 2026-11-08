# Changelog

Krabka versions and tags the whole workspace as one unit, so this crate has no
release of its own. The [root changelog](../../CHANGELOG.md) records every
release that contains `krabka-remote-storage`.

## [Unreleased]

### Changed

- At `krabka.version` 3 the S3 and GCS backends write tiered segment keys
  under the configured prefix trimmed of surrounding whitespace and `/`, so
  ` tier/ ` writes `tier/…`. Below that level they keep writing the prefix as
  configured, byte for byte. `KeyPrefixGate` and `KeyPrefixForm` carry the
  choice, and `S3RemoteStorage::with_key_prefix_gate` installs it. Fetches and
  deletes find a segment under either spelling at every level.
- `VerifyRequest::prefix` and `krabka-worm-verify --prefix` read the archive
  under both spellings of the prefix, and verify a partition whose chain is
  split across them as one chain, reported below the trimmed prefix.

The history of this crate before krabka-broker became its own repository is in
[robot-head/crabka](https://github.com/robot-head/crabka).
