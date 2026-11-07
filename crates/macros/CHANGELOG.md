# Changelog

Krabka versions and tags the whole workspace as one unit, so this crate has no
release of its own. The [root changelog](../../CHANGELOG.md) records every
release that contains `krabka-macros`.

## [Unreleased]

### Changed

- `quorum_epoch_encoder!` now requires the declared function to return
  `Option<Bytes>`, not `Bytes`, and the generated body returns what the
  caller's `encode_body` returns. An encoder that fails no longer yields a
  truncated body.
