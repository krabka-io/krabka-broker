# Changelog

Krabka versions and tags the whole workspace as one unit, so this crate has no
release of its own. The [root changelog](../../CHANGELOG.md) records every
release that contains `krabka-macros`.

## [Unreleased]

### Changed (breaking)

- `quorum_epoch_encoder!` now requires the declared function to return
  `Option<Bytes>`, not `Bytes`, and the generated body returns what the
  caller's `encode_body` returns. An encoder that fails no longer yields a
  truncated body.
- `#[bootstrap_cli_fields]` no longer puts a clap `env` attribute on
  `--bootstrap-server`. It adds `Cli::try_parse_with_env(argv, env)` instead,
  which reads `KRABKA_BOOTSTRAP_SERVER` through the lookup the caller passes,
  so a test can supply its own environment. A caller that parses with
  `Cli::parse_from` no longer gets the fallback.

### Added

- `bootstrap_cli_entrypoint!`, the `run_from_args` of a tool whose `Cli`
  carries `#[bootstrap_cli_fields]`. It parses with
  `Cli::try_parse_with_env` over the process environment.
