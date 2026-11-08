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

### Removed

- The `#[bootstrap_cli_fields]` attribute and the `connect_cli_client!` macro. The operator CLIs, their only users, now flatten the `ConnectionArgs` flag group of the workspace-private `krabka-cli-connection` crate. Removing public macros breaks the crate's API, so the release that carries this is at least a minor version, 1.2.0.
