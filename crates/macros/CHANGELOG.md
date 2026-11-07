# Changelog

Krabka versions and tags the whole workspace as one unit, so this crate has no
release of its own. The [root changelog](../../CHANGELOG.md) records every
release that contains `krabka-macros`.

## [Unreleased]

### Removed

- The `#[bootstrap_cli_fields]` attribute and the `connect_cli_client!` macro. The operator CLIs now flatten the `ConnectionArgs` flag group of the workspace-private `krabka-cli-connection` crate, which adds `--command-config` and builds the client with its TLS and SASL policy. Removing public macros breaks the crate's API, so the release that carries this needs a minor version bump, to 1.2.0 at least.
