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
