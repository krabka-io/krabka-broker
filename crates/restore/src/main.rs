//! `krabka-restore` — rebuild a krabka log directory from a tiered-storage
//! archive.
//!
//! The `krabka` operator CLI spells this `krabka restore`. It resolves an
//! unknown subcommand to `krabka-<name>` on `PATH`, the way git resolves
//! `git foo` to `git-foo`, so the binary carries that name.

krabka_macros::cli_main!(krabka_restore, flavor = "multi_thread");
