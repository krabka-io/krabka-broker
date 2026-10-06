//! `krabka-backup` — copies the inputs a restore needs off a running cluster.
//!
//! The `krabka` operator CLI resolves an unknown subcommand to `krabka-<name>`
//! on `PATH`, the way git resolves `git foo` to `git-foo`, so this binary's
//! name is what makes `krabka backup` work.

krabka_macros::cli_main!(krabka_backup);
