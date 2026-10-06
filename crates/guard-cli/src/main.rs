//! `krabka-guard` — one command for one incident.
//!
//! The monorepo spells an operator command as a subcommand of `crabka`. Here
//! this is its own binary, beside `krabka-barrier`, because the rest of that
//! CLI stayed behind with the gres layer. The arguments are the same either
//! way.

krabka_macros::cli_main!(krabka_guard);
