//! `krabka-barrier` — administer krabka barrier groups.
//!
//! The monorepo spells this `krabka barrier`, as a subcommand of the operator
//! CLI. Here it is its own binary, because the rest of that CLI stayed behind
//! with the gres layer. The arguments are the same either way.

krabka_macros::cli_main!(krabka_barrier);
