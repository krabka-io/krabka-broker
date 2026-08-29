//! Conversion predicates between classic and next-gen consumer groups
//! (KIP-848).
//!
//! This module owns the [`super::config::ConsumerGroupMigrationPolicy`], the
//! convertibility predicates, and the state-translation helpers that live
//! migration uses.
//!
//! This file is the module root. `upgrade` holds the classic-to-next-gen
//! direction, `downgrade` the reverse, `assignment` the target-to-wire-blob
//! translation both directions need, and `hosted_classic` the classic RPCs an
//! upgraded group still serves.

mod assignment;
mod downgrade;
mod hosted_classic;
mod upgrade;

pub(crate) use self::{
    assignment::target_to_consumer_assignment,
    downgrade::{consumer_is_convertible, convert_consumer_to_classic, downgrade_pending_records},
    hosted_classic::{
        ClassicMemberRegistration, build_hosted_classic_join_result, serve_classic_heartbeat,
        serve_classic_sync, upsert_classic_member,
    },
    upgrade::{
        classic_is_convertible, convert_classic_to_consumer, decode_consumer_subscription,
        upgrade_pending_records,
    },
};
