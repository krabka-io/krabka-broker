//! Exhaustive-enumeration and proptest verification of `SimpleAclAuthorizer`
//! precedence against an INDEPENDENT oracle.
//!
//! The authorizer is a sequential pure decision function: super-user bypass >
//! deny-wins > allow > default-deny. It composes that order with Literal,
//! Literal-`*`, and Prefixed resource matching, with principal and host
//! wildcards, and with the one-way operation-implication table. Exhaustive
//! enumeration and proptest are therefore the honest fit, and stateright is
//! not, because there are no transitions. This mirrors the quota-precedence
//! slice.
//!
//! The oracle re-derives the decision from first principles with its own
//! matching predicates and its own implication arrows. It never calls the
//! production `matches_*` or `implies`. The suite therefore catches a
//! production regression instead of spot-checking it: a dropped or flipped
//! implication arrow, broken deny-wins, or a matching bug. Every case also
//! asserts that the broker (`MetadataImage`) and gateway (`AclCache`) decision
//! paths agree, with no drift.

use std::{collections::HashSet, net::SocketAddr};

use krabka_metadata::{
    AclEntry, AclOperation, MetadataImage, MetadataRecord, PatternType, PermissionType,
    ResourceType,
};
use krabka_security::{AuthMethod, Principal};
use uuid::Uuid;

use crate::{AclCache, AuthorizationRequest, AuthorizationResult, Authorizer, SimpleAclAuthorizer};

// ----- exhaustive enumeration -----

const ALICE: &str = "alice";

mod oracle;

mod fixtures;

mod exhaustive;

#[cfg(test)]
mod fuzz;

use fixtures::{check, entry, principal};
use oracle::oracle_decision;
