//! Per-member bookkeeping for the next-gen protocol: building a member from a
//! heartbeat, applying steady-state updates to one, choosing the assignor, and
//! driving the reconciler when the group is dirty.

use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant},
};

use krabka_protocol::{
    owned::consumer_group_heartbeat_request::ConsumerGroupHeartbeatRequest, primitives::uuid::Uuid,
};

use super::{FALLBACK_REBALANCE_TIMEOUT_MS, MetadataProvider};
use crate::coordinator::unified::{
    ClientIdentity,
    assignor::Assignor,
    config::NextGenConfig,
    consumer_state::{GroupState, MemberState},
    persistence_next_gen::MemberAssignmentState,
    reconciler,
};

/// The partitions a member reports that it owns in its heartbeat. An absent
/// `topic_partitions` means "unchanged". The caller then substitutes the
/// member's current assignment, so that a keepalive can still take newly freed
/// partitions.
pub(super) fn reported_owned(req: &ConsumerGroupHeartbeatRequest) -> HashMap<Uuid, Vec<i32>> {
    req.topic_partitions
        .as_ref()
        .map(|tp| {
            tp.iter()
                .map(|t| (t.topic_id, t.partitions.clone()))
                .collect()
        })
        .unwrap_or_default()
}

/// RE2J's `ERR_INVALID_PERL_OP`, the description Kafka formats into the
/// `INVALID_REGULAR_EXPRESSION` message when a pattern names a flag RE2J does
/// not have.
const RE2J_INVALID_PERL_OP: &str = "invalid or unsupported Perl syntax";

/// The inline flags RE2J's `Parser.parsePerlFlags` accepts, plus the `-` that
/// negates the ones that follow it.
const RE2J_INLINE_FLAGS: [char; 5] = ['i', 'm', 's', 'U', '-'];

/// The first inline flag in `pattern` that RE2J's `parsePerlFlags` does not
/// accept, if any.
///
/// RE2J's flag set is exactly `i`, `m`, `s` and `U`; Rust's `regex` also takes
/// `x` (verbose), `u` (Unicode) and `R` (CRLF), and would otherwise admit a
/// pattern Kafka answers `INVALID_REGULAR_EXPRESSION` to. The scan tracks
/// escapes and character classes so a `(` that is a literal, and everything
/// inside `[...]`, is left alone, and it hands every other `(?` form -- named
/// captures, non-capturing groups, the lookarounds both engines reject -- to
/// the regex parser rather than judging it here.
fn re2j_unsupported_inline_flag(pattern: &str) -> Option<char> {
    let chars: Vec<char> = pattern.chars().collect();
    let mut index = 0;
    let mut in_class = false;
    while index < chars.len() {
        let current = chars[index];
        if current == '\\' {
            index += 2;
            continue;
        }
        if in_class {
            in_class = current != ']';
            index += 1;
            continue;
        }
        if current == '[' {
            in_class = true;
            index += 1;
            continue;
        }
        index += 1;
        if current != '(' || chars.get(index) != Some(&'?') {
            continue;
        }
        // `(?P<`, `(?<`, `(?'`, `(?:`, `(?=` and `(?!` are not flag groups.
        if matches!(
            chars.get(index + 1),
            Some('P' | ':' | '<' | '\'' | '=' | '!')
        ) {
            continue;
        }
        let mut flag = index + 1;
        while let Some(&candidate) = chars.get(flag) {
            if candidate == ')' || candidate == ':' {
                break;
            }
            if !RE2J_INLINE_FLAGS.contains(&candidate) {
                return Some(candidate);
            }
            flag += 1;
        }
    }
    None
}

/// Rejects a heartbeat whose `SubscribedTopicRegex` does not compile, with the
/// message Kafka builds in
/// `GroupMetadataManager.throwIfRegularExpressionIsInvalid`.
///
/// Kafka compiles the pattern with `com.google.re2j.Pattern.compile` and
/// raises `InvalidRegularExpressionException` (`INVALID_REGULAR_EXPRESSION`,
/// 128) before it writes any member record, so the heartbeat that carries the
/// bad pattern fails and the member is not admitted.
///
/// We compile with `regex::Regex::new`, that is with Unicode mode ON, rather
/// than `RegexBuilder::unicode(false)`, even though RE2J's `\d`, `\w`, `\s`,
/// and `\b` are ASCII-only:
///
/// - This function decides *acceptance*, and Unicode mode accepts a strict
///   superset of what `unicode(false)` accepts. RE2J supports the Unicode
///   classes `\pN` and `\p{Greek}`; Rust's `regex` rejects those outright when
///   Unicode is off, so `unicode(false)` would answer 128 to patterns real
///   Kafka compiles happily. Being stricter than Kafka is the worse failure.
/// - The ASCII/Unicode split cannot change a *match* result here: the pattern
///   is only ever matched against Kafka topic names, whose legal alphabet is
///   `[a-zA-Z0-9._-]`. On ASCII-only inputs `\d`, `\w`, `\s` and `\b` mean the
///   same thing in both engines.
///
/// ## Residual divergence from RE2J
///
/// `regex` is not RE2J's grammar, so acceptance can still differ in both
/// directions. Only the divergence that a client is likely to hit is screened
/// out ahead of the compile, by [`re2j_unsupported_inline_flag`]: an inline
/// flag group naming `x`, `u` or `R`, which `regex` takes and RE2J rejects.
/// What is knowingly left:
///
/// - **Accepted here, rejected by RE2J.** `regex`'s character-class set
///   operations (`[\w&&\d]`, `[a-z--b]`, nested `[[a-z]x]`), which RE2J reads
///   as ordinary class members or as a syntax error. Screening these would
///   need a class parser, and getting one wrong rejects patterns Kafka takes.
/// - **Rejected here, accepted by RE2J.** `\Q...\E` literal quoting, which
///   RE2 supports and `regex` has no equivalent for, and a repetition large
///   enough to exceed `regex`'s compiled-size limit.
///
/// Neither residue can change which topics a subscription matches: an accepted
/// pattern is matched by the same `regex` that accepted it, against ASCII
/// topic names.
///
/// The tree's `java_regex` -- which `ListTransactions` uses, because KIP-664
/// specifies `java.util.regex` -- is not the closer engine for this field.
/// `java.util.regex` accepts backreferences, lookaround and possessive
/// quantifiers that RE2J rejects outright, so it over-accepts by more than
/// `regex` does, and it is a backtracking engine: this pattern is re-matched
/// against every topic name on every metadata refresh, where RE2J's and
/// `regex`'s linear-time guarantee is what keeps a subscription from becoming
/// a denial of service.
fn check_subscribed_topic_regex(pattern: &str) -> Result<(), String> {
    if re2j_unsupported_inline_flag(pattern).is_some() {
        return Err(format!(
            "SubscribedTopicRegex `{pattern}` is not a valid regular expression: \
             {RE2J_INVALID_PERL_OP}."
        ));
    }
    regex::Regex::new(pattern).map(|_| ()).map_err(|error| {
        // `regex`'s Display is a multi-line diagram; flatten it so the
        // response's `error_message` stays one line.
        let detail = error
            .to_string()
            .lines()
            .map(str::trim)
            .filter(|line| !line.is_empty())
            .collect::<Vec<_>>()
            .join(" ");
        format!("SubscribedTopicRegex `{pattern}` is not a valid regular expression: {detail}.")
    })
}

/// Applies steady-state member updates and runs reconciliation. It returns
/// `true` when a change happened that needs a log write, and the
/// `INVALID_REGULAR_EXPRESSION` message when the heartbeat carries a
/// `SubscribedTopicRegex` that does not compile.
pub(super) fn update_member_state(
    state: &mut GroupState,
    config: &NextGenConfig,
    metadata: &dyn MetadataProvider,
    req: &ConsumerGroupHeartbeatRequest,
    client: ClientIdentity<'_>,
    now: Instant,
    cur_epoch: i32,
) -> Result<bool, String> {
    // Kafka validates the pattern before it touches member state, and only
    // when the heartbeat carries one that differs from the member's stored
    // pattern. Do the same, so a rejected heartbeat leaves the group exactly
    // as it found it.
    if let Some(pattern) = req.subscribed_topic_regex.as_deref()
        && state
            .members
            .get(&req.member_id)
            .is_none_or(|m| m.subscribed_topic_regex.as_deref() != Some(pattern))
    {
        check_subscribed_topic_regex(pattern)?;
    }
    let mut member_metadata_changed = false;
    let mut became_dirty = false;
    if let Some(m) = state.members.get_mut(&req.member_id) {
        m.last_seen = now;
        if m.client_id != client.id {
            m.client_id = client.id.to_string();
            member_metadata_changed = true;
        }
        if m.client_host != client.host {
            m.client_host = client.host.to_string();
            member_metadata_changed = true;
        }
        if let Some(ref names) = req.subscribed_topic_names {
            let set: std::collections::HashSet<String> = names.iter().cloned().collect();
            if set != m.subscribed_topic_names {
                m.subscribed_topic_names = set;
                became_dirty = true;
                member_metadata_changed = true;
            }
        }
        // KIP-848 v1+: `subscribed_topic_regex` may change independently
        // of `subscribed_topic_names`. Only mark dirty when it actually
        // changes; the client re-sends the same regex on every
        // heartbeat as long as the subscription is stable.
        if req.subscribed_topic_regex != m.subscribed_topic_regex {
            // Recompile the cached regex only here — the one place the
            // pattern actually changes (the client re-sends the same regex
            // every heartbeat while the subscription is stable).
            m.set_regex(req.subscribed_topic_regex.clone());
            state.dirty = true;
        }
    }
    if became_dirty {
        state.dirty = true;
    }
    let was_dirty = state.dirty;
    run_reconcile(state, config, metadata);
    let epoch_advanced = state.target.epoch > cur_epoch;
    if epoch_advanced {
        state.advance_member_epoch(&req.member_id);
    }
    // Reconcile this member's current assignment against the (possibly new)
    // target and what it reports owning: grant free target partitions, mark
    // revocations, and withhold partitions still held by another member. A
    // heartbeat without `topic_partitions` is a keepalive — reuse the member's
    // current assignment as its owned set so it can still pick up freed partitions.
    let owned = if req.topic_partitions.is_some() {
        reported_owned(req)
    } else {
        state
            .members
            .get(&req.member_id)
            .map(|m| m.assigned_partitions.clone())
            .unwrap_or_default()
    };
    let assignment_changed = state.reconcile_member(&req.member_id, &owned);
    Ok(member_metadata_changed || was_dirty || epoch_advanced || assignment_changed)
}

pub(super) fn run_reconcile(
    state: &mut GroupState,
    config: &NextGenConfig,
    metadata: &dyn MetadataProvider,
) {
    // `metadata.snapshot()` rebuilds HashMaps over every cluster topic /
    // partition — far too expensive to run on a steady-state no-op
    // heartbeat. `reconcile_if_dirty` early-returns when `!dirty`, so gate
    // the snapshot on the same condition: only pay for it when we will
    // actually recompute. Behavior when dirty is identical to before.
    if !state.dirty {
        return;
    }
    let input = metadata.snapshot();
    let assignor = pick_assignor(state, config);
    reconciler::reconcile_if_dirty(state, &input, &*assignor);
}

fn pick_assignor(state: &GroupState, config: &NextGenConfig) -> Arc<dyn Assignor> {
    for m in state.members.values() {
        if let Some(name) = m.server_assignor.as_deref()
            && let Some(a) = config.find_assignor(name)
        {
            return a;
        }
    }
    config
        .assignors
        .first()
        .cloned()
        .expect("NextGenConfig must have at least one registered assignor")
}

/// Builds a first-join member, rejecting a heartbeat whose
/// `SubscribedTopicRegex` does not compile.
///
/// Kafka runs the same check on the join path, before any member record is
/// written, so a first heartbeat with a bad pattern fails instead of admitting
/// a member that would then never receive partitions.
pub(super) fn try_build_member(
    member_id: &str,
    req: &ConsumerGroupHeartbeatRequest,
    client: ClientIdentity<'_>,
    now: Instant,
) -> Result<MemberState, String> {
    if let Some(pattern) = req.subscribed_topic_regex.as_deref() {
        check_subscribed_topic_regex(pattern)?;
    }
    Ok(build_member(member_id, req, client, now))
}

pub(super) fn build_member(
    member_id: &str,
    req: &ConsumerGroupHeartbeatRequest,
    client: ClientIdentity<'_>,
    now: Instant,
) -> MemberState {
    let subs: std::collections::HashSet<String> = req
        .subscribed_topic_names
        .clone()
        .unwrap_or_default()
        .into_iter()
        .collect();
    let mut member = MemberState {
        member_id: member_id.into(),
        instance_id: req.instance_id.clone(),
        rack_id: req.rack_id.clone(),
        client_id: client.id.into(),
        client_host: client.host.into(),
        subscribed_topic_names: subs,
        subscribed_topic_regex: req.subscribed_topic_regex.clone(),
        compiled_regex: crate::coordinator::unified::consumer_state::CompiledRegex::Absent,
        server_assignor: req.server_assignor.clone(),
        rebalance_timeout: Duration::from_millis(
            u64::try_from(req.rebalance_timeout_ms.max(0)).unwrap_or(FALLBACK_REBALANCE_TIMEOUT_MS),
        ),
        member_epoch: 0,
        previous_member_epoch: 0,
        assignment_state: MemberAssignmentState::Stable,
        assigned_partitions: HashMap::new(),
        partitions_pending_revocation: HashMap::new(),
        last_seen: now,
        classic: None,
    };
    // The struct literal above sets the pattern string directly, so fill the
    // compiled cache from it. Without this a member that joins with a regex
    // never compiles one: the steady-state path recompiles only when the
    // pattern *changes*, and the client re-sends the same pattern forever.
    member.sync_regex_cache();
    member
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use assert2::{assert, check};

    use super::*;
    use crate::coordinator::unified::{
        GroupCoordinator,
        actor::{
            GroupActorMessage,
            heartbeat::step_heartbeat,
            test_support::{StaticMetadata, empty_metadata},
        },
        assignor::{Assignment, MemberSubscription, TopicMetadata},
        offsets_log::fake::InMemoryOffsetsLog,
        reconciler::ReconcileInput,
    };

    #[test]
    fn subscription_change_persists_every_reconciled_assignment() {
        let config = NextGenConfig::default();
        let first_topic = Uuid([10; 16]);
        let second_topic = Uuid([11; 16]);
        let metadata = StaticMetadata {
            input: ReconcileInput {
                topic_id_by_name: [
                    ("first".into(), first_topic),
                    ("second".into(), second_topic),
                ]
                .into(),
                partitions_per_topic: [(first_topic, 2), (second_topic, 2)].into(),
                ..Default::default()
            },
        };
        let mut state = GroupState::new("g");
        for member_id in ["m1", "m2"] {
            state.add_or_update_member(build_member(
                member_id,
                &ConsumerGroupHeartbeatRequest {
                    subscribed_topic_names: Some(vec!["first".into()]),
                    rebalance_timeout_ms: 60_000,
                    ..Default::default()
                },
                crate::coordinator::unified::ClientIdentity {
                    id: "client",
                    host: "host",
                },
                Instant::now(),
            ));
        }
        run_reconcile(&mut state, &config, &metadata);
        state.advance_member_epoch("m1");
        state.advance_member_epoch("m2");
        let member_epoch = state.group_epoch;

        let step = step_heartbeat(
            &mut state,
            &config,
            &metadata,
            &ConsumerGroupHeartbeatRequest {
                group_id: "g".into(),
                member_id: "m2".into(),
                member_epoch,
                subscribed_topic_names: Some(vec!["second".into()]),
                rebalance_timeout_ms: 60_000,
                ..Default::default()
            },
            crate::coordinator::unified::ClientIdentity {
                id: "client",
                host: "host",
            },
            Instant::now(),
        );

        let mut target_ids: Vec<&str> = step
            .pending
            .target_per_member
            .iter()
            .map(|(member_id, _)| member_id.as_str())
            .collect();
        let mut current_ids: Vec<&str> = step
            .pending
            .current_per_member
            .iter()
            .map(|(member_id, _)| member_id.as_str())
            .collect();
        target_ids.sort_unstable();
        current_ids.sort_unstable();

        check!(step.pending.target_metadata.is_some());
        check!(target_ids == vec!["m1", "m2"]);
        assert!(current_ids == vec!["m1", "m2"]);
    }

    /// A group holding one member subscribed by regex, already reconciled and
    /// at a stable epoch.
    fn group_with_regex_member(metadata: &StaticMetadata, pattern: &str) -> GroupState {
        let config = NextGenConfig::default();
        let mut state = GroupState::new("g");
        state.add_or_update_member(build_member(
            "m1",
            &ConsumerGroupHeartbeatRequest {
                subscribed_topic_regex: Some(pattern.into()),
                rebalance_timeout_ms: 60_000,
                ..Default::default()
            },
            ClientIdentity {
                id: "client",
                host: "host",
            },
            Instant::now(),
        ));
        run_reconcile(&mut state, &config, metadata);
        state.advance_member_epoch("m1");
        state
    }

    fn orders_metadata() -> StaticMetadata {
        let orders = Uuid([12; 16]);
        StaticMetadata {
            input: ReconcileInput {
                topic_id_by_name: [("orders-eu".into(), orders)].into(),
                partitions_per_topic: [(orders, 2)].into(),
                ..Default::default()
            },
        }
    }

    /// Kafka's `throwIfRegularExpressionIsInvalid` fails the heartbeat that
    /// carries a bad pattern, before any member record is written, so the
    /// joining member is never admitted.
    #[test]
    fn invalid_regex_on_join_rejects_the_heartbeat() {
        let config = NextGenConfig::default();
        let metadata = orders_metadata();
        for pattern in ["(", "[a-", "a{2,1}"] {
            let mut state = GroupState::new("g");
            let step = step_heartbeat(
                &mut state,
                &config,
                &metadata,
                &ConsumerGroupHeartbeatRequest {
                    group_id: "g".into(),
                    member_id: "m1".into(),
                    member_epoch: 0,
                    subscribed_topic_regex: Some(pattern.into()),
                    rebalance_timeout_ms: 60_000,
                    ..Default::default()
                },
                ClientIdentity {
                    id: "client",
                    host: "host",
                },
                Instant::now(),
            );

            check!(
                step.response.error_code == crate::codes::INVALID_REGULAR_EXPRESSION,
                "{pattern}"
            );
            check!(
                step.response
                    .error_message
                    .as_deref()
                    .is_some_and(|m| m.starts_with(&format!(
                        "SubscribedTopicRegex `{pattern}` is not a valid regular expression: "
                    ))),
                "{pattern}: {:?}",
                step.response.error_message,
            );
            check!(step.pending.is_empty(), "{pattern}");
            assert!(state.members.is_empty(), "{pattern}");
        }
    }

    /// A pattern change to something that does not compile leaves the existing
    /// member exactly as it was: same pattern, same epoch, group not dirty.
    #[test]
    fn invalid_regex_on_pattern_change_leaves_member_untouched() {
        let config = NextGenConfig::default();
        let metadata = orders_metadata();
        for pattern in ["(", "[a-", "a{2,1}"] {
            let mut state = group_with_regex_member(&metadata, "^orders-.*");
            let member_epoch = state.members["m1"].member_epoch;
            let group_epoch = state.group_epoch;

            let result = update_member_state(
                &mut state,
                &config,
                &metadata,
                &ConsumerGroupHeartbeatRequest {
                    group_id: "g".into(),
                    member_id: "m1".into(),
                    member_epoch,
                    subscribed_topic_regex: Some(pattern.into()),
                    rebalance_timeout_ms: 60_000,
                    ..Default::default()
                },
                ClientIdentity {
                    id: "other-client",
                    host: "other-host",
                },
                Instant::now(),
                member_epoch,
            );

            check!(result.is_err(), "{pattern}");
            let member = &state.members["m1"];
            check!(
                member.subscribed_topic_regex.as_deref() == Some("^orders-.*"),
                "{pattern}"
            );
            check!(member.client_id == "client", "{pattern}");
            check!(member.member_epoch == member_epoch, "{pattern}");
            check!(!state.dirty, "{pattern}");
            assert!(state.group_epoch == group_epoch, "{pattern}");
        }
    }

    /// The rejection is specific to the bad pattern: a valid one still admits
    /// the member and reconciles the topics it matches.
    #[test]
    fn valid_regex_still_reconciles() {
        let config = NextGenConfig::default();
        let metadata = orders_metadata();
        let mut state = GroupState::new("g");

        let step = step_heartbeat(
            &mut state,
            &config,
            &metadata,
            &ConsumerGroupHeartbeatRequest {
                group_id: "g".into(),
                member_id: "m1".into(),
                member_epoch: 0,
                subscribed_topic_regex: Some("^orders-.*".into()),
                rebalance_timeout_ms: 60_000,
                ..Default::default()
            },
            ClientIdentity {
                id: "client",
                host: "host",
            },
            Instant::now(),
        );

        check!(step.response.error_code == 0);
        check!(step.response.error_message.is_none());
        let assigned: Vec<i32> = state.members["m1"]
            .assigned_partitions
            .values()
            .flatten()
            .copied()
            .collect();
        assert!(assigned.len() == 2, "{:?}", state.members["m1"]);
    }

    /// Acceptance parity with RE2J, the engine Kafka validates with. Rust's
    /// `regex` runs in Unicode mode here on purpose: it accepts everything
    /// RE2J does in these cases, where `RegexBuilder::unicode(false)` would
    /// reject the Unicode classes RE2J supports.
    #[test]
    fn regex_acceptance_matches_re2j() {
        for (pattern, accepted) in [
            // ASCII in RE2J, Unicode-aware in Rust — both compile.
            (r"\d+", true),
            (r"\w+", true),
            (r"x", true),
            // Unicode classes: RE2J supports them, and `unicode(false)` would
            // not.
            (r"\pN", true),
            (r"\p{Greek}", true),
            // Named groups: RE2's `(?P<name>)` spelling and the modern
            // `(?<name>)` spelling.
            (r"(?P<name>a)", true),
            (r"(?<name>a)", true),
            // Rejected by both engines.
            ("(", false),
            ("[a-", false),
            // Inline flags. RE2J's `parsePerlFlags` takes only `i`, `m`, `s`
            // and `U`, with `-` to negate; `regex` also takes `x`, `u` and
            // `R`, so those must be rejected here to stay with Kafka.
            ("(?i)abc", true),
            ("(?im)abc", true),
            ("(?i-s)abc", true),
            ("(?U)a+", true),
            ("(?i:abc)", true),
            ("(?:abc)", true),
            ("(?x) a b c", false),
            ("(?x:abc)", false),
            ("(?iu)abc", false),
            ("(?-u)abc", false),
            ("(?R)abc", false),
            // A `(?` that is not a flag group at all: the literal `(` an
            // escape produces, and one inside a character class.
            (r"\(?abc", true),
            ("[(?x]abc", true),
            (r"a\[(?x)", false),
        ] {
            check!(
                check_subscribed_topic_regex(pattern).is_ok() == accepted,
                "{pattern}: {:?}",
                check_subscribed_topic_regex(pattern),
            );
        }
    }

    /// A flag RE2J does not have is answered with the message Kafka builds
    /// from RE2J's own `PatternSyntaxException.getDescription`, so a client
    /// that reads `error_message` sees the same text either broker produced.
    #[test]
    fn an_re2j_unsupported_flag_carries_kafkas_message() {
        check!(
            check_subscribed_topic_regex("(?x)abc")
                == Err(
                    "SubscribedTopicRegex `(?x)abc` is not a valid regular expression: \
                        invalid or unsupported Perl syntax."
                        .to_string()
                )
        );
    }

    /// The one behavioral difference the Unicode choice leaves — `\d` matching
    /// a non-ASCII digit — cannot be observed through a subscription, because
    /// Kafka topic names are drawn from `[a-zA-Z0-9._-]`.
    #[test]
    fn unicode_digit_class_cannot_change_a_topic_name_match() {
        let re = regex::Regex::new(r"^t\d+$").expect("compiles");
        check!(re.is_match("t42"));
        check!(!re.is_match("t-42"));
        // Unicode-aware in Rust, ASCII-only in RE2J; no legal topic name can
        // contain this character, so the divergence is unreachable.
        assert!(re.is_match("t\u{0663}"));
    }

    #[derive(Debug)]
    struct CountingAssignor {
        calls: Arc<AtomicUsize>,
    }
    impl Assignor for CountingAssignor {
        fn name(&self) -> &'static str {
            "counting"
        }
        fn assign(&self, _members: &[MemberSubscription], _topics: &TopicMetadata) -> Assignment {
            self.calls.fetch_add(1, Ordering::SeqCst);
            std::collections::HashMap::new()
        }
    }

    #[test]
    fn pick_assignor_skips_unregistered_member_preference() {
        let config = NextGenConfig::default();
        let mut state = crate::coordinator::unified::consumer_state::GroupState::new("g");
        let mut m = build_member(
            "m1",
            &ConsumerGroupHeartbeatRequest::default(),
            crate::coordinator::unified::ClientIdentity {
                id: "client-a",
                host: "h",
            },
            Instant::now(),
        );
        m.server_assignor = Some("ghost".into());
        state.members.insert("m1".into(), m);

        let picked = pick_assignor(&state, &config);
        assert!(picked.name() == "uniform");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn custom_assignor_invoked_when_requested() {
        let calls = Arc::new(AtomicUsize::new(0));
        let mut config = NextGenConfig::default();
        config
            .register_assignor(Arc::new(CountingAssignor {
                calls: calls.clone(),
            }))
            .unwrap();

        let log = Arc::new(InMemoryOffsetsLog::default());
        let coord = Arc::new(GroupCoordinator::new(
            config,
            crate::coordinator::unified::share::config::ShareGroupConfig::default(),
            empty_metadata(),
            log,
            crate::coordinator::unified::streams::config::StreamsGroupConfig::default(),
        ));
        let handle = coord.get_or_create_consumer("g");

        let (tx, rx) = tokio::sync::oneshot::channel();
        handle
            .tx
            .send(GroupActorMessage::Heartbeat {
                request: ConsumerGroupHeartbeatRequest {
                    group_id: "g".into(),
                    member_id: String::new(),
                    member_epoch: 0,
                    subscribed_topic_names: Some(vec!["t".into()]),
                    server_assignor: Some("counting".into()),
                    rebalance_timeout_ms: 60_000,
                    ..Default::default()
                },
                client_id: "client-a".into(),
                client_host: String::new(),
                reply: tx,
            })
            .await
            .unwrap();
        let resp = rx.await.unwrap();
        assert!(resp.error_code == 0);
        assert!(
            calls.load(Ordering::SeqCst) >= 1,
            "custom assignor must be invoked at least once",
        );
    }
}
