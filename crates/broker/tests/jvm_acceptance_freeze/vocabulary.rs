//! The words and values these cases pin down.
//!
//! The two exception class names are what Kafka's own `Errors.forCode` builds
//! from the codes KFC-9 reuses, the topics and scopes are what each case
//! freezes, and the operators are the three principals the two-person rule is
//! written around. The refusal builders at the end rebuild the sentences the
//! broker words, so a case can assert one appears in what a JVM tool printed.

/// The JVM exception that `Errors.forCode` gives `POLICY_VIOLATION` (44).
///
/// The fully-qualified name is the assertion, and not the bare class name. A
/// bare `PolicyViolationException` would also match a sentence that merely
/// mentioned it, and the point of the check is that Kafka's own client
/// constructed this class.
pub(super) const POLICY_VIOLATION_EXCEPTION: &str =
    "org.apache.kafka.common.errors.PolicyViolationException";

/// The JVM exception that `Errors.forCode` gives `INVALID_CONFIG` (40).
pub(super) const INVALID_CONFIG_EXCEPTION: &str =
    "org.apache.kafka.common.errors.InvalidConfigurationException";

/// The break-glass action name that `break_glass.signed_actions`, the audit
/// event, the metric label and the refusal message all spell one way.
pub(super) const ACTION_DELETE_TOPIC: &str = "delete_topic";

/// The action name of an unclean `ElectLeaders`.
pub(super) const ACTION_UNCLEAN_ELECT_LEADERS: &str = "unclean_elect_leaders";

/// The wire value of `BreakGlassAction::UncleanElectLeaders` on the
/// krabka-private `ProposeBreakGlass` request (api key 1017).
///
/// The broker's own mapping is crate-private, so the value is written out
/// here. It is part of the private API's contract, and a change to it that
/// this constant did not follow would show up as a proposal that authorizes
/// nothing.
pub(super) const WIRE_UNCLEAN_ELECT_LEADERS: i8 = 2;

/// Where [`crate::jvm_acceptance::ClientPropsFile::mount_str`] puts the
/// properties file inside the container, so every JVM tool flag can name a
/// fixed path.
pub(super) const CONTAINER_PROPS: &str = "/client.properties";

/// The listener name the SASL case gives its one listener.
pub(super) const SASL_LISTENER: &str = "SASL_PLAINTEXT";

// ── Topics, scopes and the operators ─────────────────────────────────────────

/// The topic that a literal-scope freeze covers.
pub(super) const LITERAL_TOPIC: &str = "kfc9-orders";
/// What the operator typed when they froze [`LITERAL_TOPIC`]. It rides in the
/// refusal, so the JVM producer must print it back.
pub(super) const LITERAL_REASON: &str = "DR cutover";

/// The namespace a prefixed-scope freeze covers.
pub(super) const PREFIX_SCOPE: &str = "kfc9-tenant-a.";
/// A topic inside [`PREFIX_SCOPE`]. Its own name is in no registry entry, so
/// refusing it exercises the prefix index rather than the literal one.
pub(super) const PREFIX_TOPIC: &str = "kfc9-tenant-a.events";
/// What the operator typed when they froze [`PREFIX_SCOPE`].
pub(super) const PREFIX_REASON: &str = "tenant offboarding";

/// The unfrozen control topic. It exists so that a refusal is shown to be the
/// freeze rather than a produce path that stopped working.
pub(super) const CONTROL_TOPIC: &str = "kfc9-control";

/// The topic the `kafka-topics --delete` case creates and fails to delete.
pub(super) const DOOMED_TOPIC: &str = "kfc9-doomed";

/// The topic the `kafka-leader-election` case elects a leader for.
pub(super) const ELECT_TOPIC: &str = "kfc9-elect";

/// The topic the `kafka-configs` case freezes, describes and fails to alter.
pub(super) const CONFIGS_TOPIC: &str = "kfc9-configs";
/// What the operator typed when they froze [`CONFIGS_TOPIC`].
pub(super) const CONFIGS_REASON: &str = "config-path check";

/// The operator who opens the break-glass proposal. They may not approve it.
pub(super) const PROPOSER: (&str, &str) = ("alice", "alice-secret");
/// The first approving operator.
pub(super) const APPROVER_ONE: (&str, &str) = ("bob", "bob-secret");
/// The second approving operator. Two distinct principals is what makes the
/// rule a two-person rule rather than a two-click rule.
pub(super) const APPROVER_TWO: (&str, &str) = ("carol", "carol-secret");

/// Every operator, in the `KafkaPrincipal` spelling `break_glass.approvers`
/// takes.
///
/// The proposer is in the set because the broker refuses a proposal from a
/// principal outside it: a proposer who is a stranger, with two approvers,
/// would make a rule about three people into a rule about two people and a
/// stranger.
pub(super) fn approver_set() -> Vec<String> {
    [PROPOSER, APPROVER_ONE, APPROVER_TWO]
        .iter()
        .map(|(user, _)| format!("User:{user}"))
        .collect()
}

// ── The refusals the broker words, rebuilt on this side ──────────────────────

/// The `error_message` that rides beside `POLICY_VIOLATION` on a produce to a
/// frozen topic.
///
/// `pattern` is `literal` or `prefixed`, and the scope is quoted because the
/// broker renders it with its `Debug` form. The quotes are part of what the
/// operator reads, so they are part of the assertion.
pub(super) fn freeze_refusal(pattern: &str, scope: &str, reason: &str) -> String {
    format!("a write freeze on the {pattern} scope {scope:?} refuses this write: {reason}")
}

/// The `error_message` that rides beside `POLICY_VIOLATION` when the
/// two-person rule finds no approval at all.
///
/// This is the `NoProposal` wording specifically. A proposal that exists but
/// is short of approvals, withdrawn, expired or already spent gets a different
/// sentence, and asserting this one is what shows the tool reached the gate
/// with an empty registry rather than tripping over a half-built proposal.
pub(super) fn no_proposal_refusal(action: &str, target: &str) -> String {
    format!("break-glass refused {action} on {target}: no approved proposal covers the request")
}

/// The refusal both alter paths give for the read-only `write.freeze` key.
///
/// KFC-9 requires the message to name the command that does set the key,
/// because a refusal with no next step leaves an operator stuck mid-incident.
pub(super) const WRITE_FREEZE_ALTER_REFUSAL: &str = "topic config write.freeze is controller-managed and read-only; \
     use `krabka-guard freeze set` to set it and `krabka-guard freeze clear` to clear it";

/// The `write.freeze` value a `DescribeConfigs` reports for a topic frozen by
/// its own name, in the `frozen:<pattern>:<scope>` form KFC-9 specifies.
pub(super) fn write_freeze_value(scope: &str) -> String {
    format!("write.freeze=frozen:literal:{scope}")
}
