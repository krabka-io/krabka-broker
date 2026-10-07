//! Common policy fields of the TOML and command-line runtime configurations.

use moxy::{
    ast::{Field, List, ParseError, Parser, Token},
    token::{Span, ToTokenStream, TokenStream, TokenTree},
};

pub(crate) fn expand(meta: TokenStream, item: TokenStream) -> Result<TokenStream, ParseError> {
    let arguments: Vec<_> = meta.into_iter().collect();
    let (startup_before, cli) = match arguments.as_slice() {
        [TokenTree::Ident(mode)] if mode == "toml" => ("client_metrics_enable", false),
        [TokenTree::Ident(mode)] if mode == "cli" => ("client_metrics_eviction_tick", true),
        _ => {
            return Err(ParseError::new(
                Span::call_site(),
                "expected `toml` or `cli`",
            ));
        }
    };
    let startup = moxy::template! {
    /// Maximum time the broker waits for a controller leader during startup.
    pub startup_leader_wait_timeout: Option<Time>,
    /// Initial delay between broker self-registration attempts.
    pub self_registration_backoff_min: Option<Time>,
    /// Maximum delay between broker self-registration attempts.
    pub self_registration_backoff_max: Option<Time>,
    /// Cadence of the KIP-853 observer promotion poll.
    pub observer_poll_interval: Option<Time>,
    /// Cadence at which the audit spool replays records it could not append.
    pub audit_spool_replay_interval: Option<Time>,
    /// Cadence of the audit statistics poll.
    pub audit_stats_poll_interval: Option<Time>,
    /// Maximum wait for the audit partition to become available.
    pub audit_partition_wait_timeout: Option<Time>,
    /// Cadence of broker liveness maintenance.
    pub liveness_tick_interval: Option<Time>,
    /// Cadence at which broker gauges are refreshed.
    pub gauge_poll_interval: Option<Time>,
    /// Cadence of in-sync-replica maintenance.
    pub isr_scan_interval: Option<Time>,
    /// Cadence of log cleaner maintenance.
    pub cleaner_interval: Option<Time>,
    /// Cadence of local-retention maintenance: how often `retention.ms`,
    /// `retention.bytes` and `segment.ms` are applied to every hosted log.
    pub log_retention_check_interval: Option<Time>,
    /// Retry delay after a KIP-113 future-log move fails.
    pub future_log_move_retry_backoff: Option<Time>,

    };
    let replication = moxy::template! {
    /// Maximum bytes a follower requests from a leader in one replication
    /// fetch. It reaches the leader as the fetch request's `max_bytes`.
    pub replication_fetch_max: Option<ByteSize>,
    /// Maximum time a leader holds a replication fetch that is not yet
    /// satisfied. It reaches the leader as the fetch request's `max_wait_ms`.
    pub replication_fetch_max_wait: Option<Time>,
    /// Minimum bytes that satisfy a replication fetch. It reaches the leader
    /// as the fetch request's `min_bytes`, which the leader honours as a
    /// floor.
    pub replication_fetch_min: Option<ByteSize>,
    /// Delay after a follower exhausts its replication throttle budget.
    pub replication_throttle_exhausted_backoff: Option<Time>,
    /// Retry delay after sending a replication request fails.
    pub replication_send_error_backoff: Option<Time>,
    /// Retry delay when the leader does not yet know the topic.
    pub replication_unknown_topic_retry_delay: Option<Time>,
    /// Retry delay after a leader-epoch fence.
    pub replication_epoch_fence_backoff: Option<Time>,
    /// Retry delay after an unexpected replication error.
    pub replication_unexpected_error_backoff: Option<Time>,
    /// Initial delay before a follower reconnects to a leader.
    pub replication_reconnect_initial_delay: Option<Time>,
    /// Maximum delay between leader reconnection attempts.
    pub replication_reconnect_delay_cap: Option<Time>,
    /// Cadence of the consumer-group session expiry scan.
    pub coordinator_session_expiry_tick: Option<Time>,
    /// Maximum wait for coordinator shutdown acknowledgements.
    pub coordinator_shutdown_ack_timeout: Option<Time>,
    /// Default KIP-848 consumer-group session timeout, Kafka's
    /// `group.consumer.session.timeout.ms`.
    pub consumer_group_session_timeout: Option<Time>,
    /// Default KIP-848 consumer-group heartbeat interval, Kafka's
    /// `group.consumer.heartbeat.interval.ms`.
    pub consumer_group_heartbeat_interval: Option<Time>,
    /// Lower bound on the negotiated consumer-group session timeout, Kafka's
    /// `group.consumer.min.session.timeout.ms`.
    pub consumer_group_min_session_timeout: Option<Time>,
    /// Upper bound on the negotiated consumer-group session timeout, Kafka's
    /// `group.consumer.max.session.timeout.ms`.
    pub consumer_group_max_session_timeout: Option<Time>,
    /// Lower bound on the negotiated consumer-group heartbeat interval,
    /// Kafka's `group.consumer.min.heartbeat.interval.ms`.
    pub consumer_group_min_heartbeat_interval: Option<Time>,
    /// Upper bound on the negotiated consumer-group heartbeat interval,
    /// Kafka's `group.consumer.max.heartbeat.interval.ms`.
    pub consumer_group_max_heartbeat_interval: Option<Time>,

    };
    let (mut tokens, body) = crate::meta::named_body(item, "runtime_policy_fields")?;
    let TokenTree::Group(group) = &mut tokens[body] else {
        unreachable!()
    };
    for (before, fields) in [
        (startup_before, startup),
        ("consumer_group_max_size", replication),
    ] {
        // TOML documentation is schema text; the CLI fields originally had no
        // help text, and clap would otherwise turn these docs into help.
        let fields = if cli {
            let mut fields = List::<Field, Token![,]>::parse_all(&Parser::from_tokens(&fields))?;
            for field in &mut fields {
                field.attrs.retain(|attr| !attr.path.is_ident("doc"));
            }
            fields.to_token_stream()
        } else {
            fields
        };
        let mut start = group
            .tokens
            .windows(2)
            .position(|pair| {
                pair[0].is_keyword_pub() && pair[1].as_ident().is_some_and(|ident| ident == before)
            })
            .ok_or_else(|| {
                ParseError::new(
                    Span::call_site(),
                    format!("missing runtime field `{before}`"),
                )
            })?;
        while start >= 2
            && group.tokens[start - 2].is_punct_pound()
            && group.tokens[start - 1]
                .as_group()
                .is_some_and(|attr| attr.delim.is_bracket())
        {
            start -= 2;
        }
        let mut body: Vec<_> = group.tokens.clone().into_iter().collect();
        body.splice(start..start, fields);
        group.tokens = body.into();
    }
    Ok(tokens.into())
}
