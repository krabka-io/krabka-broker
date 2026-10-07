//! Item generators for fixtures shared across crate boundaries.

use moxy::{
    ast::ParseError,
    token::{Ident, Span, TokenStream, TokenTree},
};

pub(crate) fn name(tokens: TokenStream) -> Result<Ident, ParseError> {
    let tokens: Vec<_> = tokens.into_iter().collect();
    match tokens.as_slice() {
        [TokenTree::Ident(name)] => Ok(name.clone()),
        _ => Err(ParseError::new(
            Span::call_site(),
            "fixture generator needs one item name",
        )),
    }
}

pub(crate) fn bounded_bfs(tokens: TokenStream) -> Result<TokenStream, ParseError> {
    let name = name(tokens)?;
    Ok(moxy::template! {
        /// Search breadth-first to completion and reject either truncation cap.
        pub(crate) fn {{ name }}<M>(model: M, label: &str, max_depth: usize, max_states: usize) -> impl ::stateright::Checker<M>
        where M: ::stateright::Model + Send + Sync + 'static,
            M::State: ::std::hash::Hash + Send + Sync + Clone + PartialEq + 'static,
            M::Action: Clone + PartialEq,
        {
            use ::stateright::{Checker as _, Model as _};
            let checker = model.checker().target_max_depth(max_depth).target_state_count(max_states).spawn_bfs().join();
            eprintln!("[{label}] unique_states={} generated={} max_depth={}", checker.unique_state_count(), checker.state_count(), checker.max_depth());
            ::assert2::assert!(checker.max_depth() < max_depth, "[{label}] hit depth cap {max_depth}: depth-truncated, not exhaustive");
            ::assert2::assert!(checker.state_count() < max_states, "[{label}] hit state cap {max_states}: truncated, not exhaustive");
            checker
        }
    })
}

pub(crate) fn compacted_batch(tokens: TokenStream) -> Result<TokenStream, ParseError> {
    let name = name(tokens)?;
    Ok(moxy::template! {
        /// Producer state and records retained in one compacted batch.
        #[derive(Debug, Clone, PartialEq, Eq)]
        struct {{ name }} {
            base_offset: i64,
            last_offset: i64,
            producer_id: i64,
            producer_epoch: i16,
            base_sequence: i32,
            control: bool,
            records: Vec<(Option<::bytes::Bytes>, Option<::bytes::Bytes>)>,
        }
        impl {{ name }} {
            fn of(batch: &::krabka_protocol::records::RecordBatch) -> Self {
                Self {
                    base_offset: batch.base_offset,
                    last_offset: batch.base_offset + i64::from(batch.last_offset_delta),
                    producer_id: batch.producer_id,
                    producer_epoch: batch.producer_epoch,
                    base_sequence: batch.base_sequence,
                    control: batch.attributes.is_control_batch(),
                    records: batch.records.iter().map(|record| (record.key.clone(), record.value.clone())).collect(),
                }
            }
            /// The original one-record batch, relocated to `offset`.
            fn whole(offset: i64, batch: &::krabka_protocol::records::RecordBatch) -> Self {
                Self { base_offset: offset, last_offset: offset, ..Self::of(batch) }
            }
            /// The bare header Kafka's `RETAIN_EMPTY` preserves.
            fn header(offset: i64, batch: &::krabka_protocol::records::RecordBatch) -> Self {
                Self { records: Vec::new(), ..Self::whole(offset, batch) }
            }
        }
    })
}

pub(crate) fn record_batch(tokens: TokenStream) -> Result<TokenStream, ParseError> {
    let name = name(tokens)?;
    Ok(moxy::template! {
        fn {{ name }}(n: i32, payload_size: usize) -> ::krabka_protocol::records::RecordBatch {
            ::krabka_protocol::records::RecordBatch {
                last_offset_delta: (n - 1).max(0),
                records: (0..n).map(|offset_delta| ::krabka_protocol::records::Record {
                    offset_delta,
                    key: Some(::bytes::Bytes::from(format!("k{offset_delta:08}"))),
                    value: Some(::bytes::Bytes::from(vec![0xABu8; payload_size])),
                    ..Default::default()
                }).collect(),
                ..Default::default()
            }
        }
    })
}

/// Generate the shared action adapter inside either simulation harness impl.
pub(crate) fn simulation_actions(root: TokenStream) -> Result<TokenStream, ParseError> {
    let root_tokens: Vec<_> = root.into_iter().collect();
    if root_tokens.is_empty() {
        return Err(ParseError::new(
            Span::call_site(),
            "simulation_actions needs the consensus crate path",
        ));
    }
    let root = TokenStream::from(root_tokens);
    Ok(moxy::template! {
        /// Apply consensus effects, leaving each harness's durability bookkeeping to its hook.
        pub(super) fn apply_action(&mut self, id: {{ root }}::types::NodeId, action: {{ root }}::action::Action) {
            use {{ root }}::{action::{Action, TimerKind}, event::Event};
            if let Some(messages) = {{ root }}::simulation_support::action_messages(id, &action, &self.voter_ids, &self.all_node_ids(), self.nodes[&id].log.log_end()) {
                for (peer, event) in messages { self.send(id, peer, event); }
                return;
            }
            match action {
                Action::SendFetch { leader_id } => {
                    self.replicate_from_leader(id, leader_id);
                    let log = &self.nodes[&id].log;
                    self.send(id, leader_id, Event::ReceiveFetch { from: id, fetch_epoch: log.last_epoch(), fetch_offset: log.end_offset() });
                }
                Action::AppendLeaderChange { epoch } => self.nodes.get_mut(&id).unwrap().log.append_in_epoch(epoch, 1),
                Action::AdvanceHighWatermark(hwm) => self.advance_simulated_watermark(id, hwm),
                Action::TruncateTo(point) => self.nodes.get_mut(&id).unwrap().log.truncate_to(point.offset),
                Action::ResetTimer { kind, deadline } => {
                    let node = self.nodes.get_mut(&id).unwrap();
                    match kind {
                        TimerKind::Election => node.election_deadline = Some(deadline),
                        TimerKind::Fetch => node.fetch_deadline = Some(deadline),
                        TimerKind::CheckQuorum => node.check_quorum_deadline = Some(deadline),
                    }
                }
                Action::TransitionedTo(_) | Action::PersistQuorumState | Action::ReplyDivergingEpoch(_) => {},
                _ => unreachable!("message actions handled above"),
            }
        }
    })
}

/// Delegate a metadata-log wrapper's unchanged operations before `async_trait` expands it.
pub(crate) fn metadata_log_delegate(
    root: TokenStream,
    item: TokenStream,
) -> Result<TokenStream, ParseError> {
    let (mut tokens, body) =
        crate::meta::item_body(item, "metadata_log_delegate", TokenTree::is_keyword_impl)?;
    let mut root_tokens: Vec<_> = root.into_iter().collect();
    // The explicit `keyed` mode opts into delegation of optional keyed writes.
    let keyed = root_tokens
        .last()
        .is_some_and(|token| token.as_ident().is_some_and(|name| name == "keyed"));
    if keyed {
        root_tokens.pop();
        if root_tokens
            .last()
            .is_none_or(|token| !token.is_punct_comma())
        {
            return Err(ParseError::new(
                Span::call_site(),
                "expected crate path, keyed",
            ));
        }
        root_tokens.pop();
    }
    if root_tokens.is_empty() {
        return Err(ParseError::new(
            Span::call_site(),
            "metadata_log_delegate needs the metadata log crate path",
        ));
    }
    let root = TokenStream::from(root_tokens);
    let TokenTree::Group(group) = &mut tokens[body] else {
        unreachable!()
    };
    let candidates = [
        (
            "partition_count",
            moxy::template! { fn partition_count(&self) -> i32 { self.inner.partition_count() } },
        ),
        (
            "publish",
            moxy::template! { async fn publish(&self, partition: i32, event: ::bytes::Bytes) -> Result<i64, {{ root }}::error::MetadataLogError> { self.inner.publish(partition, event).await } },
        ),
        (
            "subscribe",
            moxy::template! { fn subscribe(&self, assignment: Vec<{{ root }}::log::PartitionStart>) -> ({{ root }}::log::MetadataEventStream, ::std::sync::Arc<dyn {{ root }}::log::AssignmentHandle>) { self.inner.subscribe(assignment) } },
        ),
        (
            "high_water_marks",
            moxy::template! { async fn high_water_marks(&self) -> Result<Vec<i64>, {{ root }}::error::MetadataLogError> { self.inner.high_water_marks().await } },
        ),
    ];
    let existing: Vec<_> = group.tokens.clone().into_iter().collect();
    let has_method = |method: &str| {
        existing.windows(2).any(|tokens| {
            tokens[0].is_keyword_fn() && tokens[1].as_ident().is_some_and(|name| name == method)
        })
    };
    let mut methods = TokenStream::new();
    for (method, implementation) in candidates {
        if !has_method(method) {
            methods.extend(implementation);
        }
    }
    if keyed && !has_method("publish_keyed") {
        methods.extend(moxy::template! {
            async fn publish_keyed(&self, partition: i32, key: ::bytes::Bytes, event: Option<::bytes::Bytes>) -> Result<i64, {{ root }}::error::MetadataLogError> {
                self.inner.publish_keyed(partition, key, event).await
            }
        });
    }
    group.tokens = methods.into_iter().chain(group.tokens.clone()).collect();
    Ok(tokens.into())
}

pub(crate) fn remote_segment_check(tokens: TokenStream) -> Result<TokenStream, ParseError> {
    let name = name(tokens)?;
    Ok(moxy::template! {
        fn {{ name }}(manager: &impl ::krabka_remote_storage::RemoteLogMetadataManager, partition: &::krabka_remote_storage::TopicIdPartition) {
            let got = manager.remote_log_segment_metadata(partition, ::krabka_ids::LeaderEpoch(0), 42).unwrap().expect("segment found");
            ::assert2::check!(got.remote_log_segment_id().id == ::uuid::Uuid::from_u128(10));
            ::assert2::check!(got.custom_metadata() == Some(&::krabka_remote_storage::CustomMetadata(vec![7])));
            ::assert2::check!(manager.highest_offset_for_epoch(partition, ::krabka_ids::LeaderEpoch(0)).unwrap() == Some(99));
        }
    })
}

/// The harness event adapter, shared by browser and storage simulations.
pub(crate) fn simulation_step(root: TokenStream) -> Result<TokenStream, ParseError> {
    let root_tokens: Vec<_> = root.into_iter().collect();
    if root_tokens.is_empty() {
        return Err(ParseError::new(
            Span::call_site(),
            "simulation_step needs the consensus crate path",
        ));
    }
    let root = TokenStream::from(root_tokens);
    Ok(moxy::template! {
        /// Feed one event to the consensus machine, then apply its effects.
        pub(super) fn step(&mut self, id: {{ root }}::NodeId, event: {{ root }}::Event) {
            let now = self.now;
            let fetch_from = if let {{ root }}::Event::ReceiveFetch { from, .. } = &event { Some(*from) } else { None };
            // Release the node borrow before translating effects involving peers.
            let actions = {
                let node = self.nodes.get_mut(&id).unwrap();
                node.machine.on_event(event, &node.log, now)
            };
            // A caught-up fetch parks: answer only new data or divergence so the
            // watchdog bounds the steady-state fetch loop deterministically.
            if let Some(follower) = fetch_from {
                let node = &self.nodes[&id];
                if node.machine.role().is_leader()
                    && let Some(response) = {{ root }}::simulation_support::fetch_response(id, node.machine.quorum_state().leader_epoch, &actions, node.log.end_offset(), self.nodes[&follower].log.end_offset()) {
                    self.send(id, follower, response);
                }
            }
            for action in actions { self.apply_action(id, action); }
            self.reconcile_timers_for_role(id);
        }
    })
}

/// Append exactly the topic and partition records used by snapshot compatibility tests.
pub(crate) fn snapshot_topic(tokens: TokenStream) -> Result<TokenStream, ParseError> {
    let name = name(tokens)?;
    Ok(moxy::template! {
        fn {{ name }}(image: &mut ::krabka_metadata::MetadataImage) {
            use ::krabka_metadata::{LeaderEpoch, MetadataRecord, NodeId, PartitionRecord, TopicRecord};
            image.apply(&MetadataRecord::V1Topic(TopicRecord {
                name: "orders".into(), topic_id: ::uuid::Uuid::new_v4(), partitions: 2, replication_factor: 1,
            }));
            for partition in 0..2 {
                image.apply(&MetadataRecord::V1Partition(PartitionRecord {
                    topic: "orders".into(), partition, leader: NodeId(1), replicas: vec![NodeId(1)], isr: vec![NodeId(1)], leader_epoch: LeaderEpoch(0), adding_replicas: vec![], removing_replicas: vec![], directories: vec![], partition_epoch: 0,
                }));
            }
        }
    })
}
