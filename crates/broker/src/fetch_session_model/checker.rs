use super::*;

impl Model for FsModel {
    type State = CacheState;
    type Action = Fetch;

    fn init_states(&self) -> Vec<Self::State> {
        // Seed: a fully-resolved session — one key with BOTH halves (name A,
        // id U) on the first partition, max_bytes 9 (a non-default sentinel).
        let mut partitions = HashMap::new();
        partitions.insert(
            FetchSessionKey {
                topic_name: NAME_A.to_string(),
                topic_id: id_of(1),
                partition: self.partitions[0],
            },
            CachedPartitionState {
                max_bytes: 9,
                ..Default::default()
            },
        );
        vec![CacheState { partitions }]
    }

    fn actions(&self, _s: &Self::State, actions: &mut Vec<Self::Action>) {
        let mut forgets: Vec<Option<(Ref, i32)>> = vec![None];
        for &r in &self.refs {
            for &p in &self.partitions {
                forgets.push(Some((r, p)));
            }
        }
        let mut subs: Vec<Option<(Ref, i32, i32)>> = vec![None];
        for &r in &self.refs {
            for &p in &self.partitions {
                for mb in [1, 2] {
                    subs.push(Some((r, p, mb)));
                }
            }
        }
        for &f in &forgets {
            for &s in &subs {
                if f.is_none() && s.is_none() {
                    continue; // empty fetch is a no-op; skip
                }
                actions.push(Fetch { forget: f, sub: s });
            }
        }
    }

    fn next_state(&self, last: &Self::State, a: Self::Action) -> Option<Self::State> {
        let mut s = last.clone();
        let forgotten: Vec<ForgottenTopic> = a
            .forget
            .into_iter()
            .map(|(r, p)| forgotten_topic(r, p))
            .collect();
        let topics: Vec<FetchTopic> = a
            .sub
            .into_iter()
            .map(|(r, p, mb)| fetch_topic(r, p, mb))
            .collect();

        apply_incremental(&mut s.partitions, &forgotten, &topics);

        // Headline safety, per transition (fires the moment a shadow appears).
        assert2::assert!(
            no_shadow(&s.partitions),
            "shadow entry after {a:?}: {:?}",
            s.proj()
        );

        // Subscription fidelity: a subscribed partition is reflected with the
        // requested max_bytes by some key matching the request's identity.
        if let Some((r, p, mb)) = a.sub {
            let present = s
                .partitions
                .iter()
                .any(|(k, st)| ref_matches(k, r, p) && st.max_bytes == mb);
            assert2::assert!(
                present,
                "subscription not reflected after {a:?}: {:?}",
                s.proj()
            );
        }

        Some(s)
    }

    fn properties(&self) -> Vec<Property<Self>> {
        vec![
            Property::always("no_shadow", |_, s: &CacheState| no_shadow(&s.partitions)),
            Property::always("no_orphan_default", |_, s: &CacheState| {
                s.partitions.values().all(|v| v.max_bytes != 0)
            }),
            // Non-vacuity witnesses.
            Property::sometimes("renamed", |_, s: &CacheState| {
                s.partitions.keys().any(|k| k.topic_name == NAME_B)
            }),
            Property::sometimes("emptied", |_, s: &CacheState| s.partitions.is_empty()),
            Property::sometimes("id_only_key", |_, s: &CacheState| {
                s.partitions.keys().any(|k| k.topic_name.is_empty())
            }),
            Property::sometimes("name_only_key", |_, s: &CacheState| {
                s.partitions.keys().any(|k| k.topic_id == WireUuid::ZERO)
            }),
            Property::sometimes("two_keys", |_, s: &CacheState| s.partitions.len() >= 2),
        ]
    }

    fn within_boundary(&self, s: &Self::State) -> bool {
        s.partitions.len() <= 8
    }
}
