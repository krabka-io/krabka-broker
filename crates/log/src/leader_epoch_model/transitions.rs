use super::*;

impl Cluster {
    pub(in super::super) fn new(replicas: usize, assign_on_election: bool) -> Self {
        let mut cluster = Self {
            epoch: LeaderEpoch(0),
            leader: 0,
            replicas: vec![Replica::default(); replicas],
            reconciled: vec![false; replicas],
            violations: Violations::default(),
            witnesses: Witnesses::default(),
        };
        if assign_on_election {
            cluster.replicas[0].assign(LeaderEpoch(0), Offset(0));
        }
        cluster
    }

    /// Apply `action`. Returns `false` when it changed nothing.
    pub(in super::super) fn step(&mut self, action: Action, assign_on_election: bool) -> bool {
        match action {
            Action::Elect(r) => {
                self.epoch = LeaderEpoch(self.epoch.0 + 1);
                self.leader = r;
                self.reconciled.fill(false);
                if assign_on_election {
                    let log_end = self.replicas[r].log_end();
                    self.replicas[r].assign(self.epoch, log_end);
                }
                true
            }
            Action::Write => {
                let epoch = self.epoch;
                self.replicas[self.leader].append(epoch);
                true
            }
            Action::Fetch(f) => self.fetch(f),
        }
    }

    pub(super) fn fetch(&mut self, f: usize) -> bool {
        let before = self.clone();
        let leader = self.replicas[self.leader].clone();
        let follower = &self.replicas[f];
        let fetch_offset = follower.log_end();
        let last_fetched_epoch = follower.epochs.last().map(|e| e.epoch);
        match leader_answer(&leader, fetch_offset, last_fetched_epoch) {
            FetchAnswer::Records => {
                self.reconciled[f] = true;
                let at = usize::try_from(fetch_offset.0).expect("fetch offset is non-negative");
                if let Some(&epoch) = leader.log.get(at) {
                    self.replicas[f].append(epoch);
                }
            }
            FetchAnswer::Diverging { epoch, end_offset } => {
                let requested = last_fetched_epoch.expect("only an epoch-carrying fetch diverges");
                let floor_and_higher = leader.epochs.iter().any(|e| e.epoch < requested)
                    && leader.epochs.iter().any(|e| e.epoch > requested);
                if floor_and_higher && leader.epochs.iter().all(|e| e.epoch != requested) {
                    self.witnesses.gap = true;
                }
                let truncation = follower_truncation(follower, epoch, end_offset);
                let agreed = common_prefix(&follower.log, &leader.log);
                let keep = usize::try_from(truncation.offset.0).expect("truncation is >= 0");
                if keep < agreed {
                    self.violations.over_truncated = true;
                }
                if keep < follower.log.len() {
                    self.witnesses.divergent_truncation = true;
                }
                if !truncation.complete {
                    self.witnesses.step_back = true;
                }
                let unchanged = self.replicas[f].clone();
                self.replicas[f].truncate(truncation.offset);
                if self.replicas[f] == unchanged {
                    self.violations.stalled = true;
                }
                self.reconciled[f] = false;
            }
            FetchAnswer::OutOfRange => {
                // `AbstractFetcherThread.fetchOffsetAndTruncate`: truncate to
                // the leader's log end when the follower is past it.
                self.violations.out_of_range = true;
                if leader.log_end() < fetch_offset {
                    self.replicas[f].truncate(leader.log_end());
                }
                self.reconciled[f] = false;
            }
        }
        *self != before
    }

    pub(in super::super) fn follower_prefix_holds(&self) -> bool {
        let leader = &self.replicas[self.leader].log;
        self.replicas.iter().enumerate().all(|(r, replica)| {
            r == self.leader || !self.reconciled[r] || leader.starts_with(&replica.log)
        })
    }

    pub(in super::super) fn checkpoints_hold(&self) -> bool {
        self.replicas
            .iter()
            .all(|replica| is_strictly_increasing(&replica.epochs))
    }

    pub(super) fn converged(&self) -> bool {
        let leader = &self.replicas[self.leader].log;
        !leader.is_empty()
            && self
                .reconciled
                .iter()
                .enumerate()
                .all(|(r, &ok)| r == self.leader || ok)
            && self.replicas.iter().all(|replica| &replica.log == leader)
    }
}
