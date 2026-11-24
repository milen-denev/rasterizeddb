//! One Raft node: a deterministic state machine over messages and ticks.
//!
//! # The contract with the caller
//!
//! The node performs no I/O. After any call that may have changed something
//! (`step`, `tick`, `propose`, …), the caller takes [`RaftNode::ready`] and,
//! **in this order**:
//!
//! 1. when `ready.snapshot` is set, installs it into the state machine and
//!    replaces its whole durable log with the snapshot's position (no
//!    entries); then truncates its durable log from `ready.truncate_from`
//!    when set, appends `ready.entries`, writes `ready.hard_state` when set,
//!    and syncs them all (a vote or a term that is not durable before a
//!    message leaves can elect two leaders);
//! 2. (nothing more for the snapshot: step 1 installed it);
//! 3. sends `ready.messages`;
//! 4. applies `ready.committed`, in order;
//! 5. calls [`RaftNode::advance`] with the ready it handled.
//!
//! The safety properties the simulation checks (`rasterizeddb_cluster_sim`):
//! at most one leader per term; logs that share an index and term are equal
//! up to it; a committed entry is in every later leader's log; no two nodes
//! apply different entries at one index.

use std::collections::{BTreeMap, VecDeque};

use crate::log::RaftLog;
use crate::types::*;

/// Timing, in ticks.
#[derive(Clone, Debug)]
pub struct Timing {
    /// A follower that hears nothing for a random time in
    /// `[election, 2 × election)` campaigns.
    pub election: u32,
    /// A leader sends heartbeats this often.
    pub heartbeat: u32,
    /// At most this many entries per Append.
    pub max_append: usize,
    /// At most this many entries unacknowledged per follower (flow control).
    pub max_inflight: u64,
}

impl Default for Timing {
    fn default() -> Self {
        Timing { election: 10, heartbeat: 2, max_append: 256, max_inflight: 4096 }
    }
}

/// What the caller must do now (module docs).
#[derive(Clone, Debug, Default)]
pub struct Ready {
    pub truncate_from: Option<Index>,
    pub entries: Vec<Entry>,
    pub hard_state: Option<HardState>,
    pub snapshot: Option<(SnapshotMeta, Vec<u8>)>,
    /// With `snapshot`: the log after it was kept (it agreed with the
    /// snapshot), so the caller drops only its durable entries through the
    /// snapshot's index. `false`: the durable log is replaced whole.
    pub snapshot_keeps_log: bool,
    pub messages: Vec<(NodeId, Message)>,
    pub committed: Vec<Entry>,
    pub reads: Vec<ReadState>,
    /// Peers that need a snapshot: the caller builds one and hands it to
    /// [`RaftNode::send_snapshot`].
    pub snapshot_wanted: Vec<NodeId>,
    /// The role changed in this ready (to drop proposals on step-down).
    pub role_changed: Option<Role>,
}

impl Ready {
    pub fn is_empty(&self) -> bool {
        self.truncate_from.is_none()
            && self.entries.is_empty()
            && self.hard_state.is_none()
            && self.snapshot.is_none()
            && self.messages.is_empty()
            && self.committed.is_empty()
            && self.reads.is_empty()
            && self.snapshot_wanted.is_empty()
            && self.role_changed.is_none()
    }
}

/// The leader's view of one follower.
#[derive(Clone, Debug)]
struct Progress {
    /// Highest index known replicated there.
    matched: Index,
    /// Next index to send.
    next: Index,
    /// Heard from since the last check-quorum round.
    active: bool,
    /// Waiting for a snapshot to land.
    snapshotting: bool,
    /// Highest read-index round it acknowledged.
    read_ack: u64,
}

/// A read waiting for a quorum's acknowledgement.
#[derive(Clone, Debug)]
struct PendingRead {
    ctx: u64,
    index: Index,
    round: u64,
}

pub struct RaftNode {
    id: NodeId,
    timing: Timing,
    log: RaftLog,
    hard: HardState,
    /// The hard state last handed out for persisting.
    hard_persisted: HardState,
    config: Config,
    /// The index of the newest config entry, and the config before it (to
    /// fall back to if that entry is truncated away).
    config_index: Index,
    config_history: Vec<(Index, Config)>,
    role: Role,
    leader: Option<NodeId>,
    elapsed: u32,
    timeout: u32,
    votes: BTreeMap<NodeId, bool>,
    progress: BTreeMap<NodeId, Progress>,
    msgs: Vec<(NodeId, Message)>,
    truncated_from: Option<Index>,
    pending_snapshot: Option<(SnapshotMeta, Vec<u8>)>,
    pending_snapshot_keeps_log: bool,
    snapshot_wanted: Vec<NodeId>,
    role_changed: Option<Role>,
    /// Read rounds (leader only).
    read_round: u64,
    reads_pending: VecDeque<PendingRead>,
    reads_ready: Vec<ReadState>,
    /// Leadership transfer target, and the ticks since it began.
    transferee: Option<(NodeId, u32)>,
    rng: u64,
}

impl RaftNode {
    /// A node restored from storage: its hard state, its log (`RaftLog::restore`)
    /// and the config in force at the log's end. `seed` fixes its election
    /// jitter (a simulation passes its own; a server, something random).
    pub fn new(id: NodeId, hard: HardState, log: RaftLog, config: Config, timing: Timing, seed: u64) -> RaftNode {
        let role = if config.learners.contains(&id) { Role::Learner } else { Role::Follower };
        let mut n = RaftNode {
            id,
            timing,
            log,
            hard,
            hard_persisted: hard,
            config_index: 0,
            config_history: vec![(0, config.clone())],
            config,
            role,
            leader: None,
            elapsed: 0,
            timeout: 0,
            votes: BTreeMap::new(),
            progress: BTreeMap::new(),
            msgs: Vec::new(),
            truncated_from: None,
            pending_snapshot: None,
            pending_snapshot_keeps_log: false,
            snapshot_wanted: Vec::new(),
            role_changed: None,
            read_round: 0,
            reads_pending: VecDeque::new(),
            reads_ready: Vec::new(),
            transferee: None,
            rng: seed | 1,
        };
        // Config entries in the restored log take effect again.
        for i in n.log.first_index()..=n.log.last_index() {
            if let Some(Entry { data: EntryData::Config(c), .. }) = n.log.entry(i).cloned() {
                n.config_history.push((i, c.clone()));
                n.config = c;
                n.config_index = i;
            }
        }
        n.reset_timeout();
        n
    }

    pub fn id(&self) -> NodeId {
        self.id
    }

    pub fn role(&self) -> Role {
        self.role
    }

    pub fn term(&self) -> Term {
        self.hard.term
    }

    pub fn leader(&self) -> Option<NodeId> {
        self.leader
    }

    pub fn config(&self) -> &Config {
        &self.config
    }

    pub fn log(&self) -> &RaftLog {
        &self.log
    }

    fn rand(&mut self) -> u64 {
        self.rng ^= self.rng << 13;
        self.rng ^= self.rng >> 7;
        self.rng ^= self.rng << 17;
        self.rng
    }

    fn reset_timeout(&mut self) {
        self.elapsed = 0;
        let e = self.timing.election.max(1) as u64;
        self.timeout = (e + self.rand() % e) as u32;
    }

    fn send(&mut self, to: NodeId, m: Message) {
        if to != self.id {
            self.msgs.push((to, m));
        }
    }

    fn set_role(&mut self, role: Role) {
        if self.role != role {
            self.role = role;
            self.role_changed = Some(role);
        }
    }

    fn become_follower(&mut self, term: Term, leader: Option<NodeId>) {
        if term > self.hard.term {
            self.hard.term = term;
            self.hard.vote = None;
        }
        self.leader = leader;
        self.votes.clear();
        self.progress.clear();
        self.transferee = None;
        // Reads in flight can no longer be confirmed by this node.
        self.reads_pending.clear();
        let role = if self.config.learners.contains(&self.id) { Role::Learner } else { Role::Follower };
        self.set_role(role);
        self.reset_timeout();
    }

    /// Whether this node may campaign at all.
    fn promotable(&self) -> bool {
        self.config.voters.contains(&self.id)
    }

    fn campaign(&mut self, pre: bool) {
        self.campaign_as(pre, false);
    }

    fn campaign_as(&mut self, pre: bool, transfer: bool) {
        if !self.promotable() {
            return;
        }
        self.votes.clear();
        self.reset_timeout();
        let term = if pre {
            self.set_role(Role::PreCandidate);
            self.hard.term + 1
        } else {
            self.set_role(Role::Candidate);
            self.hard.term += 1;
            self.hard.vote = Some(self.id);
            self.leader = None;
            self.hard.term
        };
        self.votes.insert(self.id, true);
        if self.quorum_votes() {
            // A single voter wins at once.
            if pre {
                self.campaign_as(false, transfer);
            } else {
                self.become_leader();
            }
            return;
        }
        let (last_index, last_term) = (self.log.last_index(), self.log.last_term());
        let voters: Vec<NodeId> = self.config.voters.iter().copied().collect();
        for v in voters {
            self.send(v, Message::RequestVote { term, from: self.id, last_index, last_term, pre, transfer });
        }
    }

    fn quorum_votes(&self) -> bool {
        self.votes.iter().filter(|&(id, &g)| g && self.config.voters.contains(id)).count() >= self.config.quorum()
    }

    fn quorum_rejected(&self) -> bool {
        let rejected = self.votes.iter().filter(|&(id, &g)| !g && self.config.voters.contains(id)).count();
        rejected > self.config.voters.len() - self.config.quorum()
    }

    fn become_leader(&mut self) {
        self.set_role(Role::Leader);
        self.leader = Some(self.id);
        self.transferee = None;
        self.progress.clear();
        let next = self.log.last_index() + 1;
        let members: Vec<NodeId> = self.config.members().collect();
        for m in members {
            self.progress.insert(m, Progress { matched: 0, next, active: true, snapshotting: false, read_ack: 0 });
        }
        if let Some(p) = self.progress.get_mut(&self.id) {
            p.matched = self.log.persisted();
        }
        // The term's first entry: committing it commits every earlier one.
        let e = Entry { term: self.hard.term, index: next, data: EntryData::Noop };
        self.log.append(e);
        self.broadcast_append();
        self.elapsed = 0;
    }

    /// Advances time by one tick.
    pub fn tick(&mut self) {
        self.elapsed += 1;
        match self.role {
            Role::Leader => {
                if self.elapsed % self.timing.heartbeat.max(1) == 0 {
                    self.broadcast_heartbeat();
                }
                if self.elapsed >= self.timing.election {
                    self.elapsed = 0;
                    self.check_quorum();
                }
                if let Some((t, waited)) = self.transferee {
                    if waited + 1 >= self.timing.election {
                        self.transferee = None; // gave up
                    } else {
                        self.transferee = Some((t, waited + 1));
                    }
                }
            }
            Role::Learner => {}
            _ => {
                if self.elapsed >= self.timeout {
                    self.campaign(true);
                }
            }
        }
    }

    /// Check-quorum: a leader that has not heard from a quorum within an
    /// election timeout steps down, so a partitioned leader stops serving
    /// reads and accepting proposals it cannot commit.
    fn check_quorum(&mut self) {
        let mut active = 0;
        let voters = self.config.voters.clone();
        // A snapshot that drew no answer in a whole round was lost (or its
        // peer restarted): ask for another. Waiting on it, the leader sent
        // that peer nothing at all, not even a heartbeat, for ever.
        let mut resend = Vec::new();
        for (id, p) in self.progress.iter_mut() {
            if *id == self.id || p.active {
                if voters.contains(id) {
                    active += 1;
                }
            } else if p.snapshotting {
                p.snapshotting = false;
                resend.push(*id);
            }
            p.active = false;
        }
        for id in resend {
            self.send_append(id);
        }
        if active < self.config.quorum() {
            let term = self.hard.term;
            self.become_follower(term, None);
        }
    }

    /// Takes a message from a peer.
    pub fn step(&mut self, m: Message) -> Result<(), String> {
        let term = m.term();
        let from = m.from();
        // Terms first (§5.1).
        match &m {
            Message::RequestVote { pre: true, .. } => {
                // A pre-vote never changes anyone's term.
            }
            Message::VoteResponse { pre: true, granted: true, .. } => {}
            _ if term > self.hard.term => {
                // Leader stickiness: a node that heard from a leader within
                // the minimum election timeout ignores a real vote request,
                // unless the leader itself asked it to campaign (TimeoutNow
                // is from the leader, and its candidate's request follows).
                if let Message::RequestVote { pre: false, transfer: false, .. } = m
                    && self.leader.is_some()
                    && self.elapsed < self.timing.election
                    && self.role != Role::Candidate
                {
                    return Ok(());
                }
                let leader = matches!(m, Message::Append { .. } | Message::Snapshot { .. }).then_some(from);
                self.become_follower(term, leader);
            }
            _ if term < self.hard.term => {
                // Stale: answer what tells the sender our term, else ignore.
                match m {
                    Message::Append { .. } => {
                        let t = self.hard.term;
                        self.send(from, Message::AppendResponse { term: t, from: self.id, success: false, index: 0, read: 0 });
                    }
                    Message::RequestVote { pre: false, .. } => {
                        let t = self.hard.term;
                        self.send(from, Message::VoteResponse { term: t, from: self.id, granted: false, pre: false });
                    }
                    _ => {}
                }
                return Ok(());
            }
            _ => {}
        }
        match m {
            Message::RequestVote { term, from, last_index, last_term, pre, .. } => {
                self.on_vote_request(term, from, last_index, last_term, pre);
            }
            Message::VoteResponse { from, granted, pre, term } => self.on_vote_response(from, granted, pre, term),
            Message::Append { from, prev_index, prev_term, entries, commit, read, .. } => {
                self.on_append(from, prev_index, prev_term, entries, commit, read)?;
            }
            Message::AppendResponse { from, success, index, read, .. } => self.on_append_response(from, success, index, read),
            Message::Snapshot { from, meta, data, .. } => self.on_snapshot(from, meta, data),
            Message::SnapshotResponse { from, index, .. } => {
                if self.role == Role::Leader
                    && let Some(p) = self.progress.get_mut(&from)
                {
                    p.active = true;
                    p.snapshotting = false;
                    p.matched = p.matched.max(index);
                    p.next = p.matched + 1;
                    self.send_append(from);
                }
            }
            Message::TimeoutNow { from, term } => {
                if term == self.hard.term && (self.leader == Some(from) || self.leader.is_none()) {
                    self.campaign_as(false, true);
                }
            }
        }
        Ok(())
    }

    fn on_vote_request(&mut self, term: Term, from: NodeId, last_index: Index, last_term: Term, pre: bool) {
        let up_to_date = self.log.is_up_to_date(last_index, last_term);
        let granted = if pre {
            // Would vote: its log is current, and it has no leader it heard
            // from lately (or the request is for a later term than ours).
            up_to_date && term > self.hard.term && (self.leader.is_none() || self.elapsed >= self.timing.election)
        } else {
            let free = self.hard.vote.is_none() || self.hard.vote == Some(from);
            free && up_to_date
        };
        if granted && !pre {
            self.hard.vote = Some(from);
            self.reset_timeout();
        }
        // A granted pre-vote answers at the asked term (the candidate counts
        // it there); a refusal carries this node's own term, so the asker
        // learns of a newer one and nobody's term is raised by a refusal.
        let reply_term = if pre && granted { term } else { self.hard.term };
        self.send(from, Message::VoteResponse { term: reply_term, from: self.id, granted, pre });
    }

    fn on_vote_response(&mut self, from: NodeId, granted: bool, pre: bool, term: Term) {
        match (self.role, pre) {
            (Role::PreCandidate, true) if term == self.hard.term + 1 => {
                self.votes.insert(from, granted);
                if self.quorum_votes() {
                    self.campaign(false);
                } else if self.quorum_rejected() {
                    let t = self.hard.term;
                    self.become_follower(t, None);
                }
            }
            (Role::Candidate, false) if term == self.hard.term => {
                self.votes.insert(from, granted);
                if self.quorum_votes() {
                    self.become_leader();
                } else if self.quorum_rejected() {
                    let t = self.hard.term;
                    self.become_follower(t, None);
                }
            }
            _ => {}
        }
    }

    fn on_append(
        &mut self,
        from: NodeId,
        prev_index: Index,
        prev_term: Term,
        entries: Vec<Entry>,
        commit: Index,
        read: u64,
    ) -> Result<(), String> {
        if self.role != Role::Follower && self.role != Role::Learner {
            let t = self.hard.term;
            self.become_follower(t, Some(from));
        }
        self.leader = Some(from);
        self.elapsed = 0;
        let term = self.hard.term;
        match self.log.maybe_append(prev_index, prev_term, &entries)? {
            Some((matched, replaced)) => {
                if let Some(at) = replaced {
                    self.truncated_from = Some(self.truncated_from.map_or(at, |t| t.min(at)));
                    self.rewind_config(at);
                }
                for e in &entries {
                    if let EntryData::Config(c) = &e.data
                        && e.index > self.config_index
                    {
                        self.apply_config(e.index, c.clone());
                    }
                }
                // Commit only what this node knows matches the leader.
                self.log.commit_to(commit.min(matched));
                self.send(from, Message::AppendResponse { term, from: self.id, success: true, index: matched, read });
            }
            None => {
                let hint = self.log.last_index().min(prev_index.saturating_sub(1));
                self.send(from, Message::AppendResponse { term, from: self.id, success: false, index: hint, read });
            }
        }
        Ok(())
    }

    fn on_append_response(&mut self, from: NodeId, success: bool, index: Index, read: u64) {
        if self.role != Role::Leader {
            return;
        }
        let max_append = self.timing.max_append;
        let Some(p) = self.progress.get_mut(&from) else {
            return;
        };
        p.active = true;
        p.read_ack = p.read_ack.max(read);
        if success {
            if index > p.matched {
                p.matched = index;
            }
            p.next = p.next.max(index + 1);
            let caught_up_transferee = self.transferee.map(|(t, _)| t) == Some(from) && p.matched == self.log.last_index();
            self.maybe_commit();
            if caught_up_transferee {
                let t = self.hard.term;
                self.send(from, Message::TimeoutNow { term: t, from: self.id });
            }
            if self.progress.get(&from).is_some_and(|p| p.next <= self.log.last_index()) {
                self.send_append(from);
            }
        } else {
            // Back up to the follower's hint, at least one step, and never to
            // or below what it is known to match. A stale rejection only costs
            // a resend.
            let _ = max_append;
            p.next = (index + 1).min(p.next.saturating_sub(1)).max(p.matched + 1).max(1);
            self.send_append(from);
        }
        self.confirm_reads();
    }

    fn on_snapshot(&mut self, from: NodeId, meta: SnapshotMeta, data: Vec<u8>) {
        self.leader = Some(from);
        self.elapsed = 0;
        let term = self.hard.term;
        if meta.index <= self.log.committed() {
            // Already have it.
            let c = self.log.committed();
            self.send(from, Message::SnapshotResponse { term, from: self.id, index: c });
            return;
        }
        self.pending_snapshot_keeps_log = self.log.restore_snapshot(meta.index, meta.term);
        // The snapshot's own record says what of the durable log stays.
        self.truncated_from = None;
        self.config = meta.config.clone();
        self.config_index = meta.index;
        self.config_history = vec![(meta.index, meta.config.clone())];
        let role = if self.config.learners.contains(&self.id) { Role::Learner } else { Role::Follower };
        self.set_role(role);
        self.pending_snapshot = Some((meta.clone(), data));
        self.send(from, Message::SnapshotResponse { term, from: self.id, index: meta.index });
    }

    fn apply_config(&mut self, index: Index, c: Config) {
        self.config_history.push((index, c.clone()));
        self.config = c;
        self.config_index = index;
        if self.role == Role::Leader {
            let next = self.log.last_index() + 1;
            let members: Vec<NodeId> = self.config.members().collect();
            for m in &members {
                self.progress.entry(*m).or_insert(Progress { matched: 0, next, active: true, snapshotting: false, read_ack: 0 });
            }
            self.progress.retain(|id, _| members.contains(id));
            if !self.config.voters.contains(&self.id) {
                // Removed: step down once the change commits (handled in
                // maybe_commit); until then keep replicating it.
            }
        } else if self.config.learners.contains(&self.id) {
            self.set_role(Role::Learner);
        } else if self.role == Role::Learner && self.config.voters.contains(&self.id) {
            self.set_role(Role::Follower);
        }
    }

    /// A truncation removed config entries at or past `at`: the config falls
    /// back to the newest one before it.
    fn rewind_config(&mut self, at: Index) {
        while self.config_history.len() > 1 && self.config_history.last().is_some_and(|(i, _)| *i >= at) {
            self.config_history.pop();
        }
        let (i, c) = self.config_history.last().cloned().unwrap_or_default();
        self.config_index = i;
        self.config = c;
    }

    fn maybe_commit(&mut self) {
        if self.role != Role::Leader {
            return;
        }
        let mut matched: Vec<Index> = self
            .config
            .voters
            .iter()
            .map(|v| if *v == self.id { self.log.persisted() } else { self.progress.get(v).map_or(0, |p| p.matched) })
            .collect();
        matched.sort_unstable_by(|a, b| b.cmp(a));
        let Some(&n) = matched.get(self.config.quorum() - 1) else {
            return;
        };
        // Only an entry of this term commits by counting (§5.4.2).
        if self.log.term(n) == Some(self.hard.term) && self.log.commit_to(n) {
            self.broadcast_append();
            // A leader the committed config removed steps down.
            if self.config_index <= self.log.committed() && !self.config.voters.contains(&self.id) {
                let t = self.hard.term;
                self.become_follower(t, None);
            }
        }
    }

    fn send_append(&mut self, to: NodeId) {
        let Some(p) = self.progress.get(&to).cloned() else {
            return;
        };
        if p.snapshotting {
            return;
        }
        let prev_index = p.next - 1;
        let Some(prev_term) = self.log.term(prev_index) else {
            // Compacted past what the follower needs.
            if let Some(p) = self.progress.get_mut(&to) {
                p.snapshotting = true;
            }
            if !self.snapshot_wanted.contains(&to) {
                self.snapshot_wanted.push(to);
            }
            return;
        };
        // Flow control: no more than max_inflight ahead of what it matched.
        let window_end = p.matched.saturating_add(self.timing.max_inflight).max(p.next);
        let hi = self.log.last_index().min(window_end);
        let entries = self.log.slice(p.next, hi, self.timing.max_append);
        if let Some(last) = entries.last()
            && let Some(p) = self.progress.get_mut(&to)
        {
            p.next = last.index + 1;
        }
        let m = Message::Append {
            term: self.hard.term,
            from: self.id,
            prev_index,
            prev_term,
            entries,
            commit: self.log.committed(),
            read: self.read_round,
        };
        self.send(to, m);
    }

    fn broadcast_append(&mut self) {
        let peers: Vec<NodeId> = self.progress.keys().copied().filter(|&p| p != self.id).collect();
        for p in peers {
            self.send_append(p);
        }
        // A cluster of one commits on its own.
        self.maybe_commit();
    }

    fn broadcast_heartbeat(&mut self) {
        let peers: Vec<NodeId> = self.progress.keys().copied().filter(|&p| p != self.id).collect();
        for to in peers {
            let p = self.progress[&to].clone();
            let prev_index = p.matched.min(self.log.last_index());
            let Some(prev_term) = self.log.term(prev_index) else {
                self.send_append(to);
                continue;
            };
            let m = Message::Append {
                term: self.hard.term,
                from: self.id,
                prev_index,
                prev_term,
                entries: Vec::new(),
                commit: self.log.committed().min(p.matched),
                read: self.read_round,
            };
            self.send(to, m);
        }
    }

    /// Proposes a command; returns its index. Leader only.
    pub fn propose(&mut self, data: Vec<u8>) -> Result<Index, ProposeError> {
        self.propose_entry(EntryData::Command(data))
    }

    fn propose_entry(&mut self, data: EntryData) -> Result<Index, ProposeError> {
        if self.role != Role::Leader {
            return Err(ProposeError::NotLeader { leader: self.leader });
        }
        if self.transferee.is_some() {
            return Err(ProposeError::Transferring);
        }
        let index = self.log.last_index() + 1;
        let config = if let EntryData::Config(c) = &data { Some(c.clone()) } else { None };
        self.log.append(Entry { term: self.hard.term, index, data });
        if let Some(c) = config {
            self.apply_config(index, c);
        }
        self.broadcast_append();
        Ok(index)
    }

    /// Changes membership by one server at a time: add or remove one voter,
    /// or add, promote or remove one learner. Leader only, one at a time.
    pub fn change_config(&mut self, next: Config) -> Result<Index, ProposeError> {
        if self.role != Role::Leader {
            return Err(ProposeError::NotLeader { leader: self.leader });
        }
        if self.config_index > self.log.committed() {
            return Err(ProposeError::ConfigInFlight);
        }
        if next.voters.is_empty() {
            return Err(ProposeError::BadConfig("a configuration needs a voter".into()));
        }
        let changed = self.config.voters.symmetric_difference(&next.voters).count();
        if changed > 1 {
            return Err(ProposeError::BadConfig("change the voters one server at a time".into()));
        }
        if next.voters.iter().any(|v| next.learners.contains(v)) {
            return Err(ProposeError::BadConfig("a node is a voter or a learner, not both".into()));
        }
        self.propose_entry(EntryData::Config(next))
    }

    /// Hands leadership to `to` once it has caught up (dissertation §3.10).
    pub fn transfer_leader(&mut self, to: NodeId) -> Result<(), ProposeError> {
        if self.role != Role::Leader {
            return Err(ProposeError::NotLeader { leader: self.leader });
        }
        if !self.config.voters.contains(&to) || to == self.id {
            return Err(ProposeError::BadConfig(format!("{to} is not another voter")));
        }
        self.transferee = Some((to, 0));
        if self.progress.get(&to).is_some_and(|p| p.matched == self.log.last_index()) {
            let t = self.hard.term;
            self.send(to, Message::TimeoutNow { term: t, from: self.id });
        } else {
            self.send_append(to);
        }
        Ok(())
    }

    /// Asks to serve a linearizable read tagged `ctx`: answered in a later
    /// ready's `reads` once a quorum has confirmed this node still leads,
    /// with the index the state machine must have applied first.
    pub fn read_index(&mut self, ctx: u64) -> Result<(), ProposeError> {
        if self.role != Role::Leader {
            return Err(ProposeError::NotLeader { leader: self.leader });
        }
        // Not until this term has committed an entry: before that the commit
        // index may be behind entries earlier leaders committed.
        if self.log.term(self.log.committed()) != Some(self.hard.term) {
            return Err(ProposeError::NotLeader { leader: Some(self.id) });
        }
        self.read_round += 1;
        let index = self.log.committed();
        self.reads_pending.push_back(PendingRead { ctx, index, round: self.read_round });
        if self.config.voters.len() == 1 {
            self.confirm_reads();
        } else {
            self.broadcast_heartbeat();
        }
        Ok(())
    }

    fn confirm_reads(&mut self) {
        if self.role != Role::Leader {
            return;
        }
        while let Some(r) = self.reads_pending.front().cloned() {
            let acks = self
                .config
                .voters
                .iter()
                .filter(|v| **v == self.id || self.progress.get(v).is_some_and(|p| p.read_ack >= r.round))
                .count();
            if acks < self.config.quorum() {
                break;
            }
            self.reads_pending.pop_front();
            self.reads_ready.push(ReadState { ctx: r.ctx, index: r.index });
        }
    }

    /// The caller has a snapshot for peer `to` (asked for in a ready's
    /// `snapshot_wanted`): the state machine as of `meta`.
    pub fn send_snapshot(&mut self, to: NodeId, meta: SnapshotMeta, data: Vec<u8>) {
        if self.role != Role::Leader {
            return;
        }
        let t = self.hard.term;
        self.send(to, Message::Snapshot { term: t, from: self.id, meta, data });
    }

    /// The state machine has a snapshot through `index`: the log before it
    /// may go.
    pub fn compact(&mut self, index: Index) -> Result<(), String> {
        self.log.compact(index)
    }

    /// What to do now (module docs). Calling it again before
    /// [`Self::advance`] returns the same durable work and any new messages.
    pub fn ready(&mut self) -> Ready {
        Ready {
            truncate_from: self.truncated_from,
            entries: self.log.unpersisted(),
            hard_state: (self.hard_with_commit() != self.hard_persisted).then(|| self.hard_with_commit()),
            snapshot: self.pending_snapshot.clone(),
            snapshot_keeps_log: self.pending_snapshot_keeps_log,
            messages: std::mem::take(&mut self.msgs),
            committed: self.log.to_apply(usize::MAX),
            reads: std::mem::take(&mut self.reads_ready),
            snapshot_wanted: std::mem::take(&mut self.snapshot_wanted),
            role_changed: self.role_changed.take(),
        }
    }

    fn hard_with_commit(&self) -> HardState {
        HardState { commit: self.log.committed(), ..self.hard }
    }

    /// The caller did what `ready` said.
    pub fn advance(&mut self, ready: &Ready) -> Result<(), String> {
        if ready.truncate_from.is_some() && ready.truncate_from == self.truncated_from {
            self.truncated_from = None;
        }
        if let Some(last) = ready.entries.last() {
            self.log.mark_persisted(last.index, last.term);
        }
        if let Some(h) = ready.hard_state {
            self.hard_persisted = h;
        }
        if ready.snapshot.is_some() {
            self.pending_snapshot = None;
        }
        if let Some(last) = ready.committed.last() {
            self.log.applied_to(last.index)?;
        }
        // The leader's own persistence counts toward its quorum.
        if self.role == Role::Leader {
            if let Some(p) = self.progress.get_mut(&self.id) {
                p.matched = self.log.persisted();
            }
            self.maybe_commit();
        }
        Ok(())
    }
}
