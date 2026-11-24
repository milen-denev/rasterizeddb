//! Raft's messages and records.

use std::collections::BTreeSet;

pub type NodeId = u64;
pub type Term = u64;
pub type Index = u64;

/// What a log entry carries.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum EntryData {
    /// A new leader's first entry: committing it commits everything before
    /// it (Raft §5.4.2) and is what lets the leader serve reads.
    Noop,
    /// A state-machine command: opaque here.
    Command(Vec<u8>),
    /// The cluster's membership from this entry on. In effect as soon as it
    /// is appended to a log, committed or not (dissertation §4.1); at most
    /// one may be uncommitted at a time.
    Config(Config),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Entry {
    pub term: Term,
    pub index: Index,
    pub data: EntryData,
}

/// Who votes and who only follows.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Config {
    pub voters: BTreeSet<NodeId>,
    /// Receive the log, never vote, never count toward a quorum: a new
    /// replica catching up (learner relocation).
    pub learners: BTreeSet<NodeId>,
}

impl Config {
    pub fn new(voters: impl IntoIterator<Item = NodeId>) -> Config {
        Config { voters: voters.into_iter().collect(), learners: BTreeSet::new() }
    }

    pub fn quorum(&self) -> usize {
        self.voters.len() / 2 + 1
    }

    pub fn contains(&self, id: NodeId) -> bool {
        self.voters.contains(&id) || self.learners.contains(&id)
    }

    pub fn members(&self) -> impl Iterator<Item = NodeId> + '_ {
        self.voters.iter().chain(&self.learners).copied()
    }
}

/// What must survive a crash besides the log: the term, the vote cast in
/// it, and how far the log is known committed.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct HardState {
    pub term: Term,
    pub vote: Option<NodeId>,
    pub commit: Index,
}

/// A snapshot's position: the state machine as of `index`, whose entry had
/// `term`, under `config`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SnapshotMeta {
    pub index: Index,
    pub term: Term,
    pub config: Config,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Message {
    /// A vote request. `pre`: a pre-vote (§9.6 of the dissertation), which
    /// asks whether a vote *would* be granted at `term` without anyone
    /// changing term, so a partitioned node cannot depose a healthy leader.
    /// `transfer`: sent by a node the leader asked to take over
    /// (`TimeoutNow`), so voters do not ignore it for having a leader.
    RequestVote { term: Term, from: NodeId, last_index: Index, last_term: Term, pre: bool, transfer: bool },
    VoteResponse { term: Term, from: NodeId, granted: bool, pre: bool },
    /// Entries after `prev_index` (whose term is `prev_term`); empty for a
    /// heartbeat. `read` is the newest read-index round the leader asks to be
    /// acknowledged.
    Append { term: Term, from: NodeId, prev_index: Index, prev_term: Term, entries: Vec<Entry>, commit: Index, read: u64 },
    /// `success`: the log matches through `index`. Otherwise `index` is the
    /// follower's hint: the last index it could possibly match.
    AppendResponse { term: Term, from: NodeId, success: bool, index: Index, read: u64 },
    /// The state machine as of `meta`, whole.
    Snapshot { term: Term, from: NodeId, meta: SnapshotMeta, data: Vec<u8> },
    SnapshotResponse { term: Term, from: NodeId, index: Index },
    /// Leadership transfer: start an election now, ignoring the leader lease.
    TimeoutNow { term: Term, from: NodeId },
}

impl Message {
    pub fn term(&self) -> Term {
        match self {
            Message::RequestVote { term, .. }
            | Message::VoteResponse { term, .. }
            | Message::Append { term, .. }
            | Message::AppendResponse { term, .. }
            | Message::Snapshot { term, .. }
            | Message::SnapshotResponse { term, .. }
            | Message::TimeoutNow { term, .. } => *term,
        }
    }

    pub fn from(&self) -> NodeId {
        match self {
            Message::RequestVote { from, .. }
            | Message::VoteResponse { from, .. }
            | Message::Append { from, .. }
            | Message::AppendResponse { from, .. }
            | Message::Snapshot { from, .. }
            | Message::SnapshotResponse { from, .. }
            | Message::TimeoutNow { from, .. } => *from,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Role {
    Follower,
    PreCandidate,
    Candidate,
    Leader,
    /// A learner never campaigns.
    Learner,
}

/// A read the leader has confirmed it may serve once the state machine has
/// applied through `index` (ReadIndex, dissertation §6.4).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ReadState {
    pub ctx: u64,
    pub index: Index,
}

/// Why a proposal was refused.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ProposeError {
    /// Only the leader proposes; `leader` is who it is believed to be.
    NotLeader { leader: Option<NodeId> },
    /// A membership change is already in flight.
    ConfigInFlight,
    /// A leadership transfer is in progress; proposals wait for its end.
    Transferring,
    /// The change is not one server at a time, or makes no voters.
    BadConfig(String),
}
