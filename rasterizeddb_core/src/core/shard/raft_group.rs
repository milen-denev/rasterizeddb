//! A shard's Raft group at run time (sharding-plan.md B3): the core
//! (`rasterizeddb_raft::RaftNode`) driven by one task that owns it.
//!
//! The task's loop, one turn per event (a tick, a peer's message, a
//! proposal, a read):
//! 1. hands the event to the node;
//! 2. takes the node's `Ready` and does what it says in the core's order:
//!    persist through [`RaftStore`] (on a blocking thread, synced), send the
//!    messages through the [`Transport`], apply the committed entries
//!    through the [`StateMachine`], in order;
//! 3. answers the proposals and reads the ready settled.
//!
//! **A proposal is answered only once its entry is applied here**, with the
//! entry's own index and term: an entry replaced by a new leader's (same
//! index, other term) fails its proposer, never succeeds it. On step-down
//! every proposal still waiting fails (`NotLeader`); its entry may still
//! commit under the next leader, and the state machine applies it then,
//! which is why commands carry their origin (the plan's "outcomes are
//! decided by log id, never TxId").

use std::collections::BTreeMap;
use std::sync::Arc;

use rasterizeddb_raft::{Config, Entry, EntryData, Index, Message, NodeId, RaftNode, Ready, Role, SnapshotMeta, Term, Timing};
use tokio::sync::{mpsc, oneshot};

use super::raft_store::RaftStore;

/// Applies committed commands to the shard's tables. Called in log order on
/// every replica (the leader included).
///
/// **Idempotent by index.** After a restart the group applies again every
/// committed entry past its snapshot, so the machine keeps its own durable
/// applied index (the plan's insert-only `__shard_applied`), written in the
/// same storage transaction as each entry's effects, and skips any index at
/// or below it.
pub trait StateMachine: Send + 'static {
    /// Applies one committed command. An error halts the replica: a state
    /// machine that cannot apply what a quorum committed has diverged.
    fn apply(&mut self, index: Index, command: &[u8]) -> Result<(), String>;
    /// The whole state as of the last applied index, for a follower too far
    /// behind for the log.
    fn snapshot(&mut self) -> Result<Vec<u8>, String>;
    /// Replaces the whole state with a snapshot's.
    fn restore(&mut self, meta: &SnapshotMeta, data: &[u8]) -> Result<(), String>;
}

/// Delivers messages to peers; best effort (Raft retries).
pub trait Transport: Send + Sync + 'static {
    fn send(&self, to: NodeId, message: Message);
}

/// Why a proposal or read did not succeed.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum GroupError {
    NotLeader { leader: Option<NodeId> },
    /// Replaced by another leader's entry, or lost on step-down: it may or
    /// may not apply; the caller treats the outcome as unknown.
    Lost,
    Halted(String),
    Other(String),
}

/// What the group reports.
#[derive(Clone, Debug, Default)]
pub struct Status {
    pub id: NodeId,
    pub role: Option<Role>,
    pub term: Term,
    pub leader: Option<NodeId>,
    pub committed: Index,
    pub applied: Index,
    pub last_index: Index,
    pub voters: Vec<NodeId>,
    pub learners: Vec<NodeId>,
    pub halted: Option<String>,
}

enum Event {
    Tick,
    Message(Message),
    Propose(Vec<u8>, oneshot::Sender<Result<Index, GroupError>>),
    Read(oneshot::Sender<Result<Index, GroupError>>),
    ChangeConfig(Config, oneshot::Sender<Result<Index, GroupError>>),
    Transfer(NodeId, oneshot::Sender<Result<(), GroupError>>),
}

/// A handle on a running group.
#[derive(Clone)]
pub struct Group {
    events: mpsc::Sender<Event>,
    status: Arc<std::sync::Mutex<Status>>,
}

/// How often the node ticks.
pub const TICK: std::time::Duration = std::time::Duration::from_millis(100);

impl Group {
    /// Starts the group's task over `store`'s restored state.
    pub fn start(
        id: NodeId,
        store: RaftStore,
        restored: super::raft_store::Restored,
        initial: Config,
        timing: Timing,
        machine: Box<dyn StateMachine>,
        transport: Arc<dyn Transport>,
    ) -> Group {
        let config = restored.snapshot.as_ref().map_or(initial, |(m, _)| m.config.clone());
        let seed = id.wrapping_mul(0x9E37_79B9_7F4A_7C15) ^ std::process::id() as u64;
        let node = RaftNode::new(id, restored.hard, restored.log, config, timing, seed);
        let (tx, rx) = mpsc::channel(4096);
        let status = Arc::new(std::sync::Mutex::new(Status { id, ..Default::default() }));
        let runner = Runner {
            node,
            store: Some(store),
            machine,
            transport,
            waiting: BTreeMap::new(),
            reads: BTreeMap::new(),
            next_read: 1,
            status: status.clone(),
            halted: None,
        };
        tokio::spawn(runner.run(rx, tx.clone()));
        Group { events: tx, status }
    }

    /// A peer's message.
    pub async fn deliver(&self, m: Message) {
        let _ = self.events.send(Event::Message(m)).await;
    }

    /// Proposes `command`; resolves once it is applied here.
    pub async fn propose(&self, command: Vec<u8>) -> Result<Index, GroupError> {
        let (tx, rx) = oneshot::channel();
        self.events.send(Event::Propose(command, tx)).await.map_err(|_| GroupError::Halted("the group stopped".into()))?;
        rx.await.map_err(|_| GroupError::Lost)?
    }

    /// A linearizable read point: resolves with an index the state machine
    /// has applied through, once a quorum has confirmed this node leads.
    pub async fn read_index(&self) -> Result<Index, GroupError> {
        let (tx, rx) = oneshot::channel();
        self.events.send(Event::Read(tx)).await.map_err(|_| GroupError::Halted("the group stopped".into()))?;
        rx.await.map_err(|_| GroupError::Lost)?
    }

    pub async fn change_config(&self, next: Config) -> Result<Index, GroupError> {
        let (tx, rx) = oneshot::channel();
        self.events.send(Event::ChangeConfig(next, tx)).await.map_err(|_| GroupError::Halted("the group stopped".into()))?;
        rx.await.map_err(|_| GroupError::Lost)?
    }

    pub async fn transfer_leader(&self, to: NodeId) -> Result<(), GroupError> {
        let (tx, rx) = oneshot::channel();
        self.events.send(Event::Transfer(to, tx)).await.map_err(|_| GroupError::Halted("the group stopped".into()))?;
        rx.await.map_err(|_| GroupError::Lost)?
    }

    pub fn status(&self) -> Status {
        self.status.lock().unwrap().clone()
    }
}

struct Runner {
    node: RaftNode,
    store: Option<RaftStore>,
    machine: Box<dyn StateMachine>,
    transport: Arc<dyn Transport>,
    /// Proposals by index, with the term their entry was appended in.
    waiting: BTreeMap<Index, (Term, oneshot::Sender<Result<Index, GroupError>>)>,
    /// Reads by context, answered once the machine applied through their index.
    reads: BTreeMap<u64, (Option<Index>, oneshot::Sender<Result<Index, GroupError>>)>,
    next_read: u64,
    status: Arc<std::sync::Mutex<Status>>,
    halted: Option<String>,
}

fn not_leader(e: rasterizeddb_raft::ProposeError) -> GroupError {
    match e {
        rasterizeddb_raft::ProposeError::NotLeader { leader } => GroupError::NotLeader { leader },
        other => GroupError::Other(format!("{other:?}")),
    }
}

impl Runner {
    async fn run(mut self, mut rx: mpsc::Receiver<Event>, tx: mpsc::Sender<Event>) {
        let ticker = tx.clone();
        tokio::spawn(async move {
            let mut every = tokio::time::interval(TICK);
            loop {
                every.tick().await;
                if ticker.send(Event::Tick).await.is_err() {
                    return;
                }
            }
        });
        drop(tx);
        while let Some(event) = rx.recv().await {
            if let Some(why) = &self.halted {
                self.refuse(event, why.clone());
                continue;
            }
            self.on_event(event);
            if let Err(e) = self.drive().await {
                log::error!("raft group {} halted: {e}", self.node.id());
                self.halted = Some(e.clone());
                for (_, (_, w)) in std::mem::take(&mut self.waiting) {
                    let _ = w.send(Err(GroupError::Halted(e.clone())));
                }
                for (_, (_, w)) in std::mem::take(&mut self.reads) {
                    let _ = w.send(Err(GroupError::Halted(e.clone())));
                }
            }
            self.publish_status();
        }
    }

    fn refuse(&self, event: Event, why: String) {
        let e = GroupError::Halted(why);
        match event {
            Event::Propose(_, w) | Event::Read(w) | Event::ChangeConfig(_, w) => {
                let _ = w.send(Err(e));
            }
            Event::Transfer(_, w) => {
                let _ = w.send(Err(e));
            }
            _ => {}
        }
    }

    fn on_event(&mut self, event: Event) {
        match event {
            Event::Tick => self.node.tick(),
            Event::Message(m) => {
                if let Err(e) = self.node.step(m) {
                    // A leader asked to replace a committed entry: a bug or a
                    // forged message. Refused; nothing changed.
                    log::error!("raft group {}: {e}", self.node.id());
                }
            }
            Event::Propose(cmd, w) => match self.node.propose(cmd) {
                Ok(index) => {
                    let term = self.node.term();
                    self.waiting.insert(index, (term, w));
                }
                Err(e) => {
                    let _ = w.send(Err(not_leader(e)));
                }
            },
            Event::ChangeConfig(c, w) => match self.node.change_config(c) {
                Ok(index) => {
                    let term = self.node.term();
                    self.waiting.insert(index, (term, w));
                }
                Err(e) => {
                    let _ = w.send(Err(not_leader(e)));
                }
            },
            Event::Read(w) => {
                let ctx = self.next_read;
                self.next_read += 1;
                match self.node.read_index(ctx) {
                    Ok(()) => {
                        self.reads.insert(ctx, (None, w));
                    }
                    Err(e) => {
                        let _ = w.send(Err(not_leader(e)));
                    }
                }
            }
            Event::Transfer(to, w) => {
                let _ = w.send(self.node.transfer_leader(to).map_err(not_leader));
            }
        }
    }

    /// Handles readies until there is nothing left to do.
    async fn drive(&mut self) -> Result<(), String> {
        loop {
            let ready = self.node.ready();
            if ready.is_empty() {
                return Ok(());
            }
            self.handle(&ready).await?;
            self.node.advance(&ready)?;
        }
    }

    async fn handle(&mut self, ready: &Ready) -> Result<(), String> {
        if let Some(role) = ready.role_changed
            && role != Role::Leader
        {
            for (_, (_, w)) in std::mem::take(&mut self.waiting) {
                let _ = w.send(Err(GroupError::Lost));
            }
            for (_, (_, w)) in std::mem::take(&mut self.reads) {
                let _ = w.send(Err(GroupError::NotLeader { leader: self.node.leader() }));
            }
        }
        // 1. Durable first, off the runtime's threads.
        if let Some((meta, data)) = &ready.snapshot {
            self.machine.restore(meta, data)?;
        }
        let mut store = self.store.take().ok_or("internal: the raft store is gone")?;
        // Only what storage needs crosses to the blocking thread.
        let (snapshot, truncate_from, entries, hard_state) =
            (ready.snapshot.clone(), ready.truncate_from, ready.entries.clone(), ready.hard_state);
        let nothing = snapshot.is_none() && truncate_from.is_none() && entries.is_empty() && hard_state.is_none();
        let (store, result) = if nothing {
            (store, Ok(()))
        } else {
            tokio::task::spawn_blocking(move || {
                let r = store.persist_parts(snapshot.as_ref(), truncate_from, &entries, hard_state.as_ref());
                (store, r)
            })
            .await
            .map_err(|e| format!("the raft store's thread failed: {e}"))?
        };
        self.store = Some(store);
        result?;
        // 2. Then the messages.
        for (to, m) in &ready.messages {
            self.transport.send(*to, m.clone());
        }
        // 3. Then the committed entries, in order.
        for e in &ready.committed {
            self.apply(e)?;
        }
        // Snapshots peers asked for.
        for to in &ready.snapshot_wanted {
            // A machine that cannot snapshot leaves the follower to be
            // relocated from a backup; the leader goes on.
            let data = match self.machine.snapshot() {
                Ok(d) => d,
                Err(e) => {
                    log::warn!("raft group {}: peer {to} needs a snapshot: {e}", self.node.id());
                    continue;
                }
            };
            let index = self.node.log().applied();
            let term = self.node.log().term(index).unwrap_or(self.node.log().snapshot_term());
            let meta = SnapshotMeta { index, term, config: self.node.config().clone() };
            self.node.send_snapshot(*to, meta, data);
        }
        // Reads: their index known now, answered once applied through it.
        for r in &ready.reads {
            if let Some(slot) = self.reads.get_mut(&r.ctx) {
                slot.0 = Some(r.index);
            }
        }
        let applied = ready.committed.last().map_or(self.node.log().applied(), |e| e.index);
        let done: Vec<u64> = self.reads.iter().filter(|(_, (i, _))| i.is_some_and(|i| i <= applied)).map(|(c, _)| *c).collect();
        for c in done {
            if let Some((Some(i), w)) = self.reads.remove(&c) {
                let _ = w.send(Ok(i));
            }
        }
        Ok(())
    }

    fn apply(&mut self, e: &Entry) -> Result<(), String> {
        if let EntryData::Command(cmd) = &e.data {
            self.machine.apply(e.index, cmd)?;
        }
        if let Some((term, w)) = self.waiting.remove(&e.index) {
            // Ours only if the entry applied is the one we appended.
            let _ = w.send(if term == e.term { Ok(e.index) } else { Err(GroupError::Lost) });
        }
        // Anything waiting below an applied index whose entry was replaced.
        let stale: Vec<Index> = self.waiting.range(..e.index).map(|(i, _)| *i).collect();
        for i in stale {
            if let Some((_, w)) = self.waiting.remove(&i) {
                let _ = w.send(Err(GroupError::Lost));
            }
        }
        Ok(())
    }

    fn publish_status(&self) {
        let mut s = self.status.lock().unwrap();
        s.role = Some(self.node.role());
        s.term = self.node.term();
        s.leader = self.node.leader();
        s.committed = self.node.log().committed();
        s.applied = self.node.log().applied();
        s.last_index = self.node.log().last_index();
        s.voters = self.node.config().voters.iter().copied().collect();
        s.learners = self.node.config().learners.iter().copied().collect();
        s.halted = self.halted.clone();
    }
}
