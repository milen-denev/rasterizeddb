//! A deterministic simulation of the Raft core (sharding-plan.md B3): nodes
//! driven through `tick`/`step`/`ready`/`advance` over a seeded network
//! that drops, delays and reorders messages and partitions the cluster,
//! with crashes that keep only what `Ready` said to make durable, local
//! log compaction, and snapshots sent to lagging followers.
//!
//! Checked at every step:
//! - **Election safety:** at most one leader per term.
//! - **State-machine safety:** every node's applied commands are a prefix of
//!   every other's, or the other way round.
//! - **Durability of commits:** a command any node applied is applied by
//!   every node that gets that far.
//!
//! And at the end, once the network is healed: a leader exists and every
//! node has applied every command acknowledged as committed.

use std::collections::BTreeMap;

use rasterizeddb_raft::*;

/// xorshift64*: reproducible from the seed.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n.max(1)
    }
    fn chance(&mut self, percent: u64) -> bool {
        self.below(100) < percent
    }
}

/// What survives a crash: exactly what `Ready` said to persist.
#[derive(Clone, Default)]
struct Disk {
    hard: HardState,
    snap_index: Index,
    snap_term: Term,
    /// The state machine as of `snap_index`.
    snap_data: Vec<u64>,
    entries: Vec<Entry>,
}

struct Node {
    raft: Option<RaftNode>,
    disk: Disk,
    /// Applied commands, in order.
    applied: Vec<u64>,
}

fn encode(sm: &[u64]) -> Vec<u8> {
    sm.iter().flat_map(|v| v.to_le_bytes()).collect()
}

fn decode(data: &[u8]) -> Vec<u64> {
    data.chunks_exact(8).map(|c| u64::from_le_bytes(c.try_into().unwrap())).collect()
}

struct Sim {
    rng: Rng,
    nodes: BTreeMap<NodeId, Node>,
    config: Config,
    timing: Timing,
    /// In flight: (deliver at step, to, message).
    net: Vec<(u64, NodeId, Message)>,
    /// Pairs that cannot talk.
    cut: Vec<(NodeId, NodeId)>,
    step: u64,
    leaders: BTreeMap<Term, NodeId>,
    next_command: u64,
    /// Commands some node applied: they must survive.
    committed: Vec<u64>,
    drop_percent: u64,
}

impl Sim {
    fn new(seed: u64, n: u64) -> Sim {
        let config = Config::new(1..=n);
        let timing = Timing { election: 10, heartbeat: 2, max_append: 16, max_inflight: 64 };
        let mut sim = Sim {
            rng: Rng(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1),
            nodes: BTreeMap::new(),
            config,
            timing,
            net: Vec::new(),
            cut: Vec::new(),
            step: 0,
            leaders: BTreeMap::new(),
            next_command: 1,
            committed: Vec::new(),
            drop_percent: 5,
        };
        for id in 1..=n {
            sim.nodes.insert(id, Node { raft: None, disk: Disk::default(), applied: Vec::new() });
            sim.start(id);
        }
        sim
    }

    /// (Re)starts `id` from its disk: the state machine from its snapshot,
    /// the log and hard state as persisted.
    fn start(&mut self, id: NodeId) {
        let seed = self.rng.next();
        let node = self.nodes.get_mut(&id).unwrap();
        let d = &node.disk;
        let log = RaftLog::restore(d.snap_index, d.snap_term, d.entries.clone(), d.hard.commit.max(d.snap_index))
            .unwrap_or_else(|e| panic!("node {id} could not restore its log: {e}"));
        node.applied = d.snap_data.clone();
        node.raft = Some(RaftNode::new(id, d.hard, log, self.config.clone(), self.timing.clone(), seed));
    }

    fn crash(&mut self, id: NodeId) {
        let node = self.nodes.get_mut(&id).unwrap();
        node.raft = None;
        node.applied.clear();
        // What it sent and what was sent to it is lost with it.
        self.net.retain(|(_, to, m)| *to != id && m.from() != id);
    }

    fn connected(&self, a: NodeId, b: NodeId) -> bool {
        !self.cut.iter().any(|&(x, y)| (x == a && y == b) || (x == b && y == a))
    }

    /// Runs `ready`/`advance` on `id` until it has nothing to do.
    fn drain(&mut self, id: NodeId) {
        loop {
            let ready = {
                let Some(raft) = self.nodes.get_mut(&id).unwrap().raft.as_mut() else { return };
                let r = raft.ready();
                if r.is_empty() {
                    return;
                }
                r
            };
            let node = self.nodes.get_mut(&id).unwrap();
            // 1. Durable work, in the order the module docs give.
            if let Some((meta, data)) = &ready.snapshot {
                node.disk.snap_index = meta.index;
                node.disk.snap_term = meta.term;
                node.disk.snap_data = decode(data);
                if ready.snapshot_keeps_log {
                    node.disk.entries.retain(|e| e.index > meta.index);
                } else {
                    node.disk.entries.clear();
                }
                node.applied = decode(data);
            }
            if let Some(from) = ready.truncate_from {
                node.disk.entries.retain(|e| e.index < from);
            }
            for e in &ready.entries {
                node.disk.entries.retain(|x| x.index < e.index);
                node.disk.entries.push(e.clone());
            }
            if let Some(h) = ready.hard_state {
                node.disk.hard = h;
            }
            // 2. Apply what is committed.
            for e in &ready.committed {
                if let EntryData::Command(c) = &e.data {
                    let v = u64::from_le_bytes(c[..8].try_into().unwrap());
                    node.applied.push(v);
                }
            }
            // 3. Messages out.
            for (to, m) in ready.messages.clone() {
                let delay = 1 + self.rng.below(3);
                self.net.push((self.step + delay, to, m));
            }
            // 4. Snapshots for peers that need one: the state machine as
            //    applied now, at the applied index.
            let raft = self.nodes.get_mut(&id).unwrap().raft.as_mut().unwrap();
            raft.advance(&ready).unwrap_or_else(|e| panic!("node {id}: advance: {e}"));
            if !ready.snapshot_wanted.is_empty() {
                let index = raft.log().applied();
                let term = raft.log().term(index).unwrap_or(raft.log().snapshot_term());
                let meta = SnapshotMeta { index, term, config: self.config.clone() };
                let data = encode(&self.nodes[&id].applied);
                let raft = self.nodes.get_mut(&id).unwrap().raft.as_mut().unwrap();
                for peer in ready.snapshot_wanted.clone() {
                    raft.send_snapshot(peer, meta.clone(), data.clone());
                }
            }
        }
    }

    /// Compacts `id`'s log through what it applied, as a snapshot on disk.
    fn compact(&mut self, id: NodeId) {
        let node = self.nodes.get_mut(&id).unwrap();
        let Some(raft) = node.raft.as_mut() else { return };
        let index = raft.log().applied();
        if index <= raft.log().snapshot_index() + 2 {
            return;
        }
        let Some(term) = raft.log().term(index) else { return };
        if raft.compact(index).is_ok() {
            node.disk.snap_index = index;
            node.disk.snap_term = term;
            node.disk.snap_data = node.applied.clone();
            node.disk.entries.retain(|e| e.index > index);
        }
    }

    fn check(&mut self) {
        for (id, node) in &self.nodes {
            if let Some(raft) = &node.raft
                && raft.role() == Role::Leader
            {
                let term = raft.term();
                match self.leaders.get(&term) {
                    Some(other) if other != id => panic!("two leaders in term {term}: {other} and {id} (step {})", self.step),
                    _ => {
                        self.leaders.insert(term, *id);
                    }
                }
            }
        }
        let applied: Vec<(&NodeId, &Vec<u64>)> = self.nodes.iter().filter(|(_, n)| n.raft.is_some()).map(|(i, n)| (i, &n.applied)).collect();
        for (a, x) in &applied {
            for (b, y) in &applied {
                let n = x.len().min(y.len());
                assert_eq!(x[..n], y[..n], "nodes {a} and {b} applied different commands (step {})", self.step);
            }
        }
        // What any node applied is committed: it must stay applied.
        let longest = applied.iter().map(|(_, a)| a.len()).max().unwrap_or(0);
        if longest > self.committed.len() {
            let longest_seq = applied.iter().find(|(_, a)| a.len() == longest).unwrap().1;
            assert_eq!(&longest_seq[..self.committed.len()], &self.committed[..], "a committed command was lost (step {})", self.step);
            self.committed = longest_seq.clone();
        }
    }

    fn leader(&self) -> Option<NodeId> {
        self.nodes.iter().find(|(_, n)| n.raft.as_ref().is_some_and(|r| r.role() == Role::Leader)).map(|(i, _)| *i)
    }

    /// One step: ticks, deliveries, faults, a proposal now and then.
    fn tick(&mut self, faults: bool) {
        self.tick_with(faults, true);
    }

    fn tick_with(&mut self, faults: bool, propose: bool) {
        self.step += 1;
        let ids: Vec<NodeId> = self.nodes.keys().copied().collect();
        for &id in &ids {
            if let Some(r) = self.nodes.get_mut(&id).unwrap().raft.as_mut() {
                r.tick();
            }
        }
        // Deliver what is due, in a shuffled order.
        let mut due: Vec<(u64, NodeId, Message)> = Vec::new();
        let mut later = Vec::new();
        for m in self.net.drain(..) {
            if m.0 <= self.step { due.push(m) } else { later.push(m) }
        }
        self.net = later;
        for i in (1..due.len()).rev() {
            let j = self.rng.below(i as u64 + 1) as usize;
            due.swap(i, j);
        }
        for (_, to, m) in due {
            let from = m.from();
            if !self.connected(from, to) || (faults && self.rng.chance(self.drop_percent)) {
                continue;
            }
            if let Some(r) = self.nodes.get_mut(&to).unwrap().raft.as_mut() {
                let _ = r.step(m);
            }
        }
        if propose
            && self.rng.chance(30)
            && let Some(l) = self.leader()
        {
            let c = self.next_command;
            if self.nodes.get_mut(&l).unwrap().raft.as_mut().unwrap().propose(c.to_le_bytes().to_vec()).is_ok() {
                self.next_command += 1;
            }
        }
        if faults {
            let pick = ids[self.rng.below(ids.len() as u64) as usize];
            match self.rng.below(1000) {
                0..=3 => self.crash(pick),
                4..=40 if self.nodes[&pick].raft.is_none() => self.start(pick),
                41..=45 => {
                    let other = ids[self.rng.below(ids.len() as u64) as usize];
                    if other != pick {
                        self.cut.push((pick, other));
                    }
                }
                46..=55 => {
                    self.cut.clear();
                }
                56..=60 => self.compact(pick),
                _ => {}
            }
        }
        for &id in &ids {
            self.drain(id);
        }
        self.check();
    }

    fn heal(&mut self) {
        self.cut.clear();
        let ids: Vec<NodeId> = self.nodes.keys().copied().collect();
        for id in ids {
            if self.nodes[&id].raft.is_none() {
                self.start(id);
            }
        }
    }
}

fn run(seed: u64, nodes: u64, steps: u64) {
    let mut sim = Sim::new(seed, nodes);
    for _ in 0..steps {
        sim.tick(true);
    }
    sim.heal();
    // Liveness: with the faults gone, a leader appears and everything
    // committed reaches every node.
    for _ in 0..1_000 {
        sim.tick(false);
    }
    // Then quiet, so the last command can reach everyone before the check.
    for _ in 0..1_000 {
        sim.tick_with(false, false);
    }
    assert!(sim.leader().is_some(), "seed {seed}: no leader after healing");
    let target = sim.committed.len();
    for (id, node) in &sim.nodes {
        if node.applied.len() < target {
            let states: Vec<String> = sim
                .nodes
                .iter()
                .map(|(i, n)| {
                    let r = n.raft.as_ref().unwrap();
                    format!(
                        "{i}: {:?} term {} leader {:?} snap {} last {} commit {} applied {} persisted {} (sm {})",
                        r.role(),
                        r.term(),
                        r.leader(),
                        r.log().snapshot_index(),
                        r.log().last_index(),
                        r.log().committed(),
                        r.log().applied(),
                        r.log().persisted(),
                        n.applied.len()
                    )
                })
                .collect();
            panic!("seed {seed}: node {id} applied {} of {target}\n{}", node.applied.len(), states.join("\n"));
        }
    }
    // And the cluster still takes new commands.
    let before = sim.committed.len();
    for _ in 0..500 {
        sim.tick(false);
    }
    assert!(sim.committed.len() > before, "seed {seed}: no progress after healing");
}

#[test]
fn raft_sim_three_nodes_many_seeds() {
    for seed in 0..400 {
        run(seed, 3, 3_000);
    }
}

#[test]
fn raft_sim_five_nodes_many_seeds() {
    for seed in 1_000..1_150 {
        run(seed, 5, 3_000);
    }
}

#[test]
fn raft_sim_single_node_commits_alone() {
    let mut sim = Sim::new(7, 1);
    for _ in 0..200 {
        sim.tick(false);
    }
    assert!(sim.committed.len() > 10);
}

#[test]
fn raft_codec_round_trips_every_message() {
    let entries = vec![
        Entry { term: 3, index: 7, data: EntryData::Noop },
        Entry { term: 3, index: 8, data: EntryData::Command(vec![1, 2, 3]) },
        Entry { term: 4, index: 9, data: EntryData::Config(Config::new([1, 2, 3])) },
    ];
    let messages = vec![
        Message::RequestVote { term: 5, from: 2, last_index: 9, last_term: 4, pre: true, transfer: false },
        Message::VoteResponse { term: 5, from: 3, granted: true, pre: false },
        Message::Append { term: 5, from: 1, prev_index: 6, prev_term: 3, entries, commit: 8, read: 2 },
        Message::AppendResponse { term: 5, from: 2, success: false, index: 4, read: 2 },
        Message::Snapshot { term: 5, from: 1, meta: SnapshotMeta { index: 9, term: 4, config: Config::new([1, 2]) }, data: vec![9; 40] },
        Message::SnapshotResponse { term: 5, from: 3, index: 9 },
        Message::TimeoutNow { term: 6, from: 1 },
    ];
    for m in messages {
        let bytes = rasterizeddb_raft::codec::encode_message(&m);
        let back = rasterizeddb_raft::codec::decode_message(&bytes).unwrap();
        assert_eq!(back, m);
        // A cut frame is an error, never a different message.
        for cut in 0..bytes.len() {
            assert!(rasterizeddb_raft::codec::decode_message(&bytes[..cut]).is_err(), "cut at {cut} of {m:?}");
        }
    }
}
