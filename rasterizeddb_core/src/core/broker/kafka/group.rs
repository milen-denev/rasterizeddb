//! Kafka's classic consumer-group protocol: JoinGroup, SyncGroup, Heartbeat
//! and LeaveGroup.
//!
//! The clients choose the assignment (the leader's assignor runs in the
//! client), so the coordinator only runs the rounds:
//!
//! 1. **Joining.** A join starts a round, or joins the one in progress. The
//!    round ends once every known member has rejoined, or at its deadline:
//!    `initial_delay` for a group that was empty, and the longest rebalance
//!    timeout otherwise. Members that did not rejoin by then, or whose session
//!    ran out, are dropped. The generation is bumped, a protocol every member
//!    offered is chosen, and every joiner is answered; the leader also gets
//!    the members' metadata.
//! 2. **Syncing.** The leader's SyncGroup carries every member's assignment,
//!    and the followers' SyncGroups wait for it.
//! 3. **Stable** until a member joins, leaves or expires. Heartbeats then
//!    answer `REBALANCE_IN_PROGRESS`, which sends every member back to step 1.
//!
//! Groups live in memory. A restart costs one rebalance; the committed offsets
//! are stored as the engine's ordered subscriptions and survive it.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::Mutex;
use tokio::sync::Notify;

pub const NONE: i16 = 0;
pub const ILLEGAL_GENERATION: i16 = 22;
pub const INCONSISTENT_GROUP_PROTOCOL: i16 = 23;
pub const UNKNOWN_MEMBER_ID: i16 = 25;
pub const INVALID_SESSION_TIMEOUT: i16 = 26;
pub const REBALANCE_IN_PROGRESS: i16 = 27;
pub const MEMBER_ID_REQUIRED: i16 = 79;

/// How long a round started on an empty group waits for more members
/// (Kafka's `group.initial.rebalance.delay.ms`, shorter by default here).
pub const INITIAL_DELAY: Duration = Duration::from_millis(300);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Phase {
    Empty,
    Joining,
    Syncing,
    Stable,
}

struct Member {
    client_id: String,
    session_timeout: Duration,
    rebalance_timeout: Duration,
    protocols: Vec<(String, Vec<u8>)>,
    last_seen: Instant,
    /// Rejoined in the round in progress.
    rejoined: bool,
}

struct State {
    phase: Phase,
    generation: i32,
    protocol_type: String,
    protocol: String,
    leader: String,
    members: BTreeMap<String, Member>,
    /// Ids handed out by `MEMBER_ID_REQUIRED` and not joined yet.
    pending: HashMap<String, Instant>,
    deadline: Instant,
    /// The round in progress started on an empty group: it waits out its
    /// whole delay for more members even once everyone known has joined.
    initial: bool,
    /// Rounds finished: a joiner waits for this to pass the value it saw.
    rounds: u64,
    assignments: HashMap<String, Vec<u8>>,
    next_id: u64,
}

pub struct Group {
    pub id: String,
    state: Mutex<State>,
    changed: Notify,
}

/// A JoinGroup's answer.
pub struct Joined {
    pub error: i16,
    pub generation: i32,
    pub protocol: String,
    pub leader: String,
    pub member_id: String,
    /// The members' ids and metadata, for the leader only.
    pub members: Vec<(String, Vec<u8>)>,
}

impl Joined {
    fn error(code: i16, member_id: &str) -> Joined {
        Joined { error: code, generation: -1, protocol: String::new(), leader: String::new(), member_id: member_id.into(), members: Vec::new() }
    }
}

impl Group {
    fn new(id: &str) -> Group {
        Group {
            id: id.to_string(),
            state: Mutex::new(State {
                phase: Phase::Empty,
                generation: 0,
                protocol_type: String::new(),
                protocol: String::new(),
                leader: String::new(),
                members: BTreeMap::new(),
                pending: HashMap::new(),
                deadline: Instant::now(),
                initial: false,
                rounds: 0,
                assignments: HashMap::new(),
                next_id: 1,
            }),
            changed: Notify::new(),
        }
    }

    /// The member id `member_id` names, if it is (or was just told to be) a
    /// member, with its generation.
    pub fn check_member(&self, member_id: &str, generation: i32) -> i16 {
        let st = self.state.lock();
        if !st.members.contains_key(member_id) {
            return UNKNOWN_MEMBER_ID;
        }
        if generation != st.generation {
            return ILLEGAL_GENERATION;
        }
        NONE
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn join(
        &self,
        member_id: &str,
        client_id: &str,
        session_timeout: Duration,
        rebalance_timeout: Duration,
        protocol_type: &str,
        protocols: Vec<(String, Vec<u8>)>,
        require_known_id: bool,
    ) -> Joined {
        if session_timeout < Duration::from_millis(1000) || session_timeout > Duration::from_secs(1800) {
            return Joined::error(INVALID_SESSION_TIMEOUT, member_id);
        }
        let now = Instant::now();
        let (my_id, seen_round) = {
            let mut st = self.state.lock();
            if !st.members.is_empty() && !st.protocol_type.is_empty() && st.protocol_type != protocol_type {
                return Joined::error(INCONSISTENT_GROUP_PROTOCOL, member_id);
            }
            let id = if member_id.is_empty() {
                let id = format!("{}-{}-{}", if client_id.is_empty() { "member" } else { client_id }, st.next_id, now_tag());
                st.next_id += 1;
                if require_known_id {
                    // KIP-394: the client rejoins with this id.
                    st.pending.insert(id.clone(), now);
                    return Joined::error(MEMBER_ID_REQUIRED, &id);
                }
                id
            } else {
                if !st.members.contains_key(member_id) && st.pending.remove(member_id).is_none() {
                    return Joined::error(UNKNOWN_MEMBER_ID, member_id);
                }
                member_id.to_string()
            };
            st.protocol_type = protocol_type.to_string();
            let fresh = !st.members.contains_key(&id);
            let m = st.members.entry(id.clone()).or_insert_with(|| Member {
                client_id: client_id.to_string(),
                session_timeout,
                rebalance_timeout,
                protocols: Vec::new(),
                last_seen: now,
                rejoined: false,
            });
            m.protocols = protocols;
            m.session_timeout = session_timeout;
            m.rebalance_timeout = rebalance_timeout;
            m.last_seen = now;
            m.rejoined = true;
            match st.phase {
                Phase::Joining => {}
                Phase::Empty => {
                    st.phase = Phase::Joining;
                    st.deadline = now + INITIAL_DELAY;
                    st.initial = true;
                }
                Phase::Syncing | Phase::Stable => {
                    let _ = fresh;
                    st.phase = Phase::Joining;
                    st.initial = false;
                    // Everyone else has a rebalance timeout to rejoin in.
                    let wait = st.members.values().map(|m| m.rebalance_timeout).max().unwrap_or(Duration::ZERO);
                    st.deadline = now + wait;
                    for (mid, m) in st.members.iter_mut() {
                        if *mid != id {
                            m.rejoined = false;
                        }
                    }
                }
            }
            (id, st.rounds)
        };
        self.changed.notify_waiters();
        loop {
            let notified = self.changed.notified();
            let wait = {
                let mut st = self.state.lock();
                if st.rounds > seen_round {
                    return self.answer(&st, &my_id);
                }
                let now = Instant::now();
                let expired = |m: &Member| now.duration_since(m.last_seen) > m.session_timeout;
                let all_in = st.members.values().all(|m| m.rejoined || expired(m));
                if (all_in && !st.initial) || now >= st.deadline {
                    self.finish_round(&mut st, now);
                    drop(st);
                    self.changed.notify_waiters();
                    continue;
                }
                st.deadline.saturating_duration_since(now)
            };
            let _ = tokio::time::timeout(wait.max(Duration::from_millis(5)), notified).await;
        }
    }

    /// Ends the joining round: drops who did not rejoin, picks the protocol
    /// and leader, bumps the generation.
    fn finish_round(&self, st: &mut State, now: Instant) {
        st.members.retain(|_, m| m.rejoined && now.duration_since(m.last_seen) <= m.session_timeout);
        st.rounds += 1;
        st.assignments.clear();
        if st.members.is_empty() {
            st.phase = Phase::Empty;
            st.leader.clear();
            st.protocol.clear();
            return;
        }
        st.generation = st.generation.wrapping_add(1).max(1);
        if !st.members.contains_key(&st.leader) {
            st.leader = st.members.keys().next().cloned().unwrap_or_default();
        }
        // The leader's first protocol that every member offers.
        let leader = &st.members[&st.leader];
        let chosen = leader
            .protocols
            .iter()
            .map(|(n, _)| n)
            .find(|n| st.members.values().all(|m| m.protocols.iter().any(|(p, _)| p == *n)))
            .cloned();
        st.protocol = chosen.unwrap_or_default();
        for m in st.members.values_mut() {
            m.rejoined = false;
            m.last_seen = now;
        }
        st.phase = Phase::Syncing;
    }

    fn answer(&self, st: &State, me: &str) -> Joined {
        if !st.members.contains_key(me) {
            return Joined::error(UNKNOWN_MEMBER_ID, me);
        }
        if st.protocol.is_empty() {
            return Joined::error(INCONSISTENT_GROUP_PROTOCOL, me);
        }
        let members = if st.leader == me {
            st.members
                .iter()
                .map(|(id, m)| {
                    let meta = m.protocols.iter().find(|(p, _)| *p == st.protocol).map(|(_, d)| d.clone()).unwrap_or_default();
                    (id.clone(), meta)
                })
                .collect()
        } else {
            Vec::new()
        };
        Joined {
            error: NONE,
            generation: st.generation,
            protocol: st.protocol.clone(),
            leader: st.leader.clone(),
            member_id: me.to_string(),
            members,
        }
    }

    /// SyncGroup: the leader hands out assignments; everyone gets theirs.
    pub async fn sync(&self, member_id: &str, generation: i32, assignments: Vec<(String, Vec<u8>)>) -> (i16, Vec<u8>) {
        {
            let mut st = self.state.lock();
            let Some(m) = st.members.get_mut(member_id) else { return (UNKNOWN_MEMBER_ID, Vec::new()) };
            m.last_seen = Instant::now();
            if generation != st.generation {
                return (ILLEGAL_GENERATION, Vec::new());
            }
            if st.phase == Phase::Joining {
                return (REBALANCE_IN_PROGRESS, Vec::new());
            }
            if st.leader == member_id && st.phase == Phase::Syncing {
                st.assignments = assignments.into_iter().collect();
                st.phase = Phase::Stable;
            }
        }
        self.changed.notify_waiters();
        let give_up = Instant::now() + Duration::from_secs(60);
        loop {
            let notified = self.changed.notified();
            {
                let st = self.state.lock();
                if st.generation != generation || st.phase == Phase::Joining {
                    return (REBALANCE_IN_PROGRESS, Vec::new());
                }
                if st.phase == Phase::Stable {
                    return (NONE, st.assignments.get(member_id).cloned().unwrap_or_default());
                }
            }
            if Instant::now() >= give_up {
                return (REBALANCE_IN_PROGRESS, Vec::new());
            }
            let _ = tokio::time::timeout(Duration::from_millis(500), notified).await;
        }
    }

    pub fn heartbeat(&self, member_id: &str, generation: i32) -> i16 {
        let mut st = self.state.lock();
        let phase = st.phase;
        let current = st.generation;
        let Some(m) = st.members.get_mut(member_id) else { return UNKNOWN_MEMBER_ID };
        m.last_seen = Instant::now();
        if phase == Phase::Joining {
            return REBALANCE_IN_PROGRESS;
        }
        if generation != current {
            return ILLEGAL_GENERATION;
        }
        NONE
    }

    pub fn leave(&self, member_id: &str) -> i16 {
        let mut st = self.state.lock();
        if st.members.remove(member_id).is_none() {
            return UNKNOWN_MEMBER_ID;
        }
        self.rebalance_after_loss(&mut st);
        drop(st);
        self.changed.notify_waiters();
        NONE
    }

    fn rebalance_after_loss(&self, st: &mut State) {
        if st.members.is_empty() {
            st.phase = Phase::Empty;
            st.leader.clear();
            st.assignments.clear();
            return;
        }
        if st.phase != Phase::Joining {
            st.phase = Phase::Joining;
            st.initial = false;
            st.deadline = Instant::now()
                + st.members.values().map(|m| m.rebalance_timeout).max().unwrap_or(Duration::ZERO);
            for m in st.members.values_mut() {
                m.rejoined = false;
            }
        }
    }

    /// Drops members whose session ran out.
    fn expire(&self) {
        let mut st = self.state.lock();
        let now = Instant::now();
        let before = st.members.len();
        st.members.retain(|_, m| now.duration_since(m.last_seen) <= m.session_timeout);
        st.pending.retain(|_, t| now.duration_since(*t) < Duration::from_secs(300));
        if st.members.len() != before {
            log::info!("kafka: group {}: {} member(s) expired", self.id, before - st.members.len());
            self.rebalance_after_loss(&mut st);
            drop(st);
            self.changed.notify_waiters();
        }
    }

    /// `(member, client id)` of every member, for logs and listings.
    pub fn members(&self) -> Vec<(String, String)> {
        self.state.lock().members.iter().map(|(k, m)| (k.clone(), m.client_id.clone())).collect()
    }
}

fn now_tag() -> u64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map_or(0, |d| d.as_micros() as u64)
}

/// Every group of one broker.
#[derive(Default)]
pub struct Groups {
    groups: Mutex<HashMap<String, Arc<Group>>>,
}

impl Groups {
    pub fn get(&self, id: &str) -> Arc<Group> {
        self.groups.lock().entry(id.to_string()).or_insert_with(|| Arc::new(Group::new(id))).clone()
    }

    pub fn existing(&self, id: &str) -> Option<Arc<Group>> {
        self.groups.lock().get(id).cloned()
    }

    /// Once a second: session expiry.
    pub fn tick(&self) {
        let all: Vec<Arc<Group>> = self.groups.lock().values().cloned().collect();
        for g in all {
            g.expire();
        }
    }
}
