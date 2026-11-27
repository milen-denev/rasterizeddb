//! The gateway's LISTEN/NOTIFY hub (sharding-plan.md B4.3).
//!
//! A NOTIFY runs on whichever shard its transaction runs on, and is sent
//! there at commit. A client's LISTEN is answered here: the session joins
//! this process's hub, and a poller on the first core reads every shard's
//! outbox for each database someone listens in, then hands each note to
//! that database's listening sessions. A session receives them while idle,
//! as a NotificationResponse, as it would from one server.

use std::collections::{HashMap, HashSet};
use std::rc::Rc;
use std::sync::Mutex;

use rasterizeddb_router::rshard::{Request, Response};
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel};

use crate::core::Core;

/// One notification: the sending backend's pid, channel and payload.
#[derive(Clone, Debug)]
pub struct Note {
    pub pid: i32,
    pub channel: String,
    pub payload: String,
}

type Listeners = HashMap<(String, String), Vec<(u64, UnboundedSender<Note>)>>;

static HUB: Mutex<Option<Listeners>> = Mutex::new(None);

fn hub<R>(f: impl FnOnce(&mut Listeners) -> R) -> R {
    let mut g = HUB.lock().unwrap_or_else(|e| e.into_inner());
    f(g.get_or_insert_with(HashMap::new))
}

/// The databases someone listens in.
pub fn databases() -> HashSet<String> {
    hub(|l| l.iter().filter(|(_, v)| !v.is_empty()).map(|((d, _), _)| d.clone()).collect())
}

/// Hands `note`, sent in `database`, to every session listening on its
/// channel there.
pub fn deliver(database: &str, note: &Note) {
    hub(|l| {
        if let Some(sessions) = l.get_mut(&(database.to_string(), note.channel.clone())) {
            sessions.retain(|(_, tx)| tx.send(note.clone()).is_ok());
        }
    });
}

/// A session's channels and its queue.
pub struct Listener {
    id: u64,
    database: String,
    tx: UnboundedSender<Note>,
    pub rx: UnboundedReceiver<Note>,
    channels: HashSet<String>,
}

impl Listener {
    pub fn new(database: &str) -> Listener {
        let (tx, rx) = unbounded_channel();
        static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
        Listener {
            id: NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed),
            database: database.to_string(),
            tx,
            rx,
            channels: HashSet::new(),
        }
    }

    pub fn listen(&mut self, channel: &str) {
        if self.channels.insert(channel.to_string()) {
            let entry = (self.id, self.tx.clone());
            hub(|l| l.entry((self.database.clone(), channel.to_string())).or_default().push(entry));
        }
    }

    pub fn unlisten(&mut self, channel: &str) {
        if self.channels.remove(channel) {
            let id = self.id;
            hub(|l| {
                if let Some(v) = l.get_mut(&(self.database.clone(), channel.to_string())) {
                    v.retain(|(s, _)| *s != id);
                }
            });
        }
    }

    pub fn unlisten_all(&mut self) {
        for c in self.channels.clone() {
            self.unlisten(&c);
        }
    }
}

impl Drop for Listener {
    fn drop(&mut self) {
        self.unlisten_all();
    }
}

/// Reads every shard's new notes for every database listened in, and
/// delivers them. `cursors` keeps, per (database, shard), where to ask from
/// next; a shard first asked only says where its sequence stands, so
/// nothing sent before anyone listened is delivered.
pub async fn poll(core: &Rc<Core>, cursors: &mut HashMap<(String, u32), u64>) {
    let dbs = databases();
    cursors.retain(|(d, _), _| dbs.contains(d));
    if dbs.is_empty() {
        return;
    }
    let shards: Vec<u32> = core.shared.map.get().shards().iter().map(|s| s.id).collect();
    for db in &dbs {
        for &s in &shards {
            let since = cursors.get(&(db.clone(), s)).copied().unwrap_or(u64::MAX);
            let Ok(link) = core.link(s) else { continue };
            let req = Request::Notifications { database: db.clone(), since }.encode();
            if let Ok(Response::Notifications { next, lost, notes }) = link.call(core, &req).await {
                if lost {
                    log::warn!("LISTEN hub: shard {s} dropped notifications of {db} before they were read");
                }
                for (pid, channel, payload) in notes {
                    deliver(db, &Note { pid, channel, payload });
                }
                cursors.insert((db.clone(), s), next);
            }
        }
    }
}

/// A NotificationResponse for `note`.
pub fn message(out: &mut Vec<u8>, note: &Note) {
    let mut body = note.pid.to_be_bytes().to_vec();
    body.extend_from_slice(note.channel.as_bytes());
    body.push(0);
    body.extend_from_slice(note.payload.as_bytes());
    body.push(0);
    crate::pg::put(out, b'A', &body);
}

#[cfg(test)]
mod tests {
    use super::*;

    fn note(channel: &str, payload: &str) -> Note {
        Note { pid: 7, channel: channel.into(), payload: payload.into() }
    }

    #[test]
    fn hub_delivers_by_database_and_channel_and_forgets_a_gone_session() {
        let mut a = Listener::new("hub_db1");
        let mut b = Listener::new("hub_db2");
        a.listen("ch");
        b.listen("ch");
        deliver("hub_db1", &note("ch", "one"));
        deliver("hub_db1", &note("other", "x"));
        assert_eq!(a.rx.try_recv().unwrap().payload, "one");
        assert!(a.rx.try_recv().is_err(), "another channel is not heard");
        assert!(b.rx.try_recv().is_err(), "another database is not heard");
        a.unlisten("ch");
        deliver("hub_db1", &note("ch", "two"));
        assert!(a.rx.try_recv().is_err());
        assert!(databases().contains("hub_db2"));
        drop(b);
        assert!(!databases().contains("hub_db2"), "a dropped session listens nowhere");
    }
}
