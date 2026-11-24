//! The byte format of messages (the transport) and of entries and hard
//! state (the log store). Little-endian; `bytes` is `len:u32 ‖ data`; a
//! decoder refuses unknown tags, short input and trailing bytes.

use crate::types::*;

/// The format's version, first byte of every encoded message.
pub const VERSION: u8 = 1;

/// The longest field a decoder accepts (1 GiB).
const MAX_FIELD: usize = 1 << 30;

#[derive(Default)]
pub struct Writer(pub Vec<u8>);

impl Writer {
    fn u8(&mut self, v: u8) {
        self.0.push(v);
    }
    fn u32(&mut self, v: u32) {
        self.0.extend_from_slice(&v.to_le_bytes());
    }
    fn u64(&mut self, v: u64) {
        self.0.extend_from_slice(&v.to_le_bytes());
    }
    fn bool(&mut self, v: bool) {
        self.0.push(v as u8);
    }
    fn bytes(&mut self, b: &[u8]) {
        self.u32(b.len() as u32);
        self.0.extend_from_slice(b);
    }
    fn ids(&mut self, ids: &std::collections::BTreeSet<NodeId>) {
        self.u32(ids.len() as u32);
        for id in ids {
            self.u64(*id);
        }
    }
    pub fn config(&mut self, c: &Config) {
        self.ids(&c.voters);
        self.ids(&c.learners);
    }
    pub fn entry(&mut self, e: &Entry) {
        self.u64(e.term);
        self.u64(e.index);
        match &e.data {
            EntryData::Noop => self.u8(0),
            EntryData::Command(d) => {
                self.u8(1);
                self.bytes(d);
            }
            EntryData::Config(c) => {
                self.u8(2);
                self.config(c);
            }
        }
    }
    pub fn hard_state(&mut self, h: &HardState) {
        self.u64(h.term);
        match h.vote {
            Some(v) => {
                self.u8(1);
                self.u64(v);
            }
            None => self.u8(0),
        }
        self.u64(h.commit);
    }
    pub fn snapshot_meta(&mut self, m: &SnapshotMeta) {
        self.u64(m.index);
        self.u64(m.term);
        self.config(&m.config);
    }
}

pub struct Reader<'a> {
    b: &'a [u8],
    at: usize,
}

impl<'a> Reader<'a> {
    pub fn new(b: &'a [u8]) -> Self {
        Reader { b, at: 0 }
    }
    fn take(&mut self, n: usize) -> Result<&'a [u8], String> {
        let end = self.at.checked_add(n).filter(|&e| e <= self.b.len()).ok_or("raft: truncated input")?;
        let s = &self.b[self.at..end];
        self.at = end;
        Ok(s)
    }
    fn u8(&mut self) -> Result<u8, String> {
        Ok(self.take(1)?[0])
    }
    fn u32(&mut self) -> Result<u32, String> {
        Ok(u32::from_le_bytes(self.take(4)?.try_into().unwrap()))
    }
    fn u64(&mut self) -> Result<u64, String> {
        Ok(u64::from_le_bytes(self.take(8)?.try_into().unwrap()))
    }
    fn bool(&mut self) -> Result<bool, String> {
        match self.u8()? {
            0 => Ok(false),
            1 => Ok(true),
            b => Err(format!("raft: flag byte {b}")),
        }
    }
    fn bytes(&mut self) -> Result<Vec<u8>, String> {
        let n = self.u32()? as usize;
        if n > MAX_FIELD {
            return Err("raft: an over-long field".into());
        }
        Ok(self.take(n)?.to_vec())
    }
    fn ids(&mut self) -> Result<std::collections::BTreeSet<NodeId>, String> {
        let n = self.u32()? as usize;
        if n > 1 << 16 {
            return Err("raft: too many members".into());
        }
        (0..n).map(|_| self.u64()).collect()
    }
    pub fn config(&mut self) -> Result<Config, String> {
        Ok(Config { voters: self.ids()?, learners: self.ids()? })
    }
    pub fn entry(&mut self) -> Result<Entry, String> {
        let term = self.u64()?;
        let index = self.u64()?;
        let data = match self.u8()? {
            0 => EntryData::Noop,
            1 => EntryData::Command(self.bytes()?),
            2 => EntryData::Config(self.config()?),
            t => return Err(format!("raft: entry tag {t}")),
        };
        Ok(Entry { term, index, data })
    }
    pub fn hard_state(&mut self) -> Result<HardState, String> {
        let term = self.u64()?;
        let vote = if self.bool()? { Some(self.u64()?) } else { None };
        Ok(HardState { term, vote, commit: self.u64()? })
    }
    pub fn snapshot_meta(&mut self) -> Result<SnapshotMeta, String> {
        Ok(SnapshotMeta { index: self.u64()?, term: self.u64()?, config: self.config()? })
    }
    pub fn done(&self) -> Result<(), String> {
        if self.at == self.b.len() { Ok(()) } else { Err("raft: trailing bytes".into()) }
    }
}

pub fn encode_entry(e: &Entry) -> Vec<u8> {
    let mut w = Writer::default();
    w.entry(e);
    w.0
}

pub fn decode_entry(b: &[u8]) -> Result<Entry, String> {
    let mut r = Reader::new(b);
    let e = r.entry()?;
    r.done()?;
    Ok(e)
}

pub fn encode_hard_state(h: &HardState) -> Vec<u8> {
    let mut w = Writer::default();
    w.hard_state(h);
    w.0
}

pub fn decode_hard_state(b: &[u8]) -> Result<HardState, String> {
    let mut r = Reader::new(b);
    let h = r.hard_state()?;
    r.done()?;
    Ok(h)
}

/// `version ‖ tag ‖ body`.
pub fn encode_message(m: &Message) -> Vec<u8> {
    let mut w = Writer(vec![VERSION]);
    match m {
        Message::RequestVote { term, from, last_index, last_term, pre, transfer } => {
            w.u8(1);
            w.u64(*term);
            w.u64(*from);
            w.u64(*last_index);
            w.u64(*last_term);
            w.bool(*pre);
            w.bool(*transfer);
        }
        Message::VoteResponse { term, from, granted, pre } => {
            w.u8(2);
            w.u64(*term);
            w.u64(*from);
            w.bool(*granted);
            w.bool(*pre);
        }
        Message::Append { term, from, prev_index, prev_term, entries, commit, read } => {
            w.u8(3);
            w.u64(*term);
            w.u64(*from);
            w.u64(*prev_index);
            w.u64(*prev_term);
            w.u32(entries.len() as u32);
            for e in entries {
                w.entry(e);
            }
            w.u64(*commit);
            w.u64(*read);
        }
        Message::AppendResponse { term, from, success, index, read } => {
            w.u8(4);
            w.u64(*term);
            w.u64(*from);
            w.bool(*success);
            w.u64(*index);
            w.u64(*read);
        }
        Message::Snapshot { term, from, meta, data } => {
            w.u8(5);
            w.u64(*term);
            w.u64(*from);
            w.snapshot_meta(meta);
            w.bytes(data);
        }
        Message::SnapshotResponse { term, from, index } => {
            w.u8(6);
            w.u64(*term);
            w.u64(*from);
            w.u64(*index);
        }
        Message::TimeoutNow { term, from } => {
            w.u8(7);
            w.u64(*term);
            w.u64(*from);
        }
    }
    w.0
}

pub fn decode_message(b: &[u8]) -> Result<Message, String> {
    let mut r = Reader::new(b);
    let v = r.u8()?;
    if v != VERSION {
        return Err(format!("raft: message version {v}"));
    }
    let m = match r.u8()? {
        1 => Message::RequestVote {
            term: r.u64()?,
            from: r.u64()?,
            last_index: r.u64()?,
            last_term: r.u64()?,
            pre: r.bool()?,
            transfer: r.bool()?,
        },
        2 => Message::VoteResponse { term: r.u64()?, from: r.u64()?, granted: r.bool()?, pre: r.bool()? },
        3 => {
            let (term, from, prev_index, prev_term) = (r.u64()?, r.u64()?, r.u64()?, r.u64()?);
            let n = r.u32()? as usize;
            if n > 1 << 20 {
                return Err("raft: too many entries".into());
            }
            let mut entries = Vec::with_capacity(n.min(1024));
            for _ in 0..n {
                entries.push(r.entry()?);
            }
            Message::Append { term, from, prev_index, prev_term, entries, commit: r.u64()?, read: r.u64()? }
        }
        4 => Message::AppendResponse { term: r.u64()?, from: r.u64()?, success: r.bool()?, index: r.u64()?, read: r.u64()? },
        5 => Message::Snapshot { term: r.u64()?, from: r.u64()?, meta: r.snapshot_meta()?, data: r.bytes()? },
        6 => Message::SnapshotResponse { term: r.u64()?, from: r.u64()?, index: r.u64()? },
        7 => Message::TimeoutNow { term: r.u64()?, from: r.u64()? },
        t => return Err(format!("raft: message tag {t}")),
    };
    r.done()?;
    Ok(m)
}
