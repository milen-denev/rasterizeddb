//! AMQP 0-9-1 framing: frames, method arguments, field tables and content
//! headers, as RabbitMQ's clients write them.

pub const PROTOCOL_HEADER: &[u8; 8] = b"AMQP\x00\x00\x09\x01";
pub const FRAME_METHOD: u8 = 1;
pub const FRAME_HEADER: u8 = 2;
pub const FRAME_BODY: u8 = 3;
pub const FRAME_HEARTBEAT: u8 = 8;
pub const FRAME_END: u8 = 0xCE;

#[derive(Debug)]
pub struct Malformed(pub &'static str);
pub type R<T> = Result<T, Malformed>;

/// One frame at the start of `buf`: `(type, channel, payload, bytes used)`.
pub fn parse_frame(buf: &[u8], max: usize) -> R<Option<(u8, u16, &[u8], usize)>> {
    if buf.len() < 7 {
        return Ok(None);
    }
    let kind = buf[0];
    let channel = u16::from_be_bytes([buf[1], buf[2]]);
    let size = u32::from_be_bytes(buf[3..7].try_into().unwrap()) as usize;
    if size > max {
        return Err(Malformed("frame larger than frame_max"));
    }
    if buf.len() < 8 + size {
        return Ok(None);
    }
    if buf[7 + size] != FRAME_END {
        return Err(Malformed("frame does not end in 0xCE"));
    }
    Ok(Some((kind, channel, &buf[7..7 + size], 8 + size)))
}

/// The length of the frame at the start of `buf`, once its header is in.
pub fn frame_need(buf: &[u8]) -> Option<usize> {
    (buf.len() >= 7).then(|| 8 + u32::from_be_bytes(buf[3..7].try_into().unwrap()) as usize)
}

/// Reads method arguments. Consecutive bits share an octet, LSB first.
pub struct In<'a> {
    b: &'a [u8],
    bits: u8,
    bit: u8,
}

impl<'a> In<'a> {
    pub fn new(b: &'a [u8]) -> Self {
        In { b, bits: 0, bit: 8 }
    }
    fn take(&mut self, n: usize) -> R<&'a [u8]> {
        self.bit = 8;
        if self.b.len() < n {
            return Err(Malformed("arguments too short"));
        }
        let (h, t) = self.b.split_at(n);
        self.b = t;
        Ok(h)
    }
    pub fn u8(&mut self) -> R<u8> {
        Ok(self.take(1)?[0])
    }
    pub fn u16(&mut self) -> R<u16> {
        Ok(u16::from_be_bytes(self.take(2)?.try_into().unwrap()))
    }
    pub fn u32(&mut self) -> R<u32> {
        Ok(u32::from_be_bytes(self.take(4)?.try_into().unwrap()))
    }
    pub fn u64(&mut self) -> R<u64> {
        Ok(u64::from_be_bytes(self.take(8)?.try_into().unwrap()))
    }
    pub fn shortstr(&mut self) -> R<&'a str> {
        let n = self.u8()? as usize;
        std::str::from_utf8(self.take(n)?).map_err(|_| Malformed("short string is not UTF-8"))
    }
    pub fn longstr(&mut self) -> R<&'a [u8]> {
        let n = self.u32()? as usize;
        self.take(n)
    }
    pub fn bit(&mut self) -> R<bool> {
        if self.bit >= 8 {
            let b = *self.b.first().ok_or(Malformed("arguments too short"))?;
            self.b = &self.b[1..];
            self.bits = b;
            self.bit = 0;
        }
        let v = self.bits & (1 << self.bit) != 0;
        self.bit += 1;
        Ok(v)
    }
    pub fn table(&mut self) -> R<Table<'a>> {
        let raw = self.longstr()?;
        Table::parse(raw)
    }
    pub fn rest(&self) -> &'a [u8] {
        self.b
    }
}

/// Writes method arguments.
pub struct Out<'a> {
    pub v: &'a mut Vec<u8>,
    bits_at: Option<usize>,
    bit: u8,
}

impl<'a> Out<'a> {
    pub fn new(v: &'a mut Vec<u8>) -> Self {
        Out { v, bits_at: None, bit: 0 }
    }
    fn flush_bits(&mut self) {
        self.bits_at = None;
        self.bit = 0;
    }
    pub fn u8(&mut self, x: u8) {
        self.flush_bits();
        self.v.push(x);
    }
    pub fn u16(&mut self, x: u16) {
        self.flush_bits();
        self.v.extend_from_slice(&x.to_be_bytes());
    }
    pub fn u32(&mut self, x: u32) {
        self.flush_bits();
        self.v.extend_from_slice(&x.to_be_bytes());
    }
    pub fn u64(&mut self, x: u64) {
        self.flush_bits();
        self.v.extend_from_slice(&x.to_be_bytes());
    }
    pub fn shortstr(&mut self, s: &str) {
        self.flush_bits();
        let n = s.len().min(255);
        self.v.push(n as u8);
        self.v.extend_from_slice(&s.as_bytes()[..n]);
    }
    pub fn longstr(&mut self, s: &[u8]) {
        self.flush_bits();
        self.v.extend_from_slice(&(s.len() as u32).to_be_bytes());
        self.v.extend_from_slice(s);
    }
    pub fn bit(&mut self, b: bool) {
        let at = match self.bits_at {
            Some(at) if self.bit < 8 => at,
            _ => {
                self.v.push(0);
                self.bit = 0;
                let at = self.v.len() - 1;
                self.bits_at = Some(at);
                at
            }
        };
        if b {
            self.v[at] |= 1 << self.bit;
        }
        self.bit += 1;
    }
    /// A table of string values (server properties, capabilities).
    pub fn table(&mut self, entries: &[(&str, Value<'_>)]) {
        self.flush_bits();
        let mut t = Vec::new();
        for (k, v) in entries {
            t.push(k.len() as u8);
            t.extend_from_slice(k.as_bytes());
            v.encode(&mut t);
        }
        self.longstr(&t);
    }
}

/// A field-table value, as far as the server reads them.
#[derive(Clone, Debug, PartialEq)]
pub enum Value<'a> {
    Bool(bool),
    Int(i64),
    Float(f64),
    Str(&'a [u8]),
    Table(Table<'a>),
    Other,
}

impl Value<'_> {
    pub fn int(&self) -> Option<i64> {
        match self {
            Value::Int(n) => Some(*n),
            Value::Float(f) => Some(*f as i64),
            Value::Str(s) => std::str::from_utf8(s).ok()?.trim().parse().ok(),
            _ => None,
        }
    }
    pub fn str(&self) -> Option<&str> {
        match self {
            Value::Str(s) => std::str::from_utf8(s).ok(),
            _ => None,
        }
    }
    pub fn bool(&self) -> Option<bool> {
        match self {
            Value::Bool(b) => Some(*b),
            Value::Int(n) => Some(*n != 0),
            _ => None,
        }
    }
    /// The value as the bytes a headers binding compares: strings as they
    /// are, numbers and booleans as their text.
    pub fn match_bytes(&self) -> Option<Vec<u8>> {
        match self {
            Value::Str(s) => Some(s.to_vec()),
            Value::Int(n) => Some(n.to_string().into_bytes()),
            Value::Bool(b) => Some(b.to_string().into_bytes()),
            Value::Float(f) => Some(f.to_string().into_bytes()),
            _ => None,
        }
    }
    fn encode(&self, out: &mut Vec<u8>) {
        match self {
            Value::Bool(b) => {
                out.push(b't');
                out.push(*b as u8);
            }
            Value::Int(n) => {
                out.push(b'l');
                out.extend_from_slice(&n.to_be_bytes());
            }
            Value::Float(f) => {
                out.push(b'd');
                out.extend_from_slice(&f.to_be_bytes());
            }
            Value::Str(s) => {
                out.push(b'S');
                out.extend_from_slice(&(s.len() as u32).to_be_bytes());
                out.extend_from_slice(s);
            }
            Value::Table(t) => {
                out.push(b'F');
                out.extend_from_slice(&(t.raw.len() as u32).to_be_bytes());
                out.extend_from_slice(t.raw);
            }
            Value::Other => out.push(b'V'),
        }
    }
}

/// A field table, kept as its bytes and read on demand.
#[derive(Clone, Debug, PartialEq, Default)]
pub struct Table<'a> {
    pub raw: &'a [u8],
}

impl<'a> Table<'a> {
    pub fn parse(raw: &'a [u8]) -> R<Table<'a>> {
        let t = Table { raw };
        // Walk it once so a malformed table is refused up front.
        for e in t.entries() {
            e?;
        }
        Ok(t)
    }

    pub fn entries(&self) -> impl Iterator<Item = R<(&'a str, Value<'a>)>> + '_ {
        let mut b = self.raw;
        std::iter::from_fn(move || {
            if b.is_empty() {
                return None;
            }
            Some((|| {
                let n = *b.first().ok_or(Malformed("table"))? as usize;
                let key = std::str::from_utf8(b.get(1..1 + n).ok_or(Malformed("table key"))?)
                    .map_err(|_| Malformed("table key is not UTF-8"))?;
                let mut rest = &b[1 + n..];
                let v = value(&mut rest)?;
                b = rest;
                Ok((key, v))
            })())
        })
    }

    pub fn get(&self, key: &str) -> Option<Value<'a>> {
        self.entries().flatten().find(|(k, _)| *k == key).map(|(_, v)| v)
    }
}

fn take<'a>(b: &mut &'a [u8], n: usize) -> R<&'a [u8]> {
    if b.len() < n {
        return Err(Malformed("field value too short"));
    }
    let (h, t) = b.split_at(n);
    *b = t;
    Ok(h)
}

fn value<'a>(b: &mut &'a [u8]) -> R<Value<'a>> {
    let kind = take(b, 1)?[0];
    Ok(match kind {
        b't' => Value::Bool(take(b, 1)?[0] != 0),
        b'b' => Value::Int(take(b, 1)?[0] as i8 as i64),
        b'B' => Value::Int(take(b, 1)?[0] as i64),
        b's' | b'U' => Value::Int(i16::from_be_bytes(take(b, 2)?.try_into().unwrap()) as i64),
        b'u' => Value::Int(u16::from_be_bytes(take(b, 2)?.try_into().unwrap()) as i64),
        b'I' => Value::Int(i32::from_be_bytes(take(b, 4)?.try_into().unwrap()) as i64),
        b'i' => Value::Int(u32::from_be_bytes(take(b, 4)?.try_into().unwrap()) as i64),
        b'l' | b'L' => Value::Int(i64::from_be_bytes(take(b, 8)?.try_into().unwrap())),
        b'T' => Value::Int(u64::from_be_bytes(take(b, 8)?.try_into().unwrap()) as i64),
        b'f' => Value::Float(f32::from_be_bytes(take(b, 4)?.try_into().unwrap()) as f64),
        b'd' => Value::Float(f64::from_be_bytes(take(b, 8)?.try_into().unwrap())),
        b'D' => {
            take(b, 5)?;
            Value::Other
        }
        b'S' | b'x' => {
            let n = u32::from_be_bytes(take(b, 4)?.try_into().unwrap()) as usize;
            Value::Str(take(b, n)?)
        }
        b'F' => {
            let n = u32::from_be_bytes(take(b, 4)?.try_into().unwrap()) as usize;
            Value::Table(Table { raw: take(b, n)? })
        }
        b'A' => {
            let n = u32::from_be_bytes(take(b, 4)?.try_into().unwrap()) as usize;
            take(b, n)?;
            Value::Other
        }
        b'V' => Value::Other,
        _ => return Err(Malformed("unknown field type")),
    })
}

/// Appends a method frame.
pub fn method(out: &mut Vec<u8>, channel: u16, class: u16, method: u16, args: impl FnOnce(&mut Out<'_>)) {
    let at = out.len();
    out.push(FRAME_METHOD);
    out.extend_from_slice(&channel.to_be_bytes());
    out.extend_from_slice(&[0; 4]);
    out.extend_from_slice(&class.to_be_bytes());
    out.extend_from_slice(&method.to_be_bytes());
    args(&mut Out::new(out));
    let size = (out.len() - at - 7) as u32;
    out[at + 3..at + 7].copy_from_slice(&size.to_be_bytes());
    out.push(FRAME_END);
}

/// Appends a content header and the body frames of one message.
/// `props` are the property flags and list as the publisher sent them.
pub fn content(out: &mut Vec<u8>, channel: u16, props: &[u8], body: &[u8], frame_max: usize) {
    let at = out.len();
    out.push(FRAME_HEADER);
    out.extend_from_slice(&channel.to_be_bytes());
    out.extend_from_slice(&[0; 4]);
    out.extend_from_slice(&60u16.to_be_bytes());
    out.extend_from_slice(&0u16.to_be_bytes());
    out.extend_from_slice(&(body.len() as u64).to_be_bytes());
    if props.len() >= 2 {
        out.extend_from_slice(props);
    } else {
        out.extend_from_slice(&0u16.to_be_bytes());
    }
    let size = (out.len() - at - 7) as u32;
    out[at + 3..at + 7].copy_from_slice(&size.to_be_bytes());
    out.push(FRAME_END);
    let room = frame_max.saturating_sub(8).max(1);
    for chunk in body.chunks(room) {
        out.push(FRAME_BODY);
        out.extend_from_slice(&channel.to_be_bytes());
        out.extend_from_slice(&(chunk.len() as u32).to_be_bytes());
        out.extend_from_slice(chunk);
        out.push(FRAME_END);
    }
}

pub fn heartbeat(out: &mut Vec<u8>) {
    out.extend_from_slice(&[FRAME_HEARTBEAT, 0, 0, 0, 0, 0, 0, FRAME_END]);
}

/// The properties a content header carries, as far as the server needs them.
#[derive(Default, Debug)]
pub struct Props<'a> {
    pub headers: Option<Table<'a>>,
    pub expiration_ms: Option<i64>,
    pub priority: Option<u8>,
}

/// Reads `flags + property list` (a content header after its body size).
pub fn parse_props(raw: &[u8]) -> R<Props<'_>> {
    let mut r = In::new(raw);
    let flags = r.u16()?;
    if flags & 1 != 0 {
        return Err(Malformed("continued property flags are not supported"));
    }
    let mut p = Props::default();
    for bit in (2..=15).rev() {
        if flags & (1 << bit) == 0 {
            continue;
        }
        match bit {
            15 | 14 | 10 | 9 | 7 | 5 | 4 | 3 | 2 => {
                r.shortstr()?;
            }
            13 => p.headers = Some(r.table()?),
            12 => {
                r.u8()?;
            }
            11 => p.priority = Some(r.u8()?),
            8 => p.expiration_ms = r.shortstr()?.trim().parse().ok(),
            6 => {
                r.u64()?;
            }
            _ => {}
        }
    }
    Ok(p)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bits_pack_lsb_first() {
        let mut v = Vec::new();
        {
            let mut o = Out::new(&mut v);
            o.bit(true);
            o.bit(false);
            o.bit(true);
            o.u16(7);
            o.bit(true);
        }
        assert_eq!(v, [0b101, 0, 7, 1]);
        let mut r = In::new(&v);
        assert!(r.bit().unwrap() && !r.bit().unwrap() && r.bit().unwrap());
        assert_eq!(r.u16().unwrap(), 7);
        assert!(r.bit().unwrap());
    }

    #[test]
    fn tables_read() {
        let mut v = Vec::new();
        Out::new(&mut v).table(&[("x-message-ttl", Value::Int(60000)), ("x-dead-letter-exchange", Value::Str(b"dlx"))]);
        let t = In::new(&v).table().unwrap();
        assert_eq!(t.get("x-message-ttl").and_then(|v| v.int()), Some(60000));
        assert_eq!(t.get("x-dead-letter-exchange").and_then(|v| v.str().map(str::to_string)), Some("dlx".into()));
    }
}
