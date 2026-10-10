//! Where a command writes its answer.
//!
//! Commands never build a reply value. They call [`Reply`] methods as they
//! go, and the implementation decides what the call becomes. [`Resp`] appends
//! RESP2 or RESP3 bytes straight into the connection's output buffer, so a
//! `GET` copies the value once, from the slot into the socket buffer, while
//! the shard lock is still held. [`Collect`] builds a small tree that the SQL
//! surface turns into rows.

/// RESP3 adds types RESP2 lacks; each method says what RESP2 sends instead.
pub trait Reply {
    fn simple(&mut self, s: &str);
    /// A whole error line, including its code (`ERR`, `WRONGTYPE`, …).
    fn error(&mut self, e: &str);
    fn int(&mut self, n: i64);
    fn bulk(&mut self, b: &[u8]);
    /// A missing value: RESP2 `$-1`, RESP3 `_`.
    fn null(&mut self);
    /// A missing array: RESP2 `*-1`, RESP3 `_`.
    fn null_array(&mut self);
    fn array(&mut self, n: usize);
    /// `n` key/value pairs follow: RESP3 `%`, RESP2 a flat array of `2n`.
    fn map(&mut self, n: usize);
    /// RESP3 `~`, RESP2 an array.
    fn set(&mut self, n: usize);
    /// RESP3 `,`, RESP2 a bulk string.
    fn double(&mut self, f: f64);
    /// RESP3 `#`, RESP2 an integer.
    fn boolean(&mut self, b: bool);
    /// RESP3 `=` with a `txt` prefix, RESP2 a bulk string.
    fn verbatim(&mut self, s: &str);
    /// Whether the client speaks RESP3, for the few replies that differ in
    /// shape rather than only in type.
    fn resp3(&self) -> bool {
        false
    }

    fn ok(&mut self) {
        self.simple("OK");
    }

    fn err(&mut self, e: &super::KvError) {
        self.error(&e.to_string());
    }
}

/// Formats a double as Redis does: `inf`, `-inf`, integers without a point,
/// everything else in its shortest round-trip form.
pub fn format_double(f: f64, out: &mut Vec<u8>) {
    if f.is_infinite() {
        out.extend_from_slice(if f > 0.0 { b"inf" } else { b"-inf" });
    } else if f.is_nan() {
        out.extend_from_slice(b"nan");
    } else if f == 0.0 && f.is_sign_negative() {
        out.extend_from_slice(b"-0");
    } else if f == f.trunc() && f.abs() < 1e17 {
        write_i64(out, f as i64);
    } else {
        use std::io::Write;
        let _ = write!(out, "{f}");
    }
}

pub fn double_string(f: f64) -> Vec<u8> {
    let mut v = Vec::with_capacity(24);
    format_double(f, &mut v);
    v
}

/// Decimal digits of `n`, without the allocation `to_string` makes.
#[inline]
pub fn write_i64(out: &mut Vec<u8>, n: i64) {
    let mut buf = [0u8; 20];
    let mut i = buf.len();
    let negative = n < 0;
    let mut v = n.unsigned_abs();
    loop {
        i -= 1;
        buf[i] = b'0' + (v % 10) as u8;
        v /= 10;
        if v == 0 {
            break;
        }
    }
    if negative {
        out.push(b'-');
    }
    out.extend_from_slice(&buf[i..]);
}

#[inline]
fn write_usize(out: &mut Vec<u8>, n: usize) {
    write_i64(out, n as i64);
}

/// RESP bytes into a buffer the connection writes out.
pub struct Resp<'a> {
    pub out: &'a mut Vec<u8>,
    pub v3: bool,
}

impl Reply for Resp<'_> {
    #[inline]
    fn simple(&mut self, s: &str) {
        self.out.push(b'+');
        self.out.extend_from_slice(s.as_bytes());
        self.out.extend_from_slice(b"\r\n");
    }
    fn error(&mut self, e: &str) {
        self.out.push(b'-');
        // A newline inside an error would end the frame early.
        for &b in e.as_bytes() {
            self.out.push(if b == b'\r' || b == b'\n' { b' ' } else { b });
        }
        self.out.extend_from_slice(b"\r\n");
    }
    #[inline]
    fn int(&mut self, n: i64) {
        self.out.push(b':');
        write_i64(self.out, n);
        self.out.extend_from_slice(b"\r\n");
    }
    #[inline]
    fn bulk(&mut self, b: &[u8]) {
        self.out.push(b'$');
        write_usize(self.out, b.len());
        self.out.extend_from_slice(b"\r\n");
        self.out.extend_from_slice(b);
        self.out.extend_from_slice(b"\r\n");
    }
    #[inline]
    fn null(&mut self) {
        self.out
            .extend_from_slice(if self.v3 { b"_\r\n" } else { b"$-1\r\n" });
    }
    fn null_array(&mut self) {
        self.out
            .extend_from_slice(if self.v3 { b"_\r\n" } else { b"*-1\r\n" });
    }
    #[inline]
    fn array(&mut self, n: usize) {
        self.out.push(b'*');
        write_usize(self.out, n);
        self.out.extend_from_slice(b"\r\n");
    }
    fn map(&mut self, n: usize) {
        if self.v3 {
            self.out.push(b'%');
            write_usize(self.out, n);
        } else {
            self.out.push(b'*');
            write_usize(self.out, n * 2);
        }
        self.out.extend_from_slice(b"\r\n");
    }
    fn set(&mut self, n: usize) {
        self.out.push(if self.v3 { b'~' } else { b'*' });
        write_usize(self.out, n);
        self.out.extend_from_slice(b"\r\n");
    }
    fn double(&mut self, f: f64) {
        let mut text = Vec::with_capacity(24);
        format_double(f, &mut text);
        if self.v3 {
            self.out.push(b',');
            self.out.extend_from_slice(&text);
            self.out.extend_from_slice(b"\r\n");
        } else {
            self.bulk(&text);
        }
    }
    fn boolean(&mut self, b: bool) {
        if self.v3 {
            self.out.extend_from_slice(if b { b"#t\r\n" } else { b"#f\r\n" });
        } else {
            self.int(i64::from(b));
        }
    }
    fn verbatim(&mut self, s: &str) {
        if self.v3 {
            self.out.push(b'=');
            write_usize(self.out, s.len() + 4);
            self.out.extend_from_slice(b"\r\ntxt:");
            self.out.extend_from_slice(s.as_bytes());
            self.out.extend_from_slice(b"\r\n");
        } else {
            self.bulk(s.as_bytes());
        }
    }
    fn resp3(&self) -> bool {
        self.v3
    }
}

/// A reply as a tree, for the SQL surface and for tests.
#[derive(Debug, Clone, PartialEq)]
pub enum Item {
    Simple(String),
    Error(String),
    Int(i64),
    Bulk(Vec<u8>),
    Null,
    NullArray,
    Double(f64),
    Bool(bool),
    Array(Vec<Item>),
    Map(Vec<(Item, Item)>),
}

/// Builds [`Item`]s. Containers are opened with a count and filled by the
/// calls that follow, so this keeps a stack of the ones still filling.
#[derive(Default)]
pub struct Collect {
    pub done: Vec<Item>,
    stack: Vec<(Kind, usize, Vec<Item>)>,
}

#[derive(Clone, Copy)]
enum Kind {
    Array,
    Map,
}

impl Collect {
    fn push(&mut self, item: Item) {
        let mut item = item;
        loop {
            match self.stack.last_mut() {
                None => {
                    self.done.push(item);
                    return;
                }
                Some((_, want, items)) => {
                    items.push(item);
                    if items.len() < *want {
                        return;
                    }
                }
            }
            let (kind, _, items) = self.stack.pop().expect("just matched");
            item = match kind {
                Kind::Array => Item::Array(items),
                Kind::Map => {
                    let mut pairs = Vec::with_capacity(items.len() / 2);
                    let mut it = items.into_iter();
                    while let (Some(k), Some(v)) = (it.next(), it.next()) {
                        pairs.push((k, v));
                    }
                    Item::Map(pairs)
                }
            };
        }
    }

    fn open(&mut self, kind: Kind, n: usize) {
        if n == 0 {
            self.push(match kind {
                Kind::Array => Item::Array(Vec::new()),
                Kind::Map => Item::Map(Vec::new()),
            });
        } else {
            self.stack.push((kind, n, Vec::with_capacity(n.min(1024))));
        }
    }

    /// The single reply a command produced.
    pub fn take(mut self) -> Item {
        self.done.pop().unwrap_or(Item::Null)
    }
}

impl Reply for Collect {
    fn simple(&mut self, s: &str) {
        self.push(Item::Simple(s.to_string()));
    }
    fn error(&mut self, e: &str) {
        self.push(Item::Error(e.to_string()));
    }
    fn int(&mut self, n: i64) {
        self.push(Item::Int(n));
    }
    fn bulk(&mut self, b: &[u8]) {
        self.push(Item::Bulk(b.to_vec()));
    }
    fn null(&mut self) {
        self.push(Item::Null);
    }
    fn null_array(&mut self) {
        self.push(Item::NullArray);
    }
    fn array(&mut self, n: usize) {
        self.open(Kind::Array, n);
    }
    fn map(&mut self, n: usize) {
        self.open(Kind::Map, n * 2);
    }
    fn set(&mut self, n: usize) {
        self.open(Kind::Array, n);
    }
    fn double(&mut self, f: f64) {
        self.push(Item::Double(f));
    }
    fn boolean(&mut self, b: bool) {
        self.push(Item::Bool(b));
    }
    fn verbatim(&mut self, s: &str) {
        self.push(Item::Bulk(s.as_bytes().to_vec()));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resp2_and_resp3_frames() {
        let mut out = Vec::new();
        let mut r = Resp { out: &mut out, v3: false };
        r.array(3);
        r.int(-42);
        r.bulk(b"hi");
        r.null();
        r.map(1);
        r.bulk(b"k");
        r.double(1.5);
        assert_eq!(out, b"*3\r\n:-42\r\n$2\r\nhi\r\n$-1\r\n*2\r\n$1\r\nk\r\n$3\r\n1.5\r\n");
        let mut out = Vec::new();
        let mut r = Resp { out: &mut out, v3: true };
        r.map(1);
        r.bulk(b"k");
        r.double(3.0);
        r.null();
        r.boolean(true);
        assert_eq!(out, b"%1\r\n$1\r\nk\r\n,3\r\n_\r\n#t\r\n");
    }

    #[test]
    fn collect_nests() {
        let mut c = Collect::default();
        c.array(2);
        c.array(0);
        c.map(1);
        c.bulk(b"f");
        c.int(1);
        assert_eq!(
            c.take(),
            Item::Array(vec![
                Item::Array(vec![]),
                Item::Map(vec![(Item::Bulk(b"f".to_vec()), Item::Int(1))])
            ])
        );
    }

    #[test]
    fn integers_and_doubles_format_like_redis() {
        for n in [0i64, 7, -7, i64::MAX, i64::MIN, 1_000_000] {
            let mut v = Vec::new();
            write_i64(&mut v, n);
            assert_eq!(v, n.to_string().into_bytes());
        }
        assert_eq!(double_string(3.0), b"3");
        assert_eq!(double_string(1.5), b"1.5");
        assert_eq!(double_string(f64::INFINITY), b"inf");
        assert_eq!(double_string(f64::NEG_INFINITY), b"-inf");
        assert_eq!(double_string(0.1), b"0.1");
    }
}
