//! Routing without parsing (§4.9 G3, B1a.6).
//!
//! Per statement: find the key value (an explicit key, a Bind parameter by
//! index, or a literal slot), normalise it to its family's bytes, hash it
//! (`rasterizeddb_router::key::shard_block`) and look the block up in the
//! map. The template that says where the key is comes from the per-core
//! cache, the shared second level, or, once per shape, a shard's
//! `Classify`.

use rasterizeddb_router::KeyFamily;
use rasterizeddb_router::key::{KeyValue, shard_block};
use rasterizeddb_router::literals::{Literal, parse_uuid};
use rasterizeddb_router::rshard::{Request, Response, RouteTemplate, Source};

use crate::core::{CallError, Core};
use crate::state::TemplateEntry;

/// An explicit route key (`SET rdb.route_key`, the native `route=` option).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum OwnedKey {
    Int(i128),
    Uuid([u8; 16]),
    Text(String),
}

impl OwnedKey {
    pub fn as_value(&self) -> KeyValue<'_> {
        match self {
            OwnedKey::Int(v) => KeyValue::Int(*v),
            OwnedKey::Uuid(b) => KeyValue::Uuid(*b),
            OwnedKey::Text(s) => KeyValue::Text(s),
        }
    }

    pub fn family(&self) -> KeyFamily {
        self.as_value().family()
    }

    /// The level-0 block this key lands in.
    pub fn block(&self) -> u64 {
        shard_block(self.as_value(), self.family()).expect("a key routes in its own family")
    }

    /// The value of `SET rdb.route_key = <v>`: a bare integer is an integer
    /// key; a quoted `'i:42'`, `'u:<uuid>'` or `'t:<text>'` names its
    /// family; any other quoted string is a text key.
    pub fn parse_setting(v: &str) -> Result<OwnedKey, String> {
        let v = v.trim();
        if let Some(inner) = v.strip_prefix('\'').and_then(|s| s.strip_suffix('\'')) {
            let inner = inner.replace("''", "'");
            if let Some(i) = inner.strip_prefix("i:") {
                return i.trim().parse().map(OwnedKey::Int).map_err(|_| format!("{i:?} is not an integer key"));
            }
            if let Some(u) = inner.strip_prefix("u:") {
                return parse_uuid(u).map(OwnedKey::Uuid).ok_or_else(|| format!("{u:?} is not a UUID"));
            }
            if let Some(t) = inner.strip_prefix("t:") {
                return Ok(OwnedKey::Text(t.to_string()));
            }
            return Ok(OwnedKey::Text(inner));
        }
        v.parse::<i128>()
            .map(OwnedKey::Int)
            .map_err(|_| format!("rdb.route_key takes an integer or a quoted string, not {v:?}"))
    }

    pub fn show(&self) -> String {
        match self {
            OwnedKey::Int(v) => format!("i:{v}"),
            OwnedKey::Uuid(b) => format!("u:{}", b.iter().map(|x| format!("{x:02x}")).collect::<String>()),
            OwnedKey::Text(s) => format!("t:{s}"),
        }
    }
}

/// A literal slot as a key of `family`, as the shard coerces it.
pub fn literal_block(lit: &Literal, family: KeyFamily) -> Option<u64> {
    let v = match family {
        KeyFamily::Integer => KeyValue::Int(lit.as_int()?),
        KeyFamily::Uuid => KeyValue::Uuid(lit.as_uuid()?),
        KeyFamily::Text => KeyValue::Text(lit.as_text()?),
    };
    shard_block(v, family).ok()
}

/// A Bind parameter as a key of `family`: text format exactly as the server
/// parses it, binary by width (int2/int4/int8 big-endian, 16 UUID octets,
/// text bytes). `None` for NULL or a value the family cannot take, which
/// leaves the statement without a key.
pub fn param_block(binary: bool, value: Option<&[u8]>, family: KeyFamily) -> Option<u64> {
    let b = value?;
    let v = match (family, binary) {
        (KeyFamily::Integer, false) => KeyValue::Int(std::str::from_utf8(b).ok()?.trim().parse().ok()?),
        (KeyFamily::Integer, true) => KeyValue::Int(match b.len() {
            2 => i16::from_be_bytes(b.try_into().ok()?) as i128,
            4 => i32::from_be_bytes(b.try_into().ok()?) as i128,
            8 => i64::from_be_bytes(b.try_into().ok()?) as i128,
            _ => return None,
        }),
        (KeyFamily::Uuid, false) => KeyValue::Uuid(parse_uuid(std::str::from_utf8(b).ok()?)?),
        (KeyFamily::Uuid, true) => KeyValue::Uuid(b.try_into().ok()?),
        (KeyFamily::Text, _) => KeyValue::Text(std::str::from_utf8(b).ok()?),
    };
    shard_block(v, family).ok()
}

/// The template for `text` in `database`: this core's cache, the shared
/// level, then a `Classify` to `shard` (the home shard by default).
pub async fn template(
    core: &Core,
    database: &str,
    source: Source,
    text: &str,
    shard: u32,
) -> Result<RouteTemplate, String> {
    let parse = source == Source::Parse;
    let min = core.shared.schema_epoch.load(std::sync::atomic::Ordering::Acquire);
    if let Some(e) = core.templates.borrow().get(database, parse, text, min) {
        core.counters.borrow_mut().template_hits += 1;
        return Ok(e.template.clone());
    }
    if let Some(e) = core.shared.templates.get(database, parse, text, min) {
        core.counters.borrow_mut().template_hits += 1;
        let t = e.template.clone();
        core.templates.borrow_mut().put(core.shared.cfg.template_cache_per_core, database, parse, text, e);
        return Ok(t);
    }
    core.counters.borrow_mut().template_misses += 1;
    let link = core.link(shard)?;
    let req = Request::Classify { database: database.to_string(), schema_epoch: min, source, text: text.to_string() }.encode();
    match link.call(core, &req).await {
        Ok(Response::Template { schema_epoch, template }) => {
            core.shared.note_schema_epoch(schema_epoch);
            let e = TemplateEntry { template: template.clone(), schema_epoch };
            core.shared.templates.put(database, parse, text, e.clone());
            core.templates.borrow_mut().put(core.shared.cfg.template_cache_per_core, database, parse, text, e);
            Ok(template)
        }
        Ok(Response::Refused(r)) => {
            core.counters.borrow_mut().classify_errors += 1;
            Err(format!("{}: {}", r.sqlstate_str(), r.message))
        }
        Ok(other) => Err(format!("unexpected reply kind {:#04x} to Classify", other.kind())),
        Err(CallError::Down(e)) | Err(CallError::Lost(e)) | Err(CallError::Bad(e)) => {
            core.counters.borrow_mut().classify_errors += 1;
            Err(e)
        }
    }
}

/// Drops a template that a shard refuted with `RDB02` (B1a.6), from both
/// levels.
pub fn refute(core: &Core, database: &str, source: Source, text: &str) {
    let parse = source == Source::Parse;
    core.templates.borrow_mut().remove(database, parse, text);
    core.shared.templates.remove(database, parse, text);
    core.counters.borrow_mut().template_refuted += 1;
}
