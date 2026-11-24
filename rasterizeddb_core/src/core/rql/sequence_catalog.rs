//! `CREATE SEQUENCE`, `ALTER SEQUENCE`, `DROP SEQUENCE`, and where those
//! sequences are kept.
//!
//! A sequence is a row of the system table [`SEQUENCE_TABLE`]: its parameters
//! and `last_value`/`is_called`, as PostgreSQL's sequence relation holds them.
//! The row is created and dropped in the statement's transaction, as a view's
//! is (`rql::views`). The counter itself lives in memory
//! (`sequence::ExplicitSequence`) and is written ahead of itself by
//! [`flush`]: the row says a value up to [`sequence::RESERVE_AHEAD`] steps past
//! the last one handed out, so a crash resumes past every value a client
//! could have seen, leaving a gap as PostgreSQL's does.

use std::sync::atomic::Ordering;

use rclite::Arc;

use crate::core::database::Database;
use crate::core::mvcc::TxId;
use crate::core::rql::sequence::{self, ExplicitSequence};

/// The system table sequences are kept in.
pub const SEQUENCE_TABLE: &str = "__rdb_sequences";

const SEQUENCE_TABLE_DDL: &str = "CREATE TABLE __rdb_sequences (name TEXT PRIMARY KEY, data_type TEXT NOT NULL, \
     increment BIGINT NOT NULL, minvalue BIGINT NOT NULL, maxvalue BIGINT NOT NULL, start_value BIGINT NOT NULL, \
     cache_size BIGINT NOT NULL, cycle BOOLEAN NOT NULL, last_value BIGINT NOT NULL, is_called BOOLEAN NOT NULL, \
     owned_by TEXT NOT NULL)";

/// A refusal: its SQLSTATE and message.
pub type Refusal = (&'static str, String);

/// A sequence's definition, as stored.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SequenceDef {
    pub name: String,
    pub data_type: String,
    pub increment: i64,
    pub min: i64,
    pub max: i64,
    pub start: i64,
    pub cache: i64,
    pub cycle: bool,
    pub last_value: i64,
    pub is_called: bool,
    /// `table.column` of `OWNED BY`, or "".
    pub owned_by: String,
}

/// The options of `CREATE` or `ALTER SEQUENCE`, each as written (`None` when
/// absent; `Some(None)` for `NO MINVALUE` and the like).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SequenceOptions {
    pub data_type: Option<String>,
    pub increment: Option<i64>,
    pub min: Option<Option<i64>>,
    pub max: Option<Option<i64>>,
    pub start: Option<i64>,
    /// `RESTART [WITH n]`: `Some(None)` restarts at `START`.
    pub restart: Option<Option<i64>>,
    pub cache: Option<i64>,
    pub cycle: Option<bool>,
    /// `OWNED BY t.c` (`Some("t.c")`) or `OWNED BY NONE` (`Some("")`).
    pub owned_by: Option<String>,
}

/// A sequence statement, parsed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SequenceStatement {
    Create { name: String, if_not_exists: bool, options: SequenceOptions },
    Alter { name: String, if_exists: bool, options: SequenceOptions, rename_to: Option<String> },
    Drop { names: Vec<String>, if_exists: bool, cascade: bool },
}

/// The bounds of a sequence's type.
fn type_bounds(data_type: &str) -> Option<(i64, i64)> {
    Some(match data_type {
        "smallint" => (i16::MIN as i64, i16::MAX as i64),
        "integer" => (i32::MIN as i64, i32::MAX as i64),
        "bigint" => (i64::MIN, i64::MAX),
        _ => return None,
    })
}

/// A type name as `AS` writes it, in PostgreSQL's spelling.
fn type_name(word: &str) -> Option<&'static str> {
    Some(match word.to_ascii_lowercase().as_str() {
        "smallint" | "int2" => "smallint",
        "integer" | "int" | "int4" => "integer",
        "bigint" | "int8" => "bigint",
        _ => return None,
    })
}

/// Splits a statement into words, keeping a quoted name whole and `,`/`.`
/// apart.
fn words(sql: &str) -> Vec<String> {
    let mut out = Vec::new();
    let bytes = sql.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        let b = bytes[i];
        if b.is_ascii_whitespace() {
            i += 1;
        } else if b == b'"' {
            let end = crate::core::sql_scan::quoted_end(bytes, i, b'"');
            out.push(sql[i..end].to_string());
            i = end;
        } else if b == b',' || b == b'.' || b == b';' {
            out.push((b as char).to_string());
            i += 1;
        } else {
            let start = i;
            while i < bytes.len() && !bytes[i].is_ascii_whitespace() && !matches!(bytes[i], b',' | b'.' | b';' | b'"') {
                i += 1;
            }
            out.push(sql[start..i].to_string());
        }
    }
    out
}

/// A name word: a quoted one as written, an unquoted one lowercased.
fn name_of(word: &str) -> String {
    if word.starts_with('"') && word.ends_with('"') && word.len() >= 2 {
        word[1..word.len() - 1].replace("\"\"", "\"")
    } else {
        word.to_ascii_lowercase()
    }
}

struct Words {
    w: Vec<String>,
    at: usize,
}

impl Words {
    fn peek(&self) -> Option<&str> {
        self.w.get(self.at).map(String::as_str)
    }
    fn eat(&mut self, kw: &str) -> bool {
        if self.peek().is_some_and(|w| w.eq_ignore_ascii_case(kw)) {
            self.at += 1;
            true
        } else {
            false
        }
    }
    fn eat_all(&mut self, kws: &[&str]) -> bool {
        let saved = self.at;
        if kws.iter().all(|k| self.eat(k)) {
            true
        } else {
            self.at = saved;
            false
        }
    }
    /// A possibly qualified name: its last segment.
    fn name(&mut self) -> Option<String> {
        let mut name = name_of(self.w.get(self.at)?);
        self.at += 1;
        while self.peek() == Some(".") {
            self.at += 1;
            name = name_of(self.w.get(self.at)?);
            self.at += 1;
        }
        Some(name)
    }
    fn number(&mut self, what: &str) -> Result<i64, Refusal> {
        let word = self.w.get(self.at).cloned().unwrap_or_default();
        self.at += 1;
        word.trim_start_matches('+')
            .parse::<i64>()
            .map_err(|_| ("22023", format!("{what} must be an integer, not \"{word}\"")))
    }
    fn done(&self) -> bool {
        self.at >= self.w.len() || (self.peek() == Some(";") && self.at + 1 == self.w.len())
    }
}

/// Parses `CREATE SEQUENCE`, `ALTER SEQUENCE` and `DROP SEQUENCE`; `None` for
/// any other statement.
pub fn parse_sequence_statement(sql: &str) -> Option<Result<SequenceStatement, Refusal>> {
    let mut w = Words { w: words(sql.trim()), at: 0 };
    if w.eat("CREATE") {
        let temporary = w.eat("TEMP") || w.eat("TEMPORARY");
        w.eat("UNLOGGED");
        if !w.eat("SEQUENCE") {
            return None;
        }
        if temporary {
            return Some(Err(("0A000", "CREATE TEMPORARY SEQUENCE is not supported yet".to_string())));
        }
        let if_not_exists = w.eat_all(&["IF", "NOT", "EXISTS"]);
        let Some(name) = w.name() else {
            return Some(Err(("42601", "syntax error in CREATE SEQUENCE: expected a name".to_string())));
        };
        return Some(options(&mut w, false).map(|options| SequenceStatement::Create { name, if_not_exists, options }));
    }
    if w.eat("ALTER") {
        if !w.eat("SEQUENCE") {
            return None;
        }
        let if_exists = w.eat_all(&["IF", "EXISTS"]);
        let Some(name) = w.name() else {
            return Some(Err(("42601", "syntax error in ALTER SEQUENCE: expected a name".to_string())));
        };
        if w.eat_all(&["RENAME", "TO"]) {
            let Some(to) = w.name() else {
                return Some(Err(("42601", "syntax error in ALTER SEQUENCE: expected the new name".to_string())));
            };
            return Some(Ok(SequenceStatement::Alter { name, if_exists, options: SequenceOptions::default(), rename_to: Some(to) }));
        }
        return Some(options(&mut w, true).map(|options| SequenceStatement::Alter { name, if_exists, options, rename_to: None }));
    }
    if w.eat("DROP") {
        if !w.eat("SEQUENCE") {
            return None;
        }
        let if_exists = w.eat_all(&["IF", "EXISTS"]);
        let mut names = Vec::new();
        loop {
            let Some(name) = w.name() else {
                return Some(Err(("42601", "syntax error in DROP SEQUENCE: expected a name".to_string())));
            };
            names.push(name);
            if !w.eat(",") {
                break;
            }
        }
        let cascade = w.eat("CASCADE");
        if !cascade {
            w.eat("RESTRICT");
        }
        if !w.done() {
            return Some(Err(("42601", format!("syntax error at or near \"{}\"", w.peek().unwrap_or("")))));
        }
        return Some(Ok(SequenceStatement::Drop { names, if_exists, cascade }));
    }
    None
}

fn options(w: &mut Words, alter: bool) -> Result<SequenceOptions, Refusal> {
    let mut o = SequenceOptions::default();
    while !w.done() {
        if w.eat("AS") {
            let word = w.w.get(w.at).cloned().unwrap_or_default();
            w.at += 1;
            o.data_type = Some(
                type_name(&word)
                    .ok_or_else(|| ("22023", "sequence type must be smallint, integer, or bigint".to_string()))?
                    .to_string(),
            );
        } else if w.eat("INCREMENT") {
            w.eat("BY");
            o.increment = Some(w.number("INCREMENT")?);
        } else if w.eat_all(&["NO", "MINVALUE"]) {
            o.min = Some(None);
        } else if w.eat_all(&["NO", "MAXVALUE"]) {
            o.max = Some(None);
        } else if w.eat_all(&["NO", "CYCLE"]) {
            o.cycle = Some(false);
        } else if w.eat("MINVALUE") {
            o.min = Some(Some(w.number("MINVALUE")?));
        } else if w.eat("MAXVALUE") {
            o.max = Some(Some(w.number("MAXVALUE")?));
        } else if w.eat("START") {
            w.eat("WITH");
            o.start = Some(w.number("START")?);
        } else if alter && w.eat("RESTART") {
            if w.eat("WITH") || w.peek().is_some_and(|p| p.trim_start_matches(['-', '+']).starts_with(|c: char| c.is_ascii_digit())) {
                o.restart = Some(Some(w.number("RESTART")?));
            } else {
                o.restart = Some(None);
            }
        } else if w.eat("CACHE") {
            o.cache = Some(w.number("CACHE")?);
        } else if w.eat("CYCLE") {
            o.cycle = Some(true);
        } else if w.eat_all(&["OWNED", "BY"]) {
            if w.eat("NONE") {
                o.owned_by = Some(String::new());
            } else {
                let mut parts = vec![name_of(w.w.get(w.at).map(String::as_str).unwrap_or(""))];
                w.at += 1;
                while w.eat(".") {
                    parts.push(name_of(w.w.get(w.at).map(String::as_str).unwrap_or("")));
                    w.at += 1;
                }
                if parts.len() < 2 {
                    return Err(("42601", "OWNED BY names a column as table.column".to_string()));
                }
                let n = parts.len();
                o.owned_by = Some(format!("{}.{}", parts[n - 2], parts[n - 1]));
            }
        } else {
            return Err(("42601", format!("syntax error at or near \"{}\"", w.peek().unwrap_or(""))));
        }
    }
    Ok(o)
}

/// The definition `options` give a new sequence `name`, with PostgreSQL's
/// defaults and checks (22023).
pub fn definition_of(name: &str, options: &SequenceOptions) -> Result<SequenceDef, Refusal> {
    let mut def = SequenceDef {
        name: name.to_string(),
        data_type: "bigint".to_string(),
        increment: 1,
        min: 1,
        max: i64::MAX,
        start: 1,
        cache: 1,
        cycle: false,
        last_value: 1,
        is_called: false,
        owned_by: String::new(),
    };
    apply(&mut def, options, true)?;
    Ok(def)
}

/// `options` applied to `def`: a new one's (`create`) or an `ALTER`'s.
pub fn apply(def: &mut SequenceDef, options: &SequenceOptions, create: bool) -> Result<(), Refusal> {
    let type_changed = options.data_type.is_some();
    if let Some(t) = &options.data_type {
        def.data_type = t.clone();
    }
    let (type_min, type_max) = type_bounds(&def.data_type).unwrap_or((i64::MIN, i64::MAX));
    if let Some(inc) = options.increment {
        if inc == 0 {
            return Err(("22023", "INCREMENT must not be zero".to_string()));
        }
        def.increment = inc;
    }
    let ascending = def.increment > 0;
    match options.min {
        Some(Some(v)) => def.min = v,
        Some(None) => def.min = if ascending { 1 } else { type_min },
        None if create || type_changed => def.min = if ascending { 1 } else { type_min },
        None => {}
    }
    match options.max {
        Some(Some(v)) => def.max = v,
        Some(None) => def.max = if ascending { type_max } else { -1 },
        None if create || type_changed => def.max = if ascending { type_max } else { -1 },
        None => {}
    }
    if def.min < type_min || def.max > type_max {
        let (which, value) = if def.min < type_min { ("MINVALUE", def.min) } else { ("MAXVALUE", def.max) };
        return Err(("22023", format!("{which} ({value}) is out of range for sequence data type {}", def.data_type)));
    }
    if def.min >= def.max {
        return Err(("22023", format!("MINVALUE ({}) must be less than MAXVALUE ({})", def.min, def.max)));
    }
    match options.start {
        Some(v) => def.start = v,
        None if create => def.start = if ascending { def.min } else { def.max },
        None => {}
    }
    if def.start < def.min {
        return Err(("22023", format!("START value ({}) cannot be less than MINVALUE ({})", def.start, def.min)));
    }
    if def.start > def.max {
        return Err(("22023", format!("START value ({}) cannot be greater than MAXVALUE ({})", def.start, def.max)));
    }
    if let Some(cache) = options.cache {
        if cache < 1 {
            return Err(("22023", format!("CACHE ({cache}) must be greater than zero")));
        }
        def.cache = cache;
    }
    if let Some(cycle) = options.cycle {
        def.cycle = cycle;
    }
    if let Some(owned) = &options.owned_by {
        def.owned_by = owned.clone();
    }
    if create {
        def.last_value = def.start;
        def.is_called = false;
    }
    if let Some(restart) = options.restart {
        let value = restart.unwrap_or(def.start);
        if value < def.min || value > def.max {
            return Err((
                "22023",
                format!("RESTART value ({value}) cannot be outside MINVALUE ({}) and MAXVALUE ({})", def.min, def.max),
            ));
        }
        def.last_value = value;
        def.is_called = false;
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Storage
// ---------------------------------------------------------------------------

/// Runs `sql` in `tx`, or on a task of its own as a statement of its own;
/// boxed, as `views::run` is and for its reason.
fn run(
    database: &Arc<Database>,
    sql: &str,
    tx: Option<TxId>,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Vec<u8>, String>> + Send>> {
    use crate::core::rql::{executor, lexer_s1::recognize_query_purpose};
    let database = database.clone();
    let sql = sql.to_string();
    Box::pin(async move {
        let purpose = recognize_query_purpose(&sql).ok_or_else(|| format!("could not parse: {sql}"))?;
        let response = match tx {
            Some(tx) => executor::execute_in_txn(purpose, database, tx).await,
            None => tokio::spawn(executor::execute_as(crate::core::mvcc::owner::OwnerKind::Statement, purpose, database))
                .await
                .map_err(|e| format!("the sequence catalogue statement did not finish: {e}"))?,
        };
        if response.first() == Some(&3) {
            return Err(String::from_utf8_lossy(&response[1..]).to_string());
        }
        Ok(response)
    })
}

/// The sequences `tx` sees, by name (all of them for `None`).
pub async fn load(database: &Arc<Database>, names: Option<&[String]>, tx: Option<TxId>) -> Result<Vec<SequenceDef>, String> {
    if !database.has_table(SEQUENCE_TABLE) {
        return Ok(Vec::new());
    }
    let filter = match names {
        Some([]) => return Ok(Vec::new()),
        Some(names) => format!(
            " WHERE name IN ({})",
            names.iter().map(|n| crate::auth::quote(n)).collect::<Vec<_>>().join(", ")
        ),
        None => String::new(),
    };
    let sql = format!(
        "SELECT name, data_type, increment, minvalue, maxvalue, start_value, cache_size, cycle, last_value, \
         is_called, owned_by FROM {SEQUENCE_TABLE}{filter}"
    );
    let response = run(database, &sql, tx).await?;
    if response.first() != Some(&2) {
        return Ok(Vec::new());
    }
    let rows = crate::core::row::row::vec_into_rows(&response[1..]).map_err(|e| e.to_string())?;
    let mut out = Vec::with_capacity(rows.len());
    for row in rows {
        let bytes = |i: usize| row.columns.get(i).map(|c| c.data.into_slice().to_vec()).unwrap_or_default();
        let text = |i: usize| String::from_utf8_lossy(&bytes(i)).into_owned();
        let int = |i: usize| {
            let b = bytes(i);
            <[u8; 8]>::try_from(b.as_slice()).map(i64::from_le_bytes).unwrap_or(0)
        };
        let flag = |i: usize| bytes(i).first().copied().unwrap_or(0) != 0;
        out.push(SequenceDef {
            name: text(0),
            data_type: text(1),
            increment: int(2),
            min: int(3),
            max: int(4),
            start: int(5),
            cache: int(6),
            cycle: flag(7),
            last_value: int(8),
            is_called: flag(9),
            owned_by: text(10),
        });
    }
    Ok(out)
}

/// Writes `def` (a new row, or the one under `previous`'s name replaced).
pub async fn store(database: &Arc<Database>, def: &SequenceDef, previous: Option<&str>, tx: Option<TxId>) -> Result<(), String> {
    use crate::auth::quote;
    if !database.has_table(SEQUENCE_TABLE) {
        run(database, SEQUENCE_TABLE_DDL, None).await?;
    }
    let sql = match previous {
        Some(previous) => format!(
            "UPDATE {SEQUENCE_TABLE} SET name = {}, data_type = {}, increment = {}, minvalue = {}, maxvalue = {}, \
             start_value = {}, cache_size = {}, cycle = {}, last_value = {}, is_called = {}, owned_by = {} WHERE name = {}",
            quote(&def.name),
            quote(&def.data_type),
            def.increment,
            def.min,
            def.max,
            def.start,
            def.cache,
            def.cycle,
            def.last_value,
            def.is_called,
            quote(&def.owned_by),
            quote(previous)
        ),
        None => format!(
            "INSERT INTO {SEQUENCE_TABLE} (name, data_type, increment, minvalue, maxvalue, start_value, cache_size, \
             cycle, last_value, is_called, owned_by) VALUES ({}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {})",
            quote(&def.name),
            quote(&def.data_type),
            def.increment,
            def.min,
            def.max,
            def.start,
            def.cache,
            def.cycle,
            def.last_value,
            def.is_called,
            quote(&def.owned_by)
        ),
    };
    run(database, &sql, tx).await?;
    Ok(())
}

/// Deletes the sequence `name`'s row.
pub async fn remove(database: &Arc<Database>, name: &str, tx: Option<TxId>) -> Result<(), String> {
    let sql = format!("DELETE FROM {SEQUENCE_TABLE} WHERE name = {}", crate::auth::quote(name));
    run(database, &sql, tx).await.map(|_| ())
}

/// The in-memory counter for `def`, as its row says: resumed at its
/// reservation, so nothing handed out before a restart is handed out again.
pub fn register(database: &Database, def: &SequenceDef) {
    sequence::register_explicit(
        database.id,
        &def.name,
        ExplicitSequence {
            data_type: def.data_type.clone(),
            increment: def.increment,
            min: def.min,
            max: def.max,
            start: def.start,
            cache: def.cache,
            cycle: def.cycle,
            last: std::sync::atomic::AtomicI64::new(def.last_value),
            called: std::sync::atomic::AtomicBool::new(def.is_called),
            reserved: std::sync::atomic::AtomicI64::new(if def.is_called {
                def.last_value
            } else {
                def.last_value.saturating_sub(def.increment)
            }),
            log_cnt: std::sync::atomic::AtomicI64::new(0),
        },
    );
}

/// The databases whose sequences this process has read, by id.
static LOADED: std::sync::LazyLock<dashmap::DashMap<u64, ()>> = std::sync::LazyLock::new(dashmap::DashMap::new);

/// Reads every committed sequence into memory, once per process, before a
/// statement may take a value from one.
pub async fn ensure_loaded(database: &Arc<Database>) -> Result<(), String> {
    if LOADED.contains_key(&database.id) || !database.has_table(SEQUENCE_TABLE) {
        return Ok(());
    }
    for def in load(database, None, None).await? {
        if sequence::explicit(database.id, &def.name).is_none() {
            register(database, &def);
        }
    }
    LOADED.insert(database.id, ());
    Ok(())
}

/// Writes ahead every sequence of `database` whose values have reached past
/// its reservation: its row's `last_value` set [`sequence::RESERVE_AHEAD`]
/// steps on, on a statement of its own (a value is not transactional).
pub async fn flush(database: &Arc<Database>) {
    if !database.has_table(SEQUENCE_TABLE) {
        return;
    }
    for (name, target) in sequence::explicit_needing_reserve(database.id) {
        let sql = format!(
            "UPDATE {SEQUENCE_TABLE} SET last_value = {target}, is_called = TRUE WHERE name = {}",
            crate::auth::quote(&name)
        );
        match run(database, &sql, None).await {
            Ok(_) => {
                if let Some(s) = sequence::explicit(database.id, &name) {
                    let reached = if s.increment > 0 {
                        s.reserved.load(Ordering::SeqCst).max(target)
                    } else {
                        s.reserved.load(Ordering::SeqCst).min(target)
                    };
                    s.reserved.store(reached, Ordering::SeqCst);
                }
            }
            Err(e) => log::warn!("sequence \"{name}\": its reservation could not be written ahead: {e}"),
        }
    }
}

/// The implicit sequences (`SERIAL`, identity, a `DEFAULT nextval`) the
/// tables of `database` read, as `(sequence, table, column)`.
pub fn implicit_sequences(database: &Database) -> Vec<(String, String, String)> {
    let mut out = Vec::new();
    for table in database.relation_names() {
        if table.starts_with("__") {
            continue;
        }
        let Some(view) = database.schema_view(&table) else {
            continue;
        };
        for d in view.schema().defaults.iter().filter(|d| !d.is_deleted) {
            if let Some(name) = sequence::parse_nextval(&d.expression) {
                out.push((name.to_ascii_lowercase(), table.clone(), d.column.clone()));
            }
        }
    }
    out
}

/// Whether `name` is a sequence `tx` can see: an explicit one, or one a
/// column default reads.
pub async fn exists(database: &Arc<Database>, name: &str, tx: Option<TxId>) -> bool {
    matches!(load(database, Some(&[name.to_string()]), tx).await, Ok(v) if !v.is_empty())
        || implicit_sequences(database).iter().any(|(s, _, _)| s == name)
}

/// `sql` with each sequence it reads as a relation (`SELECT last_value FROM
/// s`, `SELECT * FROM s`) written as the one-row derived table PostgreSQL's
/// sequence relation is: `last_value`, `log_cnt`, `is_called`. `None` when it
/// reads none. It was 42P01: a sequence is no table here.
///
/// An explicit sequence is read off its counter (`sequence::explicit`). An
/// implicit one (a `SERIAL` or identity column's, a name ending `_seq`) off
/// its counter once seeded, else off its column, which is what the counter
/// would be seeded from: its largest value, or the start (1) and not called
/// for an empty table. A write to one is PostgreSQL's 42809.
pub fn relations_as_rows(database: &Database, sql: &str) -> Result<Option<String>, Refusal> {
    use crate::core::sql_scan::contains_ignore_ascii_case;
    // Every statement passes here: a statement naming no sequence pays one
    // scan for `_` and one load (`explicit_mentioned_in`).
    let implicit = contains_ignore_ascii_case(sql, b"_seq");
    let explicit = sequence::explicit_mentioned_in(database.id, sql) && database.has_table(SEQUENCE_TABLE);
    if !explicit && !implicit {
        return Ok(None);
    }
    if !(contains_ignore_ascii_case(sql, b"FROM")
        || contains_ignore_ascii_case(sql, b"INTO")
        || contains_ignore_ascii_case(sql, b"UPDATE"))
    {
        return Ok(None);
    }
    let mut implicit_names: Option<Vec<(String, String, String)>> = None;
    let mut replaced: Vec<(std::ops::Range<usize>, String)> = Vec::new();
    for r in crate::core::rql::lock_set::relation_references(sql) {
        if database.has_table(&r.name)
            || database.views.contains(&r.name)
            || crate::core::rql::views::cte_names(sql).contains(&r.name.to_ascii_lowercase())
        {
            continue;
        }
        let explicit = explicit.then(|| sequence::explicit(database.id, &r.name)).flatten();
        if r.mode != crate::core::mvcc::table_locks::TableMode::AccessShare {
            let is_sequence = explicit.is_some()
                || implicit
                    && implicit_names.get_or_insert_with(|| implicit_sequences(database)).iter().any(|(s, _, _)| *s == r.name);
            if is_sequence {
                return Err(("42809", format!("cannot change sequence \"{}\"", r.name)));
            }
            continue;
        }
        let row = if let Some(s) = explicit {
            let called = s.called.load(Ordering::SeqCst);
            format!(
                "SELECT {}::bigint AS last_value, {}::bigint AS log_cnt, {} AS is_called",
                s.last.load(Ordering::SeqCst),
                if called { s.log_cnt.load(Ordering::Relaxed) } else { 0 },
                if called { "true" } else { "false" }
            )
        } else {
            let names = implicit_names.get_or_insert_with(|| implicit_sequences(database));
            let Some((_, table, column)) = names.iter().find(|(s, _, _)| *s == r.name) else {
                continue;
            };
            // `log_cnt` as PostgreSQL's would stand after `last` values from
            // 1 with no checkpoint between (see `ExplicitSequence::log_cnt`).
            match sequence::current(database.id, &r.name) {
                Some(last) if last > 0 => format!(
                    "SELECT {last}::bigint AS last_value, {}::bigint AS log_cnt, true AS is_called",
                    sequence::RESERVE_AHEAD - (last - 1) % (sequence::RESERVE_AHEAD + 1)
                ),
                Some(_) => "SELECT 1::bigint AS last_value, 0::bigint AS log_cnt, false AS is_called".to_string(),
                None => {
                    let (table, column) =
                        (crate::core::rql::views::quote_ident(table), crate::core::rql::views::quote_ident(column));
                    format!(
                        "SELECT coalesce(max({column}), 1)::bigint AS last_value, 0::bigint AS log_cnt, \
                         max({column}) IS NOT NULL AS is_called FROM {table}"
                    )
                }
            }
        };
        let derived = if r.aliased {
            format!("({row})")
        } else {
            format!("({row}) AS {}", crate::core::rql::views::quote_ident(&r.name))
        };
        replaced.push((r.span, derived));
    }
    if replaced.is_empty() {
        return Ok(None);
    }
    let mut text = sql.to_string();
    replaced.sort_by_key(|(span, _)| std::cmp::Reverse(span.start));
    for (span, derived) in replaced {
        text.replace_range(span, &derived);
    }
    Ok(Some(text))
}

/// The sequences a statement's `nextval`/`currval`/`setval` calls name, read
/// off its text (`nextval('s')`, `nextval('public.s'::regclass)`).
pub fn named_in(sql: &str) -> Vec<String> {
    use crate::core::sql_scan::{skip_comment, skip_quoted};
    let lower = sql.to_ascii_lowercase();
    let bytes = lower.as_bytes();
    // Where calls can be: a call written inside a string, a quoted name or a
    // comment (`'nextval(''t_id_seq'')'`, as a column default is reported)
    // names no sequence.
    let mut code = vec![true; bytes.len()];
    let mut i = 0;
    while i < bytes.len() {
        match skip_quoted(bytes, i).or_else(|| skip_comment(bytes, i)) {
            Some(end) => {
                let end = end.clamp(i + 1, bytes.len());
                code[i..end].fill(false);
                i = end;
            }
            None => i += 1,
        }
    }
    let mut out = Vec::new();
    for func in ["nextval", "currval", "setval"] {
        let mut from = 0;
        while let Some(at) = lower[from..].find(func).map(|a| a + from) {
            from = at + func.len();
            let before_ok = at == 0 || !(lower.as_bytes()[at - 1].is_ascii_alphanumeric() || lower.as_bytes()[at - 1] == b'_');
            let rest = lower[from..].trim_start();
            if !before_ok || !code[at] || !rest.starts_with('(') {
                continue;
            }
            let open = sql.len() - rest.len();
            let inner = &sql[open + 1..];
            let arg_end = inner.find([',', ')']).unwrap_or(inner.len());
            let arg = inner[..arg_end].trim();
            let arg = arg.split("::").next().unwrap_or(arg).trim();
            if arg.starts_with('\'') && arg.ends_with('\'') && arg.len() >= 2 {
                let name = sequence::sequence_name_of(&arg[1..arg.len() - 1].replace("''", "'"));
                if !out.contains(&name) {
                    out.push(name);
                }
            }
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create(sql: &str) -> SequenceStatement {
        parse_sequence_statement(sql).expect("a sequence statement").expect("parses")
    }

    #[test]
    fn statements_and_defaults_are_postgresqls() {
        let SequenceStatement::Create { name, options, .. } =
            create("CREATE SEQUENCE IF NOT EXISTS public.s AS integer INCREMENT BY 5 START WITH 10 NO CYCLE OWNED BY t.id")
        else {
            panic!()
        };
        assert_eq!(name, "s");
        let def = definition_of(&name, &options).unwrap();
        assert_eq!((def.increment, def.min, def.max, def.start), (5, 1, i32::MAX as i64, 10));
        assert_eq!(def.owned_by, "t.id");
        let SequenceStatement::Create { options, .. } = create("CREATE SEQUENCE d INCREMENT -1") else { panic!() };
        let def = definition_of("d", &options).unwrap();
        assert_eq!((def.min, def.max, def.start), (i64::MIN, -1, -1));
        let SequenceStatement::Create { options, .. } = create("CREATE SEQUENCE z INCREMENT 0") else { panic!() };
        assert_eq!(definition_of("z", &options).unwrap_err().0, "22023");
        let SequenceStatement::Create { options, .. } = create("CREATE SEQUENCE z MINVALUE 5 START 1") else { panic!() };
        assert_eq!(definition_of("z", &options).unwrap_err().0, "22023");
        assert_eq!(
            create("DROP SEQUENCE IF EXISTS a, b CASCADE"),
            SequenceStatement::Drop { names: vec!["a".into(), "b".into()], if_exists: true, cascade: true }
        );
        let SequenceStatement::Alter { options, .. } = create("ALTER SEQUENCE s RESTART WITH 7") else { panic!() };
        assert_eq!(options.restart, Some(Some(7)));
        let SequenceStatement::Alter { options, .. } = create("ALTER SEQUENCE s RESTART") else { panic!() };
        assert_eq!(options.restart, Some(None));
        assert!(parse_sequence_statement("CREATE TABLE s (a INT)").is_none());
    }

    #[test]
    fn calls_name_their_sequences() {
        assert_eq!(
            named_in("SELECT nextval('public.s'::regclass), currval('\"S\"'), setval('t_id_seq', 5), xnextval('q')"),
            vec!["s", "S", "t_id_seq"]
        );
        // A call inside a string, a quoted name or a comment is text.
        assert!(named_in("SELECT 'nextval(''t_id_seq'')' AS d, \"currval('q')\" -- setval('r', 1)\n").is_empty());
        assert_eq!(named_in("SELECT 'x', nextval('s') /* currval('q') */"), vec!["s"]);
    }
}
