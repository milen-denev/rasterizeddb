//! The twin-table model test (`docs/partitioning.md` §16 I6): a partitioned
//! table answers like a plain twin.
//!
//! One database holds `twin_part` (partitioned) and `twin_plain` (the same
//! columns and constraints, no partitioning clause), and one seeded random
//! statement stream drives both, each through its own pgwire session. Every
//! statement runs on `twin_part` first. Then:
//!
//! - a kind in [`REFUSED`] must be refused by the partitioned side: an error
//!   with the entry's SQLSTATE and "not supported" in its text. The twin does
//!   not run it, so the two stay equal. A refused kind that succeeds fails the
//!   test, whatever it answered, unless its entry names a finding under which
//!   it may succeed with the twin's answer (`or_like_twin`);
//! - any other kind runs on the twin too, and the answers (rows, column names,
//!   command tags) must agree, or both must fail with the same SQLSTATE. A
//!   partitioned side that fails where the twin succeeds fails the test, and
//!   so does the reverse;
//! - a statement that fails on the partitioned side for a reason other than
//!   the refusal (a duplicate key, say) runs on the twin, which must fail the
//!   same way, whether or not the kind is refused.
//!
//! Inside a transaction block any failure ends the block: both sides roll
//! back, so a refused write never reaches the twin.
//!
//! [`REFUSED`] is the one place that says what phase 2a does not do yet.
//! Slices S9 to S12 each remove their kinds from it; the twin is their
//! done-check. [`KNOWN_DIVERGENCES`] lists allowed kinds whose answers differ
//! today because of a bug found by this test, each with its finding; an entry
//! that stops diverging fails the test, so it is removed with the fix.
//!
//! Midway through each run the server restarts (the database is closed and
//! reopened from its directory). At the end the partitioned table must be the
//! same one, its shell still in `Database.tables`, and no file named after it
//! in the directory (I8).
//!
//! Bounded and deterministic: `PART_TWIN_SEED` picks one seed (default: two
//! fixed ones), `PART_TWIN_OPS` the statements per seed (default 300). The
//! seed is in every failure message.

mod partition_wire;

use std::collections::{BTreeMap, BTreeSet};
use std::time::{Duration, Instant};

use partition_wire::*;

// ---------------------------------------------------------------------------
// The statement kinds, and what the partitioned side does not support yet.
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
enum Kind {
    // Writes.
    InsertOne,
    InsertMulti,
    InsertNulls,
    InsertQuotedKey,
    InsertSelect,
    InsertOnConflictNothing,
    InsertOnConflictUpdate,
    InsertReturning,
    UpdateByKey,
    UpdateByPartitionKey,
    UpdateByPrefix,
    UpdateByNonKey,
    UpdateRelocating,
    UpdateKeySameValue,
    UpdateKeyChange,
    UpdateReturning,
    UpdateFrom,
    DeleteByKey,
    DeleteByPrefix,
    DeleteByNonKey,
    DeleteIn,
    DeleteReturning,
    DeleteUsing,
    // Writes elsewhere that read the partitioned table.
    InsertIntoSinkFromT,
    UpdateDimViaSubquery,
    // Maintenance.
    Vacuum,
    VacuumFull,
    Analyze,
    // Reads.
    SelectAll,
    SelectByKey,
    SelectByPartitionKey,
    SelectByPrefix,
    SelectIn,
    SelectInComposite,
    SelectOr,
    SelectOrUnconstrained,
    SelectRange,
    SelectNonKeyRange,
    SelectRowValue,
    SelectSpellings,
    SelectNonKey,
    SelectLimitZero,
    CountAll,
    CountByPrefix,
    Aggregates,
    GroupBy,
    GroupByOrdered,
    OrderByLimit,
    Distinct,
    CountDistinct,
    Window,
    Join,
    LeftJoin,
    SelfJoin,
    InSubquery,
    Union,
    Cte,
    ForUpdate,
    SkipLocked,
    // Empty routes (I6): a never-written tenant, and a contradiction.
    EmptyRouteCount,
    EmptyRouteMax,
    EmptyRouteAggregates,
    EmptyRouteContradiction,
    EmptyRouteContradictionRows,
    EmptyRouteUpdateReturning,
    EmptyRouteDeleteReturning,
    // Prepared statements, several runs each, alternating tenants.
    PreparedSelect,
    PreparedCount,
    PreparedInsert,
    PreparedUpdate,
    PreparedDelete,
    // Catalogs and introspection.
    CatalogColumns,
    CatalogTables,
    CatalogClass,
    CatalogStat,
    CatalogIndexes,
    Describe,
    // Reads of the other per-side tables, so a write that reached them is seen.
    ReadSinkAndDim,
    // Structure.
    TxCommit,
    TxRollback,
    Restart,
}

use Kind::*;

const ALL_KINDS: &[Kind] = &[
    InsertOne, InsertMulti, InsertNulls, InsertQuotedKey, InsertSelect, InsertOnConflictNothing,
    InsertOnConflictUpdate, InsertReturning, UpdateByKey, UpdateByPartitionKey, UpdateByPrefix, UpdateByNonKey,
    UpdateRelocating, UpdateKeySameValue, UpdateKeyChange, UpdateReturning, UpdateFrom, DeleteByKey,
    DeleteByPrefix, DeleteByNonKey, DeleteIn, DeleteReturning, DeleteUsing, InsertIntoSinkFromT,
    UpdateDimViaSubquery, Vacuum, VacuumFull, Analyze, SelectAll, SelectByKey, SelectByPartitionKey,
    SelectByPrefix, SelectIn, SelectInComposite, SelectOr, SelectOrUnconstrained, SelectRange,
    SelectNonKeyRange, SelectRowValue, SelectSpellings, SelectNonKey, SelectLimitZero, CountAll, CountByPrefix,
    Aggregates, GroupBy, GroupByOrdered, OrderByLimit, Distinct, CountDistinct, Window, Join, LeftJoin,
    SelfJoin, InSubquery, Union, Cte, ForUpdate, SkipLocked, EmptyRouteCount, EmptyRouteMax,
    EmptyRouteAggregates, EmptyRouteContradiction, EmptyRouteContradictionRows, EmptyRouteUpdateReturning,
    EmptyRouteDeleteReturning, PreparedSelect, PreparedCount, PreparedInsert, PreparedUpdate, PreparedDelete,
    CatalogColumns, CatalogTables, CatalogClass, CatalogStat, CatalogIndexes, Describe, ReadSinkAndDim,
    TxCommit, TxRollback, Restart,
];

/// A kind the partitioned table refuses today: the SQLSTATE the refusal
/// must carry (with "not supported" in its text) and what lifts it.
struct Refusal {
    kind: Kind,
    code: &'static str,
    lifted_by: &'static str,
    /// A finding under which this kind may also succeed, provided its answer
    /// is the twin's (the twin then runs it too). `None`: success fails.
    or_like_twin: Option<&'static str>,
}

const fn refused(kind: Kind, lifted_by: &'static str) -> Refusal {
    Refusal { kind, code: "0A000", lifted_by, or_like_twin: None }
}

/// What phase 2a's partitioned table does not do yet. One entry per kind.
/// A slice that builds a kind removes its entry; the twin then compares that
/// kind's answers instead of demanding a refusal.
const REFUSED: &[Refusal] = &[
    // S9 (routed INSERT) is built: InsertOne, InsertMulti, InsertNulls,
    // InsertQuotedKey, InsertSelect and PreparedInsert are compared with the
    // twin, and so is what they store (`Twin::compare_storage`).
    // 2b: ON CONFLICT and RETURNING on partitioned tables (§15).
    refused(InsertOnConflictNothing, "phase 2b"),
    refused(InsertOnConflictUpdate, "phase 2b"),
    refused(InsertReturning, "phase 2b"),
    refused(UpdateReturning, "phase 2b"),
    refused(DeleteReturning, "phase 2b"),
    refused(EmptyRouteUpdateReturning, "phase 2b"),
    refused(EmptyRouteDeleteReturning, "phase 2b"),
    // S11: routed UPDATE and DELETE, VACUUM fan-out.
    refused(UpdateByKey, "S11"),
    refused(UpdateByPartitionKey, "S11"),
    refused(UpdateByPrefix, "S11"),
    refused(UpdateByNonKey, "S11"),
    refused(UpdateRelocating, "S11"),
    refused(UpdateKeySameValue, "S11"),
    refused(DeleteByKey, "S11"),
    refused(DeleteByPrefix, "S11"),
    refused(DeleteByNonKey, "S11"),
    refused(DeleteIn, "S11"),
    refused(PreparedUpdate, "S11"),
    refused(PreparedDelete, "S11"),
    refused(Vacuum, "S11"),
    refused(VacuumFull, "S11"),
    // A key change moves a row between partitions (I4): phase 6.
    refused(UpdateKeyChange, "phase 6"),
    // Phase 3: UPDATE FROM, DELETE USING, FOR UPDATE, SKIP LOCKED.
    refused(UpdateFrom, "phase 3"),
    refused(DeleteUsing, "phase 3"),
    refused(ForUpdate, "phase 3"),
    refused(SkipLocked, "phase 3"),
    // S10: single-snapshot fan-out SELECT and COUNT.
    refused(SelectAll, "S10"),
    refused(SelectByKey, "S10"),
    refused(SelectByPartitionKey, "S10"),
    refused(SelectByPrefix, "S10"),
    refused(SelectIn, "S10"),
    refused(SelectInComposite, "S10"),
    refused(SelectOr, "S10"),
    refused(SelectOrUnconstrained, "S10"),
    refused(SelectRange, "S10"),
    refused(SelectNonKeyRange, "S10"),
    refused(SelectRowValue, "S10"),
    refused(SelectSpellings, "S10"),
    refused(SelectNonKey, "S10"),
    refused(SelectLimitZero, "S10"),
    refused(CountAll, "S10"),
    refused(CountByPrefix, "S10"),
    refused(Aggregates, "S10"),
    refused(GroupBy, "S10"),
    refused(GroupByOrdered, "S10 (S12 for a multi-partition ORDER BY)"),
    refused(Distinct, "S10"),
    refused(CountDistinct, "S10"),
    refused(Window, "S10"),
    refused(Cte, "S10"),
    refused(Union, "S10"),
    refused(PreparedSelect, "S10"),
    refused(PreparedCount, "S10"),
    refused(InsertIntoSinkFromT, "S10"),
    refused(EmptyRouteCount, "S10"),
    refused(EmptyRouteMax, "S10"),
    refused(EmptyRouteAggregates, "S10"),
    refused(EmptyRouteContradiction, "S10"),
    refused(EmptyRouteContradictionRows, "S10"),
    // S12: ORDER BY across partitions.
    refused(OrderByLimit, "S12"),
    // 2b: joins.
    refused(Join, "phase 2b"),
    refused(LeftJoin, "phase 2b"),
    refused(SelfJoin, "phase 2b"),
    // S10. Finding F3: a refusal inside an `IN (SELECT …)` subquery is
    // reported as 42601 (syntax error), not 0A000.
    Refusal { kind: InSubquery, code: "42601", lifted_by: "S10", or_like_twin: None },
    Refusal { kind: UpdateDimViaSubquery, code: "42601", lifted_by: "S10", or_like_twin: None },
];

/// Allowed kinds whose answers differ today, each with the finding that
/// explains it. A difference in one of these is recorded, not failed; a run
/// in which one of them never differs fails, so the entry goes with the fix.
const KNOWN_DIVERGENCES: &[(Kind, &str)] = &[(
    CatalogIndexes,
    "F1: until its first reopen, pg_indexes lists no implicit index for a plain table whose only \
     uniqueness is a composite PRIMARY KEY or UNIQUE, or a CREATE INDEX (index_set reads the plain \
     table's live index set); the partitioned table lists it from its schema at once. The twins \
     agree after the restart",
)];

fn refusal(kind: Kind) -> Option<&'static Refusal> {
    REFUSED.iter().find(|r| r.kind == kind)
}

// ---------------------------------------------------------------------------
// Configurations.
// ---------------------------------------------------------------------------

const PART: &str = "twin_part";
const PLAIN: &str = "twin_plain";
const COLUMNS: &str = "(tenant_id BIGINT NOT NULL, grp BIGINT NOT NULL, id BIGINT NOT NULL, n INT, s TEXT, \
                       f DOUBLE PRECISION, PRIMARY KEY (tenant_id, grp, id))";
const KEY_ORDER: &str = "tenant_id, grp, id";

/// Tenants written to. `NEVER` is never written.
const TENANTS: i64 = 12;
const NEVER: i64 = 999;
const GROUPS: i64 = 3;
const IDS: i64 = 30;

struct Config {
    key: &'static [&'static str],
    partitions: u32,
}

/// Each side's names: `{t}` the table under test, `{d}` a dimension table,
/// `{s}` a sink. The stage table is shared and read-only.
struct Side {
    t: &'static str,
    d: &'static str,
    s: &'static str,
}

const SIDE_PART: Side = Side { t: PART, d: "dim_part", s: "sink_part" };
const SIDE_PLAIN: Side = Side { t: PLAIN, d: "dim_plain", s: "sink_plain" };

fn render(template: &str, side: &Side) -> String {
    template.replace("{t}", side.t).replace("{d}", side.d).replace("{s}", side.s)
}

/// Undoes [`render`] in an answer, so the two sides compare.
fn unrender(text: &str) -> String {
    let mut out = text.to_string();
    for side in [&SIDE_PART, &SIDE_PLAIN] {
        out = out.replace(side.d, "{d}").replace(side.s, "{s}").replace(side.t, "{t}");
        out = out
            .replace(&side.d.to_ascii_uppercase(), "{D}")
            .replace(&side.s.to_ascii_uppercase(), "{S}")
            .replace(&side.t.to_ascii_uppercase(), "{T}");
    }
    out
}

// ---------------------------------------------------------------------------
// Statement generation.
// ---------------------------------------------------------------------------

enum Step {
    Simple(String),
    Prepared { sql: String, runs: Vec<Vec<Option<String>>> },
    /// BEGIN, the inner operations, then COMMIT (true) or ROLLBACK.
    Block { inner: Vec<Op>, commit: bool },
    Restart,
}

struct Op {
    kind: Kind,
    step: Step,
    /// The statement's ORDER BY fixes the row order; otherwise rows compare
    /// as multisets.
    ordered: bool,
}

struct Gen {
    rng: fastrand::Rng,
    key: &'static [&'static str],
}

const WORDS: &[&str] = &["alpha", "beta", "gamma", "delta", "", "x y", "it''s"];

impl Gen {
    fn tenant(&mut self) -> i64 {
        self.rng.i64(1..=TENANTS)
    }
    fn grp(&mut self) -> i64 {
        self.rng.i64(1..=GROUPS)
    }
    fn id(&mut self) -> i64 {
        self.rng.i64(1..=IDS)
    }
    fn n(&mut self) -> String {
        if self.rng.u8(..10) == 0 { "NULL".into() } else { self.rng.i32(-50..100).to_string() }
    }
    fn s(&mut self) -> String {
        if self.rng.u8(..10) == 0 {
            "NULL".into()
        } else {
            format!("'{}'", WORDS[self.rng.usize(..WORDS.len())])
        }
    }
    /// Quarters, so every sum is exact in any order.
    fn f(&mut self) -> String {
        if self.rng.u8(..10) == 0 { "NULL".into() } else { format!("{}", self.rng.i32(-40..40) as f64 * 0.25) }
    }
    fn row(&mut self) -> String {
        let (t, g, i) = (self.tenant(), self.grp(), self.id());
        let (n, s, f) = (self.n(), self.s(), self.f());
        format!("({t}, {g}, {i}, {n}, {s}, {f})")
    }
    /// Equality on every partition-key column: one route.
    fn partition_key_eq(&mut self) -> String {
        let t = self.tenant();
        let g = self.grp();
        let i = self.id();
        self.key
            .iter()
            .map(|c| match *c {
                "tenant_id" => format!("tenant_id = {t}"),
                "grp" => format!("grp = {g}"),
                _ => format!("id = {i}"),
            })
            .collect::<Vec<_>>()
            .join(" AND ")
    }
    fn pk_eq(&mut self) -> String {
        format!("tenant_id = {} AND grp = {} AND id = {}", self.tenant(), self.grp(), self.id())
    }
    fn tenant_list(&mut self) -> String {
        let k = self.rng.usize(1..=4);
        (0..k).map(|_| self.tenant().to_string()).collect::<Vec<_>>().join(", ")
    }

    fn simple(&mut self, kind: Kind, sql: String) -> Op {
        Op { kind, step: Step::Simple(sql), ordered: false }
    }

    fn ordered(&mut self, kind: Kind, sql: String) -> Op {
        Op { kind, step: Step::Simple(sql), ordered: true }
    }

    fn prepared(&mut self, kind: Kind, sql: &str, runs: Vec<Vec<Option<String>>>) -> Op {
        Op { kind, step: Step::Prepared { sql: sql.to_string(), runs }, ordered: false }
    }

    fn op(&mut self, kind: Kind) -> Op {
        let cols = "(tenant_id, grp, id, n, s, f)";
        match kind {
            InsertOne => {
                let r = self.row();
                self.simple(kind, format!("INSERT INTO {{t}} {cols} VALUES {r}"))
            }
            InsertMulti => {
                let k = self.rng.usize(2..=5);
                let rows: Vec<String> = (0..k).map(|_| self.row()).collect();
                self.simple(kind, format!("INSERT INTO {{t}} {cols} VALUES {}", rows.join(", ")))
            }
            InsertNulls => {
                let (t, g, i) = (self.tenant(), self.grp(), self.id());
                self.simple(kind, format!("INSERT INTO {{t}} (tenant_id, grp, id) VALUES ({t}, {g}, {i})"))
            }
            InsertQuotedKey => {
                // I2's spellings: a quoted literal and a cast for key values.
                let (t, g, i) = (self.tenant(), self.grp(), self.id());
                let n = self.n();
                self.simple(
                    kind,
                    format!("INSERT INTO {{t}} (tenant_id, grp, id, n) VALUES ('{t}', {g}::bigint, {i}, {n})"),
                )
            }
            InsertSelect => {
                let (t, g) = (self.tenant(), self.grp());
                let m = self.rng.i64(0..IDS);
                self.simple(
                    kind,
                    format!(
                        "INSERT INTO {{t}} {cols} SELECT tenant_id, grp, id, n, s, f FROM stage \
                         WHERE tenant_id = {t} AND grp = {g} AND id > {m}"
                    ),
                )
            }
            InsertOnConflictNothing => {
                let r = self.row();
                self.simple(kind, format!("INSERT INTO {{t}} {cols} VALUES {r} ON CONFLICT DO NOTHING"))
            }
            InsertOnConflictUpdate => {
                let r = self.row();
                let n = self.n();
                self.simple(
                    kind,
                    format!(
                        "INSERT INTO {{t}} {cols} VALUES {r} ON CONFLICT (tenant_id, grp, id) \
                         DO UPDATE SET n = {n}, s = 'upserted'"
                    ),
                )
            }
            InsertReturning => {
                let r = self.row();
                self.simple(kind, format!("INSERT INTO {{t}} {cols} VALUES {r} RETURNING tenant_id, grp, id, n, s"))
            }
            UpdateByKey => {
                let w = self.pk_eq();
                self.simple(kind, format!("UPDATE {{t}} SET n = n + 1 WHERE {w}"))
            }
            UpdateByPartitionKey => {
                let w = self.partition_key_eq();
                self.simple(kind, format!("UPDATE {{t}} SET n = n + 1, f = f + 0.25 WHERE {w}"))
            }
            UpdateByPrefix => {
                let t = self.tenant();
                self.simple(kind, format!("UPDATE {{t}} SET n = n - 1 WHERE tenant_id = {t}"))
            }
            UpdateByNonKey => {
                let v = self.rng.i32(-50..100);
                self.simple(kind, format!("UPDATE {{t}} SET n = n + 2 WHERE n > {v}"))
            }
            UpdateRelocating => {
                let (t, g) = (self.tenant(), self.grp());
                let long = "z".repeat(self.rng.usize(40..200));
                self.simple(kind, format!("UPDATE {{t}} SET s = '{long}' WHERE tenant_id = {t} AND grp = {g}"))
            }
            UpdateKeySameValue => {
                let (t, g) = (self.tenant(), self.grp());
                self.simple(
                    kind,
                    format!("UPDATE {{t}} SET tenant_id = {t}, grp = {g}, n = n + 1 WHERE tenant_id = {t} AND grp = {g}"),
                )
            }
            UpdateKeyChange => {
                let w = self.pk_eq();
                let t = self.tenant() + 100;
                self.simple(kind, format!("UPDATE {{t}} SET tenant_id = {t} WHERE {w}"))
            }
            UpdateReturning => {
                let t = self.tenant();
                self.simple(
                    kind,
                    format!("UPDATE {{t}} SET n = n * 2 WHERE tenant_id = {t} RETURNING tenant_id, grp, id, n"),
                )
            }
            UpdateFrom => {
                let t = self.tenant();
                self.simple(
                    kind,
                    format!(
                        "UPDATE {{t}} SET n = 7 FROM {{d}} WHERE {{t}}.tenant_id = {{d}}.tenant_id AND {{d}}.tenant_id = {t}"
                    ),
                )
            }
            DeleteByKey => {
                let w = self.pk_eq();
                self.simple(kind, format!("DELETE FROM {{t}} WHERE {w}"))
            }
            DeleteByPrefix => {
                let t = self.tenant();
                let i = self.id();
                self.simple(kind, format!("DELETE FROM {{t}} WHERE tenant_id = {t} AND id > {i}"))
            }
            DeleteByNonKey => {
                let v = self.rng.i32(-60..-20);
                self.simple(kind, format!("DELETE FROM {{t}} WHERE n < {v}"))
            }
            DeleteIn => {
                let l = self.tenant_list();
                let i = self.id();
                self.simple(kind, format!("DELETE FROM {{t}} WHERE tenant_id IN ({l}) AND id = {i}"))
            }
            DeleteReturning => {
                let (t, g) = (self.tenant(), self.grp());
                self.simple(
                    kind,
                    format!("DELETE FROM {{t}} WHERE tenant_id = {t} AND grp = {g} RETURNING tenant_id, grp, id, n"),
                )
            }
            DeleteUsing => {
                let t = self.tenant();
                self.simple(
                    kind,
                    format!(
                        "DELETE FROM {{t}} USING {{d}} WHERE {{t}}.tenant_id = {{d}}.tenant_id AND {{d}}.tenant_id = {t} \
                         AND {{t}}.n > 90"
                    ),
                )
            }
            InsertIntoSinkFromT => {
                let g = self.grp();
                self.simple(
                    kind,
                    format!("INSERT INTO {{s}} (tenant_id, id, n) SELECT tenant_id, id, n FROM {{t}} WHERE grp = {g}"),
                )
            }
            UpdateDimViaSubquery => {
                let v = self.rng.i32(-50..100);
                let w = WORDS[self.rng.usize(..4)];
                self.simple(
                    kind,
                    format!(
                        "UPDATE {{d}} SET label = '{w}' WHERE tenant_id IN (SELECT tenant_id FROM {{t}} WHERE n > {v})"
                    ),
                )
            }
            Vacuum => self.simple(kind, "VACUUM {t}".into()),
            VacuumFull => self.simple(kind, "VACUUM FULL {t}".into()),
            Analyze => self.simple(kind, "ANALYZE {t}".into()),
            SelectAll => self.ordered(kind, format!("SELECT * FROM {{t}} ORDER BY {KEY_ORDER}")),
            SelectByKey => {
                let w = self.pk_eq();
                self.simple(kind, format!("SELECT * FROM {{t}} WHERE {w}"))
            }
            SelectByPartitionKey => {
                let w = self.partition_key_eq();
                self.simple(kind, format!("SELECT tenant_id, grp, id, n, s FROM {{t}} WHERE {w}"))
            }
            SelectByPrefix => {
                let t = self.tenant();
                self.simple(kind, format!("SELECT * FROM {{t}} WHERE tenant_id = {t}"))
            }
            SelectIn => {
                let l = self.tenant_list();
                self.simple(kind, format!("SELECT tenant_id, grp, id, f FROM {{t}} WHERE tenant_id IN ({l})"))
            }
            SelectInComposite => {
                let t = self.tenant();
                self.simple(kind, format!("SELECT * FROM {{t}} WHERE tenant_id = {t} AND grp IN (1, 3)"))
            }
            SelectOr => {
                let (a, b) = (self.tenant(), self.tenant());
                let g = self.grp();
                self.simple(
                    kind,
                    format!("SELECT * FROM {{t}} WHERE (tenant_id = {a} AND grp = {g}) OR tenant_id = {b}"),
                )
            }
            SelectOrUnconstrained => {
                let a = self.tenant();
                let v = self.rng.i32(50..100);
                self.simple(kind, format!("SELECT * FROM {{t}} WHERE tenant_id = {a} OR n > {v}"))
            }
            SelectRange => {
                let a = self.tenant();
                let b = a + self.rng.i64(0..4);
                self.simple(kind, format!("SELECT * FROM {{t}} WHERE tenant_id BETWEEN {a} AND {b}"))
            }
            SelectNonKeyRange => {
                let t = self.tenant();
                let i = self.id();
                self.simple(kind, format!("SELECT * FROM {{t}} WHERE tenant_id >= {t} AND id < {i}"))
            }
            SelectRowValue => {
                let (t, g) = (self.tenant(), self.grp());
                self.simple(kind, format!("SELECT * FROM {{t}} WHERE (tenant_id, grp) > ({t}, {g})"))
            }
            SelectSpellings => {
                let t = self.tenant();
                let w = match self.rng.u8(..3) {
                    0 => format!("tenant_id = '{t}'"),
                    1 => format!("tenant_id = {t}::bigint"),
                    _ => format!("tenant_id = CAST({t} AS BIGINT)"),
                };
                self.simple(kind, format!("SELECT * FROM {{t}} WHERE {w}"))
            }
            SelectNonKey => {
                let s = WORDS[self.rng.usize(..4)];
                self.simple(kind, format!("SELECT * FROM {{t}} WHERE s = '{s}' OR n IS NULL"))
            }
            SelectLimitZero => self.simple(kind, "SELECT * FROM {t} LIMIT 0".into()),
            CountAll => self.simple(kind, "SELECT COUNT(*) FROM {t}".into()),
            CountByPrefix => {
                let t = self.tenant();
                self.simple(kind, format!("SELECT COUNT(*), COUNT(n) FROM {{t}} WHERE tenant_id = {t}"))
            }
            Aggregates => {
                let w = if self.rng.bool() { format!(" WHERE tenant_id = {}", self.tenant()) } else { String::new() };
                self.simple(
                    kind,
                    format!("SELECT COUNT(*), SUM(n), MIN(n), MAX(n), AVG(f), SUM(f), MIN(s), MAX(s) FROM {{t}}{w}"),
                )
            }
            GroupBy => self.simple(
                kind,
                "SELECT tenant_id, COUNT(*), SUM(n), MAX(f) FROM {t} GROUP BY tenant_id HAVING COUNT(*) > 1".into(),
            ),
            GroupByOrdered => self.ordered(
                kind,
                "SELECT tenant_id, grp, COUNT(*), MIN(id) FROM {t} GROUP BY tenant_id, grp ORDER BY tenant_id, grp".into(),
            ),
            OrderByLimit => {
                let k = self.rng.usize(1..=6);
                let o = self.rng.usize(0..=4);
                let w = if self.rng.bool() { format!(" WHERE tenant_id = {}", self.tenant()) } else { String::new() };
                // The key breaks every tie, so the order is fixed; a NULL n
                // sorts by the engine's rule on both sides alike.
                self.ordered(
                    kind,
                    format!("SELECT tenant_id, grp, id, n FROM {{t}}{w} ORDER BY n DESC, {KEY_ORDER} LIMIT {k} OFFSET {o}"),
                )
            }
            Distinct => self.simple(kind, "SELECT DISTINCT grp, s FROM {t}".into()),
            CountDistinct => self.simple(kind, "SELECT COUNT(DISTINCT tenant_id), COUNT(DISTINCT s) FROM {t}".into()),
            Window => self.simple(
                kind,
                "SELECT tenant_id, grp, id, ROW_NUMBER() OVER (PARTITION BY tenant_id ORDER BY grp, id) FROM {t}".into(),
            ),
            Join => {
                let g = self.grp();
                self.simple(
                    kind,
                    format!(
                        "SELECT {{t}}.tenant_id, {{t}}.id, {{d}}.label FROM {{t}} JOIN {{d}} \
                         ON {{t}}.tenant_id = {{d}}.tenant_id WHERE {{t}}.grp = {g}"
                    ),
                )
            }
            LeftJoin => self.simple(
                kind,
                "SELECT {d}.tenant_id, {d}.label, {t}.id FROM {d} LEFT JOIN {t} ON {t}.tenant_id = {d}.tenant_id".into(),
            ),
            SelfJoin => self.simple(
                kind,
                "SELECT COUNT(*) FROM {t} a JOIN {t} b ON a.tenant_id = b.tenant_id AND a.grp = b.grp".into(),
            ),
            InSubquery => {
                let v = self.rng.i32(-50..100);
                self.simple(
                    kind,
                    format!("SELECT tenant_id, label FROM {{d}} WHERE tenant_id IN (SELECT tenant_id FROM {{t}} WHERE n > {v})"),
                )
            }
            Union => self.simple(kind, "SELECT tenant_id FROM {t} UNION SELECT tenant_id FROM {d}".into()),
            Cte => {
                let g = self.grp();
                self.simple(
                    kind,
                    format!("WITH x AS (SELECT tenant_id, n FROM {{t}} WHERE grp = {g}) SELECT COUNT(*), SUM(n) FROM x"),
                )
            }
            ForUpdate => {
                let t = self.tenant();
                self.simple(kind, format!("SELECT * FROM {{t}} WHERE tenant_id = {t} FOR UPDATE"))
            }
            SkipLocked => {
                let t = self.tenant();
                self.simple(kind, format!("SELECT * FROM {{t}} WHERE tenant_id = {t} FOR UPDATE SKIP LOCKED"))
            }
            EmptyRouteCount => self.simple(kind, format!("SELECT COUNT(*) FROM {{t}} WHERE tenant_id = {NEVER}")),
            EmptyRouteMax => self.simple(kind, format!("SELECT MAX(n) FROM {{t}} WHERE tenant_id = {NEVER}")),
            EmptyRouteAggregates => self.simple(
                kind,
                format!("SELECT SUM(n), MIN(s), AVG(f), COUNT(n), COUNT(*) FROM {{t}} WHERE tenant_id = {NEVER}"),
            ),
            EmptyRouteContradiction => {
                let extra = if self.key.contains(&"grp") { " AND grp = 1 AND grp = 2" } else { "" };
                self.simple(
                    kind,
                    format!("SELECT COUNT(*), MAX(n) FROM {{t}} WHERE tenant_id = 1 AND tenant_id = 2{extra}"),
                )
            }
            EmptyRouteContradictionRows => {
                self.simple(kind, "SELECT * FROM {t} WHERE tenant_id = 1 AND tenant_id = 2".into())
            }
            EmptyRouteUpdateReturning => self.simple(
                kind,
                format!("UPDATE {{t}} SET n = n + 1 WHERE tenant_id = {NEVER} RETURNING tenant_id, id"),
            ),
            EmptyRouteDeleteReturning => {
                self.simple(kind, "DELETE FROM {t} WHERE tenant_id = 1 AND tenant_id = 2 RETURNING id".into())
            }
            PreparedSelect => {
                let runs = self.prepared_runs(|g| vec![Some(g.tenant().to_string()), Some(g.grp().to_string())]);
                self.prepared(kind, "SELECT * FROM {t} WHERE tenant_id = $1 AND grp = $2", runs)
            }
            PreparedCount => {
                let runs = self.prepared_runs(|g| vec![Some(g.tenant().to_string())]);
                self.prepared(kind, "SELECT COUNT(*) FROM {t} WHERE tenant_id = $1", runs)
            }
            PreparedInsert => {
                let runs = self.prepared_runs(|g| {
                    let n = g.rng.i32(-50..100).to_string();
                    let s = WORDS[g.rng.usize(..4)].to_string();
                    let f = format!("{}", g.rng.i32(-40..40) as f64 * 0.25);
                    vec![
                        Some(g.tenant().to_string()),
                        Some(g.grp().to_string()),
                        Some(g.id().to_string()),
                        Some(n),
                        if g.rng.u8(..5) == 0 { None } else { Some(s) },
                        Some(f),
                    ]
                });
                self.prepared(kind, "INSERT INTO {t} (tenant_id, grp, id, n, s, f) VALUES ($1, $2, $3, $4, $5, $6)", runs)
            }
            PreparedUpdate => {
                let runs = self.prepared_runs(|g| {
                    vec![Some(g.rng.i32(-50..100).to_string()), Some(g.tenant().to_string()), Some(g.grp().to_string())]
                });
                self.prepared(kind, "UPDATE {t} SET n = $1 WHERE tenant_id = $2 AND grp = $3", runs)
            }
            PreparedDelete => {
                let runs = self.prepared_runs(|g| vec![Some(g.tenant().to_string()), Some(g.id().to_string())]);
                self.prepared(kind, "DELETE FROM {t} WHERE tenant_id = $1 AND id = $2", runs)
            }
            CatalogColumns => self.ordered(
                kind,
                "SELECT column_name, data_type, is_nullable, ordinal_position FROM information_schema.columns \
                 WHERE table_name = '{t}' ORDER BY ordinal_position"
                    .into(),
            ),
            CatalogTables => {
                self.simple(kind, "SELECT * FROM information_schema.tables WHERE table_name = '{t}'".into())
            }
            CatalogClass => {
                self.simple(kind, "SELECT relname, relkind, reltuples FROM pg_class WHERE relname = '{t}'".into())
            }
            CatalogStat => self.simple(
                kind,
                "SELECT schemaname, relname, n_live_tup, n_dead_tup FROM pg_stat_user_tables WHERE relname = '{t}'".into(),
            ),
            CatalogIndexes => self.simple(kind, "SELECT * FROM pg_indexes WHERE tablename = '{t}'".into()),
            Describe => self.ordered(kind, "DESCRIBE {t}".into()),
            ReadSinkAndDim => {
                if self.rng.bool() {
                    self.simple(kind, "SELECT tenant_id, id, n FROM {s}".into())
                } else {
                    self.simple(kind, "SELECT tenant_id, label FROM {d}".into())
                }
            }
            TxCommit | TxRollback => {
                let k = self.rng.usize(1..=4);
                let inner = (0..k)
                    .map(|_| {
                        let kind = BLOCK_KINDS[self.rng.usize(..BLOCK_KINDS.len())];
                        self.op(kind)
                    })
                    .collect();
                Op { kind, step: Step::Block { inner, commit: kind == TxCommit }, ordered: false }
            }
            Restart => Op { kind, step: Step::Restart, ordered: false },
        }
    }

    /// Two to five runs: past the third, where the prepared-SELECT cache
    /// starts answering from its learned parse (I11).
    fn prepared_runs(&mut self, mut one: impl FnMut(&mut Gen) -> Vec<Option<String>>) -> Vec<Vec<Option<String>>> {
        let k = self.rng.usize(2..=5);
        (0..k).map(|_| one(self)).collect()
    }
}

/// Kinds a transaction block draws from: no VACUUM (not in a block), no
/// nested block, no restart.
const BLOCK_KINDS: &[Kind] = &[
    InsertOne, InsertMulti, InsertSelect, InsertOnConflictUpdate, UpdateByKey, UpdateByPrefix, UpdateRelocating,
    DeleteByKey, DeleteByNonKey, SelectByPrefix, CountAll, Aggregates, ForUpdate, SkipLocked, PreparedInsert,
    PreparedSelect, InsertIntoSinkFromT, ReadSinkAndDim,
];

/// How often each kind is drawn after the first pass (which draws each once).
fn weight(kind: Kind) -> u32 {
    match kind {
        InsertOne | InsertMulti | PreparedInsert | InsertSelect => 6,
        TxCommit | TxRollback => 4,
        Restart => 0,
        CatalogColumns | CatalogTables | CatalogClass | CatalogStat | CatalogIndexes | Describe => 1,
        Vacuum | VacuumFull | Analyze => 1,
        _ => 2,
    }
}

// ---------------------------------------------------------------------------
// The run.
// ---------------------------------------------------------------------------

#[derive(Default)]
struct Tally {
    refused: usize,
    agreed: usize,
    both_failed: usize,
    diverged_known: usize,
}

struct Twin {
    server: Option<Server>,
    cp: Option<Client>,
    cq: Option<Client>,
    seed: u64,
    label: String,
    uid: u64,
    log: Vec<String>,
    tally: BTreeMap<Kind, Tally>,
    /// How many times [`Twin::compare_storage`] ran.
    storage_checks: usize,
}

/// Kinds that can change what `{t}` stores: the storage comparison runs
/// after each.
fn writes_t(kind: Kind) -> bool {
    matches!(
        kind,
        InsertOne
            | InsertMulti
            | InsertNulls
            | InsertQuotedKey
            | InsertSelect
            | InsertOnConflictNothing
            | InsertOnConflictUpdate
            | InsertReturning
            | UpdateByKey
            | UpdateByPartitionKey
            | UpdateByPrefix
            | UpdateByNonKey
            | UpdateRelocating
            | UpdateKeySameValue
            | UpdateKeyChange
            | UpdateReturning
            | UpdateFrom
            | DeleteByKey
            | DeleteByPrefix
            | DeleteByNonKey
            | DeleteIn
            | DeleteReturning
            | DeleteUsing
            | EmptyRouteUpdateReturning
            | EmptyRouteDeleteReturning
            | PreparedInsert
            | PreparedUpdate
            | PreparedDelete
            | Vacuum
            | VacuumFull
            | TxCommit
            | TxRollback
            | Restart
    )
}

/// What an operation came to.
enum Verdict {
    /// The partitioned side refused; the twin was not run.
    Refused,
    /// Both ran; equal answers, or the same failure.
    Agreed { failed: bool },
}

impl Twin {
    fn cp(&mut self) -> &mut Client {
        self.cp.as_mut().unwrap()
    }
    fn cq(&mut self) -> &mut Client {
        self.cq.as_mut().unwrap()
    }

    fn fail(&self, at: usize, op_kind: Kind, why: String) -> ! {
        let tail: Vec<&String> = self.log.iter().rev().take(20).collect::<Vec<_>>().into_iter().rev().collect();
        panic!(
            "twin divergence: {} seed {:#x} (PART_TWIN_SEED={}), op {at}, kind {op_kind:?}\n{why}\n\
             last statements:\n  {}",
            self.label,
            self.seed,
            self.seed,
            tail.iter().map(|s| s.as_str()).collect::<Vec<_>>().join("\n  ")
        );
    }

    fn compare(&mut self, at: usize, op: &Op, sql: &str, p: &Reply, q: &Reply) -> Result<(), String> {
        let norm = |r: &Reply, ordered: bool| {
            let cols: Vec<String> = r.columns.iter().map(|c| unrender(c)).collect();
            let mut rows: Vec<Vec<Option<String>>> =
                r.rows.iter().map(|row| row.iter().map(|v| v.as_deref().map(unrender)).collect()).collect();
            if !ordered {
                rows.sort();
            }
            let tags: Vec<String> = r.tags.iter().map(|t| unrender(t)).collect();
            (cols, rows, tags)
        };
        let (a, b) = (norm(p, op.ordered), norm(q, op.ordered));
        if a == b {
            return Ok(());
        }
        let _ = at;
        Err(format!(
            "answers differ for `{sql}`\n  partitioned: cols {:?} tags {:?} rows {:?}\n  twin:        cols {:?} tags {:?} rows {:?}",
            a.0, a.2, a.1, b.0, b.2, b.1
        ))
    }

    /// One statement on both sides, by the rule in the module comment.
    fn judge(
        &mut self,
        at: usize,
        op: &Op,
        sql_p: &str,
        p: Outcome,
        answers: bool,
        run_q: &mut dyn FnMut(&mut Twin) -> Outcome,
    ) -> Verdict {
        let kind = op.kind;
        let refused = refusal(kind);
        // A success is compared with the twin's, not failed, for a Parse (which
        // answers nothing yet; Execute refuses) and under a finding's `or_like_twin`.
        let refuses_success = answers && refused.is_some_and(|r| r.or_like_twin.is_none());
        match (refused, p) {
            (Some(r), Err(e)) if e.code == r.code && e.message.to_ascii_lowercase().contains("not supported") => {
                Verdict::Refused
            }
            (Some(r), Ok(reply)) if refuses_success => self.fail(
                at,
                kind,
                format!(
                    "{kind:?} is in REFUSED (until {}) and the partitioned table answered it: `{sql_p}` -> \
                     tags {:?}, {} rows {:?}",
                    r.lifted_by,
                    reply.tags,
                    reply.rows.len(),
                    reply.rows.iter().take(5).collect::<Vec<_>>()
                ),
            ),
            (_, Err(e)) => {
                // Not the expected refusal: the twin must fail the same way.
                let q = run_q(self);
                match q {
                    Err(eq) if eq.code == e.code => Verdict::Agreed { failed: true },
                    Err(eq) => self.fail(
                        at,
                        kind,
                        format!("different failures for `{sql_p}`:\n  partitioned: {e}\n  twin:        {eq}"),
                    ),
                    Ok(reply) => self.fail(
                        at,
                        kind,
                        format!(
                            "the partitioned table failed where the twin succeeded{}: `{sql_p}`\n  partitioned: {e}\n  \
                             twin: tags {:?}, {} rows",
                            if refused.is_some() { " (and not with the kind's refusal)" } else { "" },
                            reply.tags,
                            reply.rows.len()
                        ),
                    ),
                }
            }
            (_, Ok(reply_p)) => match run_q(self) {
                Err(eq) => self.fail(
                    at,
                    kind,
                    format!("the partitioned table succeeded where the twin failed: `{sql_p}`\n  twin: {eq}\n  partitioned: {reply_p:?}"),
                ),
                Ok(reply_q) => {
                    if let Err(why) = self.compare(at, op, sql_p, &reply_p, &reply_q) {
                        if KNOWN_DIVERGENCES.iter().any(|(k, _)| *k == kind) {
                            self.tally.entry(kind).or_default().diverged_known += 1;
                        } else {
                            self.fail(at, kind, why);
                        }
                    }
                    Verdict::Agreed { failed: false }
                }
            },
        }
    }

    /// What both tables store, read through the API, must be the same rows,
    /// and every partitioned row must sit in the partition owning its point
    /// (I4). Until S10 the partitioned side's `SELECT` is refused, so this is
    /// how a write that answered like the twin's but stored something else
    /// (a lost row, a duplicate, a wrong value, a misrouted row) is caught.
    fn compare_storage(&mut self, at: usize, kind: Kind) {
        let db = self.server.as_ref().unwrap().db.clone();
        let p = stored_rows(&db, PART);
        let q = stored_rows(&db, PLAIN);
        if p != q {
            let only_p: Vec<_> = p.iter().filter(|r| !q.contains(r)).take(5).collect();
            let only_q: Vec<_> = q.iter().filter(|r| !p.contains(r)).take(5).collect();
            self.fail(
                at,
                kind,
                format!(
                    "the tables store different rows: partitioned {} rows, twin {}\n  only partitioned: {only_p:?}\n  \
                     only twin: {only_q:?}",
                    p.len(),
                    q.len()
                ),
            );
        }
        let misplaced = misplaced_rows(&db, PART);
        if !misplaced.is_empty() {
            self.fail(at, kind, format!("rows outside their partition's range: {misplaced:?}"));
        }
        self.storage_checks += 1;
    }

    fn note(&mut self, kind: Kind, verdict: &Verdict) {
        let t = self.tally.entry(kind).or_default();
        match verdict {
            Verdict::Refused => t.refused += 1,
            Verdict::Agreed { failed: true } => t.both_failed += 1,
            Verdict::Agreed { failed: false } => t.agreed += 1,
        }
    }

    /// Runs one operation. Returns whether it ended in a failure (refusal or
    /// error), which inside a block ends the block.
    fn run(&mut self, at: usize, op: &Op) -> bool {
        match &op.step {
            Step::Simple(template) => {
                let sql_p = render(template, &SIDE_PART);
                let sql_q = render(template, &SIDE_PLAIN);
                self.log.push(sql_p.clone());
                let p = self.cp().simple(&sql_p);
                let verdict = self.judge(at, op, &sql_p, p, true, &mut |tw: &mut Twin| tw.cq().simple(&sql_q));
                self.note(op.kind, &verdict);
                !matches!(verdict, Verdict::Agreed { failed: false })
            }
            Step::Prepared { sql, runs } => {
                let sql_p = render(sql, &SIDE_PART);
                let sql_q = render(sql, &SIDE_PLAIN);
                let name = format!("tw{at}");
                self.log.push(format!("PREPARE {name} AS {sql_p}"));
                let p = self.cp().prepare(&name, &sql_p);
                let name_q = name.clone();
                let verdict = self.judge(at, op, &sql_p, p, false, &mut |tw: &mut Twin| tw.cq().prepare(&name_q, &sql_q));
                let mut failed = !matches!(verdict, Verdict::Agreed { failed: false });
                if failed {
                    self.note(op.kind, &verdict);
                } else {
                    for params in runs {
                        self.log.push(format!("EXECUTE {name} {params:?}"));
                        let p = self.cp().execute_prepared(&name, params);
                        let name_q = name.clone();
                        let params_q = params.clone();
                        let verdict = self.judge(at, op, &format!("{sql_p} with {params:?}"), p, true, &mut |tw: &mut Twin| {
                            tw.cq().execute_prepared(&name_q, &params_q)
                        });
                        self.note(op.kind, &verdict);
                        if !matches!(verdict, Verdict::Agreed { failed: false }) {
                            failed = true;
                            break;
                        }
                    }
                }
                self.cp().close_prepared(&name);
                self.cq().close_prepared(&name);
                failed
            }
            Step::Block { inner, commit } => {
                self.log.push("BEGIN".into());
                self.cp().ok("BEGIN");
                self.cq().ok("BEGIN");
                let mut aborted = false;
                for inner_op in inner {
                    if self.run(at, inner_op) {
                        aborted = true;
                        break;
                    }
                }
                let end = if *commit && !aborted { "COMMIT" } else { "ROLLBACK" };
                self.log.push(end.into());
                let p = self.cp().simple(end);
                let q = self.cq().simple(end);
                match (p, q) {
                    (Ok(a), Ok(b)) if a.tags == b.tags => {}
                    (p, q) => self.fail(at, op.kind, format!("{end} differs:\n  partitioned: {p:?}\n  twin: {q:?}")),
                }
                self.note(op.kind, &Verdict::Agreed { failed: aborted });
                false
            }
            Step::Restart => {
                self.log.push("-- restart".into());
                self.cp = None;
                self.cq = None;
                let server = self.server.take().unwrap();
                let server = server.restart();
                self.cp = Some(server.connect());
                self.cq = Some(server.connect());
                let uid = server.db.partitioned.get(PART).map(|t| t.uid());
                assert_eq!(uid, Some(self.uid), "{}: the partitioned table did not survive a restart", self.label);
                self.server = Some(server);
                self.note(op.kind, &Verdict::Agreed { failed: false });
                false
            }
        }
    }
}

fn env_u64(name: &str) -> Option<u64> {
    let v = std::env::var(name).ok()?;
    let v = v.trim();
    if let Some(hex) = v.strip_prefix("0x") { u64::from_str_radix(hex, 16).ok() } else { v.parse().ok() }
}

fn seeds() -> Vec<u64> {
    match env_u64("PART_TWIN_SEED") {
        Some(s) => vec![s],
        None => vec![0x5eed_0001, 0x5eed_7a11],
    }
}

fn ops_per_seed() -> usize {
    env_u64("PART_TWIN_OPS").map(|n| n as usize).unwrap_or(300)
}

/// Total bytes under `dir`.
fn dir_bytes(dir: &std::path::Path) -> u64 {
    let mut total = 0;
    for e in std::fs::read_dir(dir).into_iter().flatten().flatten() {
        let m = match e.metadata() {
            Ok(m) => m,
            Err(_) => continue,
        };
        total += if m.is_dir() { dir_bytes(&e.path()) } else { m.len() };
    }
    total
}

fn run_twin(cfg: Config) {
    for seed in seeds() {
        run_twin_seed(&cfg, seed);
    }
}

fn run_twin_seed(cfg: &Config, seed: u64) {
    let started = Instant::now();
    let label = format!("HASH ({}) PARTITIONS {}", cfg.key.join(", "), cfg.partitions);
    let dir = tempfile::tempdir().expect("a temp dir");
    let server = Server::start(dir.path().to_str().unwrap());
    let mut setup = server.connect();
    setup.ok(&format!(
        "CREATE TABLE {PART} {COLUMNS} PARTITION BY HASH ({}) PARTITIONS {}",
        cfg.key.join(", "),
        cfg.partitions
    ));
    setup.ok(&format!("CREATE TABLE {PLAIN} {COLUMNS}"));
    for side in [&SIDE_PART, &SIDE_PLAIN] {
        setup.ok(&format!("CREATE TABLE {} (tenant_id BIGINT NOT NULL, label TEXT)", side.d));
        let dims: Vec<String> = (1..=TENANTS).step_by(2).map(|t| format!("({t}, 'tenant {t}')")).collect();
        setup.ok(&format!("INSERT INTO {} (tenant_id, label) VALUES {}", side.d, dims.join(", ")));
        setup.ok(&format!("CREATE TABLE {} (tenant_id BIGINT, id BIGINT, n INT)", side.s));
    }
    // The shared, read-only source of INSERT … SELECT.
    setup.ok(&format!("CREATE TABLE stage {COLUMNS}"));
    let mut stage_rows = Vec::new();
    for t in 1..=TENANTS {
        for g in 1..=GROUPS {
            for i in (1..=IDS).step_by(3) {
                stage_rows.push(format!("({t}, {g}, {i}, {}, 'staged', {})", (t * 7 + i) % 90, (i % 8) as f64 * 0.25));
            }
        }
    }
    for chunk in stage_rows.chunks(100) {
        setup.ok(&format!("INSERT INTO stage (tenant_id, grp, id, n, s, f) VALUES {}", chunk.join(", ")));
    }
    // Both tables start with the same rows, written through SQL (routed
    // INSERT, S9), so the catalogs compare over real rows and an empty
    // answer cannot pass for a right one once a slice lifts a read. Every
    // tenant but one gets rows; `NEVER` gets none.
    let mut seed_rng = fastrand::Rng::with_seed(seed ^ 0xa5a5);
    let mut keys = BTreeSet::new();
    while keys.len() < 40 {
        keys.insert((seed_rng.i64(1..=TENANTS), seed_rng.i64(1..=GROUPS), seed_rng.i64(1..=IDS)));
    }
    let mut seeder = Gen { rng: seed_rng, key: cfg.key };
    let inserts: Vec<String> = keys
        .iter()
        .map(|(t, g, i)| {
            let (n, s, f) = (seeder.n(), seeder.s(), seeder.f());
            format!("INSERT INTO {PART} (tenant_id, grp, id, n, s, f) VALUES ({t}, {g}, {i}, {n}, {s}, {f})")
        })
        .collect();
    for sql in &inserts {
        setup.ok(&sql.replace(PART, PLAIN));
        setup.ok(sql);
    }
    drop(setup);
    let uid = server.db.partitioned.get(PART).expect("the partitioned table").uid();

    let mut twin = Twin {
        cp: Some(server.connect()),
        cq: Some(server.connect()),
        server: Some(server),
        seed,
        label: label.clone(),
        uid,
        log: Vec::new(),
        tally: BTreeMap::new(),
        storage_checks: 0,
    };
    // The seeded rows, before anything else runs.
    twin.compare_storage(0, Restart);
    let mut generator = Gen { rng: fastrand::Rng::with_seed(seed), key: cfg.key };

    // First every kind once, in a seeded order; then weighted draws.
    let mut order: Vec<Kind> = ALL_KINDS.iter().copied().filter(|k| *k != Restart).collect();
    generator.rng.shuffle(&mut order);
    let total_weight: u32 = ALL_KINDS.iter().map(|k| weight(*k)).sum();
    let ops = ops_per_seed().max(order.len() + 1);
    // After the first pass, so CatalogIndexes runs once before the restart (F1).
    let restart_at = (ops / 2).max(order.len());
    for at in 0..ops {
        let kind = if at == restart_at {
            Restart
        } else if let Some(k) = order.get(at) {
            *k
        } else {
            let mut pick = generator.rng.u32(..total_weight);
            *ALL_KINDS
                .iter()
                .find(|k| {
                    let w = weight(**k);
                    if pick < w {
                        true
                    } else {
                        pick -= w;
                        false
                    }
                })
                .unwrap()
        };
        let op = generator.op(kind);
        twin.run(at, &op);
        if writes_t(kind) {
            twin.compare_storage(at, kind);
        }
        assert!(
            started.elapsed() < Duration::from_secs(600),
            "{label} seed {seed:#x}: over the wall-clock cap at op {at}"
        );
    }
    // The last word: both tables in full.
    let last = generator.op(SelectAll);
    twin.run(ops, &last);
    twin.compare_storage(ops, SelectAll);

    // Every kind was exercised, and every known divergence still diverges.
    for kind in ALL_KINDS {
        assert!(twin.tally.contains_key(kind), "{label} seed {seed:#x}: {kind:?} never ran");
    }
    for (kind, finding) in KNOWN_DIVERGENCES {
        let t = twin.tally.get(kind).expect("ran");
        assert!(
            t.diverged_known > 0,
            "{label} seed {seed:#x}: {kind:?} is in KNOWN_DIVERGENCES but agreed every time; remove the entry \
             ({finding})"
        );
    }
    // A refused kind ran and was refused; a kind not in REFUSED was never refused.
    for (kind, t) in &twin.tally {
        if refusal(*kind).is_none() {
            assert_eq!(t.refused, 0, "{label}: {kind:?} counted a refusal while not in REFUSED");
        }
    }

    // I8's tail: the shell is still the partitioned table's, and the root
    // holds no file under the logical name.
    let server = twin.server.take().unwrap();
    assert_eq!(server.db.partitioned.get(PART).map(|t| t.uid()), Some(uid), "{label}");
    assert!(server.db.tables.get(PART).is_some_and(|t| t.schema.name == PART), "{label}: the shell is gone");
    assert_eq!(files_of_table(dir.path(), PART), Vec::<String>::new(), "{label}: files under the logical name");
    let bytes = dir_bytes(dir.path());
    assert!(bytes < 50 * 1024 * 1024, "{label}: the run wrote {bytes} bytes");

    let summary: Vec<String> = twin
        .tally
        .iter()
        .map(|(k, t)| format!("{k:?}: refused {} agreed {} both-failed {} known-diverged {}", t.refused, t.agreed, t.both_failed, t.diverged_known))
        .collect();
    eprintln!(
        "{label} seed {seed:#x}: {ops} ops in {:.1}s, {} bytes, {} storage comparisons\n  {}",
        started.elapsed().as_secs_f64(),
        bytes,
        twin.storage_checks,
        summary.join("\n  ")
    );
    twin.cp = None;
    twin.cq = None;
    drop(server);
}

#[test]
fn twin_hash_tenant_7() {
    run_twin(Config { key: &["tenant_id"], partitions: 7 });
}

#[test]
fn twin_hash_tenant_64() {
    run_twin(Config { key: &["tenant_id"], partitions: 64 });
}

#[test]
fn twin_hash_tenant_grp_64() {
    run_twin(Config { key: &["tenant_id", "grp"], partitions: 64 });
}

#[test]
fn twin_hash_full_key_7() {
    run_twin(Config { key: &["tenant_id", "grp", "id"], partitions: 7 });
}

/// The two lists name each kind at most once, and only real kinds.
#[test]
fn refused_set_is_one_entry_per_kind() {
    let mut seen = BTreeSet::new();
    for r in REFUSED {
        assert!(seen.insert(r.kind), "{:?} is in REFUSED twice", r.kind);
        assert!(ALL_KINDS.contains(&r.kind));
    }
    for (k, _) in KNOWN_DIVERGENCES {
        assert!(refusal(*k).is_none(), "{k:?} is both refused and a known divergence");
    }
    let mut all = BTreeSet::new();
    for k in ALL_KINDS {
        assert!(all.insert(*k), "{k:?} is in ALL_KINDS twice");
    }
}
