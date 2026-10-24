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
//!   same way, whether or not the kind is refused. Only a write may do that:
//!   a read kind that failed on both compared nothing, and fails the run.
//!
//! Inside a transaction block any failure ends the block: both sides roll
//! back, so a refused write never reaches the twin.
//!
//! [`REFUSED`] is the one place that says what the partitioned table does
//! not do yet: since phase 2b, only what phases 3 and 6 build. Slices S9 to
//! S19 each removed their kinds from it; the twin was their done-check. [`KNOWN_DIVERGENCES`] lists allowed kinds whose answers differ
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
use rasterizeddb_core::pgwire::PREPARED_RUNS;

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
    // ON CONFLICT on partitioned tables (S18): DO NOTHING … RETURNING (only
    // the inserted rows come back), DO UPDATE … WHERE with EXCLUDED,
    // DO UPDATE … RETURNING (the post-image), a multi-row upsert over three
    // tenants with a duplicate (the row count compared), INSERT … SELECT
    // … ON CONFLICT, a key column assigned EXCLUDED's own value, and a
    // prepared upsert alternating tenants.
    InsertOnConflictNothingReturning,
    InsertOnConflictUpdateWhere,
    InsertOnConflictUpdateReturning,
    InsertMultiOnConflict,
    InsertSelectOnConflict,
    InsertOnConflictKeyAssign,
    // A DO UPDATE that would move the row to another point: refused (phase
    // 6). Run in a rolled-back block after an insert of the same key, so
    // the conflict, and with it the refusal, always happens.
    InsertOnConflictKeyChange,
    PreparedUpsert,
    // ON CONFLICT errors, the same SQLSTATE on both table kinds with
    // nothing written (1c421c4): a target that is a strict subset of the
    // key (42P10), and a multi-row DO UPDATE proposing one key twice (21000).
    InsertOnConflictSubsetTarget,
    InsertOnConflictUpdateTwice,
    InsertReturning,
    UpdateByKey,
    UpdateByPartitionKey,
    UpdateByPrefix,
    UpdateByNonKey,
    UpdateByFunction,
    UpdateRelocating,
    UpdateKeySameValue,
    UpdateKeyChange,
    UpdateReturning,
    // RETURNING expressions and aliases over a fan-out (S17).
    UpdateReturningExpr,
    UpdateFrom,
    DeleteByKey,
    DeleteByPrefix,
    DeleteByNonKey,
    DeleteByFunction,
    DeleteIn,
    DeleteReturning,
    // An unrouted DELETE … RETURNING * inside a block, rolled back (S17).
    DeleteReturningFanOut,
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
    SelectFunctionWhere,
    SelectLimitZero,
    CountAll,
    CountByPrefix,
    Aggregates,
    GroupBy,
    GroupByOrdered,
    OrderByLimit,
    OrderByExpr,
    Distinct,
    CountDistinct,
    Window,
    Join,
    LeftJoin,
    SelfJoin,
    // Joins over partitioned tables (S19): a join whose WHERE pins the
    // partitioned side's key (routed), one that fans out (ORDER BY/LIMIT, a
    // CTE, a derived table, an IN subquery, the key pinned only on the
    // other side), two partitioned tables joined (`{u}`), aggregates over a
    // join, a prepared join, and a partitioned source of UPDATE … FROM and
    // DELETE … USING writing a plain target (`{d}`).
    JoinRouted,
    JoinFanOut,
    JoinPartitionedBoth,
    JoinAggregates,
    PreparedJoin,
    UpdateDimFromT,
    DeleteDimUsingT,
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
    // PreparedSelect and PreparedCount run past the learned parse (S15): the
    // partitioned side must answer its later runs from the cache.
    PreparedSelect,
    PreparedCount,
    // A prepared SELECT whose Describe labels columns by select-item alias,
    // each item used once (owner constraint 1, 1099cb0).
    PreparedDescribeAlias,
    // A prepared SELECT learned, then an unrelated table created and dropped
    // between its runs: the parse is learned again, and no route was kept.
    PreparedSelectAcrossDdl,
    PreparedInsert,
    PreparedUpdate,
    PreparedDelete,
    // A prepared UPDATE … RETURNING with an alias and an expression (S17):
    // the Describe and every execution compared.
    PreparedUpdateReturning,
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
    InsertOnConflictUpdate, InsertOnConflictNothingReturning, InsertOnConflictUpdateWhere,
    InsertOnConflictUpdateReturning, InsertMultiOnConflict, InsertSelectOnConflict, InsertOnConflictKeyAssign,
    InsertOnConflictKeyChange, PreparedUpsert, InsertOnConflictSubsetTarget, InsertOnConflictUpdateTwice,
    InsertReturning, UpdateByKey, UpdateByPartitionKey, UpdateByPrefix, UpdateByNonKey,
    UpdateByFunction, UpdateRelocating, UpdateKeySameValue, UpdateKeyChange, UpdateReturning, UpdateReturningExpr,
    UpdateFrom, DeleteByKey, DeleteByPrefix, DeleteByNonKey, DeleteByFunction, DeleteIn, DeleteReturning,
    DeleteReturningFanOut, DeleteUsing, InsertIntoSinkFromT,
    UpdateDimViaSubquery, Vacuum, VacuumFull, Analyze, SelectAll, SelectByKey, SelectByPartitionKey,
    SelectByPrefix, SelectIn, SelectInComposite, SelectOr, SelectOrUnconstrained, SelectRange,
    SelectNonKeyRange, SelectRowValue, SelectSpellings, SelectNonKey, SelectFunctionWhere, SelectLimitZero,
    CountAll, CountByPrefix, Aggregates, GroupBy, GroupByOrdered, OrderByLimit, OrderByExpr, Distinct,
    CountDistinct, Window, Join, LeftJoin,
    SelfJoin, JoinRouted, JoinFanOut, JoinPartitionedBoth, JoinAggregates, PreparedJoin, UpdateDimFromT,
    DeleteDimUsingT, InSubquery, Union, Cte, ForUpdate, SkipLocked, EmptyRouteCount, EmptyRouteMax,
    EmptyRouteAggregates, EmptyRouteContradiction, EmptyRouteContradictionRows, EmptyRouteUpdateReturning,
    EmptyRouteDeleteReturning, PreparedSelect, PreparedCount, PreparedDescribeAlias, PreparedSelectAcrossDdl,
    PreparedInsert, PreparedUpdate, PreparedDelete, PreparedUpdateReturning, CatalogColumns, CatalogTables, CatalogClass, CatalogStat, CatalogIndexes, Describe, ReadSinkAndDim,
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

/// What the partitioned table does not do yet, as of phase 2b: the phase 3
/// and phase 6 kinds. One entry per kind.
/// A slice that builds a kind removes its entry; the twin then compares that
/// kind's answers instead of demanding a refusal.
const REFUSED: &[Refusal] = &[
    // S9 (routed INSERT) is built: InsertOne, InsertMulti, InsertNulls,
    // InsertQuotedKey, InsertSelect and PreparedInsert are compared with the
    // twin, and so is what they store (`Twin::compare_storage`).
    // S17 (RETURNING on INSERT, UPDATE and DELETE) is built: InsertReturning,
    // UpdateReturning, UpdateReturningExpr, DeleteReturning,
    // DeleteReturningFanOut, PreparedUpdateReturning and the empty-route
    // UPDATE and DELETE … RETURNING are compared with the twin, rows as
    // multisets, names and type OIDs included, inside blocks too.
    // S18 (ON CONFLICT) is built: InsertOnConflictNothing,
    // InsertOnConflictUpdate, InsertOnConflictNothingReturning,
    // InsertOnConflictUpdateWhere, InsertOnConflictUpdateReturning,
    // InsertMultiOnConflict, InsertSelectOnConflict, InsertOnConflictKeyAssign
    // and PreparedUpsert are compared with the twin, and so is what they
    // store, inside blocks too. A DO UPDATE that changes a key value moves
    // the row (I4): phase 6.
    refused(InsertOnConflictKeyChange, "phase 6"),
    // S11 (routed UPDATE and DELETE, VACUUM fan-out) is built: UpdateByKey,
    // UpdateByPartitionKey, UpdateByPrefix, UpdateByNonKey, UpdateRelocating,
    // UpdateKeySameValue (a key assigned the value its WHERE pins), DeleteByKey,
    // DeleteByPrefix, DeleteByNonKey, DeleteIn, PreparedUpdate, PreparedDelete,
    // Vacuum and VacuumFull are compared with the twin, inside blocks too.
    // A key change moves a row between partitions (I4): phase 6.
    refused(UpdateKeyChange, "phase 6"),
    // Phase 3: UPDATE FROM and DELETE USING with a partitioned target (a
    // partitioned source is built since S19: UpdateDimFromT,
    // DeleteDimUsingT), FOR UPDATE, SKIP LOCKED.
    refused(UpdateFrom, "phase 3"),
    refused(DeleteUsing, "phase 3"),
    refused(ForUpdate, "phase 3"),
    refused(SkipLocked, "phase 3"),
    // S10 (single-snapshot fan-out SELECT and COUNT) is built: every read
    // kind is compared with the twin, ORDER BY across partitions included
    // (correct by sorting the partitions' top-N together; S12 makes it
    // cheap), and so are the reads inside INSERT … SELECT, IN (SELECT …),
    // UNION and CTEs, the empty routes, and prepared SELECT and COUNT. Since
    // S15 those two run from the learned parse, routed per execution (I11).
    // S19 (joins) is built: Join, LeftJoin, SelfJoin, JoinRouted,
    // JoinFanOut, JoinPartitionedBoth, JoinAggregates and PreparedJoin are
    // compared with the twin, names and type OIDs included, inside blocks
    // too, every relation and partition read under the join's one snapshot.
];

/// Allowed kinds whose answers differ today, each with the finding that
/// explains it. A difference in one of these is recorded, not failed; a run
/// in which one of them never differs fails, so the entry goes with the fix.
///
/// Empty since F1 (pg_indexes listed no implicit index for a plain table's
/// composite key until its first reopen) was fixed upstream by fec2dd9 (a
/// composite key is indexed and named from creation).
const KNOWN_DIVERGENCES: &[(Kind, &str)] = &[];

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

/// Each side's names: `{t}` the table under test, `{d}` a dimension table
/// (keyed by tenant, so `UPDATE {d} … FROM {t}` can write each row back),
/// `{s}` a sink, `{u}` a second table partitioned on the partitioned side
/// (read-only, for joins of two partitioned tables, S19). The stage table is
/// shared and read-only.
struct Side {
    t: &'static str,
    d: &'static str,
    s: &'static str,
    u: &'static str,
}

const SIDE_PART: Side = Side { t: PART, d: "dim_part", s: "sink_part", u: "pair_part" };
const SIDE_PLAIN: Side = Side { t: PLAIN, d: "dim_plain", s: "sink_plain", u: "pair_plain" };

fn render(template: &str, side: &Side) -> String {
    template.replace("{t}", side.t).replace("{d}", side.d).replace("{s}", side.s).replace("{u}", side.u)
}

/// Undoes [`render`] in an answer, so the two sides compare.
fn unrender(text: &str) -> String {
    let mut out = text.to_string();
    for side in [&SIDE_PART, &SIDE_PLAIN] {
        out = out.replace(side.d, "{d}").replace(side.s, "{s}").replace(side.u, "{u}").replace(side.t, "{t}");
        out = out
            .replace(&side.d.to_ascii_uppercase(), "{D}")
            .replace(&side.s.to_ascii_uppercase(), "{S}")
            .replace(&side.u.to_ascii_uppercase(), "{U}")
            .replace(&side.t.to_ascii_uppercase(), "{T}");
    }
    out
}

// ---------------------------------------------------------------------------
// Statement generation.
// ---------------------------------------------------------------------------

enum Step {
    Simple(String),
    Prepared {
        sql: String,
        runs: Vec<Vec<Option<String>>>,
        /// From this run (0-based) on, the partitioned side must answer from
        /// the learned parse (`PREPARED_RUNS` moves): the first two runs
        /// teach it, with values that place unambiguously.
        cached_from: Option<usize>,
        /// Run once (on the shared database) after the run of that index.
        ddl_after: Vec<(usize, String)>,
        /// The column names the statement's Describe must give, on both sides.
        columns: Option<&'static [&'static str]>,
    },
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
        // One row in eight under a negative tenant, so a function of the key
        // (`ABS(tenant_id) = t`) matches rows in two partitions.
        let t = if self.rng.u8(..8) == 0 { -self.tenant() } else { self.tenant() };
        let (g, i) = (self.grp(), self.id());
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

    /// A RETURNING list: columns, `*`, columns out of order, or aliases and
    /// expressions (INSERT … RETURNING col AS alias, upstream since 3fa2fc3).
    fn returning_list(&mut self) -> &'static str {
        match self.rng.u8(..4) {
            0 => "tenant_id, grp, id, n, s",
            1 => "*",
            2 => "s, id, f, tenant_id",
            _ => "tenant_id AS t, id AS k, n * 2 + 1, COALESCE(s, 'x') AS s2",
        }
    }

    /// An ON CONFLICT target that a partitioned table's unique key matches
    /// (every key column named), with its trailing space: none, the primary
    /// key's columns in some order, or the key by constraint name.
    fn conflict_target(&mut self) -> &'static str {
        match self.rng.u8(..4) {
            0 => "",
            1 => "(tenant_id, grp, id) ",
            2 => "(id, tenant_id, grp) ",
            _ => "ON CONSTRAINT {t}_pkey ",
        }
    }

    fn simple(&mut self, kind: Kind, sql: String) -> Op {
        Op { kind, step: Step::Simple(sql), ordered: false }
    }

    fn ordered(&mut self, kind: Kind, sql: String) -> Op {
        Op { kind, step: Step::Simple(sql), ordered: true }
    }

    fn prepared(&mut self, kind: Kind, sql: &str, runs: Vec<Vec<Option<String>>>) -> Op {
        let step = Step::Prepared { sql: sql.to_string(), runs, cached_from: None, ddl_after: Vec::new(), columns: None };
        Op { kind, step, ordered: false }
    }

    /// A prepared statement run past its learned parse: four to six runs, the
    /// first two with tenants in `4..=9` (single digits, never a group number,
    /// so `WhereTemplate::derive` places each value) differing in every value,
    /// and the partitioned side answering from the cache from the third.
    fn learned(&mut self, kind: Kind, sql: &str, with_grp: bool, columns: Option<&'static [&'static str]>) -> Op {
        let t1 = self.rng.i64(4..=9);
        let t2 = loop {
            let t = self.rng.i64(4..=9);
            if t != t1 {
                break t;
            }
        };
        let g1 = self.grp();
        let g2 = if g1 == GROUPS { 1 } else { g1 + 1 };
        let one = |t: i64, g: i64| {
            let mut run = vec![Some(t.to_string())];
            if with_grp {
                run.push(Some(g.to_string()));
            }
            run
        };
        let mut runs = vec![one(t1, g1), one(t2, g2)];
        for _ in 0..self.rng.usize(2..=4) {
            let (t, g) = (self.tenant(), self.grp());
            runs.push(one(t, g));
        }
        let step = Step::Prepared { sql: sql.to_string(), runs, cached_from: Some(2), ddl_after: Vec::new(), columns };
        Op { kind, step, ordered: false }
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
                let target = self.conflict_target();
                self.simple(kind, format!("INSERT INTO {{t}} {cols} VALUES {r} ON CONFLICT {target}DO NOTHING"))
            }
            InsertOnConflictUpdate => {
                let r = self.row();
                let n = self.n();
                let set = match self.rng.u8(..3) {
                    0 => format!("n = {n}, s = 'upserted'"),
                    1 => "n = EXCLUDED.n, s = EXCLUDED.s, f = EXCLUDED.f".to_string(),
                    _ => "n = COALESCE({t}.n, 0) + COALESCE(EXCLUDED.n, 1), s = 'again'".to_string(),
                };
                let target = if self.rng.u8(..4) == 0 { "ON CONSTRAINT {t}_pkey" } else { "(tenant_id, grp, id)" };
                self.simple(kind, format!("INSERT INTO {{t}} {cols} VALUES {r} ON CONFLICT {target} DO UPDATE SET {set}"))
            }
            InsertOnConflictNothingReturning => {
                // One row or several; only the rows inserted come back.
                let k = self.rng.usize(1..=4);
                let rows: Vec<String> = (0..k).map(|_| self.row()).collect();
                let target = self.conflict_target();
                let list = self.returning_list();
                self.simple(
                    kind,
                    format!("INSERT INTO {{t}} {cols} VALUES {} ON CONFLICT {target}DO NOTHING RETURNING {list}", rows.join(", ")),
                )
            }
            InsertOnConflictUpdateWhere => {
                let r = self.row();
                let v = self.rng.i32(-50..100);
                let condition = match self.rng.u8(..4) {
                    0 => format!("{{t}}.n < {v}"),
                    1 => "EXCLUDED.n IS NOT NULL AND {t}.n IS DISTINCT FROM EXCLUDED.n".to_string(),
                    2 => format!("EXCLUDED.n > {v} OR {{t}}.s IS NULL"),
                    _ => "{t}.s <> 'kept'".to_string(),
                };
                self.simple(
                    kind,
                    format!(
                        "INSERT INTO {{t}} {cols} VALUES {r} ON CONFLICT (tenant_id, grp, id) \
                         DO UPDATE SET n = EXCLUDED.n, s = 'kept' WHERE {condition}"
                    ),
                )
            }
            InsertOnConflictUpdateReturning => {
                // The post-image comes back: an updated row's new values, an
                // inserted row's own. One row or several (each placed in its
                // own partition), WHERE or not.
                let k = self.rng.usize(1..=3);
                let rows: Vec<String> = (0..k).map(|_| self.row()).collect();
                let condition = if self.rng.bool() { " WHERE {t}.n IS NOT NULL" } else { "" };
                let list = self.returning_list();
                self.simple(
                    kind,
                    format!(
                        "INSERT INTO {{t}} {cols} VALUES {} ON CONFLICT (tenant_id, grp, id) \
                         DO UPDATE SET n = COALESCE({{t}}.n, 0) + 1, f = EXCLUDED.f{condition} RETURNING {list}",
                        rows.join(", ")
                    ),
                )
            }
            InsertMultiOnConflict => {
                // Three tenants, one tuple repeated: DO NOTHING skips the
                // repeat (and any stored key), and the count says how many
                // landed. DO UPDATE gets distinct keys (PostgreSQL refuses a
                // row affected twice).
                let (a, b) = (self.row(), self.row());
                let c = self.row();
                if self.rng.bool() {
                    let target = self.conflict_target();
                    self.simple(kind, format!("INSERT INTO {{t}} {cols} VALUES {a}, {b}, {a}, {c} ON CONFLICT {target}DO NOTHING"))
                } else {
                    let tenants = [self.tenant(), self.tenant(), self.tenant()];
                    let i = self.id();
                    let rows: Vec<String> = tenants
                        .iter()
                        .enumerate()
                        .map(|(j, t)| format!("({t}, {}, {i}, {}, 'multi', 0.5)", j as i64 + 1, self.n()))
                        .collect();
                    self.simple(
                        kind,
                        format!(
                            "INSERT INTO {{t}} {cols} VALUES {} ON CONFLICT (tenant_id, grp, id) \
                             DO UPDATE SET n = {{t}}.n + 1, s = EXCLUDED.s",
                            rows.join(", ")
                        ),
                    )
                }
            }
            InsertSelectOnConflict => {
                let t = self.tenant();
                let m = self.rng.i64(0..IDS);
                let tail = match self.rng.u8(..3) {
                    0 => "DO NOTHING".to_string(),
                    1 => "DO UPDATE SET n = COALESCE({t}.n, 0) + EXCLUDED.n".to_string(),
                    _ => "DO UPDATE SET s = EXCLUDED.s RETURNING tenant_id, grp, id, s".to_string(),
                };
                self.simple(
                    kind,
                    format!(
                        "INSERT INTO {{t}} {cols} SELECT tenant_id, grp, id, n, s, f FROM stage \
                         WHERE tenant_id = {t} AND id > {m} ON CONFLICT (tenant_id, grp, id) {tail}"
                    ),
                )
            }
            InsertOnConflictKeyAssign => {
                // A key column assigned EXCLUDED's own value keeps the point.
                let r = self.row();
                let n = self.n();
                let set = if self.rng.bool() {
                    format!("tenant_id = EXCLUDED.tenant_id, n = {n}")
                } else {
                    "tenant_id = EXCLUDED.tenant_id, grp = EXCLUDED.grp, id = EXCLUDED.id, s = 'assigned'".to_string()
                };
                self.simple(kind, format!("INSERT INTO {{t}} {cols} VALUES {r} ON CONFLICT (tenant_id, grp, id) DO UPDATE SET {set}"))
            }
            InsertOnConflictKeyChange => {
                let r = self.row();
                // tenant_id leads the key in every configuration, so each of
                // these moves the point there.
                let set = match self.rng.u8(..3) {
                    0 => "tenant_id = EXCLUDED.tenant_id + 100",
                    1 => "tenant_id = {t}.tenant_id - 1000, n = 0",
                    _ => "n = 1, tenant_id = 77",
                };
                let seed = Op {
                    kind: InsertOnConflictNothing,
                    step: Step::Simple(format!("INSERT INTO {{t}} {cols} VALUES {r} ON CONFLICT DO NOTHING")),
                    ordered: false,
                };
                let change = Op {
                    kind,
                    step: Step::Simple(format!(
                        "INSERT INTO {{t}} {cols} VALUES {r} ON CONFLICT (tenant_id, grp, id) DO UPDATE SET {set}"
                    )),
                    ordered: false,
                };
                Op { kind, step: Step::Block { inner: vec![seed, change], commit: false }, ordered: false }
            }
            PreparedUpsert => {
                let runs = self.prepared_runs(|g| {
                    vec![
                        Some(g.tenant().to_string()),
                        Some(g.grp().to_string()),
                        Some(g.id().to_string()),
                        Some(g.rng.i32(-50..100).to_string()),
                    ]
                });
                self.prepared(
                    kind,
                    "INSERT INTO {t} (tenant_id, grp, id, n) VALUES ($1, $2, $3, $4) ON CONFLICT (tenant_id, grp, id) \
                     DO UPDATE SET n = EXCLUDED.n + COALESCE({t}.n, 0)",
                    runs,
                )
            }
            InsertOnConflictSubsetTarget => {
                let r = self.row();
                let target = if self.rng.bool() { "(tenant_id)" } else { "(grp, tenant_id)" };
                let action = if self.rng.bool() { "DO NOTHING" } else { "DO UPDATE SET n = EXCLUDED.n" };
                self.simple(kind, format!("INSERT INTO {{t}} {cols} VALUES {r} ON CONFLICT {target} {action}"))
            }
            InsertOnConflictUpdateTwice => {
                let (t, g, i) = (self.tenant(), self.grp(), self.id());
                let other = (self.tenant(), self.grp(), self.id());
                self.simple(
                    kind,
                    format!(
                        "INSERT INTO {{t}} (tenant_id, grp, id, n) VALUES ({t}, {g}, {i}, 1), ({}, {}, {}, 2), \
                         ({t}, {g}, {i}, 3) ON CONFLICT (tenant_id, grp, id) DO UPDATE SET n = EXCLUDED.n",
                        other.0, other.1, other.2
                    ),
                )
            }
            InsertReturning => {
                // One row, several (each placed in its own partition), and
                // INSERT … SELECT; a column list, `*`, or columns out of order.
                let list = self.returning_list();
                let sql = match self.rng.u8(..3) {
                    0 => format!("INSERT INTO {{t}} {cols} VALUES {} RETURNING {list}", self.row()),
                    1 => {
                        let k = self.rng.usize(2..=4);
                        let rows: Vec<String> = (0..k).map(|_| self.row()).collect();
                        format!("INSERT INTO {{t}} {cols} VALUES {} RETURNING {list}", rows.join(", "))
                    }
                    _ => {
                        let (t, g) = (self.tenant(), self.grp());
                        let m = self.rng.i64(0..IDS);
                        format!(
                            "INSERT INTO {{t}} {cols} SELECT tenant_id, grp, id, n, s, f FROM stage \
                             WHERE tenant_id = {t} AND grp = {g} AND id > {m} RETURNING {list}"
                        )
                    }
                };
                self.simple(kind, sql)
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
            UpdateByFunction => {
                let t = self.tenant();
                let v = self.rng.i32(-50..100);
                let w = WORDS[self.rng.usize(..4)];
                let clause = match self.rng.u8(..4) {
                    0 => format!("ABS(tenant_id) = {t}"),
                    1 => format!("UPPER(s) = '{}'", w.to_ascii_uppercase()),
                    2 => format!("COALESCE(n, 0) > {v} AND tenant_id = {t}"),
                    _ => format!("LOWER(s) = '{w}' OR COALESCE(tenant_id, 0) = {t}"),
                };
                self.simple(kind, format!("UPDATE {{t}} SET n = COALESCE(n, 0) + 3 WHERE {clause}"))
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
            UpdateReturningExpr => {
                // A fan-out: no key column in the WHERE.
                let v = self.rng.i32(-50..100);
                self.simple(
                    kind,
                    format!(
                        "UPDATE {{t}} SET n = n + 1 WHERE n > {v} \
                         RETURNING tenant_id, grp, id, n * 2 + 1, COALESCE(s, 'x') AS s2, f AS ff"
                    ),
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
            DeleteByFunction => {
                let t = self.tenant();
                let w = WORDS[self.rng.usize(..4)];
                let clause = match self.rng.u8(..3) {
                    0 => format!("ABS(tenant_id) = {t} AND LOWER(s) = '{w}'"),
                    1 => format!("COALESCE(n, 0) < -45 OR UPPER(s) = '{}'", w.to_ascii_uppercase()),
                    _ => format!("tenant_id = {t} AND COALESCE(s, '') = ''"),
                };
                self.simple(kind, format!("DELETE FROM {{t}} WHERE {clause}"))
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
            DeleteReturningFanOut => {
                let v = self.rng.i32(-50..60);
                let inner = Op {
                    kind,
                    step: Step::Simple(format!("DELETE FROM {{t}} WHERE n < {v} OR s IS NULL RETURNING *")),
                    ordered: false,
                };
                Op { kind, step: Step::Block { inner: vec![inner], commit: false }, ordered: false }
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
            SelectFunctionWhere => {
                // Upstream's 4301045 evaluates these; none proves the key,
                // wrapping it or not, so each fans out (I3) unless a key
                // equality beside it routes.
                let t = self.tenant();
                let v = self.rng.i32(-50..100);
                let w = WORDS[self.rng.usize(..4)];
                let clause = match self.rng.u8(..7) {
                    0 => format!("ABS(tenant_id) = {t}"),
                    1 => format!("COALESCE(tenant_id, 0) = {t}"),
                    2 => format!("COALESCE(n, -1) > {v}"),
                    3 => format!("UPPER(s) = '{}'", w.to_ascii_uppercase()),
                    4 => format!("LOWER(s) = '{w}' OR tenant_id = {t}"),
                    5 => format!("tenant_id = {t} AND COALESCE(s, 'none') <> 'none'"),
                    _ => format!("ABS(n) < {} AND ABS(tenant_id) IN ({}, {})", v.abs(), self.tenant(), self.tenant()),
                };
                self.simple(kind, format!("SELECT * FROM {{t}} WHERE {clause}"))
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
            OrderByExpr => {
                // Unparenthesised since 4301045. The key breaks every tie; a
                // NULL n sorts by the engine's rule on both sides alike.
                let k = self.rng.usize(1..=6);
                let w = if self.rng.bool() { format!(" WHERE tenant_id = {}", self.tenant()) } else { String::new() };
                let dir = if self.rng.bool() { " DESC" } else { "" };
                self.ordered(
                    kind,
                    format!(
                        "SELECT tenant_id, grp, id, n * 2 + 1 AS x FROM {{t}}{w} ORDER BY n * 2 + 1{dir}, {KEY_ORDER} LIMIT {k}"
                    ),
                )
            }
            Distinct => self.simple(kind, "SELECT DISTINCT grp, s FROM {t}".into()),
            // Several DISTINCT aggregates in one SELECT, mixed with plain ones
            // and grouped: native in the fold since 4301045. Before it, every
            // one of these failed on both tables alike, so the kind was never
            // compared; the end-of-run check now fails a read that does that.
            CountDistinct => {
                let sql = match self.rng.u8(..4) {
                    0 => "SELECT COUNT(DISTINCT tenant_id), COUNT(DISTINCT s) FROM {t}".to_string(),
                    1 => "SELECT COUNT(DISTINCT grp), COUNT(DISTINCT n), COUNT(*), SUM(n), MAX(s) FROM {t}".to_string(),
                    2 => "SELECT grp, COUNT(DISTINCT tenant_id), COUNT(DISTINCT s) FROM {t} GROUP BY grp".to_string(),
                    _ => format!(
                        "SELECT COUNT(DISTINCT s), COUNT(DISTINCT f), AVG(f) FROM {{t}} WHERE tenant_id IN ({})",
                        self.tenant_list()
                    ),
                };
                self.simple(kind, sql)
            }
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
            JoinRouted => {
                let (k, g) = (self.tenant(), self.grp());
                let sql = match self.rng.u8(..3) {
                    0 => format!(
                        "SELECT {{t}}.tenant_id, {{t}}.grp, {{t}}.id, {{t}}.n, {{d}}.label FROM {{t}} JOIN {{d}} \
                         ON {{t}}.tenant_id = {{d}}.tenant_id WHERE {{t}}.tenant_id = {k} AND {{t}}.grp = {g}"
                    ),
                    // The plain side's label is NULL-extended for an even tenant.
                    1 => format!(
                        "SELECT {{t}}.id, {{t}}.s, {{t}}.f, {{d}}.label FROM {{t}} LEFT JOIN {{d}} \
                         ON {{d}}.tenant_id = {{t}}.tenant_id WHERE {{t}}.tenant_id = {k}"
                    ),
                    _ => format!(
                        "SELECT x.tenant_id, x.id, y.label FROM {{t}} x JOIN {{d}} y ON x.tenant_id = y.tenant_id \
                         WHERE x.tenant_id IN ({})",
                        self.tenant_list()
                    ),
                };
                self.simple(kind, sql)
            }
            JoinFanOut => {
                let v = self.rng.i32(-50..100);
                let (g, k) = (self.grp(), self.tenant());
                match self.rng.u8(..6) {
                    0 => {
                        let l = self.rng.usize(1..=8);
                        self.ordered(
                            kind,
                            format!(
                                "SELECT {{t}}.tenant_id, {{t}}.grp, {{t}}.id, {{t}}.n, {{d}}.label FROM {{t}} JOIN {{d}} \
                                 ON {{t}}.tenant_id = {{d}}.tenant_id WHERE {{t}}.n > {v} \
                                 ORDER BY {{t}}.n DESC, {{t}}.tenant_id, {{t}}.grp, {{t}}.id LIMIT {l}"
                            ),
                        )
                    }
                    1 => self.simple(
                        kind,
                        format!(
                            "WITH x AS (SELECT tenant_id, n FROM {{t}} WHERE grp = {g}) \
                             SELECT x.tenant_id, x.n, {{d}}.label FROM x JOIN {{d}} ON x.tenant_id = {{d}}.tenant_id"
                        ),
                    ),
                    2 => self.simple(
                        kind,
                        "SELECT {d}.tenant_id, m.mx FROM {d} JOIN (SELECT tenant_id, MAX(n) AS mx FROM {t} GROUP BY tenant_id) m \
                         ON m.tenant_id = {d}.tenant_id"
                            .into(),
                    ),
                    // The key pinned on the other side only: no transitive
                    // inference in 2b, so the partitioned side fans out.
                    3 => self.simple(
                        kind,
                        format!(
                            "SELECT {{d}}.tenant_id, {{t}}.id, {{t}}.n FROM {{d}} JOIN {{t}} ON {{t}}.tenant_id = {{d}}.tenant_id \
                             WHERE {{d}}.tenant_id = {k}"
                        ),
                    ),
                    4 => self.simple(
                        kind,
                        format!(
                            "SELECT {{t}}.tenant_id, {{t}}.id, {{d}}.label FROM {{t}} JOIN {{d}} ON {{t}}.tenant_id = {{d}}.tenant_id \
                             WHERE {{t}}.tenant_id IN (SELECT tenant_id FROM {{u}} WHERE w > {v})"
                        ),
                    ),
                    _ => self.simple(
                        kind,
                        "SELECT {d}.label, {t}.id, {t}.s FROM {d} JOIN {t} ON {t}.tenant_id = {d}.tenant_id \
                         WHERE {t}.s IS NULL OR {t}.n < 0"
                            .into(),
                    ),
                }
            }
            JoinPartitionedBoth => {
                let (k, v) = (self.tenant(), self.rng.i32(0..130));
                let sql = match self.rng.u8(..4) {
                    0 => "SELECT {t}.tenant_id, {t}.grp, {t}.id, {u}.w, {u}.tag FROM {t} JOIN {u} \
                          ON {t}.tenant_id = {u}.tenant_id AND {t}.grp = {u}.grp"
                        .to_string(),
                    1 => format!(
                        "SELECT {{t}}.tenant_id, {{t}}.id, {{u}}.w, COALESCE({{u}}.tag, 'none') FROM {{t}} LEFT JOIN {{u}} \
                         ON {{u}}.tenant_id = {{t}}.tenant_id AND {{u}}.grp = {{t}}.grp WHERE {{t}}.tenant_id = {k}"
                    ),
                    // The table under test null-supplying, the other routed.
                    2 => format!(
                        "SELECT {{u}}.tenant_id, {{u}}.grp, {{t}}.id, {{t}}.n FROM {{u}} LEFT JOIN {{t}} \
                         ON {{t}}.tenant_id = {{u}}.tenant_id AND {{t}}.grp = {{u}}.grp WHERE {{u}}.tenant_id = {k}"
                    ),
                    _ => format!(
                        "SELECT COUNT(*), SUM({{u}}.w), MAX({{t}}.n), MIN({{t}}.s) FROM {{t}} JOIN {{u}} \
                         ON {{t}}.tenant_id = {{u}}.tenant_id AND {{t}}.grp = {{u}}.grp WHERE {{u}}.w > {v}"
                    ),
                };
                self.simple(kind, sql)
            }
            JoinAggregates => match self.rng.u8(..3) {
                0 => self.simple(
                    kind,
                    "SELECT {d}.label, COUNT(*), SUM({t}.n), MIN({t}.s), MAX({t}.f) FROM {t} JOIN {d} \
                     ON {t}.tenant_id = {d}.tenant_id GROUP BY {d}.label"
                        .into(),
                ),
                1 => self.simple(
                    kind,
                    "SELECT COUNT(*), COUNT({t}.n), SUM({t}.f), AVG({t}.f) FROM {d} LEFT JOIN {t} \
                     ON {t}.tenant_id = {d}.tenant_id"
                        .into(),
                ),
                _ => self.ordered(
                    kind,
                    "SELECT {t}.tenant_id, COUNT(*), MAX({t}.id) FROM {t} JOIN {d} ON {t}.tenant_id = {d}.tenant_id \
                     GROUP BY {t}.tenant_id ORDER BY {t}.tenant_id"
                        .into(),
                ),
            },
            PreparedJoin => {
                if self.rng.bool() {
                    let runs = self.prepared_runs(|g| vec![Some(g.tenant().to_string())]);
                    self.prepared(
                        kind,
                        "SELECT {t}.id, {t}.n, {d}.label FROM {t} JOIN {d} ON {t}.tenant_id = {d}.tenant_id \
                         WHERE {t}.tenant_id = $1",
                        runs,
                    )
                } else {
                    let runs = self.prepared_runs(|g| vec![Some(g.tenant().to_string()), Some(g.id().to_string())]);
                    self.prepared(
                        kind,
                        "SELECT a.id, b.id, b.n FROM {t} a JOIN {t} b ON a.tenant_id = b.tenant_id AND a.grp = b.grp \
                         WHERE a.tenant_id = $1 AND a.id < $2",
                        runs,
                    )
                }
            }
            UpdateDimFromT => {
                // A constant label, so which source row a dimension row
                // pairs with first cannot matter.
                let w = WORDS[self.rng.usize(..4)];
                let v = self.rng.i32(-50..100);
                let sql = match self.rng.u8(..3) {
                    0 => format!(
                        "UPDATE {{d}} SET label = '{w}' FROM {{t}} WHERE {{d}}.tenant_id = {{t}}.tenant_id AND {{t}}.n > {v}"
                    ),
                    1 => format!(
                        "UPDATE {{d}} SET label = '{w}' FROM {{t}} WHERE {{d}}.tenant_id = {{t}}.tenant_id \
                         AND {{t}}.tenant_id = {} RETURNING {{d}}.tenant_id, {{d}}.label",
                        self.tenant()
                    ),
                    _ => format!(
                        "UPDATE {{d}} SET label = '{w}' FROM {{t}} x WHERE {{d}}.tenant_id = x.tenant_id AND x.s IS NULL"
                    ),
                };
                self.simple(kind, sql)
            }
            DeleteDimUsingT => {
                // Rolled back, so the dimension keeps its rows for the other
                // kinds; the read inside the block sees what it removed.
                let g = self.grp();
                let delete = if self.rng.bool() {
                    format!("DELETE FROM {{d}} USING {{t}} WHERE {{d}}.tenant_id = {{t}}.tenant_id AND {{t}}.grp = {g}")
                } else {
                    format!(
                        "DELETE FROM {{d}} USING {{t}} WHERE {{d}}.tenant_id = {{t}}.tenant_id AND {{t}}.tenant_id = {} \
                         RETURNING {{d}}.tenant_id, {{d}}.label",
                        self.tenant()
                    )
                };
                let inner = vec![
                    Op { kind, step: Step::Simple(delete), ordered: false },
                    Op { kind, step: Step::Simple("SELECT tenant_id, label FROM {d}".into()), ordered: false },
                ];
                Op { kind, step: Step::Block { inner, commit: false }, ordered: false }
            }
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
            PreparedSelect => self.learned(kind, "SELECT * FROM {t} WHERE tenant_id = $1 AND grp = $2", true, None),
            PreparedCount => self.learned(kind, "SELECT COUNT(*) FROM {t} WHERE tenant_id = $1", false, None),
            PreparedDescribeAlias => {
                if self.rng.bool() {
                    self.learned(kind, "SELECT n AS v, tenant_id AS t FROM {t} WHERE tenant_id = $1", false, Some(&["v", "t"]))
                } else {
                    // The same column twice: each item labels one column.
                    let sql = "SELECT tenant_id AS a, tenant_id AS b FROM {t} WHERE tenant_id = $1";
                    self.learned(kind, sql, false, Some(&["a", "b"]))
                }
            }
            PreparedSelectAcrossDdl => {
                let mut op = self.learned(kind, "SELECT * FROM {t} WHERE tenant_id = $1 AND grp = $2", true, None);
                if let Step::Prepared { runs, ddl_after, cached_from, .. } = &mut op.step {
                    // Learned by run 2 and used at run 2; the CREATE after it
                    // resets the parse, which runs 3 and 4 learn again, and the
                    // DROP after run 4 resets it once more. No cached-run check:
                    // which runs relearn depends on the values drawn.
                    while runs.len() < 6 {
                        let (t, g) = (self.rng.i64(1..=TENANTS), self.rng.i64(1..=GROUPS));
                        runs.push(vec![Some(t.to_string()), Some(g.to_string())]);
                    }
                    let name = format!("twin_ddl_{}", self.rng.u32(..));
                    ddl_after.push((2, format!("CREATE TABLE {name} (x INT)")));
                    ddl_after.push((4, format!("DROP TABLE {name}")));
                    *cached_from = None;
                }
                op
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
            PreparedUpdateReturning => {
                let runs = self.prepared_runs(|g| vec![Some(g.rng.i32(-3..4).to_string()), Some(g.tenant().to_string())]);
                let mut op = self.prepared(
                    kind,
                    "UPDATE {t} SET n = n + $1 WHERE tenant_id = $2 RETURNING grp, id AS v, n * 2, s",
                    runs,
                );
                if let Step::Prepared { columns, .. } = &mut op.step {
                    *columns = Some(&["grp", "v", "?column?", "s"]);
                }
                op
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
    InsertOne, InsertMulti, InsertSelect, InsertOnConflictNothing, InsertOnConflictUpdate,
    InsertOnConflictNothingReturning, InsertOnConflictUpdateWhere, InsertOnConflictUpdateReturning,
    InsertMultiOnConflict, InsertSelectOnConflict, InsertOnConflictKeyAssign, PreparedUpsert, InsertReturning, UpdateByKey, UpdateByPrefix,
    UpdateRelocating, UpdateByNonKey, UpdateByFunction, UpdateKeySameValue, UpdateReturning, UpdateReturningExpr,
    DeleteByKey, DeleteByNonKey, DeleteIn, DeleteReturning, PreparedUpdateReturning, SelectByPrefix,
    SelectFunctionWhere, CountAll,
    Aggregates, ForUpdate, SkipLocked, PreparedInsert, PreparedSelect, PreparedUpdate, InsertIntoSinkFromT,
    ReadSinkAndDim, Join, LeftJoin, JoinRouted, JoinPartitionedBoth, UpdateDimFromT,
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
    /// Rows the agreeing answers held, so a kind that only ever agreed on
    /// nothing is caught (`ANSWERING_KINDS`).
    rows: usize,
}

/// Read kinds that must agree on at least one row per run: an empty answer
/// on both sides compares nothing.
const ANSWERING_KINDS: &[Kind] =
    &[Join, LeftJoin, SelfJoin, JoinRouted, JoinFanOut, JoinPartitionedBoth, JoinAggregates, PreparedJoin];

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
    /// Committed blocks that changed rows in two or more partitions.
    cross_partition_blocks: usize,
    /// Partitioned-side executions that answered from a learned parse.
    cached_runs: usize,
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
            | InsertOnConflictNothingReturning
            | InsertOnConflictUpdateWhere
            | InsertOnConflictUpdateReturning
            | InsertMultiOnConflict
            | InsertSelectOnConflict
            | InsertOnConflictKeyAssign
            | InsertOnConflictKeyChange
            | PreparedUpsert
            | InsertOnConflictSubsetTarget
            | InsertOnConflictUpdateTwice
            | InsertReturning
            | UpdateByKey
            | UpdateByPartitionKey
            | UpdateByPrefix
            | UpdateByNonKey
            | UpdateByFunction
            | UpdateRelocating
            | UpdateKeySameValue
            | UpdateKeyChange
            | UpdateReturning
            | UpdateReturningExpr
            | UpdateFrom
            | DeleteByKey
            | DeleteByPrefix
            | DeleteByNonKey
            | DeleteByFunction
            | DeleteIn
            | DeleteReturning
            | DeleteReturningFanOut
            | DeleteUsing
            | EmptyRouteUpdateReturning
            | EmptyRouteDeleteReturning
            | PreparedInsert
            | PreparedUpdate
            | PreparedDelete
            | PreparedUpdateReturning
            | Vacuum
            | VacuumFull
            | TxCommit
            | TxRollback
            | Restart
    )
}

/// Kinds that may fail on both tables alike and still have compared
/// something: writes, which can hit a duplicate key on both, and blocks,
/// which end when one does.
fn may_fail_on_both(kind: Kind) -> bool {
    writes_t(kind) || matches!(kind, InsertIntoSinkFromT | UpdateDimViaSubquery)
}

/// Kinds that must fail, on both table kinds, with this SQLSTATE and
/// nothing written.
fn expected_error(kind: Kind) -> Option<&'static str> {
    match kind {
        InsertOnConflictSubsetTarget => Some("42P10"),
        InsertOnConflictUpdateTwice => Some("21000"),
        _ => None,
    }
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
            // Names and types both: Describe and payload share one source,
            // and a wrong type is a wrongly decoded value.
            let cols: Vec<String> = cols.iter().zip(&r.types).map(|(c, t)| format!("{c}:{t}")).collect();
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
                    self.tally.entry(kind).or_default().rows += reply_p.rows.len();
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
    /// (I4). Read below SQL, so a write that answered like the twin's but
    /// stored something else (a lost row, a duplicate, a wrong value, a
    /// misrouted row) is caught even where a read path would hide it.
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
                let expected = expected_error(op.kind);
                let before = expected.map(|_| {
                    let db = &self.server.as_ref().unwrap().db;
                    (stored_rows(db, PART), stored_rows(db, PLAIN))
                });
                let p = self.cp().simple(&sql_p);
                if let Some(code) = expected {
                    match &p {
                        Err(e) if e.code == code => {}
                        other => self.fail(at, op.kind, format!("`{sql_p}` must fail with {code}: {other:?}")),
                    }
                }
                let verdict = self.judge(at, op, &sql_p, p, true, &mut |tw: &mut Twin| tw.cq().simple(&sql_q));
                if let Some(before) = before {
                    let db = &self.server.as_ref().unwrap().db;
                    if (stored_rows(db, PART), stored_rows(db, PLAIN)) != before {
                        self.fail(at, op.kind, format!("`{sql_p}` failed and still wrote rows"));
                    }
                    if !matches!(verdict, Verdict::Agreed { failed: true }) {
                        self.fail(at, op.kind, format!("`{sql_p}` did not fail alike on both tables"));
                    }
                }
                self.note(op.kind, &verdict);
                !matches!(verdict, Verdict::Agreed { failed: false })
            }
            Step::Prepared { sql, runs, cached_from, ddl_after, columns } => {
                let sql_p = render(sql, &SIDE_PART);
                let sql_q = render(sql, &SIDE_PLAIN);
                let name = format!("tw{at}");
                self.log.push(format!("PREPARE {name} AS {sql_p}"));
                let p = self.cp().prepare(&name, &sql_p);
                if let (Some(want), Ok(reply)) = (columns, &p) {
                    if reply.columns != *want {
                        self.fail(at, op.kind, format!("`{sql_p}` described as {:?}, not {want:?}", reply.columns));
                    }
                }
                let name_q = name.clone();
                let verdict = self.judge(at, op, &sql_p, p, false, &mut |tw: &mut Twin| tw.cq().prepare(&name_q, &sql_q));
                let mut failed = !matches!(verdict, Verdict::Agreed { failed: false });
                if failed {
                    self.note(op.kind, &verdict);
                } else {
                    for (i, params) in runs.iter().enumerate() {
                        self.log.push(format!("EXECUTE {name} {params:?}"));
                        let before = PREPARED_RUNS.load(std::sync::atomic::Ordering::Relaxed);
                        let p = self.cp().execute_prepared(&name, params);
                        // Other twins in this process only add to the count, so
                        // a run that moved nothing did not use the cache.
                        let moved = PREPARED_RUNS.load(std::sync::atomic::Ordering::Relaxed) - before;
                        if cached_from.is_some_and(|from| i >= from) && p.is_ok() {
                            if moved == 0 {
                                self.fail(
                                    at,
                                    op.kind,
                                    format!("run {i} of `{sql_p}` ({params:?}) did not answer from the learned parse"),
                                );
                            }
                            self.cached_runs += 1;
                        }
                        if let (Some(want), Ok(reply)) = (columns, &p) {
                            if reply.columns != *want {
                                self.fail(at, op.kind, format!("run {i} of `{sql_p}`: columns {:?}, not {want:?}", reply.columns));
                            }
                        }
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
                        for (_, ddl) in ddl_after.iter().filter(|(after, _)| *after == i) {
                            self.log.push(ddl.clone());
                            self.cp().ok(ddl);
                        }
                    }
                }
                self.cp().close_prepared(&name);
                self.cq().close_prepared(&name);
                failed
            }
            Step::Block { inner, commit } => {
                let before = commit.then(|| by_partition(&self.server.as_ref().unwrap().db));
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
                if let (Some(before), false) = (before, aborted) {
                    let after = by_partition(&self.server.as_ref().unwrap().db);
                    let pids: BTreeSet<&u32> = before.keys().chain(after.keys()).collect();
                    let changed = pids.into_iter().filter(|pid| before.get(*pid) != after.get(*pid)).count();
                    if changed >= 2 {
                        self.cross_partition_blocks += 1;
                    }
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
                let uid = server.db.partitioned_table(PART).map(|t| t.uid());
                assert_eq!(uid, Some(self.uid), "{}: the partitioned table did not survive a restart", self.label);
                self.server = Some(server);
                self.note(op.kind, &Verdict::Agreed { failed: false });
                false
            }
        }
    }
}

/// The partitioned table's rows by partition, each partition's sorted.
fn by_partition(db: &rclite::Arc<rasterizeddb_core::core::database::Database>) -> BTreeMap<u32, Vec<StoredRow>> {
    stored_rows_by_partition(db, PART)
        .into_iter()
        .map(|(pid, mut rows)| {
            rows.sort();
            (pid, rows)
        })
        .collect()
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
        setup.ok(&format!("CREATE TABLE {} (tenant_id BIGINT NOT NULL PRIMARY KEY, label TEXT)", side.d));
        let dims: Vec<String> = (1..=TENANTS).step_by(2).map(|t| format!("({t}, 'tenant {t}')")).collect();
        setup.ok(&format!("INSERT INTO {} (tenant_id, label) VALUES {}", side.d, dims.join(", ")));
        setup.ok(&format!("CREATE TABLE {} (tenant_id BIGINT, id BIGINT, n INT)", side.s));
        // The second partitioned table (S19), read-only: most (tenant, grp)
        // pairs, NULLs in both value columns.
        let partitioning = if side.u == SIDE_PART.u { " PARTITION BY HASH (tenant_id) PARTITIONS 5" } else { "" };
        setup.ok(&format!(
            "CREATE TABLE {} (tenant_id BIGINT NOT NULL, grp BIGINT NOT NULL, w INT, tag TEXT, \
             PRIMARY KEY (tenant_id, grp)){partitioning}",
            side.u
        ));
        let pairs: Vec<String> = (1..=TENANTS)
            .flat_map(|t| (1..=GROUPS).map(move |g| (t, g)))
            .filter(|(t, g)| (t + g) % 4 != 0)
            .map(|(t, g)| {
                let w = if (t * g) % 5 == 0 { "NULL".to_string() } else { (t * 10 + g).to_string() };
                let tag = if t % 3 == 0 { "NULL".to_string() } else { format!("'tag {t}.{g}'") };
                format!("({t}, {g}, {w}, {tag})")
            })
            .collect();
        setup.ok(&format!("INSERT INTO {} (tenant_id, grp, w, tag) VALUES {}", side.u, pairs.join(", ")));
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
    let uid = server.db.partitioned_table(PART).expect("the partitioned table").uid();

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
        cross_partition_blocks: 0,
        cached_runs: 0,
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
    for kind in ANSWERING_KINDS {
        let rows = twin.tally.get(kind).map_or(0, |t| t.rows);
        assert!(rows > 0, "{label} seed {seed:#x}: {kind:?} agreed only on empty answers, so it compared nothing");
    }
    // A refused kind ran and was refused; a kind not in REFUSED was never refused.
    for (kind, t) in &twin.tally {
        if refusal(*kind).is_none() {
            assert_eq!(t.refused, 0, "{label}: {kind:?} counted a refusal while not in REFUSED");
        }
    }
    // A read that fails on both tables alike compared nothing: CountDistinct
    // did that, unnoticed, until upstream's 4301045. Only a write may (a
    // duplicate key on both); a read kind not in REFUSED must answer.
    for (kind, t) in &twin.tally {
        if refusal(*kind).is_none() && !may_fail_on_both(*kind) {
            assert_eq!(
                t.both_failed, 0,
                "{label} seed {seed:#x}: {kind:?} failed on both tables {} time(s) and agreed {}, so those runs \
                 compared nothing",
                t.both_failed, t.agreed
            );
        }
    }

    // I8's tail: the shell is still the partitioned table's, and the root
    // holds no file under the logical name.
    let server = twin.server.take().unwrap();
    assert_eq!(server.db.partitioned_table(PART).map(|t| t.uid()), Some(uid), "{label}");
    assert!(server.db.table_for_tests(PART).is_some_and(|t| t.schema.name == PART), "{label}: the shell is gone");
    assert_eq!(files_of_table(dir.path(), PART), Vec::<String>::new(), "{label}: files under the logical name");
    let bytes = dir_bytes(dir.path());
    assert!(bytes < 50 * 1024 * 1024, "{label}: the run wrote {bytes} bytes");

    let summary: Vec<String> = twin
        .tally
        .iter()
        .map(|(k, t)| {
            format!(
                "{k:?}: refused {} agreed {} both-failed {} known-diverged {} rows {}",
                t.refused, t.agreed, t.both_failed, t.diverged_known, t.rows
            )
        })
        .collect();
    assert!(twin.cached_runs > 0, "{label} seed {seed:#x}: no prepared execution answered from a learned parse");
    eprintln!(
        "{label} seed {seed:#x}: {ops} ops in {:.1}s, {} bytes, {} storage comparisons, {} committed blocks \
         across partitions, {} cached prepared runs\n  {}",
        started.elapsed().as_secs_f64(),
        bytes,
        twin.storage_checks,
        twin.cross_partition_blocks,
        twin.cached_runs,
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
