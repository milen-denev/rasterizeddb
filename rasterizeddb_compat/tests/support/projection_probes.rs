//! Projections that leave a WHERE column out, checked frame by frame.
//!
//! A predicate reading a column the select list does not makes the scan fetch
//! it. When the requested columns lead the predicate's fetch in order, the
//! scan sends that fetch as it is, and the executor has to decode each row and
//! trim the predicate's columns off. Otherwise the scan re-fetches only the
//! requested columns and there is nothing to trim. The executor skips the
//! decode exactly when the scan says it did not over-fetch, so these probes
//! hold both kinds: if the scan's answer is wrong, a predicate column reaches
//! the client and a DataRow carries more fields than its RowDescription.

use crate::raw_wire::{RawConn, Shape};

/// The rows of `ov`, by `a_id`.
pub const ROWS: i64 = 60;

fn score(i: i64) -> i64 {
    (i * 7) % 100
}

pub fn create_statements() -> Vec<String> {
    // Names chosen for their order: the predicate's fetch lists its columns
    // alphabetically, so `a_id` leads it whenever the predicate reads it.
    let mut out = vec![
        "CREATE TABLE ov (a_id BIGINT NOT NULL, doc JSONB NOT NULL, score BIGINT NOT NULL, \
         z_tag TEXT NOT NULL)"
            .to_string(),
    ];
    let values: Vec<String> = (0..ROWS)
        .map(|i| {
            format!(
                "({i}, '{{\"g\": {}, \"name\": \"row{i}\", \"pad\": \"{}\"}}', {}, 'tag{}')",
                i % 3,
                "x".repeat(40),
                score(i),
                i % 4
            )
        })
        .collect();
    out.push(format!("INSERT INTO ov (a_id, doc, score, z_tag) VALUES {}", values.join(", ")));
    out
}

pub struct Probe {
    pub sql: &'static str,
    pub columns: usize,
    pub ordered: bool,
    pub expected: Vec<Vec<String>>,
}

fn ids(keep: impl Fn(i64) -> bool) -> Vec<Vec<String>> {
    (0..ROWS).filter(|i| keep(*i)).map(|i| vec![i.to_string()]).collect()
}

pub fn probes() -> Vec<Probe> {
    vec![
        // Over-fetched: `a_id` leads the predicate's fetch [a_id, score].
        Probe {
            sql: "SELECT a_id FROM ov WHERE a_id >= 10 AND score < 50",
            columns: 1,
            ordered: false,
            expected: ids(|i| i >= 10 && score(i) < 50),
        },
        // Over-fetched with the document riding behind the id.
        Probe {
            sql: "SELECT a_id FROM ov WHERE a_id < 40 AND doc @> '{\"g\": 1}'",
            columns: 1,
            ordered: false,
            expected: ids(|i| i < 40 && i % 3 == 1),
        },
        Probe {
            sql: "SELECT a_id FROM ov WHERE a_id < 30 AND doc->>'name' = 'row7'",
            columns: 1,
            ordered: false,
            expected: ids(|i| i == 7),
        },
        // Two requested columns leading [a_id, score, z_tag].
        Probe {
            sql: "SELECT a_id, score FROM ov WHERE a_id > 5 AND score > 10 AND z_tag = 'tag1'",
            columns: 2,
            ordered: false,
            expected: (0..ROWS)
                .filter(|i| *i > 5 && score(*i) > 10 && i % 4 == 1)
                .map(|i| vec![i.to_string(), score(i).to_string()])
                .collect(),
        },
        // Over-fetched, through the row-mode sort. With no LIMIT there is no
        // top-N, whose deferred payload fetch would re-read just the
        // requested columns.
        Probe {
            sql: "SELECT a_id FROM ov WHERE a_id >= 10 AND score < 50 ORDER BY a_id DESC",
            columns: 1,
            ordered: true,
            expected: {
                let mut v = ids(|i| i >= 10 && score(i) < 50);
                v.reverse();
                v
            },
        },
        Probe {
            sql: "SELECT a_id FROM ov WHERE a_id >= 10 AND score < 50 ORDER BY a_id DESC LIMIT 5",
            columns: 1,
            ordered: true,
            expected: {
                let mut v = ids(|i| i >= 10 && score(i) < 50);
                v.reverse();
                v.truncate(5);
                v
            },
        },
        // Not over-fetched: the requested columns do not lead the predicate's
        // fetch, so each matched row is re-fetched with exactly them.
        Probe {
            sql: "SELECT a_id FROM ov WHERE score < 50",
            columns: 1,
            ordered: false,
            expected: ids(|i| score(i) < 50),
        },
        Probe {
            sql: "SELECT score FROM ov WHERE a_id >= 10 AND score < 50",
            columns: 1,
            ordered: false,
            expected: (0..ROWS)
                .filter(|i| *i >= 10 && score(*i) < 50)
                .map(|i| vec![score(i).to_string()])
                .collect(),
        },
        Probe {
            sql: "SELECT z_tag FROM ov WHERE doc @> '{\"g\": 2}'",
            columns: 1,
            ordered: false,
            expected: (0..ROWS)
                .filter(|i| i % 3 == 2)
                .map(|i| vec![format!("tag{}", i % 4)])
                .collect(),
        },
        Probe {
            sql: "SELECT a_id, z_tag FROM ov WHERE a_id < 20 AND score > 30",
            columns: 2,
            ordered: false,
            expected: (0..ROWS)
                .filter(|i| *i < 20 && score(*i) > 30)
                .map(|i| vec![i.to_string(), format!("tag{}", i % 4)])
                .collect(),
        },
        Probe {
            sql: "SELECT a_id FROM ov WHERE score < 50 ORDER BY a_id LIMIT 4 OFFSET 3",
            columns: 1,
            ordered: true,
            expected: {
                let v = ids(|i| score(i) < 50);
                v[3..7].to_vec()
            },
        },
    ]
}

fn check(probe: &Probe, path: &str, got: Result<Shape, String>) {
    let shape = got.unwrap_or_else(|e| panic!("[{path}] {}: {e}", probe.sql));
    assert_eq!(
        shape.described,
        Some(probe.columns),
        "[{path}] {}: described {:?} columns",
        probe.sql,
        shape.described
    );
    for row in &shape.rows {
        assert_eq!(
            row.len(),
            probe.columns,
            "[{path}] {}: a DataRow carried {:?}, a predicate column reached the client",
            probe.sql,
            row
        );
    }
    let mut rows: Vec<Vec<String>> = shape
        .rows
        .into_iter()
        .map(|r| r.into_iter().map(|f| f.expect("no column here is NULL")).collect())
        .collect();
    let mut expected = probe.expected.clone();
    if !probe.ordered {
        let key = |r: &Vec<String>| r.iter().map(|f| format!("{f:>12}")).collect::<Vec<_>>();
        rows.sort_by_key(key);
        expected.sort_by_key(key);
    }
    assert!(!expected.is_empty(), "{}: a probe must match something", probe.sql);
    assert_eq!(rows, expected, "[{path}] {}", probe.sql);
}

/// Every probe as a simple query, then as a named statement executed
/// `executions` times, which is what moves it onto the cached parse.
pub async fn run_probes(port: u16, executions: usize) {
    let mut conn = RawConn::connect(port).await;
    for statement in create_statements() {
        conn.simple(&statement).await.unwrap_or_else(|e| panic!("{statement}: {e}"));
    }
    for (n, probe) in probes().iter().enumerate() {
        check(probe, "simple", conn.simple(probe.sql).await);
        check(probe, "simple again", conn.simple(probe.sql).await);
        let name = format!("probe{n}");
        conn.parse(&name, probe.sql)
            .await
            .unwrap_or_else(|e| panic!("parse {}: {e}", probe.sql));
        for run in 0..executions {
            check(probe, &format!("prepared run {run}"), conn.execute(&name).await);
        }
    }
}
