//! `native_retag` rewrites a native answer cell by cell in place of decoding
//! every row into a `Row` and serializing it again. The first version is
//! kept as the oracle (`native_retag_by_rows`): over rows of every stored
//! type the retag reads, under every declared type it acts on, with scales
//! and without, both must give the same bytes.

use super::{native_retag, native_retag_by_rows};
use crate::core::db_type::DbType;
use crate::core::row::row::{Column, Row};

fn block(bytes: &[u8]) -> crate::memory_pool::MemoryBlock {
    let mut b = crate::memory_pool::MEMORY_POOL.acquire(bytes.len());
    if !bytes.is_empty() {
        b.into_slice_mut().copy_from_slice(bytes);
    }
    b
}

fn answer(rows: &[Vec<(DbType, Vec<u8>)>]) -> Vec<u8> {
    let mut out = vec![2u8];
    for cells in rows {
        let row = Row {
            position: 0,
            columns: cells
                .iter()
                .enumerate()
                .map(|(at, (t, data))| Column { schema_id: at as u32 + 3, data: block(data), column_type: t.clone() })
                .collect(),
            length: 0,
        };
        row.extend_serialized(&mut out);
    }
    out
}

/// A stored cell of each kind the retag can meet, with its bytes.
fn stored(kind: usize, seed: u64) -> (DbType, Vec<u8>) {
    match kind % 8 {
        0 => (DbType::DECIMAL, ((seed as i128 - 500) * 1_000_000_000_000).to_le_bytes().to_vec()),
        1 => (DbType::DECIMAL, (seed as i128 * 7 + 3).to_le_bytes().to_vec()),
        2 => (DbType::DECIMAL, vec![1, 2, 3]),
        3 => (DbType::STRING, format!("text {seed}").into_bytes()),
        4 => (
            DbType::JSONB,
            crate::core::jsonb::encode_text(&format!("{{\"a\": {seed}, \"b\": [1, \"x\", null]}}")).expect("json"),
        ),
        5 => (DbType::DATETIME, (seed as i64 * 86_400_000).to_le_bytes().to_vec()),
        6 => (DbType::I64, (seed as i64).to_le_bytes().to_vec()),
        _ => (DbType::NULL, Vec::new()),
    }
}

fn declared(kind: usize) -> Option<DbType> {
    match kind % 8 {
        0 => Some(DbType::JSONB),
        1 => Some(DbType::JSONTEXT),
        2 => Some(DbType::BYTEA),
        3 => Some(DbType::ARRAY),
        4 => Some(DbType::TIMESTAMP),
        5 => Some(DbType::DECIMAL),
        6 => Some(DbType::STRING),
        _ => None,
    }
}

#[test]
fn the_cell_rewrite_answers_as_the_row_rewrite() {
    let mut checked = 0;
    for width in 1..=4usize {
        for layout in 0..64usize {
            let kinds: Vec<usize> = (0..width).map(|c| (layout >> (c * 2)) + c * 3 + layout).collect();
            for decl in 0..8usize {
                let fields: Vec<(String, DbType)> = (0..width)
                    .filter_map(|c| declared(decl + c).map(|t| (format!("c{c}"), t)))
                    .collect();
                let scales: Vec<u32> = (0..width).map(|c| ((decl + c) % 5) as u32).collect();
                for rows in [1usize, 3] {
                    let body: Vec<Vec<(DbType, Vec<u8>)>> = (0..rows)
                        .map(|r| kinds.iter().map(|k| stored(*k, (r * 31 + layout) as u64)).collect())
                        .collect();
                    let a = answer(&body);
                    for (f, s) in [
                        (Some(fields.as_slice()), Some(scales.as_slice())),
                        (Some(fields.as_slice()), None),
                        (None, None),
                    ] {
                        assert_eq!(
                            native_retag(a.clone(), f, s),
                            native_retag_by_rows(a.clone(), f, s),
                            "width {width} layout {layout} decl {decl} rows {rows}"
                        );
                        checked += 1;
                    }
                }
            }
        }
    }
    assert!(checked > 5000);
}

#[test]
fn answers_it_does_not_read_are_left_alone() {
    for answer in [vec![], vec![0u8, 1, 2], vec![3u8, b'x'], vec![2u8], vec![2u8, 9, 0, 0, 0, 1]] {
        let fields = [("a".to_string(), DbType::JSONB)];
        assert_eq!(native_retag(answer.clone(), Some(&fields), None), answer);
        assert_eq!(native_retag(answer.clone(), Some(&fields), None), native_retag_by_rows(answer, Some(&fields), None));
    }
}
