//! The range guard (§4.5, invariant I4): every row a partition stores lies in
//! that partition's point range, and no update moves a row's point.
//!
//! It checks the row, not the branch that writes it: an insert's final
//! `RowWrite`, and an update's merged one before any write branch (the
//! fixed-width fast path, the in-place overwrite and the relocation alike).
//! Routing being a superset (I3) is only half of "a query never misses a
//! row"; this is the other half, so a routing or reshape bug fails the
//! statement instead of storing a row no routed query looks for.
//!
//! The update rule is "the point is unchanged", not "the row stays in this
//! partition": the point depends only on the frozen hash, so every node
//! agrees, while partition membership depends on this node's map (§12).

use crate::core::row::row::RowWrite;

use super::key::KeySpec;
use super::map::PointRange;

/// What a key change is refused with. "not supported" makes it 0A000, and it
/// carries no retry marker: re-running the statement would fail the same way.
pub const KEY_CHANGE_REFUSAL: &str = "changing a partition key value is not supported yet";

/// A partition's key layout and the half-open point range it owns.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RangeGuard {
    spec: KeySpec,
    range: PointRange,
}

impl RangeGuard {
    pub fn new(spec: KeySpec, range: PointRange) -> Self {
        RangeGuard { spec, range }
    }

    pub fn spec(&self) -> &KeySpec {
        &self.spec
    }

    pub fn range(&self) -> PointRange {
        self.range
    }

    /// The routing point of the row `w` writes: its key columns, found by
    /// write order, hashed with their declared types. A key column missing
    /// from the write is an error, never a guess.
    pub fn point_of_write(&self, w: &RowWrite) -> Result<u64, String> {
        let mut columns: smallvec::SmallVec<[&[u8]; super::key::MAX_ARITY]> = smallvec::SmallVec::new();
        for level in self.spec.levels() {
            let payload = w
                .columns_writing_data
                .iter()
                .find(|c| c.write_order == level.write_order)
                .ok_or_else(|| {
                    format!(
                        "internal routing error: the row carries no value for partition key column {}",
                        level.write_order
                    )
                })?;
            columns.push(payload.data.into_slice());
        }
        self.spec.point_of(&columns)
    }

    /// Errs unless the row `w` writes has its point in this partition's
    /// range. A failure is a routing bug, so it reads as an internal error
    /// (XX000), never as something the client did.
    pub fn check_insert(&self, w: &RowWrite) -> std::io::Result<()> {
        let point = self.point_of_write(w).map_err(internal)?;
        self.check_in_range(point)
    }

    /// Errs unless `after`, an update's merged row, has the same point as
    /// `before`, the version it replaces, and that point is in range. A
    /// changed point is refused with [`KEY_CHANGE_REFUSAL`] (0A000), even
    /// when the new point would still fall in this partition.
    pub fn check_update(&self, before: &RowWrite, after: &RowWrite) -> std::io::Result<()> {
        let before = self.point_of_write(before).map_err(internal)?;
        self.check_update_from(before, after)
    }

    /// [`Self::check_update`] with the old version's point already computed,
    /// for a caller that merges the assignments into the old row in place.
    pub fn check_update_from(&self, before: u64, after: &RowWrite) -> std::io::Result<()> {
        let after = self.point_of_write(after).map_err(internal)?;
        if after != before {
            return Err(std::io::Error::new(std::io::ErrorKind::Unsupported, KEY_CHANGE_REFUSAL));
        }
        self.check_in_range(after)
    }

    fn check_in_range(&self, point: u64) -> std::io::Result<()> {
        if self.range.contains(point) {
            return Ok(());
        }
        // Worded to avoid every phrase `sqlstate_for` maps elsewhere ("out of
        // range" is 22003), so it stays XX000.
        Err(internal(format!(
            "internal routing error: a row with partition point {:#018x} reached the partition owning [{:#x}, {:#x})",
            point, self.range.lo, self.range.hi
        )))
    }
}

fn internal(message: String) -> std::io::Error {
    std::io::Error::other(message)
}

#[cfg(test)]
mod tests {
    use smallvec::SmallVec;

    use super::*;
    use crate::core::db_type::DbType;
    use crate::core::row::row::ColumnWritePayload;
    use crate::memory_pool::MEMORY_POOL;

    fn payload(bytes: &[u8], write_order: u32, column_type: DbType) -> ColumnWritePayload {
        let mut mb = MEMORY_POOL.acquire(bytes.len());
        mb.into_slice_mut().copy_from_slice(bytes);
        let size = column_type.get_size();
        ColumnWritePayload {
            data: mb,
            write_order,
            column_type,
            size,
        }
    }

    fn row(tenant: i64, label: &str) -> RowWrite {
        let mut w = RowWrite {
            columns_writing_data: SmallVec::new(),
        };
        w.columns_writing_data.push(payload(&tenant.to_le_bytes(), 0, DbType::I64));
        w.columns_writing_data.push(payload(label.as_bytes(), 1, DbType::STRING));
        w
    }

    fn spec() -> KeySpec {
        KeySpec::new(&[(0, DbType::I64)]).unwrap()
    }

    fn guard_owning(tenant: i64) -> RangeGuard {
        let p = spec().point_of(&[&tenant.to_le_bytes()]).unwrap();
        RangeGuard::new(spec(), PointRange::point(p))
    }

    #[test]
    fn the_point_is_the_key_specs_point() {
        let g = guard_owning(42);
        let expected = spec().point_of(&[&42i64.to_le_bytes()]).unwrap();
        assert_eq!(g.point_of_write(&row(42, "a")).unwrap(), expected);
        // Non-key columns do not move it.
        assert_eq!(g.point_of_write(&row(42, "something else")).unwrap(), expected);
    }

    #[test]
    fn an_insert_in_range_passes_and_one_outside_is_an_internal_error() {
        let g = guard_owning(42);
        g.check_insert(&row(42, "a")).unwrap();
        let e = g.check_insert(&row(43, "a")).unwrap_err().to_string();
        assert!(e.contains("internal routing error"), "{e}");
        assert!(!e.contains("not supported") && !e.contains("out of range"), "{e}");
    }

    #[test]
    fn a_missing_key_column_is_refused() {
        let g = guard_owning(42);
        let mut w = row(42, "a");
        w.columns_writing_data.remove(0);
        assert!(g.check_insert(&w).is_err());
        // A wrong width too: guessing would place the row nowhere.
        let mut w = RowWrite {
            columns_writing_data: SmallVec::new(),
        };
        w.columns_writing_data.push(payload(&42i32.to_le_bytes(), 0, DbType::I32));
        assert!(g.check_insert(&w).is_err());
    }

    #[test]
    fn an_update_must_keep_its_point() {
        // The whole space, so only the "unchanged" rule can refuse.
        let g = RangeGuard::new(spec(), PointRange::FULL);
        g.check_update(&row(42, "a"), &row(42, "b")).unwrap();
        let e = g.check_update(&row(42, "a"), &row(43, "a")).unwrap_err();
        assert_eq!(e.kind(), std::io::ErrorKind::Unsupported);
        assert_eq!(e.to_string(), KEY_CHANGE_REFUSAL);
    }

    #[test]
    fn an_unchanged_point_outside_the_range_is_still_refused() {
        let g = guard_owning(42);
        let e = g.check_update(&row(43, "a"), &row(43, "b")).unwrap_err().to_string();
        assert!(e.contains("internal routing error"), "{e}");
    }
}
