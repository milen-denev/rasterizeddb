use crate::{
    core::{db_type::DbType, row::row_pointer::RowPointer},
    memory_pool::MemoryBlock,
};

use rclite::Arc;
use smallvec::SmallVec;

use crate::core::{
    row::{
        error::{Result, RowError},
        row::{ColumnFetchingData, Row, RowFetch},
        row_pointer::RowPointerIterator,
        schema::SchemaField,
    },
    storage_providers::traits::StorageIO,
};

#[derive(Clone)]
pub struct ColumnData {
    pub data: MemoryBlock,
    pub field_info: FieldInfo,
}

#[derive(Clone)]
pub struct FieldInfo {
    pub schema_id: u64,
    pub db_type: DbType,
    pub fetch_idx: usize,
    pub row_pointer: RowPointer
}

pub type ColumnDataOwnedBatch = SmallVec<[ColumnData; 32]>;

/// Reads row pointers from `pointers_io` and fetches a single column from `rows_io`, yielding
/// batches of `ColumnData`.
///
/// This is intended for scans that want to stream a single column efficiently.
pub struct ColumnDataIterator<S: StorageIO> {
    pointers: RowPointerIterator<S>,
    rows_io: Arc<S>,

    schema_id: u64,
    db_type: DbType,
    fetch_idx: usize,
    batch_size: usize,

    row_fetch: RowFetch,
    row_reuse: Row,
}

impl<S: StorageIO> ColumnDataIterator<S> {
    /// Creates an iterator for a single schema field (identified by `schema_id`).
    ///
    /// `schema_id` is expected to match the field's `write_order` (this is how `SchemaCalculator`
    /// assigns column schema IDs throughout the codebase).
    pub async fn new(
        schema: &[SchemaField],
        schema_id: u64,
        pointers_io: Arc<S>,
        rows_io: Arc<S>,
        batch_size: usize,
    ) -> Result<Self> {
        let (offset, field) = find_offset_and_field(schema, schema_id).ok_or_else(|| {
            RowError::InvalidColumn(format!("Unknown schema_id {}", schema_id))
        })?;

        let pointers = RowPointerIterator::new(Arc::clone(&pointers_io)).await?;

        let row_fetch = RowFetch {
            columns_fetching_data: smallvec::smallvec![ColumnFetchingData {
                column_offset: offset,
                column_type: field.db_type.clone(),
                size: field.size,
                schema_id,
            }],
        };

        Ok(Self {
            pointers,
            rows_io,
            schema_id,
            db_type: field.db_type.clone(),
            fetch_idx: 0,
            batch_size,
            row_fetch,
            row_reuse: Row::default(),
        })
    }

    /// Returns the next batch of column values (up to `batch_size`).
    ///
    /// Returns `Ok(None)` when the underlying row-pointer stream is exhausted.
    pub async fn next(&mut self) -> Result<Option<ColumnDataOwnedBatch>> {
        if self.batch_size == 0 {
            return Ok(None);
        }

        let mut out = ColumnDataOwnedBatch::new();

        while out.len() < self.batch_size {
            let Some(row_pointer) = self.pointers.next_row_pointer().await? else {
                break;
            };

            // Read the requested column from the row.
            row_pointer
                .fetch_row_reuse_async(Arc::clone(&self.rows_io), &self.row_fetch, &mut self.row_reuse)
                .await?;

            // We fetch exactly one column, so it should be at index 0.
            // Deep-clone the MemoryBlock because `row_reuse` will be reused/overwritten.
            let data = self.row_reuse.columns[self.fetch_idx].data.deep_clone();

            out.push(ColumnData {
                data,
                field_info: FieldInfo {
                    schema_id: self.schema_id,
                    db_type: self.db_type.clone(),
                    fetch_idx: self.fetch_idx,
                    row_pointer,
                },
            });
        }

        if out.is_empty() {
            Ok(None)
        } else {
            Ok(Some(out))
        }
    }
}

fn find_offset_and_field<'a>(schema: &'a [SchemaField], schema_id: u64) -> Option<(u32, &'a SchemaField)> {
    let mut fields: Vec<&SchemaField> = schema.iter().collect();
    fields.sort_by_key(|f| f.write_order);

    let mut offset: u32 = 0;
    for f in fields {
        if f.write_order == schema_id {
            return Some((offset, f));
        }
        offset = offset.saturating_add(f.size);
    }
    None
}