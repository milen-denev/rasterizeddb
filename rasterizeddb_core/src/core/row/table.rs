use cacheguard::CacheGuard;
use log::{error, info};
use itertools::Itertools;
use smallvec::SmallVec;
use std::io::Result;
use std::{
    io,
    sync::atomic::{AtomicBool, AtomicU64},
};

use rclite::Arc;

use super::schema::TableSchema;
use crate::core::processor::concurrent_processor::ConcurrentProcessor;
use crate::core::rql::lexer_ct::CreateColumnData;
use crate::core::processor::concurrent_processor::{ATOMIC_CACHE, ENABLE_CACHE};

#[cfg(not(feature = "sme_v2"))]
use crate::core::sme::scanner::{spawn_table_rules_scanner, TableRulesScannerHandle};

#[cfg(feature = "sme_v2")]
use crate::core::sme_v2::scanner::{
    spawn_table_rules_scanner_v2 as spawn_table_rules_scanner,
    TableRulesScannerHandleV2 as TableRulesScannerHandle,
};

#[cfg(not(feature = "sme_v2"))]
use crate::core::sme::semantic_mapping_engine::SME;
use crate::core::{
    row::{
        row::{Row, RowFetch, RowWrite},
        row_pointer::{RowPointer, RowPointerIterator},
        schema::SchemaField,
    },
    storage_providers::traits::StorageIO,
};

use crate::core::row::row::{ColumnFetchingData, ColumnWritePayload};
use crate::core::row::schema::SchemaCalculator;

use std::time::Duration;

pub struct Table<S: StorageIO> {
    pub schema: TableSchema,
    pub io_pointers: Arc<S>,
    pub io_rows: Arc<S>,
    pub io_schema: Arc<S>,
    pub hard_locked: CacheGuard<AtomicBool>,
    pub len: CacheGuard<AtomicU64>,
    pub last_row_id: CacheGuard<AtomicU64>,
    pub concurrent_processor: ConcurrentProcessor,

    sme_scanner: Option<TableRulesScannerHandle>,
}

unsafe impl<S: StorageIO> Send for Table<S> {}
unsafe impl<S: StorageIO> Sync for Table<S> {}

impl<S: StorageIO> Table<S> {
    #[inline]
    fn invalidate_query_caches(&self) {
        // Query-result cache (WHERE -> Vec<RowPointer>) lives across queries.
        // Since deletes/updates tombstone pointers (but don't remove row bytes),
        // cached pointers can cause deleted rows to reappear until restart.
        if ENABLE_CACHE.get().copied().unwrap_or(false) {
            if let Some(cache) = ATOMIC_CACHE.get() {
                _ = cache.clear();
            }
        }

        // SME v1 candidate cache can also go stale across mutations.
        #[cfg(not(feature = "sme_v2"))]
        {
            if let Some(engine) = SME.get() {
                _ = engine.clear_candidates();
            }
        }
    }

    pub async fn new(table_name: &str, initial_io: Arc<S>, columns: Vec<CreateColumnData>) -> Self {
        let io_pointers = Arc::new(
            initial_io
                .create_new(format!("{}_pointers.db", table_name))
                .await,
        );

        let io_rows = Arc::new(initial_io.create_new(format!("{}.db", table_name)).await);

        let io_schema = Arc::new(
            initial_io
                .create_new(format!("{}_schema.db", table_name))
                .await,
        );

        let io_pointers_clone = io_pointers.clone();
        let io_rows_clone = io_rows.clone();
        let io_schema_clone = io_schema.clone();

        tokio::spawn(async move {
            tokio::join!(io_rows_clone.start_service(), io_pointers_clone.start_service(), io_schema_clone.start_service());
        });

        let schema = {
            if let Ok(schema) = TableSchema::load(io_schema.clone()).await {
                schema
            } else {
                let mut schema = TableSchema::new(table_name.to_string(), false);

                schema
                    .save(io_schema.clone())
                    .await
                    .expect("Failed to save initial schema");

                for column in columns {
                    //TODO is_unique
                    schema
                        .add_field(io_schema.clone(), column.name, column.data_type, false)
                        .await;
                }

                schema
            }
        };

        info!("Loaded schema: {:?}", schema);

        // SME: scan table and emit semantic rules to `TABLENAME_rules.db`.
        // Scans are on-demand (triggered by INSERT/UPDATE/DELETE), not periodic.
        let sme_scanner = spawn_table_rules_scanner(
            initial_io.clone(),
            table_name.to_string(),
            schema.fields.clone(),
            io_pointers.clone(),
            io_rows.clone(),
            Duration::from_millis(0),
            true,
        );

        let mut pointer_iterator = RowPointerIterator::new(io_pointers.clone()).await.unwrap();

        let (atomic_last_id, atomic_table_length) =
            if let Some(row_pointer) = pointer_iterator.read_last().await {
                let last_id = row_pointer.id;
                let atomic_last_id = AtomicU64::new(last_id);

                let table_length = row_pointer.position;
                let atomic_table_length = AtomicU64::new(table_length);

                (atomic_last_id, atomic_table_length)
            } else {
                let atomic_last_id = AtomicU64::new(0);
                let atomic_table_length = AtomicU64::new(0);
                (atomic_last_id, atomic_table_length)
            };

        Self {
            schema,
            io_pointers,
            io_rows,
            io_schema,
            hard_locked: AtomicBool::new(false).into(),
            len: atomic_table_length.into(),
            last_row_id: atomic_last_id.into(),
            concurrent_processor: ConcurrentProcessor::new(),
            sme_scanner: Some(sme_scanner),
        }
    }

    pub async fn insert_row(&self, row_write: RowWrite) -> Result<()> {
        // Implementation for inserting a row into the table

        info!(
            "Inserting row with {} columns",
            row_write.columns_writing_data.len()
        );

        // Ensure SME v1 never uses stale rules across mutations.
        #[cfg(not(feature = "sme_v2"))]
        {
            if let Some(engine) = SME.get() {
                engine.mark_table_rules_dirty(&self.schema.name);
                _ = engine.remove_table_rules_for_table(&self.schema.name);
            }
        }

        let result = RowPointer::write_row_with_pointer_record_pos(
            self.io_pointers.clone(),
            self.io_rows.clone(),
            &self.last_row_id,
            &self.len,
            #[cfg(feature = "enable_long_row")]
            cluster,
            &row_write,
        )
        .await;

        if let Err(e) = result {
            error!("Failed to insert row: {}", e);
            #[cfg(not(feature = "sme_v2"))]
            {
                if let Some(engine) = SME.get() {
                    engine.clear_table_rules_dirty(&self.schema.name);
                }
            }
            return Err(io::Error::new(
                io::ErrorKind::Other,
                format!("Failed to insert row: {}", e),
            ));
        } else if let Ok((pointer_record_pos, row_pointer)) = result {
            info!("Successfully inserted row with pointer: {:?}", row_pointer);

            #[cfg(feature = "sme_v2")]
            {
                crate::core::sme_v2::dirty_row_tracker::dirty_row_tracker()
                    .mark_pointer_record_pos(&self.schema.name, pointer_record_pos);
            }
        }

        self.invalidate_query_caches();

        // Trigger an async rules rebuild for this table.
        if let Some(handle) = &self.sme_scanner {
            handle.request_scan();
        }

        return Ok(());
    }

    pub async fn query_row(
        &self,
        query: &str,
        schema_fields: &Vec<SchemaField>,
        query_row_fetch: RowFetch,
        requested_row_fetch: RowFetch,
        limit: Option<u64>,
        order_by: Option<String>,
    ) -> Result<Vec<Row>> {
        // Implementation for querying a row from the table

        info!("Querying row");

        let mut iterator = RowPointerIterator::new(self.io_pointers.clone())
            .await
            .unwrap();

        let rows = self
            .concurrent_processor
            .process(
                &self.schema.name,
                query,
                query_row_fetch,
                requested_row_fetch,
                schema_fields,
                self.io_rows.clone(),
                &mut iterator,
                limit,
                order_by,
            )
            .await;

        info!("Query executed, returning {} rows", rows.len());

        return Ok(rows);
    }

    fn build_full_row_fetch(schema_fields: &SmallVec<[SchemaField; 20]>) -> RowFetch {
        let schema_calc = SchemaCalculator::default();
        let mut columns_fetching_data = smallvec::SmallVec::new();
        for field in schema_fields
            .iter()
            .sorted_by(|a, b| a.write_order.cmp(&b.write_order))
        {
            let offset = schema_calc.calculate_schema_offset(&field.name, schema_fields);
            columns_fetching_data.push(ColumnFetchingData {
                column_offset: offset.0,
                column_type: field.db_type.clone(),
                size: field.size,
                schema_id: offset.1,
            });
        }
        RowFetch {
            columns_fetching_data,
        }
    }

    pub async fn delete_rows(
        &self,
        where_clause: &str,
        query_row_fetch: RowFetch,
        limit: Option<u64>,
    ) -> Result<u64> {
        info!("Deleting rows");

        let mut iterator = RowPointerIterator::new(self.io_pointers.clone())
            .await
            .unwrap();

        let matches = self
            .concurrent_processor
            .process_row_pointers(
                &self.schema.name,
                where_clause,
                query_row_fetch,
                &self.schema.fields,
                self.io_rows.clone(),
                &mut iterator,
                limit,
            )
            .await;

        let mut deleted_count: u64 = 0;
        let mut marked_dirty = false;
        for mut pwo in matches.into_iter() {
            if pwo.pointer.deleted {
                continue;
            }

            if let Err(e) = pwo
                .pointer
                .delete(self.io_pointers.clone(), pwo.pointer_record_pos)
                .await
            {
                error!("Failed to delete row pointer {:?}: {}", pwo.pointer, e);
                continue;
            }

            #[cfg(feature = "sme_v2")]
            {
                crate::core::sme_v2::dirty_row_tracker::dirty_row_tracker()
                    .mark_pointer_record_pos(&self.schema.name, pwo.pointer_record_pos);
            }

            if !marked_dirty {
                #[cfg(not(feature = "sme_v2"))]
                {
                    if let Some(engine) = SME.get() {
                        engine.mark_table_rules_dirty(&self.schema.name);
                        _ = engine.remove_table_rules_for_table(&self.schema.name);
                    }
                }
                marked_dirty = true;
            }

            deleted_count = deleted_count.saturating_add(1);
        }

        if deleted_count > 0 {
            self.invalidate_query_caches();

            if let Some(handle) = &self.sme_scanner {
                handle.request_scan();
            }
        }

        Ok(deleted_count)
    }

    pub async fn update_rows(
        &self,
        where_clause: &str,
        query_row_fetch: RowFetch,
        updates: smallvec::SmallVec<[ColumnWritePayload; 32]>,
        limit: Option<u64>,
    ) -> Result<u64> {
        info!("Updating rows");

        let mut iterator = RowPointerIterator::new(self.io_pointers.clone())
            .await
            .unwrap();

        let matches = self
            .concurrent_processor
            .process_row_pointers(
                &self.schema.name,
                where_clause,
                query_row_fetch,
                &self.schema.fields,
                self.io_rows.clone(),
                &mut iterator,
                limit,
            )
            .await;

        let full_fetch = Self::build_full_row_fetch(&self.schema.fields);

        let mut updated_count: u64 = 0;
        let mut row = Row::default();

        let mut marked_dirty = false;

        for pwo in matches.into_iter() {
            if pwo.pointer.deleted {
                continue;
            }

            // Load full row so we can merge updates.
            if pwo
                .pointer
                .fetch_row_reuse_async(self.io_rows.clone(), &full_fetch, &mut row)
                .await
                .is_err()
            {
                continue;
            }

            // Build RowWrite from the existing row (all columns), keyed by write_order.
            let mut row_write = RowWrite {
                columns_writing_data: smallvec::SmallVec::new(),
            };

            for field in self
                .schema
                .fields
                .iter()
                .sorted_by(|a, b| a.write_order.cmp(&b.write_order))
            {
                let write_order = field.write_order as u32;
                let col = row
                    .columns
                    .iter()
                    .find(|c| c.schema_id as u32 == write_order);

                if let Some(col) = col {
                    row_write.columns_writing_data.push(ColumnWritePayload {
                        data: col.data.clone(),
                        write_order,
                        column_type: field.db_type.clone(),
                        size: field.size,
                    });
                } else {
                    // Missing columns shouldn't normally happen for a full fetch; treat as empty.
                    row_write.columns_writing_data.push(ColumnWritePayload {
                        data: crate::memory_pool::MemoryBlock::default(),
                        write_order,
                        column_type: field.db_type.clone(),
                        size: field.size,
                    });
                }
            }

            // Apply updates in-place in the RowWrite payloads.
            for upd in updates.iter() {
                if let Some(existing) = row_write
                    .columns_writing_data
                    .iter_mut()
                    .find(|c| c.write_order == upd.write_order)
                {
                    existing.data = upd.data.clone();
                    existing.column_type = upd.column_type.clone();
                    existing.size = upd.size;
                }
            }

            let mut pointer = pwo.pointer.clone();

            let update_result = pointer
                .update_with_pointer_record_pos(
                    self.io_pointers.clone(),
                    self.io_rows.clone(),
                    pwo.pointer_record_pos,
                    &self.last_row_id,
                    &self.len,
                    &row_write,
                )
                .await;

            if update_result.is_ok() {
                #[cfg(feature = "sme_v2")]
                {
                    let (updated_pointer, new_pointer_pos) = update_result.as_ref().unwrap();
                    let _ = updated_pointer;

                    let tracker = crate::core::sme_v2::dirty_row_tracker::dirty_row_tracker();
                    tracker.mark_pointer_record_pos(&self.schema.name, pwo.pointer_record_pos);
                    if let Some(pos) = *new_pointer_pos {
                        tracker.mark_pointer_record_pos(&self.schema.name, pos);
                    }
                }

                if !marked_dirty {
                    #[cfg(not(feature = "sme_v2"))]
                    {
                        if let Some(engine) = SME.get() {
                            engine.mark_table_rules_dirty(&self.schema.name);
                            _ = engine.remove_table_rules_for_table(&self.schema.name);
                        }
                    }
                    marked_dirty = true;
                }
                updated_count = updated_count.saturating_add(1);
            }
        }

        if updated_count > 0 {
            self.invalidate_query_caches();

            if let Some(handle) = &self.sme_scanner {
                handle.request_scan();
            }
        }

        Ok(updated_count)
    }
}
