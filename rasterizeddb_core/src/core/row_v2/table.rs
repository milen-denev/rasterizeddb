use std::sync::atomic::AtomicBool;

use crate::core::storage_providers::traits::StorageIO;

use super::schema::TableSchema;

pub struct Table<S: StorageIO> {
    pub schema: TableSchema,
    pub io: Box<S>,
    pub hard_locked: AtomicBool
}

unsafe impl<S: StorageIO> Send for Table<S> {}
unsafe impl<S: StorageIO> Sync for Table<S> {}

impl<S: StorageIO> Table<S> {
    pub fn new(schema: TableSchema, io: Box<S>) -> Self {
        Self {
            schema,
            io,
            hard_locked: AtomicBool::new(false)
        }
    }

    //pub fn executute_query(&self, )
}