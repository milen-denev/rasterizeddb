use rastdp::receiver::Receiver;

use super::{storage_providers::traits::IOOperationsSync, table::Table};

pub struct Database<S: IOOperationsSync> {
    config_table: Table<S>,
    tables: Vec<Table<S>>,
    server: Receiver
}

impl<S: IOOperationsSync> Database<S> {
    pub async fn new() {

    }
}