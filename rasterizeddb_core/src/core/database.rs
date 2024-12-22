use std::marker::PhantomData;

use super::storage_providers::traits::IOOperationsSync;

pub struct Database<S: IOOperationsSync> {
    io_sync: PhantomData<S>
}