use std::marker::PhantomData;

use super::file_handlers::{IOOperationsAsync, IOOperationsSync};

pub struct Database<'a, S: IOOperationsSync, A: IOOperationsAsync<'a>> {
    io_sync: PhantomData<S>,
    io_async: &'a PhantomData<A>,
}