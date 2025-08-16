pub mod file_sync;
pub mod memory;
pub mod traits;

pub mod mock_file_sync;

#[cfg(unix)]
pub mod io_uring_reader;

use crc::{Crc, CRC_32_ISO_HDLC};

pub const CRC: Crc::<u32>  = Crc::<u32>::new(&CRC_32_ISO_HDLC);