pub mod backend;
pub mod file_sync;
pub mod helpers;
#[cfg(feature = "kv_storage")]
pub mod kv;
pub mod memory;
pub mod mock_file_sync;
pub mod traits;

use crc::{CRC_32_ISO_HDLC, Crc};

pub const CRC: Crc<u32> = Crc::<u32>::new(&CRC_32_ISO_HDLC);
