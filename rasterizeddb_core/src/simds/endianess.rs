use std::arch::{asm, x86_64::__m128i};
/// Reads an 8‑bit signed integer from memory.
#[inline(always)]
pub unsafe fn read_i8(ptr: *const u8) -> i8 {
    #[cfg(target_arch = "x86_64")]
    {
        let val: i8;
        unsafe {
            asm!(
                "mov {0}, byte ptr [{1}]",
                out(reg_byte) val,
                in(reg) ptr,
                options(nostack, pure, readonly)
            );
        }
        return val;
    }
    #[cfg(not(target_arch = "x86_64"))]
    {
        core::ptr::read_unaligned(ptr as *const i8)
    }
}

/// Reads an 8‑bit unsigned integer from memory.
#[inline(always)]
pub unsafe fn read_u8(ptr: *const u8) -> u8 {
    #[cfg(target_arch = "x86_64")]
    {
        let val: u8;
        unsafe {
            asm!(
                "mov {0}, byte ptr [{1}]",
                out(reg_byte) val,
                in(reg) ptr,
                options(nostack, pure, readonly)
            );
        }
        return val;
    }
    #[cfg(not(target_arch = "x86_64"))]
    {
        core::ptr::read_unaligned(ptr as *const u8)
    }
}

/// Reads a 16‑bit signed integer from memory.
#[inline(always)]
pub unsafe fn read_i16(ptr: *const u8) -> i16 {
    #[cfg(target_arch = "x86_64")]
    {
        let val: i16;
        unsafe {
            asm!(
                "mov {0:x}, word ptr [{1}]",
                out(reg) val,
                in(reg) ptr,
                options(nostack, pure, readonly)
            );
        }
        return val;
    }
    #[cfg(not(target_arch = "x86_64"))]
    {
        core::ptr::read_unaligned(ptr as *const i16)
    }
}

/// Reads a 16‑bit unsigned integer from memory.
#[inline(always)]
pub unsafe fn read_u16(ptr: *const u8) -> u16 {
    #[cfg(target_arch = "x86_64")]
    {
        let val: u16;
        unsafe {
            asm!(
                "mov {0:x}, word ptr [{1}]",
                out(reg) val,
                in(reg) ptr,
                options(nostack, pure, readonly)
            );
        }
        return val;
    }
    #[cfg(not(target_arch = "x86_64"))]
    {
        core::ptr::read_unaligned(ptr as *const u16)
    }
}

/// Reads a 32‑bit signed integer from memory.
#[inline(always)]
pub unsafe fn read_i32(ptr: *const u8) -> i32 {
    #[cfg(target_arch = "x86_64")]
    {
        let val: i32;
        unsafe {
            asm!(
                "mov {0:e}, dword ptr [{1}]",
                out(reg) val,
                in(reg) ptr,
                options(nostack, pure, readonly)
            );
        }
        return val;
    }
    #[cfg(not(target_arch = "x86_64"))]
    {
        core::ptr::read_unaligned(ptr as *const i32)
    }
}

/// Reads a 32‑bit unsigned integer from memory.
#[inline(always)]
pub unsafe fn read_u32(ptr: *const u8) -> u32 {
    #[cfg(target_arch = "x86_64")]
    {
        let val: u32;
        unsafe {
            asm!(
                "mov {0:e}, dword ptr [{1}]",
                out(reg) val,
                in(reg) ptr,
                options(nostack, pure, readonly)
            );
        }
        return val;
    }
    #[cfg(not(target_arch = "x86_64"))]
    {
        core::ptr::read_unaligned(ptr as *const u32)
    }
}

/// Reads a 64‑bit signed integer from memory.
#[inline(always)]
pub unsafe fn read_i64(ptr: *const u8) -> i64 {
    #[cfg(target_arch = "x86_64")]
    {
        let val: i64;
        unsafe {
            asm!(
                "mov {0}, qword ptr [{1}]",
                out(reg) val,
                in(reg) ptr,
                options(nostack, pure, readonly)
            );
        }
        return val;
    }
    #[cfg(not(target_arch = "x86_64"))]
    {
        core::ptr::read_unaligned(ptr as *const i64)
    }
}

/// Reads a 64‑bit unsigned integer from memory.
#[inline(always)]
pub unsafe fn read_u64(ptr: *const u8) -> u64 {
    #[cfg(target_arch = "x86_64")]
    {
        let val: u64;
        unsafe {
            asm!(
                "mov {0}, qword ptr [{1}]",
                out(reg) val,
                in(reg) ptr,
                options(nostack, pure, readonly)
            );
        }
        return val;
    }
    #[cfg(not(target_arch = "x86_64"))]
    {
        core::ptr::read_unaligned(ptr as *const u64)
    }
}

/// Reads a 128‑bit signed integer from memory.
/// Uses `__m128i` as an intermediate then transmutes to `i128`.
#[inline(always)]
pub unsafe fn read_i128(ptr: *const u8) -> i128 {
    #[cfg(target_arch = "x86_64")]
    {
        let val: __m128i;
        unsafe {
            asm!(
                "movdqu {0}, xmmword ptr [{1}]",
                out(xmm_reg) val,
                in(reg) ptr,
                options(nostack, pure, readonly)
            );
        }
        unsafe { core::mem::transmute(val) }
    }
    #[cfg(not(target_arch = "x86_64"))]
    {
        core::ptr::read_unaligned(ptr as *const i128)
    }
}

/// Reads a 128‑bit unsigned integer from memory.
/// Uses `__m128i` as an intermediate then transmutes to `u128`.
#[inline(always)]
pub unsafe fn read_u128(ptr: *const u8) -> u128 {
    #[cfg(target_arch = "x86_64")]
    {
        let val: __m128i;
        unsafe {
            asm!(
                "movdqu {0}, xmmword ptr [{1}]",
                out(xmm_reg) val,
                in(reg) ptr,
                options(nostack, pure, readonly)
            );
        }
        unsafe { core::mem::transmute(val) }
    }
    #[cfg(not(target_arch = "x86_64"))]
    {
        core::ptr::read_unaligned(ptr as *const u128)
    }
}

/// Reads a 32‑bit floating‑point value from memory using the movss instruction.
#[inline(always)]
pub unsafe fn read_f32(ptr: *const u8) -> f32 {
    #[cfg(target_arch = "x86_64")]
    {
        let val: f32;
        unsafe {
            asm!(
                "movss {0}, dword ptr [{1}]",
                out(xmm_reg) val,
                in(reg) ptr,
                options(nostack, pure, readonly)
            );
        }
        return val;
    }
    #[cfg(not(target_arch = "x86_64"))]
    {
        core::ptr::read_unaligned(ptr as *const f32)
    }
}

/// Reads a 64‑bit floating‑point value from memory using the movsd instruction.
#[inline(always)]
pub unsafe fn read_f64(ptr: *const u8) -> f64 {
    #[cfg(target_arch = "x86_64")]
    {
        let val: f64;
        unsafe {
            asm!(
                "movsd {0}, qword ptr [{1}]",
                out(xmm_reg) val,
                in(reg) ptr,
                options(nostack, pure, readonly)
            );
        }
        return val;
    }
    #[cfg(not(target_arch = "x86_64"))]
    {
        core::ptr::read_unaligned(ptr as *const f64)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_i8() {
        let data: [u8; 1] = [0x7F]; // 127 in i8
        unsafe {
            assert_eq!(read_i8(data.as_ptr()), 127);
        }
    }

    #[test]
    fn test_u8() {
        let data: [u8; 1] = [0xFF]; // 255 in u8
        unsafe {
            assert_eq!(read_u8(data.as_ptr()), 255);
        }
    }

    #[test]
    fn test_i16() {
        // 0x7FFF in little-endian is [0xFF, 0x7F]
        let data: [u8; 2] = [0xFF, 0x7F];
        unsafe {
            assert_eq!(read_i16(data.as_ptr()), 0x7FFF);
        }
    }

    #[test]
    fn test_u16() {
        // 0xFFFF in little-endian is [0xFF, 0xFF]
        let data: [u8; 2] = [0xFF, 0xFF];
        unsafe {
            assert_eq!(read_u16(data.as_ptr()), 0xFFFF);
        }
    }

    #[test]
    fn test_i32() {
        // 0x7FFFFFFF in little-endian is [0xFF, 0xFF, 0xFF, 0x7F]
        let data: [u8; 4] = [0xFF, 0xFF, 0xFF, 0x7F];
        unsafe {
            assert_eq!(read_i32(data.as_ptr()), 0x7FFFFFFF);
        }
    }

    #[test]
    fn test_u32() {
        // 0xFFFFFFFF in little-endian is [0xFF, 0xFF, 0xFF, 0xFF]
        let data: [u8; 4] = [0xFF, 0xFF, 0xFF, 0xFF];
        unsafe {
            assert_eq!(read_u32(data.as_ptr()), 0xFFFFFFFF);
        }
    }

    #[test]
    fn test_i64() {
        // 0x7FFFFFFFFFFFFFFF in little-endian:
        let data: [u8; 8] = [0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x7F];
        unsafe {
            assert_eq!(read_i64(data.as_ptr()), 0x7FFFFFFFFFFFFFFF);
        }
    }

    #[test]
    fn test_u64() {
        // 0xFFFFFFFFFFFFFFFF in little-endian:
        let data: [u8; 8] = [0xFF; 8];
        unsafe {
            assert_eq!(read_u64(data.as_ptr()), 0xFFFFFFFFFFFFFFFF);
        }
    }

    #[test]
    fn test_i128() {
        // Use a known 128-bit value.
        let value: i128 = 0x7FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF;
        let bytes = value.to_le_bytes();
        unsafe {
            assert_eq!(read_i128(bytes.as_ptr()), value);
        }
    }

    #[test]
    fn test_u128() {
        // Use a known 128-bit value.
        let value: u128 = 0xFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF;
        let bytes = value.to_le_bytes();
        unsafe {
            assert_eq!(read_u128(bytes.as_ptr()), value);
        }
    }

    #[test]
    fn test_f32() {
        let value: f32 = 3.1415927;
        let bytes = value.to_le_bytes();
        unsafe {
            let read_val = read_f32(bytes.as_ptr());
            assert_eq!(read_val.to_bits(), value.to_bits());
        }
    }

    #[test]
    fn test_f64() {
        let value: f64 = 3.141592653589793;
        let bytes = value.to_le_bytes();
        unsafe {
            let read_val = read_f64(bytes.as_ptr());
            assert_eq!(read_val.to_bits(), value.to_bits());
        }
    }
}
