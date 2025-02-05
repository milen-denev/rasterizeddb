use std::arch::asm;

#[inline(always)]
pub unsafe fn read_u64(ptr: *const u8) -> u64 {
    #[cfg(target_arch = "x86_64")]
    {
        {
            let val: u64;
            asm!(
                "mov {0}, qword ptr [{1}]",
                out(reg) val,
                in(reg) ptr,
                options(nostack, pure, readonly)
            );
            return val;
        }
    }

    #[cfg(not(target_arch = "x86_64"))]
    {
        // Portable fallback for non-x86_64 CPUs
        core::ptr::read_unaligned(ptr as *const u64)
    }
}

#[inline(always)]
pub unsafe fn read_u32(ptr: *const u8) -> u32 {
    #[cfg(target_arch = "x86_64")]
    {
        let val: u32;
        asm!(
            "mov {0:e}, dword ptr [{1}]",
            out(reg) val,
            in(reg) ptr,
            options(nostack, pure, readonly)
        );
        return val;
    }

    #[cfg(not(target_arch = "x86_64"))]
    {
        // Portable fallback for non-x86_64 CPUs
        core::ptr::read_unaligned(ptr as *const u32)
    }
}

#[inline(always)]
pub unsafe fn read_u8(ptr: *const u8) -> u8 {
    #[cfg(target_arch = "x86_64")]
    {
        let val: u8;
        asm!(
            "mov {0}, byte ptr [{1}]",
            out(reg_byte) val,
            in(reg) ptr,
            options(nostack, pure, readonly)
        );
        return val;
    }

    #[cfg(not(target_arch = "x86_64"))]
    {
        // Portable fallback
        core::ptr::read(ptr)
    }
}
