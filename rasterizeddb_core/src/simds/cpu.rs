//! Which instruction set the hand-written SIMD kernels run on.
//!
//! # Why the features are read with `cpuid` rather than `is_x86_feature_detected!`
//!
//! The crate is built with `-C target-cpu=x86-64-v3` (`.cargo/config.toml`),
//! and under that flag `is_x86_feature_detected!("avx2")` is folded to a
//! constant `true` at compile time: the macro trusts the build, so it can
//! never report that the build is wrong for the machine it landed on. The
//! startup check exists for exactly that case, so it asks the CPU directly.
//! AVX-512 is outside the baseline and is not folded either way, but it is
//! read the same way so every feature in the startup line has one source.
//!
//! The OS has to have enabled the register state too, which `cpuid` alone does
//! not say: `XGETBV` reads `XCR0`, and a kernel that does not save the ZMM
//! registers on a context switch makes AVX-512 unusable whatever the CPU
//! supports.
//!
//! # The levels
//!
//! [`SimdLevel::Avx512`] requires F, BW, VL and DQ together: the byte and
//! word compares need BW, and every Intel part with AVX-512 since Skylake-SP
//! has all four. [`SimdLevel::Avx2`] is the baseline every build already
//! assumes, and the level the production server (an i5-10400) runs, since it
//! has no AVX-512 at all. [`SimdLevel::Scalar`] is the reference the other two
//! are tested against. In this build the compiler still vectorizes it with AVX2
//! where it can, so "scalar" means "not hand-written", not "no vector
//! instructions".
//!
//! [`init`] fixes the level once at startup; [`level`] reads it. A process that
//! never calls `init` (the tests, the examples) gets the best level the CPU
//! supports.

use std::sync::OnceLock;
use std::sync::atomic::{AtomicU8, Ordering};

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum SimdLevel {
    Scalar = 0,
    Avx2 = 1,
    Avx512 = 2,
}

impl SimdLevel {
    pub fn name(self) -> &'static str {
        match self {
            SimdLevel::Scalar => "scalar",
            SimdLevel::Avx2 => "avx2",
            SimdLevel::Avx512 => "avx512",
        }
    }

    fn from_u8(v: u8) -> Option<Self> {
        match v {
            0 => Some(SimdLevel::Scalar),
            1 => Some(SimdLevel::Avx2),
            2 => Some(SimdLevel::Avx512),
            _ => None,
        }
    }
}

impl std::fmt::Display for SimdLevel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.name())
    }
}

/// What `--simd` asked for.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SimdRequest {
    /// The best level this CPU supports.
    Auto,
    /// Exactly this level, or refuse to start. For forcing the AVX2 path on a
    /// machine that has AVX-512, which is how a result measured on the
    /// development box is checked against what production will run.
    Fixed(SimdLevel),
}

impl std::str::FromStr for SimdRequest {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.trim().to_ascii_lowercase().as_str() {
            "auto" => Ok(SimdRequest::Auto),
            "scalar" | "none" | "off" => Ok(SimdRequest::Fixed(SimdLevel::Scalar)),
            "avx2" => Ok(SimdRequest::Fixed(SimdLevel::Avx2)),
            "avx512" | "avx-512" => Ok(SimdRequest::Fixed(SimdLevel::Avx512)),
            other => Err(format!(
                "unknown SIMD level '{other}'; expected auto, avx512, avx2 or scalar"
            )),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Vendor {
    Intel,
    Amd,
    Other,
}

/// The CPU's features as the CPU and the OS report them.
#[derive(Clone, Debug, Default)]
pub struct CpuFeatures {
    pub vendor: Option<Vendor>,
    pub family: u32,
    pub model: u32,
    pub brand: String,
    /// `XCR0` has the YMM state enabled.
    pub os_avx: bool,
    /// `XCR0` has the opmask and both ZMM states enabled.
    pub os_avx512: bool,
    pub avx: bool,
    pub avx2: bool,
    pub bmi1: bool,
    pub bmi2: bool,
    pub fma: bool,
    pub lzcnt: bool,
    pub movbe: bool,
    pub popcnt: bool,
    pub avx512f: bool,
    pub avx512bw: bool,
    pub avx512vl: bool,
    pub avx512dq: bool,
    pub avx512cd: bool,
    /// Byte permutes across a whole register (`vpermb`, `vpermi2b`). Not
    /// part of [`SimdLevel::Avx512`]: Skylake-SP has AVX-512 without it, so a
    /// kernel that wants it checks this as well.
    pub avx512vbmi: bool,
    pub avx512vbmi2: bool,
}

impl CpuFeatures {
    /// The x86-64-v3 features this build was compiled to assume, that this CPU
    /// does not have. Not empty means the binary can fault with an illegal
    /// instruction anywhere, so the server must not start.
    pub fn baseline_missing(&self) -> Vec<&'static str> {
        if !cfg!(target_arch = "x86_64") {
            return Vec::new();
        }
        let mut missing = Vec::new();
        for (have, name) in [
            (self.avx && self.os_avx, "avx"),
            (self.avx2 && self.os_avx, "avx2"),
            (self.bmi1, "bmi1"),
            (self.bmi2, "bmi2"),
            (self.fma, "fma"),
            (self.lzcnt, "lzcnt"),
            (self.movbe, "movbe"),
            (self.popcnt, "popcnt"),
        ] {
            if !have {
                missing.push(name);
            }
        }
        missing
    }

    pub fn supports(&self, level: SimdLevel) -> bool {
        match level {
            SimdLevel::Scalar => true,
            SimdLevel::Avx2 => cfg!(target_arch = "x86_64") && self.avx2 && self.os_avx,
            SimdLevel::Avx512 => {
                self.supports(SimdLevel::Avx2)
                    && self.os_avx512
                    && self.avx512f
                    && self.avx512bw
                    && self.avx512vl
                    && self.avx512dq
            }
        }
    }

    pub fn best(&self) -> SimdLevel {
        [SimdLevel::Avx512, SimdLevel::Avx2]
            .into_iter()
            .find(|l| self.supports(*l))
            .unwrap_or(SimdLevel::Scalar)
    }

    /// Whether `PEXT`/`PDEP` are single fast instructions here.
    ///
    /// They are on every Intel part with BMI2 (3 cycles since Haswell). On AMD
    /// before Zen 3 (family 0x19) they are microcoded, with a latency that
    /// depends on the mask, up to a few hundred cycles. So a kernel that packs
    /// a mask with `PEXT` beats the shuffle sequence on Intel and loses badly on
    /// Zen 2, which makes this a routing decision rather than a detail.
    pub fn fast_pext(&self) -> bool {
        self.bmi2
            && match self.vendor {
                Some(Vendor::Intel) => true,
                Some(Vendor::Amd) => self.family >= 0x19,
                _ => false,
            }
    }

    /// The flags the startup line lists, in a fixed order.
    pub fn flag_list(&self) -> String {
        let mut v: Vec<&str> = Vec::new();
        for (have, name) in [
            (self.avx2, "avx2"),
            (self.bmi2, "bmi2"),
            (self.fma, "fma"),
            (self.avx512f, "avx512f"),
            (self.avx512bw, "avx512bw"),
            (self.avx512vl, "avx512vl"),
            (self.avx512dq, "avx512dq"),
            (self.avx512cd, "avx512cd"),
            (self.avx512vbmi, "avx512vbmi"),
            (self.avx512vbmi2, "avx512vbmi2"),
        ] {
            if have {
                v.push(name);
            }
        }
        if self.avx512f && !self.os_avx512 {
            v.push("(avx512 state disabled by the OS)");
        }
        v.join(" ")
    }

    #[cfg(target_arch = "x86_64")]
    fn detect() -> Self {
        use std::arch::x86_64::{__cpuid, __cpuid_count};

        #[allow(unused_unsafe)]
        let leaf = |eax: u32, ecx: u32| unsafe { __cpuid_count(eax, ecx) };
        #[allow(unused_unsafe)]
        let max_leaf = unsafe { __cpuid(0) }.eax;
        #[allow(unused_unsafe)]
        let max_ext = unsafe { __cpuid(0x8000_0000) }.eax;

        let l0 = leaf(0, 0);
        let mut vendor_bytes = [0u8; 12];
        vendor_bytes[0..4].copy_from_slice(&l0.ebx.to_le_bytes());
        vendor_bytes[4..8].copy_from_slice(&l0.edx.to_le_bytes());
        vendor_bytes[8..12].copy_from_slice(&l0.ecx.to_le_bytes());
        let vendor = match &vendor_bytes {
            b"GenuineIntel" => Vendor::Intel,
            b"AuthenticAMD" | b"HygonGenuine" => Vendor::Amd,
            _ => Vendor::Other,
        };

        let mut f = CpuFeatures {
            vendor: Some(vendor),
            ..Default::default()
        };

        if max_leaf >= 1 {
            let l1 = leaf(1, 0);
            let base_family = (l1.eax >> 8) & 0xF;
            let base_model = (l1.eax >> 4) & 0xF;
            f.family = if base_family == 0xF {
                base_family + ((l1.eax >> 20) & 0xFF)
            } else {
                base_family
            };
            f.model = if base_family == 0x6 || base_family == 0xF {
                base_model | (((l1.eax >> 16) & 0xF) << 4)
            } else {
                base_model
            };
            f.fma = l1.ecx & (1 << 12) != 0;
            f.movbe = l1.ecx & (1 << 22) != 0;
            f.popcnt = l1.ecx & (1 << 23) != 0;
            let osxsave = l1.ecx & (1 << 27) != 0;
            f.avx = l1.ecx & (1 << 28) != 0;
            if osxsave {
                let xcr0 = xgetbv0();
                // SSE (bit 1) and YMM (bit 2).
                f.os_avx = xcr0 & 0x6 == 0x6;
                // Plus opmask (5), ZMM_Hi256 (6) and Hi16_ZMM (7).
                f.os_avx512 = xcr0 & 0xE6 == 0xE6;
            }
        }
        if max_leaf >= 7 {
            let l7 = leaf(7, 0);
            f.bmi1 = l7.ebx & (1 << 3) != 0;
            f.avx2 = l7.ebx & (1 << 5) != 0;
            f.bmi2 = l7.ebx & (1 << 8) != 0;
            f.avx512f = l7.ebx & (1 << 16) != 0;
            f.avx512dq = l7.ebx & (1 << 17) != 0;
            f.avx512cd = l7.ebx & (1 << 28) != 0;
            f.avx512bw = l7.ebx & (1 << 30) != 0;
            f.avx512vl = l7.ebx & (1 << 31) != 0;
            f.avx512vbmi = l7.ecx & (1 << 1) != 0;
            f.avx512vbmi2 = l7.ecx & (1 << 6) != 0;
        }
        if max_ext >= 0x8000_0001 {
            f.lzcnt = leaf(0x8000_0001, 0).ecx & (1 << 5) != 0;
        }
        if max_ext >= 0x8000_0004 {
            let mut brand = Vec::with_capacity(48);
            for l in 0x8000_0002u32..=0x8000_0004 {
                let r = leaf(l, 0);
                for w in [r.eax, r.ebx, r.ecx, r.edx] {
                    brand.extend_from_slice(&w.to_le_bytes());
                }
            }
            f.brand = String::from_utf8_lossy(&brand)
                .trim_matches(char::from(0))
                .trim()
                .to_string();
        }
        f
    }

    #[cfg(not(target_arch = "x86_64"))]
    fn detect() -> Self {
        CpuFeatures::default()
    }
}

/// `XCR0`, read with inline assembly rather than `_xgetbv`: the intrinsic needs
/// the `xsave` target feature enabled on the caller, and this runs before
/// anything is known about the CPU.
#[cfg(target_arch = "x86_64")]
fn xgetbv0() -> u64 {
    let (lo, hi): (u32, u32);
    unsafe {
        std::arch::asm!(
            "xgetbv",
            in("ecx") 0u32,
            out("eax") lo,
            out("edx") hi,
            options(nomem, nostack, preserves_flags),
        );
    }
    ((hi as u64) << 32) | lo as u64
}

static FEATURES: OnceLock<CpuFeatures> = OnceLock::new();

/// Unset until [`init`] or the first [`level`].
const UNSET: u8 = u8::MAX;
static LEVEL: AtomicU8 = AtomicU8::new(UNSET);

/// The CPU's features, read once.
pub fn features() -> &'static CpuFeatures {
    FEATURES.get_or_init(CpuFeatures::detect)
}

/// The level every dispatcher routes through.
#[inline]
pub fn level() -> SimdLevel {
    if let Some(l) = SimdLevel::from_u8(LEVEL.load(Ordering::Relaxed)) {
        return l;
    }
    let best = features().best();
    // Whoever gets here first decides; a concurrent `init` wins over this
    // default because it stores unconditionally.
    let _ = LEVEL.compare_exchange(UNSET, best as u8, Ordering::Relaxed, Ordering::Relaxed);
    SimdLevel::from_u8(LEVEL.load(Ordering::Relaxed)).unwrap_or(best)
}

/// Every level this CPU can run, lowest first. For tests that must hold each
/// kernel to the same answers, rather than only the one [`level`] picks.
pub fn runnable_levels() -> Vec<SimdLevel> {
    let f = features();
    [SimdLevel::Scalar, SimdLevel::Avx2, SimdLevel::Avx512]
        .into_iter()
        .filter(|l| f.supports(*l))
        .collect()
}

/// Fix the level for this process. Call once at startup, before any query
/// runs: a kernel that has already resolved its function pointer (the filter
/// index's `probe_fn`) keeps the level it saw.
///
/// An explicit level the CPU cannot run is an error rather than a quiet
/// downgrade. Someone who asked for `avx512` is measuring or debugging
/// that path, and silently giving them `avx2` would make the measurement a lie.
pub fn init(request: SimdRequest) -> Result<SimdLevel, String> {
    let f = features();
    let chosen = match request {
        SimdRequest::Auto => f.best(),
        SimdRequest::Fixed(l) if f.supports(l) => l,
        SimdRequest::Fixed(l) => {
            return Err(format!(
                "--simd {l} was requested, but this CPU ({}) does not support it; it supports: {}",
                describe_cpu(f),
                f.flag_list()
            ));
        }
    };
    LEVEL.store(chosen as u8, Ordering::Relaxed);
    Ok(chosen)
}

/// Exit with a readable message if this CPU lacks the features the binary was
/// compiled to assume. Call first thing in `main`: the alternative is a
/// `SIGILL` at whichever instruction happens to need them first, with nothing
/// saying why.
pub fn ensure_baseline_or_exit() {
    let f = features();
    let missing = f.baseline_missing();
    if !missing.is_empty() {
        eprintln!(
            "this build of RasterizedDB requires an x86-64-v3 CPU (AVX2, BMI1/2, FMA, LZCNT, MOVBE); \
             this CPU ({}) lacks: {}. Rebuild with a lower -C target-cpu in .cargo/config.toml.",
            describe_cpu(f),
            missing.join(", ")
        );
        std::process::exit(1);
    }
}

/// Brand string, or vendor/family/model when there is none.
pub fn describe_cpu(f: &CpuFeatures) -> String {
    if !f.brand.is_empty() {
        return f.brand.clone();
    }
    format!("{:?} family {:#x} model {:#x}", f.vendor, f.family, f.model)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn requests_parse() {
        assert_eq!("auto".parse::<SimdRequest>(), Ok(SimdRequest::Auto));
        assert_eq!(
            "AVX512".parse::<SimdRequest>(),
            Ok(SimdRequest::Fixed(SimdLevel::Avx512))
        );
        assert_eq!(
            "avx2".parse::<SimdRequest>(),
            Ok(SimdRequest::Fixed(SimdLevel::Avx2))
        );
        assert_eq!(
            "scalar".parse::<SimdRequest>(),
            Ok(SimdRequest::Fixed(SimdLevel::Scalar))
        );
        assert!("sse9".parse::<SimdRequest>().is_err());
    }

    /// The build assumes x86-64-v3, so a machine that can run this test at all
    /// has the baseline; reporting it missing would be a detection bug.
    #[cfg(target_arch = "x86_64")]
    #[test]
    fn this_machine_has_the_baseline_it_was_built_for() {
        let f = features();
        assert!(f.baseline_missing().is_empty(), "{f:?}");
        assert!(f.supports(SimdLevel::Avx2));
        assert!(runnable_levels().contains(&SimdLevel::Avx2));
    }

    /// `cpuid` against the standard library's runtime detection, for the one
    /// feature the build does not assume (so the macro is not folded).
    #[cfg(target_arch = "x86_64")]
    #[test]
    fn avx512_detection_agrees_with_std() {
        let f = features();
        let std_says = std::is_x86_feature_detected!("avx512f")
            && std::is_x86_feature_detected!("avx512bw")
            && std::is_x86_feature_detected!("avx512vl")
            && std::is_x86_feature_detected!("avx512dq");
        assert_eq!(f.supports(SimdLevel::Avx512), std_says, "{f:?}");
        assert_eq!(f.avx512vbmi, std::is_x86_feature_detected!("avx512vbmi"), "{f:?}");
    }
}
