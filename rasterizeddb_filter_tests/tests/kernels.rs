//! Bit-sliced plane kernels: SIMD paths against the scalar oracle, and the
//! slot read/write primitive that insert, update and delete all share.

use rasterizeddb_core::core::filter::hash::{CODE_EMPTY, CODE_NULL, fingerprint};
use rasterizeddb_core::core::filter::kernels::*;

fn random_planes(f: u8) -> Vec<u64> {
    (0..f as usize * ZONE_WORDS).map(|_| fastrand::u64(..)).collect()
}

#[test]
fn geometry_is_what_the_layout_assumes() {
    // A plane is exactly one cache line and exactly one AVX-512 register, and a
    // zone's mask drops straight into a slot bitmap with no bit shuffling.
    assert_eq!(ZONE_ROWS, 512);
    assert_eq!(ZONE_WORDS, ZONE_ROWS / 64);
    assert_eq!(ZONE_WORDS * 8, 64, "one plane must be one 64-byte cache line");
    assert!(MIN_F >= 4 && MAX_F <= 16 && MIN_F < MAX_F);
}

#[test]
fn simd_matches_scalar_across_every_width() {
    let probe = probe_fn();
    for f in MIN_F..=MAX_F {
        for _ in 0..500 {
            let planes = random_planes(f);
            let g = fingerprint(fastrand::u64(..), f);

            let mut want = [0u64; ZONE_WORDS];
            probe_zone_scalar(&planes, f, g, &mut want);

            let mut got = [0u64; ZONE_WORDS];
            probe(&planes, f, g, &mut got);

            assert_eq!(got, want, "kernel {} disagrees at f={}", active_kernel(), f);
        }
    }
}

#[test]
fn simd_matches_scalar_on_degenerate_planes() {
    let probe = probe_fn();
    for f in MIN_F..=MAX_F {
        for planes in [
            vec![0u64; f as usize * ZONE_WORDS],
            vec![u64::MAX; f as usize * ZONE_WORDS],
        ] {
            for g in [CODE_FIRST_LOCAL, (1u32 << f) - 1, fingerprint(fastrand::u64(..), f)] {
                let g = g.min((1u32 << f) - 1).max(2);
                let mut want = [0u64; ZONE_WORDS];
                probe_zone_scalar(&planes, f, g, &mut want);
                let mut got = [0u64; ZONE_WORDS];
                probe(&planes, f, g, &mut got);
                assert_eq!(got, want, "f={} g={}", f, g);
            }
        }
    }
}
const CODE_FIRST_LOCAL: u32 = 2;

#[test]
fn slot_roundtrip_over_every_position_and_width() {
    for f in MIN_F..=MAX_F {
        let mut planes = vec![0u64; f as usize * ZONE_WORDS];
        let mut expect = vec![0u32; ZONE_ROWS];
        for local in 0..ZONE_ROWS {
            let g = fingerprint(fastrand::u64(..), f);
            write_slot(&mut planes, f, local, g);
            expect[local] = g;
        }
        for local in 0..ZONE_ROWS {
            assert_eq!(read_slot(&planes, f, local), expect[local], "f={} slot={}", f, local);
        }
    }
}

#[test]
fn writing_one_slot_leaves_its_neighbours_alone() {
    // Bit-slicing means one slot's fingerprint is spread across f words; a
    // wrong mask would silently corrupt the slots either side.
    let f = 8u8;
    for target in [0usize, 1, 63, 64, 65, 255, 256, 510, 511] {
        let mut planes = vec![0u64; f as usize * ZONE_WORDS];
        let codes: Vec<u32> =
            (0..ZONE_ROWS).map(|_| fingerprint(fastrand::u64(..), f)).collect();
        for (i, g) in codes.iter().enumerate() {
            write_slot(&mut planes, f, i, *g);
        }

        let replacement = fingerprint(fastrand::u64(..), f);
        write_slot(&mut planes, f, target, replacement);

        for i in 0..ZONE_ROWS {
            let want = if i == target { replacement } else { codes[i] };
            assert_eq!(read_slot(&planes, f, i), want, "target={} disturbed slot {}", target, i);
        }
    }
}

#[test]
fn clearing_a_slot_writes_the_empty_code() {
    let f = 10u8;
    let mut planes = vec![0u64; f as usize * ZONE_WORDS];
    for i in 0..ZONE_ROWS {
        write_slot(&mut planes, f, i, fingerprint(fastrand::u64(..), f));
    }
    write_slot(&mut planes, f, 300, CODE_EMPTY);
    assert_eq!(read_slot(&planes, f, 300), CODE_EMPTY);

    // An all-zero region reads as "matches nothing", which is what makes a
    // freshly grown zone and a deleted slot safe by default.
    let zero = vec![0u64; f as usize * ZONE_WORDS];
    for local in 0..ZONE_ROWS {
        assert_eq!(read_slot(&zero, f, local), CODE_EMPTY);
    }
}

#[test]
fn probe_finds_every_written_slot() {
    // The kernel-level statement of "no false negatives".
    let probe = probe_fn();
    for f in MIN_F..=MAX_F {
        let mut planes = vec![0u64; f as usize * ZONE_WORDS];
        let codes: Vec<u32> =
            (0..ZONE_ROWS).map(|_| fingerprint(fastrand::u64(..), f)).collect();
        for (i, g) in codes.iter().enumerate() {
            write_slot(&mut planes, f, i, *g);
        }
        let mut out = [0u64; ZONE_WORDS];
        for (i, g) in codes.iter().enumerate() {
            probe(&planes, f, *g, &mut out);
            assert!(
                (out[i / 64] >> (i % 64)) & 1 == 1,
                "f={}: slot {} lost its own code",
                f,
                i
            );
        }
    }
}

#[test]
fn probing_an_empty_zone_matches_nothing() {
    let probe = probe_fn();
    for f in MIN_F..=MAX_F {
        let planes = vec![0u64; f as usize * ZONE_WORDS];
        let mut out = [0u64; ZONE_WORDS];
        for _ in 0..50 {
            probe(&planes, f, fingerprint(fastrand::u64(..), f), &mut out);
            assert!(out.iter().all(|w| *w == 0), "f={}: empty zone matched", f);
        }
        // And the reserved codes match nothing either.
        for g in [CODE_EMPTY, CODE_NULL] {
            probe(&planes, f, g, &mut out);
            let _ = g;
        }
    }
}

#[test]
fn a_uniform_zone_matches_all_or_nothing() {
    let probe = probe_fn();
    let f = 8u8;
    let g = fingerprint(fastrand::u64(..), f);
    let mut planes = vec![0u64; f as usize * ZONE_WORDS];
    for i in 0..ZONE_ROWS {
        write_slot(&mut planes, f, i, g);
    }

    let mut out = [0u64; ZONE_WORDS];
    probe(&planes, f, g, &mut out);
    assert!(out.iter().all(|w| *w == u64::MAX), "every slot holds g; all should match");

    let other = {
        let mut o = fingerprint(fastrand::u64(..), f);
        while o == g {
            o = fingerprint(fastrand::u64(..), f);
        }
        o
    };
    probe(&planes, f, other, &mut out);
    assert!(out.iter().all(|w| *w == 0), "no slot holds `other`; none should match");
}

#[test]
fn false_positive_rate_tracks_theory() {
    let probe = probe_fn();
    for f in [6u8, 8, 10, 12] {
        let mut planes = vec![0u64; f as usize * ZONE_WORDS];
        for i in 0..ZONE_ROWS {
            write_slot(&mut planes, f, i, fingerprint(fastrand::u64(..), f));
        }
        let mut out = [0u64; ZONE_WORDS];
        let trials = 400usize;
        let mut hits = 0u64;
        for _ in 0..trials {
            probe(&planes, f, fingerprint(fastrand::u64(..), f), &mut out);
            hits += out.iter().map(|w| w.count_ones() as u64).sum::<u64>();
        }
        let observed = hits as f64 / (trials * ZONE_ROWS) as f64;
        let theory = 1.0 / ((1u32 << f) - 2) as f64;
        assert!(
            observed < theory * 2.5,
            "f={}: observed FPP {:.6} far above theory {:.6}",
            f,
            observed,
            theory
        );
    }
}

#[test]
fn active_kernel_is_reported_and_stable() {
    let a = active_kernel();
    assert!(matches!(a, "avx512f" | "avx2" | "scalar"), "unexpected kernel {}", a);
    assert_eq!(a, active_kernel(), "kernel selection must be stable");
    // Dispatch resolves once; repeated calls must hand back the same function.
    let (f1, f2) = (probe_fn(), probe_fn());
    assert!(std::ptr::fn_addr_eq(f1, f2));
}
