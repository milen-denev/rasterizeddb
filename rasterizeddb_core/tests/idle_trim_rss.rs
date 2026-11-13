//! `release_freed_memory` (what the idle trimmer runs) gives memory freed in
//! worker threads' arenas back to the kernel. A binary of its own so that no
//! other test's allocations move the resident size between the readings.

#[cfg(all(target_os = "linux", target_env = "gnu"))]
#[test]
fn a_trim_gives_freed_arena_memory_back() {
    fn rss_kb() -> u64 {
        let statm = std::fs::read_to_string("/proc/self/statm").unwrap();
        let pages: u64 = statm.split_whitespace().nth(1).unwrap().parse().unwrap();
        pages * 4
    }
    // Small blocks, so they come from the threads' arenas rather than mmap,
    // all alive at once and then freed on the threads that made them: about
    // 200 MB, with one block in a hundred kept alive between them, so no
    // arena can give the space back from its top on its own: only a trim
    // reaches the free pages in between.
    let handles: Vec<_> = (0..4)
        .map(|_| {
            std::thread::spawn(|| {
                let mut blocks: Vec<Vec<u8>> = (0..50_000).map(|i| vec![(i % 251) as u8; 1000]).collect();
                std::hint::black_box(&blocks);
                // One block in a hundred outlives the rest, scattered through
                // the arena, as a server's long-lived allocations are.
                let kept: Vec<Vec<u8>> = (0..blocks.len()).step_by(100).map(|i| std::mem::take(&mut blocks[i])).collect();
                drop(blocks);
                kept
            })
        })
        .collect();
    let pins: Vec<Vec<Vec<u8>>> = handles.into_iter().map(|h| h.join().unwrap()).collect();
    let held = rss_kb();
    rasterizeddb_core::release_freed_memory();
    let after = rss_kb();
    assert!(held > 150_000, "the freed blocks should still be held before the trim: {held} kB");
    assert!(
        after + 100_000 < held,
        "a trim should return the freed arenas: {held} kB before, {after} kB after"
    );
    drop(pins);
}
