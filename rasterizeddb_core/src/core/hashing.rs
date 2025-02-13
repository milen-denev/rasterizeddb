use ahash::RandomState;
use once_cell::sync::Lazy;

pub(crate) static HASHER: Lazy<RandomState> = Lazy::new(|| {
    let hash_builder = RandomState::with_seed(1);
    return hash_builder;
});

pub(crate) fn get_hash(query: &str) -> u64 {
    let hash = HASHER.hash_one(query);
    return hash;
}
