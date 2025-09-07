use std::sync::Arc;

#[derive(Clone)]
pub struct Chunk {
    pub start: u64,
    pub data: Arc<[u8]>,
}

impl Chunk {
    pub fn empty() -> Arc<Self> {
        Arc::new(Self {
            start: 0,
            data: Arc::from(&[][..]),
        })
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.data.len()
    }

    #[inline]
    pub fn end(&self) -> u64 {
        self.start + self.len() as u64
    }

    #[inline]
    pub fn contains(&self, pos: u64) -> bool {
        pos >= self.start && pos < self.end()
    }
}
