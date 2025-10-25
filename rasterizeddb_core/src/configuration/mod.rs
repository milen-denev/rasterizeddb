#[derive(Clone)]
pub struct Configuration {
    pub location: Option<String>,
    pub batch_size: Option<usize>,
    pub concurrent_threads: Option<usize>
}