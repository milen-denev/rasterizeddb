pub struct Configuration {
    pub location: Option<String>,
    pub batch_size: Option<usize>,
    pub concurrent_threads: Option<usize>,
    /// Extra directories this database may write into, beyond `location`.
    ///
    /// Each is `PATH` or `PATH:ROLE[:WEIGHT]`, matching `--storage-location`.
    /// Applied on top of what the pool file already records rather than
    /// replacing it: placement is persisted, so a flag that reset the pool
    /// would make every table created into a second directory unfindable at the
    /// next start-up.
    pub storage_locations: Vec<String>,
    /// `primary`, `round-robin` or `free-space`. `None` leaves whatever the
    /// pool file says.
    pub storage_policy: Option<String>,
    /// Free-space floor below which a location stops being chosen, e.g. `2GiB`.
    pub storage_min_free: Option<String>,
}
