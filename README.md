
# Rasterized DB
## A high-performance database written from scratch in Rust — now fully rewritten.

### Guide on how to run

rasterizeddb_server-PLATFORM-x64.exe --location /your-location/bla-bla --concurrent_threads 16 --batch_size 16000

*batch_size*: recommended value is 16K, but you can play around with more or less depending on your case. The lesser the number of rows, the lower the batch size must be.
*concurrent_threads*: must equal to the number of CPU threads of lower.

### Guide on how to run the client-side

Refer to *test_client/src/main.rs* for examples! 

### Vision

#### Complete Rewrite
Rasterized DB has recently undergone a complete rewrite from the ground up and is still in an active state of redevelopment. This major overhaul has brought significant architectural changes to improve stability, performance, and developer experience. Many components are being redesigned for long-term maintainability, and the database will continue to evolve rapidly until its core design reaches maturity.

#### From Schemaless to Schema-Full
Originally conceived as a schemaless database, Rasterized DB is now schema-full. The database itself now manages and enforces the schema, reducing complexity for client applications and enabling stronger data consistency. This change also opens the door to richer query capabilities and deeper optimizations at the storage and execution layers.

#### PostgreSQL Dialect Compatibility
To make adoption easier, Rasterized DB aims for compatibility with the PostgreSQL SQL dialect, the most widely used and supported SQL standard in production environments. While not all PostgreSQL features are currently implemented, this compatibility goal ensures developers can leverage familiar syntax and tools without learning a proprietary query language from scratch.

#### Performance Philosophy
Rasterized DB takes inspiration from how the web handles caching. Just like a browser reuses files when their cache headers indicate they’re still valid, Rasterized DB hashes each query so repeated requests can instantly fetch results from a known storage offset without rescanning the dataset.

Future updates will introduce in-memory row caching, allowing certain queries to bypass disk entirely. This approach will merge the speed of in-memory stores like Redis with the persistence of a traditional database.

#### Why Rust?
Rust combines zero-cost abstractions with low-level performance control, making it ideal for building a database that is both fast and safe. Its memory safety guarantees reduce the risk of crashes or data corruption, while still enabling optimizations close to the hardware.

#### Scalability and Future Features

Rasterized DB is designed to handle virtually unlimited data sizes, row counts, and column counts. Planned features include:

- Advanced data types: arrays, vectors, tensors, and more
- Row insertion, updates, and deletions via SQL
- Vacuuming unused space
- UUID (GUID) support
- Fully functional RETURN, LIMIT, and advanced SELECT capabilities
- Server mode with network access
- Sharding for horizontal scalability
- Compression for storage efficiency
- Table immutability options

#### ORM & Ecosystem
An official Rust ORM is in development, with a C# ORM planned afterward. These tools will provide a seamless experience when integrating Rasterized DB into applications.

#### Stability
Currently, Rasterized DB is not stable. Table formats, storage engines, and query processing internals are likely to change until version 1.0.0. Use it at your own risk in production environments.

### How to use the current API?

```rust
// COMING SOON
// Please refer to the main.rs and core/mock_table.rs and core/mock_helpers.rs to see API in use.
```

##### Sponsor

[![Buy me a coffe](https://raw.githubusercontent.com/vasundhasauras/badge-bmc/1bf9f937862f918818d3528cce12256be0116570/badges/coffee/buy%20me%20a%20coffee/bm_coffee.svg "Buy me a coffe")](https://buymeacoffee.com/milen.denev)

### License
Everything in this directory is distributed under GNU GENERAL PUBLIC LICENSE version 3.
