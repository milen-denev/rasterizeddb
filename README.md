# Rasterized DB

## A new schemaless, high-performance database written from scratch in Rust.

### Vision

#### Why schemaless?

In Rasterized DB, the schema is maintained on the client side instead of the database. This design allows for flexibility and adaptability. To support this approach, an ORM (Object-Relational Mapper) for Rust will be released soon, catering to a variety of development needs.

#### How Does It Achieve Better Performance Than Conventional Databases?

Rasterized DB adopts a novel approach inspired by how the web works. For example, when you receive a file with specific headers, the file isn’t re-fetched if it hasn’t expired; instead, the existing copy is used. Similarly, every query in Rasterized DB is hashed. If the same query is repeated, the corresponding row is loaded directly from a specific file location without needing to scan.

Future updates will introduce an in-memory cache for rows, allowing the system to bypass the file system entirely. This will provide a combined functionality similar to Redis and a database, offering fast performance and seamless integration.

#### Why Rust?

Rust provides high-level, zero-cost abstractions combined with low-level optimization capabilities. This makes development faster while achieving the best possible performance. Rust's safety guarantees also contribute to the reliability and robustness of the database.

#### Does it have limits?

Rasterized DB is designed with scalability and flexibility in mind, aiming to accommodate all potential use cases. Future updates will introduce support for new and exotic data types, enabling the database to natively handle complex data structures. Examples include one-dimensional and multi-dimensional arrays, vectors, and tensors.

Theoretically, the database can support limitless amounts of data, row sizes, and the number of columns.

#### What is RQL? 

Rasterized Query Language (RQL) is a SQL-inspired dialect tailored to the unique requirements of Rasterized DB. Since the schema is managed by the client and not the database server, traditional SQL cannot be used directly.

RQL is also planned to include advanced features inspired by Microsoft’s Kusto Query Language, enhancing its expressiveness and functionality.

#### Will it have an ORM?

Yes, a standalone ORM for Rust is planned, with a C# version to follow soon after.

#### Which futures are missing?

The following features are currently missing but are planned for future updates:

- Updating rows
- Vacuuming empty space
- UUID (GUID) support
- Fully functional RETURN, LIMIT, and SELECT commands among many other SQL features missing
- Server features
- Sharding
- Compression
- Table Immutability 

Many additional enhancements are also planned to make the database more robust and feature-complete.

#### Is it stable?
Short answer, no. It will get through many refinements and even changes to the table files. Until version 1.0.0 use it on your own risk.

### How to use the current API?

#### Create a static TABLE
```rust
static TABLE: LazyLock<Arc<tokio::sync::RwLock<Table>>> = LazyLock::new(|| 
    Arc::new(RwLock::const_new(Table::init("Some\\Folder", false).unwrap()))
);
```

#### Create columns, rows and insert them

```rust
let mut c1 = Column::new(10 as i32).unwrap();
let mut c2 = Column::new(50.0 as f64).unwrap();
let mut c3 = Column::new("This is awesome").unwrap();
let mut columns_buffer: Vec<u8> = Vec::with_capacity(
    c1.len() + 
    c2.len() +
    c3.len() +
);
columns_buffer.append(&mut c1.into_vec().unwrap());
columns_buffer.append(&mut c2.into_vec().unwrap());
columns_buffer.append(&mut c3.into_vec().unwrap());

let table_clone = TABLE.clone();
let mut mutable_table = table_clone.write().await;

// Can be combined with tokio::Spawn as well
mutable_table.insert_row(&mut InsertRow {
    columns_data: columns_buffer
}).await.unwrap();
```
#### Build in-memory file indexes
```rust
mutable_table.rebuild_in_memory_indexes();
```

#### Retrieve a row
```rust
let row_by_id = mutable_table.first_or_default_by_id(10).unwrap().unwrap();

// Read columns
for column in Column::from_buffer(&row2.columns_data).unwrap().iter() {
    println!("{}", column.into_value());
}

// Column index, value that must be equal
let row_by_column_value = mutable_table.first_or_default_by_column(2, "This is awesome").unwrap().unwrap();

//Rasterized Query Language (Alpha)
let query_evaluation = parse_rql(&format!(r#"
    BEGIN
    SELECT FROM NAME_DOESNT_MATTER_FOR_NOW
    WHERE COL(2) = 'This is awesome'
    END
"#)).unwrap();

// Uses cache: If the same query is repeated, the time to retrieve a row should be in the single-digit range.
let row_by_query = mutable_table.first_or_default_by_query(query_evaluation).await.unwrap().unwrap();
```

#### Delete a row
```rust
// Invalidates cache automatically
mutable_table.delete_row_by_id(10).unwrap();
```

### License
Everything in this directory is distributed under GNU GENERAL PUBLIC LICENSE version 3.