use std::io;

use rclite::Arc;

use itertools::Itertools;
use smallvec::SmallVec;

use crate::{
    core::{db_type::DbType, storage_providers::traits::StorageIO},
    memory_pool::{MEMORY_POOL, MemoryBlock},
};

pub(crate) const SCHEMA_FIELD_SIZE: u64 = 129 + // up to 128 ascii chars for name + null terminator
    2 + // db_type u16,
    4 + // size u32,
    8 + // offset u64,
    8 + // write_order u64,
    1 + // is_unique u8
    1; // is_deleted u8

/// Size of each chunk to read from storage
const SCHEMA_FIELD_CHUNK_SIZE: usize = SCHEMA_FIELD_SIZE as usize * 100;

/// Number of schema fields to retrieve in a batch
const BATCH_SIZE: usize = 100;

/// Bit 0 of a field record's spare type byte (130): the column is a
/// `timestamp` (without time zone). See [`SchemaField::without_time_zone`].
const TIMESTAMP_WITHOUT_TIME_ZONE: u8 = 0x01;

const TABLE_SIZE: usize = 129 * 2 + 1; // 128 chars for name  + null terminator and primary key + null terminator

/// Opens the incarnation record that follows the header in `_schema.db`.
///
/// The first byte is one a header cannot start with — a header starts with the
/// table name in UTF-8, which never holds `0xFF`, or with the `0` of an empty
/// one — so a reader walking several headers ([`TableSchemaIterator`]) can tell the two
/// apart. Version 1 in the last byte.
const INCARNATION_MAGIC: [u8; 8] = [0xFF, b'R', b'Z', b'I', b'N', b'C', 0, 1];

/// The incarnation record: [`INCARNATION_MAGIC`], the incarnation as a
/// little-endian `u128`, and a CRC-32 of those 24 bytes, little-endian.
///
/// After the header, because the header is fixed-width and every binary,
/// older ones included, reads exactly [`TABLE_SIZE`] bytes of it and nothing
/// further: an older binary opens a file with the record exactly as it opened
/// one without. Header rewrites write over the header only, so they keep it.
const INCARNATION_LEN: usize = INCARNATION_MAGIC.len() + 16 + 4;

/// The incarnation record for `incarnation`.
fn incarnation_record(incarnation: u128) -> [u8; INCARNATION_LEN] {
    let mut record = [0u8; INCARNATION_LEN];
    record[..8].copy_from_slice(&INCARNATION_MAGIC);
    record[8..24].copy_from_slice(&incarnation.to_le_bytes());
    let crc = crc32fast::hash(&record[..24]);
    record[24..].copy_from_slice(&crc.to_le_bytes());
    record
}

/// The incarnation `bytes` records, or `None` if they are not a whole,
/// verifying record of a non-zero incarnation.
fn parse_incarnation(bytes: &[u8]) -> Option<u128> {
    let record = bytes.get(..INCARNATION_LEN)?;
    if record[..8] != INCARNATION_MAGIC {
        return None;
    }
    let crc = u32::from_le_bytes(record[24..].try_into().expect("four bytes"));
    if crc32fast::hash(&record[..24]) != crc {
        return None;
    }
    let incarnation = u128::from_le_bytes(record[8..24].try_into().expect("sixteen bytes"));
    (incarnation != 0).then_some(incarnation)
}

/// A new, random, non-zero incarnation.
pub fn mint_incarnation() -> u128 {
    loop {
        let mut bytes = [0u8; 16];
        rand::fill(&mut bytes);
        let incarnation = u128::from_le_bytes(bytes);
        if incarnation != 0 {
            return incarnation;
        }
    }
}

#[derive(Debug, Clone)]
pub struct SchemaField {
    pub name: String,
    pub db_type: DbType,
    /// A `DATETIME` column declared `timestamp` (without time zone) rather
    /// than `timestamptz`; always `false` for any other type.
    ///
    /// The two are stored alike, eight bytes of microseconds, so this is
    /// metadata: a literal's zone offset is discarded for such a column rather
    /// than applied (`'… 10:00+02'` stores 10:00, as PostgreSQL stores it), and
    /// the column is described as `timestamp` and shown with no `+00`
    /// ([`DbType::TIMESTAMP`], [`SchemaField::literal_type`]).
    ///
    /// Persisted in the field record's spare type byte (130), which every
    /// earlier build wrote as zero and reads past: a table created before this
    /// existed reads as `timestamptz`, which is what it always behaved as, and
    /// an earlier build opening a newer table does the same.
    pub without_time_zone: bool,
    pub size: u32,
    pub offset: u64,
    pub write_order: u64,
    pub is_unique: bool,
    pub is_deleted: bool,
    /// The `s` of a `NUMERIC(p, s)` declaration, when the column has one.
    ///
    /// A `DECIMAL` is stored as an `i128` of units scaled by this many decimal
    /// places, so a comparison against a literal is only meaningful once both
    /// sides are at the same scale. It lives on the field rather than only in
    /// the type-modifier sidecar because the query planner has the schema and
    /// not the sidecar — and without it every predicate over a
    /// `NUMERIC(p, s)` column with `s` other than the engine default answered
    /// from a value a power of ten away from the stored one. `WHERE price > 50`
    /// over a `NUMERIC(10,2)` column returned no rows, and no error.
    ///
    /// `None` means the engine default,
    /// [`crate::core::value_codec::DECIMAL_SCALE`].
    pub scale: Option<u32>,
    /// The declared length of a `character(n)` or `character varying(n)`
    /// column, from its marker in the type-modifier sidecar
    /// (`type_modifier::TextTypmod`).
    ///
    /// On the field for the reason `scale` is: the planner, the tokenizer and
    /// the evaluator are handed fields, not the sidecar, and a `character(n)`
    /// column is stored without its trailing spaces, so a literal compared
    /// with one has to be read the same way (`'b  '` is `'b'`) or the
    /// comparison answers from a value the column cannot hold.
    ///
    /// Packed into four bytes ([`crate::core::rql::type_modifier::PackedTypmod`]);
    /// read through [`SchemaField::text_typmod`]. A `timestamp(p)`-style
    /// precision is carried here too.
    pub typmod_packed: Option<crate::core::rql::type_modifier::PackedTypmod>,
}

impl SchemaField {
    /// The column's declared length or precision, when it has one.
    #[inline]
    pub fn text_typmod(&self) -> Option<crate::core::rql::type_modifier::TextTypmod> {
        self.typmod_packed.map(crate::core::rql::type_modifier::PackedTypmod::unpack)
    }

    /// Sets [`SchemaField::text_typmod`].
    #[inline]
    pub fn set_text_typmod(&mut self, typmod: Option<crate::core::rql::type_modifier::TextTypmod>) {
        self.typmod_packed = typmod.and_then(crate::core::rql::type_modifier::PackedTypmod::pack);
    }

    pub fn new(
        name: String,
        db_type: DbType,
        size: u32,
        offset: u64,
        write_order: u64,
        is_unique: bool,
    ) -> Self {
        // `DbType::TIMESTAMP` names a `timestamp` column; it is stored as a
        // `DATETIME` that carries the flag.
        let without_time_zone = db_type == DbType::TIMESTAMP;
        let db_type = if without_time_zone { DbType::DATETIME } else { db_type };
        SchemaField {
            name,
            db_type,
            without_time_zone,
            size,
            offset,
            write_order,
            is_unique,
            scale: None,
            typmod_packed: None,
            is_deleted: false,
        }
    }

    /// The type a literal written for this column is read as: its own, except
    /// that a `timestamp` column reads one as [`DbType::TIMESTAMP`], so a
    /// zone offset in it is discarded rather than applied.
    pub fn literal_type(&self) -> DbType {
        if self.without_time_zone && self.db_type == DbType::DATETIME {
            DbType::TIMESTAMP
        } else {
            self.db_type.clone()
        }
    }

    pub fn set_unique(&mut self) {
        self.is_unique = true;
    }

    pub fn into_vec(&self) -> Vec<u8> {
        let mut vec = vec![0; SCHEMA_FIELD_SIZE as usize];

        // The column's name as it is, UTF-8, cut only at a character
        // boundary within 128 bytes. Filtered to ASCII letters and digits,
        // as it was, `naïve` came back from the next open as `nave` and
        // `"日本"` as "", so every statement naming the column failed, and
        // every insert into a NOT NULL one was refused as a NULL. A record
        // written that way reads back as written.
        let name_bytes = self.name.as_bytes();
        let mut copy_len = name_bytes.len().min(128);
        while !self.name.is_char_boundary(copy_len) {
            copy_len -= 1;
        }
        vec[..copy_len].copy_from_slice(&name_bytes[..copy_len]);

        // Name is already null terminated by the initial zeroes

        // Add type at position 129
        let type_bytes = (self.db_type.to_byte() as u16).to_le_bytes();
        vec[129..129 + 2].copy_from_slice(&type_bytes);
        // The type's high byte is spare (every type fits the low one, and the
        // reader has always taken only that): bit 0 marks a `timestamp`
        // column ([`SchemaField::without_time_zone`]).
        if self.without_time_zone && self.db_type == DbType::DATETIME {
            vec[130] |= TIMESTAMP_WITHOUT_TIME_ZONE;
        }

        // Add size at position 131
        let size_bytes = self.size.to_le_bytes();
        vec[131..131 + 4].copy_from_slice(&size_bytes);

        // Add offset at position 135
        let offset_bytes = self.offset.to_le_bytes();
        vec[135..135 + 8].copy_from_slice(&offset_bytes);

        // Add write_order at position 143
        let write_order_bytes = self.write_order.to_le_bytes();
        vec[143..143 + 8].copy_from_slice(&write_order_bytes);

        // Add is_unique at position 151
        vec[151] = if self.is_unique { 1 } else { 0 };

        // Add is_deleted at position 152
        vec[152] = if self.is_deleted { 1 } else { 0 };

        vec
    }

    pub fn from_vec(vec: &[u8]) -> Result<Self, String> {
        if vec.len() != SCHEMA_FIELD_SIZE as usize {
            return Err(format!(
                "Invalid schema field size: expected {} bytes, got {}",
                SCHEMA_FIELD_SIZE,
                vec.len()
            ));
        }

        // Extract name (read until null terminator or max 128 chars)
        let name_end = vec.iter().take(129).position(|&b| b == 0).unwrap_or(128);

        let name = match std::str::from_utf8(&vec[0..name_end]) {
            Ok(s) => s.to_string(),
            Err(e) => return Err(format!("Invalid UTF-8 in schema field name: {}", e)),
        };

        // Extract type from position 129
        let db_type = DbType::from_byte(vec[129]);
        let without_time_zone =
            db_type == DbType::DATETIME && vec[130] & TIMESTAMP_WITHOUT_TIME_ZONE != 0;

        // Extract size from position 131
        let size = u32::from_le_bytes([vec[131], vec[132], vec[133], vec[134]]);

        // Extract offset from position 135
        let offset = u64::from_le_bytes([
            vec[135], vec[136], vec[137], vec[138], vec[139], vec[140], vec[141], vec[142],
        ]);

        // Extract write_order from position 143
        let write_order = u64::from_le_bytes([
            vec[143], vec[144], vec[145], vec[146], vec[147], vec[148], vec[149], vec[150],
        ]);

        // Extract is_unique from position 151
        let is_unique = vec[151] != 0;
        let is_deleted = vec[152] != 0;

        Ok(SchemaField {
            name,
            db_type,
            without_time_zone,
            size,
            offset,
            write_order,
            is_unique,
            is_deleted,
            // Not part of the persisted record. `TableSchema::load` fills it in
            // from the type-modifier sidecar once every field has been read.
            scale: None,
            typmod_packed: None,
        })
    }

    pub async fn save_field(&self, io: &mut impl StorageIO) -> io::Result<()> {
        // A logged append committed here, so a write that fails (a full disk,
        // a file-size limit, a key/value put) is this call's error: the
        // immediate `append_data` only logged it, and a CREATE then reported
        // a column its files did not hold.
        append_committed(io, &self.into_vec()).await
    }
}

/// Appends `bytes` to the file `io` writes and commits it, returning the
/// append's or the commit's error: what the immediate `append_data` does,
/// except that its failure (a full disk, a file-size limit, a refused
/// key/value put) was only logged there. The commit is the deferred kind, as
/// before: durable with the statement's next log sync, or, on a key/value
/// store, once its family has stored it. It also lands any staged bytes of
/// the file (`commit_durable`), which the immediate append did not: harmless,
/// since schema and field files are only ever appended immediately and never
/// stage.
pub(crate) async fn append_committed<S: StorageIO>(io: &S, bytes: &[u8]) -> io::Result<()> {
    let txn = io.begin_txn();
    let (_, token) = io.append_data_txn(txn, bytes).await;
    io.commit_durable(token).await
}

#[derive(Debug, Clone, Default)]
pub struct SchemaCalculator;

impl SchemaCalculator {
    pub fn calculate_schema_offset(&self, field_name: &str, fields: &[SchemaField]) -> (u32, u64) {
        let mut offset = 0;

        for field in fields
            .iter()
            .sorted_by(|x, y| Ord::cmp(&x.write_order, &y.write_order))
        {
            if field.name == field_name {
                return (offset, field.write_order);
            }
            //println!("Field: {} - Offset: {}", field.name, offset);
            offset = offset.saturating_add(field.size as u32);
        }

        (0, 0)
    }
}

/// Iterates over SchemaFields from a StorageIO in 64KB chunks
pub struct SchemaFieldIterator<'a, S: StorageIO> {
    /// Reference to the storage provider
    io: &'a mut S,
    /// Current position in the storage
    position: u64,
    /// Buffer to store chunks
    buffer: MemoryBlock,
    /// Current index in the buffer
    buffer_index: usize,
    /// Valid data length in the buffer
    buffer_valid_length: usize,
    /// Total length of the storage
    total_length: usize,
    /// End of data flag
    end_of_data: bool,
}

impl<'a, S: StorageIO> SchemaFieldIterator<'a, S> {
    /// Create a new SchemaFieldIterator for the given StorageIO
    pub async fn new(io: &'a mut S) -> io::Result<Self> {
        let total_length = io.get_len().await as usize;

        let buffer_size = if total_length > SCHEMA_FIELD_CHUNK_SIZE {
            SCHEMA_FIELD_CHUNK_SIZE
        } else {
            total_length
        };

        let memory_block = MEMORY_POOL.acquire(buffer_size);

        let mut iterator = SchemaFieldIterator {
            io,
            position: 0,
            buffer: memory_block,
            buffer_index: 0,
            buffer_valid_length: 0,
            total_length,
            end_of_data: false,
        };

        // Load the first chunk of data
        iterator.load_next_chunk().await?;

        Ok(iterator)
    }

    /// Reset the iterator to the beginning of the data
    pub fn reset(&mut self) {
        self.position = 0;
        self.buffer_index = 0;
        self.buffer_valid_length = 0;
        self.end_of_data = false;
    }

    /// Load the next chunk of data into the buffer
    async fn load_next_chunk(&mut self) -> io::Result<()> {
        // Reset buffer index
        self.buffer_index = 0;

        // Check if we've reached the end of the storage
        if self.position >= self.total_length as u64 {
            self.end_of_data = true;
            return Ok(());
        }

        // Calculate how many bytes to read (may be less than CHUNK_SIZE at the end)
        let bytes_remaining = self.total_length as u64 - self.position;
        let bytes_to_read = std::cmp::min(bytes_remaining, SCHEMA_FIELD_CHUNK_SIZE as u64);

        // Get a buffer of appropriate size from the memory pool
        self.buffer = MEMORY_POOL.acquire(bytes_to_read as usize);

        // Read data into the buffer and track actual bytes read
        let mut read_position = self.position;
        let read_result = self
            .io
            .read_data_into_buffer(&mut read_position, &mut self.buffer.into_slice_mut())
            .await;

        // Compute how many bytes were actually read based on the advanced position
        let bytes_read = (read_position - self.position) as usize;
        self.buffer_valid_length = bytes_read;
        self.position = read_position;

        // If nothing was read, we're at EOF
        if bytes_read == 0 {
            self.end_of_data = true;
            return Ok(());
        }

        // Propagate any non-EOF errors; UnexpectedEof is acceptable as we track bytes_read
        if let Err(e) = read_result {
            if e.kind() != io::ErrorKind::UnexpectedEof {
                return Err(e);
            }
        }

        Ok(())
    }

    /// Get the next SchemaField from the buffer, loading a new chunk if necessary
    pub async fn next_schema_field(&mut self) -> io::Result<Option<SchemaField>> {
        loop {
            // If we've reached the end of the data, return None
            if self.end_of_data {
                return Ok(None);
            }

            // If we've reached the end of the current buffer, load the next chunk
            if self.buffer_index >= self.buffer_valid_length {
                self.load_next_chunk().await?;

                // If loading the next chunk reached the end of data, return None
                if self.end_of_data {
                    return Ok(None);
                }
            }

            // If the remaining buffer is smaller than a single field, load the next chunk
            if self.buffer_index + SCHEMA_FIELD_SIZE as usize > self.buffer_valid_length {
                self.load_next_chunk().await?;
                if self.end_of_data {
                    return Ok(None);
                }
            }

            // Extract the schema field data from the buffer
            let field_data = &self.buffer.into_slice()
                [self.buffer_index..self.buffer_index + SCHEMA_FIELD_SIZE as usize];

            // A record of zeros is no column (every one has a name): the
            // remains of an append a power cut kept the length of and dropped
            // the page of (`torn_tail::is_unwritten`). Read, it was a column
            // named "" at write order 0.
            if crate::core::torn_tail::is_unwritten(field_data) {
                self.buffer_index += SCHEMA_FIELD_SIZE as usize;
                continue;
            }

            // Parse the SchemaField from the buffer
            let schema_field = match SchemaField::from_vec(field_data) {
                Ok(field) => field,
                Err(_e) => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "Failed to parse SchemaField.",
                    ));
                }
            };

            // Advance the buffer index
            self.buffer_index += SCHEMA_FIELD_SIZE as usize;

            return Ok(Some(schema_field));
        }
    }

    /// Get multiple SchemaFields at once, up to BATCH_SIZE
    pub async fn next_schema_fields(&mut self) -> io::Result<SmallVec<[SchemaField; 20]>> {
        let mut fields = SmallVec::new();

        for _ in 0..BATCH_SIZE {
            match self.next_schema_field().await? {
                Some(field) => fields.push(field),
                None => break,
            }
        }

        Ok(fields)
    }

    /// Alias for next_schema_field to provide a more standard interface
    pub async fn next(&mut self) -> io::Result<Option<SchemaField>> {
        self.next_schema_field().await
    }
}

/// Every declaration list but `fields` is a `Vec`, not an inline `SmallVec`:
/// almost every table has none of them, and inline they were 2.4 KB of empty
/// slots in every schema — one per open table, and in every clone. See the
/// `table_schema_stays_small` test.
#[derive(Debug, Clone)]
pub struct TableSchema {
    pub name: String,
    pub fields: SmallVec<[SchemaField; 20]>,
    pub primary_key: Option<String>,
    pub is_immutable: bool,
    /// Foreign keys this table declares, loaded from its sidecar.
    pub foreign_keys: Vec<crate::core::rql::foreign_key::ForeignKey>,
    /// `DEFAULT` expressions, loaded from their sidecar.
    pub defaults: Vec<crate::core::rql::column_default::ColumnDefault>,
    /// Columns declared `JSON`/`JSONB`, each backed by its own table.
    pub json_columns: Vec<crate::core::rql::json_map::JsonColumn>,
    /// Indexes declared over a path into a document column.
    ///
    /// Read from a sidecar like the other declarations. Unlike them it changes
    /// no row layout — it says which paths the filter index should carry, and
    /// nothing else about the table depends on it.
    pub path_indexes: Vec<crate::core::rql::path_index::PathIndex>,
    /// Indexes declared by `CREATE INDEX`, loaded from their sidecar. The
    /// implicit ones a `PRIMARY KEY` or `UNIQUE` constraint brings are not
    /// recorded here: they are derived from the constraints when the table
    /// opens. See [`crate::core::index`].
    pub secondary_indexes: Vec<crate::core::index::IndexDecl>,
    /// Table-level `UNIQUE` constraints, composite ones included, loaded from
    /// their sidecar. A single-column `UNIQUE` written inline on a column is
    /// recorded on the field instead, as `SchemaField::is_unique`.
    pub unique_keys: Vec<crate::core::rql::unique_key::UniqueKey>,
    /// Declared precision and scale per column — the `(p, s)` of
    /// `NUMERIC(p, s)`, loaded from their sidecar. A column with no entry keeps
    /// the engine's default scale, which is what its rows were written with.
    pub type_modifiers: Vec<crate::core::rql::type_modifier::TypeModifier>,
    /// `CHECK` constraints, loaded from their sidecar.
    pub checks: Vec<crate::core::rql::check::CheckConstraint>,
    /// `GENERATED ALWAYS AS (…) STORED` expressions, loaded from their sidecar.
    ///
    /// Separate from `defaults` because the two differ in when they apply — see
    /// [`crate::core::rql::generated`].
    pub generated: Vec<crate::core::rql::generated::GeneratedColumn>,
    /// Which incarnation of the table under this name this is (C7): random,
    /// minted by [`TableSchema::new`], so a table created — or dropped and
    /// created again, which an `ALTER` rewrite does — gets a new one, and kept
    /// by every rewrite of the header (`RENAME`, `PRIMARY KEY`).
    ///
    /// Recorded after the header in `_schema.db` (see [`INCARNATION_LEN`]),
    /// where a binary that predates it never reads. `0` means none is
    /// recorded: a file written before it existed, or one whose record does not
    /// verify. Anything derived from the table and kept on disk (an index
    /// snapshot) records the incarnation it was taken from and is trusted only
    /// while it matches, and `0` never does.
    pub incarnation: u128,
}


impl TableSchema {
    pub fn new(name: String, is_immutable: bool) -> Self {
        TableSchema {
            name,
            fields: SmallVec::new(),
            primary_key: None,
            is_immutable,
            foreign_keys: Vec::new(),
            defaults: Vec::new(),
            json_columns: Vec::new(),
            path_indexes: Vec::new(),
            secondary_indexes: Vec::new(),
            unique_keys: Vec::new(),
            type_modifiers: Vec::new(),
            generated: Vec::new(),
            checks: Vec::new(),
            incarnation: mint_incarnation(),
        }
    }

    pub async fn load<S: StorageIO>(schema_io: Arc<S>) -> io::Result<Self> {
        // If the schema file is empty or too small, treat it as uninitialized.
        // This avoids "successfully" loading a zeroed buffer and ending up with an empty schema.
        let schema_len = schema_io.get_len().await;
        if schema_len < TABLE_SIZE as u64 {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                "Schema not initialized",
            ));
        }

        let mut buffer = MEMORY_POOL.acquire(TABLE_SIZE);
        schema_io
            .read_data_into_buffer(&mut 0, &mut buffer.into_slice_mut())
            .await?;

        let mut table_schema = match TableSchema::from_vec(&buffer.into_slice()) {
            Ok(s) => s,
            Err(e) => return Err(io::Error::new(io::ErrorKind::InvalidData, e)),
        };
        // The incarnation record after the header, if the file has one. A
        // record that is missing, short, unreadable or does not verify leaves
        // `0`, which nothing derived from the table is ever trusted against.
        if schema_len >= (TABLE_SIZE + INCARNATION_LEN) as u64 {
            let mut record = [0u8; INCARNATION_LEN];
            if schema_io
                .read_data_into_buffer(&mut (TABLE_SIZE as u64), &mut record)
                .await
                .is_ok()
            {
                table_schema.incarnation = parse_incarnation(&record).unwrap_or(0);
            }
        }
        let mut field_io = schema_io
            .create_new(format!(
                "{}_{}",
                schema_io.get_name().replace(".db", ""),
                "FIELDS.db"
            ))
            .await;
        let mut field_iterator = SchemaFieldIterator::new(&mut field_io).await?;

        let mut all_fields: SmallVec<[SchemaField; 20]> = SmallVec::new();

        while let Ok(mut fields) = field_iterator.next_schema_fields().await {
            if fields.is_empty() {
                break;
            }
            all_fields.append(&mut fields);
        }

        // Deduplicate by field name. The tie-break is load-bearing, and it used
        // to be backwards.
        //
        // A name can be held by both a tombstone and a live field: `RENAME
        // COLUMN` is allowed to rename *onto* a dropped column's name, which is
        // what makes the standard retype dance work (add `_tmp`, copy, drop the
        // original, rename `_tmp` back). That leaves two records with the same
        // name — one dead at the old offset with the old type and width, one
        // live at the new one.
        //
        // Keeping the earliest occurrence kept the *lowest* write_order, and the
        // lowest is the oldest, which for this collision is always the dead one.
        // So the table loaded its own pre-migration ancestor and every by-name
        // lookup — 25+ of them across the query path, none filtering
        // `is_deleted` — resolved to a field at the wrong offset with the wrong
        // type. Reads returned the migrated-away value; the first write after
        // that would put bytes at the wrong offset with the wrong width. Two
        // tables in the live deployment were in this state.
        //
        // So: prefer a live record over a tombstone, and the newest among
        // equals. Fixing it here repairs schemas already written that way, which
        // is why it is worth doing even though `RENAME COLUMN` no longer creates
        // the collision.
        let mut best: std::collections::HashMap<String, SchemaField> =
            std::collections::HashMap::new();
        for f in all_fields.into_iter() {
            match best.get(&f.name) {
                // `false < true`, so a live record outranks a tombstone, and a
                // higher write_order outranks a lower one within the same kind.
                Some(existing)
                    if (!f.is_deleted, f.write_order)
                        <= (!existing.is_deleted, existing.write_order) => {}
                _ => {
                    best.insert(f.name.clone(), f);
                }
            }
        }
        // Ordered by write_order as before: callers index this by position in
        // places that have not been taught otherwise.
        let mut deduped: SmallVec<[SchemaField; 20]> = best.into_values().collect();
        deduped.sort_by_key(|f| f.write_order);

        table_schema.fields = deduped;
        table_schema.foreign_keys = crate::core::rql::foreign_key::load(schema_io.clone()).await;
        table_schema.defaults = crate::core::rql::column_default::load(schema_io.clone()).await;
        table_schema.json_columns = crate::core::rql::json_map::load(schema_io.clone()).await;
        table_schema.path_indexes = crate::core::rql::path_index::load(schema_io.clone()).await;
        table_schema.secondary_indexes = crate::core::index::decl::load(schema_io.clone()).await;
        table_schema.unique_keys = crate::core::rql::unique_key::load(schema_io.clone()).await;
        table_schema.type_modifiers =
            crate::core::rql::type_modifier::load(schema_io.clone()).await;
        // Copied onto the fields, because the planner is handed the schema and
        // never sees the sidecar. See `SchemaField::scale`.
        for field in table_schema.fields.iter_mut() {
            field.scale = table_schema
                .type_modifiers
                .iter()
                .find(|m| m.column.eq_ignore_ascii_case(&field.name) && !m.is_deleted)
                .map(|m| m.scale as u32);
            // And the declared text length, for the same reason.
            let typmod = (!field.is_deleted)
                .then(|| crate::core::rql::type_modifier::text_typmod_of(&field.name, &table_schema.type_modifiers))
                .flatten();
            field.set_text_typmod(typmod);
        }
        table_schema.generated = crate::core::rql::generated::load(schema_io.clone()).await;
        table_schema.checks = crate::core::rql::check::load(schema_io.clone()).await;
        // A check this binary keeps unenforced, or cannot compile (stored by
        // an older one), is said in the log when the table opens: it was said
        // only to the session that declared it.
        for check in table_schema.checks.iter().filter(|c| !c.is_deleted) {
            match crate::core::rql::executor::check_status(&check.expression, &table_schema.fields) {
                Ok(None) => {}
                Ok(Some(why)) => crate::core::rql::executor::report_unenforced_once(&table_schema.name, &check.name, &why, true),
                Err(why) => crate::core::rql::executor::report_unenforced_once(&table_schema.name, &check.name, &why, false),
            }
        }

        Ok(table_schema)
    }

    pub fn into_vec(&self) -> Vec<u8> {
        // Always create a buffer of TABLE_SIZE (129*2) bytes, initialized with zeros
        let mut vec = vec![0; TABLE_SIZE];

        // The table's name as it is, UTF-8, cut only at a character boundary
        // within 128 bytes, as the column names are.
        let name_bytes = self.name.as_bytes();
        let mut copy_len = name_bytes.len().min(128);
        while !self.name.is_char_boundary(copy_len) {
            copy_len -= 1;
        }
        vec[..copy_len].copy_from_slice(&name_bytes[..copy_len]);
        // The null terminator is already there from initializing with zeros

        // Primary key bytes (starting at position 129): the column's name as
        // it is, UTF-8, cut only at a character boundary within 128 bytes.
        // It must name the column exactly. Filtered to ASCII letters and
        // digits, as it was, `"日本"` was recorded as "" and `naïve` as
        // `nave`, and from the next open every insert was refused as a NULL
        // in a column of that name. A header written that way reads back as
        // written.
        if let Some(primary_key) = &self.primary_key {
            let pk_bytes = primary_key.as_bytes();
            let mut pk_copy_len = pk_bytes.len().min(128);
            while !primary_key.is_char_boundary(pk_copy_len) {
                pk_copy_len -= 1;
            }
            vec[129..(129 + pk_copy_len)].copy_from_slice(&pk_bytes[..pk_copy_len]);
            // The null terminator is already there from initializing with zeros
        }
        // If no primary key, this section remains all zeros

        vec[129 + 129] = self.is_immutable as u8; // Null terminator for primary key

        vec
    }

    pub fn from_vec(vec: &[u8]) -> Result<Self, String> {
        if vec.len() != TABLE_SIZE {
            return Err(format!(
                "Invalid table schema size: expected {} bytes, got {}",
                TABLE_SIZE,
                vec.len()
            ));
        }

        // Extract table name (read until null terminator or max 128 chars)
        let name_end = vec.iter().take(129).position(|&b| b == 0).unwrap_or(128);
        let name = match std::str::from_utf8(&vec[0..name_end]) {
            Ok(s) => s.to_string(),
            Err(e) => return Err(format!("Invalid UTF-8 in table name: {}", e)),
        };

        // Extract primary key (read until null terminator, starting at position 129)
        // Check if the first byte of the primary key section is 0 (no primary key)
        let primary_key = if vec[129] == 0 {
            None
        } else {
            // Find the null terminator in the primary key section
            let pk_end = vec[129..].iter().position(|&b| b == 0).unwrap_or(128);
            match std::str::from_utf8(&vec[129..(129 + pk_end)]) {
                Ok(s) if !s.is_empty() => Some(s.to_string()),
                _ => None,
            }
        };

        let immutable = if 0 == vec[129 + 129] { false } else { true };

        Ok(TableSchema {
            name,
            fields: SmallVec::new(), // Fields are stored separately
            primary_key,
            is_immutable: immutable,
            // Foreign keys and defaults live in their own sidecars; `load`
            // fills them in.
            foreign_keys: Vec::new(),
            defaults: Vec::new(),
            json_columns: Vec::new(),
            path_indexes: Vec::new(),
            secondary_indexes: Vec::new(),
            unique_keys: Vec::new(),
            type_modifiers: Vec::new(),
            generated: Vec::new(),
            checks: Vec::new(),
            // Not part of the header; `load` reads it from the record after.
            incarnation: 0,
        })
    }

    pub async fn save<S: StorageIO>(&self, schema_io: Arc<S>) -> io::Result<()> {
        let mut data = self.into_vec();
        // In the same append as the header, so a schema file never holds a
        // header whose record is still to come.
        if self.incarnation != 0 {
            data.extend_from_slice(&incarnation_record(self.incarnation));
        }
        // Committed here, so a failed write is the caller's error rather than
        // a log line (`append_committed`).
        append_committed(&*schema_io, &data).await
    }

    /// Overwrites the header record without appending a second one.
    ///
    /// [`save`](Self::save) appends, which is right the first time and wrong
    /// every time after: the file is a `TABLE_SIZE` header followed by field
    /// records, so a second append would be read back as a malformed field. The
    /// header is fixed-width and lives at offset zero, so writing over it is
    /// exact, and leaves the incarnation record after it as it was.
    pub async fn rewrite_header<S: StorageIO>(&self, schema_io: Arc<S>) -> io::Result<()> {
        let data = self.into_vec();
        schema_io.write_data(0, &data).await;
        Ok(())
    }

    /// [`Self::try_add_field`] for a caller with nowhere to send its error,
    /// which is logged. The field is kept only once its record is written.
    pub async fn add_field<S: StorageIO>(
        &mut self,
        schema_io: Arc<S>,
        field_name: String,
        db_type: DbType,
        is_unique: bool,
    ) {
        if let Err(e) = self.try_add_field(schema_io, field_name.clone(), db_type, is_unique).await {
            log::error!("column {field_name} of {} could not be recorded: {e}", self.name);
        }
    }

    /// Adds a field and writes its record, returning the write's error. The
    /// field joins `self.fields` only once its record is written: a schema
    /// holding a column its file does not would lose it at the next open.
    pub async fn try_add_field<S: StorageIO>(
        &mut self,
        schema_io: Arc<S>,
        field_name: String,
        db_type: DbType,
        is_unique: bool,
    ) -> io::Result<()> {
        let mut field_io = schema_io
            .create_new(format!(
                "{}_{}",
                schema_io.get_name().replace(".db", ""),
                "FIELDS.db"
            ))
            .await;

        // `self.fields` is the authority for what this schema already holds:
        // `load` populates it from storage, and each `add_field` appends to it.
        // Re-reading the field file here instead would depend on the append
        // being visible through a freshly opened handle, which the in-memory
        // backend does not guarantee — and when that read came back empty every
        // column was assigned `write_order` 0 and `offset` 0. Identical write
        // orders then made `schema_id` useless: two different columns compared
        // equal, so a SELECT could be served the WHERE clause's columns instead
        // of the ones it asked for.
        if self.fields.iter().any(|f| f.name == field_name) {
            return Ok(());
        }

        let offset: u64 = self.fields.iter().map(|f| f.size as u64).sum();
        let write_order: u64 = self
            .fields
            .iter()
            .map(|f| f.write_order)
            .max()
            .map(|m| m + 1)
            .unwrap_or(0);

        let field = SchemaField::new(
            field_name,
            db_type.clone(),
            db_type.get_size(),
            offset,
            write_order,
            is_unique,
        );

        field.save_field(&mut field_io).await?;
        self.fields.push(field);
        Ok(())
    }

    /// Renames a field in the schema and in its persisted record.
    ///
    /// Nothing in a row moves: the layout is keyed by `write_order`, and the
    /// name is metadata. The record is rewritten in place at its own offset,
    /// so the file keeps its fixed-stride shape.
    pub async fn rename_field<S: StorageIO>(
        &mut self,
        from: &str,
        to: &str,
        schema_io: Arc<S>,
    ) -> Result<(), String> {
        let filtered: String = to
            .chars()
            .filter(|c| c.is_ascii_alphanumeric() || *c == '_')
            .collect();
        if filtered != to {
            return Err(format!(
                "column name '{}' contains characters that cannot be stored",
                to
            ));
        }
        if to.len() > 128 {
            return Err("column names are limited to 128 characters".to_string());
        }

        let mut field_io = schema_io
            .create_new(format!(
                "{}_{}",
                schema_io.get_name().replace(".db", ""),
                "FIELDS.db"
            ))
            .await;

        let mut position = 0u64;
        let mut found = false;
        let mut all_fields: SmallVec<[SchemaField; 20]> = SmallVec::new();
        {
            let mut iterator = SchemaFieldIterator::new(&mut field_io)
                .await
                .map_err(|e| e.to_string())?;
            while let Ok(mut fields) = iterator.next_schema_fields().await {
                if fields.is_empty() {
                    break;
                }
                all_fields.append(&mut fields);
            }
        }

        for field in all_fields.iter_mut() {
            if field.name.eq_ignore_ascii_case(from) {
                field.name = to.to_string();
                let vec = field.into_vec();
                // Through the log, not `write_data_seek`. The record being
                // overwritten went in as a logged append, and every open
                // replays the log: an unlogged overwrite was undone by that
                // replay, so a rename came back under the old name after a
                // restart — with the defaults, scale and companion already
                // moved to the new one.
                field_io.write_data(position, &vec).await;
                found = true;
                break;
            }
            position += SCHEMA_FIELD_SIZE;
        }

        if !found {
            return Err(format!("column '{}' does not exist", from));
        }

        for field in self.fields.iter_mut() {
            if field.name.eq_ignore_ascii_case(from) {
                field.name = to.to_string();
            }
        }

        Ok(())
    }

    pub fn set_primary_key(&mut self, primary_key: String) {
        self.primary_key = Some(primary_key);
    }

    /// Clears a column's `UNIQUE` flag, persisting it the way
    /// `mark_field_as_deleted` persists a drop.
    ///
    /// A column declared `UNIQUE` inline carries the constraint as this flag
    /// rather than as a named sidecar, so dropping it by name needs a way to
    /// turn it off.
    pub async fn set_field_unique<S: StorageIO>(
        &mut self,
        field_name: &str,
        unique: bool,
        schema_io: Arc<S>,
    ) {
        let mut field_io = schema_io
            .create_new(format!(
                "{}_{}",
                schema_io.get_name().replace(".db", ""),
                "FIELDS.db"
            ))
            .await;
        let mut field_iterator = SchemaFieldIterator::new(&mut field_io).await.unwrap();
        let mut all_fields: SmallVec<[SchemaField; 20]> = SmallVec::new();
        while let Ok(mut fields) = field_iterator.next_schema_fields().await {
            if fields.is_empty() {
                break;
            }
            all_fields.append(&mut fields);
        }
        let mut position = 0;
        for field in all_fields.iter_mut() {
            if field.name.eq_ignore_ascii_case(field_name) {
                field.is_unique = unique;
                let vec = field.into_vec();
                // Logged, as `rename_field` explains: unlogged, the replay at
                // the next open restored the flag and the dropped UNIQUE was
                // enforced again.
                field_io.write_data(position, &vec).await;
                break;
            }
            position += SCHEMA_FIELD_SIZE;
        }
        self.fields = all_fields;
    }

    pub async fn mark_field_as_deleted<S: StorageIO>(
        &mut self,
        field_name: &str,
        schema_io: Arc<S>,
    ) {
        // Reload fields from disk to ensure we have the latest state
        let mut field_io = schema_io
            .create_new(format!(
                "{}_{}",
                schema_io.get_name().replace(".db", ""),
                "FIELDS.db"
            ))
            .await;
        let mut field_iterator = SchemaFieldIterator::new(&mut field_io).await.unwrap();
        let mut all_fields: SmallVec<[SchemaField; 20]> = SmallVec::new();
        while let Ok(mut fields) = field_iterator.next_schema_fields().await {
            if fields.is_empty() {
                break;
            }
            all_fields.append(&mut fields);
        }
        let mut position = 0;
        for field in all_fields.iter_mut() {
            if field.name == field_name {
                field.is_deleted = true;
                let vec = field.into_vec();
                // Logged, as `rename_field` explains: unlogged, the replay at
                // the next open put the original record back and the dropped
                // column returned.
                field_io.write_data(position, &vec).await;
                break;
            }
            position += SCHEMA_FIELD_SIZE;
        }
        // Update self.fields to reflect the change
        self.fields = all_fields;
    }
}

/// Iterates over TableSchema objects from a StorageIO in chunks
pub struct TableSchemaIterator<S: StorageIO> {
    /// Reference to the storage provider
    io: Arc<S>,
    /// Current position in the storage
    position: u64,
    /// Buffer to store chunks
    buffer: MemoryBlock,
    /// Current index in the buffer
    buffer_index: usize,
    /// Valid data length in the buffer
    buffer_valid_length: usize,
    /// Total length of the storage
    total_length: usize,
    /// End of data flag
    end_of_data: bool,
}

impl<S: StorageIO> TableSchemaIterator<S> {
    /// Create a new TableSchemaIterator for the given StorageIO
    pub async fn new(io: Arc<S>) -> io::Result<Self> {
        let total_length = io.get_len().await as usize;

        // Use a reasonable chunk size (multiple of TABLE_SIZE)
        let chunk_size = 100 * TABLE_SIZE;

        let buffer_size = if total_length > chunk_size {
            chunk_size
        } else {
            total_length
        };

        let memory_block = MEMORY_POOL.acquire(buffer_size);

        let mut iterator = TableSchemaIterator {
            io,
            position: 0,
            buffer: memory_block,
            buffer_index: 0,
            buffer_valid_length: 0,
            total_length,
            end_of_data: false,
        };

        // Load the first chunk of data
        iterator.load_next_chunk().await?;

        Ok(iterator)
    }

    /// Reset the iterator to the beginning of the data
    pub fn reset(&mut self) {
        self.position = 0;
        self.buffer_index = 0;
        self.buffer_valid_length = 0;
        self.end_of_data = false;
    }

    /// Load the next chunk of data into the buffer
    async fn load_next_chunk(&mut self) -> io::Result<()> {
        // Reset buffer index
        self.buffer_index = 0;

        // Check if we've reached the end of the storage
        if self.position >= self.total_length as u64 {
            self.end_of_data = true;
            return Ok(());
        }

        // Calculate how many bytes to read
        let bytes_remaining = self.total_length as u64 - self.position;
        let chunk_size = 100 * TABLE_SIZE;
        let bytes_to_read = std::cmp::min(bytes_remaining, chunk_size as u64);

        // Get a buffer of appropriate size from the memory pool
        self.buffer = MEMORY_POOL.acquire(bytes_to_read as usize);

        // Read data into the buffer and track actual bytes read
        let mut read_position = self.position;
        let read_result = self
            .io
            .read_data_into_buffer(&mut read_position, &mut self.buffer.into_slice_mut())
            .await;

        let bytes_read = (read_position - self.position) as usize;
        self.buffer_valid_length = bytes_read;
        self.position = read_position;

        if bytes_read == 0 {
            self.end_of_data = true;
            return Ok(());
        }

        if let Err(e) = read_result {
            if e.kind() != io::ErrorKind::UnexpectedEof {
                return Err(e);
            }
        }

        Ok(())
    }

    /// Get the next TableSchema from the buffer, loading a new chunk if necessary
    pub async fn next_table_schema(&mut self) -> io::Result<Option<TableSchema>> {
        // If we've reached the end of the data, return None
        if self.end_of_data {
            return Ok(None);
        }

        // A header and the incarnation record after it are read from one
        // buffer. Records put headers off the chunk grid, so when this buffer
        // holds less than both and the file has more, the next chunk starts at
        // the first unread byte rather than past the end of this buffer, which
        // would cut whatever straddles the boundary.
        if self.buffer_index + TABLE_SIZE + INCARNATION_LEN > self.buffer_valid_length
            && self.position < self.total_length as u64
        {
            self.position -= (self.buffer_valid_length - self.buffer_index) as u64;
            self.load_next_chunk().await?;
            if self.end_of_data {
                return Ok(None);
            }
        }

        // A record this buffer holds without its header (one that did not
        // verify after it) is stepped over; no header starts with that byte.
        if self.buffer.into_slice()[..self.buffer_valid_length].get(self.buffer_index)
            == Some(&INCARNATION_MAGIC[0])
        {
            self.buffer_index =
                (self.buffer_index + INCARNATION_LEN).min(self.buffer_valid_length);
        }

        // Fewer bytes than a header left in the file: the end.
        if self.buffer_index + TABLE_SIZE > self.buffer_valid_length {
            self.end_of_data = true;
            return Ok(None);
        }

        // Extract the table schema data from the buffer
        let schema_data =
            &self.buffer.into_slice()[self.buffer_index..self.buffer_index + TABLE_SIZE];

        // Parse the TableSchema from the buffer
        let mut table_schema = match TableSchema::from_vec(schema_data) {
            Ok(schema) => schema,
            Err(_e) => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "Failed to parse TableSchema.",
                ));
            }
        };

        table_schema.fields.clear();

        let field_io = self
            .io
            .create_new(format!(
                "{}_{}",
                self.io.get_name().replace(".db", ""),
                "FIELDS.db"
            ))
            .await;

        // Load all fields associated with this table schema using a direct read
        let total_len = field_io.get_len().await as usize;
        if total_len > 0 {
            let mut pos = 0u64;
            let mut buf = vec![0u8; total_len];
            let _ = field_io.read_data_into_buffer(&mut pos, &mut buf).await;
            let mut idx = 0usize;
            while idx + SCHEMA_FIELD_SIZE as usize <= total_len {
                if let Ok(field) =
                    SchemaField::from_vec(&buf[idx..idx + SCHEMA_FIELD_SIZE as usize])
                {
                    table_schema.fields.push(field);
                }
                idx += SCHEMA_FIELD_SIZE as usize;
            }
        }

        // Deduplicate by field name, keep earliest write_order and stable order
        if !table_schema.fields.is_empty() {
            let mut seen = std::collections::HashSet::new();
            let mut sorted: Vec<SchemaField> = table_schema.fields.drain(..).collect();
            sorted.sort_by_key(|f| f.write_order);
            for f in sorted.into_iter() {
                if seen.insert(f.name.clone()) {
                    table_schema.fields.push(f);
                }
            }
        }

        // Advance the buffer index, past the incarnation record too if one
        // follows.
        self.buffer_index += TABLE_SIZE;
        if let Some(incarnation) = parse_incarnation(
            &self.buffer.into_slice()[self.buffer_index..self.buffer_valid_length],
        ) {
            table_schema.incarnation = incarnation;
            self.buffer_index += INCARNATION_LEN;
        }

        Ok(Some(table_schema))
    }

    /// Alias for next_table_schema to provide a more standard interface
    pub async fn next(&mut self) -> io::Result<Option<TableSchema>> {
        self.next_table_schema().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// One `TableSchema` per open table, and one more in every clone. With
    /// nine inline `SmallVec`s of declarations it was 3720 bytes, about 2.4 KB
    /// of that empty slots, since almost no table declares any of them. 1408
    /// before the 16-byte incarnation; 1744 since each of the twenty inline
    /// fields carries its declared text length (`SchemaField::text_typmod`, 8
    /// bytes) and `DbType` grew to 4 bytes for `BPCHAR`'s length.
    #[test]
    fn table_schema_stays_small() {
        let size = std::mem::size_of::<TableSchema>();
        assert!(size <= 1744, "TableSchema grew to {size} bytes");
        assert!(std::mem::size_of::<SchemaField>() <= 64, "SchemaField grew");
    }
    use crate::core::{db_type::DbType, storage_providers::file_sync::LocalStorageProvider};
    use std::{fs, io, path::Path};

    // Helper function to create a schema field and return it
    fn create_test_schema_field(
        name: &str,
        db_type: DbType,
        offset: u64,
        write_order: u64,
        is_unique: bool,
    ) -> SchemaField {
        let db_size = db_type.get_size();
        let schema_field = SchemaField::new(
            name.to_string(),
            db_type,
            db_size,
            offset,
            write_order,
            is_unique,
        );

        schema_field
    }

    // Helper function to save a schema field to storage
    async fn save_schema_field(
        field: &SchemaField,
        storage: &mut LocalStorageProvider,
    ) -> io::Result<()> {
        field.save_field(storage).await
    }

    fn clean_storage(temp_loc: &Path, table_name: &str) {
        let main_path = temp_loc.join(table_name);
        let _ = fs::remove_file(&main_path);
        let tmp_path = temp_loc.join(format!("{}.tmp", table_name));
        let _ = fs::remove_file(tmp_path);
    }

    async fn make_clean_provider(temp_loc: &Path, table_name: &str) -> LocalStorageProvider {
        clean_storage(temp_loc, table_name);
        LocalStorageProvider::new(temp_loc.to_str().unwrap(), Some(table_name)).await
    }

    #[tokio::test]
    async fn test_schema_field_iterator_empty() {
        let temp_loc = std::env::temp_dir().join("test_schema_field_iterator_empty");

        let mut mock_io =
            make_clean_provider(temp_loc.as_path(), "test_schema_field_iterator_empty").await;

        let mut iterator = SchemaFieldIterator::new(&mut mock_io).await.unwrap();

        // Should return None for an empty storage
        assert!(iterator.next_schema_field().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_schema_field_iterator_single_field() {
        // Create a single schema field
        let schema_field = create_test_schema_field("id", DbType::I32, 0, 1, true);

        let temp_loc = std::env::temp_dir().join("test_schema_field_iterator_single_field");

        let mut mock_io = make_clean_provider(
            temp_loc.as_path(),
            "test_schema_field_iterator_single_field",
        )
        .await;

        save_schema_field(&schema_field, &mut mock_io)
            .await
            .unwrap();

        let mut schema_field_iterator = SchemaFieldIterator::new(&mut mock_io).await.unwrap();

        // Should return our schema field
        let read_field = schema_field_iterator
            .next_schema_field()
            .await
            .unwrap()
            .unwrap();

        assert_eq!(read_field.name, "id");
        assert_eq!(read_field.db_type, DbType::I32);
        assert_eq!(read_field.offset, 0);
        assert_eq!(read_field.write_order, 1);
        assert_eq!(read_field.is_unique, true);

        // Next call should return None
        assert!(
            schema_field_iterator
                .next_schema_field()
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn test_schema_field_iterator_2_fields() {
        // Create schema fields
        let schema_field1 = create_test_schema_field("id", DbType::I32, 0, 1, true);

        let temp_loc = std::env::temp_dir().join("test_schema_field_iterator_2_fields");

        let mut mock_io =
            make_clean_provider(temp_loc.as_path(), "test_schema_field_iterator_2_fields").await;

        save_schema_field(&schema_field1, &mut mock_io)
            .await
            .unwrap();
        save_schema_field(&schema_field1, &mut mock_io)
            .await
            .unwrap();

        let mut schema_field_iterator = SchemaFieldIterator::new(&mut mock_io).await.unwrap();

        // Should return our first schema field
        let read_field = schema_field_iterator
            .next_schema_field()
            .await
            .unwrap()
            .unwrap();

        assert_eq!(read_field.name, "id");
        assert_eq!(read_field.db_type, DbType::I32);
        assert_eq!(read_field.offset, 0);
        assert_eq!(read_field.write_order, 1);
        assert_eq!(read_field.is_unique, true);

        // Should return the second copy of the same schema field
        assert!(
            schema_field_iterator
                .next_schema_field()
                .await
                .unwrap()
                .is_some()
        );

        // Next call should return None
        assert!(
            schema_field_iterator
                .next_schema_field()
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn test_schema_field_iterator_3_fields() {
        // Create schema fields
        let schema_field1 = create_test_schema_field("id", DbType::I32, 0, 1, true);
        let schema_field2 = create_test_schema_field("name", DbType::STRING, 8, 2, false);

        let temp_loc = std::env::temp_dir().join("test_schema_field_iterator_3_fields");

        let mut mock_io =
            make_clean_provider(temp_loc.as_path(), "test_schema_field_iterator_3_fields").await;

        save_schema_field(&schema_field1, &mut mock_io)
            .await
            .unwrap();
        save_schema_field(&schema_field2, &mut mock_io)
            .await
            .unwrap();
        save_schema_field(&schema_field1, &mut mock_io)
            .await
            .unwrap();

        let mut schema_field_iterator = SchemaFieldIterator::new(&mut mock_io).await.unwrap();

        // Should return our first schema field
        let read_field = schema_field_iterator
            .next_schema_field()
            .await
            .unwrap()
            .unwrap();

        assert_eq!(read_field.name, "id");
        assert_eq!(read_field.db_type, DbType::I32);
        assert_eq!(read_field.offset, 0);
        assert_eq!(read_field.write_order, 1);
        assert_eq!(read_field.is_unique, true);

        // Should return the second schema field
        let read_field = schema_field_iterator
            .next_schema_field()
            .await
            .unwrap()
            .unwrap();

        assert_eq!(read_field.name, "name");
        assert_eq!(read_field.db_type, DbType::STRING);
        assert_eq!(read_field.offset, 8);
        assert_eq!(read_field.write_order, 2);
        assert_eq!(read_field.is_unique, false);

        // Should return the third schema field (duplicate of the first)
        assert!(
            schema_field_iterator
                .next_schema_field()
                .await
                .unwrap()
                .is_some()
        );

        // Next call should return None
        assert!(
            schema_field_iterator
                .next_schema_field()
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn test_schema_field_iterator_batch_retrieval() {
        let temp_loc = std::env::temp_dir().join("test_schema_field_iterator_batch_retrieval");

        let mut mock_io = make_clean_provider(
            temp_loc.as_path(),
            "test_schema_field_iterator_batch_retrieval",
        )
        .await;

        // Create and save multiple schema fields (more than BATCH_SIZE)
        for i in 0..BATCH_SIZE + 10 {
            let field_name = format!("field{}", i);
            let schema_field = create_test_schema_field(
                &field_name,
                DbType::I32,
                i as u64 * 8,
                i as u64,
                i % 2 == 0, // Make even-numbered fields unique
            );

            save_schema_field(&schema_field, &mut mock_io)
                .await
                .unwrap();
        }

        let mut iterator = SchemaFieldIterator::new(&mut mock_io).await.unwrap();

        // Test batch retrieval
        let first_batch = iterator.next_schema_fields().await.unwrap();
        assert_eq!(first_batch.len(), BATCH_SIZE);

        // Check first and last items in the batch
        assert_eq!(first_batch[0].name, "field0");
        assert_eq!(first_batch[0].is_unique, true);
        assert_eq!(
            first_batch[BATCH_SIZE - 1].name,
            format!("field{}", BATCH_SIZE - 1)
        );

        // Test retrieval of remaining items
        let second_batch = iterator.next_schema_fields().await.unwrap();
        assert_eq!(second_batch.len(), 10);

        // Ensure no more items
        let empty_batch = iterator.next_schema_fields().await.unwrap();
        assert_eq!(empty_batch.len(), 0);
    }

    #[tokio::test]
    async fn test_schema_field_iterator_reset() {
        let temp_loc = std::env::temp_dir().join("test_schema_field_iterator_reset");

        let mut mock_io =
            make_clean_provider(temp_loc.as_path(), "test_schema_field_iterator_reset").await;

        // Create and save schema fields
        let schema_field1 = create_test_schema_field("id", DbType::I32, 0, 1, true);
        let schema_field2 = create_test_schema_field("name", DbType::STRING, 8, 2, false);

        save_schema_field(&schema_field1, &mut mock_io)
            .await
            .unwrap();
        save_schema_field(&schema_field2, &mut mock_io)
            .await
            .unwrap();

        // Create iterator and consume all items
        let mut iterator = SchemaFieldIterator::new(&mut mock_io).await.unwrap();

        assert!(iterator.next_schema_field().await.unwrap().is_some());
        assert!(iterator.next_schema_field().await.unwrap().is_some());
        assert!(iterator.next_schema_field().await.unwrap().is_none());

        // Reset and verify we can read the items again
        iterator.reset();

        // Should be able to read the items again
        let read_field1 = iterator.next_schema_field().await.unwrap().unwrap();
        assert_eq!(read_field1.name, "id");

        let read_field2 = iterator.next_schema_field().await.unwrap().unwrap();
        assert_eq!(read_field2.name, "name");

        // No more items
        assert!(iterator.next_schema_field().await.unwrap().is_none());
    }

    // Helper function to create a test TableSchema
    fn create_test_table_schema(name: &str, primary_key: Option<&str>) -> TableSchema {
        let mut table = TableSchema::new(name.to_string(), false);
        if let Some(pk) = primary_key {
            table.set_primary_key(pk.to_string());
        }
        table
    }

    // Helper function to create a test TableSchema
    fn create_test_table_schema_immutable(name: &str, primary_key: Option<&str>) -> TableSchema {
        let mut table = TableSchema::new(name.to_string(), true);
        if let Some(pk) = primary_key {
            table.set_primary_key(pk.to_string());
        }
        table
    }

    // Helper function to save a table schema to storage
    async fn save_table_schema<S: StorageIO>(
        schema: &TableSchema,
        storage: Arc<S>,
    ) -> io::Result<()> {
        schema.save(storage).await
    }

    #[test]
    fn test_table_schema_into_vec() {
        // Table with no primary key
        let schema = create_test_table_schema("users", None);
        let vec = schema.into_vec();

        // Verify size
        assert_eq!(vec.len(), TABLE_SIZE);

        // Verify name
        let name_bytes = "users".as_bytes();
        for (i, &b) in name_bytes.iter().enumerate() {
            assert_eq!(vec[i], b);
        }

        // Verify null terminator after name
        assert_eq!(vec[5], 0);

        // Verify primary key section is all zeros
        for i in 129..TABLE_SIZE {
            assert_eq!(vec[i], 0);
        }

        // Table with primary key
        let schema = create_test_table_schema("customers", Some("id"));
        let vec = schema.into_vec();

        // Verify name
        let name_bytes = "customers".as_bytes();
        for (i, &b) in name_bytes.iter().enumerate() {
            assert_eq!(vec[i], b);
        }

        // Verify primary key
        let pk_bytes = "id".as_bytes();
        for (i, &b) in pk_bytes.iter().enumerate() {
            assert_eq!(vec[129 + i], b);
        }

        // Verify null terminator after primary key
        assert_eq!(vec[131], 0);
    }

    #[test]
    fn test_table_schema_from_vec() {
        // Create a buffer for a table with name "users" and primary key "id"
        let mut vec = vec![0; TABLE_SIZE];

        // Set name
        let name = "users";
        let name_bytes = name.as_bytes();
        vec[..name_bytes.len()].copy_from_slice(name_bytes);

        // Set primary key
        let pk = "id";
        let pk_bytes = pk.as_bytes();
        vec[129..129 + pk_bytes.len()].copy_from_slice(pk_bytes);

        // Parse the buffer
        let schema = TableSchema::from_vec(&vec).unwrap();

        // Verify parsed data
        assert_eq!(schema.name, "users");
        assert_eq!(schema.primary_key, Some("id".to_string()));

        // Create a buffer for a table with name "products" and no primary key
        let mut vec = vec![0; TABLE_SIZE];

        // Set name
        let name = "products";
        let name_bytes = name.as_bytes();
        vec[..name_bytes.len()].copy_from_slice(name_bytes);

        // Parse the buffer
        let schema = TableSchema::from_vec(&vec).unwrap();

        // Verify parsed data
        assert_eq!(schema.name, "products");
        assert_eq!(schema.primary_key, None);
    }

    #[tokio::test]
    async fn test_table_schema_serialize_deserialize() {
        let temp_loc = std::env::temp_dir().join("test_table_schema_serialize_deserialize");

        let mock_io = LocalStorageProvider::new(
            temp_loc.to_str().unwrap(),
            Some("test_table_schema_serialize_deserialize"),
        )
        .await;

        let mock_io = Arc::new(mock_io);

        // Create schema with fields and indexes
        let mut schema = create_test_table_schema("orders", Some("order_id"));

        schema.save(mock_io.clone()).await.unwrap();

        // Add fields

        schema
            .add_field(mock_io.clone(), "order_id".to_string(), DbType::I64, false)
            .await;

        schema
            .add_field(
                mock_io.clone(),
                "created_at".to_string(),
                DbType::U64,
                false,
            )
            .await;

        // Serialize
        let vec = schema.into_vec();

        // Deserialize
        let deserialized = TableSchema::from_vec(&vec).unwrap();

        // Verify core properties (fields are not part of serialization)
        assert_eq!(deserialized.name, "orders");
        assert_eq!(deserialized.primary_key, Some("order_id".to_string()));
        assert!(deserialized.fields.is_empty());
    }

    #[tokio::test]
    async fn test_table_schema_mutability() {
        let temp_loc = std::env::temp_dir().join("test_table_schema_mutability");

        let mock_io = LocalStorageProvider::new(
            temp_loc.to_str().unwrap(),
            Some("test_table_schema_mutability"),
        )
        .await;

        let mock_io = Arc::new(mock_io);

        // Create schema with fields and indexes
        let schema = create_test_table_schema_immutable("orders", Some("order_id"));

        schema.save(mock_io.clone()).await.unwrap();

        let mut schema_iter = TableSchemaIterator::new(mock_io.clone()).await.unwrap();

        let schema = schema_iter.next_table_schema().await.unwrap().unwrap();

        // Verify core properties (fields are not part of serialization)
        assert_eq!(schema.is_immutable, true);
    }

    #[tokio::test]
    async fn test_table_schema_mutability_2() {
        let temp_loc = std::env::temp_dir().join("test_table_schema_mutability_2");

        let mock_io = LocalStorageProvider::new(
            temp_loc.to_str().unwrap(),
            Some("test_table_schema_mutability_2"),
        )
        .await;

        let mock_io = Arc::new(mock_io);

        // Create schema with fields and indexes
        let schema = create_test_table_schema("orders", Some("order_id"));

        schema.save(mock_io.clone()).await.unwrap();

        let mut schema_iter = TableSchemaIterator::new(mock_io.clone()).await.unwrap();

        let schema = schema_iter.next_table_schema().await.unwrap().unwrap();

        // Verify core properties (fields are not part of serialization)
        assert_eq!(schema.is_immutable, false);
    }

    /// A column's name reads back exactly as written: a quoted identifier may
    /// hold any character, and the record is what every open reads it from.
    #[test]
    fn a_column_name_reads_back_as_written() {
        for name in ["naïve", "日本", "with space", "a-b", &"é".repeat(70)] {
            let field = create_test_schema_field(name, DbType::I32, 0, 0, false);
            let back = SchemaField::from_vec(&field.into_vec()).unwrap().name;
            if name.len() <= 128 {
                assert_eq!(back, name);
            } else {
                assert_eq!(back, "é".repeat(64));
            }
        }
    }

    /// A `timestamp` column's flag rides in the record's spare type byte: it
    /// reads back, a `timestamptz` and every other type read back without it,
    /// and the low byte an earlier build reads the type from is unchanged
    /// (`DATETIME`, 15), so such a build opens the table as the `timestamptz`
    /// it always took the column for.
    #[test]
    fn a_timestamp_column_keeps_its_flag_in_the_record() {
        let ts = SchemaField::new("at".into(), DbType::TIMESTAMP, 8, 0, 0, false);
        assert_eq!((ts.db_type.clone(), ts.without_time_zone), (DbType::DATETIME, true));
        let bytes = ts.into_vec();
        assert_eq!((bytes[129], bytes[130]), (15, 1));
        let back = SchemaField::from_vec(&bytes).unwrap();
        assert_eq!((back.db_type.clone(), back.without_time_zone), (DbType::DATETIME, true));
        assert_eq!(back.literal_type(), DbType::TIMESTAMP);

        let tz = SchemaField::new("at".into(), DbType::DATETIME, 8, 0, 0, false);
        let bytes = tz.into_vec();
        assert_eq!((bytes[129], bytes[130]), (15, 0));
        let back = SchemaField::from_vec(&bytes).unwrap();
        assert!(!back.without_time_zone);
        assert_eq!(back.literal_type(), DbType::DATETIME);

        // The bit means nothing on another type.
        let mut other = SchemaField::new("n".into(), DbType::I64, 8, 0, 0, false).into_vec();
        other[130] = 1;
        assert!(!SchemaField::from_vec(&other).unwrap().without_time_zone);
    }

    #[test]
    fn test_table_schema_special_chars() {
        // Create schema with special characters in name and primary key
        let mut schema = TableSchema::new("users!@#123".to_string(), true);
        schema.set_primary_key("id!@#123".to_string());

        // Serialize
        let vec = schema.into_vec();

        // Deserialize
        let deserialized = TableSchema::from_vec(&vec).unwrap();

        // Both names read back exactly as written.
        assert_eq!(deserialized.name, "users!@#123");
        assert_eq!(deserialized.primary_key, Some("id!@#123".to_string()));

        for pk in ["日本", "naïve", "with space", &"é".repeat(70)] {
            let mut s = TableSchema::new("t".to_string(), false);
            s.set_primary_key(pk.to_string());
            let back = TableSchema::from_vec(&s.into_vec()).unwrap().primary_key.unwrap();
            if pk.len() <= 128 {
                assert_eq!(back, pk);
            } else {
                // Cut at a character boundary within 128 bytes, never inside one.
                assert_eq!(back, "é".repeat(64));
            }
        }
    }

    #[tokio::test]
    async fn test_table_schema_iterator_empty() {
        let temp_loc = std::env::temp_dir().join("test_table_schema_iterator_empty");

        let mock_io =
            make_clean_provider(temp_loc.as_path(), "test_table_schema_iterator_empty").await;

        let mock_io = Arc::new(mock_io);

        let mut iterator = TableSchemaIterator::new(mock_io.clone()).await.unwrap();

        // Should return None for an empty storage
        assert!(iterator.next_table_schema().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_table_schema_iterator_single_schema() {
        // Create a single table schema
        let table_schema = create_test_table_schema("users", Some("id"));

        let temp_loc = std::env::temp_dir().join("test_table_schema_iterator_single_schema");

        let mock_io = make_clean_provider(
            temp_loc.as_path(),
            "test_table_schema_iterator_single_schema",
        )
        .await;

        let mock_io = Arc::new(mock_io);

        save_table_schema(&table_schema, mock_io.clone())
            .await
            .unwrap();

        let mut iterator = TableSchemaIterator::new(mock_io.clone()).await.unwrap();

        // Should return our table schema
        let read_schema = iterator.next_table_schema().await.unwrap().unwrap();

        assert_eq!(read_schema.name, "users");
        assert_eq!(read_schema.primary_key, Some("id".to_string()));

        // Next call should return None
        assert!(iterator.next_table_schema().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_table_schema_iterator_multiple_schemas() {
        // Create schemas
        let schema1 = create_test_table_schema("users", Some("id"));
        let schema2 = create_test_table_schema("products", Some("product_id"));
        let schema3 = create_test_table_schema("orders", None);

        let temp_loc = std::env::temp_dir().join("test_table_schema_iterator_multiple_schemas");

        let mock_io = make_clean_provider(
            temp_loc.as_path(),
            "test_table_schema_iterator_multiple_schemas",
        )
        .await;

        let mock_io = Arc::new(mock_io);

        // Save schemas to storage
        save_table_schema(&schema1, mock_io.clone()).await.unwrap();
        save_table_schema(&schema2, mock_io.clone()).await.unwrap();
        save_table_schema(&schema3, mock_io.clone()).await.unwrap();

        // Create iterator
        let mut iterator = TableSchemaIterator::new(mock_io.clone()).await.unwrap();

        // Get first schema
        let read_schema1 = iterator.next_table_schema().await.unwrap().unwrap();
        assert_eq!(read_schema1.name, "users");
        assert_eq!(read_schema1.primary_key, Some("id".to_string()));

        // Get second schema
        let read_schema2 = iterator.next_table_schema().await.unwrap().unwrap();
        assert_eq!(read_schema2.name, "products");
        assert_eq!(read_schema2.primary_key, Some("product_id".to_string()));

        // Get third schema
        let read_schema3 = iterator.next_table_schema().await.unwrap().unwrap();
        assert_eq!(read_schema3.name, "orders");
        assert_eq!(read_schema3.primary_key, None);

        // No more schemas
        assert!(iterator.next_table_schema().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_table_schema_iterator_reset() {
        let temp_loc = std::env::temp_dir().join("test_table_schema_iterator_reset");

        let mock_io =
            make_clean_provider(temp_loc.as_path(), "test_table_schema_iterator_reset").await;

        let mock_io = Arc::new(mock_io);

        // Create schemas
        let schema1 = create_test_table_schema("users", Some("id"));
        let schema2 = create_test_table_schema("products", None);

        // Save schemas to storage
        save_table_schema(&schema1, mock_io.clone()).await.unwrap();
        save_table_schema(&schema2, mock_io.clone()).await.unwrap();

        // Create iterator and read all schemas
        let mut iterator = TableSchemaIterator::new(mock_io.clone()).await.unwrap();

        assert!(iterator.next_table_schema().await.unwrap().is_some());
        assert!(iterator.next_table_schema().await.unwrap().is_some());
        assert!(iterator.next_table_schema().await.unwrap().is_none());

        // Reset iterator
        iterator.reset();

        // Should be able to read schemas again
        let read_schema1 = iterator.next_table_schema().await.unwrap().unwrap();
        assert_eq!(read_schema1.name, "users");

        let read_schema2 = iterator.next_table_schema().await.unwrap().unwrap();
        assert_eq!(read_schema2.name, "products");

        // No more schemas
        assert!(iterator.next_table_schema().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_table_schema_save_and_iterator() {
        // Create multiple schemas
        let temp_loc = std::env::temp_dir().join("test_table_schema_save_and_iterator");

        let mock_io =
            make_clean_provider(temp_loc.as_path(), "test_table_schema_save_and_iterator").await;

        let mock_io = Arc::new(mock_io);

        // Create and save schemas
        for i in 0..5 {
            let table_name = format!("table{}", i);
            let primary_key = if i % 2 == 0 {
                Some(format!("id{}", i))
            } else {
                None
            };

            let schema = create_test_table_schema(&table_name, primary_key.as_deref());
            save_table_schema(&schema, mock_io.clone()).await.unwrap();
        }

        // Read back using iterator
        let mut iterator = TableSchemaIterator::new(mock_io.clone()).await.unwrap();

        for i in 0..5 {
            let schema = iterator.next_table_schema().await.unwrap().unwrap();

            let expected_name = format!("table{}", i);
            let expected_pk = if i % 2 == 0 {
                Some(format!("id{}", i))
            } else {
                None
            };

            assert_eq!(schema.name, expected_name);
            assert_eq!(schema.primary_key, expected_pk);
        }

        // No more schemas
        assert!(iterator.next_table_schema().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_table_schema_long_names() {
        // Create a schema with long name and primary key
        let long_name = "a".repeat(200); // More than 128 chars
        let long_pk = "b".repeat(200); // More than 128 chars

        let schema = create_test_table_schema(&long_name, Some(&long_pk));

        let temp_loc = std::env::temp_dir().join("test_table_schema_long_names");

        let mock_io = make_clean_provider(temp_loc.as_path(), "test_table_schema_long_names").await;

        let mock_io = Arc::new(mock_io);

        save_table_schema(&schema, mock_io.clone()).await.unwrap();

        // Read back using iterator
        let mut iterator = TableSchemaIterator::new(mock_io.clone()).await.unwrap();
        let read_schema = iterator.next_table_schema().await.unwrap().unwrap();

        // Should be truncated to 128 chars
        assert_eq!(read_schema.name.len(), 128);
        assert_eq!(read_schema.primary_key.clone().unwrap().len(), 128);

        assert_eq!(read_schema.name, "a".repeat(128));
        assert_eq!(
            read_schema.primary_key.as_ref().unwrap().to_string(),
            "b".repeat(128)
        );
    }

    #[tokio::test]
    async fn test_table_schema_complete_set() {
        let temp_loc = std::env::temp_dir().join("test_table_schema_complete_set");

        let _mock_io_fields = make_clean_provider(
            temp_loc.as_path(),
            "test_table_schema_complete_set_FIELDS.db",
        )
        .await;

        let _mock_io_fields_tmp = make_clean_provider(
            temp_loc.as_path(),
            "test_table_schema_complete_set_FIELDS.db.tmp",
        )
        .await;

        let mock_io =
            make_clean_provider(temp_loc.as_path(), "test_table_schema_complete_set").await;

        let mock_io = Arc::new(mock_io);

        // Create schemas
        let mut schema1 = create_test_table_schema("users", Some("id"));

        schema1.save(mock_io.clone()).await.unwrap();

        schema1
            .add_field(mock_io.clone(), "email".into(), DbType::STRING, true)
            .await;

        schema1
            .add_field(mock_io.clone(), "created_at".into(), DbType::U64, false)
            .await;

        schema1
            .add_field(mock_io.clone(), "updated_at".into(), DbType::U64, false)
            .await;

        schema1
            .add_field(mock_io.clone(), "first_name".into(), DbType::STRING, false)
            .await;

        schema1
            .add_field(mock_io.clone(), "last_name".into(), DbType::STRING, false)
            .await;

        schema1
            .add_field(mock_io.clone(), "user_id".into(), DbType::U128, false)
            .await;

        let mut iterator = TableSchemaIterator::new(mock_io.clone()).await.unwrap();
        let read_schema1 = iterator.next_table_schema().await.unwrap().unwrap();
        assert_eq!(read_schema1.name, "users");

        assert_eq!(read_schema1.primary_key, Some("id".to_string()));

        assert_eq!(read_schema1.fields.len(), 6);

        assert_eq!(read_schema1.fields[0].name, "email");
        assert_eq!(read_schema1.fields[0].db_type, DbType::STRING);
        assert_eq!(read_schema1.fields[0].is_unique, true);
        assert_eq!(read_schema1.fields[0].offset, 0);
        assert_eq!(read_schema1.fields[0].write_order, 0);
        assert_eq!(read_schema1.fields[0].size, 8 + 4);

        assert_eq!(read_schema1.fields[1].name, "created_at");
        assert_eq!(read_schema1.fields[1].db_type, DbType::U64);
        assert_eq!(read_schema1.fields[1].is_unique, false);
        assert_eq!(read_schema1.fields[1].offset, 8 + 4);
        assert_eq!(read_schema1.fields[1].write_order, 1);
        assert_eq!(read_schema1.fields[1].size, 8);

        assert_eq!(read_schema1.fields[2].name, "updated_at");
        assert_eq!(read_schema1.fields[2].db_type, DbType::U64);
        assert_eq!(read_schema1.fields[2].is_unique, false);
        assert_eq!(read_schema1.fields[2].offset, 4 + 8 + 8);
        assert_eq!(read_schema1.fields[2].write_order, 2);
        assert_eq!(read_schema1.fields[2].size, 8);

        assert_eq!(read_schema1.fields[3].name, "first_name");
        assert_eq!(read_schema1.fields[3].db_type, DbType::STRING);
        assert_eq!(read_schema1.fields[3].is_unique, false);
        assert_eq!(read_schema1.fields[3].offset, 4 + 8 + 8 + 8);
        assert_eq!(read_schema1.fields[3].write_order, 3);
        assert_eq!(read_schema1.fields[3].size, 8 + 4);

        assert_eq!(read_schema1.fields[4].name, "last_name");
        assert_eq!(read_schema1.fields[4].db_type, DbType::STRING);
        assert_eq!(read_schema1.fields[4].is_unique, false);
        assert_eq!(read_schema1.fields[4].offset, 4 + 8 + 8 + 8 + 4 + 8);
        assert_eq!(read_schema1.fields[4].write_order, 4);
        assert_eq!(read_schema1.fields[4].size, 8 + 4);

        assert_eq!(read_schema1.fields[5].name, "user_id");
        assert_eq!(read_schema1.fields[5].db_type, DbType::U128);
        assert_eq!(read_schema1.fields[5].is_unique, false);
        assert_eq!(read_schema1.fields[5].offset, 4 + 8 + 8 + 8 + 4 + 8 + 4 + 8);
        assert_eq!(read_schema1.fields[5].write_order, 5);
        assert_eq!(read_schema1.fields[5].size, 16);
    }

    #[tokio::test]
    async fn test_table_mark_field_as_deleted() {
        let temp_loc = std::env::temp_dir().join("test_table_mark_field_as_deleted");

        let _mock_io_fields = LocalStorageProvider::new(
            temp_loc.to_str().unwrap(),
            Some("test_table_schema_complete_set_FIELDS.db"),
        )
        .await;

        let _mock_io_fields_tmp = LocalStorageProvider::new(
            temp_loc.to_str().unwrap(),
            Some("test_table_schema_complete_set_FIELDS.db.tmp"),
        )
        .await;

        let mock_io = LocalStorageProvider::new(
            temp_loc.to_str().unwrap(),
            Some("test_table_mark_field_as_deleted"),
        )
        .await;

        let mock_io = Arc::new(mock_io);

        // Create schemas
        let mut schema1 = create_test_table_schema("users", Some("id"));
        let schema2 = create_test_table_schema("other_users", Some("id2"));

        //let mut mock_io_fields = mock_io.create_new(format!("{}_{}", mock_io.get_name().replace(".db", ""), "FIELDS.db")).await;

        schema1.save(mock_io.clone()).await.unwrap();
        schema2.save(mock_io.clone()).await.unwrap();

        schema1
            .add_field(mock_io.clone(), "email".into(), DbType::STRING, true)
            .await;

        schema1
            .add_field(mock_io.clone(), "created_at".into(), DbType::U64, false)
            .await;

        schema1
            .add_field(mock_io.clone(), "updated_at".into(), DbType::U64, false)
            .await;

        schema1
            .add_field(mock_io.clone(), "first_name".into(), DbType::STRING, false)
            .await;

        schema1
            .add_field(mock_io.clone(), "last_name".into(), DbType::STRING, false)
            .await;

        schema1
            .add_field(mock_io.clone(), "user_id".into(), DbType::U128, false)
            .await;

        schema1
            .mark_field_as_deleted("user_id", mock_io.clone())
            .await;

        schema1
            .mark_field_as_deleted("first_name", mock_io.clone())
            .await;

        drop(schema1);

        let mut iterator = TableSchemaIterator::new(mock_io.clone()).await.unwrap();

        let read_schema1 = iterator.next_table_schema().await.unwrap().unwrap();
        assert_eq!(read_schema1.name, "users");

        assert_eq!(read_schema1.primary_key, Some("id".to_string()));
        assert_eq!(read_schema1.fields.len(), 6);

        assert_eq!(read_schema1.fields[5].name, "user_id");
        assert_eq!(read_schema1.fields[5].db_type, DbType::U128);
        assert_eq!(read_schema1.fields[5].is_unique, false);
        assert_eq!(read_schema1.fields[5].offset, 4 + 8 + 8 + 8 + 4 + 8 + 4 + 8);
        assert_eq!(read_schema1.fields[5].write_order, 5);
        assert_eq!(read_schema1.fields[5].size, 16);
        assert_eq!(read_schema1.fields[5].is_deleted, true);

        assert_eq!(read_schema1.fields[3].name, "first_name");
        assert_eq!(read_schema1.fields[3].db_type, DbType::STRING);
        assert_eq!(read_schema1.fields[3].is_unique, false);
        assert_eq!(read_schema1.fields[3].offset, 4 + 8 + 8 + 8);
        assert_eq!(read_schema1.fields[3].write_order, 3);
        assert_eq!(read_schema1.fields[3].size, 8 + 4);
        assert_eq!(read_schema1.fields[3].is_deleted, true);
    }
    /// The retype dance that damaged two tables in the live deployment.
    ///
    /// `ALTER TABLE` has no `ALTER COLUMN TYPE` that rewrites in place, so the
    /// standard way to change a column's type is: add `<name>_tmp` with the new
    /// type, copy the values across, drop the original, rename `_tmp` back. That
    /// last step renames *onto* a tombstoned name, which is deliberately allowed
    /// — and leaves two field records sharing a name.
    ///
    /// The load then has to pick one. Picking the lowest `write_order` picked
    /// the tombstone every time: old offset, old type, old width. A `SELECT`
    /// returned the value the migration had moved *away from*, reporting
    /// success, and it survived restarts because every load made the same wrong
    /// choice.
    #[tokio::test]
    async fn a_rename_onto_a_tombstoned_name_loads_the_live_field() {
        let temp_loc = std::env::temp_dir().join("schema_rename_onto_tombstone");
        _ = std::fs::remove_dir_all(&temp_loc);
        let mock_io = Arc::new(
            LocalStorageProvider::new(
                temp_loc.to_str().unwrap(),
                Some("schema_rename_onto_tombstone"),
            )
            .await,
        );

        let mut schema = create_test_table_schema("retype", Some("id"));
        schema.save(mock_io.clone()).await.unwrap();
        schema
            .add_field(
                mock_io.clone(),
                "payload_format".into(),
                DbType::STRING,
                false,
            )
            .await;
        let dead_order = schema
            .fields
            .iter()
            .find(|f| f.name == "payload_format")
            .expect("added")
            .write_order;

        schema
            .add_field(
                mock_io.clone(),
                "payload_format_tmp".into(),
                DbType::I16,
                false,
            )
            .await;
        schema
            .mark_field_as_deleted("payload_format", mock_io.clone())
            .await;
        schema
            .rename_field("payload_format_tmp", "payload_format", mock_io.clone())
            .await
            .expect("rename onto the tombstoned name");

        let loaded = TableSchema::load(mock_io.clone()).await.expect("load");
        let field = loaded
            .fields
            .iter()
            .find(|f| f.name == "payload_format")
            .expect("the renamed column is gone entirely");

        assert!(
            !field.is_deleted,
            "the load resolved `payload_format` to the tombstone, so every by-name \
             lookup reads the pre-migration column at its old offset"
        );
        assert_eq!(
            field.db_type,
            DbType::I16,
            "the load kept the old column's type, so values are read at the wrong width"
        );
        assert!(
            field.write_order > dead_order,
            "the load kept the older record; the live one is always the newer"
        );
    }

    async fn incarnation_provider(dir: &tempfile::TempDir) -> Arc<LocalStorageProvider> {
        Arc::new(LocalStorageProvider::new(dir.path().to_str().unwrap(), Some("inc_schema.db")).await)
    }

    /// C7: every new schema gets its own non-zero incarnation; `save` records
    /// it and `load` reads it back; a header rewrite (`PRIMARY KEY`, `RENAME`)
    /// keeps it.
    #[tokio::test]
    async fn a_saved_schema_keeps_its_incarnation_across_loads_and_header_rewrites() {
        let a = TableSchema::new("inc".into(), false);
        let b = TableSchema::new("inc".into(), false);
        assert_ne!(a.incarnation, 0);
        assert_ne!(a.incarnation, b.incarnation, "two creations of one name share an incarnation");

        let dir = tempfile::tempdir().unwrap();
        let io = incarnation_provider(&dir).await;
        let mut schema = a.clone();
        schema.save(io.clone()).await.unwrap();
        schema
            .add_field(io.clone(), "id".into(), DbType::I64, true)
            .await;
        let loaded = TableSchema::load(io.clone()).await.expect("load");
        assert_eq!(loaded.incarnation, a.incarnation, "the incarnation did not survive a load");
        assert_eq!(loaded.fields.len(), 1);

        schema.set_primary_key("id".into());
        schema.name = "renamed".into();
        schema.rewrite_header(io.clone()).await.unwrap();
        let loaded = TableSchema::load(io.clone()).await.expect("load");
        assert_eq!(loaded.primary_key.as_deref(), Some("id"));
        assert_eq!(loaded.name, "renamed");
        assert_eq!(loaded.incarnation, a.incarnation, "a header rewrite lost the incarnation");
    }

    /// A `_schema.db` written before incarnations existed is the header alone:
    /// it loads exactly as before, with none recorded. A record that does not
    /// verify reads as none too, never as some other incarnation.
    #[tokio::test]
    async fn a_schema_without_a_verifying_incarnation_loads_with_none() {
        let dir = tempfile::tempdir().unwrap();
        let io = incarnation_provider(&dir).await;
        let schema = TableSchema::new("legacy".into(), true);
        io.append_data(&schema.into_vec(), true).await;
        let loaded = TableSchema::load(io.clone()).await.expect("a legacy schema must load");
        assert_eq!(loaded.name, "legacy");
        assert!(loaded.is_immutable);
        assert_eq!(loaded.incarnation, 0);

        let dir = tempfile::tempdir().unwrap();
        let io = incarnation_provider(&dir).await;
        schema.save(io.clone()).await.unwrap();
        assert_eq!(TableSchema::load(io.clone()).await.unwrap().incarnation, schema.incarnation);
        // One bit of the incarnation flipped: the CRC no longer verifies.
        let mut record = incarnation_record(schema.incarnation);
        record[10] ^= 0x01;
        io.write_data(TABLE_SIZE as u64, &record).await;
        let loaded = TableSchema::load(io.clone()).await.expect("load");
        assert_eq!(loaded.name, "legacy");
        assert_eq!(loaded.incarnation, 0, "a corrupt record was trusted");

        // And a record of incarnation zero is none, however well it verifies.
        assert_eq!(parse_incarnation(&incarnation_record(0)), None);
        assert_eq!(parse_incarnation(&incarnation_record(7)), Some(7));
        assert_eq!(parse_incarnation(&incarnation_record(7)[..INCARNATION_LEN - 1]), None);
    }

    /// The record's first byte is one no header can start with, whatever the
    /// table's name: names are UTF-8, which never holds `0xFF`, or empty.
    #[test]
    fn no_header_starts_like_an_incarnation_record() {
        for name in ["", "a", "Z9", "_x", "weird-name.with dots", "\u{ff}x", "日本", "\u{10ffff}"] {
            let header = TableSchema::new(name.into(), false).into_vec();
            assert_ne!(header[0], INCARNATION_MAGIC[0], "{name:?}");
        }
    }

    /// Several schemas saved into one file, each followed by its record, still
    /// iterate one by one with their own incarnations, including across the
    /// iterator's chunk boundary, which records no longer align with.
    #[tokio::test]
    async fn the_schema_iterator_steps_over_incarnation_records() {
        let dir = tempfile::tempdir().unwrap();
        let io = incarnation_provider(&dir).await;
        let schemas: Vec<TableSchema> = (0..150)
            .map(|i| TableSchema::new(format!("t{i}"), i % 2 == 0))
            .collect();
        for schema in &schemas {
            schema.save(io.clone()).await.unwrap();
        }
        let mut iterator = TableSchemaIterator::new(io.clone()).await.unwrap();
        let mut seen = Vec::new();
        while let Some(schema) = iterator.next_table_schema().await.unwrap() {
            seen.push((schema.name, schema.is_immutable, schema.incarnation));
        }
        let expected: Vec<(String, bool, u128)> = schemas
            .iter()
            .map(|s| (s.name.clone(), s.is_immutable, s.incarnation))
            .collect();
        assert_eq!(seen.len(), expected.len(), "the iterator lost or invented schemas");
        for (got, want) in seen.iter().zip(&expected) {
            assert_eq!(got.0, want.0);
            assert_eq!(got.1, want.1, "{}", want.0);
            assert_eq!(got.2, want.2, "{}: the wrong incarnation", want.0);
        }
    }
}
