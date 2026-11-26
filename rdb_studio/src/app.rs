//! The application: what it holds, what can happen to it, and what it draws.
//!
//! Everything that touches the network is a [`Task`]; nothing in `update`
//! blocks. That is not a style preference — a `SHOW TABLES` against a hundred
//! tables is a hundred `stat` calls on the server, and a console that froze
//! while waiting would be unusable against exactly the databases it is most
//! needed for.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use iced::widget::text_editor;
use iced::{Element, Subscription, Task};

use crate::conn::{self, Session};
use crate::dialect;
use crate::model::*;
use crate::profiles;
use crate::types;
use crate::views;

/// How many rows the data browser fetches at a time.
///
/// Large enough that scrolling a table feels continuous, small enough that a
/// mis-clicked `SELECT *` on a billion-row table returns rather than filling
/// memory. The user can change it in the browser's footer.
pub const DEFAULT_PAGE: usize = 200;

/// Statements the activity page asks for.
///
/// Below what a RasterizedDB server keeps by default (256, and
/// `SET CONFIG query_stats.recent` moves it), so the page shows the whole ring
/// on a stock server and a bounded slice of a larger one. The same number is
/// sent to PostgreSQL, where it caps `pg_stat_activity` — a server with more
/// than two hundred backends running statements at once has a problem this
/// page is not the right lens for.
pub const RECENT_STATEMENTS: usize = 200;

pub struct Studio {
    /// The connection form, which is also the profile being edited.
    pub form: Connection,
    pub profiles: Vec<Connection>,
    pub session: Option<Session>,
    pub connecting: bool,
    pub connect_error: Option<String>,

    pub tab: Tab,
    pub status: Status,

    pub databases: Vec<DatabaseInfo>,
    pub tables: Vec<TableInfo>,
    pub table_filter: String,
    pub selected_table: Option<String>,

    pub db_stats: Vec<Stat>,
    pub table_stats: Vec<Stat>,
    pub columns: Vec<ColumnInfo>,
    /// The selected table's indexes, loaded once its columns are: which index
    /// is the primary key's is read against them. See `ColumnsLoaded`.
    pub indexes: Vec<IndexInfo>,
    /// The selected table's partitions. Empty for a table that is not
    /// partitioned, which is asked of `SHOW TABLE STATS` first rather than by
    /// sending a `SHOW PARTITIONS` that every plain table refuses.
    pub partitions: Vec<PartitionInfo>,
    /// Which lens the lower half of the table pane shows.
    pub detail_section: DetailSection,
    /// An index the user has confirmed dropping, as `(table, index)`: armed
    /// when the statement succeeds, checked against the reloaded list. A
    /// server before fc3849c answers dropping a constraint's index with
    /// success and keeps it, so success alone is not taken as the answer.
    pub expect_gone: Option<(String, String)>,
    pub verify_gone: Option<(String, String)>,
    /// Whether the partitions lens lists every declared partition, or only
    /// those holding rows. Only those by default: at the default of 64, the
    /// few a small table uses are otherwise below the fold.
    pub show_empty_partitions: bool,

    pub schema: SchemaGraph,
    pub schema_pan: iced::Vector,
    pub schema_zoom: f32,
    /// Tessellated schema-map geometry, cleared whenever the map moves or the
    /// schema changes. See [`crate::views::schema`].
    pub schema_cache: iced::widget::canvas::Cache,

    pub config: Vec<ConfigEntry>,
    pub config_filter: String,
    /// Edits not yet applied, keyed by `section.key`. Held separately from the
    /// entries so that a refresh landing mid-edit does not discard what the
    /// user typed.
    pub config_edits: HashMap<String, String>,

    pub data: Grid,
    pub data_page: usize,
    pub page_size: usize,
    pub data_total: Option<u64>,
    /// Where each page of the data browser ended, so the next one can start
    /// after its last key instead of at an offset.
    pub data_keyset: Keyset,
    /// The data browser's filter box, as typed.
    pub data_filter: String,
    /// The filter the page on screen was read with.
    pub data_filter_applied: Option<dialect::RowFilter>,
    pub editing: Option<CellEdit>,
    pub inserting: Option<Vec<String>>,
    /// Column widths the user has dragged to, keyed by table and column so two
    /// tables do not share one.
    pub column_widths: HashMap<String, f32>,
    pub resizing: Option<Resize>,
    /// How each grid is sorted, keyed by the same scope its widths are. Empty
    /// means every grid is in the order the server returned it, which is the
    /// order that was there before any of this existed.
    pub sort: HashMap<String, GridSort>,
    /// Where the data grid is scrolled to, and how tall its window is, so it
    /// only builds the rows that can be seen. `None` until the first scroll.
    pub data_viewport: Option<(f32, f32)>,

    pub editor: text_editor::Content,
    /// Whether the editor has been grown to take most of the screen, for a
    /// long script, at the expense of the results under it.
    pub editor_tall: bool,
    pub results: Vec<Outcome>,
    pub query_error: Option<String>,
    pub history: Vec<HistoryEntry>,
    /// The statement that is in flight, waiting for its outcome before it is
    /// worth filing in the history.
    pub pending_sql: Option<String>,
    /// How long the last run took, for the results header.
    pub last_elapsed: Option<Duration>,
    /// What stops the run in flight; `None` when nothing is running.
    pub query_cancel: Option<crate::conn::Canceller>,
    /// A cancel has been sent for the run in flight.
    pub cancel_sent: bool,
    /// How many rows of each result the editor keeps.
    pub row_limit: RowLimit,
    /// What the last run's statements raised as notices.
    pub query_notices: Vec<String>,
    /// The server's roles, for the ROLES tab.
    pub roles: Vec<RoleInfo>,
    /// What each role may reach: grants, default privileges, policies.
    pub role_access: crate::model::RoleAccess,
    /// The role whose detail the ROLES tab shows.
    pub role_selected: Option<String>,
    /// The ROLES tab's form inputs, by key; reset when another role is chosen.
    pub role_inputs: std::collections::BTreeMap<String, String>,
    /// What Ctrl+Space offered for the word before the editor's cursor.
    pub completions: Vec<String>,
    /// The KEYS tab.
    pub kv: KvBrowser,
    /// Where the editor's own transaction stands, when the transport says.
    pub editor_transaction: Option<rasterizeddb_client::TransactionStatus>,

    pub modal: Option<Modal>,
    /// A dialog's statement is with the server. The dialog stays open until
    /// the answer comes back, so a refusal is shown where the form still is
    /// rather than in the status bar after the form has gone; this stops a
    /// second press sending it twice meanwhile.
    pub modal_pending: bool,

    // --- the live dashboard
    pub metrics: Metrics,
    /// Whether the dashboard is sampling. Paused means *no statement is sent*,
    /// not "the chart is frozen": a console left open on a production database
    /// should cost nothing when nobody is looking at it.
    pub live: bool,
    pub interval: u64,
    /// When the last sample was taken, so a rate divides by the time that
    /// actually passed rather than by the interval that was asked for.
    pub sampled_at: Option<Instant>,
    /// Why the last sample failed, shown on the dashboard rather than in the
    /// status bar — see [`crate::views::dashboard`].
    pub metrics_error: Option<String>,

    // --- backups
    /// Everything the backup page reads, or `None` before the first load.
    pub backup: Option<BackupSnapshot>,

    // --- importing another database
    pub import: dialect::ImportRequest,
    pub import_running: bool,
    pub import_report: Vec<String>,

    // --- activity
    /// Everything the activity page reads, or `None` before the first load.
    pub activity: Option<ActivitySnapshot>,

    // --- benchmark
    /// The benchmark page's own state; see [`crate::views::bench`].
    pub bench: views::bench::Page,

    // --- branches
    /// Everything the branches page reads, or `None` before the first load.
    pub branches: Option<BranchesSnapshot>,
    pub branch_form: BranchForm,
    /// The branch whose diff is shown, and the diff: `None` while it loads.
    pub branch_diff: Option<(String, Option<Result<Vec<BranchDiffRow>, String>>)>,

    // --- broker
    /// Everything the broker page reads, or `None` before the first load.
    pub broker: Option<BrokerSnapshot>,
    pub broker_form: BrokerForm,
    pub broker_publish: BrokerPublishForm,
    pub broker_read: BrokerReadForm,
    /// What the message panel shows: a READ, a POP or a topic's partitions.
    pub broker_browse: Option<BrokerBrowse>,
    pub broker_perm: BrokerPermForm,
    pub merge_policy: MergePolicy,
    pub merge_allow_drop: bool,

    // --- shards
    pub shards: Option<ShardsSnapshot>,
    // --- ledger (enterprise)
    #[cfg(feature = "enterprise")]
    pub ledger: Option<rdb_studio_enterprise::LedgerPage>,
    /// The last `SHOW REBALANCE PLAN` on the shards page, or why it failed.
    pub rebalance_plan: Option<Result<Vec<RebalanceStep>, String>>,

    // --- replication
    pub replication: Option<ReplicationSnapshot>,
    pub peer_name: String,
    pub peer_address: String,

    // --- storage placement
    /// Everything the storage page reads, or `None` before the first load.
    pub storage: Option<StorageSnapshot>,
    pub storage_path: String,
    pub storage_role: &'static str,
    pub storage_weight: String,
    pub storage_min_free: String,
    /// The table a move is being set up for, and where it would go. Kept in the
    /// app rather than in the view because the destination picker has to
    /// survive the redraw between choosing a table and choosing a directory.
    pub storage_move_table: Option<String>,
    pub storage_move_to: Option<String>,
    /// A rebalance's report — a line per table it moved, or would move.
    ///
    /// It is the entire point of a dry run, so it is state rather than a status
    /// line that scrolls past.
    pub storage_report: Vec<String>,
    pub storage_busy: bool,
    /// Which backup action is in flight, for the button state and the status
    /// line. `None` means idle.
    pub backup_busy: Option<String>,
    pub restore_dir: String,
    pub restore_kind: RestoreKind,
    pub restore_snapshot: String,
    pub restore_lsn: String,
    pub restore_time: String,
    pub restore_running: bool,
    pub restore_report: Option<RestoreReport>,

    /// Set when the server does not look like the dialect the connection says
    /// it is. Sticky rather than a passing status line: every panel will fail
    /// until it is fixed, and a message that scrolls past leaves the user
    /// looking at six empty tabs with no explanation.
    pub flavor_warning: Option<String>,
}

/// Where each page of the data browser ended, for paging by key.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct Keyset {
    /// The statement of the first page the ends belong to
    /// ([`Studio::page_query`]); another table, filter, order or page size
    /// starts over.
    pub signature: String,
    /// The key literal after page `p`'s last row, at `after[p]`; `None` when
    /// that page had no row or no usable key.
    pub after: Vec<Option<String>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Tab {
    Dashboard,
    Overview,
    Tables,
    Schema,
    Data,
    Query,
    Activity,
    Bench,
    Backup,
    Branches,
    Storage,
    Replication,
    Shards,
    Migrate,
    Keys,
    Broker,
    Roles,
    Config,
    /// Ledger tables and secret vaults (the enterprise build).
    #[cfg(feature = "enterprise")]
    Ledger,
}

impl Tab {
    #[cfg(feature = "enterprise")]
    pub const ALL: [Tab; 19] = {
        let mut all = [Tab::Ledger; 19];
        let mut i = 0;
        while i < 18 {
            all[i] = Self::GPL[i];
            i += 1;
        }
        all
    };
    #[cfg(not(feature = "enterprise"))]
    pub const ALL: [Tab; 18] = Self::GPL;

    const GPL: [Tab; 18] = [
        Tab::Dashboard,
        Tab::Overview,
        Tab::Tables,
        Tab::Schema,
        Tab::Data,
        Tab::Query,
        // Next to QUERY, and after it: the two are the same subject from
        // opposite ends. QUERY is the statement you are writing; ACTIVITY is
        // every statement the server has already answered, this console's
        // included.
        Tab::Activity,
        // After ACTIVITY: what the server has done, then how fast it can do
        // one thing again.
        Tab::Bench,
        Tab::Backup,
        // Next to BACKUP: both are copies of the data, one kept for a bad
        // day and one to work on.
        Tab::Branches,
        // Next to BACKUP rather than next to CONFIG: both pages are about where
        // the bytes are and what happens when a disk goes, and an operator
        // opening one usually wants the other.
        Tab::Storage,
        Tab::Replication,
        // After REPLICATION: both are about the other nodes, one holding
        // copies of this one's rows, the other holding the rest of them.
        Tab::Shards,
        Tab::Migrate,
        // The key/value store: data, but not tables.
        Tab::Keys,
        // Next to KEYS: the other thing a server holds that is not a table,
        // messages instead of values.
        Tab::Broker,
        // Before CONFIG: who may connect, then how the server behaves.
        Tab::Roles,
        Tab::Config,
    ];

    pub fn label(&self) -> &'static str {
        match self {
            Tab::Dashboard => "DASHBOARD",
            Tab::Overview => "OVERVIEW",
            Tab::Tables => "TABLES",
            Tab::Schema => "SCHEMA",
            Tab::Data => "DATA",
            Tab::Query => "QUERY",
            Tab::Activity => "ACTIVITY",
            Tab::Bench => "BENCH",
            Tab::Backup => "BACKUP",
            Tab::Branches => "BRANCHES",
            Tab::Storage => "STORAGE",
            Tab::Replication => "REPLICATION",
            Tab::Shards => "SHARDS",
            Tab::Migrate => "MIGRATE",
            Tab::Keys => "KEYS",
            Tab::Broker => "BROKER",
            Tab::Roles => "ROLES",
            Tab::Config => "CONFIG",
            #[cfg(feature = "enterprise")]
            Tab::Ledger => "LEDGER",
        }
    }
}

/// What the status bar says. A console that reports nothing about a statement
/// it is running looks broken while it works.
#[derive(Debug, Clone, PartialEq)]
pub enum Status {
    Idle,
    Busy(String),
    Done(String),
    Failed(String),
}

/// A column resize in progress.
#[derive(Debug, Clone, PartialEq)]
pub struct Resize {
    pub key: String,
    /// Cursor position when the drag started, in the grid's own coordinates.
    /// `None` until the first movement, because the press itself carries no
    /// position.
    pub from: Option<f32>,
    pub start_width: f32,
}

/// How one grid is sorted.
///
/// Held per grid rather than per column so that clicking a second header
/// replaces the sort instead of adding to it: a console sorts by one thing, and
/// a multi-column sort nobody can see the shape of is a grid whose order looks
/// arbitrary.
#[derive(Debug, Clone, PartialEq)]
pub struct GridSort {
    /// The [`crate::views::GridColumn`] key, not the header text — the same
    /// identity a remembered width is stored against.
    pub column: String,
    pub descending: bool,
}

/// A cell being edited in place.
#[derive(Debug, Clone, PartialEq)]
pub struct CellEdit {
    pub row: usize,
    pub column: usize,
    pub value: String,
    /// Whether the user has explicitly set the cell to `NULL`, which is not the
    /// same as clearing it to an empty string.
    pub null: bool,
    /// The `WHERE` clause identifying this row, worked out when the edit began.
    /// Empty when the row cannot be addressed, which is what disables the edit.
    pub key: String,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Modal {
    CreateTable {
        name: String,
        columns: Vec<NewColumn>,
        partitioning: Partitioning,
        error: Option<String>,
    },
    CreateDatabase { name: String, error: Option<String> },
    AddColumn { table: String, column: NewColumn, error: Option<String> },
    CreateIndex { table: String, index: NewIndex, error: Option<String> },
    Confirm { title: String, body: String, statement: String, refresh: Refresh },
    /// Renaming a column, changing its type, or whether it takes NULL.
    EditColumn { table: String, edit: ColumnEdit, error: Option<String> },
    /// A table's row compression (RasterizedDB): every row's variable-width
    /// values stored as one zstd frame.
    RowCompression { table: String, edit: RowCompressionEdit, error: Option<String> },
    CreateRole {
        name: String,
        password: String,
        login: bool,
        superuser: bool,
        error: Option<String>,
    },
    RolePassword { role: String, password: String, error: Option<String> },
    /// A restore, which reports rows rather than a count and therefore cannot
    /// go through the ordinary confirmation path.
    ConfirmRestore { body: String, statement: String },
    /// An import, likewise — and it reports a whole document.
    ConfirmImport { body: String, statement: String },
    /// A CSV file read and ready to go into the selected table.
    ConfirmCsv {
        table: String,
        file: String,
        rows: usize,
        columns: Vec<String>,
        scripts: Vec<crate::import::Script>,
    },
}

/// A column being changed in a dialog: what it is, and what it is to become.
#[derive(Debug, Clone, PartialEq)]
pub struct ColumnEdit {
    pub original: String,
    pub original_type: String,
    pub original_nullable: bool,
    pub name: String,
    /// Free text: whatever the server takes after `TYPE`, `NUMERIC(12,2)`
    /// included.
    pub data_type: String,
    pub nullable: bool,
    /// What `SHOW TABLE STATS` says the column is compressed with (`zstd(9)`),
    /// empty for none or when the server does not say.
    pub original_compression: String,
    pub compression: String,
}

impl ColumnEdit {
    /// `compression`: the column's current method, as `SHOW TABLE STATS`'
    /// `compressed_columns` names it, or empty.
    pub fn of(column: &ColumnInfo, compression: &str) -> Self {
        ColumnEdit {
            original: column.name.clone(),
            original_type: column.data_type.clone(),
            original_nullable: column.nullable,
            name: column.name.clone(),
            data_type: column.data_type.clone(),
            nullable: column.nullable,
            original_compression: compression.to_string(),
            compression: compression.to_string(),
        }
    }

    /// The statements the change takes, as one script: the type and the
    /// nullability under the old name, then the rename, so each names a
    /// column that exists when it runs. Sent together, they run as one
    /// implicit transaction, so a refused type change leaves the name alone.
    pub fn statements(&self, table: &str) -> Result<String, String> {
        let name = self.name.trim();
        let data_type = self.data_type.trim();
        if name.is_empty() {
            return Err("the column needs a name".to_string());
        }
        if data_type.is_empty() {
            return Err("the column needs a type".to_string());
        }
        if data_type.contains(';') || data_type.contains("--") || data_type.contains("/*") {
            return Err("a type is one type name, like NUMERIC(12,2)".to_string());
        }
        let table = dialect::quote_ident(table);
        let column = dialect::quote_ident(&self.original);
        let mut statements = Vec::new();
        if !data_type.eq_ignore_ascii_case(self.original_type.trim()) {
            statements.push(format!(
                "ALTER TABLE {table} ALTER COLUMN {column} TYPE {data_type}"
            ));
        }
        if self.nullable != self.original_nullable {
            statements.push(format!(
                "ALTER TABLE {table} ALTER COLUMN {column} {} NOT NULL",
                if self.nullable { "DROP" } else { "SET" }
            ));
        }
        // New values only, as in PostgreSQL: rows already written keep what
        // they have until the table is recompressed.
        if !self.compression.trim().eq_ignore_ascii_case(self.original_compression.trim()) {
            let method = if self.compression.trim().is_empty() {
                "default".to_string()
            } else {
                compression_method(&self.compression)?
            };
            statements.push(format!(
                "ALTER TABLE {table} ALTER COLUMN {column} SET COMPRESSION {method}"
            ));
        }
        if name != self.original {
            statements.push(format!(
                "ALTER TABLE {table} RENAME COLUMN {column} TO {}",
                dialect::quote_ident(name)
            ));
        }
        if statements.is_empty() {
            return Err("nothing is changed".to_string());
        }
        Ok(statements.join(";\n"))
    }
}

/// A column being described in a dialog.
#[derive(Debug, Clone, PartialEq)]
pub struct NewColumn {
    pub name: String,
    pub choice: types::Choice,
    /// `VARCHAR(n)`.
    pub length: String,
    /// `NUMERIC(p, s)`.
    pub precision: String,
    pub scale: String,
    /// Whether the user has picked a type. Until they have, typing a name
    /// re-guesses one; after, it never overrides what they chose.
    pub type_chosen: bool,
    pub nullable: bool,
    pub primary_key: bool,
    pub unique: bool,
    pub default: String,
    /// `COMPRESSION …`, empty for the server's default (none).
    pub compression: String,
}

impl NewColumn {
    pub fn new(flavor: Flavor) -> Self {
        let choice = types::default_for(flavor);
        let (length, precision, scale) = types::defaults(choice);
        Self {
            name: String::new(),
            choice,
            length,
            precision,
            scale,
            type_chosen: false,
            nullable: true,
            primary_key: false,
            unique: false,
            default: String::new(),
            compression: String::new(),
        }
    }

    /// The key column a new table starts with.
    pub fn key(flavor: Flavor) -> Self {
        let choice = types::default_key(flavor);
        let (length, precision, scale) = types::defaults(choice);
        Self {
            name: "id".to_string(),
            choice,
            length,
            precision,
            scale,
            type_chosen: true,
            nullable: false,
            primary_key: true,
            unique: false,
            default: String::new(),
            compression: String::new(),
        }
    }

    /// The DDL this column is written as.
    pub fn data_type(&self) -> String {
        types::render(self.choice, &self.length, &self.precision, &self.scale)
    }

    /// Applies a chosen type, resetting its parameters to that type's defaults.
    ///
    /// Carrying `NUMERIC`'s scale over to `VARCHAR` would produce
    /// `VARCHAR(12)` from a box the user never looked at.
    pub fn choose(&mut self, choice: types::Choice) {
        self.choice = choice;
        let (length, precision, scale) = types::defaults(choice);
        self.length = length;
        self.precision = precision;
        self.scale = scale;
        self.type_chosen = true;
    }

    /// Renames the column, re-guessing its type while the user has not chosen.
    pub fn rename(&mut self, name: String, flavor: Flavor) {
        self.name = name;
        if self.type_chosen {
            return;
        }
        if let Some(guess) = types::infer(&self.name, flavor) {
            self.choice = guess;
            let (length, precision, scale) = types::defaults(guess);
            self.length = length;
            self.precision = precision;
            self.scale = scale;
        }
    }
}

/// How a new table is spread over partitions. RasterizedDB only.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct Partitioning {
    pub enabled: bool,
    /// Positions in the dialog's column list, in key order. Positions rather
    /// than names, because a column can be renamed after it was picked; the
    /// list is renumbered when a column is removed.
    pub key: Vec<usize>,
    /// `PARTITIONS n`. Empty leaves it to the server's default.
    pub count: String,
}

/// The most columns a partition key takes: the hash is split 48/16 or
/// 48/8/8 between them, and there is no fourth width.
pub const MAX_PARTITION_KEY: usize = 3;

/// What a new index is over.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndexKind {
    /// One or more plain columns, by B-tree or hash.
    Columns,
    /// A whole `jsonb` document, by GIN.
    Document,
    /// One field of a document, `((doc->>'k'))`.
    Path,
}

impl IndexKind {
    pub const ALL: [IndexKind; 3] = [IndexKind::Columns, IndexKind::Document, IndexKind::Path];

    pub fn label(&self) -> &'static str {
        match self {
            IndexKind::Columns => "COLUMNS",
            IndexKind::Document => "DOCUMENT (GIN)",
            IndexKind::Path => "DOCUMENT PATH",
        }
    }
}

/// How a column index is built.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndexMethod {
    /// No `USING`: the server chooses from the column types.
    Automatic,
    Btree,
    Hash,
}

impl IndexMethod {
    pub const ALL: [IndexMethod; 3] = [IndexMethod::Automatic, IndexMethod::Btree, IndexMethod::Hash];

    fn using(&self) -> Option<&'static str> {
        match self {
            IndexMethod::Automatic => None,
            IndexMethod::Btree => Some("btree"),
            IndexMethod::Hash => Some("hash"),
        }
    }

    pub fn help(&self) -> &'static str {
        match self {
            IndexMethod::Automatic => {
                "the server chooses: hash when every column is UUID, JSONB, MACADDR or CHAR, B-tree otherwise"
            }
            IndexMethod::Btree => {
                "answers =, IN, ranges and LIKE 'prefix%', on the leading column of a composite key"
            }
            IndexMethod::Hash => "answers = and IN only, on every column of the key; one probe",
        }
    }
}

impl std::fmt::Display for IndexMethod {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            IndexMethod::Automatic => "automatic",
            IndexMethod::Btree => "B-tree",
            IndexMethod::Hash => "hash",
        })
    }
}

/// An index being described in a dialog.
#[derive(Debug, Clone, PartialEq)]
pub struct NewIndex {
    /// Empty lets the server derive one from the table and columns.
    pub name: String,
    pub kind: IndexKind,
    pub method: IndexMethod,
    /// In key order, which is the order they were picked in: a composite
    /// B-tree answers ranges on its first column only.
    pub columns: Vec<String>,
    /// The `jsonb` column a document or path index is over.
    pub document: String,
    /// `jsonb_path_ops` rather than the default `jsonb_ops`.
    pub path_ops: bool,
    /// The field, as dot-separated keys: `status`, `user.id`.
    pub path: String,
    pub unique: bool,
}

impl NewIndex {
    pub fn new(document: String) -> Self {
        Self {
            name: String::new(),
            kind: IndexKind::Columns,
            method: IndexMethod::Automatic,
            columns: Vec::new(),
            document,
            path_ops: false,
            path: String::new(),
            unique: false,
        }
    }
}

/// One change to the index dialog.
#[derive(Debug, Clone, PartialEq)]
pub enum IndexEdit {
    Name(String),
    Kind(IndexKind),
    Method(IndexMethod),
    /// Adds the column to the key, or takes it out.
    Column(String),
    Document(String),
    PathOps(bool),
    Path(String),
    Unique(bool),
}

/// One change to the partitioning half of the table dialog.
#[derive(Debug, Clone, PartialEq)]
pub enum PartitionEdit {
    Enabled(bool),
    /// Adds the column at this position to the key, or takes it out.
    Key(usize),
    Count(String),
}

/// What the lower half of the table pane shows.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DetailSection {
    Indexes,
    Partitions,
    Statistics,
}

/// What to reload after a statement succeeds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Refresh {
    Keys,
    Roles,
    Tables,
    Databases,
    CurrentTable,
    Backups,
    Replication,
    Storage,
    Activity,
    /// A branch was made, reset, swapped, merged or dropped: the branch list,
    /// and the databases, which branches are.
    Branches,
    /// A rebalance was started: the shards page and its status.
    Shards,
    /// A topic, queue, subscription, exchange or binding was made or
    /// dropped, or a message published: the broker page.
    Broker,
    /// A ledger table or vault changed (a confirmed hold, release, delete,
    /// purge or key rotation): the LEDGER page.
    #[cfg(feature = "enterprise")]
    Ledger,
}

#[derive(Debug, Clone)]
pub enum Message {
    // --- connection screen
    FormName(String),
    FormHost(String),
    FormPort(String),
    FormDatabase(String),
    FormUser(String),
    FormPassword(String),
    FormFlavor(Flavor),
    FormTls(TlsMode),
    FormTransport(crate::model::Transport),
    FormLogin(crate::model::NativeLogin),
    FormVerify(bool),
    FormCaFile(String),
    FormSavePassword(bool),
    FormPasswordStore(PasswordStore),
    LoadProfile(usize),
    /// A saved profile's password, read from the OS keyring for the form.
    SavedPasswordRead(String, Result<Option<String>, String>),
    SaveProfile,
    /// The keyring's part of saving a profile is done (or refused).
    ProfileSaved(Connection, Result<(), String>),
    DeleteProfile(usize),
    /// A deleted profile's password is out of the OS keyring (or is not).
    ProfileForgotten(String, Result<(), String>),
    Connect,
    Connected(Result<Session, String>),
    FlavorDetected(Flavor),
    Disconnect,

    // --- navigation
    SelectTab(Tab),
    SelectTable(String),
    SelectDatabase(String),
    /// Moves the tab strip sideways by this many pixels: its `<` and `>`
    /// buttons, and a wheel turned over it.
    ScrollTabs(f32),
    TableFilter(String),
    RefreshAll,

    // --- loading
    StatsLoaded(Result<Vec<Stat>, String>),
    TablesLoaded(Result<Vec<TableInfo>, String>),
    DatabasesLoaded(Result<Vec<DatabaseInfo>, String>),
    TableStatsLoaded(Result<Vec<Stat>, String>),
    ColumnsLoaded(Result<Vec<ColumnInfo>, String>),
    /// Tagged with the table they are for: a slow answer for the table that
    /// was selected before must not be shown under the one selected now.
    IndexesLoaded(String, Result<Vec<IndexInfo>, String>),
    PartitionsLoaded(String, Result<Vec<PartitionInfo>, String>),
    SchemaLoaded(Result<SchemaGraph, String>),
    ConfigLoaded(Result<Vec<ConfigEntry>, String>),
    RolesLoaded(Result<Vec<RoleInfo>, String>),
    RoleAccessLoaded(Box<crate::model::RoleAccess>),
    /// Show (or, with `None`, close) one role's detail.
    RoleManage(Option<String>),
    /// A ROLES form input changed: (key, value).
    RoleInput(String, String),
    /// Run a statement the ROLES detail built, then reload the tab.
    RoleRun(String),
    /// The KEYS tab's pattern box.
    KvPattern(String),
    /// Scan from the start (`false`) or on from the last page (`true`).
    KvScan(bool),
    KvScanned(bool, Result<(Option<String>, Vec<String>, Option<u64>), String>),
    KvSelect(String),
    KvLoaded(Result<KvEntry, String>),
    /// Open the dialog that sets a role's password.
    RolePassword(String),
    /// A page of the data browser: which page it was, the signature of the
    /// statement it was read under ([`Studio::page_query`]), and the rows.
    DataLoaded(usize, String, Result<(Grid, Option<u64>), String>),

    // --- backups
    ReloadBackups,
    BackupLoaded(Box<BackupSnapshot>),
    Backup(BackupAction),
    BackupFinished(String, Duration, Result<String, String>),
    UseSnapshot(String),
    RestoreDir(String),
    RestoreKindChanged(RestoreKind),
    RestoreSnapshot(String),
    RestoreLsn(String),
    RestoreTime(String),
    RunRestore,
    RestoreFinished(String, Duration, Result<Grid, String>),

    // --- importing another database
    ImportField(ImportField, String),
    ImportSslMode(dialect::SslMode),
    ImportDryRun(bool),
    ImportSkipConstraints(bool),
    RunImport,
    ImportFinished(Duration, Result<Vec<String>, String>),
    CopyImportReport,

    // --- storage placement
    ReloadStorage,
    StorageLoaded(Box<StorageSnapshot>),
    StoragePath(String),
    StorageRole(&'static str),
    StorageWeight(String),
    StorageMinFree(String),
    AddStorageLocation,
    DropStorageLocation(String),
    EnableStorageLocation(String, bool),
    SetStorageRole(String, &'static str),
    SetStorageWeight(String, u32),
    SetStoragePolicy(&'static str),
    SetStorageMinFree,
    /// Which table a move is being set up for. `None` closes the form.
    StorageMoveTable(Option<String>),
    StorageMoveTo(String),
    RunStorageMove,
    /// `true` for a dry run.
    Rebalance(bool),
    /// A statement that answers with lines rather than a row count.
    StorageReport(Result<Vec<String>, String>),
    DismissStorageReport,

    // --- activity
    ReloadActivity,
    ActivityLoaded(Box<ActivitySnapshot>),
    ResetStatementStats,

    // --- benchmark
    Bench(views::bench::BenchMessage),

    // --- branches
    ReloadBranches,
    BranchesLoaded(Box<BranchesSnapshot>),
    BranchField(views::branches::BranchField, String),
    BranchPoint(BranchPointKind),
    BranchKeepBase(bool),
    CreateBranch,
    DiffBranch(String),
    BranchDiffLoaded(String, Result<Vec<BranchDiffRow>, String>),
    MergePolicyChosen(MergePolicy),
    MergeAllowDrop(bool),
    MergeBranch(String),
    ResetBranch(String),
    PromoteBranch(String),
    DropBranch(String),
    // --- broker
    ReloadBroker,
    BrokerLoaded(Box<BrokerSnapshot>),
    BrokerKind(BrokerCreateKind),
    BrokerField(views::broker::BrokerField, String),
    BrokerViaExchange(bool),
    BrokerCreate,
    BrokerPublish,
    /// READ a topic (or a queue: it is a topic) into the message panel.
    BrokerRead(String),
    /// POP a queue: takes and acknowledges what it shows.
    BrokerPop(String),
    BrokerPartitions(String),
    /// A READ, POP or SHOW PARTITIONS answered: its statement and rows.
    BrokerBrowseLoaded(String, Result<Grid, String>),
    BrokerCloseBrowse,
    /// TOPIC, QUEUE or EXCHANGE, and its name.
    BrokerDrop(&'static str, String),
    /// A subscription's topic and name.
    BrokerDropSubscription(String, String),
    BrokerUnbind(BrokerBinding),
    BrokerPermKind(BrokerObjectKind),
    /// The first (`true`) or second privilege of the kind, on or off.
    BrokerPermToggle(bool, bool),
    /// GRANT (`false`) or REVOKE (`true`) what the form says.
    BrokerGrant(bool),
    BrokerOwner,
    /// REVOKE one listed grant.
    BrokerRevokeRow(BrokerGrant),

    // --- shards
    ReloadShards,
    // --- ledger (enterprise)
    #[cfg(feature = "enterprise")]
    ReloadLedger,
    #[cfg(feature = "enterprise")]
    LedgerLoaded(Box<rdb_studio_enterprise::LedgerPage>),
    #[cfg(feature = "enterprise")]
    VerifyLedger,
    #[cfg(feature = "enterprise")]
    LedgerVerified(Vec<String>),
    #[cfg(feature = "enterprise")]
    LedgerAudit(String),
    #[cfg(feature = "enterprise")]
    LedgerAuditLoaded(String, Vec<String>),
    /// Runs a statement the LEDGER page built, then reloads the page.
    #[cfg(feature = "enterprise")]
    LedgerRun(String),
    #[cfg(feature = "enterprise")]
    LedgerRan(Box<(String, Result<Vec<String>, String>)>),
    /// A LEDGER page input changed: (key, value).
    #[cfg(feature = "enterprise")]
    LedgerInput(String, String),
    /// Open a ledger table on the DATA tab.
    #[cfg(feature = "enterprise")]
    LedgerOpenTable(String),
    ShardsLoaded(Box<ShardsSnapshot>),
    /// Asks the gateway for the planner's moves.
    PlanRebalance,
    RebalancePlanned(Result<Vec<RebalanceStep>, String>),
    /// Confirms, then starts a rebalance on the gateway.
    StartRebalance,

    // --- replication
    ReloadReplication,
    ReplicationLoaded(Box<ReplicationSnapshot>),
    SetReplicationActive(bool),
    PeerName(String),
    PeerAddress(String),
    AddPeer,
    PausePeer(String, bool),
    ResyncPeer(String),
    DropPeer(String),

    // --- live dashboard
    Tick,
    Sampled(Result<Vec<Stat>, String>),
    ToggleLive,
    SetInterval(u64),
    ClearMetrics,

    // --- schema canvas
    SchemaPan(iced::Vector),
    SchemaZoom(f32),
    SchemaRelayout,

    // --- data browser
    PageNext,
    PagePrev,
    PageSize(String),
    DataScrolled(iced::widget::scrollable::Viewport),
    /// Sort a grid by one of its columns: the grid's scope, then the column
    /// key. Clicking the same header again reverses it, and a third time
    /// returns the grid to the server's order.
    SortColumn(String, String),
    BeginColumnResize(String, f32),
    ColumnResizeMove(f32),
    EndColumnResize,
    ResetColumnWidth(String),
    BeginEdit { row: usize, column: usize },
    EditValue(String),
    EditNull(bool),
    CommitEdit,
    CancelEdit,
    BeginInsert,
    CancelInsert,
    InsertValue(usize, String),
    CommitInsert,
    DeleteSelectedRow(usize),

    // --- query editor
    EditorAction(text_editor::Action),
    /// Enter, keeping the line's indentation.
    EditorNewline,
    /// Ctrl+/: comment the touched lines out, or back in.
    ToggleComment,
    /// Ctrl+Space: complete the name before the cursor.
    Complete,
    /// A completion picked from the list.
    ApplyCompletion(String),
    RunQuery,
    RunSelection,
    /// Ctrl+Shift+Enter: the statement the cursor is in, and no other.
    RunStatement,
    /// Put the cursor on a finding from the problems list.
    JumpTo(usize, usize),
    ToggleEditorSize,
    FormatSql,
    ClearEditor,
    InsertTemplate(Template),
    CopyResult(usize, crate::export::Format),
    ExportResult(usize, crate::export::Format),
    ExportData(crate::export::Format),
    ExportHistory,
    OpenSqlFile,
    SaveSqlFile,
    SqlFileOpened(Result<Option<(std::path::PathBuf, String)>, String>),
    /// Pick a CSV file to import into the selected table.
    ImportCsv,
    CsvOpened(Result<Option<(std::path::PathBuf, String)>, String>),
    /// How many rows went in, and the failure that stopped the rest.
    CsvImported(u64, Option<String>, Duration),
    FileSaved(Result<Option<std::path::PathBuf>, String>),
    Copied(String),
    LoadHistory(usize),
    RunHistory(usize),
    CopyHistory(usize),
    DeleteHistory(usize),
    ClearHistory,
    QueryFinished(crate::conn::EditorAnswer, Duration),
    /// The data browser's filter box.
    DataFilter(String),
    ApplyDataFilter,
    ClearDataFilter,
    /// Stop the run in flight.
    CancelQuery,
    CancelSent(Result<String, String>),
    SetRowLimit(RowLimit),
    /// Keep the editor's open transaction from being closed as idle.
    EditorKeepalive,
    EditorKept(Result<(), String>),

    // --- configuration
    ConfigFilter(String),
    ConfigEdit(String, String),
    ApplyConfig(String),
    ConfigApplied(String, Duration, Result<String, String>),

    // --- modals and statements
    OpenModal(ModalKind),
    ModalField(ModalField, String),
    ModalToggle(ModalField, bool),
    ModalAddColumn,
    /// Open the dialog that renames a column or changes its type.
    EditColumn(String),
    ModalRemoveColumn(usize),
    ModalColumnField(usize, ModalField, String),
    ModalColumnToggle(usize, ModalField, bool),
    ModalType(types::Choice),
    ModalColumnType(usize, types::Choice),
    ModalSubmit,
    ModalCancel,
    IndexForm(IndexEdit),
    PartitionForm(PartitionEdit),
    Confirm { title: String, body: String, statement: String, refresh: Refresh },
    StatementFinished(Refresh, String, Duration, Result<String, String>),
    /// A dialog's statement came back; see [`Studio::modal_pending`].
    ModalStatementFinished(Refresh, String, Duration, Result<String, String>),

    // --- the table pane
    SelectDetailSection(DetailSection),
    /// Confirm, then drop an index or the constraint owning it, and check
    /// afterwards that it is gone.
    DropIndex { title: String, body: String, statement: String, index: String },
    ShowEmptyPartitions(bool),
    CheckIndexes,
    IndexCheckFinished(String, Duration, Result<Grid, String>),
}

/// The things that can be done to a backup destination.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackupAction {
    Snapshot,
    ArchiveWal,
    /// Applies the retention policy. **Deletes objects from the bucket**, so it
    /// goes through a confirmation naming what it will remove.
    Prune,
    /// Takes a fresh full snapshot and then deletes every earlier generation,
    /// leaving the bucket holding one self-contained backup.
    ///
    /// **Deletes objects**, so it confirms — but the order is what makes it
    /// safe to run: nothing is removed unless the new snapshot succeeded, so a
    /// failed rollover leaves the previous backup exactly as it was.
    Rollover,
}

impl BackupAction {
    fn label(&self) -> &'static str {
        match self {
            BackupAction::Snapshot => "BACKUP NOW",
            BackupAction::ArchiveWal => "ARCHIVE WAL",
            BackupAction::Prune => "PRUNE",
            BackupAction::Rollover => "FORCE ROLLOVER",
        }
    }

    fn sql(&self) -> &'static str {
        match self {
            BackupAction::Snapshot => crate::dialect::backup_now_sql(),
            BackupAction::ArchiveWal => crate::dialect::archive_wal_sql(),
            BackupAction::Prune => crate::dialect::prune_sql(),
            BackupAction::Rollover => crate::dialect::rollover_sql(),
        }
    }
}

/// A statement skeleton for the selected table.
///
/// Not a wizard: it writes the statement into the editor and leaves it there.
/// The point is to save typing the column list correctly, which is where the
/// mistakes are.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Template {
    Select,
    Count,
    Insert,
    Update,
    Delete,
}

impl Template {
    pub const ALL: [Template; 5] = [
        Template::Select,
        Template::Count,
        Template::Insert,
        Template::Update,
        Template::Delete,
    ];

    pub fn label(&self) -> &'static str {
        match self {
            Template::Select => "SELECT",
            Template::Count => "COUNT",
            Template::Insert => "INSERT",
            Template::Update => "UPDATE",
            Template::Delete => "DELETE",
        }
    }

    /// Builds the statement, with the table's real columns in it.
    ///
    /// The write templates carry a `WHERE` that matches nothing — `WHERE
    /// <key> = ?` with a placeholder — rather than no `WHERE` at all. An
    /// `UPDATE` skeleton without one is a whole-table update waiting for
    /// somebody to press run before finishing the thought.
    pub fn render(&self, table: &str, columns: &[ColumnInfo], key: Option<&str>) -> String {
        let quoted = crate::dialect::quote_ident(table);
        let names: Vec<String> = columns
            .iter()
            .map(|c| crate::dialect::quote_ident(&c.name))
            .collect();
        let key_predicate = match key {
            Some(key) => format!("{} = ?", crate::dialect::quote_ident(key)),
            None => "?".to_string(),
        };

        match self {
            Template::Select => format!(
                "SELECT {}\nFROM {}\nLIMIT 100;",
                if names.is_empty() { "*".to_string() } else { names.join(", ") },
                quoted
            ),
            Template::Count => format!("SELECT COUNT(*) AS n\nFROM {};", quoted),
            Template::Insert => {
                // The key is left out when the server generates it, which is
                // the common case and the one people get wrong.
                let writable: Vec<&ColumnInfo> = columns
                    .iter()
                    .filter(|c| !(Some(c.name.as_str()) == key && c.default.contains("nextval")))
                    .collect();
                format!(
                    "INSERT INTO {} ({})\nVALUES ({});",
                    quoted,
                    writable
                        .iter()
                        .map(|c| crate::dialect::quote_ident(&c.name))
                        .collect::<Vec<_>>()
                        .join(", "),
                    writable.iter().map(|_| "?").collect::<Vec<_>>().join(", ")
                )
            }
            Template::Update => format!(
                "UPDATE {}\nSET {}\nWHERE {};",
                quoted,
                columns
                    .iter()
                    .filter(|c| Some(c.name.as_str()) != key)
                    .map(|c| format!("{} = ?", crate::dialect::quote_ident(&c.name)))
                    .collect::<Vec<_>>()
                    .join(", "),
                key_predicate
            ),
            Template::Delete => format!("DELETE FROM {}\nWHERE {};", quoted, key_predicate),
        }
    }
}

impl std::fmt::Display for Template {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.label())
    }
}

/// Which box of the import form was typed into.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ImportField {
    Host,
    Port,
    Database,
    Schema,
    User,
    Password,
    Target,
    Exclude,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ModalKind {
    CreateRole,
    CreateTable,
    CreateDatabase,
    AddColumn,
    CreateIndex,
    /// A table's `row_compression`, and a rewrite of its rows under it.
    RowCompression,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ModalField {
    /// A compression method: `zstd`, `zstd(9)`, `none`, `default`.
    Compression,
    /// Whether to rewrite the existing rows (`ALTER TABLE … RECOMPRESS`).
    Recompress,
    Name,
    Length,
    Precision,
    Scale,
    Nullable,
    PrimaryKey,
    Unique,
    Default,
    /// A column's type, as written.
    DataType,
    Password,
    Login,
    Superuser,
}

/// Connects with the profile's saved password read from the OS keyring first,
/// when it keeps one there and none was typed.
async fn connect_saved(profile: Connection) -> Result<Session, String> {
    let profile = if profile.password.is_empty() && profile.password_in_keyring() {
        profiles::blocking(move || profiles::with_saved_password(profile, &profiles::OsKeyring))
            .await?
    } else {
        profile
    };
    conn::connect(profile).await
}

/// Reads a profile's saved password from the OS keyring into the form, so
/// that it shows as filled in and goes with the profile if it is saved under
/// another name.
fn read_saved_password(profile: &Connection) -> Task<Message> {
    if !profile.password.is_empty() || !profile.password_in_keyring() {
        return Task::none();
    }
    let name = profile.name.clone();
    let lookup = name.clone();
    Task::perform(
        profiles::blocking(move || profiles::Vault::get(&profiles::OsKeyring, &lookup)),
        move |read| Message::SavedPasswordRead(name, read),
    )
}

impl Studio {
    pub fn new(opening: Option<String>) -> (Self, Task<Message>) {
        let saved = profiles::load();
        // A profile named on the command line is opened, not merely selected:
        // `rdb_studio local` should put you in front of the database, and
        // stopping at a filled-in form with one button to press is a step that
        // exists only because the code was easier to write that way.
        let named = opening
            .as_deref()
            .and_then(|name| saved.iter().find(|p| p.name == name).cloned());
        let form = named
            .clone()
            .or_else(|| saved.first().cloned())
            .unwrap_or_default();

        let studio = Self {
            form,
            profiles: saved,
            session: None,
            connecting: false,
            connect_error: None,
            tab: Tab::Dashboard,
            status: Status::Idle,
            databases: Vec::new(),
            tables: Vec::new(),
            table_filter: String::new(),
            selected_table: None,
            db_stats: Vec::new(),
            table_stats: Vec::new(),
            columns: Vec::new(),
            indexes: Vec::new(),
            partitions: Vec::new(),
            detail_section: DetailSection::Statistics,
            expect_gone: None,
            verify_gone: None,
            show_empty_partitions: false,
            schema: SchemaGraph::default(),
            schema_pan: iced::Vector::ZERO,
            schema_zoom: 1.0,
            schema_cache: iced::widget::canvas::Cache::new(),
            config: Vec::new(),
            config_filter: String::new(),
            config_edits: HashMap::new(),
            data: Grid::default(),
            data_page: 0,
            page_size: DEFAULT_PAGE,
            data_total: None,
            data_keyset: Keyset::default(),
            data_filter: String::new(),
            data_filter_applied: None,
            editing: None,
            inserting: None,
            column_widths: HashMap::new(),
            resizing: None,
            sort: HashMap::new(),
            data_viewport: None,
            editor: text_editor::Content::with_text(
                "-- Ctrl+Enter runs everything in this editor.\n\
                 -- Several statements are fine; each reports separately.\n\
                 SHOW TABLES;\n",
            ),
            editor_tall: false,
            results: Vec::new(),
            query_error: None,
            history: Vec::new(),
            pending_sql: None,
            last_elapsed: None,
            query_cancel: None,
            cancel_sent: false,
            row_limit: RowLimit::default(),
            query_notices: Vec::new(),
            roles: Vec::new(),
            role_access: crate::model::RoleAccess::default(),
            role_selected: None,
            role_inputs: std::collections::BTreeMap::new(),
            completions: Vec::new(),
            kv: KvBrowser::default(),
            editor_transaction: None,
            modal: None,
            modal_pending: false,
            metrics: Metrics::default(),
            live: true,
            interval: 2,
            sampled_at: None,
            metrics_error: None,
            flavor_warning: None,
            backup: None,
            import: dialect::ImportRequest::default(),
            import_running: false,
            import_report: Vec::new(),
            activity: None,
            bench: views::bench::Page::default(),
            branches: None,
            branch_form: BranchForm::default(),
            branch_diff: None,
            broker: None,
            broker_form: BrokerForm::default(),
            broker_publish: BrokerPublishForm::default(),
            broker_read: BrokerReadForm::default(),
            broker_browse: None,
            broker_perm: BrokerPermForm::default(),
            merge_policy: MergePolicy::Fail,
            merge_allow_drop: false,
            shards: None,
            #[cfg(feature = "enterprise")]
            ledger: None,
            rebalance_plan: None,
            replication: None,
            peer_name: String::new(),
            peer_address: String::new(),
            storage: None,
            storage_path: String::new(),
            storage_role: "primary",
            storage_weight: "1".to_string(),
            storage_min_free: String::new(),
            storage_move_table: None,
            storage_move_to: None,
            storage_report: Vec::new(),
            storage_busy: false,
            backup_busy: None,
            restore_dir: String::new(),
            restore_kind: RestoreKind::Latest,
            restore_snapshot: String::new(),
            restore_lsn: String::new(),
            restore_time: String::new(),
            restore_running: false,
            restore_report: None,
        };

        let opening = named.is_some();
        let mut studio = studio;
        if opening {
            studio.connecting = true;
            studio.status = Status::Busy(format!("connecting to {}", studio.form.host));
        }

        let boot = if opening {
            let profile = studio.form.clone();
            Task::perform(connect_saved(profile), Message::Connected)
        } else {
            read_saved_password(&studio.form)
        };

        (studio, boot)
    }

    pub fn title(&self) -> String {
        match &self.session {
            Some(session) => format!(
                "rdb_studio — {} · {} · {}",
                session.database, session.server, session.flavor
            ),
            None => "rdb_studio".to_string(),
        }
    }

    /// The dashboard's clock, and only when it is actually being watched.
    ///
    /// Three conditions, each of which has to hold: there is a connection, the
    /// dashboard is not paused, and its tab is the one on screen. The last one
    /// matters most — a console sitting on the query editor for an hour should
    /// not be sending a statistics query every two seconds to a database
    /// nobody is looking at.
    pub fn subscription(&self) -> Subscription<Message> {
        let mut subscriptions = Vec::new();
        // A rebalance running: its status every two seconds.
        if self.session.is_some() && self.tab == Tab::Shards && self.shards.as_ref().is_some_and(ShardsSnapshot::rebalance_running) {
            subscriptions.push(iced::time::every(Duration::from_secs(2)).map(|_| Message::ReloadShards));
        }
        if self.session.is_some() && self.live && self.tab == Tab::Dashboard {
            subscriptions.push(
                iced::time::every(Duration::from_secs(self.interval.max(1))).map(|_| Message::Tick),
            );
        }
        // A native server closes a connection quiet for its request timeout
        // (30 s by default), rolling back the block on it: an open block in
        // the editor is kept alive while its user thinks.
        let in_block = self
            .editor_transaction
            .is_some_and(|t| t != rasterizeddb_client::TransactionStatus::Idle);
        if in_block && self.query_cancel.is_none() {
            subscriptions.push(
                iced::time::every(EDITOR_KEEPALIVE).map(|_| Message::EditorKeepalive),
            );
        }
        Subscription::batch(subscriptions)
    }

    pub fn view(&self) -> Element<'_, Message> {
        views::view(self)
    }

    /// The dialect in play, defaulting to RasterizedDB before a connection
    /// exists so the connection screen has something to show.
    pub fn flavor(&self) -> Flavor {
        self.session.as_ref().map(|s| s.flavor).unwrap_or(self.form.flavor)
    }

    /// The selected table's partition key, when it is partitioned.
    ///
    /// Read from `SHOW TABLE STATS`, which reports it only for a partitioned
    /// table: `SHOW TABLES` lists one exactly like a plain table.
    pub fn partition_key(&self) -> Option<&str> {
        self.table_stats
            .iter()
            .find(|s| s.metric == "partition_key")
            .map(|s| s.value.as_str())
    }

    /// The selected table's `jsonb` columns, which are what a document or path
    /// index can be over.
    pub fn document_columns(&self) -> Vec<String> {
        self.columns
            .iter()
            .filter(|c| c.data_type.to_ascii_uppercase().contains("JSON"))
            .map(|c| c.name.clone())
            .collect()
    }

    /// The tables the sidebar shows, after the filter box.
    pub fn filtered_tables(&self) -> Vec<&TableInfo> {
        let needle = self.table_filter.to_ascii_lowercase();
        self.tables
            .iter()
            .filter(|t| needle.is_empty() || t.name.to_ascii_lowercase().contains(&needle))
            .collect()
    }

    /// The sort a header click asked the data browser for, if any.
    ///
    /// Keyed by table, like the grid it belongs to: opening another table shows
    /// that table's own sort rather than inheriting one for a column it may not
    /// even have.
    pub fn data_sort(&self) -> Option<GridSort> {
        let table = self.selected_table.as_deref()?;
        let sort = self.sort.get(&format!("{}{}", views::DATA_SCOPE, table))?;

        // A column that has since been dropped would go out as `ORDER BY
        // "gone"`, and the browser would answer every page with the server's
        // error — under a header that no longer has the column to click a
        // second time. An empty column list means they have not loaded yet,
        // which is not the same as the column being absent.
        if !self.columns.is_empty()
            && !self.columns.iter().any(|c| c.name.eq_ignore_ascii_case(&sort.column))
        {
            return None;
        }
        Some(sort.clone())
    }

    /// The column the data browser orders and addresses rows by.
    ///
    /// A table with nothing that identifies one row cannot be edited row-wise:
    /// an `UPDATE` matching on the row's own values hits every duplicate of it,
    /// reports success, and is undetectable afterwards. The browser goes
    /// read-only rather than doing that.
    ///
    /// It does *not* insist on a declared primary key. A `UNIQUE NOT NULL`
    /// column addresses exactly one row too, and requiring the declaration made
    /// every table created before the engine started recording primary keys
    /// read-only — which is most of the tables in any existing database. See
    /// [`RowKey::choose`].
    pub fn row_key(&self) -> Option<RowKey> {
        let table = self.selected_table.as_ref()?;
        let declared = self
            .tables
            .iter()
            .find(|t| &t.name == table)
            .map(|t| t.primary_key.as_str())
            .unwrap_or("");
        RowKey::choose(declared, &self.columns)
    }

    pub fn update(&mut self, message: Message) -> Task<Message> {
        match message {
            // ---- connection form -------------------------------------------
            Message::FormName(v) => { self.form.name = v; Task::none() }
            Message::FormHost(v) => { self.form.host = v; Task::none() }
            Message::FormPort(v) => {
                // Kept as typed until it parses, so backspacing to empty does
                // not snap the field back to a number the user did not type.
                if v.is_empty() {
                    self.form.port = 0;
                } else if let Ok(port) = v.parse() {
                    self.form.port = port;
                }
                Task::none()
            }
            Message::FormDatabase(v) => { self.form.database = v; Task::none() }
            Message::FormUser(v) => { self.form.user = v; Task::none() }
            Message::FormPassword(v) => { self.form.password = v; Task::none() }
            Message::FormFlavor(v) => {
                // Switching flavour can switch the protocol (PostgreSQL has
                // only the wire protocol), and a port still at the old
                // protocol's default is almost always a mistake about to be
                // made, so it follows. One somebody typed is theirs.
                let before = self.form.effective_transport().default_port();
                self.form.flavor = v;
                if self.form.port == before || self.form.port == 0 {
                    self.form.port = self.form.effective_transport().default_port();
                }
                Task::none()
            }
            Message::FormTls(v) => { self.form.tls = v; Task::none() }
            Message::FormTransport(v) => {
                // A port still at the other protocol's default follows the
                // switch; one somebody typed is theirs.
                if self.form.port == self.form.transport.default_port() || self.form.port == 0 {
                    self.form.port = v.default_port();
                }
                self.form.transport = v;
                Task::none()
            }
            Message::FormLogin(v) => { self.form.login = v; Task::none() }
            Message::FormVerify(v) => { self.form.verify_server = v; Task::none() }
            Message::FormCaFile(v) => { self.form.ca_file = v; Task::none() }
            Message::FormSavePassword(v) => { self.form.save_password = v; Task::none() }
            Message::FormPasswordStore(v) => { self.form.password_store = Some(v); Task::none() }

            Message::LoadProfile(index) => {
                if let Some(profile) = self.profiles.get(index) {
                    self.form = profile.clone();
                }
                read_saved_password(&self.form)
            }
            Message::SavedPasswordRead(name, read) => {
                match read {
                    // Only into the profile it was read for, and never over
                    // one typed while the keyring was being asked.
                    Ok(Some(password)) => {
                        if self.form.name == name && self.form.password.is_empty() {
                            self.form.password = password;
                        }
                    }
                    Ok(None) => {}
                    Err(e) => {
                        self.status = Status::Failed(format!(
                            "could not read the saved password of `{name}`: {e}"
                        ))
                    }
                }
                Task::none()
            }
            Message::SaveProfile => {
                let form = self.form.clone();
                let previous = self.profiles.iter().find(|p| p.name == form.name).cloned();
                if !profiles::keeping_touches_keyring(&form, previous.as_ref()) {
                    return self.update(Message::ProfileSaved(form, Ok(())));
                }
                self.status = Status::Busy(format!("saving the password of `{}`", form.name));
                let keeping = form.clone();
                Task::perform(
                    profiles::blocking(move || {
                        profiles::keep_password(&keeping, previous.as_ref(), &profiles::OsKeyring)
                    }),
                    move |kept| Message::ProfileSaved(form, kept),
                )
            }
            Message::ProfileSaved(form, kept) => {
                if let Err(e) = kept {
                    self.status = Status::Failed(format!("profile `{}` was not saved: {e}", form.name));
                    return Task::none();
                }
                match self.profiles.iter().position(|p| p.name == form.name) {
                    Some(index) => self.profiles[index] = form.clone(),
                    None => self.profiles.push(form.clone()),
                }
                self.status = match profiles::save(&self.profiles) {
                    Ok(()) => Status::Done(format!("saved profile `{}`", form.name)),
                    Err(e) => Status::Failed(e),
                };
                Task::none()
            }
            Message::DeleteProfile(index) => {
                if index >= self.profiles.len() {
                    return Task::none();
                }
                let removed = self.profiles.remove(index);
                if let Err(e) = profiles::save(&self.profiles) {
                    self.status = Status::Failed(e);
                }
                if !removed.password_in_keyring() {
                    return Task::none();
                }
                let name = removed.name.clone();
                Task::perform(
                    profiles::blocking(move || {
                        profiles::forget_password(&removed, &profiles::OsKeyring)
                    }),
                    move |forgotten| Message::ProfileForgotten(name, forgotten),
                )
            }
            Message::ProfileForgotten(name, forgotten) => {
                if let Err(e) = forgotten {
                    self.status = Status::Failed(format!(
                        "deleted profile `{name}`, but its password is still in the OS keyring: {e}"
                    ));
                }
                Task::none()
            }

            Message::Connect => {
                self.connecting = true;
                self.connect_error = None;
                self.status = Status::Busy(format!("connecting to {}", self.form.host));
                let profile = self.form.clone();
                Task::perform(connect_saved(profile), Message::Connected)
            }
            Message::Connected(Ok(session)) => {
                self.connecting = false;
                self.session = Some(session.clone());
                self.query_cancel = None;
                self.cancel_sent = false;
                self.editor_transaction = None;
                self.status = Status::Done(format!(
                    "connected to {} ({}, {})",
                    session.server,
                    session.transport,
                    match session.link() {
                        "TLS VERIFIED" => "TLS, server verified",
                        "TLS" => "TLS, server not verified",
                        "SOCKET" => "unix socket",
                        _ => "cleartext",
                    }
                ));
                // The benchmark's default follows the session: what the
                // other protocol offers would be refused.
                self.bench.protocol = match (session.transport, self.bench.protocol) {
                    (crate::model::Transport::Native, _) => crate::bench::Protocol::Native,
                    (crate::model::Transport::Wire, crate::bench::Protocol::Native) => {
                        crate::bench::Protocol::Simple
                    }
                    (crate::model::Transport::Wire, protocol) => protocol,
                };
                Task::batch([
                    Task::perform(async move { conn::detect_flavor(&session).await }, Message::FlavorDetected),
                    self.reload_everything(),
                ])
            }
            Message::Connected(Err(e)) => {
                self.connecting = false;
                self.connect_error = Some(e.clone());
                self.status = Status::Failed(e);
                Task::none()
            }
            Message::FlavorDetected(detected) => {
                // Reported, never silently applied: the user chose a dialect,
                // and a console that overrode that choice would send
                // `pg_catalog` queries to a server the user told it was not
                // PostgreSQL — or the reverse.
                self.flavor_warning = (detected != self.flavor()).then(|| {
                    format!(
                        "This server answers like {}, but the connection is set to {}. \
                         Every panel will fail until the dialect matches — reconnect with \
                         Dialect set to {}.",
                        detected,
                        self.flavor(),
                        detected
                    )
                });
                Task::none()
            }
            Message::Disconnect => {
                // Its workers have connections of their own; they would carry
                // on against a server the console has left.
                self.bench.stop();
                self.session = None;
                self.query_cancel = None;
                self.cancel_sent = false;
                self.editor_transaction = None;
                self.kv = KvBrowser::default();
                self.roles.clear();
                self.flavor_warning = None;
                self.backup = None;
                self.activity = None;
                self.replication = None;
                self.shards = None;
                self.rebalance_plan = None;
                self.storage = None;
                self.storage_report.clear();
                self.tables.clear();
                self.db_stats.clear();
                self.schema = SchemaGraph::default();
                self.data = Grid::default();
                self.status = Status::Idle;
                Task::none()
            }

            // ---- navigation ------------------------------------------------
            Message::SelectTab(tab) => {
                self.tab = tab;
                // A revealed vault value does not outlive the page it was
                // revealed on.
                #[cfg(feature = "enterprise")]
                if tab != Tab::Ledger {
                    if let Some(page) = self.ledger.as_mut() {
                        page.hide();
                    }
                }
                match tab {
                    // Sampled on arrival so the page is not blank for a tick.
                    Tab::Dashboard if self.live => self.sample(),
                    // Loaded on arrival rather than up front: the schema map
                    // and the configuration are each a round trip nobody
                    // needs until they open the tab.
                    Tab::Schema if self.schema.tables.is_empty() => self.load_schema(),
                    Tab::Config if self.config.is_empty() => self.load_config(),
                    Tab::Roles => self.load_roles(),
                    Tab::Keys if self.kv.keys.is_empty() => self.kv_scan(false),
                    Tab::Data if self.data.columns.is_empty() => self.load_page(),
                    // Always reloaded rather than cached: a backup catalogue
                    // that is minutes stale is the one thing on this page that
                    // must not be, since the whole point of opening it is
                    // usually that something has gone wrong.
                    Tab::Backup => self.load_backups(),
                    // And again, most of all here: this page is a list of what
                    // the server has just done, so a cached one is a list of
                    // what it had done when the tab was last open.
                    Tab::Activity => self.load_activity(),
                    // Same reasoning as the backup catalogue: a stale answer on
                    // this page is worse than a slow one.
                    Tab::Replication => self.load_replication(),
                    // A map changes when a shard is added or a range moves.
                    Tab::Shards => self.load_shards(),
                    #[cfg(feature = "enterprise")]
                    Tab::Ledger => self.load_ledger(),
                    // And again: free space is the figure this page exists to
                    // report, and a cached one is worse than useless.
                    Tab::Storage => self.load_storage(),
                    // A list of databases, which other consoles make and drop.
                    Tab::Branches => self.load_branches(),
                    // Depths and backlogs move all the time.
                    Tab::Broker => self.load_broker(),
                    _ => Task::none(),
                }
            }
            Message::SelectTable(name) => {
                let changed = self.selected_table.as_deref() != Some(name.as_str());
                self.selected_table = Some(name);
                if changed {
                    self.data_page = 0;
                    self.data_viewport = None;
                    self.data = Grid::default();
                    self.data_total = None;
                    // A condition on the old table's columns.
                    self.data_filter.clear();
                    self.data_filter_applied = None;
                    self.editing = None;
                    self.inserting = None;
                    // The previous table's columns are not this one's. Left in
                    // place, `row_key` reads them and pages the new table by a
                    // column that belongs to the old one — `SELECT * FROM
                    // "rdb_roles" ORDER BY "id"`, which has no `id`, and the
                    // whole browser reports an error for the table.
                    self.columns.clear();
                    self.indexes.clear();
                    self.partitions.clear();
                }
                // No `load_page` here: it is issued once the columns land, so
                // the key it orders by is this table's. See `ColumnsLoaded`.
                Task::batch([self.load_table_stats(), self.load_columns()])
            }
            Message::SelectDatabase(name) => {
                // Switching database means a new connection: the protocol binds
                // one per session, and both engines refuse to change it after
                // start-up.
                self.form.database = name;
                self.update(Message::Connect)
            }
            Message::ScrollTabs(by) => iced::widget::operation::scroll_by(
                crate::views::tab_strip_id(),
                iced::widget::operation::AbsoluteOffset { x: by, y: 0.0 },
            ),
            Message::TableFilter(v) => { self.table_filter = v; Task::none() }
            Message::RefreshAll => self.reload_everything(),

            // ---- loading ---------------------------------------------------
            Message::StatsLoaded(result) => {
                self.absorb(result, |studio, stats| studio.db_stats = stats)
            }
            Message::TablesLoaded(result) => {
                let previous = self.selected_table.clone();
                let task = self.absorb(result, |studio, tables| {
                    studio.tables = tables;
                    // A table that vanished under us must not stay selected, or
                    // every panel keeps querying a relation that is gone.
                    if let Some(selected) = &studio.selected_table
                        && !studio.tables.iter().any(|t| &t.name == selected)
                    {
                        studio.selected_table = None;
                    }
                    if studio.selected_table.is_none() {
                        studio.selected_table = studio.tables.first().map(|t| t.name.clone());
                    }
                });

                // Selecting a table here has to pull its columns, statistics and
                // first page, exactly as clicking one does. Without this the
                // table auto-selected on connect showed empty panels until the
                // user clicked something — and clicking the *already selected*
                // row is a no-op, so the way out was not obvious.
                if self.selected_table != previous && self.selected_table.is_some() {
                    self.columns.clear();
                    return Task::batch([task, self.load_table_stats(), self.load_columns()]);
                }
                task
            }
            Message::DatabasesLoaded(result) => {
                // A server without a database list is not an error worth
                // showing: RasterizedDB serving one database still answers, and
                // a PostgreSQL role without `pg_database` access simply gets a
                // sidebar with one entry.
                if let Ok(databases) = result {
                    self.databases = databases;
                }
                Task::none()
            }
            Message::TableStatsLoaded(result) => {
                let task = self.absorb(result, |studio, stats| studio.table_stats = stats);
                // Partitions are asked for only once the table says it has
                // them: `SHOW PARTITIONS` refuses every plain table, and that
                // refusal would land in the status bar on every click.
                if self.partition_key().is_some() {
                    Task::batch([task, self.load_partitions()])
                } else {
                    self.partitions.clear();
                    if self.detail_section == DetailSection::Partitions {
                        self.detail_section = DetailSection::Statistics;
                    }
                    task
                }
            }
            Message::IndexesLoaded(table, result) => {
                if self.selected_table.as_deref() != Some(table.as_str()) {
                    return Task::none();
                }
                let task = self.absorb(result, |studio, indexes| studio.indexes = indexes);
                if let Some((dropped_from, index)) = self.verify_gone.take()
                    && dropped_from == table
                    && self.indexes.iter().any(|i| i.name == index)
                {
                    self.status = Status::Failed(format!(
                        "the server reported success, but `{}` is still there: a RasterizedDB \
                         server before fc3849c keeps a constraint's index when asked to drop it",
                        index
                    ));
                }
                task
            }
            Message::PartitionsLoaded(table, result) => {
                if self.selected_table.as_deref() != Some(table.as_str()) {
                    return Task::none();
                }
                self.absorb(result, |studio, partitions| studio.partitions = partitions)
            }
            Message::ColumnsLoaded(result) => {
                // The page waits for this: `row_key` needs the columns of the
                // table being paged, not of whichever one was open before.
                // Loaded even when the columns failed, since an unordered page
                // is better than none — `row_key` then finds no key and the
                // query goes out without an ORDER BY.
                let task = self.absorb(result, |studio, columns| studio.columns = columns);
                Task::batch([task, self.load_page(), self.load_indexes()])
            }
            Message::SchemaLoaded(result) => self.absorb(result, |studio, schema| {
                studio.schema = schema;
                studio.schema_pan = iced::Vector::ZERO;
                studio.schema_zoom = 1.0;
                studio.schema_cache.clear();
            }),
            Message::KvPattern(pattern) => { self.kv.pattern = pattern; Task::none() }
            Message::KvScan(more) => self.kv_scan(more),
            Message::KvScanned(more, result) => self.absorb(result, |studio, (cursor, keys, size)| {
                if !more {
                    studio.kv.keys.clear();
                }
                studio.kv.keys.extend(keys);
                studio.kv.cursor = cursor;
                if size.is_some() {
                    studio.kv.size = size;
                }
            }),
            Message::KvSelect(key) => {
                let Some(session) = self.session.clone() else { return Task::none() };
                self.kv.selected = None;
                Task::perform(
                    async move {
                        let described = session.run(&dialect::kv_describe_sql(&key)).await?;
                        let first = |i: usize| match described.get(i) {
                            Some(Outcome::Rows(grid)) => {
                                grid.rows.first().and_then(|r| r.first().cloned()).flatten()
                            }
                            _ => None,
                        };
                        let kind = first(0).unwrap_or_else(|| "none".to_string());
                        let ttl = first(1).and_then(|t| t.parse().ok()).unwrap_or(-1);
                        let value = match dialect::kv_value_sql(&kind, &key) {
                            Some(sql) => Some(session.query(&sql).await?),
                            None => None,
                        };
                        Ok(KvEntry { key, kind, ttl, value })
                    },
                    Message::KvLoaded,
                )
            }
            Message::KvLoaded(result) => self.absorb(result, |studio, entry| {
                studio.kv.selected = Some(entry);
            }),
            Message::RolesLoaded(result) => self.absorb(result, |studio, roles| {
                studio.roles = roles;
                if studio.role_selected.as_ref().is_some_and(|r| !studio.roles.iter().any(|x| &x.name == r)) {
                    studio.role_selected = None;
                }
            }),
            Message::RoleAccessLoaded(access) => {
                self.role_access = *access;
                Task::none()
            }
            Message::RoleManage(role) => {
                self.role_inputs.clear();
                if let Some(r) = role.as_ref().and_then(|n| self.roles.iter().find(|r| &r.name == n)) {
                    // The attribute form starts from what the role has.
                    for (k, on) in [
                        ("login", r.can_login),
                        ("superuser", r.superuser),
                        ("createdb", r.create_db),
                        ("createrole", r.create_role),
                        ("bypassrls", r.bypass_rls),
                    ] {
                        self.role_inputs.insert(k.to_string(), on.to_string());
                    }
                    if r.conn_limit >= 0 {
                        self.role_inputs.insert("connlimit".into(), r.conn_limit.to_string());
                    }
                    if let Some(v) = &r.valid_until {
                        self.role_inputs.insert("validuntil".into(), v.clone());
                    }
                }
                self.role_selected = role;
                Task::none()
            }
            Message::RoleInput(key, value) => {
                self.role_inputs.insert(key, value);
                Task::none()
            }
            Message::RoleRun(sql) => {
                // A form's text is spent once sent; the attribute toggles stay.
                for k in ["password", "rename", "grant_table", "policy_name", "policy_using", "policy_check", "member"] {
                    self.role_inputs.remove(k);
                }
                self.statement(sql, Refresh::Roles)
            }
            Message::ConfigLoaded(result) => self.absorb(result, |studio, config| {
                studio.config = config;
                studio.config_edits.clear();
            }),
            Message::DataLoaded(page, signature, result) => self.absorb(result, |studio, (grid, total)| {
                studio.note_page_end(page, &signature, &grid);
                studio.data = grid;
                if let Some(total) = total {
                    studio.data_total = Some(total);
                }
            }),

            // ---- backups ------------------------------------------------------
            Message::ReloadBackups => self.load_backups(),
            Message::BackupLoaded(snapshot) => {
                // Never routed through `absorb`: every failure here is a state
                // of the backup system that the page explains far better than a
                // red status line would, and none of them is a reason to stop
                // showing the rest of the console.
                self.backup = Some(*snapshot);
                Task::none()
            }
            Message::Backup(action) => self.run_backup(action),
            Message::BackupFinished(sql, elapsed, Ok(message)) => {
                self.backup_busy = None;
                self.record(sql, elapsed, RunOutcome::Ok { statements: 1, rows: 0, affected: 0 });
                // A snapshot of a large database is minutes of work, and how
                // long it took is the figure an operator writes down.
                self.status = Status::Done(format!("{} · {}", message, duration(elapsed)));
                self.load_backups()
            }
            Message::BackupFinished(sql, elapsed, Err(e)) => {
                self.backup_busy = None;
                self.record(sql, elapsed, RunOutcome::Failed(e.clone()));
                self.status = Status::Failed(e);
                Task::none()
            }

            Message::UseSnapshot(id) => {
                self.restore_kind = RestoreKind::Snapshot;
                self.restore_snapshot = id;
                Task::none()
            }
            Message::RestoreDir(v) => { self.restore_dir = v; Task::none() }
            Message::RestoreKindChanged(kind) => { self.restore_kind = kind; Task::none() }
            Message::RestoreSnapshot(v) => { self.restore_snapshot = v; Task::none() }
            Message::RestoreLsn(v) => {
                // Only digits: an LSN is a commit sequence, and letting a typo
                // through means the server rejects the statement after the
                // confirmation dialog has been read and accepted.
                self.restore_lsn = v.chars().filter(char::is_ascii_digit).collect();
                Task::none()
            }
            Message::RestoreTime(v) => { self.restore_time = v; Task::none() }
            Message::RunRestore => self.confirm_restore(),
            Message::RestoreFinished(sql, elapsed, Ok(grid)) => {
                self.restore_running = false;
                self.record(sql, elapsed, RunOutcome::Ok { statements: 1, rows: 0, affected: 0 });
                self.restore_report = dialect::parse_restore_report(&grid);
                self.status = Status::Done(match &self.restore_report {
                    Some(report) if report.records_discarded > 0 => format!(
                        "restored into {} in {} — {} records discarded",
                        report.target_dir,
                        duration(elapsed),
                        report.records_discarded
                    ),
                    Some(report) => {
                        format!("restored into {} in {}", report.target_dir, duration(elapsed))
                    }
                    None => "restore finished".to_string(),
                });
                Task::none()
            }
            Message::RestoreFinished(sql, elapsed, Err(e)) => {
                self.restore_running = false;
                self.record(sql, elapsed, RunOutcome::Failed(e.clone()));
                self.status = Status::Failed(e);
                Task::none()
            }

            // ---- importing another database -----------------------------------
            Message::ImportField(field, value) => {
                let import = &mut self.import;
                match field {
                    ImportField::Host => import.host = value,
                    ImportField::Port => import.port = digits(&value),
                    ImportField::Database => import.database = value,
                    ImportField::Schema => import.schema = value,
                    ImportField::User => import.user = value,
                    ImportField::Password => import.password = value,
                    ImportField::Target => import.target = value,
                    ImportField::Exclude => import.exclude = value,
                }
                Task::none()
            }
            Message::ImportSslMode(mode) => { self.import.sslmode = mode; Task::none() }
            Message::ImportDryRun(v) => { self.import.dry_run = v; Task::none() }
            Message::ImportSkipConstraints(v) => { self.import.skip_constraints = v; Task::none() }

            Message::RunImport => {
                let sql = match dialect::import_sql(&self.import) {
                    Ok(sql) => sql,
                    Err(e) => { self.status = Status::Failed(e); return Task::none() }
                };

                // A preview writes nothing, so it just runs. A real import
                // creates a database and streams another one into it, which is
                // worth being asked about once.
                if !self.import.dry_run {
                    self.modal = Some(Modal::ConfirmImport {
                        body: format!(
                            "This creates the database `{}` here and streams every row of `{}` on \
                             {} into it.\n\nOn a large source that is a long time and a lot of \
                             disk. It cannot be rolled back — to undo it, drop the database it \
                             created.\n\nRun it with Preview ticked first if you have not.",
                            self.import.target.trim(),
                            self.import.database.trim(),
                            self.import.host.trim(),
                        ),
                        statement: sql,
                    });
                    return Task::none();
                }
                self.start_import(sql)
            }
            Message::ImportFinished(elapsed, result) => {
                self.import_running = false;
                match result {
                    Ok(lines) => {
                        self.status = Status::Done(format!(
                            "{} finished in {}",
                            if self.import.dry_run { "preview" } else { "import" },
                            duration(elapsed)
                        ));
                        self.import_report = lines;
                        // A real import creates a database, so the sidebar is
                        // out of date the moment it finishes.
                        if self.import.dry_run {
                            Task::none()
                        } else {
                            self.load_databases()
                        }
                    }
                    Err(e) => {
                        self.import_report = vec![e.clone()];
                        self.status = Status::Failed(e);
                        Task::none()
                    }
                }
            }
            Message::CopyImportReport => {
                let report = self.import_report.join("\n");
                Task::batch([
                    iced::clipboard::write(report),
                    Task::done(Message::Copied("report copied".into())),
                ])
            }

            // ---- storage placement --------------------------------------------
            Message::ReloadStorage => self.load_storage(),
            Message::StorageLoaded(snapshot) => {
                self.storage = Some(*snapshot);
                // The destination a half-filled move form named may not be a
                // location any more. Left dangling it would send a table to a
                // directory the pool has dropped, which the server refuses —
                // clearer to drop the selection than to explain the refusal.
                if let (Some(destination), Some(snapshot)) =
                    (self.storage_move_to.clone(), self.storage.as_ref())
                {
                    if !snapshot.locations.iter().any(|l| l.path == destination) {
                        self.storage_move_to = None;
                    }
                }
                Task::none()
            }
            Message::StoragePath(v) => { self.storage_path = v; Task::none() }
            Message::StorageRole(v) => { self.storage_role = v; Task::none() }
            Message::StorageWeight(v) => { self.storage_weight = v; Task::none() }
            Message::StorageMinFree(v) => { self.storage_min_free = v; Task::none() }
            Message::AddStorageLocation => {
                match dialect::storage_add_location_sql(
                    &self.storage_path,
                    self.storage_role,
                    &self.storage_weight,
                ) {
                    Ok(sql) => {
                        self.storage_path.clear();
                        self.storage_weight = "1".to_string();
                        self.statement(sql, Refresh::Storage)
                    }
                    Err(e) => { self.status = Status::Failed(e); Task::none() }
                }
            }
            Message::DropStorageLocation(path) => {
                self.modal = Some(Modal::Confirm {
                    title: format!("Remove {}", path),
                    body: format!(
                        "{path} stops being somewhere this database writes. The server refuses \
                         this while the location still holds tables, so nothing can be orphaned \
                         by it — but anything else in that directory, a backup copy included, is \
                         left behind and nothing will read it again."
                    ),
                    statement: dialect::storage_drop_location_sql(&path),
                    refresh: Refresh::Storage,
                });
                Task::none()
            }
            Message::EnableStorageLocation(path, enable) => {
                if enable {
                    return self.statement(
                        dialect::storage_enable_location_sql(&path, true),
                        Refresh::Storage,
                    );
                }
                // Disabling is a drain, not a freeze, and the difference is
                // worth saying: the tables already there stay there and stay
                // writable until something moves them.
                self.modal = Some(Modal::Confirm {
                    title: format!("Stop using {}", path),
                    body: format!(
                        "New tables stop being placed in {path}. The tables already there stay \
                         where they are and keep working — run a rebalance to move them out, \
                         which is what has to happen before the location can be removed."
                    ),
                    statement: dialect::storage_enable_location_sql(&path, false),
                    refresh: Refresh::Storage,
                });
                Task::none()
            }
            Message::SetStorageRole(path, role) => {
                self.statement(dialect::storage_set_role_sql(&path, role), Refresh::Storage)
            }
            Message::SetStorageWeight(path, weight) => {
                self.statement(dialect::storage_set_weight_sql(&path, weight), Refresh::Storage)
            }
            Message::SetStoragePolicy(policy) => {
                self.statement(dialect::storage_set_policy_sql(policy), Refresh::Storage)
            }
            Message::SetStorageMinFree => {
                match dialect::storage_set_min_free_sql(&self.storage_min_free) {
                    Ok(sql) => {
                        self.storage_min_free.clear();
                        self.statement(sql, Refresh::Storage)
                    }
                    Err(e) => { self.status = Status::Failed(e); Task::none() }
                }
            }
            Message::StorageMoveTable(table) => {
                self.storage_move_table = table;
                self.storage_move_to = None;
                Task::none()
            }
            Message::StorageMoveTo(path) => { self.storage_move_to = Some(path); Task::none() }
            Message::RunStorageMove => {
                let (Some(table), Some(destination)) =
                    (self.storage_move_table.clone(), self.storage_move_to.clone())
                else {
                    self.status = Status::Failed("pick a table and a directory".to_string());
                    return Task::none();
                };
                self.storage_move_table = None;
                self.storage_move_to = None;
                self.modal = Some(Modal::Confirm {
                    title: format!("Move {}", table),
                    body: format!(
                        "Every file {table} owns is copied to {destination} and the originals are \
                         then released. It is one copy of the whole table, which on a large one \
                         takes a while and, until it finishes, the table is briefly not there for \
                         statements arriving in the meantime. Nothing is deleted until the copy \
                         has been verified."
                    ),
                    statement: dialect::storage_move_table_sql(&table, &destination),
                    refresh: Refresh::Storage,
                });
                Task::none()
            }
            Message::Rebalance(dry_run) => {
                let sql = dialect::storage_rebalance_sql(dry_run).to_string();
                if dry_run {
                    return self.storage_statement(sql);
                }
                self.modal = Some(Modal::Confirm {
                    title: "Rebalance".to_string(),
                    body: "Every table the policy would move is copied to its new directory, one \
                           at a time. Run the dry run first: it lists exactly what this will do \
                           and how much it will copy, and this page shows that list. A rebalance \
                           can be a lot of I/O."
                        .to_string(),
                    statement: dialect::storage_rebalance_sql(false).to_string(),
                    refresh: Refresh::Storage,
                });
                Task::none()
            }
            Message::StorageReport(result) => {
                self.storage_busy = false;
                match result {
                    Ok(lines) => {
                        self.status = Status::Done("rebalance plan ready".to_string());
                        self.storage_report = lines;
                    }
                    Err(e) => {
                        self.storage_report = vec![e.clone()];
                        self.status = Status::Failed(e);
                    }
                }
                self.load_storage()
            }
            Message::DismissStorageReport => { self.storage_report.clear(); Task::none() }

            // ---- activity -----------------------------------------------------
            Message::Bench(message) => self.update_bench(message),
            Message::ReloadActivity => self.load_activity(),
            Message::ActivityLoaded(snapshot) => {
                self.activity = Some(*snapshot);
                Task::none()
            }
            Message::ResetStatementStats => {
                // Confirmed rather than done, and not because it is dangerous:
                // it is not. It is *shared* — every other session loses the
                // history too, and the slow statement someone else is halfway
                // through diagnosing goes with it.
                self.modal = Some(Modal::Confirm {
                    title: "Forget every statement".to_string(),
                    body: "The server drops what it has recorded: the recent list and the \
                           per-shape totals, for every session and not just this console. \
                           Nothing that has run is affected — this is the record of it, not the \
                           thing itself — but anybody else looking at a slow statement right now \
                           loses it mid-sentence."
                        .to_string(),
                    statement: dialect::reset_statement_stats_sql(self.flavor()).to_string(),
                    refresh: Refresh::Activity,
                });
                Task::none()
            }

            // ---- replication --------------------------------------------------
            Message::ReloadBranches => self.load_branches(),
            Message::BranchesLoaded(snapshot) => {
                self.branches = Some(*snapshot);
                Task::none()
            }
            Message::BranchField(field, value) => {
                let form = &mut self.branch_form;
                match field {
                    views::branches::BranchField::Name => form.name = value,
                    views::branches::BranchField::Source => form.source = value,
                    views::branches::BranchField::Value => form.value = value,
                }
                Task::none()
            }
            Message::BranchPoint(kind) => {
                if self.branch_form.point != kind {
                    self.branch_form.value.clear();
                }
                self.branch_form.point = kind;
                Task::none()
            }
            Message::BranchKeepBase(keep) => {
                self.branch_form.keep_base = keep;
                Task::none()
            }
            Message::CreateBranch => match dialect::create_branch_sql(&self.branch_form) {
                Ok(sql) => {
                    self.branch_form = BranchForm::default();
                    self.statement(sql, Refresh::Branches)
                }
                Err(e) => {
                    self.status = Status::Failed(e);
                    Task::none()
                }
            },
            Message::DiffBranch(name) => {
                self.branch_diff = Some((name.clone(), None));
                let Some(session) = self.session.clone() else { return Task::none() };
                let sql = dialect::branch_diff_sql(&name);
                Task::perform(
                    async move { session.query(&sql).await.map(|g| dialect::parse_branch_diff(&g)) },
                    move |result| Message::BranchDiffLoaded(name.clone(), result),
                )
            }
            Message::BranchDiffLoaded(name, result) => {
                // Only for the branch still shown: a slow answer for another
                // must not land under this one.
                if let Some((shown, diff)) = &mut self.branch_diff
                    && *shown == name
                {
                    *diff = Some(result);
                }
                Task::none()
            }
            Message::MergePolicyChosen(policy) => {
                self.merge_policy = policy;
                Task::none()
            }
            Message::MergeAllowDrop(allow) => {
                self.merge_allow_drop = allow;
                Task::none()
            }
            Message::MergeBranch(name) => {
                let parent = self
                    .branches
                    .as_ref()
                    .and_then(|s| s.branches.iter().find(|b| b.name == name))
                    .map(|b| b.parent.clone())
                    .unwrap_or_else(|| "its parent".to_string());
                self.modal = Some(Modal::Confirm {
                    title: format!("Merge {name} into {parent}"),
                    body: format!(
                        "Everything {name} changed since its base is written to {parent} in one \
                         transaction: its schema changes first, then its rows. With FAIL, a row \
                         both sides changed stops the whole merge and nothing is written. Run DIFF \
                         first to see what this will do."
                    ),
                    statement: dialect::merge_branch_sql(&name, self.merge_policy, self.merge_allow_drop),
                    refresh: Refresh::Branches,
                });
                Task::none()
            }
            Message::ResetBranch(name) => {
                self.modal = Some(Modal::Confirm {
                    title: format!("Reset {name}"),
                    body: format!(
                        "{name} is copied from its parent again, as the parent is now. Everything \
                         written on {name} since it was made is discarded. Nobody may be connected \
                         to it."
                    ),
                    statement: dialect::reset_branch_sql(&name),
                    refresh: Refresh::Branches,
                });
                Task::none()
            }
            Message::PromoteBranch(name) => {
                self.modal = Some(Modal::Confirm {
                    title: format!("Promote {name}"),
                    body: format!(
                        "{name} takes its parent's name, and the parent as it is now is kept as \
                         branch {name}. Clients connecting by the parent's name get the branch's \
                         data from then on. Nobody may be connected to either."
                    ),
                    statement: dialect::promote_branch_sql(&name),
                    refresh: Refresh::Branches,
                });
                Task::none()
            }
            Message::DropBranch(name) => {
                self.modal = Some(Modal::Confirm {
                    title: format!("Drop {name}"),
                    body: format!(
                        "Branch {name} and its merge base are deleted, with everything written on \
                         it. Its parent is not touched."
                    ),
                    statement: dialect::drop_branch_sql(&name),
                    refresh: Refresh::Branches,
                });
                Task::none()
            }
            Message::ReloadBroker => self.load_broker(),
            Message::BrokerLoaded(snapshot) => {
                self.broker = Some(*snapshot);
                Task::none()
            }
            Message::BrokerKind(kind) => {
                self.broker_form.kind = kind;
                Task::none()
            }
            Message::BrokerField(field, value) => {
                use views::broker::BrokerField as F;
                let (f, p, r) = (&mut self.broker_form, &mut self.broker_publish, &mut self.broker_read);
                match field {
                    F::Name => f.name = value,
                    F::Partitions => f.partitions = value,
                    F::Durability => f.durability = value,
                    F::Retention => f.retention = value,
                    F::RetentionBytes => f.retention_bytes = value,
                    F::MaxDeliveries => f.max_deliveries = value,
                    F::DeadLetter => f.dead_letter = value,
                    F::AckTimeout => f.ack_timeout = value,
                    F::ExchangeType => f.exchange_type = value,
                    F::Target => f.target = value,
                    F::Mode => f.mode = value,
                    F::From => f.from = value,
                    F::Key => f.key = value,
                    F::PublishTarget => p.target = value,
                    F::PublishRoutingKey => p.routing_key = value,
                    F::PublishValue => p.value = value,
                    F::PublishKey => p.key = value,
                    F::PublishHeaderName => p.header_name = value,
                    F::PublishHeaderValue => p.header_value = value,
                    F::ReadPartition => r.partition = value,
                    F::ReadFrom => r.from = value,
                    F::ReadLimit => r.limit = value,
                    F::PermObject => self.broker_perm.object = value,
                    F::PermGrantee => self.broker_perm.grantee = value,
                    F::PermOwner => self.broker_perm.owner = value,
                }
                Task::none()
            }
            Message::BrokerViaExchange(via) => {
                self.broker_publish.via_exchange = via;
                Task::none()
            }
            Message::BrokerCreate => match dialect::broker_create_sql(&self.broker_form) {
                Ok(sql) => {
                    // The kind stays: making several of one kind is the usual case.
                    self.broker_form = BrokerForm { kind: self.broker_form.kind, ..BrokerForm::default() };
                    self.statement(sql, Refresh::Broker)
                }
                Err(e) => {
                    self.status = Status::Failed(e);
                    Task::none()
                }
            },
            Message::BrokerPublish => match dialect::broker_publish_sql(&self.broker_publish) {
                // The form is kept: publishing a few in a row is the usual case.
                Ok(sql) => self.statement(sql, Refresh::Broker),
                Err(e) => {
                    self.status = Status::Failed(e);
                    Task::none()
                }
            },
            Message::BrokerRead(topic) => match dialect::broker_read_sql(&topic, &self.broker_read) {
                Ok(sql) => self.broker_browse(format!("{} (READ, nothing taken)", topic.to_uppercase()), sql),
                Err(e) => {
                    self.status = Status::Failed(e);
                    Task::none()
                }
            },
            Message::BrokerPop(queue) => match dialect::broker_pop_sql(&queue, &self.broker_read.limit) {
                Ok(sql) => self.broker_browse(format!("{} (POPPED: taken and acknowledged)", queue.to_uppercase()), sql),
                Err(e) => {
                    self.status = Status::Failed(e);
                    Task::none()
                }
            },
            Message::BrokerPartitions(topic) => {
                let sql = dialect::broker_partitions_sql(&topic);
                self.broker_browse(format!("{} PARTITIONS", topic.to_uppercase()), sql)
            }
            Message::BrokerBrowseLoaded(sql, result) => {
                let popped = sql.starts_with("BROKER POP");
                // Only for the statement still shown: a slow answer for an
                // earlier one must not replace it.
                if let Some(browse) = &mut self.broker_browse
                    && browse.statement == sql
                {
                    browse.result = Some(result);
                }
                // A POP changed the queue's depth.
                if popped { self.load_broker() } else { Task::none() }
            }
            Message::BrokerCloseBrowse => {
                self.broker_browse = None;
                Task::none()
            }
            Message::BrokerDrop(what, name) => {
                let body = match what {
                    "TOPIC" => format!("Topic {name} is deleted with every message in it, and its subscriptions with it."),
                    "QUEUE" => format!("Queue {name} is deleted with every message in it, delivered or not."),
                    _ => format!("Exchange {name} is deleted with its bindings. The topics and queues it routed to stay."),
                };
                self.modal = Some(Modal::Confirm {
                    title: format!("Drop {} {name}", what.to_lowercase()),
                    body,
                    statement: dialect::broker_drop_sql(what, &name),
                    refresh: Refresh::Broker,
                });
                Task::none()
            }
            Message::BrokerDropSubscription(topic, name) => {
                self.modal = Some(Modal::Confirm {
                    title: format!("Drop subscription {name}"),
                    body: format!(
                        "Subscription {name} on {topic} is deleted with its position and its unacknowledged \
                         deliveries. The topic's messages stay."
                    ),
                    statement: dialect::broker_drop_subscription_sql(&topic, &name),
                    refresh: Refresh::Broker,
                });
                Task::none()
            }
            Message::BrokerUnbind(binding) => self.statement(dialect::broker_unbind_sql(&binding), Refresh::Broker),
            Message::BrokerPermKind(kind) => {
                if self.broker_perm.kind != kind {
                    // The privileges are the kind's own.
                    self.broker_perm.first = false;
                    self.broker_perm.second = false;
                }
                self.broker_perm.kind = kind;
                Task::none()
            }
            Message::BrokerPermToggle(first, on) => {
                if first {
                    self.broker_perm.first = on;
                } else {
                    self.broker_perm.second = on;
                }
                Task::none()
            }
            Message::BrokerGrant(revoke) => match dialect::broker_grant_sql(&self.broker_perm, revoke) {
                Ok(sql) => self.statement(sql, Refresh::Broker),
                Err(e) => {
                    self.status = Status::Failed(e);
                    Task::none()
                }
            },
            Message::BrokerOwner => match dialect::broker_owner_sql(&self.broker_perm) {
                Ok(sql) => {
                    let (kind, object, owner) =
                        (self.broker_perm.kind, self.broker_perm.object.trim().to_string(), self.broker_perm.owner.trim().to_string());
                    self.modal = Some(Modal::Confirm {
                        title: format!("Give {} {object} to {owner}", kind.label().to_lowercase()),
                        body: format!(
                            "{owner} becomes the owner of {object}: only it (or a superuser) may then alter or drop \
                             it, or grant on it."
                        ),
                        statement: sql,
                        refresh: Refresh::Broker,
                    });
                    Task::none()
                }
                Err(e) => {
                    self.status = Status::Failed(e);
                    Task::none()
                }
            },
            Message::BrokerRevokeRow(grant) => self.statement(dialect::broker_revoke_row_sql(&grant), Refresh::Broker),

            Message::ReloadShards => self.load_shards(),
            #[cfg(feature = "enterprise")]
            Message::ReloadLedger => self.load_ledger(),
            #[cfg(feature = "enterprise")]
            Message::LedgerLoaded(page) => {
                let mut page = *page;
                match self.ledger.take() {
                    Some(old) => page.carry_over(old),
                    None => page.prefill(),
                }
                self.ledger = Some(page);
                Task::none()
            }
            #[cfg(feature = "enterprise")]
            Message::LedgerRun(sql) => {
                let Some(session) = self.session.clone() else { return Task::none() };
                Task::perform(
                    async move {
                        let result = session.query(&sql).await.map(|grid| first_column(&grid));
                        Box::new((sql, result))
                    },
                    Message::LedgerRan,
                )
            }
            #[cfg(feature = "enterprise")]
            Message::LedgerRan(answer) => {
                let (sql, result) = *answer;
                if let Some(page) = self.ledger.as_mut() {
                    page.ran(&sql, result);
                }
                // A reveal keeps the page as it is; anything else may have
                // changed what it shows.
                let reveal = sql.split_whitespace().nth(2).is_some_and(|w| w.eq_ignore_ascii_case("GET"));
                if reveal { Task::none() } else { self.load_ledger() }
            }
            #[cfg(feature = "enterprise")]
            Message::LedgerInput(key, value) => {
                if let Some(page) = self.ledger.as_mut() {
                    page.input(key, value);
                }
                Task::none()
            }
            #[cfg(feature = "enterprise")]
            Message::LedgerOpenTable(name) => {
                let select = self.update(Message::SelectTable(name));
                Task::batch([select, self.update(Message::SelectTab(Tab::Data))])
            }
            #[cfg(feature = "enterprise")]
            Message::VerifyLedger => {
                let Some(session) = self.session.clone() else { return Task::none() };
                Task::perform(
                    async move {
                        match session.query(rdb_studio_enterprise::VERIFY_SQL).await {
                            Ok(grid) => first_column(&grid),
                            Err(e) => vec![format!("UNKNOWN   {e}")],
                        }
                    },
                    Message::LedgerVerified,
                )
            }
            #[cfg(feature = "enterprise")]
            Message::LedgerVerified(lines) => {
                if let Some(page) = self.ledger.as_mut() {
                    page.verify = Some(lines);
                }
                Task::none()
            }
            #[cfg(feature = "enterprise")]
            Message::LedgerAudit(vault) => {
                let Some(session) = self.session.clone() else { return Task::none() };
                Task::perform(
                    async move {
                        let lines = match session.query(&rdb_studio_enterprise::audit_sql(&vault)).await {
                            Ok(grid) => first_column(&grid),
                            Err(e) => vec![e],
                        };
                        (vault, lines)
                    },
                    |(vault, lines)| Message::LedgerAuditLoaded(vault, lines),
                )
            }
            #[cfg(feature = "enterprise")]
            Message::LedgerAuditLoaded(vault, lines) => {
                if let Some(v) = self.ledger.as_mut().and_then(|p| p.vaults.iter_mut().find(|v| v.name == vault)) {
                    v.audit = Some(lines);
                }
                Task::none()
            }
            Message::ShardsLoaded(snapshot) => {
                self.shards = Some(*snapshot);
                Task::none()
            }
            Message::PlanRebalance => {
                let Some(session) = self.session.clone() else { return Task::none() };
                self.rebalance_plan = None;
                Task::perform(
                    async move { session.query(dialect::SHOW_REBALANCE_PLAN_SQL).await.map(|g| dialect::parse_rebalance_plan(&g)) },
                    Message::RebalancePlanned,
                )
            }
            Message::RebalancePlanned(plan) => {
                self.rebalance_plan = Some(plan);
                Task::none()
            }
            Message::StartRebalance => {
                let moves = match &self.rebalance_plan {
                    Some(Ok(steps)) => steps.iter().filter(|s| s.kind == "move").count(),
                    _ => 0,
                };
                self.modal = Some(Modal::Confirm {
                    title: "Rebalance the cluster".to_string(),
                    body: format!(
                        "The gateway runs rdb-shard rebalance --apply --online: each planned range \
                         ({moves} in the last plan) is copied to its new shard at one snapshot, \
                         verified, and handed off one map epoch up, while writes go on. Each move \
                         copies its rows across the network and the disks; the source rows stay until \
                         `rdb-shard retire`. Progress shows on this page."
                    ),
                    statement: dialect::REBALANCE_SQL.to_string(),
                    refresh: Refresh::Shards,
                });
                Task::none()
            }
            Message::ReloadReplication => self.load_replication(),
            Message::ReplicationLoaded(snapshot) => {
                self.replication = Some(*snapshot);
                Task::none()
            }
            Message::SetReplicationActive(active) => {
                let sql = dialect::replication_enable_sql(active).to_string();
                if active {
                    return self.statement(sql, Refresh::Replication);
                }
                // Turning it off stops shipping. Nothing is lost that was
                // already sent, but everything committed while it is off is
                // only here — which is the sort of thing to be told before
                // rather than after.
                self.modal = Some(Modal::Confirm {
                    title: "Disable replication".to_string(),
                    body: "Committed transactions stop being shipped to the peers. Whatever is \
                           written while it is off exists only on this node, and the peers will \
                           have to catch up — or resynchronise, if the history needed has been \
                           retired by then."
                        .to_string(),
                    statement: sql,
                    refresh: Refresh::Replication,
                });
                Task::none()
            }
            Message::PeerName(v) => { self.peer_name = v; Task::none() }
            Message::PeerAddress(v) => { self.peer_address = v; Task::none() }
            Message::AddPeer => {
                match dialect::peer_add_sql(&self.peer_name, &self.peer_address) {
                    Ok(sql) => {
                        self.peer_name.clear();
                        self.peer_address.clear();
                        self.statement(sql, Refresh::Replication)
                    }
                    Err(e) => { self.status = Status::Failed(e); Task::none() }
                }
            }
            Message::PausePeer(peer, pause) => {
                self.statement(dialect::peer_pause_sql(&peer, pause), Refresh::Replication)
            }
            Message::ResyncPeer(peer) => {
                self.modal = Some(Modal::Confirm {
                    title: format!("Resynchronise {}", peer),
                    body: format!(
                        "This re-sends {peer} everything still in the local history, from the \
                         beginning. It is the right thing to do for a peer that has fallen past \
                         the retention and cannot catch up — and it is a lot of traffic, which on \
                         a large database is a long time and, across regions, real money."
                    ),
                    statement: dialect::peer_resync_sql(&peer),
                    refresh: Refresh::Replication,
                });
                Task::none()
            }
            Message::DropPeer(peer) => {
                self.modal = Some(Modal::Confirm {
                    title: format!("Drop {}", peer),
                    body: format!(
                        "{peer} stops receiving anything committed here, immediately and without \
                         it being told. Its copy keeps whatever it already had, which then \
                         diverges from this one. Adding it back later means resynchronising."
                    ),
                    statement: dialect::peer_drop_sql(&peer),
                    refresh: Refresh::Replication,
                });
                Task::none()
            }

            // ---- live dashboard ---------------------------------------------
            Message::Tick => self.sample(),
            Message::Sampled(Ok(stats)) => {
                let elapsed = self
                    .sampled_at
                    .map(|at| at.elapsed().as_secs_f64())
                    .unwrap_or(self.interval as f64)
                    .max(0.001);
                self.sampled_at = Some(Instant::now());
                self.metrics.sample(&stats, elapsed);
                // The same reading refreshes the overview, so watching the
                // dashboard keeps the rest of the console current for free.
                self.db_stats = stats;
                self.metrics_error = None;
                Task::none()
            }
            Message::Sampled(Err(e)) => {
                // Recorded, not raised: sampling is a poll, and one failed poll
                // must not leave a red status line behind after it recovers.
                self.metrics_error = Some(e);
                Task::none()
            }
            Message::ToggleLive => {
                self.live = !self.live;
                // Resuming samples at once rather than waiting out a tick,
                // which at the 30-second setting is a long time to look at a
                // page that says it is live.
                if self.live { self.sample() } else { Task::none() }
            }
            Message::SetInterval(seconds) => {
                self.interval = seconds.clamp(1, 3600);
                Task::none()
            }
            Message::ClearMetrics => {
                self.metrics.clear();
                self.sampled_at = None;
                self.metrics_error = None;
                Task::none()
            }

            // ---- schema canvas ---------------------------------------------
            Message::SchemaPan(delta) => {
                self.schema_pan = self.schema_pan + delta;
                self.schema_cache.clear();
                Task::none()
            }
            Message::SchemaZoom(factor) => {
                self.schema_zoom = (self.schema_zoom * factor).clamp(0.25, 3.0);
                self.schema_cache.clear();
                Task::none()
            }
            Message::SchemaRelayout => {
                self.schema_pan = iced::Vector::ZERO;
                self.schema_zoom = 1.0;
                self.schema_cache.clear();
                self.load_schema()
            }

            // ---- data browser ----------------------------------------------
            Message::PageNext => {
                self.data_page += 1;
                self.data_viewport = None;
                self.load_page()
            }
            Message::PagePrev => {
                self.data_page = self.data_page.saturating_sub(1);
                self.data_viewport = None;
                self.load_page()
            }
            Message::PageSize(v) => {
                if let Ok(size) = v.parse::<usize>() {
                    self.page_size = size.clamp(1, 5_000);
                    self.data_page = 0;
                }
                Task::none()
            }
            Message::DataScrolled(viewport) => {
                self.data_viewport = Some((
                    viewport.absolute_offset().y,
                    viewport.bounds().height,
                ));
                Task::none()
            }
            Message::SortColumn(scope, column) => {
                // Ascending, then descending, then back to the order the
                // server returned. The third state matters: a grid the console
                // has re-sorted shows a different order from the one the
                // statement produced, and there has to be a way back to it
                // without reconnecting.
                match self.sort.get(&scope) {
                    Some(sort) if sort.column == column && !sort.descending => {
                        self.sort.insert(scope.clone(), GridSort { column, descending: true });
                    }
                    Some(sort) if sort.column == column => {
                        self.sort.remove(&scope);
                    }
                    _ => {
                        self.sort.insert(scope.clone(), GridSort { column, descending: false });
                    }
                }

                // The data browser holds one page of a table, so sorting it
                // here would sort the page rather than the table — the first
                // hundred rows the server happened to return, rearranged, over
                // a heading that claims to order the whole thing. It goes back
                // to the server instead, from page one, because the sort
                // changes which rows the first page even contains.
                if scope.starts_with(views::DATA_SCOPE) {
                    self.data_page = 0;
                    self.editing = None;
                    return self.load_page();
                }
                Task::none()
            }
            Message::BeginColumnResize(key, width) => {
                self.resizing = Some(Resize { key, from: None, start_width: width });
                Task::none()
            }
            Message::ColumnResizeMove(x) => {
                let Some(resize) = &mut self.resizing else { return Task::none() };
                // The press gives no position, so the first movement after it
                // is the origin. Treating the press as the origin would make
                // the column jump by however far the cursor had already
                // travelled inside the grid.
                let from = *resize.from.get_or_insert(x);
                // A floor rather than a clamp at both ends: dragging a column
                // to nothing makes it unfindable, and there is no reason to
                // stop someone making one very wide.
                let width = (resize.start_width + (x - from)).max(48.0);
                self.column_widths.insert(resize.key.clone(), width);
                Task::none()
            }
            Message::EndColumnResize => { self.resizing = None; Task::none() }
            Message::ResetColumnWidth(key) => {
                // Back to the content-derived default, which is what a
                // double-click means everywhere else.
                self.column_widths.remove(&key);
                self.resizing = None;
                Task::none()
            }
            Message::BeginEdit { row, column } => {
                let key = self.where_clause_for(row);
                let value = self
                    .data
                    .rows
                    .get(row)
                    .and_then(|r| r.get(column))
                    .cloned()
                    .flatten();
                self.editing = Some(CellEdit {
                    row,
                    column,
                    null: value.is_none(),
                    value: value.unwrap_or_default(),
                    key: key.unwrap_or_default(),
                });
                Task::none()
            }
            Message::EditValue(v) => {
                if let Some(edit) = &mut self.editing {
                    edit.value = v;
                    // Typing into a cell means the user wants that value, not a
                    // NULL, so the flag clears itself rather than needing a
                    // second click.
                    edit.null = false;
                }
                Task::none()
            }
            Message::EditNull(v) => {
                if let Some(edit) = &mut self.editing {
                    edit.null = v;
                }
                Task::none()
            }
            Message::CancelEdit => { self.editing = None; Task::none() }
            Message::CommitEdit => self.commit_edit(),

            Message::BeginInsert => {
                self.inserting = Some(vec![String::new(); self.data.columns.len().max(1)]);
                Task::none()
            }
            Message::CancelInsert => { self.inserting = None; Task::none() }
            Message::InsertValue(index, v) => {
                if let Some(row) = &mut self.inserting
                    && let Some(cell) = row.get_mut(index)
                {
                    *cell = v;
                }
                Task::none()
            }
            Message::CommitInsert => self.commit_insert(),
            Message::DeleteSelectedRow(row) => self.delete_row(row),

            // ---- query editor ----------------------------------------------
            Message::EditorAction(action) => {
                // The list was for the word as it stood; typing on changes it.
                if action.is_edit() {
                    self.completions.clear();
                }
                self.editor.perform(action);
                Task::none()
            }
            Message::Complete => {
                let prefix = crate::sqledit::word_before_cursor(&self.editor);
                let candidates = crate::sqledit::completions(
                    &prefix,
                    &self.completion_names(),
                    crate::sqllex::KEYWORDS,
                );
                self.completions.clear();
                match candidates.as_slice() {
                    [] => {
                        self.status = Status::Failed(if prefix.is_empty() {
                            "type the start of a name, then Ctrl+Space".to_string()
                        } else {
                            format!("nothing known starts with `{}`", prefix)
                        });
                    }
                    [only] => crate::sqledit::complete(&mut self.editor, only),
                    _ => {
                        if let Some(common) = crate::sqledit::common_completion(&prefix, &candidates) {
                            crate::sqledit::complete(&mut self.editor, &common);
                        }
                        self.completions = candidates;
                    }
                }
                iced::widget::operation::focus(crate::views::query::editor_id())
            }
            Message::ApplyCompletion(choice) => {
                crate::sqledit::complete(&mut self.editor, &choice);
                self.completions.clear();
                iced::widget::operation::focus(crate::views::query::editor_id())
            }
            Message::EditorNewline => { crate::sqledit::newline(&mut self.editor); Task::none() }
            Message::ToggleComment => { crate::sqledit::toggle_comment(&mut self.editor); Task::none() }
            Message::RunStatement => {
                let cursor = self.editor.cursor().position;
                match crate::sqllex::statement_at(&self.editor.text(), cursor.line, cursor.column) {
                    Some(sql) => self.run_sql(sql),
                    None => {
                        self.status = Status::Failed("there is no statement under the cursor".into());
                        Task::none()
                    }
                }
            }
            Message::JumpTo(line, column) => {
                crate::sqledit::place_cursor(&mut self.editor, line, column);
                iced::widget::operation::focus(crate::views::query::editor_id())
            }
            Message::ToggleEditorSize => { self.editor_tall = !self.editor_tall; Task::none() }
            Message::RunQuery => {
                let sql = self.editor.text();
                self.run_sql(sql)
            }
            Message::RunSelection => {
                // Running only what is highlighted is how anyone works through
                // a scratch buffer of half a dozen statements.
                match self.editor.selection() {
                    Some(sql) if !sql.trim().is_empty() => self.run_sql(sql),
                    _ => {
                        self.status =
                            Status::Failed("nothing is selected — highlight a statement first".into());
                        Task::none()
                    }
                }
            }
            Message::FormatSql => {
                let formatted = crate::sqlfmt::format(&self.editor.text());
                self.editor = text_editor::Content::with_text(&formatted);
                Task::none()
            }
            Message::ClearEditor => {
                self.editor = text_editor::Content::new();
                self.results.clear();
                self.query_error = None;
                Task::none()
            }
            Message::InsertTemplate(template) => {
                let Some(table) = self.selected_table.clone() else {
                    self.status = Status::Failed("select a table first".into());
                    return Task::none();
                };
                let key = self.row_key().map(|k| k.column);
                let sql = template.render(&table, &self.columns, key.as_deref());
                self.editor = text_editor::Content::with_text(&sql);
                self.tab = Tab::Query;
                Task::none()
            }
            Message::CopyResult(index, format) => {
                let Some(Outcome::Rows(grid)) = self.results.get(index) else {
                    return Task::none();
                };
                let table = self.selected_table.clone().unwrap_or_default();
                // In the order it is on screen, not the order it arrived: a
                // result someone re-sorted and then copied has to come out the
                // way they were looking at it.
                let grid = views::in_sorted_order(self.sort.get(views::QUERY_SCOPE), grid);
                let rendered = crate::export::render(&grid, format, &table);
                let rows = grid.rows.len();
                Task::batch([
                    iced::clipboard::write(rendered),
                    Task::done(Message::Copied(format!(
                        "{} row{} copied as {}",
                        count(rows as u64),
                        if rows == 1 { "" } else { "s" },
                        format.label()
                    ))),
                ])
            }
            Message::ExportResult(index, format) => {
                let Some(Outcome::Rows(grid)) = self.results.get(index) else {
                    return Task::none();
                };
                let table = self.selected_table.clone().unwrap_or_default();
                let grid = views::in_sorted_order(self.sort.get(views::QUERY_SCOPE), grid);
                let contents = crate::export::render(&grid, format, &table);
                let base = if table.is_empty() {
                    format!("result-{}", index + 1)
                } else {
                    table
                };
                self.save_file(&base, format.extension(), format.label(), contents)
            }
            Message::ExportData(format) => {
                if self.data.rows.is_empty() {
                    self.status = Status::Failed("there is nothing on this page to export".into());
                    return Task::none();
                }
                let table = self.selected_table.clone().unwrap_or_default();
                let contents = crate::export::render(&self.data, format, &table);
                // Named for the page it is, not for the table: exporting page 3
                // of a table and getting `users.csv` invites someone to think
                // they have the table.
                let base = format!("{}-page-{}", table, self.data_page + 1);
                self.save_file(&base, format.extension(), format.label(), contents)
            }
            Message::ExportHistory => {
                if self.history.is_empty() {
                    self.status = Status::Failed("there is no history to export".into());
                    return Task::none();
                }
                let contents = self.history_as_sql();
                self.save_file("session-history", "sql", "SQL", contents)
            }

            Message::ImportCsv => {
                if self.selected_table.is_none() || self.columns.is_empty() {
                    self.status = Status::Failed("select a table first".into());
                    return Task::none();
                }
                Task::perform(crate::files::open("CSV", &["csv", "txt"]), Message::CsvOpened)
            }
            Message::CsvOpened(Ok(None)) => Task::none(),
            Message::CsvOpened(Err(e)) => { self.status = Status::Failed(e); Task::none() }
            Message::CsvOpened(Ok(Some((path, contents)))) => {
                let Some(table) = self.selected_table.clone() else { return Task::none() };
                let file = path.display().to_string();
                let read = crate::import::parse(&contents)
                    .and_then(|csv| {
                        crate::import::scripts(&table, &csv, &self.columns)
                            .map(|scripts| (csv, scripts))
                    });
                match read {
                    Err(e) => self.status = Status::Failed(format!("{}: {}", file, e)),
                    Ok((csv, scripts)) => {
                        self.modal = Some(Modal::ConfirmCsv {
                            table,
                            file,
                            rows: csv.rows.len(),
                            columns: csv.header,
                            scripts,
                        });
                        self.modal_pending = false;
                    }
                }
                Task::none()
            }
            Message::CsvImported(rows, failure, elapsed) => {
                self.status = match failure {
                    None => Status::Done(format!(
                        "{} row{} imported in {}",
                        count(rows),
                        if rows == 1 { "" } else { "s" },
                        duration(elapsed)
                    )),
                    Some(e) => Status::Failed(format!(
                        "the import stopped after {} row{}: {}",
                        count(rows),
                        if rows == 1 { "" } else { "s" },
                        e
                    )),
                };
                self.data_total = None;
                Task::batch([self.load_page(), self.load_table_stats()])
            }

            Message::OpenSqlFile => Task::perform(
                crate::files::open("SQL", &["sql", "txt"]),
                Message::SqlFileOpened,
            ),
            Message::SqlFileOpened(Ok(Some((path, contents)))) => {
                self.editor = text_editor::Content::with_text(&contents);
                self.tab = Tab::Query;
                self.status = Status::Done(format!("opened {}", path.display()));
                Task::none()
            }
            // Dismissing the dialog is a decision, not a failure.
            Message::SqlFileOpened(Ok(None)) => Task::none(),
            Message::SqlFileOpened(Err(e)) => { self.status = Status::Failed(e); Task::none() }

            Message::SaveSqlFile => {
                let contents = self.editor.text();
                if contents.trim().is_empty() {
                    self.status = Status::Failed("the editor is empty".into());
                    return Task::none();
                }
                self.save_file("query", "sql", "SQL", contents)
            }
            Message::FileSaved(Ok(Some(path))) => {
                self.status = Status::Done(format!("written to {}", path.display()));
                Task::none()
            }
            Message::FileSaved(Ok(None)) => Task::none(),
            Message::FileSaved(Err(e)) => { self.status = Status::Failed(e); Task::none() }

            Message::Copied(what) => { self.status = Status::Done(what); Task::none() }

            Message::LoadHistory(index) => {
                // Loads, and deliberately does not run. This list holds the
                // DROP and DELETE statements of the session as readily as the
                // SELECTs, and a click that re-ran one would be a loaded gun.
                let Some(entry) = self.history.get(index) else { return Task::none() };
                self.editor = text_editor::Content::with_text(&entry.sql);
                self.tab = Tab::Query;
                Task::none()
            }
            Message::RunHistory(index) => {
                let Some(sql) = self.history.get(index).map(|e| e.sql.clone()) else {
                    return Task::none();
                };
                self.editor = text_editor::Content::with_text(&sql);
                self.tab = Tab::Query;
                self.run_sql(sql)
            }
            Message::CopyHistory(index) => {
                let Some(entry) = self.history.get(index) else { return Task::none() };
                let sql = entry.sql.clone();
                Task::batch([
                    iced::clipboard::write(sql),
                    Task::done(Message::Copied("statement copied".into())),
                ])
            }
            Message::DeleteHistory(index) => {
                if index < self.history.len() {
                    self.history.remove(index);
                }
                Task::none()
            }
            Message::ClearHistory => { self.history.clear(); Task::none() }

            Message::QueryFinished(answer, elapsed) => {
                let sql = self.pending_sql.take().unwrap_or_default();
                self.query_cancel = None;
                self.cancel_sent = false;
                self.query_notices = answer.notices;
                if answer.transaction.is_some() {
                    self.editor_transaction = answer.transaction;
                }
                match answer.result {
                    Ok(outcomes) => {
                        let rows: usize = outcomes
                            .iter()
                            .map(|o| match o {
                                Outcome::Rows(grid) => grid.rows.len(),
                                Outcome::Affected { .. } => 0,
                            })
                            .sum();
                        let affected: u64 = outcomes
                            .iter()
                            .map(|o| match o {
                                Outcome::Affected { rows, .. } => *rows,
                                Outcome::Rows(_) => 0,
                            })
                            .sum();
                        let outcome = RunOutcome::Ok {
                            statements: outcomes.len(),
                            rows,
                            affected,
                        };
                        self.status = Status::Done(format!(
                            "{} in {}",
                            outcome.summary(),
                            duration(elapsed)
                        ));
                        self.record(sql, elapsed, outcome);
                        self.query_error = None;
                        self.results = outcomes;
                        self.last_elapsed = Some(elapsed);
                    }
                    Err(e) => {
                        self.record(sql, elapsed, RunOutcome::Failed(e.clone()));
                        self.results.clear();
                        self.query_error = Some(e.clone());
                        self.status = Status::Failed(e);
                        self.last_elapsed = Some(elapsed);
                    }
                }
                Task::none()
            }
            Message::CancelQuery => {
                let Some(canceller) = self.query_cancel.clone() else { return Task::none() };
                self.cancel_sent = true;
                self.status = Status::Busy("cancelling".to_string());
                Task::perform(canceller.cancel(), Message::CancelSent)
            }
            Message::CancelSent(result) => {
                // The run reports its own end (57014); this only says whether
                // the request got there, and only while the run is still out.
                if self.query_cancel.is_some() {
                    self.status = match result {
                        Ok(said) => Status::Busy(said),
                        Err(e) => Status::Failed(e),
                    };
                }
                Task::none()
            }
            Message::SetRowLimit(limit) => { self.row_limit = limit; Task::none() }
            Message::DataFilter(text) => { self.data_filter = text; Task::none() }
            Message::ApplyDataFilter => match dialect::RowFilter::parse(&self.data_filter) {
                Err(e) => { self.status = Status::Failed(e); Task::none() }
                Ok(filter) => {
                    self.data_filter_applied = filter;
                    self.data_page = 0;
                    self.data_total = None;
                    self.data_viewport = None;
                    self.editing = None;
                    self.load_page()
                }
            },
            Message::ClearDataFilter => {
                self.data_filter.clear();
                if self.data_filter_applied.take().is_none() {
                    return Task::none();
                }
                self.data_page = 0;
                self.data_total = None;
                self.data_viewport = None;
                self.editing = None;
                self.load_page()
            }
            Message::EditorKeepalive => {
                let Some(session) = self.session.clone() else { return Task::none() };
                Task::perform(
                    async move { session.keep_editor_alive().await },
                    Message::EditorKept,
                )
            }
            Message::EditorKept(result) => {
                if let Some(session) = &self.session {
                    self.editor_transaction = session.editor_transaction();
                }
                if let Err(e) = result {
                    self.status = Status::Failed(format!("the editor's transaction was lost: {}", e));
                }
                Task::none()
            }

            // ---- configuration ---------------------------------------------
            Message::ConfigFilter(v) => { self.config_filter = v; Task::none() }
            Message::ConfigEdit(key, value) => {
                self.config_edits.insert(key, value);
                Task::none()
            }
            Message::ApplyConfig(key) => {
                let Some(session) = self.session.clone() else { return Task::none() };
                let Some(entry) = self.config.iter().find(|e| e.qualified() == key).cloned() else {
                    return Task::none();
                };
                let Some(value) = self.config_edits.get(&key).cloned() else { return Task::none() };

                match dialect::set_config_sql(session.flavor, &entry, &value) {
                    Err(e) => { self.status = Status::Failed(e); Task::none() }
                    Ok(sql) => {
                        self.status = Status::Busy(format!("setting {}", key));
                        Task::perform(
                            async move {
                                let started = Instant::now();
                                let result = session.execute(&sql).await;
                                (sql, started.elapsed(), result)
                            },
                            |(sql, elapsed, result)| Message::ConfigApplied(sql, elapsed, result),
                        )
                    }
                }
            }
            Message::ConfigApplied(sql, elapsed, Ok(message)) => {
                self.record(sql, elapsed, RunOutcome::Ok { statements: 1, rows: 0, affected: 0 });
                self.status = Status::Done(format!("{} · {}", message, duration(elapsed)));
                self.load_config()
            }
            Message::ConfigApplied(sql, elapsed, Err(e)) => {
                self.record(sql, elapsed, RunOutcome::Failed(e.clone()));
                self.status = Status::Failed(e);
                Task::none()
            }

            // ---- modals ------------------------------------------------------
            Message::OpenModal(kind) => {
                self.modal = Some(match kind {
                    ModalKind::CreateRole => Modal::CreateRole {
                        name: String::new(),
                        password: String::new(),
                        login: true,
                        superuser: false,
                        error: None,
                    },
                    ModalKind::CreateTable => Modal::CreateTable {
                        name: String::new(),
                        columns: vec![NewColumn::key(self.flavor())],
                        partitioning: Partitioning { count: "64".to_string(), ..Partitioning::default() },
                        error: None,
                    },
                    ModalKind::CreateDatabase => {
                        Modal::CreateDatabase { name: String::new(), error: None }
                    }
                    ModalKind::AddColumn => Modal::AddColumn {
                        table: self.selected_table.clone().unwrap_or_default(),
                        column: NewColumn::new(self.flavor()),
                        error: None,
                    },
                    ModalKind::RowCompression => Modal::RowCompression {
                        table: self.selected_table.clone().unwrap_or_default(),
                        edit: RowCompressionEdit::of(
                            &stat_value(&self.table_stats, "row_compression").unwrap_or_default(),
                        ),
                        error: None,
                    },
                    ModalKind::CreateIndex => Modal::CreateIndex {
                        table: self.selected_table.clone().unwrap_or_default(),
                        index: NewIndex::new(
                            self.document_columns().first().cloned().unwrap_or_default(),
                        ),
                        error: None,
                    },
                });
                self.modal_pending = false;
                Task::none()
            }
            Message::ModalCancel => {
                self.modal = None;
                self.modal_pending = false;
                self.expect_gone = None;
                Task::none()
            }
            Message::DropIndex { title, body, statement, index } => {
                self.expect_gone = self.selected_table.clone().map(|table| (table, index));
                self.modal = Some(Modal::Confirm { title, body, statement, refresh: Refresh::CurrentTable });
                Task::none()
            }
            Message::IndexForm(edit) => {
                let documents = self.document_columns();
                if let Some(Modal::CreateIndex { index, .. }) = &mut self.modal {
                    match edit {
                        IndexEdit::Name(v) => index.name = v,
                        IndexEdit::Kind(kind) => {
                            index.kind = kind;
                            if index.document.is_empty() {
                                index.document = documents.first().cloned().unwrap_or_default();
                            }
                        }
                        IndexEdit::Method(method) => index.method = method,
                        IndexEdit::Column(column) => {
                            match index.columns.iter().position(|c| *c == column) {
                                Some(at) => { index.columns.remove(at); }
                                None => index.columns.push(column),
                            }
                        }
                        IndexEdit::Document(v) => index.document = v,
                        IndexEdit::PathOps(v) => index.path_ops = v,
                        IndexEdit::Path(v) => index.path = v,
                        IndexEdit::Unique(v) => index.unique = v,
                    }
                }
                Task::none()
            }
            Message::PartitionForm(edit) => {
                if let Some(Modal::CreateTable { columns, partitioning, .. }) = &mut self.modal {
                    match edit {
                        PartitionEdit::Enabled(v) => partitioning.enabled = v,
                        PartitionEdit::Count(v) => partitioning.count = digits(&v),
                        PartitionEdit::Key(at) => {
                            if let Some(found) = partitioning.key.iter().position(|k| *k == at) {
                                partitioning.key.remove(found);
                            } else if partitioning.key.len() < MAX_PARTITION_KEY
                                && let Some(column) = columns.get_mut(at)
                            {
                                // A key column must be NOT NULL: a NULL hashes
                                // like 0 or '' and `IS NULL` could never route.
                                // Ticking it here, visibly, beats a refusal.
                                column.nullable = false;
                                partitioning.key.push(at);
                            }
                        }
                    }
                }
                Task::none()
            }
            Message::ModalField(field, value) => { self.modal_field(field, value); Task::none() }
            Message::ModalToggle(field, value) => { self.modal_toggle(field, value); Task::none() }
            Message::ModalAddColumn => {
                let fresh = NewColumn::new(self.flavor());
                if let Some(Modal::CreateTable { columns, .. }) = &mut self.modal {
                    columns.push(fresh);
                }
                Task::none()
            }
            Message::ModalRemoveColumn(index) => {
                if let Some(Modal::CreateTable { columns, partitioning, .. }) = &mut self.modal
                    && index < columns.len()
                    && columns.len() > 1
                {
                    columns.remove(index);
                    partitioning.key.retain(|k| *k != index);
                    for k in partitioning.key.iter_mut() {
                        if *k > index {
                            *k -= 1;
                        }
                    }
                }
                Task::none()
            }
            Message::ModalColumnField(index, field, value) => {
                let flavor = self.flavor();
                self.modal_column(index, |column| match field {
                    ModalField::Name => column.rename(value.clone(), flavor),
                    ModalField::Length => column.length = digits(&value),
                    ModalField::Precision => column.precision = digits(&value),
                    ModalField::Scale => column.scale = digits(&value),
                    ModalField::Default => column.default = value.clone(),
                    _ => {}
                });
                Task::none()
            }
            Message::ModalType(choice) => {
                if let Some(Modal::AddColumn { column, .. }) = &mut self.modal {
                    column.choose(choice);
                }
                Task::none()
            }
            Message::ModalColumnType(index, choice) => {
                self.modal_column(index, |column| column.choose(choice));
                Task::none()
            }
            Message::ModalColumnToggle(index, field, value) => {
                self.modal_column(index, |column| match field {
                    ModalField::Nullable => column.nullable = value,
                    ModalField::PrimaryKey => {
                        column.primary_key = value;
                        // A key column is NOT NULL however it is written;
                        // the box says so rather than contradicting the DDL.
                        if value {
                            column.nullable = false;
                        }
                    }
                    ModalField::Unique => column.unique = value,
                    _ => {}
                });
                Task::none()
            }
            Message::ModalSubmit => self.submit_modal(),
            Message::RolePassword(role) => {
                self.modal = Some(Modal::RolePassword { role, password: String::new(), error: None });
                self.modal_pending = false;
                Task::none()
            }
            Message::EditColumn(name) => {
                let (Some(table), Some(info)) = (
                    self.selected_table.clone(),
                    self.columns.iter().find(|c| c.name == name),
                ) else {
                    return Task::none();
                };
                let compression = column_compressions(&self.table_stats)
                    .into_iter()
                    .find(|(column, _)| column == &name)
                    .map(|(_, method)| method)
                    .unwrap_or_default();
                self.modal = Some(Modal::EditColumn {
                    table,
                    edit: ColumnEdit::of(info, &compression),
                    error: None,
                });
                self.modal_pending = false;
                Task::none()
            }

            Message::Confirm { title, body, statement, refresh } => {
                self.modal = Some(Modal::Confirm { title, body, statement, refresh });
                Task::none()
            }
            Message::StatementFinished(refresh, sql, elapsed, Ok(message)) => {
                self.record(
                    sql,
                    elapsed,
                    RunOutcome::Ok { statements: 1, rows: 0, affected: 0 },
                );
                self.status = Status::Done(format!("{} · {}", message, duration(elapsed)));
                if refresh == Refresh::CurrentTable {
                    self.verify_gone = self.expect_gone.take();
                }
                match refresh {
                    Refresh::Tables => Task::batch([self.load_tables(), self.load_schema()]),
                    Refresh::Databases => self.load_databases(),
                    Refresh::Roles => self.load_roles(),
                    Refresh::Keys => {
                        self.kv.selected = None;
                        self.kv_scan(false)
                    }
                    Refresh::CurrentTable => Task::batch([
                        self.load_page(),
                        self.load_table_stats(),
                        self.load_columns(),
                    ]),
                    Refresh::Backups => self.load_backups(),
                    Refresh::Replication => self.load_replication(),
                    Refresh::Shards => self.load_shards(),
                    Refresh::Storage => self.load_storage(),
                    Refresh::Activity => self.load_activity(),
                    Refresh::Broker => self.load_broker(),
                    #[cfg(feature = "enterprise")]
                    Refresh::Ledger => self.load_ledger(),
                    Refresh::Branches => {
                        // A merge changed what a diff would say.
                        self.branch_diff = None;
                        Task::batch([self.load_branches(), self.load_databases()])
                    }
                }
            }
            Message::StatementFinished(_, sql, elapsed, Err(e)) => {
                self.expect_gone = None;
                self.record(sql, elapsed, RunOutcome::Failed(e.clone()));
                self.status = Status::Failed(e);
                Task::none()
            }
            Message::ModalStatementFinished(refresh, sql, elapsed, result) => {
                // Cancelled meanwhile: the answer still counts, but the dialog
                // it was for is gone and whatever is open now is another one.
                let waiting = std::mem::take(&mut self.modal_pending);
                match &result {
                    Ok(_) if waiting => self.modal = None,
                    Err(e) if waiting => self.set_modal_error(e.clone()),
                    _ => {}
                }
                self.update(Message::StatementFinished(refresh, sql, elapsed, result))
            }

            // ---- the table pane --------------------------------------------
            Message::SelectDetailSection(section) => {
                self.detail_section = section;
                Task::none()
            }
            Message::ShowEmptyPartitions(show) => {
                self.show_empty_partitions = show;
                Task::none()
            }
            Message::CheckIndexes => {
                let (Some(session), Some(table)) = (self.session.clone(), self.selected_table.clone())
                else {
                    return Task::none();
                };
                self.status = Status::Busy(format!("checking the indexes of {}", table));
                let sql = dialect::index_check_sql(&table);
                Task::perform(
                    async move {
                        let started = Instant::now();
                        let result = session.query(&sql).await;
                        (sql, started.elapsed(), result)
                    },
                    |(sql, elapsed, result)| Message::IndexCheckFinished(sql, elapsed, result),
                )
            }
            Message::IndexCheckFinished(sql, elapsed, Ok(grid)) => {
                self.record(sql, elapsed, RunOutcome::Ok { statements: 1, rows: grid.rows.len(), affected: 0 });
                self.status = Status::Done(dialect::index_check_summary(&grid));
                Task::none()
            }
            Message::IndexCheckFinished(sql, elapsed, Err(e)) => {
                self.record(sql, elapsed, RunOutcome::Failed(e.clone()));
                self.status = Status::Failed(e);
                Task::none()
            }
        }
    }

    // -- loading -------------------------------------------------------------

    /// Stores a loaded value, or reports why it did not load.
    ///
    /// Every panel loads independently, so one failing — a `pg_stat` view a
    /// role cannot read, say — must leave the others showing what they have.
    fn absorb<T>(
        &mut self,
        result: Result<T, String>,
        apply: impl FnOnce(&mut Self, T),
    ) -> Task<Message> {
        match result {
            Ok(value) => {
                apply(self, value);
                if matches!(self.status, Status::Busy(_)) {
                    self.status = Status::Idle;
                }
            }
            Err(e) => self.status = Status::Failed(e),
        }
        Task::none()
    }

    fn reload_everything(&mut self) -> Task<Message> {
        self.status = Status::Busy("loading".to_string());
        Task::batch([
            self.load_stats(),
            self.load_tables(),
            self.load_databases(),
            self.load_schema(),
            self.load_config(),
            self.load_backups(),
            self.load_replication(),
            self.load_storage(),
        ])
    }

    /// Runs an import, or a preview of one.
    ///
    /// It goes through `query` rather than `execute` because the server answers
    /// with a report — a line per table, plus whatever it could not carry — and
    /// that report is the entire point of a preview.
    fn start_import(&mut self, sql: String) -> Task<Message> {
        let Some(session) = self.session.clone() else { return Task::none() };
        self.import_running = true;
        self.import_report.clear();
        self.status = Status::Busy(
            if self.import.dry_run { "previewing" } else { "importing" }.to_string(),
        );
        // The password is in the statement, so the history gets the statement
        // with it blanked. A console that filed someone's production password
        // into a list they can export would be the worst kind of helpful.
        self.record(
            redact_password(&sql),
            Duration::default(),
            RunOutcome::Ok { statements: 1, rows: 0, affected: 0 },
        );

        Task::perform(
            async move {
                let started = Instant::now();
                let result = session.query(&sql).await.map(|grid| {
                    crate::dialect::parse_import_report(&grid)
                });
                (started.elapsed(), result)
            },
            |(elapsed, result)| Message::ImportFinished(elapsed, result),
        )
    }

    /// Reads the recent statements and the per-shape totals in one task.
    ///
    /// Both go through the same "did the server understand the statement"
    /// check as the backup, replication and storage pages, and here it earns
    /// its keep twice over: a RasterizedDB build without `query_stats` answers
    /// `SHOW QUERIES` as a runtime parameter rather than refusing it, and a
    /// PostgreSQL without the extension loaded refuses `pg_stat_statements`
    /// outright. Both would otherwise render as a server that has run nothing,
    /// which is the one thing neither of them is.
    ///
    /// The two halves are checked separately because they fail separately: on
    /// PostgreSQL `pg_stat_activity` always answers while `pg_stat_statements`
    /// usually does not, and a page that hid the half that worked would be
    /// blank on most PostgreSQL servers.
    fn load_activity(&self) -> Task<Message> {
        let Some(session) = self.session.clone() else { return Task::none() };
        let flavor = session.flavor;
        let recent_sql = dialect::recent_statements_sql(flavor, RECENT_STATEMENTS);
        let shapes_sql = dialect::statement_shapes_sql(flavor).to_string();

        Task::perform(
            async move {
                let (recent, recent_supported) = match session.query(&recent_sql).await {
                    Ok(grid) if grid.column_index("statement").is_some() => {
                        (dialect::parse_statement_records(&grid), true)
                    }
                    _ => (Vec::new(), false),
                };
                let (shapes, shapes_supported) = match session.query(&shapes_sql).await {
                    Ok(grid) if grid.column_index("calls").is_some() => {
                        (dialect::parse_statement_shapes(&grid), true)
                    }
                    _ => (Vec::new(), false),
                };
                Box::new(ActivitySnapshot { recent, recent_supported, shapes, shapes_supported })
            },
            Message::ActivityLoaded,
        )
    }

    /// Reads node state and the peer list in one task.
    ///
    /// Both go through the same "did the server understand the statement"
    /// check as the backup page — an older build answers an unknown `SHOW x`
    /// with an empty column named after it rather than refusing it, and that
    /// parses into a status result that says nothing while looking like one.
    /// Reads the branch list. A server without branches refuses `SHOW
    /// BRANCHES`, which the page says rather than showing an empty list.
    fn load_branches(&self) -> Task<Message> {
        let Some(session) = self.session.clone() else { return Task::none() };
        Task::perform(
            async move {
                match session.query(dialect::BRANCHES_SQL).await {
                    Ok(grid) if grid.column_index("branch").is_some() => Box::new(BranchesSnapshot {
                        supported: true,
                        branches: dialect::parse_branches(&grid),
                    }),
                    _ => Box::new(BranchesSnapshot::default()),
                }
            },
            Message::BranchesLoaded,
        )
    }

    /// Reads the broker's topics, queues, subscriptions, exchanges and
    /// bindings in one task. A server that refuses the first has no broker.
    fn load_broker(&self) -> Task<Message> {
        let Some(session) = self.session.clone() else { return Task::none() };
        Task::perform(
            async move {
                let topics = match session.query(dialect::BROKER_TOPICS_SQL).await {
                    Ok(grid) => grid,
                    Err(refusal) => return Box::new(BrokerSnapshot { refusal, ..BrokerSnapshot::default() }),
                };
                let read = |grid: Result<Grid, String>| grid.unwrap_or_default();
                let queues = read(session.query(dialect::BROKER_QUEUES_SQL).await);
                let subscriptions = read(session.query(dialect::BROKER_SUBSCRIPTIONS_SQL).await);
                let exchanges = read(session.query(dialect::BROKER_EXCHANGES_SQL).await);
                let bindings = read(session.query(dialect::BROKER_BINDINGS_SQL).await);
                // Refused by a server without broker permissions: no grants.
                let grants = read(session.query(dialect::BROKER_GRANTS_SQL).await);
                Box::new(BrokerSnapshot {
                    supported: true,
                    refusal: String::new(),
                    topics: dialect::parse_broker_topics(&topics),
                    queues: dialect::parse_broker_queues(&queues),
                    subscriptions: dialect::parse_broker_subscriptions(&subscriptions),
                    exchanges: dialect::parse_broker_exchanges(&exchanges),
                    bindings: dialect::parse_broker_bindings(&bindings),
                    grants: dialect::parse_broker_grants(&grants),
                })
            },
            Message::BrokerLoaded,
        )
    }

    /// Runs a READ, POP or SHOW PARTITIONS into the message panel.
    fn broker_browse(&mut self, title: String, sql: String) -> Task<Message> {
        let Some(session) = self.session.clone() else { return Task::none() };
        self.broker_browse = Some(BrokerBrowse { title, statement: sql.clone(), result: None });
        Task::perform(
            async move {
                let result = session.query(&sql).await;
                (sql, result)
            },
            |(sql, result)| Message::BrokerBrowseLoaded(sql, result),
        )
    }

    /// The database this console is connected to.
    pub fn current_database(&self) -> String {
        self.session.as_ref().map(|s| s.database.clone()).unwrap_or_default()
    }

    /// Reads the cluster map, the gateway's rows (when the connection is to
    /// one), this shard's load and its prepared two-phase parts, in one task.
    /// Each statement a node does not answer leaves its part empty: a shard
    /// refuses `SHOW GATEWAY`, and a plain node answers `SHOW SHARDS` with no
    /// rows.
    #[cfg(feature = "enterprise")]
    fn load_ledger(&self) -> Task<Message> {
        let Some(session) = self.session.clone() else { return Task::none() };
        Task::perform(
            async move {
                let mut page = rdb_studio_enterprise::LedgerPage::default();
                match session.query(rdb_studio_enterprise::SHOW_SQL).await {
                    Err(e) => page.error = Some(e),
                    Ok(grid) => {
                        page.show = first_column(&grid);
                        for name in rdb_studio_enterprise::LedgerPage::ledger_names(&page.show) {
                            if let Ok(grid) = session.query(&rdb_studio_enterprise::count_sql(&name)).await {
                                if let Some(n) = first_column(&grid).first().and_then(|c| c.trim().parse().ok()) {
                                    page.counts.insert(name, n);
                                }
                            }
                        }
                        for name in rdb_studio_enterprise::LedgerPage::vault_names(&page.show) {
                            let secrets = match session.query(&rdb_studio_enterprise::list_sql(&name)).await {
                                Ok(grid) => first_column(&grid),
                                Err(e) => vec![e],
                            };
                            page.vaults.push(rdb_studio_enterprise::VaultView { name, secrets, audit: None });
                        }
                    }
                }
                Box::new(page)
            },
            Message::LedgerLoaded,
        )
    }

    fn load_shards(&self) -> Task<Message> {
        let Some(session) = self.session.clone() else { return Task::none() };
        let Some(shards_sql) = dialect::shards_sql(session.flavor) else {
            return Task::done(Message::ShardsLoaded(Box::default()));
        };
        Task::perform(
            async move {
                let mut snapshot = ShardsSnapshot::default();
                if let Ok(grid) = session.query(shards_sql).await {
                    dialect::parse_shards(&grid, &mut snapshot);
                }
                if let Ok(grid) = session.query(dialect::SHOW_GATEWAY_SQL).await {
                    snapshot.gateway = dialect::parse_gateway(&grid);
                }
                // Through a gateway, its rebalance: refused for a role it does
                // not name, which the page then says instead of the controls.
                if !snapshot.gateway.is_empty() {
                    match session.query(dialect::SHOW_REBALANCE_STATUS_SQL).await {
                        Ok(grid) => snapshot.rebalance = dialect::parse_gateway(&grid),
                        Err(e) => snapshot.rebalance_refused = Some(e),
                    }
                }
                if snapshot.supported {
                    if let Ok(grid) = session.query(dialect::SHOW_SHARD_LOAD_SQL).await {
                        snapshot.load = dialect::parse_shard_load(&grid);
                    }
                    if let Ok(grid) = session.query(dialect::SHOW_PREPARED_SQL).await {
                        snapshot.prepared = dialect::parse_prepared(&grid);
                    }
                }
                Box::new(snapshot)
            },
            Message::ShardsLoaded,
        )
    }

    fn load_replication(&self) -> Task<Message> {
        let Some(session) = self.session.clone() else { return Task::none() };
        let flavor = session.flavor;
        let stats_sql = dialect::replication_stats_sql(flavor).to_string();
        let peers_sql = dialect::replication_peers_sql(flavor).to_string();

        Task::perform(
            async move {
                let (stats, supported) = match session.query(&stats_sql).await {
                    Ok(grid) if grid.column_index("metric").is_some() => {
                        (dialect::parse_stats(&grid), true)
                    }
                    _ => (Vec::new(), false),
                };
                let peers = match session.query(&peers_sql).await {
                    Ok(grid) if grid.column_index("peer").is_some() => dialect::parse_peers(&grid),
                    _ => Vec::new(),
                };
                Box::new(ReplicationSnapshot { stats, supported, peers })
            },
            Message::ReplicationLoaded,
        )
    }

    /// Reads the pool, its directories and every table's placement in one task.
    ///
    /// All three go through the same "did the server understand the statement"
    /// check the backup and replication pages use. A build without the
    /// `storage_pools` feature does not refuse `SHOW STORAGE STATS`: the wire
    /// layer answers an unknown `SHOW x` as a runtime parameter, with one empty
    /// column named after it, which parses into a snapshot that looks exactly
    /// like a database with one directory. Those are different things and the
    /// page says which it is looking at.
    fn load_storage(&self) -> Task<Message> {
        let Some(session) = self.session.clone() else { return Task::none() };
        let flavor = session.flavor;
        let stats_sql = dialect::storage_stats_sql(flavor).to_string();
        let locations_sql = dialect::storage_locations_sql(flavor).to_string();
        let placements_sql = dialect::storage_placements_sql(flavor).to_string();
        let stores_sql = dialect::storage_stores_sql(flavor).map(str::to_string);

        Task::perform(
            async move {
                let (stats, supported) = match session.query(&stats_sql).await {
                    Ok(grid) if grid.column_index("metric").is_some() => {
                        (dialect::parse_stats(&grid), true)
                    }
                    _ => (Vec::new(), false),
                };
                let locations = match session.query(&locations_sql).await {
                    Ok(grid) if grid.column_index("location").is_some() => {
                        dialect::parse_storage_locations(&grid)
                    }
                    _ => Vec::new(),
                };
                let placements = match session.query(&placements_sql).await {
                    Ok(grid) if grid.column_index("table_name").is_some() => {
                        dialect::parse_storage_placements(&grid)
                    }
                    _ => Vec::new(),
                };
                // Not an error when absent: a server built without
                // `kv_storage` answers an unknown SHOW as a parameter, and one
                // with it but no stores answers no rows.
                let stores = match stores_sql {
                    Some(sql) => match session.query(&sql).await {
                        Ok(grid) if grid.column_index("store").is_some() => {
                            dialect::parse_storage_stores(&grid)
                        }
                        _ => Vec::new(),
                    },
                    None => Vec::new(),
                };
                Box::new(StorageSnapshot { stats, supported, locations, stores, placements })
            },
            Message::StorageLoaded,
        )
    }

    /// Runs a storage statement whose answer is lines of text.
    ///
    /// Through `query` rather than `execute` for the same reason the import
    /// does: a dry run reports a plan, and a plan reported as "0 rows affected"
    /// is no plan at all.
    fn storage_statement(&mut self, sql: String) -> Task<Message> {
        let Some(session) = self.session.clone() else { return Task::none() };
        self.storage_busy = true;
        self.storage_report.clear();
        self.status = Status::Busy("planning".to_string());

        Task::perform(
            async move {
                session
                    .query(&sql)
                    .await
                    .map(|grid| dialect::parse_report_lines(&grid))
            },
            Message::StorageReport,
        )
    }

    /// Reads the whole backup picture in one task.
    ///
    /// Both statements are asked and neither failure is fatal, because the
    /// *combination* is what says which screen to show — see
    /// [`BackupSnapshot::state`].
    fn load_backups(&self) -> Task<Message> {
        let Some(session) = self.session.clone() else { return Task::none() };
        let flavor = session.flavor;
        let status_sql = dialect::backup_status_sql(flavor).to_string();
        let catalogue_sql = dialect::backups_sql(flavor).map(str::to_string);

        Task::perform(
            async move {
                let (status, status_supported) = match session.query(&status_sql).await {
                    // A server that does not know the statement answers as if
                    // it were a runtime parameter: one column, named after the
                    // statement, holding nothing. The `metric` column is what
                    // says a real status result came back.
                    Ok(grid) if grid.column_index("metric").is_some() => {
                        (dialect::parse_stats(&grid), true)
                    }
                    Ok(_) => (Vec::new(), false),
                    Err(_) => (Vec::new(), false),
                };

                let catalogue = match catalogue_sql {
                    Some(sql) => session.query(&sql).await.map(|g| dialect::parse_backups(&g)),
                    None => Ok(Vec::new()),
                };

                Box::new(BackupSnapshot { status, status_supported, catalogue })
            },
            Message::BackupLoaded,
        )
    }

    /// Runs one backup action, confirming the destructive one first.
    ///
    /// `PRUNE` is the only one that removes anything, and what it removes is
    /// decided by a retention policy set on the command line rather than by
    /// anything on this screen — so the confirmation names the policy rather
    /// than asking "are you sure".
    fn run_backup(&mut self, action: BackupAction) -> Task<Message> {
        if action == BackupAction::Prune {
            let retained = self
                .backup
                .as_ref()
                .and_then(|snapshot| {
                    snapshot
                        .status
                        .iter()
                        .find(|s| s.metric == "retain_snapshots")
                })
                .and_then(|s| s.value.trim().parse::<u64>().ok())
                .unwrap_or(0);

            let body = if retained == 0 {
                "Retention is 0, which means every snapshot is kept. This will delete nothing."
                    .to_string()
            } else {
                let older = self
                    .backup
                    .as_ref()
                    .and_then(|snapshot| snapshot.catalogue.as_ref().ok())
                    .map(|entries| entries.iter().filter(|e| e.is_snapshot()).count())
                    .unwrap_or(0)
                    .saturating_sub(retained as usize);
                format!(
                    "This keeps the newest {retained} snapshots and permanently deletes the rest \
                     from the bucket, along with the log bundles only they needed.\n\n\
                     {older} snapshot{} would go. Objects deleted from S3 are not recoverable, and \
                     any point-in-time target that depended on them stops being reachable.",
                    if older == 1 { "" } else { "s" }
                )
            };

            self.modal = Some(Modal::Confirm {
                title: "Prune backups".to_string(),
                body,
                statement: action.sql().to_string(),
                refresh: Refresh::Backups,
            });
            return Task::none();
        }

        if action == BackupAction::Rollover {
            // Named plainly, because the order is the whole reason this is
            // safe to press: the new snapshot has to land before anything old
            // is removed, so the worst case is a failed rollover that changed
            // nothing rather than a window with no backup.
            let segments = self
                .backup
                .as_ref()
                .and_then(|snapshot| snapshot.catalogue.as_ref().ok())
                .map(|entries| entries.iter().filter(|e| !e.is_snapshot()).count())
                .unwrap_or(0);

            self.modal = Some(Modal::Confirm {
                title: "Force a rollover".to_string(),
                body: format!(
                    "This copies the whole database to a fresh snapshot and, once that has \
                     landed, permanently deletes every earlier generation — the older \
                     snapshots and the {segments} log bundle{} they needed.\n\n\
                     Nothing is deleted unless the new snapshot succeeds, so a rollover that \
                     fails leaves the current backup untouched. Afterwards the bucket holds \
                     one self-contained backup, and point-in-time targets before it stop \
                     being reachable.",
                    if segments == 1 { "" } else { "s" }
                ),
                statement: action.sql().to_string(),
                refresh: Refresh::Backups,
            });
            return Task::none();
        }

        let Some(session) = self.session.clone() else { return Task::none() };
        let sql = action.sql().to_string();
        self.backup_busy = Some(action.label().to_string());
        self.status = Status::Busy(action.label().to_string());
        Task::perform(
            async move {
                let started = Instant::now();
                let result = session.execute(&sql).await;
                (sql, started.elapsed(), result)
            },
            |(sql, elapsed, result)| Message::BackupFinished(sql, elapsed, result),
        )
    }

    /// The recovery target the restore form currently describes.
    pub fn restore_target(&self) -> RestoreTarget {
        match self.restore_kind {
            RestoreKind::Latest => RestoreTarget::Latest,
            RestoreKind::Snapshot => RestoreTarget::Snapshot(self.restore_snapshot.clone()),
            // An unparseable box reads as 0 rather than as `Latest`: a target
            // that silently became "everything" would restore *more* than was
            // asked for, and the warning about discarded commits would vanish
            // from the dialog at the same moment.
            RestoreKind::Lsn => RestoreTarget::Lsn(self.restore_lsn.trim().parse().unwrap_or(0)),
            RestoreKind::Time => RestoreTarget::Time(self.restore_time.clone()),
        }
    }

    fn confirm_restore(&mut self) -> Task<Message> {
        let target = self.restore_target();
        let sql = match dialect::restore_sql(&self.restore_dir, &target) {
            Ok(sql) => sql,
            Err(e) => {
                self.status = Status::Failed(e);
                return Task::none();
            }
        };

        let mut body = format!(
            "This writes a complete database into:\n\n    {}\n\nAnything already in that \
             directory can be overwritten, and there is no undo.",
            self.restore_dir.trim()
        );
        if target.discards_later_commits() {
            body.push_str(&format!(
                "\n\nRecovering to {} deliberately leaves out every commit after that point. \
                 The copy will not contain them.",
                target.label()
            ));
        }
        if matches!(target, RestoreTarget::Time(_)) {
            body.push_str(
                "\n\nA time target resolves to the end of the last log bundle archived at or \
                 before that instant, so the cut can land earlier than you asked.",
            );
        }
        body.push_str("\n\nThe running database is not touched.");

        self.modal = Some(Modal::ConfirmRestore { body, statement: sql });
        Task::none()
    }

    /// Takes one reading for the dashboard.
    ///
    /// The same statement as [`load_stats`](Self::load_stats), routed to a
    /// different message: a sample must not clear the status bar or report a
    /// transient failure there, and the overview's own load must not be
    /// mistaken for a sample and used to compute a rate over the wrong
    /// interval.
    fn sample(&self) -> Task<Message> {
        let Some(session) = self.session.clone() else { return Task::none() };
        let sql = dialect::database_stats_sql(session.flavor).to_string();
        Task::perform(
            async move { session.query(&sql).await.map(|g| dialect::parse_stats(&g)) },
            Message::Sampled,
        )
    }

    fn load_stats(&self) -> Task<Message> {
        let Some(session) = self.session.clone() else { return Task::none() };
        let sql = dialect::database_stats_sql(session.flavor).to_string();
        Task::perform(
            async move { session.query(&sql).await.map(|g| dialect::parse_stats(&g)) },
            Message::StatsLoaded,
        )
    }

    fn load_tables(&self) -> Task<Message> {
        let Some(session) = self.session.clone() else { return Task::none() };
        let sql = dialect::tables_sql(session.flavor).to_string();
        Task::perform(
            async move { session.query(&sql).await.map(|g| dialect::parse_tables(&g)) },
            Message::TablesLoaded,
        )
    }

    fn load_databases(&self) -> Task<Message> {
        let Some(session) = self.session.clone() else { return Task::none() };
        let sql = dialect::databases_sql(session.flavor).to_string();
        Task::perform(
            async move { session.query(&sql).await.map(|g| dialect::parse_databases(&g)) },
            Message::DatabasesLoaded,
        )
    }

    fn load_table_stats(&self) -> Task<Message> {
        let (Some(session), Some(table)) = (self.session.clone(), self.selected_table.clone())
        else {
            return Task::none();
        };
        let sql = dialect::table_stats_sql(session.flavor, &table);
        Task::perform(
            async move { session.query(&sql).await.map(|g| dialect::parse_stats(&g)) },
            Message::TableStatsLoaded,
        )
    }

    fn load_columns(&self) -> Task<Message> {
        let (Some(session), Some(table)) = (self.session.clone(), self.selected_table.clone())
        else {
            return Task::none();
        };
        let sql = dialect::columns_sql(session.flavor, &table);
        Task::perform(
            async move { session.query(&sql).await.map(|g| dialect::parse_columns(&g)) },
            Message::ColumnsLoaded,
        )
    }

    /// The selected table's indexes. Issued once its columns have landed,
    /// which is what tells a RasterizedDB primary key's index from a unique
    /// one; see [`dialect::parse_indexes`].
    fn load_indexes(&self) -> Task<Message> {
        let (Some(session), Some(table)) = (self.session.clone(), self.selected_table.clone())
        else {
            return Task::none();
        };
        let sql = dialect::indexes_sql(session.flavor, &table);
        let fallback = dialect::indexes_fallback_sql(session.flavor, &table);
        let primary: Vec<String> = self
            .columns
            .iter()
            .filter(|c| c.primary_key)
            .map(|c| c.name.clone())
            .collect();
        Task::perform(
            async move {
                let primary: Vec<&str> = primary.iter().map(String::as_str).collect();
                // A server that cannot answer the owners' join is read as
                // before: `pg_indexes` alone.
                let grid = match (session.query(&sql).await, fallback) {
                    (Err(_), Some(fallback)) => session.query(&fallback).await,
                    (result, _) => result,
                };
                let result = grid.map(|g| dialect::parse_indexes(&g, &primary));
                (table, result)
            },
            |(table, result)| Message::IndexesLoaded(table, result),
        )
    }

    fn load_partitions(&self) -> Task<Message> {
        let (Some(session), Some(table)) = (self.session.clone(), self.selected_table.clone())
        else {
            return Task::none();
        };
        let sql = dialect::partitions_sql(&table);
        Task::perform(
            async move {
                let result = session.query(&sql).await.map(|g| dialect::parse_partitions(&g));
                (table, result)
            },
            |(table, result)| Message::PartitionsLoaded(table, result),
        )
    }

    /// Loads the whole graph: the edges, and every table's columns.
    ///
    /// The columns are fetched per table rather than in one statement because
    /// the RasterizedDB side has no way to ask for all of them at once, and a
    /// query the two dialects cannot share would defeat the point of the
    /// dialect layer. Schema maps are opened rarely and the requests run
    /// concurrently, so the round trips are affordable.
    fn load_schema(&self) -> Task<Message> {
        let Some(session) = self.session.clone() else { return Task::none() };
        let flavor = session.flavor;
        let edges_sql = dialect::schema_map_sql(flavor).to_string();
        let tables_sql = dialect::tables_sql(flavor).to_string();

        Task::perform(
            async move {
                let tables = dialect::parse_tables(&session.query(&tables_sql).await?);
                let edges = dialect::parse_relationships(&session.query(&edges_sql).await?);

                let mut nodes = Vec::with_capacity(tables.len());
                for table in &tables {
                    let sql = dialect::columns_sql(flavor, &table.name);
                    let columns = match session.query(&sql).await {
                        Ok(grid) => dialect::parse_columns(&grid),
                        // A table whose columns cannot be read still belongs on
                        // the map; drawing it empty beats dropping it and
                        // leaving its edges pointing at nothing.
                        Err(_) => Vec::new(),
                    };
                    nodes.push(TableNode {
                        name: table.name.clone(),
                        columns,
                        rows: table.row_slots,
                        x: 0.0,
                        y: 0.0,
                    });
                }

                let mut graph = SchemaGraph { tables: nodes, edges };
                crate::views::schema::layout(&mut graph);
                Ok(graph)
            },
            Message::SchemaLoaded,
        )
    }

    /// The names Ctrl+Space completes: every table, the selected table's
    /// columns, and every column the schema map has loaded.
    fn completion_names(&self) -> Vec<String> {
        let mut names: Vec<String> = self.tables.iter().map(|t| t.name.clone()).collect();
        names.extend(self.columns.iter().map(|c| c.name.clone()));
        for table in &self.schema.tables {
            names.push(table.name.clone());
            names.extend(table.columns.iter().map(|c| c.name.clone()));
        }
        names
    }

    /// A page of the KEYS tab's scan: from the start, or on from the last
    /// page's cursor. The start also counts the store.
    fn kv_scan(&mut self, more: bool) -> Task<Message> {
        let Some(session) = self.session.clone() else { return Task::none() };
        if session.flavor != Flavor::Rasterized {
            return Task::none();
        }
        let cursor = if more { self.kv.cursor.clone().unwrap_or_default() } else { "0".into() };
        if more && self.kv.cursor.is_none() {
            return Task::none();
        }
        let sql = dialect::kv_scan_sql(&cursor, &self.kv.pattern, KV_PAGE);
        Task::perform(
            async move {
                let (cursor, keys) = dialect::parse_kv_scan(&session.query(&sql).await?);
                let size = if more {
                    None
                } else {
                    session
                        .query("KV DBSIZE")
                        .await
                        .ok()
                        .and_then(|g| g.number(0, "value"))
                };
                Ok((cursor, keys, size))
            },
            move |result| Message::KvScanned(more, result),
        )
    }

    fn load_roles(&self) -> Task<Message> {
        Task::batch([self.load_role_list(), self.load_role_access()])
    }

    /// Grants, default privileges and policies. Each part a server refuses
    /// is left empty and named in `errors`; the rest still shows.
    fn load_role_access(&self) -> Task<Message> {
        let Some(session) = self.session.clone() else { return Task::none() };
        Task::perform(
            async move {
                let mut access = crate::model::RoleAccess::default();
                match session.query(dialect::table_grants_sql()).await {
                    Ok(g) => (access.grants, access.owned) = dialect::parse_table_grants(&g),
                    Err(e) => access.errors.push(format!("table grants: {e}")),
                }
                match session.query(dialect::default_privileges_sql()).await {
                    Ok(g) => access.defaults = dialect::parse_default_privileges(&g),
                    Err(e) => access.errors.push(format!("default privileges: {e}")),
                }
                match session.query(dialect::policies_sql(session.flavor)).await {
                    Ok(g) => access.policies = dialect::parse_policies(&g),
                    Err(e) => access.errors.push(format!("policies: {e}")),
                }
                if let Ok(g) = session.query(dialect::rls_tables_sql()).await {
                    access.rls_tables = (0..g.rows.len()).filter_map(|r| g.value(r, "name").map(str::to_string)).collect();
                }
                Box::new(access)
            },
            Message::RoleAccessLoaded,
        )
    }

    fn load_role_list(&self) -> Task<Message> {
        let Some(session) = self.session.clone() else { return Task::none() };
        let sql = dialect::roles_sql(session.flavor);
        Task::perform(
            async move {
                let roles = dialect::parse_roles(&session.query(sql).await?);
                // Memberships are an addition: a server without them still
                // lists its roles.
                Ok(match session.query(dialect::memberships_sql()).await {
                    Ok(grid) => dialect::add_memberships(roles, &grid),
                    Err(_) => roles,
                })
            },
            Message::RolesLoaded,
        )
    }

    fn load_config(&self) -> Task<Message> {
        let Some(session) = self.session.clone() else { return Task::none() };
        let sql = dialect::config_sql(session.flavor).to_string();
        Task::perform(
            async move { session.query(&sql).await.map(|g| dialect::parse_config(&g)) },
            Message::ConfigLoaded,
        )
    }

    /// One page of the selected table, and — on the first page only — the exact
    /// row count.
    ///
    /// The count is a scan on both engines, so it is not repeated for every
    /// page; the footer shows the figure it got when the table was opened.
    fn load_page(&self) -> Task<Message> {
        let (Some(session), Some(table)) = (self.session.clone(), self.selected_table.clone())
        else {
            return Task::none();
        };
        let filter = self.data_filter_applied.clone();
        let Some((sql, signature)) = self.page_query() else {
            return Task::none();
        };
        let page = self.data_page;
        let count_sql =
            (self.data_total.is_none()).then(|| dialect::count_sql(&table, filter.as_ref()));

        Task::perform(
            async move {
                let grid = session.query(&sql).await?;
                let total = match count_sql {
                    Some(sql) => session
                        .query(&sql)
                        .await
                        .ok()
                        .and_then(|g| g.number(0, "n").or_else(|| g.number(0, "count"))),
                    None => None,
                };
                Ok((grid, total))
            },
            move |result| Message::DataLoaded(page, signature, result),
        )
    }

    /// The key column, its direction and kind, when the browser's order is
    /// that key's alone and its values can be written back exactly: then a
    /// page can start after the previous page's last key.
    fn keyset_order(&self) -> Option<(String, bool, dialect::KeysetKind)> {
        let key = self.row_key()?;
        let descending = match self.data_sort() {
            None => false,
            Some(sort) if sort.column == key.column => sort.descending,
            // Ordered by another column first: rows sharing its value are
            // ordered by the key, so the key alone does not say where a page
            // ended.
            Some(_) => return None,
        };
        let column = self.columns.iter().find(|c| c.name == key.column)?;
        let kind = dialect::KeysetKind::of(&column.data_type)?;
        Some((key.column, descending, kind))
    }

    /// The statement for the data browser's current page, and the signature
    /// that ties a page's end to it: the first page's statement, which names
    /// the table, filter, order and page size. A page starts after the
    /// previous page's last key when that is known under the same
    /// signature, and at an offset otherwise.
    pub fn page_query(&self) -> Option<(String, String)> {
        let table = self.selected_table.as_deref()?;
        let key = self.row_key();
        let sort = self.data_sort();
        let filter = self.data_filter_applied.as_ref();
        let at = |offset| {
            dialect::page_sql(
                table,
                filter,
                key.as_ref().map(|k| k.column.as_str()),
                sort.as_ref().map(|s| (s.column.as_str(), s.descending)),
                self.page_size,
                offset,
            )
        };
        let signature = at(0);
        let page = self.data_page;
        let after = (page > 0 && self.data_keyset.signature == signature)
            .then(|| self.data_keyset.after.get(page - 1).cloned().flatten())
            .flatten();
        let sql = match (after, self.keyset_order()) {
            (Some(after), Some((key, descending, _))) => {
                dialect::page_after_sql(table, filter, &key, descending, &after, self.page_size)
            }
            _ => at(page * self.page_size),
        };
        Some((sql, signature))
    }

    /// Records where page `page` ended, when it is still the page shown under
    /// the same statement. A late answer (two NEXTs pressed quickly, or a
    /// page read before the filter changed) is not recorded: its last key
    /// would start some other page, and that page would skip or repeat rows
    /// with nothing to show for it.
    fn note_page_end(&mut self, page: usize, signature: &str, grid: &Grid) {
        let current = self.page_query().map(|(_, signature)| signature);
        if page != self.data_page || current.as_deref() != Some(signature) {
            return;
        }
        if self.data_keyset.signature != signature {
            self.data_keyset = Keyset {
                signature: signature.to_string(),
                after: Vec::new(),
            };
        }
        self.data_keyset.after.truncate(page);
        if self.data_keyset.after.len() != page {
            return;
        }
        let after = self.keyset_order().and_then(|(key, _, kind)| {
            let at = grid.columns.iter().position(|c| *c == key)?;
            let last = grid.rows.last()?;
            dialect::key_literal(last.get(at)?.as_deref()?, kind)
        });
        self.data_keyset.after.push(after);
    }

    // -- writing -------------------------------------------------------------

    fn run_sql(&mut self, sql: String) -> Task<Message> {
        let Some(session) = self.session.clone() else { return Task::none() };
        if sql.trim().is_empty() {
            return Task::none();
        }
        if self.query_cancel.is_some() {
            self.status = Status::Failed("a run is still in flight — wait for it, or cancel it".into());
            return Task::none();
        }
        self.status = Status::Busy("running".to_string());
        // Held rather than pushed: the entry is only worth keeping once it has
        // an outcome and a duration to keep with it.
        self.pending_sql = Some(sql.clone());
        let canceller = session.canceller();
        self.query_cancel = Some(canceller.clone());
        self.cancel_sent = false;
        let max_rows = self.row_limit.rows();

        Task::perform(
            async move {
                // Round trip, measured around the whole exchange. It includes
                // the network, so it is never labelled as the server's time.
                let started = Instant::now();
                let answer = session.run_editor(&sql, max_rows, &canceller).await;
                (answer, started.elapsed())
            },
            |(answer, elapsed)| Message::QueryFinished(answer, elapsed),
        )
    }

    /// Asks where to save something and writes it.
    fn save_file(
        &mut self,
        base: &str,
        extension: &'static str,
        kind: &'static str,
        contents: String,
    ) -> Task<Message> {
        let spec = crate::files::SaveSpec::new(
            crate::files::suggested_name(base, extension),
            kind,
            match extension {
                "csv" => &["csv"],
                "json" => &["json"],
                "md" => &["md", "markdown"],
                _ => &["sql"],
            },
        );
        Task::perform(crate::files::save(spec, contents), Message::FileSaved)
    }

    /// The session's statements as a runnable script.
    fn history_as_sql(&self) -> String {
        history_as_sql(&self.history)
    }

    /// Files a finished statement in the history.
    fn record(&mut self, sql: String, elapsed: Duration, outcome: RunOutcome) {
        if sql.trim().is_empty() {
            return;
        }
        // Kept for the session, shown, and exported as a script: never with
        // a password in it.
        let sql = dialect::redact_passwords(&sql);
        self.history.push(HistoryEntry {
            sql,
            elapsed,
            at: Instant::now(),
            outcome,
        });
        // A convenience, not a record: an unbounded one is a slow leak in a
        // console left open for days.
        while self.history.len() > 200 {
            self.history.remove(0);
        }
    }

    /// Runs a statement a dialog or a button built, rather than one typed.
    ///
    /// It goes into the same history as anything typed: what a console did on
    /// your behalf is exactly what you want to find when you are working out
    /// what happened.
    fn statement(&mut self, sql: String, refresh: Refresh) -> Task<Message> {
        let Some(session) = self.session.clone() else { return Task::none() };
        self.status = Status::Busy("running".to_string());

        Task::perform(
            async move {
                let started = Instant::now();
                let result = session.execute(&sql).await;
                (sql, started.elapsed(), result)
            },
            move |(sql, elapsed, result)| {
                Message::StatementFinished(refresh, sql, elapsed, result)
            },
        )
    }

    /// The `WHERE` clause addressing one row of the data grid.
    ///
    /// `None` when the table has no single-column primary key. Every write path
    /// checks this first: without it an `UPDATE` would have to match on the
    /// row's values, which hits every duplicate of that row rather than the one
    /// the user clicked.
    fn where_clause_for(&self, row: usize) -> Option<String> {
        let key = self.row_key()?;
        let index = self.data.column_index(&key.column)?;
        let value = self.data.rows.get(row)?.get(index)?;
        Some(format!(
            "{} {}",
            dialect::quote_ident(&key.column),
            match value {
                None => "IS NULL".to_string(),
                Some(text) => format!("= {}", dialect::quote_literal(text)),
            }
        ))
    }

    fn commit_edit(&mut self) -> Task<Message> {
        let Some(edit) = self.editing.take() else { return Task::none() };
        let Some(table) = self.selected_table.clone() else { return Task::none() };
        let Some(column) = self.data.columns.get(edit.column).cloned() else {
            return Task::none();
        };

        if edit.key.is_empty() {
            self.status = Status::Failed(NO_KEY.to_string());
            return Task::none();
        }

        let value = if edit.null { None } else { Some(edit.value.as_str()) };
        let sql = format!(
            "UPDATE {} SET {} = {} WHERE {}",
            dialect::quote_ident(&table),
            dialect::quote_ident(&column),
            dialect::sql_value(value),
            edit.key
        );
        self.statement(sql, Refresh::CurrentTable)
    }

    fn commit_insert(&mut self) -> Task<Message> {
        let Some(values) = self.inserting.take() else { return Task::none() };
        let Some(table) = self.selected_table.clone() else { return Task::none() };

        // An empty field means "leave it out", not "the empty string": a column
        // with a `DEFAULT` or a generated key must not be handed `''`. A user
        // who wants an empty string has a column that accepts one and can set
        // it afterwards, which is the rarer case of the two.
        let mut columns = Vec::new();
        let mut literals = Vec::new();
        for (index, value) in values.iter().enumerate() {
            if value.trim().is_empty() {
                continue;
            }
            let Some(name) = self.data.columns.get(index) else { continue };
            columns.push(dialect::quote_ident(name));
            literals.push(if value.eq_ignore_ascii_case("null") {
                "NULL".to_string()
            } else {
                dialect::quote_literal(value)
            });
        }

        if columns.is_empty() {
            self.status = Status::Failed("nothing to insert: every field was left blank".to_string());
            return Task::none();
        }

        let sql = format!(
            "INSERT INTO {} ({}) VALUES ({})",
            dialect::quote_ident(&table),
            columns.join(", "),
            literals.join(", ")
        );
        self.statement(sql, Refresh::CurrentTable)
    }

    fn delete_row(&mut self, row: usize) -> Task<Message> {
        let Some(table) = self.selected_table.clone() else { return Task::none() };
        let Some(key) = self.where_clause_for(row) else {
            self.status = Status::Failed(NO_KEY.to_string());
            return Task::none();
        };
        let sql = format!("DELETE FROM {} WHERE {}", dialect::quote_ident(&table), key);
        self.modal = Some(Modal::Confirm {
            title: "Delete row".to_string(),
            body: format!("This deletes the row where {}. It cannot be undone.", key),
            statement: sql,
            refresh: Refresh::CurrentTable,
        });
        Task::none()
    }

    // -- modals --------------------------------------------------------------

    fn modal_field(&mut self, field: ModalField, value: String) {
        let flavor = self.flavor();
        match &mut self.modal {
            Some(Modal::CreateTable { name, .. }) if field == ModalField::Name => *name = value,
            Some(Modal::CreateDatabase { name, .. }) if field == ModalField::Name => *name = value,
            Some(Modal::CreateRole { name, password, .. }) => match field {
                ModalField::Name => *name = value,
                ModalField::Password => *password = value,
                _ => {}
            },
            Some(Modal::RolePassword { password, .. }) if field == ModalField::Password => {
                *password = value
            }
            Some(Modal::EditColumn { edit, .. }) => match field {
                ModalField::Name => edit.name = value,
                ModalField::DataType => edit.data_type = value,
                ModalField::Compression => edit.compression = value,
                _ => {}
            },
            Some(Modal::RowCompression { edit, .. }) if field == ModalField::Compression => {
                edit.setting = value
            }
            Some(Modal::AddColumn { column, .. }) => match field {
                ModalField::Name => column.rename(value, flavor),
                ModalField::Length => column.length = digits(&value),
                ModalField::Precision => column.precision = digits(&value),
                ModalField::Scale => column.scale = digits(&value),
                ModalField::Default => column.default = value,
                ModalField::Compression => column.compression = value,
                _ => {}
            },
            _ => {}
        }
    }

    fn modal_toggle(&mut self, field: ModalField, value: bool) {
        if let Some(Modal::CreateRole { login, superuser, .. }) = &mut self.modal {
            match field {
                ModalField::Login => *login = value,
                ModalField::Superuser => *superuser = value,
                _ => {}
            }
        }
        if let Some(Modal::EditColumn { edit, .. }) = &mut self.modal
            && field == ModalField::Nullable
        {
            edit.nullable = value;
        }
        if let Some(Modal::RowCompression { edit, .. }) = &mut self.modal
            && field == ModalField::Recompress
        {
            edit.recompress = value;
        }
        if let Some(Modal::AddColumn { column, .. }) = &mut self.modal {
            match field {
                ModalField::Nullable => column.nullable = value,
                ModalField::Unique => column.unique = value,
                ModalField::PrimaryKey => column.primary_key = value,
                _ => {}
            }
        }
    }

    fn modal_column(&mut self, index: usize, apply: impl FnOnce(&mut NewColumn)) {
        if let Some(Modal::CreateTable { columns, .. }) = &mut self.modal
            && let Some(column) = columns.get_mut(index)
        {
            apply(column);
        }
    }

    fn submit_modal(&mut self) -> Task<Message> {
        if self.modal_pending {
            return Task::none();
        }
        let Some(modal) = self.modal.take() else { return Task::none() };
        match modal {
            Modal::Confirm { statement, refresh, .. } => self.statement(statement, refresh),

            Modal::ConfirmImport { statement, .. } => self.start_import(statement),

            Modal::ConfirmCsv { scripts, .. } => {
                let Some(session) = self.session.clone() else { return Task::none() };
                self.status = Status::Busy("importing".to_string());
                Task::perform(
                    async move {
                        let started = Instant::now();
                        let mut imported = 0u64;
                        for script in &scripts {
                            if let Err(e) = session.execute(&script.sql).await {
                                return (
                                    imported,
                                    Some(format!(
                                        "records {}–{} of the file were refused, and none of them \
                                         went in: {}",
                                        script.first, script.last, e
                                    )),
                                    started.elapsed(),
                                );
                            }
                            imported += (script.last - script.first + 1) as u64;
                        }
                        (imported, None, started.elapsed())
                    },
                    |(rows, failure, elapsed)| Message::CsvImported(rows, failure, elapsed),
                )
            }

            Modal::ConfirmRestore { statement, .. } => {
                let Some(session) = self.session.clone() else { return Task::none() };
                self.restore_running = true;
                self.restore_report = None;
                self.status = Status::Busy("restoring".to_string());
                Task::perform(
                    async move {
                        let started = Instant::now();
                        let result = session.query(&statement).await;
                        (statement, started.elapsed(), result)
                    },
                    |(sql, elapsed, result)| Message::RestoreFinished(sql, elapsed, result),
                )
            }

            Modal::CreateDatabase { name, .. } => {
                if name.trim().is_empty() {
                    self.modal = Some(Modal::CreateDatabase {
                        name,
                        error: Some("a database needs a name".to_string()),
                    });
                    return Task::none();
                }
                let sql = format!("CREATE DATABASE {}", dialect::quote_ident(name.trim()));
                self.modal_statement(Modal::CreateDatabase { name, error: None }, sql, Refresh::Databases)
            }

            Modal::CreateTable { name, columns, partitioning, .. } => {
                match create_table_with(&name, &columns, &partitioning) {
                    Ok(sql) => self.modal_statement(
                        Modal::CreateTable { name, columns, partitioning, error: None },
                        sql,
                        Refresh::Tables,
                    ),
                    Err(e) => {
                        self.modal =
                            Some(Modal::CreateTable { name, columns, partitioning, error: Some(e) });
                        Task::none()
                    }
                }
            }

            Modal::CreateRole { name, password, login, superuser, .. } => {
                match dialect::create_role_sql(&name, &password, login, superuser) {
                    Ok(sql) => self.modal_statement(
                        Modal::CreateRole { name, password, login, superuser, error: None },
                        sql,
                        Refresh::Roles,
                    ),
                    Err(e) => {
                        self.modal = Some(Modal::CreateRole {
                            name,
                            password,
                            login,
                            superuser,
                            error: Some(e),
                        });
                        Task::none()
                    }
                }
            }
            Modal::RolePassword { role, password, .. } => {
                match dialect::role_password_sql(&role, &password) {
                    Ok(sql) => self.modal_statement(
                        Modal::RolePassword { role, password, error: None },
                        sql,
                        Refresh::Roles,
                    ),
                    Err(e) => {
                        self.modal = Some(Modal::RolePassword { role, password, error: Some(e) });
                        Task::none()
                    }
                }
            }

            Modal::EditColumn { table, edit, .. } => match edit.statements(&table) {
                Ok(sql) => self.modal_statement(
                    Modal::EditColumn { table, edit, error: None },
                    sql,
                    Refresh::CurrentTable,
                ),
                Err(e) => {
                    self.modal = Some(Modal::EditColumn { table, edit, error: Some(e) });
                    Task::none()
                }
            },

            Modal::RowCompression { table, edit, .. } => match edit.statements(&table) {
                Ok(sql) => self.modal_statement(
                    Modal::RowCompression { table, edit, error: None },
                    sql,
                    Refresh::CurrentTable,
                ),
                Err(e) => {
                    self.modal = Some(Modal::RowCompression { table, edit, error: Some(e) });
                    Task::none()
                }
            },

            Modal::AddColumn { table, column, .. } => match add_column_sql(&table, &column) {
                Ok(sql) => self.modal_statement(
                    Modal::AddColumn { table, column, error: None },
                    sql,
                    Refresh::CurrentTable,
                ),
                Err(e) => {
                    self.modal = Some(Modal::AddColumn { table, column, error: Some(e) });
                    Task::none()
                }
            },

            Modal::CreateIndex { table, index, .. } => match create_index_sql(&table, &index) {
                Ok(sql) => self.modal_statement(
                    Modal::CreateIndex { table, index, error: None },
                    sql,
                    Refresh::CurrentTable,
                ),
                Err(e) => {
                    self.modal = Some(Modal::CreateIndex { table, index, error: Some(e) });
                    Task::none()
                }
            },
        }
    }

    /// Sends a dialog's statement and keeps the dialog open until it answers.
    ///
    /// The server is the authority on what it accepts, and its refusals are
    /// specific — "partition key column `k` is nullable", "a B-tree cannot
    /// order a CHAR column" — so they belong next to the field they are about,
    /// with the form still filled in, rather than in the status bar after the
    /// form has been thrown away.
    fn modal_statement(&mut self, modal: Modal, sql: String, refresh: Refresh) -> Task<Message> {
        let Some(session) = self.session.clone() else { return Task::none() };
        self.modal = Some(modal);
        self.modal_pending = true;
        self.status = Status::Busy("running".to_string());
        Task::perform(
            async move {
                let started = Instant::now();
                let result = session.execute(&sql).await;
                (sql, started.elapsed(), result)
            },
            move |(sql, elapsed, result)| {
                Message::ModalStatementFinished(refresh, sql, elapsed, result)
            },
        )
    }

    fn set_modal_error(&mut self, message: String) {
        if let Some(
            Modal::CreateTable { error, .. }
            | Modal::CreateDatabase { error, .. }
            | Modal::AddColumn { error, .. }
            | Modal::EditColumn { error, .. }
            | Modal::RowCompression { error, .. }
            | Modal::CreateRole { error, .. }
            | Modal::RolePassword { error, .. }
            | Modal::CreateIndex { error, .. },
        ) = &mut self.modal
        {
            *error = Some(message);
        }
    }
}

/// Why a table cannot be edited row-wise, and what would fix it.
pub const NO_KEY: &str = "no column identifies a single row of this table: it declares no \
     primary key and has no UNIQUE NOT NULL column. Add one, and the browser can address rows.";

/// The session's statements as a runnable script.
///
/// Comments carry what the history pane shows — how long each took and what
/// came back — and a statement that **failed is commented out**. Exporting a
/// script that silently re-runs the failures would be handing someone a rake:
/// they are in the file because they are part of the story, not because they
/// should happen again.
pub fn history_as_sql(history: &[HistoryEntry]) -> String {
    let mut out = String::from(
        "-- rdb_studio session history\n\
         -- Statements that failed are commented out.\n\n",
    );

    for entry in history {
        match &entry.outcome {
            RunOutcome::Ok { .. } => {
                out.push_str(&format!(
                    "-- {} · {}\n",
                    entry.outcome.summary(),
                    duration(entry.elapsed)
                ));
                out.push_str(entry.sql.trim().trim_end_matches(';').trim_end());
                out.push_str(";\n\n");
            }
            RunOutcome::Failed(error) => {
                out.push_str(&format!("-- FAILED: {}\n", error.replace('\n', " ")));
                for line in entry.sql.trim().lines() {
                    out.push_str("-- ");
                    out.push_str(line);
                    out.push('\n');
                }
                out.push('\n');
            }
        }
    }
    out
}

/// Blanks the password in an `IMPORT` statement.
///
/// The statement carries the source database's password as a literal, and the
/// history is a list somebody can read on screen and export to a file.
pub fn redact_password(sql: &str) -> String {
    let Some(at) = sql.find("PASSWORD '") else { return sql.to_string() };
    let start = at + "PASSWORD '".len();

    // To the closing quote, respecting the doubling that escapes one.
    let bytes: Vec<char> = sql.chars().collect();
    let mut index = sql[..start].chars().count();
    while index < bytes.len() {
        if bytes[index] == '\'' {
            if bytes.get(index + 1) == Some(&'\'') {
                index += 2;
                continue;
            }
            break;
        }
        index += 1;
    }

    let head: String = bytes[..sql[..start].chars().count()].iter().collect();
    let tail: String = bytes[index..].iter().collect();
    format!("{}<redacted>{}", head, tail)
}

/// Keeps a numeric field numeric while it is being typed.
fn digits(value: &str) -> String {
    value.chars().filter(char::is_ascii_digit).collect()
}

/// Builds a `CREATE TABLE`, refusing the shapes that would fail on the server.
///
/// Validating here rather than sending it means the error names the field that
/// is wrong while the dialog is still open, instead of arriving as a syntax
/// error after the dialog has closed and the input is gone.
pub fn create_table_sql(name: &str, columns: &[NewColumn]) -> Result<String, String> {
    create_table_with(name, columns, &Partitioning::default())
}

/// [`create_table_sql`], partitioned when `partitioning` says so.
///
/// More than one column marked as the primary key makes a composite key,
/// written as a table constraint — which is what a partitioned table usually
/// needs, since its key has to contain every partition key column.
pub fn create_table_with(
    name: &str,
    columns: &[NewColumn],
    partitioning: &Partitioning,
) -> Result<String, String> {
    let name = name.trim();
    if name.is_empty() {
        return Err("the table needs a name".to_string());
    }
    let usable: Vec<&NewColumn> = columns.iter().filter(|c| !c.name.trim().is_empty()).collect();
    if usable.is_empty() {
        return Err("the table needs at least one column".to_string());
    }
    if usable.iter().any(|c| c.data_type().trim().is_empty()) {
        return Err("every column needs a type".to_string());
    }
    let composite_key = usable.iter().filter(|c| c.primary_key).count() > 1;

    let mut seen = std::collections::HashSet::new();
    for column in &usable {
        if !seen.insert(column.name.trim().to_ascii_lowercase()) {
            return Err(format!("`{}` is declared twice", column.name.trim()));
        }
    }

    let body: Vec<String> = usable
        .iter()
        .map(|column| {
            let mut part = format!(
                "{} {}",
                dialect::quote_ident(column.name.trim()),
                column.data_type().trim()
            );
            if column.primary_key && !composite_key {
                part.push_str(" PRIMARY KEY");
            } else if column.primary_key {
                // A key column is NOT NULL either way; saying so lets a
                // partition key check that reads the column see it.
                part.push_str(" NOT NULL");
            } else {
                // `PRIMARY KEY` already implies both, and writing them as well
                // is refused by some parsers rather than being redundant.
                if !column.nullable {
                    part.push_str(" NOT NULL");
                }
                if column.unique {
                    part.push_str(" UNIQUE");
                }
            }
            if !column.default.trim().is_empty() {
                part.push_str(&format!(" DEFAULT {}", column.default.trim()));
            }
            part
        })
        .collect();

    let mut body = body;
    if composite_key {
        let key: Vec<String> = usable
            .iter()
            .filter(|c| c.primary_key)
            .map(|c| dialect::quote_ident(c.name.trim()))
            .collect();
        body.push(format!("PRIMARY KEY ({})", key.join(", ")));
    }

    let mut sql = format!(
        "CREATE TABLE {} ({})",
        dialect::quote_ident(name),
        body.join(", ")
    );
    if partitioning.enabled {
        sql.push_str(&partition_clause(columns, partitioning)?);
    }
    Ok(sql)
}

/// ` PARTITION BY HASH (…) [PARTITIONS n]`, checked against what the server
/// refuses that the dialog can see for itself. The rest — a key type that
/// cannot route, a unique key missing a key column — the server explains
/// better, and the dialog stays open to show it.
fn partition_clause(columns: &[NewColumn], partitioning: &Partitioning) -> Result<String, String> {
    if partitioning.key.is_empty() {
        return Err("pick the partition key: one to three columns".to_string());
    }
    if partitioning.key.len() > MAX_PARTITION_KEY {
        return Err(format!("a partition key has at most {} columns", MAX_PARTITION_KEY));
    }
    let mut key = Vec::new();
    for at in &partitioning.key {
        let Some(column) = columns.get(*at).filter(|c| !c.name.trim().is_empty()) else {
            return Err("a partition key column has no name".to_string());
        };
        if column.nullable && !column.primary_key {
            return Err(format!(
                "partition key column `{}` must be NOT NULL: a NULL could never be routed",
                column.name.trim()
            ));
        }
        key.push(dialect::quote_ident(column.name.trim()));
    }
    let mut clause = format!(" PARTITION BY HASH ({})", key.join(", "));
    let count = partitioning.count.trim();
    if !count.is_empty() {
        match count.parse::<u32>() {
            Ok(n) if (1..=2048).contains(&n) => clause.push_str(&format!(" PARTITIONS {}", n)),
            _ => return Err("PARTITIONS takes a whole number from 1 to 2048".to_string()),
        }
    }
    Ok(clause)
}

/// Builds a `CREATE INDEX` from the dialog.
///
/// The type rules — no B-tree over `CHAR`, nothing over `INTERVAL` — are left
/// to the server, which states them precisely and keeps them current; this
/// checks only that the form says what to index.
pub fn create_index_sql(table: &str, index: &NewIndex) -> Result<String, String> {
    if table.trim().is_empty() {
        return Err("no table is selected".to_string());
    }
    let name = match index.name.trim() {
        "" => String::new(),
        name => format!("{} ", dialect::quote_ident(name)),
    };
    let on = dialect::quote_ident(table.trim());

    match index.kind {
        IndexKind::Columns => {
            if index.columns.is_empty() {
                return Err("pick the columns to index, in key order".to_string());
            }
            let columns: Vec<String> = index.columns.iter().map(|c| dialect::quote_ident(c)).collect();
            Ok(format!(
                "CREATE {}INDEX {}ON {} {}({})",
                if index.unique { "UNIQUE " } else { "" },
                name,
                on,
                index.method.using().map(|m| format!("USING {} ", m)).unwrap_or_default(),
                columns.join(", ")
            ))
        }
        IndexKind::Document => {
            if index.document.trim().is_empty() {
                return Err("this table has no jsonb column to index".to_string());
            }
            Ok(format!(
                "CREATE INDEX {}ON {} USING gin ({}{})",
                name,
                on,
                dialect::quote_ident(index.document.trim()),
                if index.path_ops { " jsonb_path_ops" } else { "" }
            ))
        }
        IndexKind::Path => {
            if index.document.trim().is_empty() {
                return Err("this table has no jsonb column to index".to_string());
            }
            let keys: Vec<&str> = index.path.split('.').map(str::trim).collect();
            if keys.iter().any(|k| k.is_empty()) {
                return Err("the path needs a key, or keys separated by dots: status, user.id".to_string());
            }
            // Every step but the last reads a document (`->`); the last reads
            // text (`->>`), which is the only form the server will index.
            let mut expression = dialect::quote_ident(index.document.trim());
            for (at, key) in keys.iter().enumerate() {
                let arrow = if at + 1 == keys.len() { "->>" } else { "->" };
                expression.push_str(&format!("{}{}", arrow, dialect::quote_literal(key)));
            }
            Ok(format!("CREATE INDEX {}ON {} (({}))", name, on, expression))
        }
    }
}

/// Builds an `ALTER TABLE … ADD COLUMN`.
pub fn add_column_sql(table: &str, column: &NewColumn) -> Result<String, String> {
    if table.trim().is_empty() {
        return Err("no table is selected".to_string());
    }
    if column.name.trim().is_empty() {
        return Err("the column needs a name".to_string());
    }
    if column.data_type().trim().is_empty() {
        return Err("the column needs a type".to_string());
    }
    // Both engines refuse this on a populated table, for the same reason: there
    // is nothing to put in the rows that already exist. Saying so here beats
    // relaying the server's error after the dialog has gone.
    if !column.nullable && column.default.trim().is_empty() {
        return Err(
            "a NOT NULL column added to a table that already has rows needs a DEFAULT".to_string(),
        );
    }

    let mut sql = format!(
        "ALTER TABLE {} ADD COLUMN {} {}",
        dialect::quote_ident(table.trim()),
        dialect::quote_ident(column.name.trim()),
        column.data_type().trim()
    );
    if !column.nullable {
        sql.push_str(" NOT NULL");
    }
    if column.unique {
        sql.push_str(" UNIQUE");
    }
    if !column.default.trim().is_empty() {
        sql.push_str(&format!(" DEFAULT {}", column.default.trim()));
    }
    if !column.compression.trim().is_empty() {
        sql.push_str(&format!(" COMPRESSION {}", compression_method(&column.compression)?));
    }
    Ok(sql)
}

/// One `SHOW TABLE STATS` row's value, by metric.
pub fn stat_value(stats: &[Stat], metric: &str) -> Option<String> {
    stats.iter().find(|s| s.metric == metric).map(|s| s.value.trim().to_string())
}

/// `SHOW TABLE STATS`' `compressed_columns` (RasterizedDB), as (column,
/// method): `body=zstd(9) +dictionary, note=zstd` gives `("body", "zstd(9)")`
/// and `("note", "zstd")`. Empty for a table with none, or a server that
/// does not report it.
pub fn column_compressions(stats: &[Stat]) -> Vec<(String, String)> {
    let Some(list) = stat_value(stats, "compressed_columns") else { return Vec::new() };
    list.split(", ")
        .filter_map(|item| {
            let (column, method) = item.trim().split_once('=')?;
            let method = method.split_whitespace().next()?;
            Some((column.to_string(), method.to_string()))
        })
        .collect()
}

/// A compression method as the server takes it, lower-cased: `zstd`,
/// `zstd(1)` to `zstd(22)` (1 fastest, 22 smallest), `none`, `default`, and
/// PostgreSQL's `lz4` and `pglz`. Anything else is refused here, so the
/// dialog says so before a statement is sent.
pub fn compression_method(text: &str) -> Result<String, String> {
    let method = text.trim().to_ascii_lowercase().replace(' ', "");
    if matches!(method.as_str(), "zstd" | "none" | "default" | "lz4" | "pglz") {
        return Ok(method);
    }
    if let Some(level) = method.strip_prefix("zstd(").and_then(|r| r.strip_suffix(')'))
        && let Ok(n) = level.parse::<u8>()
        && (1..=22).contains(&n)
    {
        return Ok(format!("zstd({n})"));
    }
    Err(format!(
        "`{}` is not a compression method: zstd, zstd(1) to zstd(22), none or default",
        text.trim()
    ))
}

/// A table's row compression being changed in a dialog.
#[derive(Debug, Clone, PartialEq)]
pub struct RowCompressionEdit {
    /// What `SHOW TABLE STATS`' `row_compression` says (`zstd(6)`), `none`
    /// when off.
    pub original: String,
    /// Empty or `none` turns it off.
    pub setting: String,
    /// Rewrite the rows already there under the new setting.
    pub recompress: bool,
}

impl RowCompressionEdit {
    pub fn of(current: &str) -> Self {
        let current = current.split_whitespace().next().unwrap_or("none").to_string();
        RowCompressionEdit { setting: current.clone(), original: current, recompress: false }
    }

    /// The statements, as one script: the setting (if it changed), then the
    /// rewrite (if asked for).
    pub fn statements(&self, table: &str) -> Result<String, String> {
        let table = dialect::quote_ident(table.trim());
        let wanted = if self.setting.trim().is_empty() { "none".to_string() } else { compression_method(&self.setting)? };
        if matches!(wanted.as_str(), "lz4" | "pglz" | "default") {
            return Err("row compression is zstd or zstd(1) to zstd(22), or none to turn it off".to_string());
        }
        let mut statements = Vec::new();
        if !wanted.eq_ignore_ascii_case(self.original.trim()) {
            statements.push(if wanted == "none" {
                format!("ALTER TABLE {table} RESET (row_compression)")
            } else {
                format!("ALTER TABLE {table} SET (row_compression = '{wanted}')")
            });
        }
        if self.recompress {
            statements.push(format!("ALTER TABLE {table} RECOMPRESS"));
        }
        if statements.is_empty() {
            return Err("nothing is changed".to_string());
        }
        Ok(statements.join(";\n"))
    }
}

/// How many keys a scan asks for at a time.
const KV_PAGE: u32 = 500;

/// The KEYS tab's state.
#[derive(Debug, Clone, PartialEq)]
pub struct KvBrowser {
    /// A Redis glob: `*`, `user:*`, `session:??`.
    pub pattern: String,
    pub keys: Vec<String>,
    /// Where the scan goes on from; `None` once it has reached the end.
    pub cursor: Option<String>,
    /// How many keys the store holds, all patterns.
    pub size: Option<u64>,
    pub selected: Option<KvEntry>,
}

impl Default for KvBrowser {
    fn default() -> Self {
        KvBrowser {
            pattern: "*".to_string(),
            keys: Vec::new(),
            cursor: None,
            size: None,
            selected: None,
        }
    }
}

/// A key read for the KEYS tab: its type, time to live (`-1` for none), and
/// its value as rows (a string's one value, a hash's fields, a list's items).
#[derive(Debug, Clone, PartialEq)]
pub struct KvEntry {
    pub key: String,
    pub kind: String,
    pub ttl: i64,
    /// `None` for a type the tab does not show.
    pub value: Option<Grid>,
}

/// How often an open block in the editor is kept alive: well inside the
/// native server's default request timeout of 30 s.
const EDITOR_KEEPALIVE: Duration = Duration::from_secs(10);

/// How many rows of each result the editor keeps. The rest are counted, not
/// held: a `SELECT *` on a big table must not take the console's memory, nor
/// lay out a grid of a million rows.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum RowLimit {
    Thousand,
    #[default]
    TenThousand,
    HundredThousand,
    Unlimited,
}

impl RowLimit {
    pub const ALL: [RowLimit; 4] = [
        RowLimit::Thousand,
        RowLimit::TenThousand,
        RowLimit::HundredThousand,
        RowLimit::Unlimited,
    ];

    pub fn rows(self) -> Option<u64> {
        match self {
            RowLimit::Thousand => Some(1_000),
            RowLimit::TenThousand => Some(10_000),
            RowLimit::HundredThousand => Some(100_000),
            RowLimit::Unlimited => None,
        }
    }
}

impl std::fmt::Display for RowLimit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            RowLimit::Thousand => "1,000 rows",
            RowLimit::TenThousand => "10,000 rows",
            RowLimit::HundredThousand => "100,000 rows",
            RowLimit::Unlimited => "no row limit",
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The type and nullability go under the old name and the rename last,
    /// so every statement names a column that exists when it runs; an
    /// unchanged field writes nothing, and a type cannot carry a second
    /// statement.
    #[test]
    fn a_column_edit_is_written_in_an_order_that_runs() {
        let info = ColumnInfo {
            name: "amt".into(),
            data_type: "NUMERIC(12,1)".into(),
            nullable: true,
            ..ColumnInfo::default()
        };
        let mut edit = ColumnEdit::of(&info, "");
        assert_eq!(edit.statements("t"), Err("nothing is changed".to_string()));
        edit.data_type = " numeric(12,1) ".into();
        assert_eq!(edit.statements("t"), Err("nothing is changed".to_string()));
        edit.name = "amount".into();
        edit.data_type = "NUMERIC(14,2)".into();
        edit.nullable = false;
        assert_eq!(
            edit.statements("t").unwrap(),
            "ALTER TABLE \"t\" ALTER COLUMN \"amt\" TYPE NUMERIC(14,2);\n\
             ALTER TABLE \"t\" ALTER COLUMN \"amt\" SET NOT NULL;\n\
             ALTER TABLE \"t\" RENAME COLUMN \"amt\" TO \"amount\""
        );
        let mut only_null = ColumnEdit::of(&ColumnInfo { nullable: false, ..info.clone() }, "");
        only_null.nullable = true;
        assert_eq!(
            only_null.statements("t").unwrap(),
            "ALTER TABLE \"t\" ALTER COLUMN \"amt\" DROP NOT NULL"
        );
        for bad in ["BIGINT; DROP TABLE t", "BIGINT -- x", "", "  "] {
            let mut edit = ColumnEdit::of(&info, "");
            edit.data_type = bad.into();
            assert!(edit.statements("t").is_err(), "{bad}");
        }
    }

    /// Compression in the dialogs: what each takes, the statement it makes,
    /// and what it refuses before anything is sent.
    #[test]
    fn compression_reaches_the_column_and_table_statements() {
        assert_eq!(compression_method(" ZSTD ( 9 ) ").unwrap(), "zstd(9)");
        for ok in ["zstd", "none", "default", "lz4", "pglz", "zstd(1)", "zstd(22)"] {
            assert!(compression_method(ok).is_ok(), "{ok}");
        }
        for bad in ["zstd(0)", "zstd(23)", "gzip", "zstd(9); DROP TABLE t", ""] {
            assert!(compression_method(bad).is_err(), "{bad}");
        }

        // A new column.
        let mut c = column("body", "TEXT");
        c.compression = "zstd(9)".into();
        assert_eq!(add_column_sql("docs", &c).unwrap(), "ALTER TABLE \"docs\" ADD COLUMN \"body\" TEXT COMPRESSION zstd(9)");
        c.compression = "brotli".into();
        assert!(add_column_sql("docs", &c).is_err());

        // An existing column: only a change makes a statement, before a rename.
        let info = ColumnInfo { name: "note".into(), data_type: "TEXT".into(), nullable: true, ..ColumnInfo::default() };
        let mut edit = ColumnEdit::of(&info, "zstd");
        assert_eq!(edit.statements("docs").unwrap_err(), "nothing is changed");
        edit.compression = "zstd(19)".into();
        edit.name = "memo".into();
        assert_eq!(
            edit.statements("docs").unwrap(),
            "ALTER TABLE \"docs\" ALTER COLUMN \"note\" SET COMPRESSION zstd(19);\nALTER TABLE \"docs\" RENAME COLUMN \"note\" TO \"memo\""
        );
        edit.name = "note".into();
        edit.compression = String::new();
        assert_eq!(edit.statements("docs").unwrap(), "ALTER TABLE \"docs\" ALTER COLUMN \"note\" SET COMPRESSION default");

        // A table's row compression, and the rewrite.
        let mut row = RowCompressionEdit::of("zstd(6) +dictionary");
        assert_eq!(row.original, "zstd(6)");
        assert_eq!(row.statements("events").unwrap_err(), "nothing is changed");
        row.recompress = true;
        assert_eq!(row.statements("events").unwrap(), "ALTER TABLE \"events\" RECOMPRESS");
        row.setting = "none".into();
        assert_eq!(row.statements("events").unwrap(), "ALTER TABLE \"events\" RESET (row_compression);\nALTER TABLE \"events\" RECOMPRESS");
        let mut off = RowCompressionEdit::of("none");
        off.setting = "zstd".into();
        assert_eq!(off.statements("events").unwrap(), "ALTER TABLE \"events\" SET (row_compression = 'zstd')");
        off.setting = "lz4".into();
        assert!(off.statements("events").is_err(), "row compression is zstd only");

        // What SHOW TABLE STATS says, read back.
        let stats = vec![
            Stat { metric: "compressed_columns".into(), value: "body=zstd(9) +dictionary, note=zstd".into(), detail: String::new() },
            Stat { metric: "row_compression".into(), value: "none".into(), detail: String::new() },
        ];
        assert_eq!(
            column_compressions(&stats),
            vec![("body".to_string(), "zstd(9)".to_string()), ("note".to_string(), "zstd".to_string())]
        );
        assert!(column_compressions(&[]).is_empty());
    }

    fn column(name: &str, ty: &str) -> NewColumn {
        let mut column = NewColumn::new(Flavor::Rasterized);
        column.name = name.to_string();
        if let Some(choice) = types::lookup(ty) {
            column.choose(choice);
        }
        column
    }

    fn table(name: &str, primary_key: &str) -> TableInfo {
        TableInfo {
            name: name.to_string(),
            storage: "file".to_string(),
            row_slots: 0,
            columns: 0,
            data_bytes: 0,
            index_bytes: 0,
            total_bytes: 0,
            reclaimable_bytes: 0,
            primary_key: primary_key.to_string(),
            immutable: false,
            indexed: true,
        }
    }

    /// Selecting a table must not page it by the previous table's key.
    ///
    /// The page used to be requested in the same batch as the columns, so it
    /// read whichever columns were still loaded — the old table's. Moving from
    /// a table with an `id` to one without sent `SELECT * FROM "rdb_roles"
    /// ORDER BY "id"`, and the browser showed an error instead of the table.
    #[test]
    fn selecting_a_table_does_not_page_it_by_the_previous_key() {
        let (mut studio, _) = Studio::new(None);
        studio.tables = vec![table("users", "id"), table("rdb_roles", "")];
        studio.selected_table = Some("users".to_string());
        studio.columns = vec![ColumnInfo {
            position: 1,
            name: "id".into(),
            data_type: "BIGINT".into(),
            nullable: false,
            unique: true,
            primary_key: true,
            default: String::new(),
            indexed: true,
            json_table: String::new(),
        }];
        assert!(studio.row_key().is_some(), "the fixture should have a key to go stale");

        let _ = studio.update(Message::SelectTable("rdb_roles".to_string()));

        assert!(
            studio.row_key().is_none(),
            "the new table was keyed by the old table's column"
        );
        assert_eq!(
            dialect::page_sql(
                "rdb_roles",
                None,
                studio.row_key().as_ref().map(|k| k.column.as_str()),
                None,
                200,
                0
            ),
            "SELECT * FROM \"rdb_roles\" LIMIT 200 OFFSET 0",
            "a table with no key is paged unordered, not by a column it lacks"
        );
    }

    fn keyed(name: &str, data_type: &str) -> ColumnInfo {
        ColumnInfo {
            position: 1,
            name: name.into(),
            data_type: data_type.into(),
            nullable: false,
            unique: true,
            primary_key: true,
            indexed: true,
            ..ColumnInfo::default()
        }
    }

    fn page_of(ids: &[&str]) -> Grid {
        Grid {
            columns: vec!["id".into(), "label".into()],
            rows: ids
                .iter()
                .map(|id| vec![Some(id.to_string()), Some(format!("row {id}"))])
                .collect(),
            tag: format!("SELECT {}", ids.len()),
            total: None,
        }
    }

    /// Past the first page, a table ordered by its integer key is paged after
    /// the previous page's last key rather than at an offset; a late answer
    /// for a page no longer shown records nothing; another order, or a key
    /// whose type is not vouched for, stays at an offset.
    ///
    /// Mutation: record a page's end without checking it is the page shown
    /// (the late page-0 answer sets the cursor page 1 starts from).
    #[test]
    fn pages_past_the_first_start_after_the_last_key() {
        let (mut studio, _) = Studio::new(None);
        studio.tables = vec![table("users", "id")];
        studio.selected_table = Some("users".to_string());
        studio.columns = vec![keyed("id", "BIGINT")];
        studio.page_size = 3;

        let (first, signature) = studio.page_query().unwrap();
        assert_eq!(first, "SELECT * FROM \"users\" ORDER BY \"id\" LIMIT 3 OFFSET 0");
        let _ = studio.update(Message::DataLoaded(0, signature.clone(), Ok((page_of(&["1", "2", "5"]), Some(9)))));

        studio.data_page = 1;
        let (second, same) = studio.page_query().unwrap();
        assert_eq!(same, signature);
        assert_eq!(second, "SELECT * FROM \"users\" WHERE \"id\" > 5 ORDER BY \"id\" LIMIT 3");
        let _ = studio.update(Message::DataLoaded(1, signature.clone(), Ok((page_of(&["7", "8", "9"]), None))));

        // A late answer for page 0, arriving while page 1 is shown, is not
        // where page 1 starts.
        let _ = studio.update(Message::DataLoaded(0, signature.clone(), Ok((page_of(&["1", "2", "3"]), None))));
        assert_eq!(
            studio.data_keyset.after,
            vec![Some("5".to_string()), Some("9".to_string())]
        );
        studio.data_page = 2;
        assert!(studio.page_query().unwrap().0.contains("WHERE \"id\" > 9 "));

        // Back to page 1: after page 0's end again.
        studio.data_page = 1;
        assert!(studio.page_query().unwrap().0.contains("WHERE \"id\" > 5 "));

        // Descending by the key: before it.
        studio.sort.insert(
            format!("{}users", views::DATA_SCOPE),
            GridSort { column: "id".into(), descending: true },
        );
        studio.data_page = 0;
        let (_, descending) = studio.page_query().unwrap();
        assert_ne!(descending, signature, "a new order is a new signature");
        let _ = studio.update(Message::DataLoaded(0, descending, Ok((page_of(&["9", "8", "7"]), None))));
        studio.data_page = 1;
        assert_eq!(
            studio.page_query().unwrap().0,
            "SELECT * FROM \"users\" WHERE \"id\" < 7 ORDER BY \"id\" DESC LIMIT 3"
        );

        // Ordered by another column first: an offset.
        studio.columns.push(ColumnInfo { name: "label".into(), data_type: "TEXT".into(), position: 2, ..ColumnInfo::default() });
        studio.sort.insert(
            format!("{}users", views::DATA_SCOPE),
            GridSort { column: "label".into(), descending: false },
        );
        studio.data_page = 0;
        let (_, by_label) = studio.page_query().unwrap();
        let _ = studio.update(Message::DataLoaded(0, by_label, Ok((page_of(&["1", "2", "5"]), None))));
        studio.data_page = 1;
        assert!(studio.page_query().unwrap().0.ends_with("LIMIT 3 OFFSET 3"));

        // A UUID key: an offset.
        studio.sort.clear();
        studio.columns = vec![keyed("id", "UUID")];
        studio.data_page = 0;
        let (_, by_uuid) = studio.page_query().unwrap();
        let _ = studio.update(Message::DataLoaded(0, by_uuid, Ok((page_of(&["a", "b", "c"]), None))));
        studio.data_page = 1;
        assert!(studio.page_query().unwrap().0.ends_with("LIMIT 3 OFFSET 3"));
    }

    /// A skeleton `UPDATE` or `DELETE` with no `WHERE` is a whole-table write
    /// waiting for somebody to press run before finishing the thought.
    #[test]
    fn the_write_templates_always_carry_a_where() {
        let columns = vec![
            ColumnInfo {
                position: 1,
                name: "id".into(),
                data_type: "BIGINT".into(),
                nullable: false,
                unique: true,
                primary_key: true,
                default: String::new(),
                indexed: true,
                json_table: String::new(),
            },
            ColumnInfo {
                position: 2,
                name: "email".into(),
                data_type: "TEXT".into(),
                nullable: true,
                unique: false,
                primary_key: false,
                default: String::new(),
                indexed: false,
                json_table: String::new(),
            },
        ];

        let update = Template::Update.render("users", &columns, Some("id"));
        assert!(update.contains("WHERE \"id\" = ?"), "{update}");
        assert!(!update.contains("\"id\" = ?,"), "the key is not also assigned: {update}");

        let delete = Template::Delete.render("users", &columns, Some("id"));
        assert!(delete.contains("WHERE \"id\" = ?"), "{delete}");

        // Even with no key to match on, the clause is there to be filled in.
        let keyless = Template::Delete.render("users", &columns, None);
        assert!(keyless.contains("WHERE"), "{keyless}");
    }

    fn entry(sql: &str, outcome: RunOutcome) -> HistoryEntry {
        HistoryEntry {
            sql: sql.to_string(),
            elapsed: Duration::from_micros(4300),
            at: Instant::now(),
            outcome,
        }
    }

    /// An exported script that re-runs the session's failures is a rake to
    /// step on. They belong in the file, commented.
    #[test]
    fn exported_history_comments_out_what_failed() {
        let history = vec![
            entry("SELECT 1", RunOutcome::Ok { statements: 1, rows: 1, affected: 0 }),
            entry("DROP TABLE nope", RunOutcome::Failed("relation does not exist".into())),
        ];
        let script = history_as_sql(&history);

        assert!(script.contains("-- 1 row · 4.30 ms\nSELECT 1;\n"), "{script}");
        assert!(script.contains("-- FAILED: relation does not exist"), "{script}");
        assert!(
            !script.contains("\nDROP TABLE nope"),
            "the failed statement must not be runnable: {script}"
        );
        assert!(script.contains("-- DROP TABLE nope"), "{script}");
    }

    /// Exactly one terminator, wherever the original had none or several.
    #[test]
    fn exported_statements_are_terminated_once() {
        for sql in ["SELECT 1", "SELECT 1;", "SELECT 1;  ", "SELECT 1;\n"] {
            let script = history_as_sql(&[entry(
                sql,
                RunOutcome::Ok { statements: 1, rows: 0, affected: 0 },
            )]);
            assert!(script.contains("SELECT 1;\n"), "{sql:?} produced {script:?}");
            assert!(!script.contains("SELECT 1;;"), "{sql:?} produced {script:?}");
        }
    }

    /// A multi-line failure comments out every line, not just the first.
    #[test]
    fn a_multi_line_failure_is_fully_commented() {
        let script = history_as_sql(&[entry(
            "UPDATE t\nSET a = 1\nWHERE b = 2",
            RunOutcome::Failed("nope".into()),
        )]);
        for line in ["-- UPDATE t", "-- SET a = 1", "-- WHERE b = 2"] {
            assert!(script.contains(line), "{line} missing from {script}");
        }
    }

    /// The import statement carries the source's password as a literal, and
    /// the history is a list somebody can read on screen and export to a file.
    #[test]
    fn an_import_does_not_file_the_password_in_the_history() {
        let sql = "IMPORT DATABASE FROM POSTGRES HOST 'db' PORT 5432 DATABASE 'shop' \
                   USER 'u' PASSWORD 'hunter2' SSLMODE 'require' INTO shop";
        let redacted = redact_password(sql);
        assert!(!redacted.contains("hunter2"), "{redacted}");
        assert!(redacted.contains("PASSWORD '<redacted>'"), "{redacted}");
        assert!(redacted.ends_with("INTO shop"), "the rest survives: {redacted}");
    }

    /// A password containing a quote is escaped by doubling it, and the
    /// redaction has to end at the real closing quote rather than the first
    /// half of the escape.
    #[test]
    fn redaction_understands_an_escaped_quote() {
        let sql = "PASSWORD 'it''s secret' SSLMODE 'require' INTO shop";
        let redacted = redact_password(sql);
        assert!(!redacted.contains("secret"), "{redacted}");
        assert!(redacted.contains("SSLMODE 'require' INTO shop"), "{redacted}");
    }

    #[test]
    fn redaction_leaves_other_statements_alone() {
        assert_eq!(redact_password("SELECT 1"), "SELECT 1");
    }

    #[test]
    fn a_select_template_names_the_columns() {
        let columns = vec![ColumnInfo {
            position: 1,
            name: "id".into(),
            data_type: "BIGINT".into(),
            nullable: false,
            unique: true,
            primary_key: true,
            default: String::new(),
            indexed: true,
            json_table: String::new(),
        }];
        let sql = Template::Select.render("users", &columns, Some("id"));
        assert_eq!(sql, "SELECT \"id\"\nFROM \"users\"\nLIMIT 100;");
        // A table whose columns are not loaded yet still gets a usable query.
        assert!(Template::Select.render("users", &[], None).contains("SELECT *"));
    }

    #[test]
    fn a_create_table_quotes_its_names() {
        let sql = create_table_sql(
            "users",
            &[
                NewColumn { primary_key: true, ..column("id", "BIGINT") },
                NewColumn { nullable: false, unique: true, ..column("email", "TEXT") },
            ],
        )
        .unwrap();
        assert_eq!(
            sql,
            "CREATE TABLE \"users\" (\"id\" BIGINT PRIMARY KEY, \"email\" TEXT NOT NULL UNIQUE)"
        );
    }

    /// `PRIMARY KEY NOT NULL UNIQUE` is refused by parsers that accept each
    /// alone, so the redundant pair must not be emitted.
    #[test]
    fn a_primary_key_does_not_also_say_not_null_and_unique() {
        let sql = create_table_sql(
            "t",
            &[NewColumn { primary_key: true, nullable: false, unique: true, ..column("id", "BIGINT") }],
        )
        .unwrap();
        assert_eq!(sql, "CREATE TABLE \"t\" (\"id\" BIGINT PRIMARY KEY)");
    }

    #[test]
    fn the_shapes_the_server_would_refuse_are_caught_first() {
        assert!(create_table_sql("", &[column("a", "TEXT")]).is_err());
        assert!(create_table_sql("t", &[]).is_err());
        // Every column always has a type now — the picker cannot be empty — so
        // the check that remains is the duplicate one below.
        assert!(create_table_sql("t", &[column("a", "TEXT"), column("A", "TEXT")])
            .unwrap_err()
            .contains("twice"));
    }

    /// Two key columns are one composite key, not an error: a partitioned
    /// table needs one whenever its key is not its whole partition key.
    #[test]
    fn two_key_columns_make_a_composite_primary_key() {
        let sql = create_table_sql(
            "t",
            &[
                NewColumn { primary_key: true, ..column("a", "BIGINT") },
                NewColumn { primary_key: true, ..column("b", "TEXT") },
            ],
        )
        .unwrap();
        assert_eq!(
            sql,
            "CREATE TABLE \"t\" (\"a\" BIGINT NOT NULL, \"b\" TEXT NOT NULL, PRIMARY KEY (\"a\", \"b\"))"
        );
    }

    #[test]
    fn a_partitioned_table_names_its_key_in_pick_order() {
        let columns = [
            NewColumn { primary_key: true, ..column("tenant_id", "BIGINT") },
            NewColumn { primary_key: true, ..column("id", "BIGINT") },
            NewColumn { nullable: false, ..column("group_id", "BIGINT") },
        ];
        let partitioning = Partitioning { enabled: true, key: vec![0, 2], count: "128".into() };
        let sql = create_table_with("assets", &columns, &partitioning).unwrap();
        assert!(
            sql.ends_with(") PARTITION BY HASH (\"tenant_id\", \"group_id\") PARTITIONS 128"),
            "{sql}"
        );
        // Blank count: the server's default, not `PARTITIONS `.
        let partitioning = Partitioning { count: String::new(), ..partitioning };
        assert!(create_table_with("assets", &columns, &partitioning).unwrap().ends_with("\"group_id\")"));
    }

    #[test]
    fn a_partition_key_the_server_would_refuse_is_caught_in_the_dialog() {
        let nullable = [column("k", "BIGINT")];
        let on = |key: Vec<usize>, count: &str| Partitioning { enabled: true, key, count: count.into() };
        assert!(create_table_with("t", &nullable, &on(vec![0], "8")).unwrap_err().contains("NOT NULL"));
        let fine = [NewColumn { nullable: false, ..column("k", "BIGINT") }];
        assert!(create_table_with("t", &fine, &on(vec![], "8")).is_err());
        assert!(create_table_with("t", &fine, &on(vec![0], "0")).is_err());
        assert!(create_table_with("t", &fine, &on(vec![0], "4096")).is_err());
        assert!(create_table_with("t", &fine, &on(vec![0], "2048")).is_ok());
        // Switched off, a picked key is not written.
        let off = Partitioning { enabled: false, ..on(vec![0], "8") };
        assert!(!create_table_with("t", &fine, &off).unwrap().contains("PARTITION"));
    }

    #[test]
    fn an_index_is_written_the_way_each_kind_is_declared() {
        let mut index = NewIndex::new("doc".into());
        assert!(create_index_sql("orders", &index).is_err(), "no columns picked");

        index.columns = vec!["customer_id".into(), "placed_at".into()];
        assert_eq!(
            create_index_sql("orders", &index).unwrap(),
            "CREATE INDEX ON \"orders\" (\"customer_id\", \"placed_at\")"
        );
        index.method = IndexMethod::Hash;
        index.name = "by_customer".into();
        index.unique = true;
        assert_eq!(
            create_index_sql("orders", &index).unwrap(),
            "CREATE UNIQUE INDEX \"by_customer\" ON \"orders\" USING hash (\"customer_id\", \"placed_at\")"
        );

        index = NewIndex { kind: IndexKind::Document, path_ops: true, ..NewIndex::new("doc".into()) };
        assert_eq!(
            create_index_sql("orders", &index).unwrap(),
            "CREATE INDEX ON \"orders\" USING gin (\"doc\" jsonb_path_ops)"
        );

        index = NewIndex { kind: IndexKind::Path, path: "user.id".into(), ..NewIndex::new("doc".into()) };
        assert_eq!(
            create_index_sql("orders", &index).unwrap(),
            "CREATE INDEX ON \"orders\" ((\"doc\"->'user'->>'id'))"
        );
        // A key with a quote in it stays inside its literal.
        index.path = "it's".into();
        assert!(create_index_sql("orders", &index).unwrap().contains("->>'it''s'"));
        index.path = "a..b".into();
        assert!(create_index_sql("orders", &index).is_err());
        index.document.clear();
        index.path = "a".into();
        assert!(create_index_sql("orders", &index).is_err());
    }

    /// The server refuses this too, but only after the dialog has closed and
    /// taken the user's input with it.
    #[test]
    fn a_not_null_column_with_no_default_is_refused_before_it_is_sent() {
        let column = NewColumn { nullable: false, ..column("added", "TEXT") };
        assert!(add_column_sql("users", &column).unwrap_err().contains("DEFAULT"));

        let with_default = NewColumn { default: "''".to_string(), ..column };
        assert_eq!(
            add_column_sql("users", &with_default).unwrap(),
            "ALTER TABLE \"users\" ADD COLUMN \"added\" TEXT NOT NULL DEFAULT ''"
        );
    }

    /// The parameters the picker collects have to reach the DDL.
    #[test]
    fn a_parameterised_type_carries_its_parameters_into_the_ddl() {
        let mut money = column("total", "NUMERIC");
        money.precision = "12".to_string();
        money.scale = "2".to_string();
        assert_eq!(
            create_table_sql("invoice", &[money]).unwrap(),
            "CREATE TABLE \"invoice\" (\"total\" NUMERIC(12, 2))"
        );
    }

    /// Typing a name guesses a type, and choosing one stops it guessing.
    #[test]
    fn the_type_is_guessed_until_it_is_chosen() {
        let mut column = NewColumn::new(Flavor::Postgres);
        column.rename("created_at".to_string(), Flavor::Postgres);
        assert_eq!(column.choice.0.name, "TIMESTAMPTZ");

        column.choose(types::lookup("TEXT").unwrap());
        column.rename("updated_at".to_string(), Flavor::Postgres);
        assert_eq!(
            column.choice.0.name, "TEXT",
            "a chosen type must survive the column being renamed"
        );
    }

    /// Switching type must not carry the previous type's parameters over.
    #[test]
    fn choosing_a_type_resets_its_parameters() {
        let mut column = NewColumn::new(Flavor::Postgres);
        column.choose(types::lookup("NUMERIC").unwrap());
        assert_eq!(column.data_type(), "NUMERIC(12, 2)");

        column.choose(types::lookup("VARCHAR").unwrap());
        assert_eq!(column.data_type(), "VARCHAR(255)");

        column.choose(types::lookup("TEXT").unwrap());
        assert_eq!(column.data_type(), "TEXT");
    }

    /// A key column starts as what that server actually keys tables with.
    #[test]
    fn the_key_column_matches_the_server() {
        assert_eq!(NewColumn::key(Flavor::Postgres).choice.0.name, "BIGSERIAL");
        assert_eq!(NewColumn::key(Flavor::Rasterized).choice.0.name, "BIGINT");
        assert!(NewColumn::key(Flavor::Postgres).primary_key);
        assert!(!NewColumn::key(Flavor::Postgres).nullable);
    }
}

/// Each row's first cell, as text: the answer of a statement that returns
/// lines (`SHOW LEDGER`, `VERIFY LEDGER`, `VAULT … LIST`).
#[cfg(feature = "enterprise")]
fn first_column(grid: &Grid) -> Vec<String> {
    grid.rows.iter().map(|r| r.first().cloned().flatten().unwrap_or_default()).collect()
}
