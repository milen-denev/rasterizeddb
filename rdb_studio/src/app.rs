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
    pub editing: Option<CellEdit>,
    pub inserting: Option<Vec<String>>,
    /// Column widths the user has dragged to, keyed by table and column so two
    /// tables do not share one.
    pub column_widths: HashMap<String, f32>,
    pub resizing: Option<Resize>,
    /// Where the data grid is scrolled to, and how tall its window is, so it
    /// only builds the rows that can be seen. `None` until the first scroll.
    pub data_viewport: Option<(f32, f32)>,

    pub editor: text_editor::Content,
    pub results: Vec<Outcome>,
    pub query_error: Option<String>,
    pub history: Vec<HistoryEntry>,
    /// The statement that is in flight, waiting for its outcome before it is
    /// worth filing in the history.
    pub pending_sql: Option<String>,
    /// How long the last run took, for the results header.
    pub last_elapsed: Option<Duration>,

    pub modal: Option<Modal>,

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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Tab {
    Dashboard,
    Overview,
    Tables,
    Schema,
    Data,
    Query,
    Backup,
    Storage,
    Replication,
    Migrate,
    Config,
}

impl Tab {
    pub const ALL: [Tab; 11] = [
        Tab::Dashboard,
        Tab::Overview,
        Tab::Tables,
        Tab::Schema,
        Tab::Data,
        Tab::Query,
        Tab::Backup,
        // Next to BACKUP rather than next to CONFIG: both pages are about where
        // the bytes are and what happens when a disk goes, and an operator
        // opening one usually wants the other.
        Tab::Storage,
        Tab::Replication,
        Tab::Migrate,
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
            Tab::Backup => "BACKUP",
            Tab::Storage => "STORAGE",
            Tab::Replication => "REPLICATION",
            Tab::Migrate => "MIGRATE",
            Tab::Config => "CONFIG",
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
    CreateTable { name: String, columns: Vec<NewColumn>, error: Option<String> },
    CreateDatabase { name: String, error: Option<String> },
    AddColumn { table: String, column: NewColumn, error: Option<String> },
    Confirm { title: String, body: String, statement: String, refresh: Refresh },
    /// A restore, which reports rows rather than a count and therefore cannot
    /// go through the ordinary confirmation path.
    ConfirmRestore { body: String, statement: String },
    /// An import, likewise — and it reports a whole document.
    ConfirmImport { body: String, statement: String },
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

/// What to reload after a statement succeeds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Refresh {
    Tables,
    Databases,
    CurrentTable,
    Backups,
    Replication,
    Storage,
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
    FormSavePassword(bool),
    LoadProfile(usize),
    SaveProfile,
    DeleteProfile(usize),
    Connect,
    Connected(Result<Session, String>),
    FlavorDetected(Flavor),
    Disconnect,

    // --- navigation
    SelectTab(Tab),
    SelectTable(String),
    SelectDatabase(String),
    TableFilter(String),
    RefreshAll,

    // --- loading
    StatsLoaded(Result<Vec<Stat>, String>),
    TablesLoaded(Result<Vec<TableInfo>, String>),
    DatabasesLoaded(Result<Vec<DatabaseInfo>, String>),
    TableStatsLoaded(Result<Vec<Stat>, String>),
    ColumnsLoaded(Result<Vec<ColumnInfo>, String>),
    SchemaLoaded(Result<SchemaGraph, String>),
    ConfigLoaded(Result<Vec<ConfigEntry>, String>),
    DataLoaded(Result<(Grid, Option<u64>), String>),

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
    RunQuery,
    RunSelection,
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
    FileSaved(Result<Option<std::path::PathBuf>, String>),
    Copied(String),
    LoadHistory(usize),
    RunHistory(usize),
    CopyHistory(usize),
    DeleteHistory(usize),
    ClearHistory,
    QueryFinished(Result<Vec<Outcome>, String>, Duration),

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
    ModalRemoveColumn(usize),
    ModalColumnField(usize, ModalField, String),
    ModalColumnToggle(usize, ModalField, bool),
    ModalType(types::Choice),
    ModalColumnType(usize, types::Choice),
    ModalSubmit,
    ModalCancel,
    Confirm { title: String, body: String, statement: String, refresh: Refresh },
    StatementFinished(Refresh, String, Duration, Result<String, String>),
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
    CreateTable,
    CreateDatabase,
    AddColumn,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ModalField {
    Name,
    Length,
    Precision,
    Scale,
    Nullable,
    PrimaryKey,
    Unique,
    Default,
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
            editing: None,
            inserting: None,
            column_widths: HashMap::new(),
            resizing: None,
            data_viewport: None,
            editor: text_editor::Content::with_text(
                "-- Ctrl+Enter runs everything in this editor.\n\
                 -- Several statements are fine; each reports separately.\n\
                 SHOW TABLES;\n",
            ),
            results: Vec::new(),
            query_error: None,
            history: Vec::new(),
            pending_sql: None,
            last_elapsed: None,
            modal: None,
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
            Task::perform(conn::connect(profile), Message::Connected)
        } else {
            Task::none()
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
        if self.session.is_none() || !self.live || self.tab != Tab::Dashboard {
            return Subscription::none();
        }
        iced::time::every(Duration::from_secs(self.interval.max(1))).map(|_| Message::Tick)
    }

    pub fn view(&self) -> Element<'_, Message> {
        views::view(self)
    }

    /// The dialect in play, defaulting to RasterizedDB before a connection
    /// exists so the connection screen has something to show.
    pub fn flavor(&self) -> Flavor {
        self.session.as_ref().map(|s| s.flavor).unwrap_or(self.form.flavor)
    }

    /// The tables the sidebar shows, after the filter box.
    pub fn filtered_tables(&self) -> Vec<&TableInfo> {
        let needle = self.table_filter.to_ascii_lowercase();
        self.tables
            .iter()
            .filter(|t| needle.is_empty() || t.name.to_ascii_lowercase().contains(&needle))
            .collect()
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
                self.form.flavor = v;
                // The two default to different ports often enough that
                // switching flavour while the port is still the other's
                // default is almost always a mistake being made.
                Task::none()
            }
            Message::FormTls(v) => { self.form.tls = v; Task::none() }
            Message::FormSavePassword(v) => { self.form.save_password = v; Task::none() }

            Message::LoadProfile(index) => {
                if let Some(profile) = self.profiles.get(index) {
                    self.form = profile.clone();
                }
                Task::none()
            }
            Message::SaveProfile => {
                match self.profiles.iter().position(|p| p.name == self.form.name) {
                    Some(index) => self.profiles[index] = self.form.clone(),
                    None => self.profiles.push(self.form.clone()),
                }
                self.status = match profiles::save(&self.profiles) {
                    Ok(()) => Status::Done(format!("saved profile `{}`", self.form.name)),
                    Err(e) => Status::Failed(e),
                };
                Task::none()
            }
            Message::DeleteProfile(index) => {
                if index < self.profiles.len() {
                    self.profiles.remove(index);
                    let _ = profiles::save(&self.profiles);
                }
                Task::none()
            }

            Message::Connect => {
                self.connecting = true;
                self.connect_error = None;
                self.status = Status::Busy(format!("connecting to {}", self.form.host));
                let profile = self.form.clone();
                Task::perform(conn::connect(profile), Message::Connected)
            }
            Message::Connected(Ok(session)) => {
                self.connecting = false;
                self.session = Some(session.clone());
                self.status = Status::Done(format!(
                    "connected to {} ({})",
                    session.server,
                    if session.encrypted { "TLS" } else { "cleartext" }
                ));
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
                self.session = None;
                self.flavor_warning = None;
                self.backup = None;
                self.replication = None;
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
                match tab {
                    // Sampled on arrival so the page is not blank for a tick.
                    Tab::Dashboard if self.live => self.sample(),
                    // Loaded on arrival rather than up front: the schema map
                    // and the configuration are each a round trip nobody
                    // needs until they open the tab.
                    Tab::Schema if self.schema.tables.is_empty() => self.load_schema(),
                    Tab::Config if self.config.is_empty() => self.load_config(),
                    Tab::Data if self.data.columns.is_empty() => self.load_page(),
                    // Always reloaded rather than cached: a backup catalogue
                    // that is minutes stale is the one thing on this page that
                    // must not be, since the whole point of opening it is
                    // usually that something has gone wrong.
                    Tab::Backup => self.load_backups(),
                    // Same reasoning as the backup catalogue: a stale answer on
                    // this page is worse than a slow one.
                    Tab::Replication => self.load_replication(),
                    // And again: free space is the figure this page exists to
                    // report, and a cached one is worse than useless.
                    Tab::Storage => self.load_storage(),
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
                    self.editing = None;
                    self.inserting = None;
                    // The previous table's columns are not this one's. Left in
                    // place, `row_key` reads them and pages the new table by a
                    // column that belongs to the old one — `SELECT * FROM
                    // "rdb_roles" ORDER BY "id"`, which has no `id`, and the
                    // whole browser reports an error for the table.
                    self.columns.clear();
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
                self.absorb(result, |studio, stats| studio.table_stats = stats)
            }
            Message::ColumnsLoaded(result) => {
                // The page waits for this: `row_key` needs the columns of the
                // table being paged, not of whichever one was open before.
                // Loaded even when the columns failed, since an unordered page
                // is better than none — `row_key` then finds no key and the
                // query goes out without an ORDER BY.
                let task = self.absorb(result, |studio, columns| studio.columns = columns);
                Task::batch([task, self.load_page()])
            }
            Message::SchemaLoaded(result) => self.absorb(result, |studio, schema| {
                studio.schema = schema;
                studio.schema_pan = iced::Vector::ZERO;
                studio.schema_zoom = 1.0;
                studio.schema_cache.clear();
            }),
            Message::ConfigLoaded(result) => self.absorb(result, |studio, config| {
                studio.config = config;
                studio.config_edits.clear();
            }),
            Message::DataLoaded(result) => self.absorb(result, |studio, (grid, total)| {
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

            // ---- replication --------------------------------------------------
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
            Message::EditorAction(action) => { self.editor.perform(action); Task::none() }
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
                let rendered = crate::export::render(grid, format, &table);
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
                let contents = crate::export::render(grid, format, &table);
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

            Message::QueryFinished(result, elapsed) => {
                let sql = self.pending_sql.take().unwrap_or_default();
                match result {
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
                    ModalKind::CreateTable => Modal::CreateTable {
                        name: String::new(),
                        columns: vec![NewColumn::key(self.flavor())],
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
                });
                Task::none()
            }
            Message::ModalCancel => { self.modal = None; Task::none() }
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
                if let Some(Modal::CreateTable { columns, .. }) = &mut self.modal
                    && index < columns.len()
                    && columns.len() > 1
                {
                    columns.remove(index);
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
                    ModalField::PrimaryKey => column.primary_key = value,
                    ModalField::Unique => column.unique = value,
                    _ => {}
                });
                Task::none()
            }
            Message::ModalSubmit => self.submit_modal(),

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
                match refresh {
                    Refresh::Tables => Task::batch([self.load_tables(), self.load_schema()]),
                    Refresh::Databases => self.load_databases(),
                    Refresh::CurrentTable => Task::batch([
                        self.load_page(),
                        self.load_table_stats(),
                        self.load_columns(),
                    ]),
                    Refresh::Backups => self.load_backups(),
                    Refresh::Replication => self.load_replication(),
                    Refresh::Storage => self.load_storage(),
                }
            }
            Message::StatementFinished(_, sql, elapsed, Err(e)) => {
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

    /// Reads node state and the peer list in one task.
    ///
    /// Both go through the same "did the server understand the statement"
    /// check as the backup page — an older build answers an unknown `SHOW x`
    /// with an empty column named after it rather than refusing it, and that
    /// parses into a status result that says nothing while looking like one.
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
                Box::new(StorageSnapshot { stats, supported, locations, placements })
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
                    "This keeps the newest {retained} snapshots and permanently deletes the rest                      from the bucket, along with the log bundles only they needed.\n\n                     {older} snapshot{} would go. Objects deleted from S3 are not recoverable, and                      any point-in-time target that depended on them stops being reachable.",
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
                    "This copies the whole database to a fresh snapshot and, once that has                      landed, permanently deletes every earlier generation — the older                      snapshots and the {segments} log bundle{} they needed.

                     Nothing is deleted unless the new snapshot succeeds, so a rollover that                      fails leaves the current backup untouched. Afterwards the bucket holds                      one self-contained backup, and point-in-time targets before it stop                      being reachable.",
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
        let key = self.row_key();
        let sql = dialect::page_sql(
            &table,
            key.as_ref().map(|k| k.column.as_str()),
            self.page_size,
            self.data_page * self.page_size,
        );
        let count_sql = (self.data_total.is_none()).then(|| dialect::count_sql(&table));

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
            Message::DataLoaded,
        )
    }

    // -- writing -------------------------------------------------------------

    fn run_sql(&mut self, sql: String) -> Task<Message> {
        let Some(session) = self.session.clone() else { return Task::none() };
        if sql.trim().is_empty() {
            return Task::none();
        }
        self.status = Status::Busy("running".to_string());
        // Held rather than pushed: the entry is only worth keeping once it has
        // an outcome and a duration to keep with it.
        self.pending_sql = Some(sql.clone());

        Task::perform(
            async move {
                // Round trip, measured around the whole exchange. It includes
                // the network, so it is never labelled as the server's time.
                let started = Instant::now();
                let result = session.run(&sql).await;
                (result, started.elapsed())
            },
            |(result, elapsed)| Message::QueryFinished(result, elapsed),
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
            Some(Modal::AddColumn { column, .. }) => match field {
                ModalField::Name => column.rename(value, flavor),
                ModalField::Length => column.length = digits(&value),
                ModalField::Precision => column.precision = digits(&value),
                ModalField::Scale => column.scale = digits(&value),
                ModalField::Default => column.default = value,
                _ => {}
            },
            _ => {}
        }
    }

    fn modal_toggle(&mut self, field: ModalField, value: bool) {
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
        let Some(modal) = self.modal.take() else { return Task::none() };
        match modal {
            Modal::Confirm { statement, refresh, .. } => self.statement(statement, refresh),

            Modal::ConfirmImport { statement, .. } => self.start_import(statement),

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
                self.statement(sql, Refresh::Databases)
            }

            Modal::CreateTable { name, columns, .. } => {
                match create_table_sql(&name, &columns) {
                    Ok(sql) => self.statement(sql, Refresh::Tables),
                    Err(e) => {
                        self.modal = Some(Modal::CreateTable { name, columns, error: Some(e) });
                        Task::none()
                    }
                }
            }

            Modal::AddColumn { table, column, .. } => match add_column_sql(&table, &column) {
                Ok(sql) => self.statement(sql, Refresh::CurrentTable),
                Err(e) => {
                    self.modal = Some(Modal::AddColumn { table, column, error: Some(e) });
                    Task::none()
                }
            },
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
        "-- rdb_studio session history\n         -- Statements that failed are commented out.\n\n",
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
    if usable.iter().filter(|c| c.primary_key).count() > 1 {
        return Err("only one column may be the primary key".to_string());
    }

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
            if column.primary_key {
                part.push_str(" PRIMARY KEY");
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

    Ok(format!(
        "CREATE TABLE {} ({})",
        dialect::quote_ident(name),
        body.join(", ")
    ))
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
    Ok(sql)
}

#[cfg(test)]
mod tests {
    use super::*;

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
                studio.row_key().as_ref().map(|k| k.column.as_str()),
                200,
                0
            ),
            "SELECT * FROM \"rdb_roles\" LIMIT 200 OFFSET 0",
            "a table with no key is paged unordered, not by a column it lacks"
        );
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
        assert!(
            create_table_sql(
                "t",
                &[
                    NewColumn { primary_key: true, ..column("a", "TEXT") },
                    NewColumn { primary_key: true, ..column("b", "TEXT") },
                ]
            )
            .is_err()
        );
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
