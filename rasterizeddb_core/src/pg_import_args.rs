//! Command-line surface for importing a PostgreSQL database.
//!
//! Kept out of `main.rs` for the same reason the backup flags are: the flags,
//! their defaults, and the rules for resolving them against libpq's environment
//! variables belong in one place, and a build without the `pg_import` feature
//! carries none of it.

use clap::Args;

use rasterizeddb_core::core::migrate::pg_client::{ConnectParams, SslMode};
use rasterizeddb_core::core::migrate::plan::{ImportOptions, WideNumeric};
use rasterizeddb_core::core::migrate::types::JsonMapping;

#[derive(Args, Debug, Clone)]
pub struct PgImportArgs {
    /// Name for the new database this import builds. Setting it is what runs
    /// an import; the server does not start afterwards unless
    /// `--pg-import-and-serve` is given.
    #[arg(long = "pg-import-into", value_name = "NAME")]
    pub into: Option<String>,

    /// Source connection URI, as in
    /// `postgresql://user:password@host:5432/dbname?sslmode=require`.
    ///
    /// The flags below override anything it says, so a URI from an
    /// application's configuration can be used with a different password.
    #[arg(long = "pg-import-from", value_name = "URI")]
    pub uri: Option<String>,

    /// Source host (default: $PGHOST, else localhost).
    #[arg(long = "pg-import-host", value_name = "HOST")]
    pub host: Option<String>,

    /// Source port (default: $PGPORT, else 5432).
    #[arg(long = "pg-import-port", value_name = "PORT")]
    pub port: Option<u16>,

    /// Source user (default: $PGUSER, else postgres).
    #[arg(long = "pg-import-user", value_name = "USER")]
    pub user: Option<String>,

    /// Password for that user.
    ///
    /// Prefer `$PGPASSWORD`, which this falls back to: a password on the
    /// command line is visible in `ps` to every other user on the machine.
    #[arg(long = "pg-import-password", value_name = "PASSWORD")]
    pub password: Option<String>,

    /// Source database (default: $PGDATABASE).
    #[arg(long = "pg-import-database", value_name = "NAME")]
    pub database: Option<String>,

    /// How the connection is protected: disable, prefer (default), require or
    /// verify-full.
    ///
    /// `require` encrypts but does not check who the server is — libpq's
    /// meaning of the word. Across anything but a trusted network, use
    /// `verify-full`.
    #[arg(long = "pg-import-sslmode", value_name = "MODE")]
    pub sslmode: Option<String>,

    /// Schema to read (default: public).
    #[arg(
        long = "pg-import-schema",
        value_name = "NAME",
        default_value = "public"
    )]
    pub schema: String,

    /// Import only these tables. Repeatable, and accepts a comma-separated list.
    #[arg(
        long = "pg-import-only-tables",
        value_name = "TABLE,…",
        value_delimiter = ','
    )]
    pub only_tables: Vec<String>,

    /// Leave these tables out. Repeatable, and accepts a comma-separated list.
    #[arg(
        long = "pg-import-exclude-tables",
        value_name = "TABLE,…",
        value_delimiter = ','
    )]
    pub exclude_tables: Vec<String>,

    /// Rows per transaction on the way in (default: 5000).
    #[arg(
        long = "pg-import-batch-rows",
        value_name = "N",
        default_value_t = 5_000
    )]
    pub batch_rows: usize,

    /// What `json` and `jsonb` columns become: `auto` (default), which uses
    /// this engine's JSONB column wherever every document in that column is an
    /// object and TEXT elsewhere; `text`, which always keeps the document
    /// exactly as written; or `native`, which always uses the JSONB column and
    /// refuses any row whose document is not an object.
    ///
    /// `auto` is the default because a `jsonb` column carried over as TEXT is
    /// refused by every typed driver — the column an application wrote as JSON
    /// becomes one it cannot read.
    #[arg(
        long = "pg-import-json",
        value_name = "auto|text|native",
        default_value = "auto"
    )]
    pub json: String,

    /// What to do with a `numeric(p, s)` finer than the nine decimal places
    /// this engine keeps: `fail` (default) or `text`, which keeps every digit
    /// but stops the column being a number.
    #[arg(
        long = "pg-import-wide-numeric",
        value_name = "fail|text",
        default_value = "fail"
    )]
    pub wide_numeric: String,

    /// Import the rows without the primary keys, unique constraints and
    /// foreign keys the source enforces.
    #[arg(long = "pg-import-without-constraints", default_value_t = false)]
    pub without_constraints: bool,

    /// Report what would be imported, and what would not come across
    /// unchanged, without creating anything.
    #[arg(long = "pg-import-dry-run", default_value_t = false)]
    pub dry_run: bool,

    /// Start the server after the import instead of exiting.
    #[arg(long = "pg-import-and-serve", default_value_t = false)]
    pub and_serve: bool,
}

impl PgImportArgs {
    /// Whether an import was asked for.
    pub fn wants_import(&self) -> bool {
        self.into.is_some()
    }

    /// Where to connect, resolved against libpq's environment variables.
    pub fn connection(&self) -> Result<ConnectParams, String> {
        let mut params = match &self.uri {
            Some(uri) => ConnectParams::parse_uri(uri)?,
            None => ConnectParams::default(),
        };

        // A flag beats the URI; the URI beats the environment; the environment
        // beats the default. Same order libpq resolves in.
        if let Some(host) = self.host.clone().or_else(|| env("PGHOST")) {
            if self.host.is_some() || self.uri.is_none() {
                params.host = host;
            }
        }
        if let Some(port) = self.port {
            params.port = port;
        } else if self.uri.is_none() {
            if let Some(port) = env("PGPORT") {
                params.port = port
                    .parse()
                    .map_err(|_| format!("PGPORT={:?} is not a port number", port))?;
            }
        }
        if let Some(user) = self.user.clone() {
            params.user = user;
        } else if self.uri.is_none() {
            if let Some(user) = env("PGUSER") {
                params.user = user;
            }
        }
        if let Some(database) = self.database.clone() {
            params.database = database;
        } else if self.uri.is_none() {
            if let Some(database) = env("PGDATABASE") {
                params.database = database;
            }
        }
        // The password is taken from the environment even when a URI was given:
        // it is the one field most often kept out of a written-down DSN.
        if let Some(password) = self.password.clone().or_else(|| env("PGPASSWORD")) {
            params.password = Some(password);
        }
        if let Some(mode) = &self.sslmode {
            params.sslmode = SslMode::parse(mode).ok_or_else(|| {
                format!(
                    "--pg-import-sslmode {:?}: expected disable, prefer, require or verify-full",
                    mode
                )
            })?;
        } else if self.uri.is_none() {
            if let Some(mode) = env("PGSSLMODE") {
                params.sslmode = SslMode::parse(&mode).ok_or_else(|| {
                    format!(
                        "PGSSLMODE={:?}: expected disable, prefer, require or verify-full",
                        mode
                    )
                })?;
            }
        }

        if params.database.is_empty() {
            return Err(
                "no source database was named; give --pg-import-database, put it in \
                 --pg-import-from, or set PGDATABASE"
                    .to_string(),
            );
        }

        Ok(params)
    }

    pub fn options(&self) -> Result<ImportOptions, String> {
        Ok(ImportOptions {
            schema: self.schema.clone(),
            include: self.only_tables.clone(),
            exclude: self.exclude_tables.clone(),
            json: JsonMapping::parse(&self.json).ok_or_else(|| {
                format!("--pg-import-json {:?}: expected text or native", self.json)
            })?,
            wide_numeric: WideNumeric::parse(&self.wide_numeric).ok_or_else(|| {
                format!(
                    "--pg-import-wide-numeric {:?}: expected fail or text",
                    self.wide_numeric
                )
            })?,
            batch_rows: self.batch_rows.max(1),
            skip_constraints: self.without_constraints,
            dry_run: self.dry_run,
        })
    }
}

fn env(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|v| !v.is_empty())
}

/// Runs the import, printing the report. Returns whether the process should
/// stop rather than go on to start a server.
pub async fn run_import(
    args: &PgImportArgs,
    catalog: &rclite::Arc<rasterizeddb_core::core::database::catalog::DatabaseCatalog>,
) -> Result<bool, String> {
    let Some(target) = args.into.clone() else {
        return Ok(false);
    };

    let source = args.connection()?;
    let options = args.options()?;

    let report =
        rasterizeddb_core::core::migrate::run::import(catalog, &source, &target, &options).await?;

    for line in report.lines() {
        println!("{}", line);
    }

    Ok(!args.and_serve)
}
