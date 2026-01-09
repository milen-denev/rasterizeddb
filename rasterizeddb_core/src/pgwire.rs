use std::{
    collections::HashMap,
    io,
    sync::Arc as StdArc,
};

use log::trace;
use rclite::Arc;

use std::io::BufReader;

use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use tokio_rustls::TlsAcceptor;

use crate::core::{
    database::Database,
    db_type::DbType,
    rql::{
        executor::execute,
        lexer_query::row_fetch_from_select_query,
        lexer_s1::recognize_query_purpose,
        lexer_s1::QueryPurpose,
    },
    row::row::{vec_into_rows, Row},
};

/// Start a minimal PostgreSQL wire-protocol server (pgwire).
///
/// This is **not** a full PostgreSQL implementation; it supports the v3 protocol
/// enough for `psql` and most simple drivers to connect using the *simple query*
/// path, and it forwards a subset of SQL to RasterizedDB's query engine.
#[derive(Clone, Copy, Debug)]
pub enum TlsMode {
    Disable,
    Prefer,
    Require,
}

#[derive(Clone, Debug)]
pub struct TlsConfig {
    pub mode: TlsMode,
    /// PEM certificate chain path (optional; if missing, a self-signed cert is generated)
    pub cert_path: Option<String>,
    /// PEM private key path (optional; if missing, a self-signed cert is generated)
    pub key_path: Option<String>,
}

pub async fn start_pgwire(
    database: Arc<Database>,
    addr: &str,
    port: u16,
    tls: TlsConfig,
) -> io::Result<()> {
    let bind_addr = format!("{}:{}", addr, port);
    let listener = TcpListener::bind(&bind_addr).await?;
    log::info!("pgwire server listening on {}", bind_addr);

    let acceptor = match tls.mode {
        TlsMode::Disable => None,
        TlsMode::Prefer | TlsMode::Require => Some(build_tls_acceptor(&tls)?),
    };

    loop {
        let (stream, peer) = listener.accept().await?;
        let db = database.clone();
        let tls_mode = tls.mode;
        let acceptor = acceptor.clone();
        tokio::spawn(async move {
            if let Err(e) = handle_connection(db, stream, acceptor, tls_mode).await {
                log::debug!("pgwire connection {} ended: {}", peer, e);
            }
        });
    }
}

const SSL_REQUEST_CODE: i32 = 80877103; // 0x04D2162F (1234, 5679)
const CANCEL_REQUEST_CODE: i32 = 80877102; // 0x04D2162E (1234, 5678)
const GSSENC_REQUEST_CODE: i32 = 80877104; // 0x04D21630 (1234, 5680)

async fn handle_connection(
    database: Arc<Database>,
    mut tcp: TcpStream,
    acceptor: Option<TlsAcceptor>,
    tls_mode: TlsMode,
) -> io::Result<()> {
    // PostgreSQL clients may send one or more 8-byte startup *requests* before the
    // real StartupMessage (e.g., GSSENCRequest, SSLRequest). If we don't answer
    // them correctly, many clients will hang during connect and pools will time out.
    loop {
        let (first_len, first_buf) = read_startup_packet(&mut tcp).await?;
        let first_code = i32::from_be_bytes(first_buf[0..4].try_into().unwrap());

        if first_len == 8 {
            match first_code {
                SSL_REQUEST_CODE => {
                    match tls_mode {
                        TlsMode::Disable => {
                            // Deny SSL.
                            tcp.write_all(b"N").await?;
                            tcp.flush().await?;
                            // Client will follow up with a cleartext StartupMessage.
                            continue;
                        }
                        TlsMode::Prefer | TlsMode::Require => {
                            tcp.write_all(b"S").await?;
                            tcp.flush().await?;
                            let acceptor = acceptor.ok_or_else(|| {
                                io::Error::new(io::ErrorKind::Other, "TLS acceptor not configured")
                            })?;
                            let mut tls_stream = acceptor.accept(tcp).await.map_err(|e| {
                                io::Error::new(io::ErrorKind::ConnectionAborted, e.to_string())
                            })?;
                            // After TLS handshake, client sends a fresh StartupMessage.
                            return run_session_after_ssl(database, &mut tls_stream).await;
                        }
                    }
                }
                GSSENC_REQUEST_CODE => {
                    // We don't support GSS-encrypted connections; explicitly deny.
                    tcp.write_all(b"N").await?;
                    tcp.flush().await?;
                    continue;
                }
                CANCEL_REQUEST_CODE => {
                    // Best-effort: we don't support cancel, so just close.
                    log::debug!("pgwire received CancelRequest; closing");
                    return Ok(());
                }
                other => {
                    // Unknown 8-byte request (e.g., future extensions). Deny in a
                    // libpq-compatible way: most of these expect a single-byte response.
                    log::debug!("pgwire received unknown 8-byte startup request code {}; denying", other);
                    tcp.write_all(b"N").await?;
                    tcp.flush().await?;
                    continue;
                }
            }
        }

        // Not an 8-byte request: this should be a cleartext StartupMessage.
        if matches!(tls_mode, TlsMode::Require) {
            // Respond in cleartext (since the client spoke cleartext) and close.
            send_fatal_error_response(
                &mut tcp,
                "08004",
                "TLS is required (send SSLRequest / use sslmode=require)",
            )
            .await?;
            return Ok(());
        }

        // StartupMessage was already read (first_len/first_buf); ignore parameters.
        let _ = (first_len, first_buf);
        return run_session_after_startup(database, &mut tcp).await;
    }
}

async fn run_session_after_ssl<S>(database: Arc<Database>, stream: &mut S) -> io::Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    // After SSL negotiation (either accepted or denied), the client sends StartupMessage.
    let _ = read_startup_packet(stream).await?;
    send_startup_responses(stream).await?;
    run_message_loop(&database, stream).await
}

async fn run_session_after_startup(database: Arc<Database>, stream: &mut TcpStream) -> io::Result<()> {
    // StartupMessage was already read by handle_connection; we ignore its parameters for now.
    send_startup_responses(stream).await?;
    run_message_loop(&database, stream).await
}

async fn send_startup_responses<S>(stream: &mut S) -> io::Result<()>
where
    S: AsyncWrite + Unpin,
{
    send_auth_ok(stream).await?;
    send_parameter_status(stream, "server_version", "16.3").await?;
    send_parameter_status(stream, "server_version_num", "160003").await?;
    send_parameter_status(stream, "client_encoding", "UTF8").await?;
    send_parameter_status(stream, "DateStyle", "ISO, MDY").await?;
    send_parameter_status(stream, "standard_conforming_strings", "on").await?;
    send_parameter_status(stream, "TimeZone", "UTC").await?;
    send_backend_key_data(stream).await?;
    send_ready_for_query(stream, b'I').await?;
    Ok(())
}

async fn run_message_loop<S>(database: &Arc<Database>, stream: &mut S) -> io::Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let mut state = SessionState::default();
    loop {
        let mut tag = [0u8; 1];
        match stream.read_exact(&mut tag).await {
            Ok(_) => {}
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(()),
            Err(e) => return Err(e),
        }

        let len = read_i32_be(stream).await?;
        if len < 4 {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "invalid message length"));
        }
        let mut payload = vec![0u8; (len - 4) as usize];
        stream.read_exact(&mut payload).await?;

        match tag[0] {
            b'Q' => {
                let query = read_cstring(&payload);
                handle_simple_query(database, stream, query).await?;
            }
            // Extended query protocol (Parse/Bind/Describe/Execute/Sync/...)
            b'P' => {
                handle_parse(&mut state, stream, &payload).await?;
            }
            b'B' => {
                handle_bind(&mut state, stream, &payload).await?;
            }
            b'D' => {
                handle_describe(database, &mut state, stream, &payload).await?;
            }
            b'E' => {
                handle_execute(database, &mut state, stream, &payload).await?;
            }
            b'C' => {
                handle_close(&mut state, stream, &payload).await?;
            }
            b'S' => {
                // Sync: indicates end of an extended-query batch.
                send_ready_for_query(stream, b'I').await?;
            }
            b'H' => {
                // Flush: no server-side buffering beyond what AsyncWrite already does.
                stream.flush().await?;
            }
            b'X' => return Ok(()),
            _ => {
                send_error_response(
                    stream,
                    "0A000",
                    &format!("unsupported frontend message type '{}'", tag[0] as char),
                )
                .await?;
                send_ready_for_query(stream, b'I').await?;
            }
        }
    }
}

#[derive(Default)]
struct SessionState {
    // statement name -> info
    statements: HashMap<String, StatementInfo>,
    // portal name -> info
    portals: HashMap<String, PortalInfo>,
}

struct StatementInfo {
    sql: String,
    param_oids: Vec<i32>,
}

#[derive(Clone, Debug, Default)]
struct PortalInfo {
    stmt_name: String,
    result_formats: Vec<i16>,
}

async fn handle_parse<S>(state: &mut SessionState, stream: &mut S, payload: &[u8]) -> io::Result<()>
where
    S: AsyncWrite + Unpin,
{
    // Parse message payload:
    // cstring stmt_name, cstring query, i16 param_count, [i32 param_oid]...
    let (stmt_name, i) = read_cstring_owned(payload, 0)?;
    let (query, i) = read_cstring_owned(payload, i)?;
    let (param_count, mut i) = read_i16_be_from(payload, i)?;

    let mut param_oids = Vec::with_capacity(param_count.max(0) as usize);
    for _ in 0..param_count {
        let (oid, next) = read_i32_be_from(payload, i)?;
        i = next;
        // Postgres uses OID=0 to mean "unspecified"; keep as-is.
        param_oids.push(oid);
    }

    state.statements.insert(
        stmt_name,
        StatementInfo {
            sql: query,
            param_oids,
        },
    );
    // ParseComplete
    send_message(stream, b'1', &[]).await
}

async fn handle_bind<S>(state: &mut SessionState, stream: &mut S, payload: &[u8]) -> io::Result<()>
where
    S: AsyncWrite + Unpin,
{
    // Bind message payload:
    // cstring portal, cstring stmt, i16 fmt_count, [i16 fmt]...
    // i16 param_count, [i32 len + bytes]..., i16 result_fmt_count, [i16 fmt]...
    let (portal, i) = read_cstring_owned(payload, 0)?;
    let (stmt, i) = read_cstring_owned(payload, i)?;
    let (fmt_count, mut i) = read_i16_be_from(payload, i)?;
    for _ in 0..fmt_count {
        let (_, next) = read_i16_be_from(payload, i)?;
        i = next;
    }
    let (param_count, mut i) = read_i16_be_from(payload, i)?;
    for _ in 0..param_count {
        let (len, next) = read_i32_be_from(payload, i)?;
        i = next;
        if len >= 0 {
            let skip = len as usize;
            if payload.len() < i + skip {
                return Err(io::Error::new(io::ErrorKind::InvalidData, "invalid Bind parameter length"));
            }
            i += skip;
        }
    }
    let (res_fmt_count, mut i) = read_i16_be_from(payload, i)?;
    let mut result_formats = Vec::with_capacity(res_fmt_count as usize);
    for _ in 0..res_fmt_count {
        let (fmt, next) = read_i16_be_from(payload, i)?;
        result_formats.push(fmt);
        i = next;
    }

    state.portals.insert(portal, PortalInfo { stmt_name: stmt, result_formats });
    // BindComplete
    send_message(stream, b'2', &[]).await
}

async fn handle_describe<S>(
    database: &Arc<Database>,
    state: &mut SessionState,
    stream: &mut S,
    payload: &[u8],
) -> io::Result<()>
where
    S: AsyncWrite + Unpin,
{
    // Describe payload: byte ('S'|'P'), cstring name
    if payload.is_empty() {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "invalid Describe payload"));
    }
    let kind = payload[0];
    let (name, _) = read_cstring_owned(payload, 1)?;

    let (stmt_name, result_formats) = match kind {
        b'S' => (name, vec![]),
        b'P' => {
            let info = state.portals.get(&name).cloned().unwrap_or_default();
            (info.stmt_name, info.result_formats)
        }
        _ => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid Describe kind",
            ))
        }
    };

    let query = state
        .statements
        .get(&stmt_name)
        .map(|s| s.sql.as_str())
        .unwrap_or("");

    // Per protocol, Describe returns ParameterDescription first.
    let param_oids: Vec<i32> = state
        .statements
        .get(&stmt_name)
        .map(|s| s.param_oids.clone())
        .unwrap_or_default();
    send_parameter_description(stream, &param_oids).await?;

    // Try to produce RowDescription for SELECT; otherwise NoData.
    if let Some((fields, is_select)) = describe_query(database, query).await? {
        if is_select {
            send_row_description(stream, &fields, &result_formats).await?;
        } else {
            // NoData
            send_message(stream, b'n', &[]).await?;
        }
        return Ok(());
    }

    send_message(stream, b'n', &[]).await
}

async fn handle_execute<S>(
    database: &Arc<Database>,
    state: &mut SessionState,
    stream: &mut S,
    payload: &[u8],
) -> io::Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    // Execute payload: cstring portal, i32 max_rows
    let (portal, i) = read_cstring_owned(payload, 0)?;
    let _ = read_i32_be_from(payload, i)?;

    let (stmt_name, result_formats) = if let Some(info) = state.portals.get(&portal) {
        (info.stmt_name.clone(), info.result_formats.clone())
    } else {
        (String::new(), vec![])
    };

    let query = state
        .statements
        .get(&stmt_name)
        .map(|s| s.sql.as_str())
        .unwrap_or("");

    if query.trim().is_empty() {
        send_error_response(stream, "26000", "prepared statement does not exist").await?;
        return Ok(());
    }

    execute_single_statement(database, stream, query, &result_formats).await
}

async fn handle_close<S>(state: &mut SessionState, stream: &mut S, payload: &[u8]) -> io::Result<()>
where
    S: AsyncWrite + Unpin,
{
    // Close payload: byte ('S'|'P'), cstring name
    if payload.is_empty() {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "invalid Close payload"));
    }
    let kind = payload[0];
    let (name, _) = read_cstring_owned(payload, 1)?;
    match kind {
        b'S' => {
            state.statements.remove(&name);
        }
        b'P' => {
            state.portals.remove(&name);
        }
        _ => {}
    }

    // CloseComplete
    send_message(stream, b'3', &[]).await
}

async fn describe_query(
    database: &Arc<Database>,
    query: &str,
) -> io::Result<Option<(Vec<(&'static str, DbType)>, bool)>> {
    let stmt = query.trim();
    if stmt.is_empty() {
        return Ok(None);
    }

    let purpose = match recognize_query_purpose(stmt) {
        Some(p) => p,
        None => return Ok(None),
    };

    match &purpose {
        QueryPurpose::QueryRows(qr) => {
            let table = match database.tables.get(&qr.table_name) {
                Some(t) => t,
                None => return Ok(None),
            };
            let parsed = match row_fetch_from_select_query(&purpose, &table.schema.fields) {
                Ok(p) => p,
                Err(_) => return Ok(None),
            };

            // NOTE: RowDescription wants &str; we keep names static-ish by leaking.
            // This is acceptable for a minimal server; statements are small and few.
            let mut fields = Vec::new();
            for c in &parsed.requested_row_fetch.columns_fetching_data {
                let field = &table.schema.fields[c.schema_id as usize];
                let leaked: &'static str = Box::leak(field.name.clone().into_boxed_str());
                fields.push((leaked, field.db_type.clone()));
            }
            Ok(Some((fields, true)))
        }
        _ => Ok(Some((Vec::new(), false))),
    }
}

async fn execute_single_statement<S>(
    database: &Arc<Database>,
    stream: &mut S,
    stmt: &str,
    result_formats: &[i16],
) -> io::Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    if handle_builtin_query(stream, stmt).await? {
        return Ok(());
    }

    let purpose = match recognize_query_purpose(stmt) {
        Some(p) => p,
        None => {
            send_error_response(stream, "42601", "syntax error or unsupported query").await?;
            return Ok(());
        }
    };

    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    enum PurposeKind {
        CreateTable,
        InsertRow,
        UpdateRow,
        DeleteRow,
        QueryRows,
        Other,
    }

    let purpose_kind = match &purpose {
        QueryPurpose::CreateTable(_) => PurposeKind::CreateTable,
        QueryPurpose::InsertRow(_) => PurposeKind::InsertRow,
        QueryPurpose::UpdateRow(_) => PurposeKind::UpdateRow,
        QueryPurpose::DeleteRow(_) => PurposeKind::DeleteRow,
        QueryPurpose::QueryRows(_) => PurposeKind::QueryRows,
        _ => PurposeKind::Other,
    };

    // Precompute SELECT column metadata (needed even if result set is empty).
    let select_fields = match &purpose {
        QueryPurpose::QueryRows(qr) => {
            let table = database.tables.get(&qr.table_name);
            if table.is_none() {
                send_error_response(stream, "42P01", "relation does not exist").await?;
                return Ok(());
            }
            let table = table.unwrap();
            match row_fetch_from_select_query(&purpose, &table.schema.fields) {
                Ok(parsed) => Some(
                    parsed
                        .requested_row_fetch
                        .columns_fetching_data
                        .iter()
                        .map(|c| {
                            let field = &table.schema.fields[c.schema_id as usize];
                            (field.name.clone(), field.db_type.clone())
                        })
                        .collect::<Vec<_>>(),
                ),
                Err(e) => {
                    send_error_response(stream, "42601", &e).await?;
                    return Ok(());
                }
            }
        }
        _ => None,
    };

    // `execute` consumes QueryPurpose, but we still want `purpose_kind`/`select_fields`.
    // Re-recognize the query to get an owned QueryPurpose to pass into the engine.
    let purpose_exec = recognize_query_purpose(stmt).ok_or_else(|| {
        io::Error::new(io::ErrorKind::InvalidData, "query purpose disappeared")
    })?;

    let resp = execute(purpose_exec, database.clone()).await;
    if resp.is_empty() {
        send_error_response(stream, "XX000", "empty engine response").await?;
        return Ok(());
    }

    match resp[0] {
        0 => {
            // OK. Some engine paths also append an 8-byte LE rows-affected count after the tag.
            let rows_affected = if resp.len() >= 9 {
                let mut arr = [0u8; 8];
                arr.copy_from_slice(&resp[1..9]);
                Some(u64::from_le_bytes(arr))
            } else {
                None
            };

            let tag = match (purpose_kind, rows_affected) {
                (PurposeKind::CreateTable, _) => "CREATE TABLE".to_string(),
                (PurposeKind::InsertRow, Some(n)) => format!("INSERT 0 {}", n),
                (PurposeKind::UpdateRow, Some(n)) => format!("UPDATE {}", n),
                (PurposeKind::DeleteRow, Some(n)) => format!("DELETE {}", n),
                // Fall back to conventional tags with unknown counts.
                (PurposeKind::InsertRow, None) => "INSERT 0 0".to_string(),
                (PurposeKind::UpdateRow, None) => "UPDATE 0".to_string(),
                (PurposeKind::DeleteRow, None) => "DELETE 0".to_string(),
                _ => "OK".to_string(),
            };

            send_command_complete(stream, &tag).await?;
        }
        1 => {
            if resp.len() < 9 {
                send_error_response(stream, "XX000", "invalid rows-affected response").await?;
                return Ok(());
            }
            let mut arr = [0u8; 8];
            arr.copy_from_slice(&resp[1..9]);
            let rows = u64::from_le_bytes(arr);
            let tag = match purpose_kind {
                PurposeKind::DeleteRow => format!("DELETE {}", rows),
                PurposeKind::InsertRow => format!("INSERT 0 {}", rows),
                PurposeKind::UpdateRow => format!("UPDATE {}", rows),
                _ => format!("UPDATE {}", rows),
            };
            send_command_complete(stream, &tag).await?;
        }
        2 => {
            let rows_bytes = &resp[1..];
            let rows = vec_into_rows(rows_bytes)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;
            send_query_result(stream, select_fields.as_deref(), &rows, result_formats).await?;
        }
        3 => {
            let msg = String::from_utf8_lossy(&resp[1..]).to_string();
            send_error_response(stream, "XX000", &msg).await?;
        }
        _ => {
            send_error_response(stream, "XX000", "unknown engine response tag").await?;
        }
    }

    Ok(())
}

fn read_cstring_owned(bytes: &[u8], start: usize) -> io::Result<(String, usize)> {
    if start > bytes.len() {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "invalid cstring offset"));
    }
    let rel_end = bytes[start..]
        .iter()
        .position(|&b| b == 0)
        .unwrap_or(bytes.len() - start);
    let end = start + rel_end;
    let s = std::str::from_utf8(&bytes[start..end]).unwrap_or("").to_string();
    let next = (end + 1).min(bytes.len());
    Ok((s, next))
}

fn read_i16_be_from(bytes: &[u8], start: usize) -> io::Result<(i16, usize)> {
    let end = start + 2;
    if bytes.len() < end {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "unexpected EOF"));
    }
    let mut buf = [0u8; 2];
    buf.copy_from_slice(&bytes[start..end]);
    Ok((i16::from_be_bytes(buf), end))
}

fn read_i32_be_from(bytes: &[u8], start: usize) -> io::Result<(i32, usize)> {
    let end = start + 4;
    if bytes.len() < end {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "unexpected EOF"));
    }
    let mut buf = [0u8; 4];
    buf.copy_from_slice(&bytes[start..end]);
    Ok((i32::from_be_bytes(buf), end))
}

async fn handle_simple_query<S>(
    database: &Arc<Database>,
    stream: &mut S,
    query: &str,
) -> io::Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    // The simple query protocol may contain multiple statements separated by ';'.
    let statements = query
        .split(';')
        .map(|s| s.trim())
        .filter(|s| !s.is_empty());

    for stmt in statements {
        execute_single_statement(database, stream, stmt, &[]).await?;
    }

    send_ready_for_query(stream, b'I').await?;
    Ok(())
}

async fn handle_builtin_query<S>(stream: &mut S, stmt: &str) -> io::Result<bool>
where
    S: AsyncWrite + Unpin,
{
    let s = stmt.trim();
    let s_lower = s.to_ascii_lowercase();

    // Transaction control (no-op).
    if matches!(s_lower.as_str(), "begin" | "begin work") {
        send_command_complete(stream, "BEGIN").await?;
        return Ok(true);
    }
    if s_lower == "commit" {
        send_command_complete(stream, "COMMIT").await?;
        return Ok(true);
    }
    if s_lower == "rollback" {
        send_command_complete(stream, "ROLLBACK").await?;
        return Ok(true);
    }

    // SET statements (psql uses these on connect).
    if s_lower.starts_with("set ") {
        send_command_complete(stream, "SET").await?;
        return Ok(true);
    }

    // Common introspection queries.
    if s_lower == "select 1" || s_lower == "select 1::int" {
        send_row_description(stream, &[("?column?", DbType::I32)], &[]).await?;
        send_data_row(stream, &[Some(b"1")]).await?;
        send_command_complete(stream, "SELECT 1").await?;
        return Ok(true);
    }

    if s_lower == "select version()" {
        send_row_description(stream, &[("version", DbType::STRING)], &[]).await?;
        send_data_row(
            stream,
            &[Some(b"rasterizeddb (pgwire) 0.2.0")],
        )
        .await?;
        send_command_complete(stream, "SELECT 1").await?;
        return Ok(true);
    }

    if s_lower.starts_with("show ") {
        let key = s_lower[5..].trim();
        let value = match key {
            "server_version" => "16.3",
            "server_version_num" => "160003",
            "client_encoding" => "UTF8",
            "datestyle" => "ISO, MDY",
            "standard_conforming_strings" => "on",
            "timezone" => "UTC",
            _ => "",
        };

        send_row_description(stream, &[(key, DbType::STRING)], &[]).await?;
        send_data_row(stream, &[Some(value.as_bytes())]).await?;
        send_command_complete(stream, "SHOW").await?;
        return Ok(true);
    }

    Ok(false)
}

async fn send_query_result<S>(
    stream: &mut S,
    fields: Option<&[(String, DbType)]>,
    rows: &[Row],
    result_formats: &[i16],
) -> io::Result<()>
where
    S: AsyncWrite + Unpin,
{
    let col_meta: Vec<(String, DbType)> = if let Some(f) = fields {
        f.to_vec()
    } else if let Some(first) = rows.first() {
        first
            .columns
            .iter()
            .enumerate()
            .map(|(i, c)| (format!("column{}", i + 1), c.column_type.clone()))
            .collect()
    } else {
        Vec::new()
    };

    let fields_ref: Vec<(&str, DbType)> = col_meta
        .iter()
        .map(|(n, t)| (n.as_str(), t.clone()))
        .collect();

    send_row_description(stream, &fields_ref, result_formats).await?;

    for row in rows {
        let mut values = Vec::with_capacity(row.columns.len());
        for (i, col) in row.columns.iter().enumerate() {
            let format_code = if result_formats.is_empty() {
                0
            } else if result_formats.len() == 1 {
                result_formats[0]
            } else {
                *result_formats.get(i).unwrap_or(&0)
            };

            let val = if format_code == 1 {
                format_column_binary(col.column_type.clone(), col.data.into_slice())
            } else {
                format_column_text(col.column_type.clone(), col.data.into_slice())
                    .map(|s| s.into_bytes())
            };
            values.push(val);
        }
        let refs: Vec<Option<&[u8]>> = values.iter().map(|v| v.as_deref()).collect();
        send_data_row(stream, &refs).await?;
    }

    send_command_complete(stream, &format!("SELECT {}", rows.len())).await?;
    Ok(())
}

fn format_column_binary(db_type: DbType, bytes: &[u8]) -> Option<Vec<u8>> {
    if matches!(db_type, DbType::NULL | DbType::START | DbType::END) {
        return None;
    }
    
    match db_type {
        DbType::I8 | DbType::U8 => {
             if bytes.len() == 1 {
                 let val = bytes[0] as i16;
                 Some(val.to_be_bytes().to_vec())
             } else { None }
        }
        DbType::I16 | DbType::U16 => {
             if bytes.len() == 2 {
                 let mut arr = [0u8; 2];
                 arr.copy_from_slice(bytes);
                 let val = u16::from_le_bytes(arr);
                 Some(val.to_be_bytes().to_vec())
             } else { None }
        }
        DbType::I32 | DbType::U32 => {
             if bytes.len() == 4 {
                 let mut arr = [0u8; 4];
                 arr.copy_from_slice(bytes);
                 let val = u32::from_le_bytes(arr);
                 Some(val.to_be_bytes().to_vec())
             } else { None }
        }
        DbType::I64 | DbType::U64 => {
             if bytes.len() == 8 {
                 let mut arr = [0u8; 8];
                 arr.copy_from_slice(bytes);
                 let val = u64::from_le_bytes(arr);
                 Some(val.to_be_bytes().to_vec())
             } else { None }
        }
        DbType::F32 => {
             if bytes.len() == 4 {
                 let mut arr = [0u8; 4];
                 arr.copy_from_slice(bytes);
                 let val = u32::from_le_bytes(arr);
                 Some(val.to_be_bytes().to_vec())
             } else { None }
        }
        DbType::F64 => {
             if bytes.len() == 8 {
                 let mut arr = [0u8; 8];
                 arr.copy_from_slice(bytes);
                 let val = u64::from_le_bytes(arr);
                 Some(val.to_be_bytes().to_vec())
             } else { None }
        }
        DbType::BOOL => {
             if !bytes.is_empty() {
                 Some(vec![if bytes[0] != 0 { 1 } else { 0 }])
             } else { Some(vec![0]) }
        }
        DbType::STRING | DbType::UNKNOWN => {
             Some(bytes.to_vec())
        }
        DbType::CHAR => {
            if bytes.len() == 4 {
                let mut arr = [0u8; 4];
                arr.copy_from_slice(bytes);
                let code = u32::from_le_bytes(arr);
                char::from_u32(code)
                    .map(|c| c.to_string().into_bytes())
                    .or_else(|| Some(bytes.to_vec()))
            } else {
                Some(bytes.to_vec())
            }
        }
        _ => format_column_text(db_type, bytes).map(|s| s.into_bytes())
    }
}

fn format_column_text(db_type: DbType, bytes: &[u8]) -> Option<String> {
    // NULL-like logical types: no value to send.
    if matches!(db_type, DbType::NULL | DbType::START | DbType::END) {
        return None;
    }

    trace!("Formatting column of type {:?} with bytes: {:?}", db_type, bytes);

    // Some older/alternate paths may store fixed-width numeric types as ASCII decimal
    // bytes (optionally NUL-padded). When pgwire formats these as binary little-endian
    // integers, the result looks like a huge, incorrect number (e.g. "200000" -> 0x323030303030).
    // Detect this shape and fall back to returning the decimal string.
    fn ascii_decimal(bytes: &[u8]) -> Option<&str> {
        if bytes.is_empty() {
            return None;
        }

        // Trim trailing NULs (common with fixed-width fields).
        let mut end = bytes.len();
        while end > 0 && bytes[end - 1] == 0 {
            end -= 1;
        }
        if end == 0 {
            return None;
        }
        let trimmed = &bytes[..end];
        if trimmed.iter().all(|b| b.is_ascii_digit()) {
            std::str::from_utf8(trimmed).ok()
        } else {
            None
        }
    }

    // Helper specifically per width because of Rust type system.
    fn le_i8(bytes: &[u8]) -> Option<i8> {
        if bytes.len() != 1 {
            return None;
        }
        Some(i8::from_le_bytes([bytes[0]]))
    }
    fn le_u8(bytes: &[u8]) -> Option<u8> {
        if bytes.len() != 1 {
            return None;
        }
        Some(u8::from_le_bytes([bytes[0]]))
    }
    fn le_i16(bytes: &[u8]) -> Option<i16> {
        if bytes.len() != 2 {
            return None;
        }
        let mut arr = [0u8; 2];
        arr.copy_from_slice(&bytes[..2]);
        Some(i16::from_le_bytes(arr))
    }
    fn le_u16(bytes: &[u8]) -> Option<u16> {
        if bytes.len() != 2 {
            return None;
        }
        let mut arr = [0u8; 2];
        arr.copy_from_slice(&bytes[..2]);
        Some(u16::from_le_bytes(arr))
    }
    fn le_i32(bytes: &[u8]) -> Option<i32> {
        if bytes.len() != 4 {
            return None;
        }
        let mut arr = [0u8; 4];
        arr.copy_from_slice(&bytes[..4]);
        Some(i32::from_le_bytes(arr))
    }
    fn le_u32(bytes: &[u8]) -> Option<u32> {
        if bytes.len() != 4 {
            return None;
        }
        let mut arr = [0u8; 4];
        arr.copy_from_slice(&bytes[..4]);
        Some(u32::from_le_bytes(arr))
    }
    fn le_i64(bytes: &[u8]) -> Option<i64> {
        if bytes.len() != 8 {
            return None;
        }
        let mut arr = [0u8; 8];
        arr.copy_from_slice(&bytes[..8]);
        Some(i64::from_le_bytes(arr))
    }
    fn le_u64(bytes: &[u8]) -> Option<u64> {
        if bytes.len() != 8 {
            return None;
        }
        let mut arr = [0u8; 8];
        arr.copy_from_slice(&bytes[..8]);
        Some(u64::from_le_bytes(arr))
    }
    fn le_i128(bytes: &[u8]) -> Option<i128> {
        if bytes.len() != 16 {
            return None;
        }
        let mut arr = [0u8; 16];
        arr.copy_from_slice(&bytes[..16]);
        Some(i128::from_le_bytes(arr))
    }
    fn le_u128(bytes: &[u8]) -> Option<u128> {
        if bytes.len() != 16 {
            return None;
        }
        let mut arr = [0u8; 16];
        arr.copy_from_slice(&bytes[..16]);
        Some(u128::from_le_bytes(arr))
    }

    let s = match db_type {
        DbType::STRING | DbType::DATETIME | DbType::UNKNOWN => {
            // Treat as UTF-8 text.
            String::from_utf8_lossy(bytes).to_string()
        }

        DbType::BOOL => {
            // Postgres canonical text is 't' or 'f'.
            let is_true = bytes.first().copied().unwrap_or(0) != 0;
            if is_true { "t".to_string() } else { "f".to_string() }
        }

        DbType::CHAR => {
            // Either a 4-byte LE codepoint or plain UTF-8.
            if bytes.len() == 4 {
                let mut arr = [0u8; 4];
                arr.copy_from_slice(bytes);
                let code = u32::from_le_bytes(arr);
                char::from_u32(code)
                    .map(|c| c.to_string())
                    .unwrap_or_else(|| code.to_string())
            } else {
                String::from_utf8_lossy(bytes).to_string()
            }
        }

        DbType::I8 => {
            if let Some(s) = ascii_decimal(bytes) {
                s.to_string()
            } else if let Some(v) = le_i8(bytes) {
                v.to_string()
            } else {
                return None;
            }
        }
        DbType::I16 => {
            if let Some(s) = ascii_decimal(bytes) {
                s.to_string()
            } else if let Some(v) = le_i16(bytes) {
                v.to_string()
            } else {
                return None;
            }
        }
        DbType::I32 => {
            if let Some(s) = ascii_decimal(bytes) {
                s.to_string()
            } else if let Some(v) = le_i32(bytes) {
                v.to_string()
            } else {
                return None;
            }
        }
        DbType::I64 => {
            if let Some(s) = ascii_decimal(bytes) {
                s.to_string()
            } else if let Some(v) = le_i64(bytes) {
                v.to_string()
            } else {
                return None;
            }
        }
        DbType::I128 => {
            if let Some(s) = ascii_decimal(bytes) {
                s.to_string()
            } else if let Some(v) = le_i128(bytes) {
                v.to_string()
            } else {
                return None;
            }
        }

        DbType::U8 => {
            if let Some(s) = ascii_decimal(bytes) {
                s.to_string()
            } else if let Some(v) = le_u8(bytes) {
                v.to_string()
            } else {
                return None;
            }
        }
        DbType::U16 => {
            if let Some(s) = ascii_decimal(bytes) {
                s.to_string()
            } else if let Some(v) = le_u16(bytes) {
                v.to_string()
            } else {
                return None;
            }
        }
        DbType::U32 => {
            if let Some(s) = ascii_decimal(bytes) {
                s.to_string()
            } else if let Some(v) = le_u32(bytes) {
                v.to_string()
            } else {
                return None;
            }
        }
        DbType::U64 => {
            if let Some(s) = ascii_decimal(bytes) {
                s.to_string()
            } else if let Some(v) = le_u64(bytes) {
                v.to_string()
            } else {
                return None;
            }
        }
        DbType::U128 => {
            if let Some(s) = ascii_decimal(bytes) {
                s.to_string()
            } else if let Some(v) = le_u128(bytes) {
                v.to_string()
            } else {
                return None;
            }
        }

        DbType::F32 => {
            // Floats are usually binary; if that fails, try ASCII.
            if bytes.len() == 4 {
                let mut arr = [0u8; 4];
                arr.copy_from_slice(&bytes[..4]);
                f32::from_le_bytes(arr).to_string()
            } else if let Ok(text) = std::str::from_utf8(bytes) {
                text.to_string()
            } else {
                return None;
            }
        }
        DbType::F64 => {
            if bytes.len() == 8 {
                let mut arr = [0u8; 8];
                arr.copy_from_slice(&bytes[..8]);
                f64::from_le_bytes(arr).to_string()
            } else if let Ok(text) = std::str::from_utf8(bytes) {
                text.to_string()
            } else {
                return None;
            }
        }

        DbType::START | DbType::END | DbType::NULL => {
            // Already handled at top, but keep arm to satisfy match exhaustiveness.
            return None;
        }
    };

    trace!("Formatted column text: {}", s);

    Some(s)
}

async fn send_auth_ok<S>(stream: &mut S) -> io::Result<()>
where
    S: AsyncWrite + Unpin,
{
    let mut payload = Vec::with_capacity(4);
    payload.extend_from_slice(&0i32.to_be_bytes());
    send_message(stream, b'R', &payload).await
}

async fn send_parameter_status<S>(stream: &mut S, key: &str, value: &str) -> io::Result<()>
where
    S: AsyncWrite + Unpin,
{
    let mut payload = Vec::with_capacity(key.len() + value.len() + 2);
    payload.extend_from_slice(key.as_bytes());
    payload.push(0);
    payload.extend_from_slice(value.as_bytes());
    payload.push(0);
    send_message(stream, b'S', &payload).await
}

async fn send_backend_key_data<S>(stream: &mut S) -> io::Result<()>
where
    S: AsyncWrite + Unpin,
{
    // We don't implement CancelRequest, so this is just filler.
    let pid = std::process::id() as i32;
    let key = fastrand::i32(..);
    let mut payload = Vec::with_capacity(8);
    payload.extend_from_slice(&pid.to_be_bytes());
    payload.extend_from_slice(&key.to_be_bytes());
    send_message(stream, b'K', &payload).await
}

async fn send_ready_for_query<S>(stream: &mut S, status: u8) -> io::Result<()>
where
    S: AsyncWrite + Unpin,
{
    send_message(stream, b'Z', &[status]).await
}

async fn send_command_complete<S>(stream: &mut S, tag: &str) -> io::Result<()>
where
    S: AsyncWrite + Unpin,
{
    let mut payload = Vec::with_capacity(tag.len() + 1);
    payload.extend_from_slice(tag.as_bytes());
    payload.push(0);
    send_message(stream, b'C', &payload).await
}

async fn send_error_response<S>(stream: &mut S, sqlstate: &str, message: &str) -> io::Result<()>
where
    S: AsyncWrite + Unpin,
{
    // ErrorResponse fields: (type_byte, cstring)..., terminated by 0.
    // Minimal set: Severity (S), SQLSTATE (C), Message (M)
    let mut payload = Vec::new();

    payload.push(b'S');
    payload.extend_from_slice(b"ERROR");
    payload.push(0);

    payload.push(b'C');
    payload.extend_from_slice(sqlstate.as_bytes());
    payload.push(0);

    payload.push(b'M');
    payload.extend_from_slice(message.as_bytes());
    payload.push(0);

    payload.push(0);

    send_message(stream, b'E', &payload).await
}
async fn send_row_description<S>(
    stream: &mut S,
    fields: &[(&str, DbType)],
    result_formats: &[i16],
) -> io::Result<()>
where
    S: AsyncWrite + Unpin,
{
    // RowDescription message:
    // 'T'
    // int32 length
    // int16 field_count
    // For each field:
    //   string field_name
    //   int32 table_oid (0 = no table)
    //   int16 column_attr (0 = no column)
    //   int32 type_oid
    //   int16 type_size
    //   int32 type_modifier
    //   int16 format_code (0 = text, 1 = binary)

    let mut payload = Vec::new();

    // number of fields
    payload.extend_from_slice(&(fields.len() as i16).to_be_bytes());

    for (i, (name, db_type)) in fields.iter().enumerate() {
        // field name
        payload.extend_from_slice(name.as_bytes());
        payload.push(0);

        // table_oid
        payload.extend_from_slice(&0i32.to_be_bytes());

        // column_attr
        payload.extend_from_slice(&0i16.to_be_bytes());

        // type_oid
        let oid = db_type.pg_oid();
        payload.extend_from_slice(&(oid as i32).to_be_bytes());

        // type_size
        let size = db_type.pg_type_size();
        payload.extend_from_slice(&(size as i16).to_be_bytes());

        // type_modifier
        payload.extend_from_slice(&(-1i32).to_be_bytes());

        // format_code
        let format_code = if result_formats.is_empty() {
            0
        } else if result_formats.len() == 1 {
            result_formats[0]
        } else {
            *result_formats.get(i).unwrap_or(&0)
        };
        payload.extend_from_slice(&format_code.to_be_bytes());
    }

    send_message(stream, b'T', &payload).await
}

async fn send_parameter_description<S>(stream: &mut S, param_oids: &[i32]) -> io::Result<()>
where
    S: AsyncWrite + Unpin,
{
    let mut payload = Vec::with_capacity(2 + (4 * param_oids.len()));
    let count: i16 = param_oids.len().try_into().unwrap_or(i16::MAX);
    payload.extend_from_slice(&count.to_be_bytes());
    for oid in param_oids {
        payload.extend_from_slice(&oid.to_be_bytes());
    }
    // ParameterDescription
    send_message(stream, b't', &payload).await
}

async fn send_data_row<S>(stream: &mut S, values: &[Option<&[u8]>]) -> io::Result<()>
where
    S: AsyncWrite + Unpin,
{
    let mut payload = Vec::new();
    let count: i16 = values.len().try_into().unwrap_or(i16::MAX);
    payload.extend_from_slice(&count.to_be_bytes());

    for v in values {
        match v {
            None => payload.extend_from_slice(&(-1i32).to_be_bytes()),
            Some(bytes) => {
                let len: i32 = bytes.len().try_into().unwrap_or(i32::MAX);
                payload.extend_from_slice(&len.to_be_bytes());
                payload.extend_from_slice(bytes);
            }
        }
    }

    send_message(stream, b'D', &payload).await
}

// fn db_type_to_pg(t: DbType) -> (i32, i16) {
//     // OIDs: https://www.postgresql.org/docs/current/datatype-oid.html
//     match t {
//         DbType::I8 | DbType::U8 => (21, 2), // int2
//         DbType::I16 | DbType::U16 => (21, 2),
//         DbType::I32 | DbType::U32 => (23, 4), // int4
//         DbType::I64 | DbType::U64 => (20, 8), // int8
//         DbType::I128 | DbType::U128 => (1700, -1), // numeric
//         DbType::F32 => (700, 4),
//         DbType::F64 => (701, 8),
//         DbType::DATETIME => (1114, 8),
//         DbType::BOOL => (16, 1),
//         DbType::CHAR | DbType::STRING | DbType::UNKNOWN | DbType::NULL | DbType::START | DbType::END => (25, -1),
//     }
// }

async fn send_message<S>(stream: &mut S, tag: u8, payload: &[u8]) -> io::Result<()>
where
    S: AsyncWrite + Unpin,
{
    let len: i32 = (4 + payload.len())
        .try_into()
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "message too large"))?;

    stream.write_all(&[tag]).await?;
    stream.write_all(&len.to_be_bytes()).await?;
    stream.write_all(payload).await?;
    // For TLS streams (tokio-rustls), writes may be buffered until flush.
    stream.flush().await?;
    Ok(())
}

async fn read_i32_be<S>(stream: &mut S) -> io::Result<i32>
where
    S: AsyncRead + Unpin,
{
    let mut buf = [0u8; 4];
    stream.read_exact(&mut buf).await?;
    Ok(i32::from_be_bytes(buf))
}

async fn read_startup_packet<S>(stream: &mut S) -> io::Result<(i32, Vec<u8>)>
where
    S: AsyncRead + Unpin,
{
    let len = read_i32_be(stream).await?;
    if len < 8 {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "invalid startup packet"));
    }
    let mut buf = vec![0u8; (len - 4) as usize];
    stream.read_exact(&mut buf).await?;
    Ok((len, buf))
}

async fn send_fatal_error_response<S>(stream: &mut S, sqlstate: &str, message: &str) -> io::Result<()>
where
    S: AsyncWrite + Unpin,
{
    let mut payload = Vec::new();

    payload.push(b'S');
    payload.extend_from_slice(b"FATAL");
    payload.push(0);

    payload.push(b'C');
    payload.extend_from_slice(sqlstate.as_bytes());
    payload.push(0);

    payload.push(b'M');
    payload.extend_from_slice(message.as_bytes());
    payload.push(0);

    payload.push(0);

    send_message(stream, b'E', &payload).await
}

fn build_tls_acceptor(tls: &TlsConfig) -> io::Result<TlsAcceptor> {
    let (certs, key) = match (&tls.cert_path, &tls.key_path) {
        (Some(cert_path), Some(key_path)) => load_tls_from_pem(cert_path, key_path)?,
        (None, None) => {
            // Reuse rastcp's self-signed generator.
            rastcp::cert::generate_self_signed_cert()
                .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?
        }
        _ => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "pgwire TLS requires both --pg-cert and --pg-key, or neither",
            ));
        }
    };

    let config = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e.to_string()))?;

    Ok(TlsAcceptor::from(StdArc::new(config)))
}

fn load_tls_from_pem(cert_path: &str, key_path: &str) -> io::Result<(Vec<CertificateDer<'static>>, PrivateKeyDer<'static>)> {
    let cert_bytes = std::fs::read(cert_path)?;
    let key_bytes = std::fs::read(key_path)?;

    let mut cert_reader = BufReader::new(cert_bytes.as_slice());
    let certs = rustls_pemfile::certs(&mut cert_reader)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e.to_string()))?;

    let mut key_reader = BufReader::new(key_bytes.as_slice());
    let key = rustls_pemfile::private_key(&mut key_reader)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e.to_string()))?
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "no private key found"))?
        .clone_key();

    Ok((certs, key))
}

fn read_cstring(bytes: &[u8]) -> &str {
    let end = bytes.iter().position(|&b| b == 0).unwrap_or(bytes.len());
    std::str::from_utf8(&bytes[..end]).unwrap_or("")
}
