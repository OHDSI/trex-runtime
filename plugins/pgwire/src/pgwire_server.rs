use std::sync::Arc;
use std::thread;
use std::time::SystemTime;

use duckdb::params;
use async_trait::async_trait;
use futures::stream;
use serde_json;
use base64::{Engine as _, engine::general_purpose};

use pgwire::api::auth::StartupHandler;
use pgwire::api::auth::sasl::SASLAuthStartupHandler;
use pgwire::api::auth::sasl::scram::{gen_salted_password, ScramAuth};
use pgwire::api::auth::{AuthSource, DefaultServerParameterProvider, LoginInfo, Password};
use pgwire::api::query::{ExtendedQueryHandler, SimpleQueryHandler};
use pgwire::api::stmt::NoopQueryParser;
use pgwire::api::results::{Response, Tag, QueryResponse, DescribeStatementResponse, DescribePortalResponse, FieldInfo};
use pgwire::api::{PgWireServerHandlers, ClientInfo, NoopHandler, Type};
use pgwire::api::portal::{Portal, Format};
use pgwire::api::stmt::StoredStatement;
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use pgwire::tokio::process_socket;

use tokio::net::TcpListener;
use tokio::sync::oneshot;

use arrow_pg::datatypes::{encode_recordbatch, into_pg_type};

use crate::{get_query_executor, get_shared_connection, QueryExecutor, QueryResult};
use crate::server_registry::{ServerHandle, ServerRegistry};

const DEBUG_LOGGING: bool = false;

#[inline]
fn log_debug(_msg: &str) {
    #[cfg(debug_assertions)]
    if DEBUG_LOGGING {
        eprintln!("[pgwire] {}", _msg);
    }
}

const SCRAM_ITERATIONS: usize = 4096;

#[derive(Clone)]
pub struct HanaCredentials {
    pub host: String,
    pub port: u16,
    pub name: String,
    pub username: String,
    pub password: String,
}

impl std::fmt::Debug for HanaCredentials {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HanaCredentials")
            .field("host", &self.host)
            .field("port", &self.port)
            .field("name", &self.name)
            .field("username", &self.username)
            .field("password", &"[REDACTED]")
            .finish()
    }
}

#[derive(Debug)]
pub enum DatabaseAction {
    SetDatabase,
    UseHana(HanaCredentials),
    Skip,
}

pub fn check_database_action(database_name: &str, db_credentials: &str) -> DatabaseAction {
    if let Ok(decoded_bytes) = general_purpose::STANDARD.decode(db_credentials) {
        if let Ok(decoded_str) = String::from_utf8(decoded_bytes) {
            if let Ok(json_value) = serde_json::from_str::<serde_json::Value>(&decoded_str) {
                if let Some(databases) = json_value.as_array() {
                    for db in databases {
                        if let Some(db_id) = db.get("id").and_then(|v| v.as_str()) {
                            if db_id == database_name {
                                if let Some(dialect) = db.get("dialect").and_then(|v| v.as_str()) {
                                    if dialect == "hana" {
                                        if let (Some(host), Some(port), Some(name)) = (
                                            db.get("host").and_then(|v| v.as_str()),
                                            db.get("port").and_then(|v| v.as_u64()),
                                            db.get("name").and_then(|v| v.as_str())
                                        ) {
                                            if let Some(credentials_array) = db.get("credentials").and_then(|v| v.as_array()) {
                                                for cred in credentials_array {
                                                    if let Some(user_scope) = cred.get("userScope").and_then(|v| v.as_str()) {
                                                        if user_scope == "Admin" {
                                                            if let (Some(username), Some(password)) = (
                                                                cred.get("username").and_then(|v| v.as_str()),
                                                                cred.get("password").and_then(|v| v.as_str())
                                                            ) {
                                                                return DatabaseAction::UseHana(HanaCredentials {
                                                                    host: host.to_string(),
                                                                    port: port as u16,
                                                                    name: name.to_string(),
                                                                    username: username.to_string(),
                                                                    password: password.to_string(),
                                                                });
                                                            }
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                        return DatabaseAction::Skip;
                                    } else {
                                        return DatabaseAction::SetDatabase;
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    DatabaseAction::Skip
}

fn get_hana_credentials_if_available(
    database: &Option<String>,
    server_host: &str,
    server_port: u16,
) -> Option<HanaCredentials> {
    if let Some(db) = database {
        if let Some(db_credentials) = ServerRegistry::instance().get_db_credentials(server_host, server_port) {
            match check_database_action(db, &db_credentials) {
                DatabaseAction::UseHana(hana_creds) => {
                    Some(hana_creds)
                }
                _ => None
            }
        } else {
            None
        }
    } else {
        None
    }
}

fn wrap_query_for_hana(query: &str, hana_creds: &HanaCredentials) -> String {
    let escaped_query = query.replace("'", "''");
    let escaped_username = hana_creds.username.replace("'", "''");
    let escaped_password = hana_creds.password.replace("'", "''");
    let escaped_host = hana_creds.host.replace("'", "''");
    let escaped_name = hana_creds.name.replace("'", "''");

    if query.to_uppercase().starts_with("SELECT") || query.to_uppercase().starts_with("WITH") {
        format!(
            "SELECT * FROM hana_scan('{}', 'hdbsql://{}:{}@{}:{}/{}')",
            escaped_query,
            escaped_username,
            escaped_password,
            escaped_host,
            hana_creds.port,
            escaped_name
        )
    } else {
        format!(
            "SELECT hana_execute('{}', 'hdbsql://{}:{}@{}:{}/{}')",
            escaped_query,
            escaped_username,
            escaped_password,
            escaped_host,
            hana_creds.port,
            escaped_name
        )
    }
}

fn execute_with_fallback<F, R>(
    primary_query: &str,
    fallback_query: Option<&str>,
    operation: F,
) -> Result<R, duckdb::Error>
where
    F: Fn(&str) -> Result<R, duckdb::Error>,
{
    let result = operation(primary_query);

    if result.is_err() && fallback_query.is_some() {
        operation(fallback_query.unwrap())
    } else {
        result
    }
}

pub fn random_salt() -> Vec<u8> {
    Vec::from(rand::random::<[u8; 10]>())
}

pub struct SimpleAuthSource {
    required_password: String,
}

impl std::fmt::Debug for SimpleAuthSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SimpleAuthSource")
            .field("required_password", &"[REDACTED]")
            .finish()
    }
}

impl SimpleAuthSource {
    pub fn new(password: String) -> Self {
        Self {
            required_password: password,
        }
    }
}

#[async_trait]
impl AuthSource for SimpleAuthSource {
    async fn get_password(&self, _login_info: &LoginInfo) -> PgWireResult<Password> {
        let salt = random_salt();
        let hash_password = gen_salted_password(&self.required_password, salt.as_ref(), SCRAM_ITERATIONS);
        Ok(Password::new(Some(salt), hash_password))
    }
}

#[derive(Clone)]
pub struct TrexQueryHandler {
    executor: Arc<QueryExecutor>,
    server_host: String,
    server_port: u16,
    worker_id: usize,
}

impl TrexQueryHandler {
    pub fn new(executor: Arc<QueryExecutor>, host: String, port: u16, worker_id: usize) -> Self {
        Self {
            executor,
            server_host: host,
            server_port: port,
            worker_id,
        }
    }
}

/// Convert trexsql statement columns to pgwire field info (for describe operations)
fn row_desc_from_stmt(stmt: &duckdb::Statement, format: &Format) -> PgWireResult<Vec<FieldInfo>> {
    let columns = stmt.column_count();
    (0..columns)
        .map(|idx| {
            let datatype = stmt.column_type(idx);
            let name = stmt.column_name(idx).map_or("unknown".to_string(), |v| v.clone());
            Ok(FieldInfo::new(
                name.to_string(),
                None,
                None,
                into_pg_type(&datatype).unwrap_or(Type::TEXT),
                format.format_for(idx),
            ))
        })
        .collect()
}

/// Convert Arrow schema to pgwire field info
fn schema_to_field_info(schema: &duckdb::arrow::datatypes::Schema, format: &Format) -> PgWireResult<Vec<FieldInfo>> {
    schema.fields().iter().enumerate().map(|(idx, field)| {
        let pg_type = arrow_type_to_pg_type(field.data_type());
        Ok(FieldInfo::new(
            field.name().clone(),
            None,
            None,
            pg_type,
            format.format_for(idx),
        ))
    }).collect()
}

/// Convert Arrow data type to PostgreSQL type
fn arrow_type_to_pg_type(arrow_type: &duckdb::arrow::datatypes::DataType) -> Type {
    use duckdb::arrow::datatypes::DataType;
    match arrow_type {
        DataType::Boolean => Type::BOOL,
        DataType::Int8 | DataType::Int16 => Type::INT2,
        DataType::Int32 => Type::INT4,
        DataType::Int64 => Type::INT8,
        DataType::UInt8 | DataType::UInt16 => Type::INT2,
        DataType::UInt32 => Type::INT4,
        DataType::UInt64 => Type::INT8,
        DataType::Float16 | DataType::Float32 => Type::FLOAT4,
        DataType::Float64 => Type::FLOAT8,
        DataType::Utf8 | DataType::LargeUtf8 => Type::TEXT,
        DataType::Date32 | DataType::Date64 => Type::DATE,
        DataType::Timestamp(_, _) => Type::TIMESTAMP,
        DataType::Time32(_) | DataType::Time64(_) => Type::TIME,
        DataType::Binary | DataType::LargeBinary => Type::BYTEA,
        _ => Type::TEXT,
    }
}

#[async_trait]
impl SimpleQueryHandler for TrexQueryHandler {
    async fn do_query<C>(&self, _client: &mut C, query: &str) -> PgWireResult<Vec<Response>>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        log_debug(&format!("SimpleQuery: {}", query));

        let login_info = LoginInfo::from_client_info(_client);
        if let Some(db) = login_info.database() {
            if let Some(db_credentials) = ServerRegistry::instance().get_db_credentials(&self.server_host, self.server_port) {
                if matches!(check_database_action(db, &db_credentials), DatabaseAction::SetDatabase) {
                    let use_rx = self.executor.submit_to(self.worker_id, format!("USE \"{}\"", db.replace('"', "\"\"")));
                    if let Ok(QueryResult::Error(err)) = use_rx.await {
                        return Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                            "ERROR".to_owned(),
                            "XX000".to_owned(),
                            format!("Failed to set database context: {}", err),
                        ))));
                    }
                }
            }
        }

        let queries: Vec<&str> = query
            .split(';')
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
            .collect();

        let mut responses = Vec::new();

        for sql in queries {
            // Apply PostgreSQL compatibility transformations
            let sql = sql.replace("::regclass", "::string")
                .replace("AND datallowconn AND NOT datistemplate", "AND NOT db.datname =('system') AND NOT db.datname =('temp')")
                .replace("pg_get_expr(ad.adbin, ad.adrelid, true)","pg_get_expr(ad.adbin, ad.adrelid)")
                .replace("pg_catalog.pg_relation_size(i.indexrelid)","''")
                .replace("pg_catalog.pg_stat_get_numscans(i.indexrelid)","''")
                .replace("pg_catalog.pg_inherits i,pg_catalog.pg_class c WHERE",
                "(select 0 as inhseqno, 0 as inhrelid, 0 as inhparent) as i join pg_catalog.pg_class as c ON")
                .replace("SELECT c.oid,c.*,t.relname as tabrelname,rt.relnamespace as refnamespace,d.description, null as consrc_copy",
                "SELECT c.oid,t.relname  as tabrelname,rt.relnamespace as refnamespace,d.description, null as consrc_copy");

            log_debug(&format!("Submitting to worker {}: {}", self.worker_id, sql));
            let result_rx = self.executor.submit_to(self.worker_id, sql.clone());

            let query_result = result_rx.await.map_err(|_| {
                PgWireError::UserError(Box::new(ErrorInfo::new(
                    "ERROR".to_owned(),
                    "XX000".to_owned(),
                    "Query execution channel closed".to_owned(),
                )))
            })?;

            match query_result {
                QueryResult::Select { schema, batches } => {
                    log_debug(&format!("Got SELECT result: {} batches", batches.len()));
                    let header = Arc::new(schema_to_field_info(&schema, &Format::UnifiedText)?);
                    let header_ref = header.clone();

                    let data: Vec<_> = batches.into_iter()
                        .flat_map(|rb| encode_recordbatch(header_ref.clone(), rb))
                        .collect();

                    responses.push(Response::Query(QueryResponse::new(
                        header,
                        stream::iter(data.into_iter()),
                    )));
                }
                QueryResult::Execute { rows_affected } => {
                    log_debug(&format!("Got EXECUTE result: {} rows", rows_affected));
                    responses.push(Response::Execution(Tag::new("OK").with_rows(rows_affected)));
                }
                QueryResult::Error(err) => {
                    log_debug(&format!("Got ERROR: {}", err));
                    return Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                        "ERROR".to_owned(),
                        "XX000".to_owned(),
                        err,
                    ))));
                }
            }
        }

        if responses.is_empty() {
            responses.push(Response::Execution(Tag::new("OK").with_rows(0)));
        }

        Ok(responses)
    }
}

#[async_trait]
impl ExtendedQueryHandler for TrexQueryHandler {
    type Statement = String;
    type QueryParser = NoopQueryParser;

    fn query_parser(&self) -> Arc<Self::QueryParser> {
        Arc::new(NoopQueryParser::new())
    }

    async fn do_query<C>(
        &self,
        _client: &mut C,
        portal: &Portal<Self::Statement>,
        _max_rows: usize,
    ) -> PgWireResult<Response>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        let query = portal.statement.statement.clone();
        log_debug(&format!("ExtendedQuery: {}", query));

        let login_info = LoginInfo::from_client_info(_client);
        if let Some(db) = login_info.database() {
            if let Some(db_credentials) = ServerRegistry::instance().get_db_credentials(&self.server_host, self.server_port) {
                if matches!(check_database_action(db, &db_credentials), DatabaseAction::SetDatabase) {
                    let use_rx = self.executor.submit_to(self.worker_id, format!("USE \"{}\"", db.replace('"', "\"\"")));
                    if let Ok(QueryResult::Error(err)) = use_rx.await {
                        return Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                            "ERROR".to_owned(),
                            "XX000".to_owned(),
                            format!("Failed to set database context: {}", err),
                        ))));
                    }
                }
            }
        }

        let result_rx = self.executor.submit_to(self.worker_id, query);

        let query_result = result_rx.await.map_err(|_| {
            PgWireError::UserError(Box::new(ErrorInfo::new(
                "ERROR".to_owned(),
                "XX000".to_owned(),
                "Query execution channel closed".to_owned(),
            )))
        })?;

        // Convert QueryResult to pgwire Response
        match query_result {
            QueryResult::Select { schema, batches } => {
                let header = Arc::new(schema_to_field_info(&schema, &Format::UnifiedText)?);
                let header_ref = header.clone();

                let data: Vec<_> = batches.into_iter()
                    .flat_map(|rb| encode_recordbatch(header_ref.clone(), rb))
                    .collect();

                Ok(Response::Query(QueryResponse::new(
                    header,
                    stream::iter(data.into_iter()),
                )))
            }
            QueryResult::Execute { rows_affected } => {
                Ok(Response::Execution(Tag::new("OK").with_rows(rows_affected)))
            }
            QueryResult::Error(err) => {
                Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                    "ERROR".to_owned(),
                    "XX000".to_owned(),
                    err,
                ))))
            }
        }
    }

    async fn do_describe_statement<C>(
        &self,
        _client: &mut C,
        stmt: &StoredStatement<Self::Statement>,
    ) -> PgWireResult<DescribeStatementResponse>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        let login_info = LoginInfo::from_client_info(_client);
        let database = login_info.database().map(|s| s.to_string());

        // Use shared connection for describe operations (quick, don't need parallel execution)
        let connection = get_shared_connection().ok_or_else(|| {
            PgWireError::UserError(Box::new(ErrorInfo::new(
                "ERROR".to_owned(),
                "XX000".to_owned(),
                "No shared connection available".to_owned(),
            )))
        })?;
        let statement = stmt.statement.clone();
        let param_types = stmt.parameter_types.clone();
        let server_host = self.server_host.clone();
        let server_port = self.server_port;

        tokio::task::spawn_blocking(move || -> PgWireResult<DescribeStatementResponse> {
            let guard = connection.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
            let conn = &*guard;

            if let Some(db) = &database {
                if let Some(db_credentials) = ServerRegistry::instance().get_db_credentials(&server_host, server_port) {
                    match check_database_action(db, &db_credentials) {
                        DatabaseAction::SetDatabase => {
                            let _ = conn.execute(&format!("USE \"{}\"", db.replace('"', "\"\"")), params![]);
                        }
                        _ => {}
                    }
                }
            }

            let hana_credentials = get_hana_credentials_if_available(&database, &server_host, server_port);

            let (actual_statement, fallback_statement) = if let Some(hana_creds) = &hana_credentials {
                (wrap_query_for_hana(&statement, hana_creds), Some(statement.clone()))
            } else {
                (statement.clone(), None)
            };

            let fallback_ref = fallback_statement.as_deref();
            let stmt = execute_with_fallback(&actual_statement, fallback_ref, |query_str| {
                conn.prepare(query_str)
            }).map_err(|e| PgWireError::ApiError(Box::new(e)))?;

            let fields = row_desc_from_stmt(&stmt, &Format::UnifiedBinary)?;
            let param_types_unwrapped: Vec<Type> = param_types.into_iter().filter_map(|t| t).collect();
            Ok(DescribeStatementResponse::new(param_types_unwrapped, fields))
        })
        .await
        .map_err(|e| {
            PgWireError::UserError(Box::new(ErrorInfo::new(
                "ERROR".to_owned(),
                "XX000".to_owned(),
                format!("Task execution failed: {}", e),
            )))
        })?
    }

    async fn do_describe_portal<C>(
        &self,
        _client: &mut C,
        portal: &Portal<Self::Statement>,
    ) -> PgWireResult<DescribePortalResponse>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        let login_info = LoginInfo::from_client_info(_client);
        let database = login_info.database().map(|s| s.to_string());

        // Use shared connection for describe operations (quick, don't need parallel execution)
        let connection = get_shared_connection().ok_or_else(|| {
            PgWireError::UserError(Box::new(ErrorInfo::new(
                "ERROR".to_owned(),
                "XX000".to_owned(),
                "No shared connection available".to_owned(),
            )))
        })?;
        let statement = portal.statement.statement.clone();
        let format = portal.result_column_format.clone();
        let server_host = self.server_host.clone();
        let server_port = self.server_port;

        tokio::task::spawn_blocking(move || -> PgWireResult<DescribePortalResponse> {
            let guard = connection.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
            let conn = &*guard;

            if let Some(db) = &database {
                if let Some(db_credentials) = ServerRegistry::instance().get_db_credentials(&server_host, server_port) {
                    match check_database_action(db, &db_credentials) {
                        DatabaseAction::SetDatabase => {
                            let _ = conn.execute(&format!("USE \"{}\"", db.replace('"', "\"\"")), params![]);
                        }
                        _ => {}
                    }
                }
            }

            let hana_credentials = get_hana_credentials_if_available(&database, &server_host, server_port);

            let (actual_statement, fallback_statement) = if let Some(hana_creds) = &hana_credentials {
                (wrap_query_for_hana(&statement, hana_creds), Some(statement.clone()))
            } else {
                (statement.clone(), None)
            };

            let fallback_ref = fallback_statement.as_deref();
            let stmt = execute_with_fallback(&actual_statement, fallback_ref, |query_str| {
                conn.prepare(query_str)
            }).map_err(|e| PgWireError::ApiError(Box::new(e)))?;

            let fields = row_desc_from_stmt(&stmt, &format)?;
            Ok(DescribePortalResponse::new(fields))
        })
        .await
        .map_err(|e| {
            PgWireError::UserError(Box::new(ErrorInfo::new(
                "ERROR".to_owned(),
                "XX000".to_owned(),
                format!("Task execution failed: {}", e),
            )))
        })?
    }
}

pub struct TrexPgWireServerFactory {
    query_handler: Arc<TrexQueryHandler>,
}

impl TrexPgWireServerFactory {
    pub fn new(executor: Arc<QueryExecutor>, host: String, port: u16, worker_id: usize) -> Self {
        Self {
            query_handler: Arc::new(TrexQueryHandler::new(executor, host, port, worker_id)),
        }
    }
}

impl PgWireServerHandlers for TrexPgWireServerFactory {
    fn simple_query_handler(&self) -> Arc<impl SimpleQueryHandler> {
        self.query_handler.clone()
    }

    fn extended_query_handler(&self) -> Arc<impl ExtendedQueryHandler> {
        self.query_handler.clone()
    }

    fn startup_handler(&self) -> Arc<impl StartupHandler> {
        Arc::new(NoopHandler)
    }
}

pub struct TrexPgWireServerWithAuth {
    query_handler: Arc<TrexQueryHandler>,
    password: String,
}

impl TrexPgWireServerWithAuth {
    pub fn new(
        executor: Arc<QueryExecutor>,
        password: String,
        host: String,
        port: u16,
        worker_id: usize,
    ) -> Self {
        Self {
            query_handler: Arc::new(TrexQueryHandler::new(executor, host, port, worker_id)),
            password,
        }
    }
}

impl PgWireServerHandlers for TrexPgWireServerWithAuth {
    fn simple_query_handler(&self) -> Arc<impl SimpleQueryHandler> {
        self.query_handler.clone()
    }

    fn extended_query_handler(&self) -> Arc<impl ExtendedQueryHandler> {
        self.query_handler.clone()
    }

    fn startup_handler(&self) -> Arc<impl StartupHandler> {
        let auth_source = SimpleAuthSource::new(self.password.clone());
        let parameter_provider = DefaultServerParameterProvider::default();
        let mut scram_auth = ScramAuth::new(Arc::new(auth_source));
        scram_auth.set_iterations(SCRAM_ITERATIONS);
        let sasl_handler = SASLAuthStartupHandler::new(Arc::new(parameter_provider))
            .with_scram(scram_auth);
        Arc::new(sasl_handler)
    }
}

pub fn start_pgwire_server_capi(
    host: String,
    port: u16,
    password: Option<&str>,
    db_credentials: String,
) -> Result<String, String> {
    if ServerRegistry::instance().is_server_running(&host, port) {
        return Err(format!("Server already running on {}:{}", host, port));
    }

    let (shutdown_tx, mut shutdown_rx) = oneshot::channel::<()>();

    let server_host = host.clone();
    let server_port = port;
    let success_host = host.clone();
    let password_opt = password.map(|s| s.to_string());
    
    let thread_handle = thread::Builder::new()
        .name(format!("pgwire-server-{}:{}", host, port))
        .spawn(move || -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()?;

            let result = rt.block_on(async move {
                let listener = TcpListener::bind(format!("{}:{}", server_host, server_port)).await?;
                log_debug(&format!("Bound to {}:{}", server_host, server_port));

                let executor = get_query_executor().ok_or_else(|| {
                    log_debug("No query executor available");
                    std::io::Error::new(std::io::ErrorKind::Other, "No query executor available")
                })?;
                log_debug(&format!("Using query executor with {} workers", executor.pool_size()));

                // Treat empty password as no authentication
                if let Some(required_password) = password_opt.filter(|p| !p.is_empty()) {
                    loop {
                        tokio::select! {
                            _ = &mut shutdown_rx => break,
                            result = listener.accept() => {
                                match result {
                                    Ok((socket, _addr)) => {
                                        let worker_id = executor.next_worker_id();
                                        let handlers = Arc::new(TrexPgWireServerWithAuth::new(executor.clone(), required_password.to_string(), server_host.clone(), server_port, worker_id));
                                        tokio::spawn(async move {
                                            let _ = process_socket(socket, None, handlers).await;
                                        });
                                    }
                                    Err(_) => break,
                                }
                            }
                        }
                    }
                } else {
                    eprintln!("WARNING: pgwire starting without authentication — all connections will have full access");
                    log_debug("Using no-auth mode");

                    loop {
                        tokio::select! {
                            _ = &mut shutdown_rx => {
                                log_debug("Received shutdown signal");
                                break;
                            }
                            result = listener.accept() => {
                                match result {
                                    Ok((socket, addr)) => {
                                        log_debug(&format!("New connection from {:?}", addr));
                                        let worker_id = executor.next_worker_id();
                                        let handlers = Arc::new(TrexPgWireServerFactory::new(executor.clone(), server_host.clone(), server_port, worker_id));
                                        tokio::spawn(async move {
                                            log_debug("Processing socket...");
                                            let result = process_socket(socket, None, handlers).await;
                                            log_debug(&format!("Socket result: {:?}", result));
                                        });
                                    }
                                    Err(e) => {
                                        log_debug(&format!("Accept error: {}", e));
                                        break;
                                    }
                                }
                            }
                        }
                    }
                }
                
                Ok(())
            });
            
            result
        })
        .map_err(|e| format!("Failed to spawn server thread: {}", e))?;

    let start_time = SystemTime::now();
    let server_handle = ServerHandle {
        thread_handle,
        shutdown_tx,
        start_time,
        db_credentials,
    };
    
    ServerRegistry::instance().register_server(host.clone(), port, server_handle)?;

    Ok(format!("Started pgwire server on {}:{}", success_host, port))
}

pub fn stop_pgwire_server(host: &str, port: u16) -> Result<String, String> {
    ServerRegistry::instance().stop_server(host, port)
}
