use std::sync::{Arc, Mutex, MutexGuard};

use async_trait::async_trait;

use duckdb::Rows;

use crate::sql::auth::{get_startup_handler, AuthType, TrexAuthSource};

use duckdb::{params, Connection, Statement, ToSql};
use pgwire::api::auth::LoginInfo;
use pgwire::api::portal::{Format, Portal};
use pgwire::api::query::{ExtendedQueryHandler, SimpleQueryHandler};
use pgwire::api::results::{
  DescribePortalResponse, DescribeStatementResponse, FieldInfo, QueryResponse,
  Response, Tag,
};

use crate::clients::pgwire::{encode_row_data, into_pg_type};
use pgwire::api::auth::md5pass::Md5PasswordAuthStartupHandler;
use pgwire::api::auth::DefaultServerParameterProvider;
use pgwire::api::copy::NoopCopyHandler;
use pgwire::api::stmt::{NoopQueryParser, StoredStatement};
use pgwire::api::{ClientInfo, Type};
use pgwire::api::{NoopErrorHandler, PgWireServerHandlers};
use pgwire::error::{PgWireError, PgWireResult};

use tracing::info;

pub struct TrexDuckDB {
  conn: Arc<Mutex<Connection>>,
  query_parser: Arc<NoopQueryParser>,
}

pub struct TrexDuckDBFactory {
  pub handler: Arc<TrexDuckDB>,
  pub auth_type: AuthType,
}

impl PgWireServerHandlers for TrexDuckDBFactory {
  type StartupHandler = Md5PasswordAuthStartupHandler<
    TrexAuthSource,
    DefaultServerParameterProvider,
  >;
  type SimpleQueryHandler = TrexDuckDB;
  type ExtendedQueryHandler = TrexDuckDB;
  type CopyHandler = NoopCopyHandler;
  type ErrorHandler = NoopErrorHandler;

  fn simple_query_handler(&self) -> Arc<Self::SimpleQueryHandler> {
    self.handler.clone()
  }

  fn extended_query_handler(&self) -> Arc<Self::ExtendedQueryHandler> {
    self.handler.clone()
  }

  fn startup_handler(&self) -> Arc<Self::StartupHandler> {
    get_startup_handler(&self.auth_type)
  }

  fn copy_handler(&self) -> Arc<Self::CopyHandler> {
    Arc::new(NoopCopyHandler)
  }

  fn error_handler(&self) -> Arc<Self::ErrorHandler> {
    Arc::new(NoopErrorHandler)
  }
}

#[async_trait]
impl SimpleQueryHandler for TrexDuckDB {
  async fn do_query<'a, C>(
    &self,
    _client: &mut C,
    query: &'a str,
  ) -> PgWireResult<Vec<Response<'a>>>
  where
    C: ClientInfo + Unpin + Send + Sync,
  {
    let login_info = LoginInfo::from_client_info(_client);
    let db = login_info.database().unwrap();

    let mut _query = &query.replace("::regclass", "::string")
        .replace("AND datallowconn AND NOT datistemplate", "AND NOT db.datname =('system') AND NOT db.datname =('temp')")
        .replace("pg_get_expr(ad.adbin, ad.adrelid, true)","pg_get_expr(ad.adbin, ad.adrelid)")
        .replace("pg_catalog.pg_relation_size(i.indexrelid)","''")
        .replace("pg_catalog.pg_stat_get_numscans(i.indexrelid)","''")
        .replace("pg_catalog.pg_inherits i,pg_catalog.pg_class c WHERE",
        "(select 0 as inhseqno, 0 as inhrelid, 0 as inhparent) as i join pg_catalog.pg_class as c ON")
        .replace("SELECT c.oid,c.*,t.relname as tabrelname,rt.relnamespace as refnamespace,d.description, null as consrc_copy",
        "SELECT c.oid,t.relname  as tabrelname,rt.relnamespace as refnamespace,d.description, null as consrc_copy");

    info!("TREX_DATABASE: {:?}", db);
    info!("QUERY: {_query}");

    let conn = self.conn.lock().unwrap();
    set_db(&conn, db);
    if _query.to_uppercase().starts_with("SELECT") {
      let mut stmt = conn
        .prepare(_query)
        .map_err(|e| PgWireError::ApiError(Box::new(e)))?;
      //let header = Arc::new(row_desc_from_stmt(&stmt, &Format::UnifiedText)?);
      let _rows = stmt.query(params![]);

      _rows
        .map(|rows| {
          let header =
            Arc::new(row_desc_from_row(&rows, &Format::UnifiedText).unwrap());
          let s = encode_row_data(rows, header.clone());
          vec![Response::Query(QueryResponse::new(header, s))]
        })
        .map_err(|e| PgWireError::ApiError(Box::new(e)))
    } else {
      conn
        .execute(_query, params![])
        .map(|affected_rows| {
          vec![Response::Execution(Tag::new("OK").with_rows(affected_rows))]
        })
        .map_err(|e| PgWireError::ApiError(Box::new(e)))
    }
  }
}

fn set_db(conn: &MutexGuard<'_, Connection>, db: &str) {
  let _ = conn
    .execute(&format!("USE {db}"), params![])
    .map_err(|error| println!("ERROR: {error}"));
}

fn row_desc_from_row(
  rows: &Rows,
  format: &Format,
) -> PgWireResult<Vec<FieldInfo>> {
  let stmt = rows.as_ref().unwrap();
  let columns = stmt.column_count();

  (0..columns)
    .map(|idx| {
      let datatype = stmt.column_type(idx);
      let name = stmt.column_name(idx).unwrap();

      Ok(FieldInfo::new(
        name.clone(),
        None,
        None,
        into_pg_type(&datatype).unwrap(),
        format.format_for(idx),
      ))
    })
    .collect()
}

fn row_desc_from_stmt(
  stmt: &Statement,
  format: &Format,
) -> PgWireResult<Vec<FieldInfo>> {
  let columns = stmt.column_count();

  (0..columns)
    .map(|idx| {
      let datatype = stmt.column_type(idx);
      let name = stmt.column_name(idx).unwrap();

      Ok(FieldInfo::new(
        name.clone(),
        None,
        None,
        into_pg_type(&datatype).unwrap(),
        format.format_for(idx),
      ))
    })
    .collect()
}

#[async_trait]
impl ExtendedQueryHandler for TrexDuckDB {
  type Statement = String;
  type QueryParser = NoopQueryParser;

  fn query_parser(&self) -> Arc<Self::QueryParser> {
    self.query_parser.clone()
  }

  async fn do_query<'a, C>(
    &self,
    _client: &mut C,
    portal: &'a Portal<Self::Statement>,
    _max_rows: usize,
  ) -> PgWireResult<Response<'a>>
  where
    C: ClientInfo + Unpin + Send + Sync,
  {
    let conn = self.conn.lock().unwrap();
    let query = &portal.statement.statement;
    let mut stmt = conn
      .prepare_cached(query)
      .map_err(|e| PgWireError::ApiError(Box::new(e)))?;
    let params = get_params(portal);
    let params_ref = params
      .iter()
      .map(|f| f.as_ref())
      .collect::<Vec<&dyn duckdb::ToSql>>();

    if query.to_uppercase().starts_with("SELECT") {
      let header =
        Arc::new(row_desc_from_stmt(&stmt, &portal.result_column_format)?);
      stmt
        .query::<&[&dyn duckdb::ToSql]>(params_ref.as_ref())
        .map(|rows| {
          let s = encode_row_data(rows, header.clone());
          Response::Query(QueryResponse::new(header, s))
        })
        .map_err(|e| PgWireError::ApiError(Box::new(e)))
    } else {
      stmt
        .execute::<&[&dyn duckdb::ToSql]>(params_ref.as_ref())
        .map(|affected_rows| {
          Response::Execution(Tag::new("OK").with_rows(affected_rows))
        })
        .map_err(|e| PgWireError::ApiError(Box::new(e)))
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
    let conn = self.conn.lock().unwrap();
    let param_types = stmt.parameter_types.clone();
    let stmt = conn
      .prepare_cached(&stmt.statement)
      .map_err(|e| PgWireError::ApiError(Box::new(e)))?;
    row_desc_from_stmt(&stmt, &Format::UnifiedBinary)
      .map(|fields| DescribeStatementResponse::new(param_types, fields))
  }

  async fn do_describe_portal<C>(
    &self,
    _client: &mut C,
    portal: &Portal<Self::Statement>,
  ) -> PgWireResult<DescribePortalResponse>
  where
    C: ClientInfo + Unpin + Send + Sync,
  {
    let conn = self.conn.lock().unwrap();
    let stmt = conn
      .prepare_cached(&portal.statement.statement)
      .map_err(|e| PgWireError::ApiError(Box::new(e)))?;
    row_desc_from_stmt(&stmt, &portal.result_column_format)
      .map(DescribePortalResponse::new)
  }
}

fn get_params(portal: &Portal<String>) -> Vec<Box<dyn ToSql>> {
  let mut results = Vec::with_capacity(portal.parameter_len());
  for i in 0..portal.parameter_len() {
    let param_type = portal.statement.parameter_types.get(i).unwrap();
    // we only support a small amount of types for demo
    match param_type {
      &Type::BOOL => {
        let param = portal.parameter::<bool>(i, param_type).unwrap();
        results.push(Box::new(param) as Box<dyn ToSql>);
      }
      &Type::INT2 => {
        let param = portal.parameter::<i16>(i, param_type).unwrap();
        results.push(Box::new(param) as Box<dyn ToSql>);
      }
      &Type::INT4 => {
        let param = portal.parameter::<i32>(i, param_type).unwrap();
        results.push(Box::new(param) as Box<dyn ToSql>);
      }
      &Type::INT8 => {
        let param = portal.parameter::<i64>(i, param_type).unwrap();
        results.push(Box::new(param) as Box<dyn ToSql>);
      }
      &Type::TEXT | &Type::VARCHAR => {
        let param = portal.parameter::<String>(i, param_type).unwrap();
        results.push(Box::new(param) as Box<dyn ToSql>);
      }
      &Type::FLOAT4 => {
        let param = portal.parameter::<f32>(i, param_type).unwrap();
        results.push(Box::new(param) as Box<dyn ToSql>);
      }
      &Type::FLOAT8 => {
        let param = portal.parameter::<f64>(i, param_type).unwrap();
        results.push(Box::new(param) as Box<dyn ToSql>);
      }
      _ => {
        unimplemented!("parameter type not supported")
      }
    }
  }

  results
}

impl TrexDuckDB {
  pub fn new(duckdb: &Arc<Mutex<Connection>>) -> TrexDuckDB {
    TrexDuckDB {
      conn: duckdb.clone(),
      query_parser: Arc::new(NoopQueryParser::new()),
    }
  }
}
