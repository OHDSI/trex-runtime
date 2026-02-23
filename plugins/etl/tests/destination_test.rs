use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use duckdb::Connection;
use etl_lib::destination::Destination;
use etl_lib::types::{
    Cell, DeleteEvent, Event, InsertEvent, PgLsn, RelationEvent, TableId, TableRow,
    TruncateEvent, Type, UpdateEvent,
};
use etl_postgres::types::{ColumnSchema, TableName, TableSchema};

use etl::destination::DuckDbDestination;
use etl::store::DuckDbStore;
use etl::type_mapping::{cell_to_sql_literal, pg_type_to_duckdb};

fn lsn() -> PgLsn {
    PgLsn::from(0)
}

fn setup() -> (Arc<Mutex<Connection>>, DuckDbDestination) {
    let conn = Connection::open_in_memory().expect("Failed to open in-memory connection");
    let conn = Arc::new(Mutex::new(conn));
    let schemas = Arc::new(Mutex::new(HashMap::new()));
    let dest = DuckDbDestination::new(conn.clone(), "test_pipeline".to_string(), schemas);
    (conn, dest)
}

fn make_test_schema() -> TableSchema {
    TableSchema {
        id: TableId::new(1001),
        name: TableName {
            schema: "public".to_string(),
            name: "users".to_string(),
        },
        column_schemas: vec![
            ColumnSchema {
                name: "id".to_string(),
                typ: Type::INT4,
                modifier: -1,
                nullable: false,
                primary: true,
            },
            ColumnSchema {
                name: "name".to_string(),
                typ: Type::VARCHAR,
                modifier: -1,
                nullable: true,
                primary: false,
            },
            ColumnSchema {
                name: "active".to_string(),
                typ: Type::BOOL,
                modifier: -1,
                nullable: true,
                primary: false,
            },
        ],
    }
}

fn relation_event(schema: &TableSchema) -> Event {
    Event::Relation(RelationEvent {
        start_lsn: lsn(),
        commit_lsn: lsn(),
        table_schema: schema.clone(),
    })
}

fn insert_event(table_id: u32, values: Vec<Cell>) -> Event {
    Event::Insert(InsertEvent {
        start_lsn: lsn(),
        commit_lsn: lsn(),
        table_id: TableId::new(table_id),
        table_row: TableRow { values },
    })
}

fn query_count(conn: &Arc<Mutex<Connection>>, sql: &str) -> i64 {
    let c = conn.lock().unwrap();
    let mut stmt = c.prepare(sql).unwrap();
    let mut rows = stmt.query([]).unwrap();
    let row = rows.next().unwrap().unwrap();
    row.get::<_, i64>(0).unwrap()
}

fn query_string(conn: &Arc<Mutex<Connection>>, sql: &str) -> String {
    let c = conn.lock().unwrap();
    let mut stmt = c.prepare(sql).unwrap();
    let mut rows = stmt.query([]).unwrap();
    let row = rows.next().unwrap().unwrap();
    row.get::<_, String>(0).unwrap()
}

#[test]
fn test_pg_type_to_duckdb_basic() {
    assert_eq!(pg_type_to_duckdb(&Type::BOOL), "BOOLEAN");
    assert_eq!(pg_type_to_duckdb(&Type::INT4), "INTEGER");
    assert_eq!(pg_type_to_duckdb(&Type::INT8), "BIGINT");
    assert_eq!(pg_type_to_duckdb(&Type::FLOAT8), "DOUBLE");
    assert_eq!(pg_type_to_duckdb(&Type::VARCHAR), "VARCHAR");
    assert_eq!(pg_type_to_duckdb(&Type::TEXT), "VARCHAR");
    assert_eq!(pg_type_to_duckdb(&Type::TIMESTAMP), "TIMESTAMP");
    assert_eq!(pg_type_to_duckdb(&Type::UUID), "UUID");
    assert_eq!(pg_type_to_duckdb(&Type::JSONB), "JSON");
    assert_eq!(pg_type_to_duckdb(&Type::BYTEA), "BLOB");
}

#[test]
fn test_cell_to_sql_literal() {
    assert_eq!(cell_to_sql_literal(&Cell::Null), "NULL");
    assert_eq!(cell_to_sql_literal(&Cell::Bool(true)), "TRUE");
    assert_eq!(cell_to_sql_literal(&Cell::Bool(false)), "FALSE");
    assert_eq!(cell_to_sql_literal(&Cell::I32(42)), "42");
    assert_eq!(cell_to_sql_literal(&Cell::I64(-100)), "-100");
    assert_eq!(
        cell_to_sql_literal(&Cell::String("hello".to_string())),
        "'hello'"
    );
    assert_eq!(
        cell_to_sql_literal(&Cell::String("it's".to_string())),
        "'it''s'"
    );
}

#[tokio::test]
async fn test_relation_creates_table() {
    let (conn, dest) = setup();
    let schema = make_test_schema();

    let events = vec![relation_event(&schema)];
    dest.write_events(events).await.unwrap();

    let count = query_count(
        &conn,
        "SELECT count(*) FROM information_schema.tables WHERE table_schema='public' AND table_name='users'",
    );
    assert_eq!(count, 1);
}

#[tokio::test]
async fn test_insert_event() {
    let (conn, dest) = setup();
    let schema = make_test_schema();

    let events = vec![
        relation_event(&schema),
        insert_event(1001, vec![
            Cell::I32(1),
            Cell::String("Alice".to_string()),
            Cell::Bool(true),
        ]),
        insert_event(1001, vec![
            Cell::I32(2),
            Cell::String("Bob".to_string()),
            Cell::Bool(false),
        ]),
    ];

    dest.write_events(events).await.unwrap();

    let count = query_count(&conn, "SELECT count(*) FROM \"public\".\"users\"");
    assert_eq!(count, 2);

    let name = query_string(&conn, "SELECT name FROM \"public\".\"users\" WHERE id = 1");
    assert_eq!(name, "Alice");
}

#[tokio::test]
async fn test_update_event() {
    let (conn, dest) = setup();
    let schema = make_test_schema();

    let events = vec![
        relation_event(&schema),
        insert_event(1001, vec![
            Cell::I32(1),
            Cell::String("Alice".to_string()),
            Cell::Bool(true),
        ]),
    ];
    dest.write_events(events).await.unwrap();

    let events = vec![
        relation_event(&schema),
        Event::Update(UpdateEvent {
            start_lsn: lsn(),
            commit_lsn: lsn(),
            table_id: TableId::new(1001),
            table_row: TableRow {
                values: vec![
                    Cell::I32(1),
                    Cell::String("Alice Updated".to_string()),
                    Cell::Bool(false),
                ],
            },
            old_table_row: None,
        }),
    ];
    dest.write_events(events).await.unwrap();

    let name = query_string(&conn, "SELECT name FROM \"public\".\"users\" WHERE id = 1");
    assert_eq!(name, "Alice Updated");
}

#[tokio::test]
async fn test_delete_event() {
    let (conn, dest) = setup();
    let schema = make_test_schema();

    let events = vec![
        relation_event(&schema),
        insert_event(1001, vec![
            Cell::I32(1),
            Cell::String("Alice".to_string()),
            Cell::Bool(true),
        ]),
    ];
    dest.write_events(events).await.unwrap();

    let events = vec![
        relation_event(&schema),
        Event::Delete(DeleteEvent {
            start_lsn: lsn(),
            commit_lsn: lsn(),
            table_id: TableId::new(1001),
            old_table_row: Some((
                false,
                TableRow {
                    values: vec![
                        Cell::I32(1),
                        Cell::String("Alice".to_string()),
                        Cell::Bool(true),
                    ],
                },
            )),
        }),
    ];
    dest.write_events(events).await.unwrap();

    let count = query_count(&conn, "SELECT count(*) FROM \"public\".\"users\"");
    assert_eq!(count, 0);
}

#[tokio::test]
async fn test_truncate_event() {
    let (conn, dest) = setup();
    let schema = make_test_schema();

    let events = vec![
        relation_event(&schema),
        insert_event(1001, vec![
            Cell::I32(1),
            Cell::String("Alice".to_string()),
            Cell::Bool(true),
        ]),
        insert_event(1001, vec![
            Cell::I32(2),
            Cell::String("Bob".to_string()),
            Cell::Bool(false),
        ]),
    ];
    dest.write_events(events).await.unwrap();

    let events = vec![
        relation_event(&schema),
        Event::Truncate(TruncateEvent {
            start_lsn: lsn(),
            commit_lsn: lsn(),
            options: 0,
            rel_ids: vec![1001],
        }),
    ];
    dest.write_events(events).await.unwrap();

    let count = query_count(&conn, "SELECT count(*) FROM \"public\".\"users\"");
    assert_eq!(count, 0);
}

#[tokio::test]
async fn test_store_table_mapping() {
    let conn = Connection::open_in_memory().expect("Failed to open in-memory connection");
    let conn = Arc::new(Mutex::new(conn));
    let schemas = Arc::new(Mutex::new(HashMap::new()));
    let store = DuckDbStore::new(conn.clone(), "test_pipeline".to_string(), schemas);

    use etl_lib::store::state::StateStore;

    store.load_table_replication_states().await.unwrap();

    let table_id = TableId::new(42);
    store
        .store_table_mapping(table_id, "public.users".to_string())
        .await
        .unwrap();

    let mapping = store.get_table_mapping(&table_id).await.unwrap();
    assert_eq!(mapping, Some("public.users".to_string()));

    let persisted = query_string(
        &conn,
        "SELECT destination_table_name FROM _etl_table_mappings WHERE source_table_id = 42",
    );
    assert_eq!(persisted, "public.users");
}
