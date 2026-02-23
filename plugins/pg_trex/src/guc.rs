use pgrx::{GucContext, GucFlags, GucRegistry, GucSetting};
use std::ffi::CString;

pub static GOSSIP_ADDR: GucSetting<Option<CString>> =
    GucSetting::<Option<CString>>::new(Some(c"127.0.0.1:7946"));

pub static FLIGHT_ADDR: GucSetting<Option<CString>> =
    GucSetting::<Option<CString>>::new(Some(c"127.0.0.1:50051"));

pub static SEEDS: GucSetting<Option<CString>> =
    GucSetting::<Option<CString>>::new(Some(c""));

pub static CLUSTER_ID: GucSetting<Option<CString>> =
    GucSetting::<Option<CString>>::new(Some(c"pg_trex"));

pub static NODE_NAME: GucSetting<Option<CString>> =
    GucSetting::<Option<CString>>::new(Some(c""));

pub static DATA_NODE: GucSetting<bool> = GucSetting::<bool>::new(true);

pub static POOL_SIZE: GucSetting<i32> = GucSetting::<i32>::new(4);

pub static CATALOG_REFRESH_SECS: GucSetting<i32> = GucSetting::<i32>::new(30);

pub static EXTENSION_DIR: GucSetting<Option<CString>> =
    GucSetting::<Option<CString>>::new(Some(c""));

pub static DATABASE: GucSetting<Option<CString>> =
    GucSetting::<Option<CString>>::new(Some(c""));

/// Get a string GUC value as an owned String, with a fallback default.
pub fn get_str(setting: &GucSetting<Option<CString>>, default: &str) -> String {
    setting
        .get()
        .and_then(|c| c.into_string().ok())
        .unwrap_or_else(|| default.to_string())
}

pub fn register_gucs() {
    GucRegistry::define_string_guc(
        c"pg_trex.gossip_addr",
        c"Gossip bind address for cluster membership",
        c"Address and port for the gossip protocol listener",
        &GOSSIP_ADDR,
        GucContext::Sighup,
        GucFlags::default(),
    );

    GucRegistry::define_string_guc(
        c"pg_trex.flight_addr",
        c"Arrow Flight server bind address",
        c"Address and port for the Flight SQL server",
        &FLIGHT_ADDR,
        GucContext::Sighup,
        GucFlags::default(),
    );

    GucRegistry::define_string_guc(
        c"pg_trex.seeds",
        c"Comma-separated seed node gossip addresses",
        c"Initial nodes to contact for cluster discovery",
        &SEEDS,
        GucContext::Sighup,
        GucFlags::default(),
    );

    GucRegistry::define_string_guc(
        c"pg_trex.cluster_id",
        c"Cluster identifier",
        c"Nodes with the same cluster_id form a cluster",
        &CLUSTER_ID,
        GucContext::Postmaster,
        GucFlags::default(),
    );

    GucRegistry::define_string_guc(
        c"pg_trex.node_name",
        c"Node name within the cluster",
        c"Defaults to hostname if empty",
        &NODE_NAME,
        GucContext::Postmaster,
        GucFlags::default(),
    );

    GucRegistry::define_bool_guc(
        c"pg_trex.data_node",
        c"Whether this node advertises local tables",
        c"Set to false for query-only coordinator nodes",
        &DATA_NODE,
        GucContext::Sighup,
        GucFlags::default(),
    );

    GucRegistry::define_int_guc(
        c"pg_trex.pool_size",
        c"Number of worker threads in the query executor pool",
        c"Each thread holds a cloned trexsql connection for parallel query execution",
        &POOL_SIZE,
        1,
        64,
        GucContext::Postmaster,
        GucFlags::default(),
    );

    GucRegistry::define_int_guc(
        c"pg_trex.catalog_refresh_secs",
        c"Catalog refresh interval in seconds",
        c"How often the worker refreshes the distributed catalog from the cluster",
        &CATALOG_REFRESH_SECS,
        1,
        3600,
        GucContext::Sighup,
        GucFlags::default(),
    );

    GucRegistry::define_string_guc(
        c"pg_trex.extension_dir",
        c"Directory containing trexsql extension files",
        c"If set, all .trex files in this directory are loaded at startup",
        &EXTENSION_DIR,
        GucContext::Postmaster,
        GucFlags::default(),
    );

    GucRegistry::define_string_guc(
        c"pg_trex.database",
        c"PostgreSQL database for SPI connections",
        c"Database the background worker connects to for pg_scan SPI queries. Falls back to POSTGRES_DB env var, then 'postgres'.",
        &DATABASE,
        GucContext::Postmaster,
        GucFlags::default(),
    );
}
