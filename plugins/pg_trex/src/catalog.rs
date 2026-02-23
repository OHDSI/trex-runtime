use std::sync::atomic::Ordering;

use crate::types::*;

/// Check if a table name exists in the distributed catalog (lock-free read).
///
/// Uses a seqlock-style protocol: read generation, read entries, read generation
/// again. If generation changed, a write was in progress so we retry.
pub fn catalog_contains_table(shmem: &PgTrexShmem, table_name: &str) -> bool {
    loop {
        let gen1 = shmem.catalog.generation.load(Ordering::Acquire);

        // Odd generation means a write is in progress -- spin
        if gen1 % 2 != 0 {
            std::hint::spin_loop();
            continue;
        }

        std::sync::atomic::fence(Ordering::Acquire);

        let count = shmem.catalog.count.load(Ordering::Acquire) as usize;
        let count = count.min(MAX_CATALOG_ENTRIES);

        let mut found = false;
        for i in 0..count {
            let entry = &shmem.catalog.entries[i];
            if entry.table_name_str() == table_name {
                found = true;
                break;
            }
        }

        std::sync::atomic::fence(Ordering::Acquire);
        let gen2 = shmem.catalog.generation.load(Ordering::Acquire);
        if gen1 == gen2 {
            return found;
        }
        // Generation changed -- writer was active, retry
        std::hint::spin_loop();
    }
}

/// Read all catalog entries (lock-free seqlock read).
pub fn read_catalog(shmem: &PgTrexShmem) -> Vec<CatalogEntry> {
    loop {
        let gen1 = shmem.catalog.generation.load(Ordering::Acquire);

        if gen1 % 2 != 0 {
            std::hint::spin_loop();
            continue;
        }

        std::sync::atomic::fence(Ordering::Acquire);

        let count = shmem.catalog.count.load(Ordering::Acquire) as usize;
        let count = count.min(MAX_CATALOG_ENTRIES);

        let mut entries = Vec::with_capacity(count);
        for i in 0..count {
            entries.push(shmem.catalog.entries[i]);
        }

        std::sync::atomic::fence(Ordering::Acquire);
        let gen2 = shmem.catalog.generation.load(Ordering::Acquire);
        if gen1 == gen2 {
            return entries;
        }
        std::hint::spin_loop();
    }
}

/// Refresh the distributed catalog from trex_db_tables() query results.
///
/// Called by the background worker periodically. Uses seqlock write protocol:
/// 1. Increment generation to odd (write in progress)
/// 2. Write entries
/// 3. Increment generation to even (write complete)
pub fn refresh_catalog(
    shmem: &PgTrexShmem,
    conn: &duckdb::Connection,
) -> Result<usize, String> {
    let mut stmt = conn
        .prepare("SELECT schema_name, table_name, node_name, approx_rows FROM trex_db_tables()")
        .map_err(|e| format!("prepare trex_db_tables: {}", e))?;

    let rows = stmt
        .query_map(duckdb::params![], |row| {
            Ok((
                row.get::<_, String>(0).unwrap_or_default(),
                row.get::<_, String>(1).unwrap_or_default(),
                row.get::<_, String>(2).unwrap_or_default(),
                row.get::<_, u64>(3).unwrap_or(0),
            ))
        })
        .map_err(|e| format!("query trex_db_tables: {}", e))?;

    let mut new_entries: Vec<CatalogEntry> = Vec::new();
    for row_result in rows {
        let (schema, table, node, approx) =
            row_result.map_err(|e| format!("read trex_db_tables row: {}", e))?;

        if new_entries.len() >= MAX_CATALOG_ENTRIES {
            pgrx::warning!(
                "pg_trex: catalog truncated at {} entries (max {})",
                new_entries.len(),
                MAX_CATALOG_ENTRIES
            );
            break;
        }

        let mut entry = CatalogEntry::default();
        copy_str_to_buf(&schema, &mut entry.schema_name);
        copy_str_to_buf(&table, &mut entry.table_name);
        copy_str_to_buf(&node, &mut entry.node_name);
        entry.approx_rows = approx;
        new_entries.push(entry);
    }

    let count = new_entries.len();

    // Seqlock write protocol
    // Step 1: AcqRel prevents subsequent writes from reordering before generation increment
    shmem
        .catalog
        .generation
        .fetch_add(1, Ordering::AcqRel);

    // Step 2: write count and entries
    shmem
        .catalog
        .count
        .store(count as u32, Ordering::Release);

    for (i, entry) in new_entries.iter().enumerate() {
        // Safety: we're the only writer (background worker), and readers use
        // seqlock-style generation checks to detect torn reads.
        unsafe {
            let base = shmem.catalog.entries.as_ptr() as *mut CatalogEntry;
            std::ptr::write(base.add(i), *entry);
        }
    }

    // Step 3: increment generation to even (write complete)
    shmem
        .catalog
        .generation
        .fetch_add(1, Ordering::Release);

    pgrx::debug2!("pg_trex: catalog refreshed with {} entries", count);

    Ok(count)
}

/// Copy a string into a fixed-size null-terminated byte buffer.
fn copy_str_to_buf(s: &str, buf: &mut [u8; 64]) {
    let bytes = s.as_bytes();
    let len = bytes.len().min(63); // leave room for null terminator
    buf[..len].copy_from_slice(&bytes[..len]);
    buf[len] = 0;
    for b in &mut buf[len + 1..] {
        *b = 0;
    }
}
