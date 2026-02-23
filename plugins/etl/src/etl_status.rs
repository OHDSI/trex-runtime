use std::ffi::CString;
use std::sync::atomic::{AtomicBool, Ordering};

use duckdb::core::{DataChunkHandle, Inserter, LogicalTypeHandle, LogicalTypeId};
use duckdb::vtab::{BindInfo, InitInfo, TableFunctionInfo, VTab};

use crate::pipeline_registry;

pub struct EtlStatusTable;

#[repr(C)]
pub struct EtlStatusBindData {}

#[repr(C)]
pub struct EtlStatusInitData {
    done: AtomicBool,
}

impl VTab for EtlStatusTable {
    type InitData = EtlStatusInitData;
    type BindData = EtlStatusBindData;

    fn bind(bind: &BindInfo) -> Result<Self::BindData, Box<dyn std::error::Error>> {
        bind.add_result_column("name", LogicalTypeHandle::from(LogicalTypeId::Varchar));
        bind.add_result_column("state", LogicalTypeHandle::from(LogicalTypeId::Varchar));
        bind.add_result_column("mode", LogicalTypeHandle::from(LogicalTypeId::Varchar));
        bind.add_result_column("connection", LogicalTypeHandle::from(LogicalTypeId::Varchar));
        bind.add_result_column("publication", LogicalTypeHandle::from(LogicalTypeId::Varchar));
        bind.add_result_column("snapshot", LogicalTypeHandle::from(LogicalTypeId::Varchar));
        bind.add_result_column(
            "rows_replicated",
            LogicalTypeHandle::from(LogicalTypeId::Varchar),
        );
        bind.add_result_column(
            "last_activity",
            LogicalTypeHandle::from(LogicalTypeId::Varchar),
        );
        bind.add_result_column("error", LogicalTypeHandle::from(LogicalTypeId::Varchar));
        Ok(EtlStatusBindData {})
    }

    fn init(_: &InitInfo) -> Result<Self::InitData, Box<dyn std::error::Error>> {
        Ok(EtlStatusInitData {
            done: AtomicBool::new(false),
        })
    }

    fn func(
        func: &TableFunctionInfo<Self>,
        output: &mut DataChunkHandle,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let init_data = func.get_init_data();

        if init_data.done.swap(true, Ordering::Relaxed) {
            output.set_len(0);
            return Ok(());
        }

        let pipelines = pipeline_registry::registry().get_all_info();

        if pipelines.is_empty() {
            output.set_len(0);
            return Ok(());
        }

        let chunk_size = pipelines.len();
        let name_vector = output.flat_vector(0);
        let state_vector = output.flat_vector(1);
        let mode_vector = output.flat_vector(2);
        let connection_vector = output.flat_vector(3);
        let publication_vector = output.flat_vector(4);
        let snapshot_vector = output.flat_vector(5);
        let rows_vector = output.flat_vector(6);
        let activity_vector = output.flat_vector(7);
        let error_vector = output.flat_vector(8);

        for (i, info) in pipelines.iter().enumerate() {
            let name = CString::new(info.name.clone())?;
            name_vector.insert(i, name);

            let state = CString::new(info.state.as_str())?;
            state_vector.insert(i, state);

            let mode = CString::new(info.mode.as_str())?;
            mode_vector.insert(i, mode);

            let conn = CString::new(info.connection_string.clone())?;
            connection_vector.insert(i, conn);

            let pub_name = CString::new(info.publication.clone())?;
            publication_vector.insert(i, pub_name);

            let snapshot = CString::new(info.snapshot_enabled.to_string())?;
            snapshot_vector.insert(i, snapshot);

            let rows = CString::new(info.rows_replicated.to_string())?;
            rows_vector.insert(i, rows);

            let activity = CString::new(
                info.last_activity
                    .map(|t| {
                        t.duration_since(std::time::UNIX_EPOCH)
                            .map(|d| d.as_secs().to_string())
                            .unwrap_or_default()
                    })
                    .unwrap_or_default(),
            )?;
            activity_vector.insert(i, activity);

            let error = CString::new(info.error_message.clone().unwrap_or_default())?;
            error_vector.insert(i, error);
        }

        output.set_len(chunk_size);
        Ok(())
    }

    fn parameters() -> Option<Vec<LogicalTypeHandle>> {
        None
    }
}
