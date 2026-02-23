use duckdb::core::{DataChunkHandle, Inserter, LogicalTypeId};
use duckdb::vtab::arrow::WritableVector;
use duckdb::vscalar::{ScalarFunctionSignature, VScalar};

use crate::pipeline_registry;

pub struct EtlStopScalar;

impl VScalar for EtlStopScalar {
    type State = ();

    unsafe fn invoke(
        _state: &Self::State,
        input: &mut DataChunkHandle,
        output: &mut dyn WritableVector,
    ) -> Result<(), Box<dyn std::error::Error>> {
        if input.len() == 0 {
            return Err("No input provided".into());
        }

        let name_vector = input.flat_vector(0);
        let name_slice =
            name_vector.as_slice_with_len::<libduckdb_sys::duckdb_string_t>(input.len());

        let pipeline_name = duckdb::types::DuckString::new(&mut { name_slice[0] })
            .as_str()
            .to_string();

        let response = pipeline_registry::registry()
            .stop(&pipeline_name)
            .map_err(|e| -> Box<dyn std::error::Error> { e.into() })?;

        let flat_vector = output.flat_vector();
        flat_vector.insert(0, &response);
        Ok(())
    }

    fn signatures() -> Vec<ScalarFunctionSignature> {
        vec![ScalarFunctionSignature::exact(
            vec![LogicalTypeId::Varchar.into()],
            LogicalTypeId::Varchar.into(),
        )]
    }
}
