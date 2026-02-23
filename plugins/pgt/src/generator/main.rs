use crate::error::TransformationResult;
use crate::dialects::hana::post_processor::PostProcessor;
use sqlparser::ast::Statement;

pub fn generate_hana_sql(statements: &[Statement]) -> TransformationResult<String> {
    let mut result = String::new();

    for (i, stmt) in statements.iter().enumerate() {
        if i > 0 {
            result.push_str(";\n\n");
        }

        result.push_str(&format!("{}", stmt));
    }

    if !statements.is_empty() {
        result.push(';');
    }

    let post_processor = PostProcessor::new();
    let processed_sql = post_processor.process(&result)?;

    Ok(processed_sql)
}
