use crate::error::AppError;
use crate::fhir::resource_registry::ResourceRegistry;

pub fn validate_dataset_id(id: &str) -> Result<(), AppError> {
    if id.is_empty() || id.len() > 128 {
        return Err(AppError::BadRequest(
            "Dataset ID must be 1-128 characters".to_string(),
        ));
    }
    if !id
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '-')
    {
        return Err(AppError::BadRequest(
            "Dataset ID must contain only alphanumeric characters and hyphens".to_string(),
        ));
    }
    Ok(())
}

pub fn validate_resource_type(resource_type: &str, registry: &ResourceRegistry) -> Result<(), AppError> {
    if resource_type.is_empty() || resource_type.len() > 64 {
        return Err(AppError::BadRequest(
            "Invalid resource type".to_string(),
        ));
    }
    if !resource_type
        .chars()
        .all(|c| c.is_ascii_alphanumeric())
    {
        return Err(AppError::BadRequest(format!(
            "Invalid resource type: '{}'",
            resource_type
        )));
    }
    if !registry.is_known_type(resource_type) {
        return Err(AppError::BadRequest(format!(
            "Unknown resource type: '{}'",
            resource_type
        )));
    }
    Ok(())
}

pub fn validate_fhir_id(id: &str) -> Result<(), AppError> {
    if id.is_empty() || id.len() > 64 {
        return Err(AppError::BadRequest(
            "Resource ID must be 1-64 characters".to_string(),
        ));
    }
    if !id
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '.' || c == '_')
    {
        return Err(AppError::BadRequest(
            "Resource ID contains invalid characters".to_string(),
        ));
    }
    Ok(())
}

pub fn validate_version_id(id: &str) -> Result<(), AppError> {
    match id.parse::<u64>() {
        Ok(v) if v > 0 => Ok(()),
        _ => Err(AppError::BadRequest(
            "Version ID must be a positive integer".to_string(),
        )),
    }
}

pub fn validate_uuid(id: &str) -> Result<(), AppError> {
    if uuid::Uuid::parse_str(id).is_err() {
        return Err(AppError::BadRequest(
            "Invalid UUID format".to_string(),
        ));
    }
    Ok(())
}

pub fn escape_identifier(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

pub fn escape_string(value: &str) -> String {
    value.replace('\'', "''")
}

pub fn to_schema_name(dataset_id: &str) -> String {
    escape_identifier(&dataset_id.replace('-', "_"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validate_dataset_id() {
        assert!(validate_dataset_id("my-dataset").is_ok());
        assert!(validate_dataset_id("abc123").is_ok());
        assert!(validate_dataset_id("").is_err());
        assert!(validate_dataset_id("a".repeat(129).as_str()).is_err());
        assert!(validate_dataset_id("bad;input").is_err());
        assert!(validate_dataset_id("bad'input").is_err());
        assert!(validate_dataset_id("bad\"input").is_err());
    }

    #[test]
    fn test_validate_fhir_id() {
        assert!(validate_fhir_id("abc-123").is_ok());
        assert!(validate_fhir_id("test.id_1").is_ok());
        assert!(validate_fhir_id("").is_err());
        assert!(validate_fhir_id("a".repeat(65).as_str()).is_err());
        assert!(validate_fhir_id("bad;id").is_err());
        assert!(validate_fhir_id("bad'id").is_err());
    }

    #[test]
    fn test_validate_version_id() {
        assert!(validate_version_id("1").is_ok());
        assert!(validate_version_id("42").is_ok());
        assert!(validate_version_id("0").is_err());
        assert!(validate_version_id("-1").is_err());
        assert!(validate_version_id("abc").is_err());
    }

    #[test]
    fn test_escape_identifier() {
        assert_eq!(escape_identifier("foo"), "\"foo\"");
        assert_eq!(escape_identifier("foo\"bar"), "\"foo\"\"bar\"");
    }

    #[test]
    fn test_escape_string() {
        assert_eq!(escape_string("hello"), "hello");
        assert_eq!(escape_string("it's"), "it''s");
    }

    #[test]
    fn test_to_schema_name() {
        assert_eq!(to_schema_name("my-dataset"), "\"my_dataset\"");
        assert_eq!(to_schema_name("plain"), "\"plain\"");
    }
}
