pub fn fhir_to_duckdb_type(fhir_type: &str) -> &'static str {
    match fhir_type {
        "boolean" => "BOOLEAN",
        "integer" => "INTEGER",
        "positiveInt" => "UINTEGER",
        "unsignedInt" => "UINTEGER",
        "decimal" => "DOUBLE",
        "string" | "code" | "id" | "markdown" | "uri" | "url" | "canonical" | "oid" | "uuid" => {
            "VARCHAR"
        }
        "date" | "dateTime" => "VARCHAR", // Partial precision prevents DATE/TIMESTAMP
        "instant" => "TIMESTAMP",
        "time" => "TIME",
        "base64Binary" => "VARCHAR",
        "xhtml" => "VARCHAR",
        _ => "VARCHAR",
    }
}

pub fn is_primitive_type(type_code: &str) -> bool {
    matches!(
        type_code,
        "boolean"
            | "integer"
            | "positiveInt"
            | "unsignedInt"
            | "decimal"
            | "string"
            | "code"
            | "id"
            | "markdown"
            | "uri"
            | "url"
            | "canonical"
            | "oid"
            | "uuid"
            | "date"
            | "dateTime"
            | "instant"
            | "time"
            | "base64Binary"
            | "xhtml"
    )
}

pub fn is_complex_type(type_code: &str) -> bool {
    !is_primitive_type(type_code) && type_code != "Resource" && type_code != "Element"
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_primitive_mappings() {
        assert_eq!(fhir_to_duckdb_type("boolean"), "BOOLEAN");
        assert_eq!(fhir_to_duckdb_type("integer"), "INTEGER");
        assert_eq!(fhir_to_duckdb_type("positiveInt"), "UINTEGER");
        assert_eq!(fhir_to_duckdb_type("decimal"), "DOUBLE");
        assert_eq!(fhir_to_duckdb_type("string"), "VARCHAR");
        assert_eq!(fhir_to_duckdb_type("code"), "VARCHAR");
        assert_eq!(fhir_to_duckdb_type("dateTime"), "VARCHAR");
        assert_eq!(fhir_to_duckdb_type("instant"), "TIMESTAMP");
        assert_eq!(fhir_to_duckdb_type("time"), "TIME");
    }

    #[test]
    fn test_is_primitive() {
        assert!(is_primitive_type("boolean"));
        assert!(is_primitive_type("string"));
        assert!(!is_primitive_type("HumanName"));
        assert!(!is_primitive_type("CodeableConcept"));
    }
}
