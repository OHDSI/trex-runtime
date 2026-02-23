use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ElmLibrary {
    pub identifier: Option<ElmIdentifier>,
    #[serde(default)]
    pub parameters: Option<ElmParameterDefs>,
    #[serde(default)]
    pub statements: Option<ElmStatements>,
    #[serde(default)]
    pub includes: Option<ElmIncludeDefs>,
    #[serde(rename = "usings", default)]
    pub usings: Option<ElmUsingDefs>,
    #[serde(default)]
    pub valueSets: Option<ElmValueSetDefs>,
    #[serde(default)]
    pub codeSystems: Option<ElmCodeSystemDefs>,
    #[serde(default)]
    pub codes: Option<ElmCodeDefs>,
    #[serde(default)]
    pub contexts: Option<ElmContextDefs>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ElmIdentifier {
    pub id: Option<String>,
    pub version: Option<String>,
    pub system: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ElmParameterDefs {
    #[serde(rename = "def", default)]
    pub defs: Vec<ElmParameterDef>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ElmParameterDef {
    pub name: String,
    #[serde(rename = "accessLevel", default)]
    pub access_level: Option<String>,
    #[serde(rename = "parameterTypeSpecifier")]
    pub parameter_type_specifier: Option<ElmTypeSpecifier>,
    pub default: Option<Box<ElmExpression>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ElmStatements {
    #[serde(rename = "def", default)]
    pub defs: Vec<ElmExpressionDef>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ElmExpressionDef {
    pub name: String,
    pub context: Option<String>,
    #[serde(rename = "accessLevel", default)]
    pub access_level: Option<String>,
    pub expression: Box<ElmExpression>,
    #[serde(rename = "resultTypeSpecifier")]
    pub result_type_specifier: Option<ElmTypeSpecifier>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ElmIncludeDefs {
    #[serde(rename = "def", default)]
    pub defs: Vec<ElmIncludeDef>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ElmIncludeDef {
    #[serde(rename = "localIdentifier")]
    pub local_identifier: String,
    pub path: String,
    pub version: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ElmUsingDefs {
    #[serde(rename = "def", default)]
    pub defs: Vec<ElmUsingDef>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ElmUsingDef {
    #[serde(rename = "localIdentifier")]
    pub local_identifier: String,
    pub uri: Option<String>,
    pub version: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ElmValueSetDefs {
    #[serde(rename = "def", default)]
    pub defs: Vec<ElmValueSetDef>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ElmValueSetDef {
    pub name: String,
    pub id: String,
    #[serde(rename = "accessLevel", default)]
    pub access_level: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ElmCodeSystemDefs {
    #[serde(rename = "def", default)]
    pub defs: Vec<ElmCodeSystemDef>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ElmCodeSystemDef {
    pub name: String,
    pub id: String,
    #[serde(rename = "accessLevel", default)]
    pub access_level: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ElmCodeDefs {
    #[serde(rename = "def", default)]
    pub defs: Vec<ElmCodeDef>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ElmCodeDef {
    pub name: String,
    pub id: String,
    pub display: Option<String>,
    #[serde(rename = "codeSystem")]
    pub code_system: Option<ElmCodeSystemRef>,
    #[serde(rename = "accessLevel", default)]
    pub access_level: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ElmCodeSystemRef {
    pub name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ElmContextDefs {
    #[serde(rename = "def", default)]
    pub defs: Vec<ElmContextDef>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ElmContextDef {
    pub name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum ElmTypeSpecifier {
    NamedTypeSpecifier {
        name: String,
    },
    ListTypeSpecifier {
        #[serde(rename = "elementType")]
        element_type: Box<ElmTypeSpecifier>,
    },
    IntervalTypeSpecifier {
        #[serde(rename = "pointType")]
        point_type: Box<ElmTypeSpecifier>,
    },
    TupleTypeSpecifier {
        #[serde(default)]
        element: Vec<ElmTupleElementDef>,
    },
    ChoiceTypeSpecifier {
        #[serde(default)]
        choice: Vec<ElmTypeSpecifier>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ElmTupleElementDef {
    pub name: String,
    #[serde(rename = "elementType")]
    pub element_type: ElmTypeSpecifier,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum ElmExpression {
    Retrieve {
        #[serde(rename = "dataType")]
        data_type: String,
        #[serde(rename = "templateId")]
        template_id: Option<String>,
        #[serde(rename = "codeProperty")]
        code_property: Option<String>,
        codes: Option<Box<ElmExpression>>,
        #[serde(rename = "dateProperty")]
        date_property: Option<String>,
        #[serde(rename = "dateRange")]
        date_range: Option<Box<ElmExpression>>,
    },
    Property {
        source: Option<Box<ElmExpression>>,
        path: String,
        scope: Option<String>,
        #[serde(rename = "resultTypeName")]
        result_type_name: Option<String>,
    },
    FunctionRef {
        name: String,
        #[serde(rename = "libraryName")]
        library_name: Option<String>,
        #[serde(default)]
        operand: Vec<ElmExpression>,
    },

    Query {
        source: Vec<ElmAliasedQuerySource>,
        #[serde(rename = "let", default)]
        let_clauses: Vec<ElmLetClause>,
        relationship: Option<Vec<ElmRelationship>>,
        #[serde(rename = "where")]
        where_clause: Option<Box<ElmExpression>>,
        #[serde(rename = "return")]
        return_clause: Option<ElmReturnClause>,
        sort: Option<ElmSortClause>,
    },
    AliasRef {
        name: String,
    },

    Literal {
        #[serde(rename = "valueType")]
        value_type: Option<String>,
        value: Option<String>,
    },
    Null {
        #[serde(rename = "resultTypeName")]
        result_type_name: Option<String>,
    },
    #[serde(rename = "List")]
    ListExpr {
        #[serde(default)]
        element: Vec<ElmExpression>,
    },
    Tuple {
        #[serde(default)]
        element: Vec<ElmTupleElement>,
    },

    Equal {
        operand: [Box<ElmExpression>; 2],
    },
    NotEqual {
        operand: [Box<ElmExpression>; 2],
    },
    Less {
        operand: [Box<ElmExpression>; 2],
    },
    LessOrEqual {
        operand: [Box<ElmExpression>; 2],
    },
    Greater {
        operand: [Box<ElmExpression>; 2],
    },
    GreaterOrEqual {
        operand: [Box<ElmExpression>; 2],
    },
    Equivalent {
        operand: [Box<ElmExpression>; 2],
    },

    And {
        operand: [Box<ElmExpression>; 2],
    },
    Or {
        operand: [Box<ElmExpression>; 2],
    },
    Not {
        operand: Box<ElmExpression>,
    },
    IsTrue {
        operand: Box<ElmExpression>,
    },
    IsFalse {
        operand: Box<ElmExpression>,
    },

    Add {
        operand: [Box<ElmExpression>; 2],
    },
    Subtract {
        operand: [Box<ElmExpression>; 2],
    },
    Multiply {
        operand: [Box<ElmExpression>; 2],
    },
    Divide {
        operand: [Box<ElmExpression>; 2],
    },
    Modulo {
        operand: [Box<ElmExpression>; 2],
    },
    Negate {
        operand: Box<ElmExpression>,
    },

    Concatenate {
        operand: Vec<ElmExpression>,
    },

    As {
        operand: Box<ElmExpression>,
        #[serde(rename = "asType")]
        as_type: Option<String>,
        #[serde(rename = "asTypeSpecifier")]
        as_type_specifier: Option<ElmTypeSpecifier>,
        strict: Option<bool>,
    },
    Is {
        operand: Box<ElmExpression>,
        #[serde(rename = "isType")]
        is_type: Option<String>,
        #[serde(rename = "isTypeSpecifier")]
        is_type_specifier: Option<ElmTypeSpecifier>,
    },
    ToBoolean {
        operand: Box<ElmExpression>,
    },
    ToInteger {
        operand: Box<ElmExpression>,
    },
    ToDecimal {
        operand: Box<ElmExpression>,
    },
    ToString {
        operand: Box<ElmExpression>,
    },
    ToDateTime {
        operand: Box<ElmExpression>,
    },
    ToDate {
        operand: Box<ElmExpression>,
    },

    Exists {
        operand: Box<ElmExpression>,
    },
    IsNull {
        operand: Box<ElmExpression>,
    },
    Coalesce {
        operand: Vec<ElmExpression>,
    },
    #[serde(rename = "If")]
    IfExpr {
        condition: Box<ElmExpression>,
        then: Box<ElmExpression>,
        #[serde(rename = "else")]
        else_clause: Box<ElmExpression>,
    },
    Case {
        comparand: Option<Box<ElmExpression>>,
        #[serde(rename = "caseItem", default)]
        case_items: Vec<ElmCaseItem>,
        #[serde(rename = "else")]
        else_clause: Box<ElmExpression>,
    },

    ExpressionRef {
        name: String,
        #[serde(rename = "libraryName")]
        library_name: Option<String>,
    },
    ParameterRef {
        name: String,
        #[serde(rename = "libraryName")]
        library_name: Option<String>,
    },
    ValueSetRef {
        name: String,
        #[serde(rename = "libraryName")]
        library_name: Option<String>,
    },
    CodeRef {
        name: String,
        #[serde(rename = "libraryName")]
        library_name: Option<String>,
    },
    CodeSystemRef {
        name: String,
        #[serde(rename = "libraryName")]
        library_name: Option<String>,
    },

    InValueSet {
        code: Box<ElmExpression>,
        valueset: Box<ElmExpression>,
    },
    In {
        operand: [Box<ElmExpression>; 2],
    },
    Contains {
        operand: [Box<ElmExpression>; 2],
    },

    Count {
        source: Box<ElmExpression>,
    },
    Sum {
        source: Box<ElmExpression>,
    },
    Min {
        source: Box<ElmExpression>,
    },
    Max {
        source: Box<ElmExpression>,
    },
    Avg {
        source: Box<ElmExpression>,
    },
    First {
        source: Box<ElmExpression>,
    },
    Last {
        source: Box<ElmExpression>,
    },
    Distinct {
        operand: Box<ElmExpression>,
    },
    Flatten {
        operand: Box<ElmExpression>,
    },
    SingletonFrom {
        operand: Box<ElmExpression>,
    },

    Interval {
        low: Option<Box<ElmExpression>>,
        high: Option<Box<ElmExpression>>,
        #[serde(rename = "lowClosed", default = "default_true")]
        low_closed: bool,
        #[serde(rename = "highClosed", default = "default_true")]
        high_closed: bool,
    },

    Now {},
    Today {},
    DateTime {
        year: Box<ElmExpression>,
        month: Option<Box<ElmExpression>>,
        day: Option<Box<ElmExpression>>,
        hour: Option<Box<ElmExpression>>,
        minute: Option<Box<ElmExpression>>,
        second: Option<Box<ElmExpression>>,
    },

    #[serde(other)]
    Unsupported,
}

fn default_true() -> bool {
    true
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ElmTupleElement {
    pub name: String,
    pub value: ElmExpression,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ElmAliasedQuerySource {
    pub alias: String,
    pub expression: ElmExpression,
    #[serde(rename = "resultTypeSpecifier")]
    pub result_type_specifier: Option<ElmTypeSpecifier>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ElmLetClause {
    pub identifier: String,
    pub expression: ElmExpression,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum ElmRelationship {
    With {
        alias: String,
        expression: ElmExpression,
        #[serde(rename = "suchThat")]
        such_that: Option<ElmExpression>,
    },
    Without {
        alias: String,
        expression: ElmExpression,
        #[serde(rename = "suchThat")]
        such_that: Option<ElmExpression>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ElmReturnClause {
    pub expression: Box<ElmExpression>,
    pub distinct: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ElmSortClause {
    #[serde(rename = "by", default)]
    pub by: Vec<ElmSortByItem>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ElmSortByItem {
    pub direction: String,
    pub path: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ElmCaseItem {
    pub when: ElmExpression,
    pub then: ElmExpression,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_simple_elm() {
        let json = r#"{
            "identifier": {"id": "Test", "version": "1.0.0"},
            "statements": {
                "def": [
                    {
                        "name": "InDemographic",
                        "context": "Patient",
                        "expression": {
                            "type": "GreaterOrEqual",
                            "operand": [
                                {
                                    "type": "Property",
                                    "path": "birthDate"
                                },
                                {
                                    "type": "Literal",
                                    "valueType": "{urn:hl7-org:elm-types:r1}Date",
                                    "value": "2000-01-01"
                                }
                            ]
                        }
                    }
                ]
            }
        }"#;

        let lib: ElmLibrary = serde_json::from_str(json).unwrap();
        assert_eq!(lib.identifier.unwrap().id.unwrap(), "Test");
        let stmts = lib.statements.unwrap();
        assert_eq!(stmts.defs.len(), 1);
        assert_eq!(stmts.defs[0].name, "InDemographic");
    }

    #[test]
    fn test_parse_retrieve() {
        let json = r#"{
            "type": "Retrieve",
            "dataType": "{http://hl7.org/fhir}Condition",
            "codeProperty": "code"
        }"#;
        let expr: ElmExpression = serde_json::from_str(json).unwrap();
        match expr {
            ElmExpression::Retrieve { data_type, .. } => {
                assert_eq!(data_type, "{http://hl7.org/fhir}Condition");
            }
            _ => panic!("Expected Retrieve"),
        }
    }

    #[test]
    fn test_parse_query() {
        let json = r#"{
            "type": "Query",
            "source": [{
                "alias": "C",
                "expression": {
                    "type": "Retrieve",
                    "dataType": "{http://hl7.org/fhir}Condition"
                }
            }],
            "where": {
                "type": "Equal",
                "operand": [
                    {"type": "Property", "path": "status", "scope": "C"},
                    {"type": "Literal", "valueType": "{urn:hl7-org:elm-types:r1}String", "value": "active"}
                ]
            }
        }"#;
        let expr: ElmExpression = serde_json::from_str(json).unwrap();
        match expr {
            ElmExpression::Query { source, where_clause, .. } => {
                assert_eq!(source.len(), 1);
                assert_eq!(source[0].alias, "C");
                assert!(where_clause.is_some());
            }
            _ => panic!("Expected Query"),
        }
    }
}
