//! Tests for semantic contract validation

use semstrait::semantic_model::{ContractMode, JoinRelationship};
use semstrait::validator::{validate_model_schema, validate_resolved_query, ViolationSeverity};
use semstrait::parser;
use semstrait::resolver::resolve_query;
use semstrait::query::QueryRequest;
use semstrait::selector::SelectedDataset;

#[test]
fn test_contract_validation_off_mode() {
    let yaml = r#"
semantic_models:
  - name: test
    contract:
      mode: off
    dimensions:
      - name: accounts
        attributes:
          - name: id
            type: i64
      - name: campaigns
        attributes:
          - name: id
            type: i64
            scopedBy: ["accounts.id"]
    datasetGroups:
      - name: test_group
        dimensions:
          - name: campaigns
            join:
              leftKey: account_id
              rightKey: id
              # Missing relationship - would be error in strict mode
        measures: []
        datasets:
          - dataset: test_table
            source:
              type: parquet
              path: "/tmp/test.parquet"
            dimensions: {}
            measures: []
"#;

    let schema = parser::parse_str(yaml).unwrap();
    let model = schema.get_model("test").unwrap();
    let validation = validate_model_schema(model);

    // Should pass with no violations in off mode
    assert!(validation.violations.is_empty());
}

#[test]
fn test_contract_validation_strict_mode() {
    let yaml = r#"
semantic_models:
  - name: test
    contract:
      mode: strict
    dimensions:
      - name: accounts
        attributes:
          - name: id
            type: i64
      - name: campaigns
        attributes:
          - name: id
            type: i64
            scopedBy: ["accounts.id"]
    datasetGroups:
      - name: test_group
        dimensions:
          - name: campaigns
            join:
              leftKey: account_id
              rightKey: id
              # Missing relationship - should be error in strict mode
        measures: []
        datasets:
          - dataset: test_table
            source:
              type: parquet
              path: "/tmp/test.parquet"
            dimensions: {}
            measures: []
"#;

    let schema = parser::parse_str(yaml).unwrap();
    let model = schema.get_model("test").unwrap();
    let validation = validate_model_schema(model);

    // Should have violations in strict mode
    assert!(!validation.violations.is_empty());
    assert!(validation.violations.iter().any(|v|
        matches!(v.severity, ViolationSeverity::Error) &&
        v.kind.to_string() == "MissingJoinRelationship"
    ));
}

#[test]
fn test_identity_scope_violation() {
    let yaml = r#"
semantic_models:
  - name: test
    contract:
      mode: strict
    dimensions:
      - name: accounts
        attributes:
          - name: id
            type: i64
      - name: campaigns
        attributes:
          - name: id
            type: i64
            scopedBy: ["accounts.id"]
    datasetGroups:
      - name: test_group
        dimensions:
          - name: campaigns
            join:
              leftKey: account_id
              rightKey: id
              relationship: many_to_one
        measures: []
        datasets:
          - dataset: test_table
            grain: ["accounts.id", "campaigns.id"]
            dimensions:
              accounts: [id]
              campaigns: [id]
            measures: []
"#;

    let schema = parser::parse_str(yaml).unwrap();
    let model = schema.get_model("test").unwrap();

    // Query that uses campaigns.id without accounts.id scope
    let request = QueryRequest {
        model: "test".to_string(),
        rows: Some(vec!["campaigns.id".to_string()]), // Missing accounts.id scope
        columns: None,
        metrics: None,
        filter: None,
        dimensions: None,
    };

    let dataset = model.get_dataset("test_table").unwrap();
    let group = model.dataset_groups.first().unwrap();
    let selected = SelectedDataset { group, dataset };
    let resolved = resolve_query(&schema, &request, &selected).unwrap();
    let validation = validate_resolved_query(model, &resolved);

    // Should have identity scope violation
    assert!(!validation.violations.is_empty());
    assert!(validation.violations.iter().any(|v|
        matches!(v.severity, ViolationSeverity::Error) &&
        v.kind.to_string() == "IdentityScopeViolation"
    ));
}

#[test]
fn test_identity_scope_compliance() {
    let yaml = r#"
semantic_models:
  - name: test
    contract:
      mode: strict
    dimensions:
      - name: accounts
        attributes:
          - name: id
            type: i64
      - name: campaigns
        attributes:
          - name: id
            type: i64
            scopedBy: ["accounts.id"]
    datasetGroups:
      - name: test_group
        dimensions:
          - name: campaigns
            join:
              leftKey: account_id
              rightKey: id
              relationship: many_to_one
        measures: []
        datasets:
          - dataset: test_table
            grain: ["accounts.id", "campaigns.id"]
            dimensions:
              accounts: [id]
              campaigns: [id]
            measures: []
"#;

    let schema = parser::parse_str(yaml).unwrap();
    let model = schema.get_model("test").unwrap();

    // Query that includes both campaigns.id and required accounts.id scope
    let request = QueryRequest {
        model: "test".to_string(),
        rows: Some(vec!["accounts.id".to_string(), "campaigns.id".to_string()]),
        columns: None,
        metrics: None,
        filter: None,
        dimensions: None,
    };

    let dataset = model.get_dataset("test_table").unwrap();
    let group = model.dataset_groups.first().unwrap();
    let selected = SelectedDataset { group, dataset };
    let resolved = resolve_query(&schema, &request, &selected).unwrap();
    let validation = validate_resolved_query(model, &resolved);

    // Should pass - no identity scope violations
    let scope_violations = validation.violations.iter()
        .filter(|v| v.kind.to_string() == "IdentityScopeViolation")
        .count();
    assert_eq!(scope_violations, 0);
}