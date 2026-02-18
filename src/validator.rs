//! Contract validation for semantic correctness guarantees
//!
//! This module implements validation that enforces semantic correctness
//! contracts to prevent double counting, ambiguous identity resolution,
//! and unsafe joins/rollups.

use crate::semantic_model::{
    SemanticModel, DatasetGroup, GroupDataset, DatasetGroupDimension, Dimension, Attribute,
    ContractMode, JoinRelationship, AggregationSemantics
};
use crate::resolver::ResolvedQuery;
use std::collections::HashSet;

/// Contract validation result
#[derive(Debug)]
pub struct ValidationResult {
    pub violations: Vec<ContractViolation>,
}

/// A contract violation that should be blocked (strict) or warned (warn)
#[derive(Debug)]
pub struct ContractViolation {
    pub severity: ViolationSeverity,
    pub kind: ViolationKind,
    pub message: String,
    pub context: String,
}

/// Severity of contract violation
#[derive(Debug)]
pub enum ViolationSeverity {
    Error,  // Blocks in strict mode
    Warning, // Warns in warn mode
}

/// Type of contract violation
#[derive(Debug)]
pub enum ViolationKind {
    /// Join relationship not declared for strict contract
    MissingJoinRelationship,
    /// Unsafe join relationship detected
    UnsafeJoinRelationship,
    /// Scoped attribute used without required scope
    IdentityScopeViolation,
    /// Invalid scoped_by reference
    InvalidScopeReference,
    /// Invalid dataset grain reference
    InvalidGrainReference,
    /// Cross-datasetGroup rollup of holistic/algebraic metric
    UnsafeCrossDatasetRollup,
}

impl std::fmt::Display for ViolationKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ViolationKind::MissingJoinRelationship => write!(f, "MissingJoinRelationship"),
            ViolationKind::UnsafeJoinRelationship => write!(f, "UnsafeJoinRelationship"),
            ViolationKind::IdentityScopeViolation => write!(f, "IdentityScopeViolation"),
            ViolationKind::InvalidScopeReference => write!(f, "InvalidScopeReference"),
            ViolationKind::InvalidGrainReference => write!(f, "InvalidGrainReference"),
            ViolationKind::UnsafeCrossDatasetRollup => write!(f, "UnsafeCrossDatasetRollup"),
        }
    }
}

/// Validate model schema for contract compliance
pub fn validate_model_schema(model: &SemanticModel) -> ValidationResult {
    let mut violations = Vec::new();

    if model.contract() == ContractMode::Off {
        return ValidationResult { violations };
    }

    // Validate all dataset groups
    for dataset_group in &model.dataset_groups {
        violations.extend(validate_dataset_group(model, dataset_group));
    }

    ValidationResult { violations }
}

/// Validate a dataset group
fn validate_dataset_group(model: &SemanticModel, dataset_group: &DatasetGroup) -> Vec<ContractViolation> {
    let mut violations = Vec::new();

    // Validate each dimension in the group
    for group_dim in &dataset_group.dimensions {
        violations.extend(validate_dimension_relationship(model, dataset_group, group_dim));
    }

    // Validate each dataset in the group
    for dataset in &dataset_group.datasets {
        violations.extend(validate_dataset_grain(model, dataset_group, dataset));
    }

    violations
}

/// Validate dimension join relationships
fn validate_dimension_relationship(
    model: &SemanticModel,
    _dataset_group: &DatasetGroup,
    group_dim: &DatasetGroupDimension,
) -> Vec<ContractViolation> {
    let mut violations = Vec::new();

    // If this is a degenerate dimension (no join), skip relationship validation
    if group_dim.is_degenerate() {
        return violations;
    }

    // In strict mode, all joined dimensions must declare relationship
    if let Some(join) = &group_dim.join {
        if join.relationship.is_none() {
            violations.push(ContractViolation {
                severity: ViolationSeverity::Error,
                kind: ViolationKind::MissingJoinRelationship,
                message: format!("Dimension '{}' has join but no relationship declared", group_dim.name),
                context: format!("Join: {} → {}", join.left_key, join.right_key),
            });
        }

        // Validate scoped_by references if they exist
        if let Some(dim) = model.get_dimension(&group_dim.name) {
            for attr in &dim.attributes {
                violations.extend(validate_scoped_by_references(model, &group_dim.name, attr));
            }
        }
    }

    violations
}

/// Validate scoped_by references in an attribute
fn validate_scoped_by_references(
    model: &SemanticModel,
    dim_name: &str,
    attr: &Attribute,
) -> Vec<ContractViolation> {
    let mut violations = Vec::new();

    for scope_ref in &attr.scoped_by {
        // Parse the scope reference (should be "dimension.attribute")
        let parts: Vec<&str> = scope_ref.split('.').collect();
        if parts.len() != 2 {
            violations.push(ContractViolation {
                severity: ViolationSeverity::Error,
                kind: ViolationKind::InvalidScopeReference,
                message: format!("Invalid scoped_by reference '{}' in {}.{}", scope_ref, dim_name, attr.name),
                context: "scoped_by must be in format 'dimension.attribute'".to_string(),
            });
            continue;
        }

        let (scope_dim, scope_attr) = (parts[0], parts[1]);

        // Check if the referenced dimension exists
        if model.get_dimension(scope_dim).is_none() {
            violations.push(ContractViolation {
                severity: ViolationSeverity::Error,
                kind: ViolationKind::InvalidScopeReference,
                message: format!("scoped_by references non-existent dimension '{}' in {}.{}", scope_dim, dim_name, attr.name),
                context: format!("Referenced in scoped_by: {}", scope_ref),
            });
            continue;
        }

        // Check if the referenced attribute exists
        if let Some(dim) = model.get_dimension(scope_dim) {
            if dim.get_attribute(scope_attr).is_none() {
                violations.push(ContractViolation {
                    severity: ViolationSeverity::Error,
                    kind: ViolationKind::InvalidScopeReference,
                    message: format!("scoped_by references non-existent attribute '{}' in {}.{}", scope_attr, dim_name, attr.name),
                    context: format!("Referenced in scoped_by: {}", scope_ref),
                });
            }
        }
    }

    violations
}

/// Validate dataset grain references
fn validate_dataset_grain(
    model: &SemanticModel,
    dataset_group: &DatasetGroup,
    dataset: &GroupDataset,
) -> Vec<ContractViolation> {
    let mut violations = Vec::new();

    for grain_ref in &dataset.grain {
        // Parse the grain reference (should be "dimension.attribute")
        let parts: Vec<&str> = grain_ref.split('.').collect();
        if parts.len() != 2 {
            violations.push(ContractViolation {
                severity: ViolationSeverity::Error,
                kind: ViolationKind::InvalidGrainReference,
                message: format!("Invalid grain reference '{}' in dataset '{}'", grain_ref, dataset.dataset),
                context: "grain must be in format 'dimension.attribute'".to_string(),
            });
            continue;
        }

        let (grain_dim, grain_attr) = (parts[0], parts[1]);

        // Check if the referenced dimension is available in this dataset
        if !dataset.has_dimension(grain_dim) {
            violations.push(ContractViolation {
                severity: ViolationSeverity::Error,
                kind: ViolationKind::InvalidGrainReference,
                message: format!("grain references dimension '{}' not available in dataset '{}'", grain_dim, dataset.dataset),
                context: format!("Grain reference: {}", grain_ref),
            });
            continue;
        }

        // Check if the referenced attribute is available in this dataset
        if !dataset.has_dimension_attribute(grain_dim, grain_attr) {
            violations.push(ContractViolation {
                severity: ViolationSeverity::Error,
                kind: ViolationKind::InvalidGrainReference,
                message: format!("grain references attribute '{}' not available in dataset '{}'", grain_attr, dataset.dataset),
                context: format!("Grain reference: {}", grain_ref),
            });
            continue;
        }

        // Additional validation: check if this attribute has scoped_by constraints
        // If so, all scope attributes must also be in grain
        if let Some(dim) = model.get_dimension(grain_dim) {
            if let Some(attr) = dim.get_attribute(grain_attr) {
                for scope_ref in &attr.scoped_by {
                    if !dataset.grain.contains(scope_ref) {
                        violations.push(ContractViolation {
                            severity: ViolationSeverity::Error,
                            kind: ViolationKind::InvalidGrainReference,
                            message: format!("grain missing scope attribute '{}' required by scoped attribute '{}' in dataset '{}'",
                                           scope_ref, grain_ref, dataset.dataset),
                            context: "Scoped attributes require their scope attributes in grain".to_string(),
                        });
                    }
                }
            }
        }
    }

    violations
}

/// Validate a resolved query for contract compliance
pub fn validate_resolved_query(
    model: &SemanticModel,
    query: &ResolvedQuery,
) -> ValidationResult {
    let mut violations = Vec::new();

    if model.contract() == ContractMode::Off {
        return ValidationResult { violations };
    }

    // Validate identity scope constraints
    violations.extend(validate_identity_scopes(model, query));

    // Validate cross-datasetGroup rollups
    violations.extend(validate_cross_dataset_rollups(model, query));

    ValidationResult { violations }
}

/// Validate identity scope constraints in query
fn validate_identity_scopes(
    model: &SemanticModel,
    query: &ResolvedQuery,
) -> Vec<ContractViolation> {
    let mut violations = Vec::new();

    // Collect all attributes used in rows, columns
    let mut used_attrs = HashSet::new();
    for attr in &query.row_attributes {
        used_attrs.insert((attr.dimension_name().to_string(), attr.attribute_name().to_string()));
    }
    for attr in &query.column_attributes {
        used_attrs.insert((attr.dimension_name().to_string(), attr.attribute_name().to_string()));
    }

    // Also include filter attributes
    for filter in &query.filters {
        used_attrs.insert((filter.attribute.dimension_name().to_string(), filter.attribute.attribute_name().to_string()));
    }

    // Check each used attribute for scope constraints
    for (dim_name, attr_name) in used_attrs {
        if let Some(dim) = model.get_dimension(&dim_name) {
            if let Some(attr) = dim.get_attribute(&attr_name) {
                violations.extend(validate_attribute_scope(model, query, &dim_name, attr));
            }
        }
    }

    violations
}

/// Validate scope constraints for a single attribute usage
fn validate_attribute_scope(
    model: &SemanticModel,
    query: &ResolvedQuery,
    dim_name: &str,
    attr: &Attribute,
) -> Vec<ContractViolation> {
    let mut violations = Vec::new();

    // Skip if no scoped_by constraints
    if attr.scoped_by.is_empty() {
        return violations;
    }

    let attr_path = format!("{}.{}", dim_name, attr.name);

    // Check if all required scope attributes are present
    for scope_ref in &attr.scoped_by {
        let scope_parts: Vec<&str> = scope_ref.split('.').collect();
        if scope_parts.len() != 2 {
            continue; // Already validated in schema validation
        }

        let (scope_dim, scope_attr) = (scope_parts[0], scope_parts[1]);

        // Check if scope attribute is in rows
        let scope_in_rows = query.row_attributes.iter().any(|a|
            a.dimension_name() == scope_dim && a.attribute_name() == scope_attr
        );

        // Check if scope attribute is in columns
        let scope_in_columns = query.column_attributes.iter().any(|a|
            a.dimension_name() == scope_dim && a.attribute_name() == scope_attr
        );

        // Check if scope attribute is in filters
        let scope_in_filters = query.filters.iter().any(|f|
            f.attribute.dimension_name() == scope_dim && f.attribute.attribute_name() == scope_attr
        );

        if !scope_in_rows && !scope_in_columns && !scope_in_filters {
            violations.push(ContractViolation {
                severity: ViolationSeverity::Error,
                kind: ViolationKind::IdentityScopeViolation,
                message: format!("Attribute '{}' requires scope attribute '{}' but it's not present in query", attr_path, scope_ref),
                context: "Identity scope violation: scoped attribute used without required scope".to_string(),
            });
        }
    }

    violations
}

/// Validate cross-datasetGroup rollups for semantic correctness
fn validate_cross_dataset_rollups(
    model: &SemanticModel,
    query: &ResolvedQuery,
) -> Vec<ContractViolation> {
    let mut violations = Vec::new();

    // Check if any metric is a cross-datasetGroup metric with unsafe rollups
    for metric in &query.metrics {
        if !metric.is_cross_dataset_group() {
            continue;
        }

        // Get the measures used by this cross-datasetGroup metric
        let measure_mappings = metric.dataset_group_measures();

        for (_tg, measure_name) in measure_mappings {
            // Find the measure definition
            for dataset_group in &model.dataset_groups {
                if let Some(measure) = dataset_group.get_measure(&measure_name) {
                    // Check if aggregation is unsafe for cross-dataset rollups
                    if !measure.aggregation.is_safe_for_cross_dataset_rollup() {
                        violations.push(ContractViolation {
                            severity: ViolationSeverity::Error,
                            kind: ViolationKind::UnsafeCrossDatasetRollup,
                            message: format!("Cross-datasetGroup metric '{}' uses {} aggregation which is not safe for rollups",
                                           metric.name, measure.aggregation),
                            context: format!("Measure '{}' uses {} which cannot be safely combined across dataset groups",
                                           measure_name, measure.aggregation),
                        });
                    }
                }
            }
        }
    }

    violations
}

/// Check if a join relationship is unsafe for the given context
pub fn is_join_relationship_unsafe(
    relationship: Option<JoinRelationship>,
    requires_distributive: bool,
) -> bool {
    match relationship {
        Some(JoinRelationship::OneToMany) | Some(JoinRelationship::ManyToMany) => {
            // These relationships can multiply rows, which is dangerous for aggregations
            // Especially problematic for distributive aggregations that expect additive behavior
            requires_distributive
        }
        Some(JoinRelationship::ManyToOne) | Some(JoinRelationship::OneToOne) => {
            // These are generally safe
            false
        }
        None => {
            // No relationship declared - assume unsafe in strict mode
            true
        }
    }
}

/// Format validation result for display
pub fn format_validation_result(result: &ValidationResult) -> String {
    if result.violations.is_empty() {
        return "✅ All contract checks passed".to_string();
    }

    let mut output = String::new();
    output.push_str("⚠️  Contract violations detected:\n");

    for (i, violation) in result.violations.iter().enumerate() {
        let severity = match violation.severity {
            ViolationSeverity::Error => "❌ ERROR",
            ViolationSeverity::Warning => "⚠️  WARNING",
        };

        output.push_str(&format!("{}. {}: {}\n", i + 1, severity, violation.message));
        if !violation.context.is_empty() {
            output.push_str(&format!("   Context: {}\n", violation.context));
        }
        output.push_str("\n");
    }

    output
}