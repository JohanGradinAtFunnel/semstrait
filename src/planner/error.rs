//! Planner errors

use std::fmt;

#[derive(Debug)]
pub enum PlanError {
    /// No measures or dimensions specified
    EmptyQuery,
    /// Invalid query configuration
    InvalidQuery(String),
    /// Contract violation blocks planning
    ContractViolation(String),
}

impl fmt::Display for PlanError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PlanError::EmptyQuery => {
                write!(f, "Query must have at least one measure or dimension")
            }
            PlanError::InvalidQuery(msg) => {
                write!(f, "Invalid query: {}", msg)
            }
            PlanError::ContractViolation(msg) => {
                write!(f, "Contract violation: {}", msg)
            }
        }
    }
}

impl std::error::Error for PlanError {}
