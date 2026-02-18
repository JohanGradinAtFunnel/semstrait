//! Demo-grade metadata/inventory computation for semantic layers
//!
//! This module provides a simple inventory system that computes and persists
//! metadata about datasets (row counts, schema hashes, watermarks, completeness).
//! Used by snapshot_store and health modules to avoid recomputing these values.

use chrono::{DateTime, Utc};
use datafusion::prelude::*;
use datafusion::arrow::array::Array;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Inventory information for a single dataset
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatasetInventory {
    /// Dataset name (e.g., "adwords_campaigns")
    pub dataset_name: String,
    /// Total number of rows
    pub row_count: usize,
    /// Hash of the schema for change detection
    pub schema_hash: String,
    /// Maximum event_time_utc if available
    pub max_event_time: Option<DateTime<Utc>>,
    /// Completeness: percentage of expected data (0.0-1.0)
    pub completeness_score: f64,
    /// Last time this dataset was ingested/updated
    pub last_ingested_at: Option<DateTime<Utc>>,
}

/// Complete inventory snapshot across all datasets
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InventorySnapshot {
    /// When this snapshot was computed
    pub computed_at: DateTime<Utc>,
    /// Individual dataset inventories
    pub datasets: HashMap<String, DatasetInventory>,
    /// Overall fingerprint combining all datasets
    pub overall_fingerprint: String,
}

/// Builder for computing inventory snapshots
pub struct InventoryBuilder {
    ctx: SessionContext,
}

impl InventoryBuilder {
    /// Create a new inventory builder
    pub fn new() -> Self {
        Self {
            ctx: SessionContext::new(),
        }
    }

    /// Compute inventory for all datasets
    pub async fn compute_inventory(
        &self,
        table_paths: &HashMap<String, String>,
        as_of: &str,
    ) -> anyhow::Result<InventorySnapshot> {
        let mut datasets = HashMap::new();

        // Compute inventory for each dataset
        for (table_name, path) in table_paths {
            let inventory = self.compute_dataset_inventory(table_name, path, as_of).await?;
            datasets.insert(table_name.clone(), inventory);
        }

        // Compute overall fingerprint
        let overall_fingerprint = self.compute_overall_fingerprint(&datasets).await;

        Ok(InventorySnapshot {
            computed_at: Utc::now(),
            datasets,
            overall_fingerprint,
        })
    }

    /// Compute inventory for a single dataset
    async fn compute_dataset_inventory(
        &self,
        dataset_name: &str,
        path: &str,
        as_of: &str,
    ) -> anyhow::Result<DatasetInventory> {
        let df = self.ctx.read_parquet(path, Default::default()).await?;

        // Find max event time before collecting (collect moves the DataFrame)
        let max_event_time = find_max_event_time(&df).await?;

        let batches = df.collect().await?;
        let row_count = batches.iter().map(|b| b.num_rows()).sum();

        // Compute schema hash
        let schema_hash = compute_schema_hash(&batches);

        // Compute completeness score (simplified - could be more sophisticated)
        let completeness_score = compute_completeness_score(&batches, as_of);

        // For demo, assume last ingested is now (in real system this would come from catalog)
        let last_ingested_at = Some(Utc::now());

        Ok(DatasetInventory {
            dataset_name: dataset_name.to_string(),
            row_count,
            schema_hash,
            max_event_time,
            completeness_score,
            last_ingested_at,
        })
    }

    /// Compute overall fingerprint combining all datasets
    async fn compute_overall_fingerprint(&self, datasets: &HashMap<String, DatasetInventory>) -> String {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let mut hasher = DefaultHasher::new();

        // Sort datasets by name for deterministic hashing
        let mut sorted_datasets: Vec<_> = datasets.iter().collect();
        sorted_datasets.sort_by_key(|(name, _)| *name);

        for (name, inventory) in sorted_datasets {
            name.hash(&mut hasher);
            inventory.row_count.hash(&mut hasher);
            inventory.schema_hash.hash(&mut hasher);
            if let Some(max_time) = inventory.max_event_time {
                max_time.timestamp().hash(&mut hasher);
            }
            (inventory.completeness_score.to_bits()).hash(&mut hasher);
        }

        format!("{:x}", hasher.finish())
    }
}

/// Compute a hash of the schema for change detection
fn compute_schema_hash(batches: &[datafusion::arrow::record_batch::RecordBatch]) -> String {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};

    let mut hasher = DefaultHasher::new();

    if let Some(first_batch) = batches.first() {
        let schema = first_batch.schema();
        for field in schema.fields() {
            field.name().hash(&mut hasher);
            format!("{:?}", field.data_type()).hash(&mut hasher);
        }
    }

    format!("{:x}", hasher.finish())
}

/// Find maximum event_time from a DataFrame
async fn find_max_event_time(df: &DataFrame) -> anyhow::Result<Option<DateTime<Utc>>> {
    // Look for event_time_utc column and find max
    if let Ok(max_time_col) = df.clone().select_columns(&["event_time_utc"]) {
        if let Ok(max_time_df) = max_time_col.aggregate(vec![], vec![
            datafusion::functions_aggregate::expr_fn::max(datafusion::logical_expr::col("event_time_utc"))
        ]) {
            let batches = max_time_df.collect().await?;
            if let Some(batch) = batches.first() {
                if let Some(col) = batch.column_by_name("max(event_time_utc)") {
                    if let Some(timestamp_array) = col.as_any().downcast_ref::<datafusion::arrow::array::TimestampMicrosecondArray>() {
                        if let Some(max_ts) = timestamp_array.value(0).into() {
                            return Ok(Some(DateTime::from_timestamp_micros(max_ts).unwrap_or(Utc::now())));
                        }
                    }
                }
            }
        }
    }
    Ok(None)
}

/// Compute completeness score (simplified implementation)
fn compute_completeness_score(
    batches: &[datafusion::arrow::record_batch::RecordBatch],
    _as_of: &str,
) -> f64 {
    if batches.is_empty() {
        return 0.0;
    }

    // Simple heuristic: score based on non-null values in key columns
    let mut total_cells = 0;
    let mut non_null_cells = 0;

    for batch in batches {
        for col_idx in 0..batch.num_columns() {
            let col = batch.column(col_idx);
            total_cells += batch.num_rows();

            // Count non-null values (simplified - assumes numeric types)
            if let Some(int_array) = col.as_any().downcast_ref::<datafusion::arrow::array::Int64Array>() {
                for i in 0..batch.num_rows() {
                    if int_array.is_valid(i) {
                        non_null_cells += 1;
                    }
                }
            } else if let Some(float_array) = col.as_any().downcast_ref::<datafusion::arrow::array::Float64Array>() {
                for i in 0..batch.num_rows() {
                    if float_array.is_valid(i) {
                        non_null_cells += 1;
                    }
                }
            } else {
                // For other types, assume they're valid if present
                non_null_cells += batch.num_rows();
            }
        }
    }

    if total_cells == 0 {
        0.0
    } else {
        non_null_cells as f64 / total_cells as f64
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_inventory_builder_creation() {
        let builder = InventoryBuilder::new();
        // Just test that it can be created
        assert!(true);
    }
}