use semstrait::{Schema, QueryRequest, plan::PlanNode};
use substrait::proto::Plan;
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use tempfile::TempDir;
use chrono::{DateTime, Utc};
use super::ReproducibilityParams;
use super::metadata::{InventoryBuilder, InventorySnapshot};

/// A snapshot store that persists all artifacts needed to reproduce a calculation
pub struct SnapshotStore {
    base_dir: PathBuf,
}

impl SnapshotStore {
    /// Create a new snapshot store
    pub fn new() -> Self {
        let base_dir = PathBuf::from(".semstrait_demo").join("snapshots");
        Self { base_dir }
    }

    /// Save a complete snapshot with all artifacts
    pub async fn save_snapshot(
        &self,
        snapshot_id: &str,
        schema: &Schema,
        request: &QueryRequest,
        plan_node: &PlanNode,
        substrait_plan: &Plan,
        repro_params: &ReproducibilityParams,
        temp_dir: &TempDir,
        table_paths: &HashMap<String, String>,
    ) -> anyhow::Result<PathBuf> {
        let snapshot_dir = self.base_dir.join(snapshot_id);
        fs::create_dir_all(&snapshot_dir)?;

        // Save individual artifacts
        self.save_params_json(&snapshot_dir, repro_params)?;
        self.save_request_json(&snapshot_dir, request)?;
        self.save_plan_json(&snapshot_dir, substrait_plan)?;
        self.save_model_yaml(&snapshot_dir, schema)?;
        self.save_inventory_json(&snapshot_dir, table_paths, &repro_params.as_of).await?;
        self.save_parquet_fixtures(&snapshot_dir, temp_dir, table_paths)?;

        Ok(snapshot_dir)
    }

    /// Save reproducibility parameters as JSON
    fn save_params_json(
        &self,
        snapshot_dir: &Path,
        repro_params: &ReproducibilityParams,
    ) -> anyhow::Result<()> {
        let params_path = snapshot_dir.join("params.json");
        let json = serde_json::to_string_pretty(repro_params)?;
        fs::write(params_path, json)?;
        Ok(())
    }

    /// Save query request as JSON
    fn save_request_json(
        &self,
        snapshot_dir: &Path,
        request: &QueryRequest,
    ) -> anyhow::Result<()> {
        let request_path = snapshot_dir.join("request.json");
        let json = serde_json::to_string_pretty(request)?;
        fs::write(request_path, json)?;
        Ok(())
    }

    /// Save Substrait plan as JSON
    fn save_plan_json(
        &self,
        snapshot_dir: &Path,
        substrait_plan: &Plan,
    ) -> anyhow::Result<()> {
        let plan_path = snapshot_dir.join("plan.json");
        let json = serde_json::to_string_pretty(substrait_plan)?;
        fs::write(plan_path, json)?;
        Ok(())
    }

    /// Save the semantic model as YAML
    fn save_model_yaml(
        &self,
        snapshot_dir: &Path,
        _schema: &Schema,
    ) -> anyhow::Result<()> {
        let model_path = snapshot_dir.join("model.yaml");
        // Save the embedded model YAML (deterministic source)
        let yaml = include_str!("../model.yaml");
        fs::write(model_path, yaml)?;
        Ok(())
    }

    /// Compute and save inventory snapshot as JSON
    async fn save_inventory_json(
        &self,
        snapshot_dir: &Path,
        table_paths: &HashMap<String, String>,
        as_of: &str,
    ) -> anyhow::Result<()> {
        let inventory_path = snapshot_dir.join("inventory.json");

        let builder = InventoryBuilder::new();
        let inventory = builder.compute_inventory(table_paths, as_of).await?;

        let json = serde_json::to_string_pretty(&inventory)?;
        fs::write(inventory_path, json)?;
        Ok(())
    }

    /// Copy Parquet fixtures to snapshot directory
    fn save_parquet_fixtures(
        &self,
        snapshot_dir: &Path,
        temp_dir: &TempDir,
        table_paths: &HashMap<String, String>,
    ) -> anyhow::Result<()> {
        let fixtures_dir = snapshot_dir.join("fixtures");
        fs::create_dir_all(&fixtures_dir)?;

        for (table_name, temp_path) in table_paths {
            let dest_path = fixtures_dir.join(format!("{}.parquet", table_name));
            fs::copy(temp_path, &dest_path)?;
        }

        Ok(())
    }

}
