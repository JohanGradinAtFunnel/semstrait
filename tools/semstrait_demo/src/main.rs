#![allow(warnings)]

use clap::{Parser, Subcommand};
use tempfile::TempDir;
use datafusion::prelude::*;
use std::collections::HashMap;
use serde::{Deserialize, Serialize};

mod parquet_generation;
mod lineage;
mod execution;
mod datafusion_execution;
mod diff_engine;
mod impact_engine;
mod health;
mod proof_pack;
mod snapshot_store;
mod reconcile;
mod artifacts;
mod incident;
mod dictionary;
mod lookup;
mod metadata;

/// Shared reproducibility and scope options (used by incident and other commands)
#[derive(Debug, Clone, Parser)]
pub struct CommonOptions {
    /// As-of timestamp for reproducibility (ISO 8601 format)
    #[arg(long, default_value = "2024-01-01T00:00:00Z")]
    pub as_of: String,

    /// Timezone for analysis
    #[arg(long, default_value = "UTC")]
    pub timezone: String,

    /// Currency for monetary values
    #[arg(long, default_value = "USD")]
    pub currency: String,

    /// FX conversion rate (USD to target currency)
    #[arg(long, default_value = "1.0")]
    pub fx_rate: f64,

    /// Attribution window in days
    #[arg(long, default_value = "30")]
    pub attribution_window: u32,

    /// Scope (e.g. subscription:ACME)
    #[arg(long)]
    pub scope: Option<String>,

    /// Analysis window (e.g. 24h, 7d)
    #[arg(long)]
    pub window: Option<String>,

    /// Custom model YAML file path (defaults to embedded model.yaml)
    #[arg(long)]
    pub model: Option<String>,
}

#[derive(Parser)]
#[command(name = "semstrait-demo")]
#[command(about = "Semantic layer trust engines: prove, diagnose, validate, and monitor data")]
#[command(version)]
struct Args {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Orchestrate health + diff + proof-pack for an incident (verdict + actions + artifacts)
    Incident {
        /// Incident name (e.g. spend_drop)
        name: String,

        /// Metric to analyze
        #[arg(long)]
        metric: String,

        /// Since date (YYYY-MM-DD)
        #[arg(long)]
        since: Option<String>,

        #[command(flatten)]
        common: CommonOptions,
    },
    /// Run semantic query and show reproducibility proof
    Run {
        #[arg(long, default_value = "union")]
        scenario: String,

        #[arg(long)]
        json: bool,

        #[arg(long)]
        no_exec: bool,

        #[command(flatten)]
        common: CommonOptions,
    },
    /// Diagnose why numbers don't match: semantic vs platform comparison
    Diff {
        /// Comma-separated metrics (e.g. total_cost,total_impressions)
        #[arg(long)]
        metrics: Option<String>,

        /// Baseline type: raw, platform:facebook, platform:adwords
        #[arg(long, default_value = "raw")]
        baseline: String,

        /// Grain for analysis: day, account, campaign, ad
        #[arg(long, default_value = "day")]
        grain: String,

        /// Include explain/driver analysis
        #[arg(long)]
        explain: bool,

        /// Scenario for fixture generation (e.g. messy_alignment)
        #[arg(long)]
        scenario: Option<String>,

        #[command(flatten)]
        common: CommonOptions,
    },
    /// Validate before live: impact analysis of proposed changes
    Impact {
        #[arg(long)]
        proposed_model: Option<String>,

        #[arg(long)]
        preview: bool,

        /// Sample window (e.g. last_30_days)
        #[arg(long)]
        sample: Option<String>,

        /// Comma-separated metrics
        #[arg(long)]
        metrics: Option<String>,

        /// Scenario for fixture generation (e.g. messy_alignment)
        #[arg(long)]
        scenario: Option<String>,

        #[command(flatten)]
        common: CommonOptions,
    },
    /// Monitor data health: safe-to-report, completeness, freshness
    Health {
        #[arg(long)]
        alert_output: Option<String>,

        /// Scenario for fixture generation (e.g. messy_alignment)
        #[arg(long)]
        scenario: Option<String>,

        #[command(flatten)]
        common: CommonOptions,
    },
    /// Drilldown analysis: show contributing rows for a table group
    Drilldown {
        table_group: String,

        #[command(flatten)]
        common: CommonOptions,
    },
    /// Generate proof pack for a metric: definition, lineage, exportables
    ProofPack {
        metric: String,

        #[command(flatten)]
        common: CommonOptions,
    },
    /// Reconcile semantic vs baseline with timezone/attribution knobs
    Reconcile {
        metric: String,

        /// Baseline: raw, platform:facebook, platform:adwords
        #[arg(long, default_value = "raw")]
        baseline: String,

        /// Attribution window (e.g. 1d_click,1d_view)
        #[arg(long)]
        attribution: Option<String>,

        /// Scenario for fixture generation (e.g. messy_alignment)
        #[arg(long)]
        scenario: Option<String>,

        #[command(flatten)]
        common: CommonOptions,
    },
    /// Export data dictionary from semantic model
    Dictionary {
        #[command(subcommand)]
        cmd: DictionaryCmd,
    },
    /// Manage lookup tables for semantic enrichment
    Lookup {
        #[command(subcommand)]
        cmd: LookupCmd,
    },
}

#[derive(Subcommand)]
enum DictionaryCmd {
    /// Export dimensions, measures, metrics to CSV or JSON
    Export {
        #[arg(long, default_value = "csv", value_parser = ["csv", "json"])]
        format: String,

        #[arg(long)]
        output: Option<String>,

        #[arg(long)]
        scope: Option<String>,

        #[arg(long)]
        model: Option<String>,
    },
}

#[derive(Subcommand)]
enum LookupCmd {
    /// Create lookup from CSV file
    Create {
        #[arg(long)]
        name: String,

        #[arg(long)]
        key: String,

        #[arg(long)]
        value: String,

        /// Source: file:<path>
        #[arg(long)]
        from: String,

        #[arg(long)]
        output_dir: Option<String>,
    },
    /// List saved lookups
    List {},
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReproducibilityParams {
    pub as_of: String,
    pub timezone: String,
    pub currency: String,
    pub fx_rate: f64,
    pub attribution_window: u32,
}

impl From<&CommonOptions> for ReproducibilityParams {
    fn from(opts: &CommonOptions) -> Self {
        Self {
            as_of: opts.as_of.clone(),
            timezone: opts.timezone.clone(),
            currency: opts.currency.clone(),
            fx_rate: opts.fx_rate,
            attribution_window: opts.attribution_window,
        }
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    match args.command {
        Commands::Incident { name, metric, since, common } => {
            handle_incident(name, metric, since, common).await
        }
        Commands::Run { scenario, json, no_exec, common } => {
            handle_run(scenario, json, no_exec, common).await
        }
        Commands::Diff { metrics, baseline, grain, explain, scenario, common } => {
            handle_diff(metrics, baseline, grain, explain, scenario, common).await
        }
        Commands::Impact { proposed_model, preview, sample, metrics, scenario, common } => {
            handle_impact(proposed_model, preview, sample, metrics, scenario, common).await
        }
        Commands::Health { scenario, alert_output, common } => {
            handle_health(scenario, common, alert_output).await
        }
        Commands::Drilldown { table_group, common } => {
            handle_drilldown(table_group, common).await
        }
        Commands::ProofPack { metric, common } => {
            handle_proof_pack(metric, common).await
        }
        Commands::Reconcile { metric, baseline, attribution, scenario, common } => {
            handle_reconcile(metric, baseline, attribution, scenario, common).await
        }
        Commands::Dictionary { cmd } => match cmd {
            DictionaryCmd::Export { format, output, scope, model } => {
                dictionary::handle_export(format, output, scope, model).await
            }
        },
        Commands::Lookup { cmd } => match cmd {
            LookupCmd::Create { name, key, value, from, output_dir } => {
                lookup::handle_create(name, key, value, from, output_dir).await
            }
            LookupCmd::List {} => lookup::handle_list().await,
        },
    }
}

async fn handle_incident(
    name: String,
    metric: String,
    since: Option<String>,
    common: CommonOptions,
) -> anyhow::Result<()> {
    incident::run_incident(&name, &metric, since.as_deref(), &common, Some(&name)).await
}

async fn handle_run(
    scenario: String,
    json: bool,
    no_exec: bool,
    common: CommonOptions,
) -> anyhow::Result<()> {
    let repro = ReproducibilityParams::from(&common);
    println!("🔍 Semstrait Demo - Run Mode");
    println!("============================");

    let (schema, model_name, request, plan_node, substrait_plan, repro_params, snapshot_id, temp_dir, table_paths) =
        setup_common(
            common.as_of.clone(),
            common.timezone.clone(),
            common.currency.clone(),
            common.fx_rate,
            common.attribution_window,
            None,
            None,
            Some(scenario),
            common.model.clone(),
        ).await?;
    let model = schema.get_model(&model_name).unwrap();

    lineage::print_lineage_report(&schema, model, &request, &plan_node, &substrait_plan, &repro_params, &snapshot_id);

    if json {
        let json_out = serde_json::to_string_pretty(&substrait_plan)?;
        println!("\n📄 Substrait Plan (JSON):");
        println!("{}", json_out);
        return Ok(());
    }

    if !no_exec {
        let table_paths = setup_table_paths(&temp_dir);
        let results = execute_common(&plan_node, &table_paths).await?;
        println!("📊 Results schema: {:?}", results[0].schema());
        execution::print_execution_results(&results)?;
    }

    println!("\n✅ Run completed successfully!");
    Ok(())
}

async fn handle_diff(
    metrics: Option<String>,
    baseline: String,
    grain: String,
    explain: bool,
    scenario: Option<String>,
    common: CommonOptions,
) -> anyhow::Result<()> {
    let metric_list = metrics
        .map(|s| s.split(',').map(|x| x.trim().to_string()).collect())
        .unwrap_or_else(|| vec!["total_cost".to_string(), "total_impressions".to_string()]);

    println!("🔍 Semstrait Demo - Diff Mode (Diagnose Why It Doesn't Match)");
    println!("============================================================");

    let (schema, model_name, request, plan_node, substrait_plan, repro_params, snapshot_id, temp_dir, table_paths) =
        setup_common(
            common.as_of.clone(),
            common.timezone.clone(),
            common.currency.clone(),
            common.fx_rate,
            common.attribution_window,
            None,
            Some(metric_list.clone()),
            scenario.clone(),
            common.model.clone(),
        ).await?;
    let model = schema.get_model(&model_name).unwrap();

    let table_paths = setup_table_paths(&temp_dir);
    let semantic_results = execute_common(&plan_node, &table_paths).await?;

    let ctx = SessionContext::new();
    let diff_result = diff_engine::execute_diff_analysis(&ctx, &schema, model, &request, &semantic_results, &table_paths).await?;

    diff_engine::print_diff_analysis(&diff_result)?;

    let snapshot_store = snapshot_store::SnapshotStore::new();
    let snapshot_dir = snapshot_store.save_snapshot(
        &snapshot_id,
        &schema,
        &request,
        &plan_node,
        &substrait_plan,
        &repro_params,
        &temp_dir,
        &table_paths,
    ).await?;

    let report_md = diff_engine::format_diff_report(&diff_result);
    artifacts::write_report_md(&snapshot_dir, &report_md)?;
    artifacts::write_report_html(&snapshot_dir, &report_md)?;
    let slack = diff_engine::format_slack_snippet(&diff_result);
    artifacts::write_slack_txt(&snapshot_dir, &slack)?;
    diff_engine::save_diff_json(&diff_result, &snapshot_dir)?;

    println!("\n✅ Diff analysis completed! Artifacts: {}", snapshot_dir.display());
    Ok(())
}

async fn handle_impact(
    proposed_model: Option<String>,
    preview: bool,
    _sample: Option<String>,
    metrics: Option<String>,
    scenario: Option<String>,
    common: CommonOptions,
) -> anyhow::Result<()> {
    let metric_list = metrics
        .map(|s| s.split(',').map(|x| x.trim().to_string()).collect())
        .unwrap_or_else(|| vec!["total_cost".to_string(), "total_impressions".to_string()]);

    if preview || proposed_model.is_none() {
        println!("🎯 Semstrait Demo - Impact Preview Mode (Sample Analysis)");
        println!("=======================================================");
    } else {
        println!("🎯 Semstrait Demo - Impact Mode (Validate Before Live)");
        println!("=====================================================");
    }

    let (schema, model_name, request, plan_node, substrait_plan, repro_params, snapshot_id, temp_dir, table_paths) =
        setup_common(
            common.as_of.clone(),
            common.timezone.clone(),
            common.currency.clone(),
            common.fx_rate,
            common.attribution_window,
            None,
            Some(metric_list),
            scenario.clone(),
            common.model.clone(),
        ).await?;
    let model = schema.get_model(&model_name).unwrap();

    let proposed_model_path = if preview || proposed_model.is_none() {
        None
    } else {
        proposed_model.as_deref()
    };

    let ctx = SessionContext::new();
    let table_paths = setup_table_paths(&temp_dir);
    let impact_result = impact_engine::execute_impact_analysis(
        &ctx,
        &schema,
        model,
        proposed_model_path,
        &request,
        &table_paths,
    ).await?;

    impact_engine::print_impact_analysis(&impact_result, preview || proposed_model.is_none())?;

    let snapshot_store = snapshot_store::SnapshotStore::new();
    let repro = ReproducibilityParams::from(&common);
    let plan_node = semstrait::planner::plan_semantic_query(&schema, model, &request)?;
    let substrait_plan = semstrait::emitter::emit_plan(&plan_node, None)?;
    let snapshot_id = execution::compute_snapshot_id(&schema, &request, &repro, &substrait_plan, &table_paths).await?;
    let snapshot_dir = snapshot_store.save_snapshot(
        &snapshot_id,
        &schema,
        &request,
        &plan_node,
        &substrait_plan,
        &repro_params,
        &temp_dir,
        &table_paths,
    ).await?;

    let report_md = format!("# Impact Report\n\nMetric deltas:\n{:#?}", impact_result.metric_deltas);
    artifacts::write_report_md(&snapshot_dir, &report_md)?;
    artifacts::write_report_html(&snapshot_dir, &report_md)?;
    artifacts::write_json(&snapshot_dir, "risk_summary.json", &impact_result)?;

    println!("\n✅ Impact analysis completed! Artifacts: {}", snapshot_dir.display());
    Ok(())
}

async fn handle_health(scenario: Option<String>, common: CommonOptions, alert_output: Option<String>) -> anyhow::Result<()> {
    let temp_dir = TempDir::new()?;
    let (_adwords_path, _facebook_path) = parquet_generation::generate_fixtures(&temp_dir, scenario.as_deref(), Some(&common.as_of))?;
    let table_paths = setup_table_paths(&temp_dir);

    let ctx = SessionContext::new();
    let health_result = health::execute_health_assessment(&ctx, &table_paths, &common.as_of).await?;

    health::print_health_assessment(&health_result)?;

    if let Some(output_path) = alert_output {
        if !health_result.alerts.is_empty() {
            let alert_json = health::export_alerts_json(&health_result.alerts)?;
            std::fs::write(&output_path, alert_json)?;
            println!("\n💾 Alerts saved to: {}", output_path);
        } else {
            println!("\n📝 No alerts to save (all systems healthy)");
        }
    }

    println!("\n✅ Health assessment completed!");
    Ok(())
}

async fn handle_drilldown(table_group: String, common: CommonOptions) -> anyhow::Result<()> {
    println!("🔬 Semstrait Demo - Drilldown Mode: {}", table_group);
    println!("==========================================");

    let temp_dir = TempDir::new()?;
    let (adwords_path, facebook_path) = parquet_generation::generate_fixtures(&temp_dir, None, None)?;

    execution::print_drilldown(&table_group, &adwords_path, &facebook_path)?;

    println!("\n✅ Drilldown analysis completed!");
    Ok(())
}

async fn handle_proof_pack(metric: String, common: CommonOptions) -> anyhow::Result<()> {
    let repro = ReproducibilityParams::from(&common);
    println!("📋 Semstrait Demo - Proof Pack Mode: {}", metric);
    println!("======================================");

    let (schema, model_name, request, plan_node, substrait_plan, repro_params, snapshot_id, temp_dir, table_paths) =
        setup_common(
            common.as_of.clone(),
            common.timezone.clone(),
            common.currency.clone(),
            common.fx_rate,
            common.attribution_window,
            None,
            Some(vec![metric.clone()]),
            None,
            common.model.clone(),
        ).await?;
    let model = schema.get_model(&model_name).unwrap();

    let proof_pack = proof_pack::generate_proof_pack(
        &schema,
        model,
        &metric,
        &repro_params,
        &snapshot_id,
        &table_paths,
        &request,
    ).await?;

    proof_pack::print_proof_pack(&proof_pack)?;

    let snapshot_store = snapshot_store::SnapshotStore::new();
    let snapshot_dir = snapshot_store.save_snapshot(
        &snapshot_id,
        &schema,
        &request,
        &plan_node,
        &substrait_plan,
        &repro_params,
        &temp_dir,
        &table_paths,
    ).await?;

    proof_pack::save_proof_pack_to_snapshot(&proof_pack, &snapshot_dir)?;

    println!("💾 Complete snapshot saved to: {}", snapshot_dir.display());
    if let Ok(canonical) = snapshot_dir.canonicalize() {
        println!("🔗 Shareable link: file://{}", canonical.display());
    }

    println!("\n✅ Proof pack and snapshot generated!");
    Ok(())
}

async fn handle_reconcile(
    metric: String,
    baseline: String,
    _attribution: Option<String>,
    scenario: Option<String>,
    common: CommonOptions,
) -> anyhow::Result<()> {
    println!("🔍 Semstrait Demo - Reconcile Mode: {}", metric);
    println!("=====================================");

    let temp_dir = TempDir::new()?;
    let (adwords_path, facebook_path) = parquet_generation::generate_fixtures(&temp_dir, scenario.as_deref(), Some(&common.as_of))?;
    let table_paths = setup_table_paths(&temp_dir);

    let mut schema = load_and_override_schema(&adwords_path, &facebook_path, None)?;
    let model_name = "marketing-demo".to_string();
    let model = schema.get_model(&model_name)
        .ok_or_else(|| anyhow::anyhow!("Model not found"))?;

    let ctx = SessionContext::new();

    let reconciliation = reconcile::execute_reconciliation(
        &ctx,
        &schema,
        model,
        &metric,
        &table_paths,
    ).await?;

    let baseline_label = if baseline.is_empty() || baseline == "raw" {
        "platform:facebook".to_string()
    } else {
        baseline.clone()
    };
    reconcile::print_reconciliation(&reconciliation, &baseline_label, scenario.as_deref())?;

    let snapshot_id = format!(
        "reconcile_{}_{}",
        metric.replace(|c: char| !c.is_alphanumeric(), "_"),
        chrono::Utc::now().format("%Y%m%d_%H%M%S")
    );
    let snapshot_dir = std::path::Path::new(".semstrait_demo").join("snapshots").join(&snapshot_id);
    reconcile::save_reconcile_artifacts(&snapshot_dir, &reconciliation, &baseline_label, &common.timezone, scenario.as_deref())?;

    println!("\n💾 Saved to: {}", snapshot_dir.display());
    if let Ok(canonical) = snapshot_dir.canonicalize() {
        println!("🔗 Shareable link: file://{}", canonical.display());
    }
    println!("\n✅ Reconciliation completed!");
    Ok(())
}

async fn setup_common(
    as_of: String,
    timezone: String,
    currency: String,
    fx_rate: f64,
    attribution_window: u32,
    rows: Option<Vec<String>>,
    metrics: Option<Vec<String>>,
    scenario: Option<String>,
    model_path: Option<String>,
) -> anyhow::Result<(semstrait::Schema, String, semstrait::QueryRequest, semstrait::plan::PlanNode, substrait::proto::Plan, ReproducibilityParams, String, TempDir, HashMap<String, String>)> {
    let temp_dir = TempDir::new()?;
    println!("📁 Using temp directory: {}", temp_dir.path().display());

    println!("\n📊 Generating Parquet fixtures...");
    let (adwords_path, facebook_path) = parquet_generation::generate_fixtures(&temp_dir, scenario.as_deref(), Some(&as_of))?;

    let mut schema = load_and_override_schema(&adwords_path, &facebook_path, model_path.as_deref())?;

    let repro_params = ReproducibilityParams {
        as_of: as_of.to_string(),
        timezone: timezone.to_string(),
        currency: currency.to_string(),
        fx_rate,
        attribution_window,
    };

    let request = semstrait::QueryRequest {
        model: "marketing-demo".to_string(),
        rows,
        metrics,
        ..Default::default()
    };

    println!("\n🔍 Query Request:");
    println!("  Model: {}", request.model);
    if let Some(ref rows) = request.rows {
        println!("  Rows: {:?}", rows);
    } else {
        println!("  Rows: (none - aggregate only)");
    }
    if let Some(ref metrics) = request.metrics {
        println!("  Metrics: {:?}", metrics);
    }

    let model_name = request.model.clone();

    let plan_node = {
        let model = schema.get_model(&model_name)
            .ok_or_else(|| anyhow::anyhow!("Model not found"))?;
        println!("\n🏗️  Planning semantic query...");
        semstrait::planner::plan_semantic_query(&schema, model, &request)?
    };

    let table_paths = setup_table_paths(&temp_dir);

    println!("📤 Emitting Substrait plan...");
    let substrait_plan = semstrait::emitter::emit_plan(&plan_node, None)?;

    let snapshot_id = execution::compute_snapshot_id(&schema, &request, &repro_params, &substrait_plan, &table_paths).await?;

    Ok((schema, model_name, request, plan_node, substrait_plan, repro_params, snapshot_id, temp_dir, table_paths))
}

async fn execute_common(plan_node: &semstrait::plan::PlanNode, table_paths: &HashMap<String, String>) -> anyhow::Result<Vec<datafusion::arrow::record_batch::RecordBatch>> {
    println!("\n⚡ Executing Substrait Plan...");
    execution::execute_substrait_plan_via_df_exec(&SessionContext::new(), plan_node, table_paths).await
}

pub fn setup_table_paths(temp_dir: &TempDir) -> HashMap<String, String> {
    let mut table_paths = HashMap::new();
    table_paths.insert(
        "adwords_campaigns".to_string(),
        temp_dir.path().join("adwords_campaigns.parquet").to_string_lossy().to_string(),
    );
    table_paths.insert(
        "facebook_campaigns".to_string(),
        temp_dir.path().join("facebook_campaigns.parquet").to_string_lossy().to_string(),
    );
    table_paths
}

pub fn load_and_override_schema(adwords_path: &str, facebook_path: &str, model_path: Option<&str>) -> anyhow::Result<semstrait::Schema> {
    let schema_yaml = if let Some(model_path) = model_path {
        std::fs::read_to_string(model_path)?
    } else {
        include_str!("../model.yaml").to_string()
    };
    let mut schema = semstrait::parser::parse_str(&schema_yaml)?;

    for model in &mut schema.semantic_models {
        for table_group in &mut model.dataset_groups {
            for table in &mut table_group.datasets {
                let new_path = match table.dataset.as_str() {
                    "adwords_campaigns" => adwords_path,
                    "facebook_campaigns" => facebook_path,
                    _ => continue,
                };

                if let semstrait::semantic_model::Source::Parquet { path } = &mut table.source {
                    *path = new_path.to_string();
                }
            }
        }
    }

    Ok(schema)
}
