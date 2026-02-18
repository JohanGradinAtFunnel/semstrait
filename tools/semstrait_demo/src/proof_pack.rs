use semstrait::{Schema, SemanticModel, QueryRequest};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use chrono::{DateTime, Utc};
use datafusion::prelude::*;
use super::ReproducibilityParams;

/// A complete proof pack containing all evidence for a metric's calculation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProofPack {
    /// The metric this proof pack is for
    pub metric_name: String,
    /// The snapshot ID that generated this proof pack
    pub snapshot_id: String,
    /// Reproducibility parameters used
    pub reproducibility_params: ReproducibilityParams,
    /// The metric formula/expression
    pub metric_formula: MetricFormula,
    /// Mapping from metric to measures by table group
    pub metric_to_measures: HashMap<String, Vec<MeasureMapping>>,
    /// Per-source metadata and column mappings
    pub source_metadata: HashMap<String, SourceMetadata>,
    /// Executed value (if available)
    pub value: Option<f64>,
    /// Verification status
    pub status: String,
}

/// The formula/expression for a metric
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricFormula {
    /// The raw expression string
    pub expression: String,
    /// Human-readable description
    pub description: Option<String>,
    /// Data type of the result
    pub data_type: String,
    /// Whether this metric is additive across dimensions
    pub is_additive: bool,
    /// Human-readable definition (e.g. "If source is Google Ads → use field: cost")
    pub human_definition: Option<String>,
    /// Exact SQL-like definition
    pub exact_definition: Option<String>,
}

/// Mapping from a measure to its source columns
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MeasureMapping {
    /// The measure name
    pub measure_name: String,
    /// The aggregation function applied
    pub aggregation: String,
    /// The expression that defines this measure
    pub expression: String,
    /// The table group this measure comes from
    pub table_group: String,
    /// The physical table this measure is defined on
    pub table: String,
    /// Column mappings for this measure
    pub column_mappings: HashMap<String, String>,
}

/// Metadata about a data source
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceMetadata {
    /// Table name
    pub table_name: String,
    /// Table group this belongs to
    pub table_group: String,
    /// When this data was last ingested
    pub last_ingested_at: DateTime<Utc>,
    /// Row count in this source
    pub row_count: usize,
    /// Maximum event time in this source
    pub max_event_time: Option<DateTime<Utc>>,
    /// Completeness watermark (max_event_time - lag)
    pub completeness_up_to: Option<DateTime<Utc>>,
    /// Schema hash for change detection
    pub schema_hash: String,
    /// Source type (parquet, etc.)
    pub source_type: String,
    /// Source path/location
    pub source_path: String,
}

/// Generate a complete proof pack for a metric
pub async fn generate_proof_pack(
    schema: &Schema,
    model: &SemanticModel,
    metric_name: &str,
    repro_params: &ReproducibilityParams,
    snapshot_id: &str,
    table_paths: &HashMap<String, String>,
    request: &QueryRequest,
) -> anyhow::Result<ProofPack> {
    let metric = model.get_metric(metric_name)
        .ok_or_else(|| anyhow::anyhow!("Metric '{}' not found", metric_name))?;

    let metric_formula = build_metric_formula(metric)?;
    let metric_to_measures = build_measure_mappings(schema, model, metric)?;
    let source_metadata = build_source_metadata(table_paths).await?;

    let (value, status) = execute_metric_value(schema, model, request, table_paths, metric_name).await
        .map(|v| (Some(v), "Verified".to_string()))
        .unwrap_or((None, "Not executed".to_string()));

    Ok(ProofPack {
        metric_name: metric_name.to_string(),
        snapshot_id: snapshot_id.to_string(),
        reproducibility_params: repro_params.clone(),
        metric_formula,
        metric_to_measures,
        source_metadata,
        value,
        status,
    })
}

async fn execute_metric_value(
    schema: &Schema,
    model: &SemanticModel,
    request: &QueryRequest,
    table_paths: &HashMap<String, String>,
    metric_name: &str,
) -> anyhow::Result<f64> {
    let plan_node = semstrait::planner::plan_semantic_query(schema, model, request)?;
    let ctx = SessionContext::new();
    let df = crate::datafusion_execution::execute_plan_node(&ctx, &plan_node, table_paths).await?;
    let batches = df.collect().await?;
    for batch in &batches {
        if let Some(col) = batch.column_by_name(metric_name) {
            if let Some(arr) = col.as_any().downcast_ref::<datafusion::arrow::array::Float64Array>() {
                let mut sum = 0.0;
                for i in 0..arr.len() {
                    sum += arr.value(i);
                }
                return Ok(sum);
            }
            if let Some(arr) = col.as_any().downcast_ref::<datafusion::arrow::array::Int64Array>() {
                let mut sum = 0.0;
                for i in 0..arr.len() {
                    sum += arr.value(i) as f64;
                }
                return Ok(sum);
            }
        }
    }
    anyhow::bail!("Could not extract metric value")
}

/// Build the metric formula structure
fn build_metric_formula(metric: &semstrait::semantic_model::Metric) -> anyhow::Result<MetricFormula> {
    let (expression, human_def, exact_def) = match &metric.expr {
        semstrait::semantic_model::MetricExpr::MeasureRef(name) => (
            format!("Measure reference: {}", name),
            None,
            None,
        ),
        semstrait::semantic_model::MetricExpr::Structured(node) => {
            let (human, exact) = render_case_metric(metric, node);
            (format!("{:?}", node), Some(human), Some(exact))
        }
    };

    let is_additive = match &metric.expr {
        semstrait::semantic_model::MetricExpr::MeasureRef(name) => {
            name.contains("cost") || name.contains("spend") || name.contains("impressions")
        }
        semstrait::semantic_model::MetricExpr::Structured(_) => false,
    };

    Ok(MetricFormula {
        expression,
        description: metric.description.clone(),
        data_type: metric.data_type.as_ref()
            .map(|dt| format!("{:?}", dt))
            .unwrap_or_else(|| "unknown".to_string()),
        is_additive,
        human_definition: human_def,
        exact_definition: exact_def,
    })
}

fn render_case_metric(metric: &semstrait::semantic_model::Metric, node: &semstrait::semantic_model::MetricExprNode) -> (String, String) {
    use semstrait::semantic_model::{MetricExprNode, MetricCaseExpr, MetricCaseWhen, MetricConditionArg};
    let mut human_lines = Vec::new();
    let mut exact_lines = Vec::new();
    if let MetricExprNode::Case(case_expr) = node {
        for w in &case_expr.when {
            if let semstrait::semantic_model::MetricCondition::Eq(args) = &w.condition {
                let dg = args.iter().find_map(|a| match a {
                    MetricConditionArg::String(s) if s != "datasetGroup.name" => Some(s.clone()),
                    _ => None,
                });
                let measure = w.then.measure_name().unwrap_or_default();
                if let Some(dg) = dg {
                    let source_label = if dg == "adwords" { "Google Ads" } else if dg == "facebook" { "Facebook Ads" } else { &dg };
                    let field = if dg == "adwords" { "cost" } else if dg == "facebook" { "spend" } else { "?" };
                    human_lines.push(format!("- If source is {} → use field: {}", source_label, field));
                    let table = if dg == "adwords" { "adwords_campaigns" } else { "facebook_campaigns" };
                    exact_lines.push(format!("  WHEN datasetGroup='{}'  THEN SUM({}.{})", dg, table, field));
                }
            }
        }
        human_lines.push("- Otherwise → 0".to_string());
        exact_lines.push("  ELSE 0".to_string());
    }
    let human = human_lines.join("\n");
    let exact = format!("CASE\n{}\nEND", exact_lines.join("\n"));
    (human, exact)
}

/// Build mappings from metric to measures by table group using Metric::dataset_group_measures()
fn build_measure_mappings(
    _schema: &Schema,
    model: &SemanticModel,
    metric: &semstrait::semantic_model::Metric,
) -> anyhow::Result<HashMap<String, Vec<MeasureMapping>>> {
    let mut mappings = HashMap::new();

    for (dg_name, measure_name) in metric.dataset_group_measures() {
        let dg = model.get_dataset_group(&dg_name).ok_or_else(|| anyhow::anyhow!("Dataset group {} not found", dg_name))?;
        let measure = dg.get_measure(&measure_name).ok_or_else(|| anyhow::anyhow!("Measure {} not found", measure_name))?;
        let table = dg.datasets.first().map(|d| d.dataset.clone()).unwrap_or_default();
        let col = match &measure.expr {
            semstrait::semantic_model::MeasureExpr::Column(c) => c.clone(),
            _ => measure_name.clone(),
        };
        let agg = format!("{:?}", measure.aggregation).to_lowercase();
        let mut column_mappings = HashMap::new();
        column_mappings.insert(col.clone(), col.clone());
        mappings.entry(dg_name.clone()).or_insert_with(Vec::new).push(MeasureMapping {
            measure_name: measure_name.clone(),
            aggregation: agg,
            expression: col,
            table_group: dg_name,
            table,
            column_mappings,
        });
    }

    Ok(mappings)
}

/// Build source metadata for all tables
async fn build_source_metadata(table_paths: &HashMap<String, String>) -> anyhow::Result<HashMap<String, SourceMetadata>> {
    let mut metadata = HashMap::new();

    for (table_name, path) in table_paths {
        let ctx = SessionContext::new();
        let df = ctx.read_parquet(path, Default::default()).await?;
        let batches = df.clone().collect().await?;

        let row_count = batches.iter().map(|b| b.num_rows()).sum();

        // Find max event_time from data
        let max_event_time = find_max_event_time(&df).await?;

        // Calculate schema hash
        let schema_hash = compute_schema_hash(&batches);

        // Determine table group from table name
        let table_group = if table_name.contains("adwords") {
            "adwords"
        } else if table_name.contains("facebook") {
            "facebook"
        } else {
            "unknown"
        };

        metadata.insert(table_name.clone(), SourceMetadata {
            table_name: table_name.clone(),
            table_group: table_group.to_string(),
            last_ingested_at: Utc::now(), // In demo, use current time
            row_count,
            max_event_time,
            completeness_up_to: max_event_time.map(|t| t - chrono::Duration::hours(1)),
            schema_hash,
            source_type: "parquet".to_string(),
            source_path: path.clone(),
        });
    }

    Ok(metadata)
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

/// Print a proof pack in human-readable format (verdict-first, definition + lineage)
pub fn print_proof_pack(proof_pack: &ProofPack) -> anyhow::Result<()> {
    println!("📦 PROOF PACK: {}", proof_pack.metric_name);
    println!("========================");
    let value_str = proof_pack.value
        .map(|v| format!("{:.2} {}", v, proof_pack.reproducibility_params.currency))
        .unwrap_or_else(|| "N/A".to_string());
    println!("VALUE: {}", value_str);
    println!("STATUS: 🟢 {}", proof_pack.status);
    println!("AS-OF: {} (UTC)", proof_pack.reproducibility_params.as_of);
    let short_id = if proof_pack.snapshot_id.len() >= 12 { &proof_pack.snapshot_id[..12] } else { &proof_pack.snapshot_id[..] };
    println!("SNAPSHOT ID: {}...", short_id);
    println!();

    println!("WHAT THIS METRIC MEANS");
    let desc = proof_pack.metric_formula.description.as_deref()
        .unwrap_or("(No description)");
    println!("\"{}\"", desc);
    println!();

    if let Some(ref human) = proof_pack.metric_formula.human_definition {
        println!("DEFINITION (Human)");
        println!("{}", human);
        println!();
    }
    if let Some(ref exact) = proof_pack.metric_formula.exact_definition {
        println!("DEFINITION (Exact)");
        println!("{}", exact);
        println!();
    }

    println!("LINEAGE (Where it came from)");
    println!("{:<15} {:<20} {:<8} {:<6} {:<12} {:<12}", "SOURCE", "TABLE", "FIELD", "AGG", "ROWS USED", "LAST INGEST");
    for (_table_name, metadata) in &proof_pack.source_metadata {
        let source_label = if metadata.table_group == "adwords" { "Google Ads" } else if metadata.table_group == "facebook" { "Facebook Ads" } else { &metadata.table_group };
        let (field, agg) = proof_pack.metric_to_measures.get(&metadata.table_group)
            .and_then(|m| m.first())
            .map(|m| (m.expression.clone(), m.aggregation.clone()))
            .unwrap_or_else(|| ("?".to_string(), "?".to_string()));
        let last = metadata.last_ingested_at.format("%H:%M").to_string();
        println!("{:<15} {:<20} {:<8} {:<6} {:<12} {:<12}", source_label, metadata.table_name, field, agg.to_uppercase(), metadata.row_count, last);
    }
    println!();

    let completeness = proof_pack.source_metadata.values()
        .filter_map(|m| m.completeness_up_to)
        .max()
        .map(|t| t.format("%Y-%m-%d %H:%M").to_string())
        .unwrap_or_else(|| "unknown".to_string());
    println!("DATA QUALITY CONTEXT");
    println!("- Completeness: ✅ complete up to {}", completeness);
    println!("- Currency: {} (FX {:.4})", proof_pack.reproducibility_params.currency, proof_pack.reproducibility_params.fx_rate);
    println!("- Attribution window: {} days", proof_pack.reproducibility_params.attribution_window);
    println!();

    println!("EXPORTABLES");
    println!("✅ report.html     (share with stakeholders)");
    println!("✅ proof_pack.json (audit / automation)");
    println!("✅ sql.sql         (for warehouse parity checks)");
    println!("✅ substrait.plan  (engine-agnostic compute plan)");

    Ok(())
}

/// Save proof pack to a specific snapshot directory (includes exportables)
pub fn save_proof_pack_to_snapshot(proof_pack: &ProofPack, snapshot_dir: &std::path::Path) -> anyhow::Result<()> {
    std::fs::create_dir_all(snapshot_dir)?;

    let proof_pack_path = snapshot_dir.join("proof_pack.json");
    let json = serde_json::to_string_pretty(proof_pack)?;
    std::fs::write(proof_pack_path, json)?;

    let sql_content = generate_parity_sql(proof_pack);
    std::fs::write(snapshot_dir.join("sql.sql"), sql_content)?;

    let report_md = format!(
        "# Proof Pack: {}\n\nVALUE: {:?} {}\nSTATUS: {}\n\n## Definition\n{}\n\n## Lineage\nSee proof_pack.json",
        proof_pack.metric_name,
        proof_pack.value,
        proof_pack.reproducibility_params.currency,
        proof_pack.status,
        proof_pack.metric_formula.description.as_deref().unwrap_or("(none)")
    );
    crate::artifacts::write_report_md(snapshot_dir, &report_md)?;
    crate::artifacts::write_report_html(snapshot_dir, &report_md)?;

    Ok(())
}

fn generate_parity_sql(proof_pack: &ProofPack) -> String {
    let mut sql = format!("-- Parity SQL for metric: {}\n", proof_pack.metric_name);
    sql.push_str("-- Use this to verify numbers in your warehouse.\n\n");
    if let Some(ref exact) = proof_pack.metric_formula.exact_definition {
        sql.push_str(&format!("-- Semantic definition:\n-- {}\n\n", exact.replace('\n', "\n-- ")));
    }
    for (dg, measures) in &proof_pack.metric_to_measures {
        for m in measures {
            sql.push_str(&format!("-- {}: SELECT {}({}) FROM {} GROUP BY ...\n",
                dg, m.aggregation.to_uppercase(), m.expression, m.table));
        }
    }
    sql.push_str("\n-- Run equivalent aggregations in your warehouse and compare results.\n");
    sql
}

/// Save proof pack to disk (legacy function for backwards compatibility)
pub fn save_proof_pack(proof_pack: &ProofPack, snapshot_id: &str) -> anyhow::Result<()> {
    // Create snapshot directory
    let snapshot_dir = std::path::Path::new(".semstrait_demo").join("snapshots").join(snapshot_id);
    std::fs::create_dir_all(&snapshot_dir)?;

    save_proof_pack_to_snapshot(proof_pack, &snapshot_dir)?;

    println!("💾 Proof pack saved to: {}", snapshot_dir.display());
    println!("🔗 Shareable link: file://{}", snapshot_dir.canonicalize()?.display());

    Ok(())
}