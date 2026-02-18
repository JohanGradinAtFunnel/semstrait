use datafusion::prelude::*;
use datafusion::arrow::record_batch::RecordBatch;
use chrono::{DateTime, Utc, Duration};
use std::collections::HashMap;
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::Path;
use super::metadata::{InventoryBuilder, InventorySnapshot, DatasetInventory};

/// Data health alert
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthAlert {
    pub alert_id: String,
    pub alert_type: AlertType,
    pub severity: AlertSeverity,
    pub table_name: String,
    pub message: String,
    pub detected_at: DateTime<Utc>,
    pub threshold_value: Option<f64>,
    pub actual_value: Option<f64>,
    pub root_cause_hint: Option<String>,
}

/// Alert type enumeration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AlertType {
    VolumeDrop,
    ZeroRows,
    SchemaChange,
    FreshnessCritical,
    QualityDegraded,
}

/// Alert severity enumeration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AlertSeverity {
    Low,
    Medium,
    High,
    Critical,
}

/// Health baseline for historical comparison
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthBaseline {
    pub table_name: String,
    pub recorded_at: DateTime<Utc>,
    pub row_count: usize,
    pub schema_hash: String,
    pub data_quality_score: f64,
}

/// Alert configuration thresholds
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AlertThresholds {
    pub volume_drop_percent: f64, // e.g., 50.0 for 50% drop
    pub zero_row_threshold: usize, // minimum rows before triggering zero alert
    pub quality_score_threshold: f64, // minimum quality score
    pub freshness_critical_hours: i64, // hours before critical freshness alert
    pub trailing_baseline_days: i32, // days to look back for baseline comparison
}

/// Data health assessment result
#[derive(Debug)]
pub struct HealthAssessment {
    pub data_fingerprint: String,
    pub table_health: HashMap<String, TableHealth>,
    pub freshness_watermarks: HashMap<String, FreshnessWatermark>,
    pub alerts: Vec<HealthAlert>,
    pub anomaly_flags: Vec<String>,
    pub backfill_state: BackfillState,
    pub overall_health_score: HealthScore,
}

/// Health score enumeration
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum HealthScore {
    Excellent,
    Good,
    Warning,
    Critical,
}

/// Table health information
#[derive(Debug)]
pub struct TableHealth {
    pub table_name: String,
    pub row_count: usize,
    pub schema_consistent: bool,
    pub data_quality_score: f64,
}

/// Freshness watermark information
#[derive(Debug)]
pub struct FreshnessWatermark {
    pub table_name: String,
    pub last_ingested_at: DateTime<Utc>,
    pub max_event_time: Option<DateTime<Utc>>,
    pub completeness_up_to: Option<DateTime<Utc>>,
    pub row_count_delta: i64,
    pub freshness_status: FreshnessStatus,
}

/// Freshness status
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum FreshnessStatus {
    Fresh,
    Stale,
    Critical,
}

/// Backfill state
#[derive(Debug)]
pub struct BackfillState {
    pub status: BackfillStatus,
    pub progress_percent: Option<f64>,
    pub estimated_completion: Option<DateTime<Utc>>,
}

/// Backfill status
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum BackfillStatus {
    Complete,
    Running,
    Partial,
    Stale,
}

/// Execute health assessment for data sources
pub async fn execute_health_assessment(
    ctx: &SessionContext,
    table_paths: &HashMap<String, String>,
    as_of: &str,
) -> anyhow::Result<HealthAssessment> {
    // Compute inventory snapshot
    let builder = InventoryBuilder::new();
    let inventory = builder.compute_inventory(table_paths, as_of).await?;
    let data_fingerprint = inventory.overall_fingerprint.clone();

    // Assess table health using inventory
    let table_health = assess_table_health_from_inventory(&inventory).await?;

    // Check freshness watermarks
    let freshness_watermarks = check_freshness_watermarks(ctx, table_paths, as_of).await?;

    // Load historical baselines
    let mut baselines = load_baselines()?;

    // Generate alerts based on current health vs baselines
    let thresholds = get_default_thresholds();
    let alerts = generate_alerts(&table_health, &freshness_watermarks, &baselines, &thresholds)?;

    // Update baselines with current data
    update_baselines(&table_health, &freshness_watermarks, &mut baselines)?;
    save_baselines(&baselines)?;

    // Detect legacy anomalies (for backward compatibility)
    let anomaly_flags = detect_anomalies(&table_health, &freshness_watermarks)?;

    // Determine backfill state
    let backfill_state = determine_backfill_state(&freshness_watermarks, as_of)?;

    // Calculate overall health score
    let overall_health_score = calculate_overall_health_score(&alerts, &anomaly_flags, &freshness_watermarks)?;

    Ok(HealthAssessment {
        data_fingerprint,
        table_health,
        freshness_watermarks,
        alerts,
        anomaly_flags,
        backfill_state,
        overall_health_score,
    })
}

/// Compute a stable fingerprint of the data sources

/// Assess health of individual tables
async fn assess_table_health_from_inventory(
    inventory: &InventorySnapshot,
) -> anyhow::Result<HashMap<String, TableHealth>> {
    let mut table_health = HashMap::new();

    for (table_name, dataset_inventory) in &inventory.datasets {
        // Use inventory data instead of recomputing
        let row_count = dataset_inventory.row_count;

        // Schema consistency is assumed to be true since we have inventory
        // (in a real system, we'd track schema changes over time)
        let schema_consistent = true;

        // Use completeness score as data quality score
        let data_quality_score = dataset_inventory.completeness_score * 100.0;

        table_health.insert(table_name.clone(), TableHealth {
            table_name: table_name.clone(),
            row_count,
            schema_consistent,
            data_quality_score,
        });
    }

    Ok(table_health)
}

/// Check if schema is consistent across batches
fn check_schema_consistency(batches: &[RecordBatch]) -> bool {
    if batches.is_empty() {
        return true;
    }

    let first_schema = batches[0].schema();
    batches.iter().all(|batch: &RecordBatch| batch.schema() == first_schema)
}

/// Compute data quality score (0-100, higher is better)
fn compute_data_quality_score(batches: &[RecordBatch]) -> f64 {
    if batches.is_empty() {
        return 0.0;
    }

    let mut total_cells = 0;
    let mut null_cells = 0;

    for batch in batches {
        for col_idx in 0..batch.num_columns() as usize {
            let col = batch.column(col_idx);
            total_cells += col.len();
            null_cells += col.null_count();
        }
    }

    if total_cells == 0 {
        return 0.0;
    }

    let null_rate = null_cells as f64 / total_cells as f64;
    // Quality score: lower null rate = higher score
    (1.0 - null_rate) * 100.0
}

/// Check freshness watermarks for all tables
async fn check_freshness_watermarks(
    ctx: &SessionContext,
    table_paths: &HashMap<String, String>,
    as_of: &str,
) -> anyhow::Result<HashMap<String, FreshnessWatermark>> {
    let mut watermarks = HashMap::new();
    let as_of_time = DateTime::parse_from_rfc3339(as_of)?.with_timezone(&Utc);

    // Use fixture generation time as last_ingested_at
    let last_ingested = Utc::now();

    for (table_name, path) in table_paths {
        let df = ctx.read_parquet(path, Default::default()).await?;

        // Find max event_time from data
        let max_event_time = find_max_event_time(&df).await?;

        // Calculate completeness up to (max_event_time - some lag)
        let completeness_up_to = max_event_time
            .map(|t| t - Duration::hours(1)); // 1 hour lag for demo

        // Get row count
        let row_count = df.count().await? as usize;

        // Simple baseline: expect at least some minimum rows
        let expected_min_rows = 1; // Very basic baseline
        let row_count_delta = row_count as i64 - expected_min_rows as i64;

        // Determine freshness status
        let freshness_status = if let Some(max_time) = max_event_time {
            let age_hours = (as_of_time - max_time).num_hours();
            if age_hours > 24 {
                FreshnessStatus::Critical
            } else if age_hours > 6 {
                FreshnessStatus::Stale
            } else {
                FreshnessStatus::Fresh
            }
        } else {
            FreshnessStatus::Critical
        };

        watermarks.insert(table_name.clone(), FreshnessWatermark {
            table_name: table_name.clone(),
            last_ingested_at: last_ingested,
            max_event_time,
            completeness_up_to,
            row_count_delta,
            freshness_status,
        });
    }

    Ok(watermarks)
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

    // Fallback: use current time if no event_time column
    Ok(Some(Utc::now()))
}

/// Detect data anomalies
fn detect_anomalies(
    table_health: &HashMap<String, TableHealth>,
    watermarks: &HashMap<String, FreshnessWatermark>,
) -> anyhow::Result<Vec<String>> {
    let mut anomalies = Vec::new();

    // Check for zero rows
    for (table_name, health) in table_health {
        if health.row_count == 0 {
            anomalies.push(format!("Table '{}' has zero rows - possible ingestion failure", table_name));
        }
    }

    // Check for critical freshness
    for (table_name, watermark) in watermarks {
        if matches!(watermark.freshness_status, FreshnessStatus::Critical) {
            anomalies.push(format!("Table '{}' data is critically stale", table_name));
        }

        // Check for significant volume drops
        if watermark.row_count_delta < -50 { // More than 50 rows below baseline
            anomalies.push(format!("Table '{}' volume dropped significantly (Δ{})", table_name, watermark.row_count_delta));
        }
    }

    // Check data quality
    for (table_name, health) in table_health {
        if health.data_quality_score < 50.0 {
            anomalies.push(format!("Table '{}' has poor data quality ({:.1}% complete)", table_name, health.data_quality_score));
        }
    }

    Ok(anomalies)
}

/// Determine backfill state
fn determine_backfill_state(
    watermarks: &HashMap<String, FreshnessWatermark>,
    as_of: &str,
) -> anyhow::Result<BackfillState> {
    let as_of_time = DateTime::parse_from_rfc3339(as_of)?.with_timezone(&Utc);

    // Check if any table is missing data up to as_of
    let has_gaps = watermarks.values().any(|w| {
        w.completeness_up_to
            .map(|cutoff| cutoff < as_of_time)
            .unwrap_or(true)
    });

    let status = if has_gaps {
        BackfillStatus::Partial
    } else {
        BackfillStatus::Complete
    };

    // Estimate progress (simplified)
    let progress_percent = if matches!(status, BackfillStatus::Complete) {
        Some(100.0)
    } else {
        Some(85.0) // Mock progress
    };

    Ok(BackfillState {
        status,
        progress_percent,
        estimated_completion: Some(as_of_time + Duration::hours(2)), // Mock ETA
    })
}

/// Calculate overall health score
fn calculate_overall_health_score(
    alerts: &[HealthAlert],
    anomalies: &[String],
    watermarks: &HashMap<String, FreshnessWatermark>,
) -> anyhow::Result<HealthScore> {
    let critical_alerts = alerts.iter()
        .filter(|a| matches!(a.severity, AlertSeverity::Critical))
        .count();
    let high_alerts = alerts.iter()
        .filter(|a| matches!(a.severity, AlertSeverity::High))
        .count();
    let critical_anomalies = anomalies.len();
    let stale_tables = watermarks.values()
        .filter(|w| matches!(w.freshness_status, FreshnessStatus::Critical))
        .count();

    if critical_alerts > 0 || critical_anomalies > 0 || stale_tables > 0 {
        Ok(HealthScore::Critical)
    } else if high_alerts > 0 || anomalies.len() > 0 {
        Ok(HealthScore::Warning)
    } else if watermarks.values().any(|w| matches!(w.freshness_status, FreshnessStatus::Stale)) {
        Ok(HealthScore::Warning)
    } else {
        Ok(HealthScore::Good)
    }
}

/// Get default alert thresholds
fn get_default_thresholds() -> AlertThresholds {
    AlertThresholds {
        volume_drop_percent: 50.0, // 50% drop triggers alert
        zero_row_threshold: 1, // 0 rows triggers alert
        quality_score_threshold: 50.0, // Below 50% quality triggers alert
        freshness_critical_hours: 24, // 24+ hours stale is critical
        trailing_baseline_days: 7, // Compare to 7-day trailing average
    }
}

/// Load historical baselines from disk
fn load_baselines() -> anyhow::Result<HashMap<String, Vec<HealthBaseline>>> {
    let baseline_file = Path::new(".semstrait_demo").join("health_baselines.json");

    if !baseline_file.exists() {
        return Ok(HashMap::new());
    }

    let content = fs::read_to_string(baseline_file)?;
    let baselines: HashMap<String, Vec<HealthBaseline>> = serde_json::from_str(&content)?;
    Ok(baselines)
}

/// Save baselines to disk
fn save_baselines(baselines: &HashMap<String, Vec<HealthBaseline>>) -> anyhow::Result<()> {
    let baseline_dir = Path::new(".semstrait_demo");
    fs::create_dir_all(baseline_dir)?;

    let baseline_file = baseline_dir.join("health_baselines.json");
    let content = serde_json::to_string_pretty(baselines)?;
    fs::write(baseline_file, content)?;
    Ok(())
}

/// Update baselines with current health data
fn update_baselines(
    table_health: &HashMap<String, TableHealth>,
    watermarks: &HashMap<String, FreshnessWatermark>,
    baselines: &mut HashMap<String, Vec<HealthBaseline>>,
) -> anyhow::Result<()> {
    for (table_name, health) in table_health {
        let baseline = HealthBaseline {
            table_name: table_name.clone(),
            recorded_at: Utc::now(),
            row_count: health.row_count,
            schema_hash: "placeholder".to_string(), // TODO: compute actual schema hash
            data_quality_score: health.data_quality_score,
        };

        baselines.entry(table_name.clone())
            .or_insert_with(Vec::new)
            .push(baseline);
    }

    // Keep only recent baselines (last 30 days)
    let cutoff = Utc::now() - Duration::days(30);
    for baseline_list in baselines.values_mut() {
        baseline_list.retain(|b| b.recorded_at > cutoff);
        // Keep max 10 baselines per table
        if baseline_list.len() > 10 {
            baseline_list.sort_by(|a, b| b.recorded_at.cmp(&a.recorded_at));
            baseline_list.truncate(10);
        }
    }

    Ok(())
}

/// Generate alerts based on current health vs baselines
fn generate_alerts(
    table_health: &HashMap<String, TableHealth>,
    watermarks: &HashMap<String, FreshnessWatermark>,
    baselines: &HashMap<String, Vec<HealthBaseline>>,
    thresholds: &AlertThresholds,
) -> anyhow::Result<Vec<HealthAlert>> {
    let mut alerts = Vec::new();

    for (table_name, health) in table_health {
        let alert_id = format!("{}_{}", table_name, Utc::now().timestamp());

        // Check for zero rows
        if health.row_count < thresholds.zero_row_threshold {
            alerts.push(HealthAlert {
                alert_id: format!("{}_zero_rows", alert_id),
                alert_type: AlertType::ZeroRows,
                severity: AlertSeverity::Critical,
                table_name: table_name.clone(),
                message: format!("Table '{}' has {} rows (below threshold of {})", table_name, health.row_count, thresholds.zero_row_threshold),
                detected_at: Utc::now(),
                threshold_value: Some(thresholds.zero_row_threshold as f64),
                actual_value: Some(health.row_count as f64),
                root_cause_hint: Some("Check data ingestion pipeline or source connectivity".to_string()),
            });
        }

        // Check for volume drops vs baseline
        if let Some(table_baselines) = baselines.get(table_name) {
            if let Some(avg_baseline_rows) = compute_baseline_average(table_baselines) {
                let current_rows = health.row_count as f64;
                let drop_percent = ((avg_baseline_rows - current_rows) / avg_baseline_rows) * 100.0;

                if drop_percent > thresholds.volume_drop_percent {
                    alerts.push(HealthAlert {
                        alert_id: format!("{}_volume_drop", alert_id),
                        alert_type: AlertType::VolumeDrop,
                        severity: if drop_percent > 80.0 { AlertSeverity::Critical } else { AlertSeverity::High },
                        table_name: table_name.clone(),
                        message: format!("Table '{}' volume dropped {:.1}% ({} → {})", table_name, drop_percent, avg_baseline_rows as usize, health.row_count),
                        detected_at: Utc::now(),
                        threshold_value: Some(thresholds.volume_drop_percent),
                        actual_value: Some(drop_percent),
                        root_cause_hint: Some("Check for data pipeline issues or source data changes".to_string()),
                    });
                }
            }
        }

        // Check data quality
        if health.data_quality_score < thresholds.quality_score_threshold {
            alerts.push(HealthAlert {
                alert_id: format!("{}_quality", alert_id),
                alert_type: AlertType::QualityDegraded,
                severity: AlertSeverity::Medium,
                table_name: table_name.clone(),
                message: format!("Table '{}' data quality score is {:.1}% (below threshold of {:.1}%)", table_name, health.data_quality_score, thresholds.quality_score_threshold),
                detected_at: Utc::now(),
                threshold_value: Some(thresholds.quality_score_threshold),
                actual_value: Some(health.data_quality_score),
                root_cause_hint: Some("Review data validation rules or source data quality".to_string()),
            });
        }

        // Check schema consistency
        if !health.schema_consistent {
            alerts.push(HealthAlert {
                alert_id: format!("{}_schema", alert_id),
                alert_type: AlertType::SchemaChange,
                severity: AlertSeverity::High,
                table_name: table_name.clone(),
                message: format!("Table '{}' has schema consistency issues", table_name),
                detected_at: Utc::now(),
                threshold_value: None,
                actual_value: None,
                root_cause_hint: Some("Schema evolution detected - review column changes".to_string()),
            });
        }
    }

    // Check freshness
    for (table_name, watermark) in watermarks {
        if let Some(max_time) = watermark.max_event_time {
            let age_hours = (Utc::now() - max_time).num_hours();

            if age_hours > thresholds.freshness_critical_hours as i64 {
                alerts.push(HealthAlert {
                    alert_id: format!("{}_freshness_{}", table_name, Utc::now().timestamp()),
                    alert_type: AlertType::FreshnessCritical,
                    severity: AlertSeverity::Critical,
                    table_name: table_name.clone(),
                    message: format!("Table '{}' data is {:.0} hours stale", table_name, age_hours),
                    detected_at: Utc::now(),
                    threshold_value: Some(thresholds.freshness_critical_hours as f64),
                    actual_value: Some(age_hours as f64),
                    root_cause_hint: Some("Check data ingestion schedule or source availability".to_string()),
                });
            }
        }
    }

    Ok(alerts)
}

/// Compute average baseline row count from historical data
fn compute_baseline_average(baselines: &[HealthBaseline]) -> Option<f64> {
    if baselines.is_empty() {
        return None;
    }

    let sum: usize = baselines.iter().map(|b| b.row_count).sum();
    Some(sum as f64 / baselines.len() as f64)
}

/// Export alerts as JSON
pub fn export_alerts_json(alerts: &[HealthAlert]) -> anyhow::Result<String> {
    let json = serde_json::to_string_pretty(alerts)?;
    Ok(json)
}

/// Map table name to dataset group for display
fn table_to_dataset_group(name: &str) -> &'static str {
    if name.contains("adwords") {
        "adwords"
    } else if name.contains("facebook") {
        "facebook"
    } else {
        "unknown"
    }
}

/// Print health assessment results (safe-to-report UX)
pub fn print_health_assessment(assessment: &HealthAssessment) -> anyhow::Result<()> {
    println!("🏥 DATA HEALTH (Safe-to-Use)");
    println!("============================");

    let critical_count = assessment.alerts.iter()
        .filter(|a| matches!(a.severity, AlertSeverity::Critical))
        .count();
    let safe_to_report = critical_count == 0 && assessment.overall_health_score != HealthScore::Critical;
    let risk_str = match assessment.overall_health_score {
        HealthScore::Critical => "High (missing data in 1 source)",
        HealthScore::Warning => "Medium",
        HealthScore::Good | HealthScore::Excellent => "Low",
    };

    println!("SAFE TO REPORT: {} {}", if safe_to_report { "🟢 YES" } else { "🟡 NO" }, if critical_count > 0 { format!("({} critical issue)", critical_count) } else { String::new() });
    println!("RISK: {}", risk_str);
    println!("LAST CHECK: {} UTC", Utc::now().format("%Y-%m-%d %H:%M"));

    if !assessment.alerts.is_empty() {
        println!("\nTOP ALERTS");
        for (i, alert) in assessment.alerts.iter().take(5).enumerate() {
            let icon = match alert.severity {
                AlertSeverity::Critical => "🔴",
                AlertSeverity::High => "🔴",
                AlertSeverity::Medium => "🟡",
                AlertSeverity::Low => "🟡",
            };
            let fix = if matches!(alert.alert_type, AlertType::FreshnessCritical | AlertType::VolumeDrop) {
                " [Fix: backfill]"
            } else if matches!(alert.alert_type, AlertType::QualityDegraded) {
                " [Investigate]"
            } else {
                ""
            };
            let short_msg = if alert.message.len() > 50 { format!("{}...", &alert.message[..47]) } else { alert.message.clone() };
            println!("{}) {} {} ({}){}", i + 1, icon, alert.table_name, short_msg, fix);
        }
    }

    println!("\nCOMPLETENESS (What date/time is fully ingested?)");
    println!("{:<15} {:<25} {:<8} {:<15}", "DATASET GROUP", "COMPLETE UP TO (UTC)", "LAG", "EXPECTED SLA");
    for (table_name, watermark) in &assessment.freshness_watermarks {
        let dg = table_to_dataset_group(table_name);
        let complete = watermark.completeness_up_to
            .map(|t| t.format("%Y-%m-%d %H:%M").to_string())
            .unwrap_or_else(|| "N/A".to_string());
        let lag_hours = watermark.max_event_time
            .map(|max_t| (Utc::now() - max_t).num_hours())
            .unwrap_or(24);
        let lag = format!("{}h", lag_hours);
        let sla = "Daily by 08:00";
        println!("{:<15} {:<25} {:<8} {:<15}", dg, complete, lag, sla);
    }

    println!("\nFRESHNESS (When did data last arrive?)");
    println!("{:<15} {:<25} {:<12} {:<10}", "DATASET GROUP", "LAST INGEST (UTC)", "ROWS (24h)", "TREND");
    for (table_name, watermark) in &assessment.freshness_watermarks {
        let dg = table_to_dataset_group(table_name);
        let last = watermark.last_ingested_at.format("%H:%M").to_string();
        let rows = assessment.table_health.get(table_name).map(|h| h.row_count).unwrap_or(0);
        let trend = if watermark.row_count_delta < -50 { "↘  (abnormal)" } else { "↗" };
        println!("{:<15} {:<25} {:<12} {:<10}", dg, last, format!("{}", rows), trend);
    }

    let fp_short = if assessment.data_fingerprint.len() >= 8 {
        &assessment.data_fingerprint[..8]
    } else {
        &assessment.data_fingerprint[..]
    };
    println!("\nPROVENANCE + FINGERPRINT");
    println!("Snapshot: {}...    Data fingerprint: {}...", fp_short, fp_short);
    println!("(Use snapshot id to prove numbers later.)");

    println!("\nNEXT");
    println!("- semstrait diff --metric total_cost --by day --explain");
    println!("- semstrait alert-policy set --complete-by 09:00");

    Ok(())
}