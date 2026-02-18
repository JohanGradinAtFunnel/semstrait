// Parquet generation enabled
use arrow::array::{ArrayRef, Float64Array, Int32Array, Int64Array, StringArray, TimestampMicrosecondArray};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use chrono::{DateTime, Utc};
use std::fs::File;
use std::sync::Arc;

/// Generate demo Parquet fixtures for two tableGroups with grain columns and realistic data.
/// - scenario: e.g. "spend_drop" for incident demo (Facebook late, lower volume)
/// - as_of: ISO timestamp for date window (e.g. "2026-02-15T23:59:00Z")
pub fn generate_fixtures(
    temp_dir: &tempfile::TempDir,
    scenario: Option<&str>,
    as_of: Option<&str>,
) -> anyhow::Result<(String, String)> {
    let adwords_path = temp_dir.path().join("adwords_campaigns.parquet");
    let facebook_path = temp_dir.path().join("facebook_campaigns.parquet");

    println!("  📊 Generating Parquet fixtures...");

    let (adwords_rows, facebook_rows) = match scenario {
        Some("spend_drop") => {
            generate_spend_drop_scenario(&adwords_path, &facebook_path, as_of)?
        }
        Some("messy_alignment") => {
            generate_messy_alignment_scenario(&adwords_path, &facebook_path, as_of)?
        }
        _ => {
            generate_adwords_data(&adwords_path)?;
            let ar = 3;
            generate_facebook_data(&facebook_path)?;
            let fr = 2;
            println!("    ✅ Generated AdWords data: {} rows", ar);
            println!("    ✅ Generated Facebook data: {} rows", fr);
            (ar, fr)
        }
    };

    if scenario == Some("spend_drop") {
        println!("    ✅ Generated AdWords data: {} rows (spend_drop scenario)", adwords_rows);
        println!("    ✅ Generated Facebook data: {} rows (spend_drop scenario)", facebook_rows);
    }

    if scenario == Some("messy_alignment") {
        println!("    ✅ Generated AdWords data: {} rows (messy_alignment scenario)", adwords_rows);
        println!("    ✅ Generated Facebook data: {} rows (messy_alignment scenario)", facebook_rows);
    }

    Ok((
        adwords_path.to_string_lossy().to_string(),
        facebook_path.to_string_lossy().to_string(),
    ))
}

/// Spend-drop scenario: AdWords complete through ~23:00, Facebook only through ~06:00 with lower volume
fn generate_spend_drop_scenario(
    adwords_path: &std::path::Path,
    facebook_path: &std::path::Path,
    as_of: Option<&str>,
) -> anyhow::Result<(usize, usize)> {
    let base_date = as_of
        .and_then(|s| s.split('T').next())
        .unwrap_or("2026-02-15");
    let base_ts = format!("{}T00:00:00Z", base_date);
    let base_time = DateTime::parse_from_rfc3339(&base_ts)?.timestamp_micros();

    // AdWords: complete through 23:00, ~1400 rows
    let ar = generate_adwords_spend_drop(adwords_path, base_time, 1423)?;

    // Facebook: complete only through 06:00, ~320 rows (stalled watermark)
    let fr = generate_facebook_spend_drop(facebook_path, base_time, 320)?;

    Ok((ar, fr))
}

/// Messy alignment scenario: 10% overhead tax, timezone drift, incomplete last 4h
fn generate_messy_alignment_scenario(
    adwords_path: &std::path::Path,
    facebook_path: &std::path::Path,
    as_of: Option<&str>,
) -> anyhow::Result<(usize, usize)> {
    let base_date = as_of
        .and_then(|s| s.split('T').next())
        .unwrap_or("2026-02-15");
    let base_ts = format!("{}T00:00:00Z", base_date);
    let base_time = DateTime::parse_from_rfc3339(&base_ts)?.timestamp_micros();

    // AdWords: complete through 23:00, ~2000 rows
    let ar = generate_adwords_messy_alignment(adwords_path, base_time, 2000)?;

    // Facebook: complete only through 20:00 (last 4h missing), ~1800 rows with tax_amount
    // Create timezone drift by concentrating rows near midnight UTC
    let fr = generate_facebook_messy_alignment(facebook_path, base_time, 1800)?;

    Ok((ar, fr))
}

fn generate_adwords_spend_drop(path: &std::path::Path, base_time: i64, row_count: usize) -> anyhow::Result<usize> {
    use arrow::array::Array;
    let schema = Arc::new(Schema::new(vec![
        Field::new("row_id", DataType::Int64, false),
        Field::new("event_time_utc", DataType::Timestamp(TimeUnit::Microsecond, None), false),
        Field::new("day", DataType::Utf8, false),
        Field::new("account_id", DataType::Int64, false),
        Field::new("campaign_id", DataType::Int64, false),
        Field::new("ad_id", DataType::Int64, false),
        Field::new("user_id", DataType::Int64, false),
        Field::new("cost", DataType::Float64, true),
        Field::new("impressions", DataType::Int64, false),
    ]));

    let mut row_ids = Vec::with_capacity(row_count);
    let mut event_times = Vec::with_capacity(row_count);
    let mut days = Vec::with_capacity(row_count);
    let mut account_ids = Vec::with_capacity(row_count);
    let mut campaign_ids = Vec::with_capacity(row_count);
    let mut ad_ids = Vec::with_capacity(row_count);
    let mut user_ids = Vec::with_capacity(row_count);
    let mut costs = Vec::with_capacity(row_count);
    let mut impressions = Vec::with_capacity(row_count);

    let day_str = chrono::DateTime::from_timestamp_micros(base_time)
        .map(|dt| dt.format("%Y-%m-%d").to_string())
        .unwrap_or_else(|| "2026-02-15".to_string());

    for i in 0..row_count {
        row_ids.push((i + 1) as i64);
        let hour = (i % 24) as i64;
        let minute = (i % 60) as i64;
        event_times.push(base_time + hour * 3_600_000_000 + minute * 60_000_000);
        days.push(day_str.clone());
        account_ids.push(1001 + (i % 3) as i64);
        campaign_ids.push(2001 + (i % 10) as i64);
        ad_ids.push(3001 + i as i64);
        user_ids.push(1001 + (i % 500) as i64);
        costs.push(Some(50.0 + (i % 100) as f64 * 0.5));
        impressions.push(1000 + (i % 5000) as i64);
    }

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(row_ids)),
            Arc::new(TimestampMicrosecondArray::from(event_times)),
            Arc::new(StringArray::from(days)),
            Arc::new(Int64Array::from(account_ids)),
            Arc::new(Int64Array::from(campaign_ids)),
            Arc::new(Int64Array::from(ad_ids)),
            Arc::new(Int64Array::from(user_ids)),
            Arc::new(Float64Array::from(costs)),
            Arc::new(Int64Array::from(impressions)),
        ],
    )?;

    let file = File::create(path)?;
    let mut writer = ArrowWriter::try_new(file, batch.schema(), None)?;
    writer.write(&batch)?;
    writer.close()?;
    Ok(row_count)
}

fn generate_facebook_spend_drop(path: &std::path::Path, base_time: i64, row_count: usize) -> anyhow::Result<usize> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("row_id", DataType::Int64, false),
        Field::new("event_time_utc", DataType::Timestamp(TimeUnit::Microsecond, None), false),
        Field::new("day", DataType::Utf8, false),
        Field::new("account_id", DataType::Int64, false),
        Field::new("campaign_id", DataType::Int64, false),
        Field::new("ad_id", DataType::Int64, false),
        Field::new("user_id", DataType::Int64, false),
        Field::new("spend", DataType::Float64, true),
        Field::new("impressions", DataType::Int64, false),
    ]));

    let mut row_ids = Vec::with_capacity(row_count);
    let mut event_times = Vec::with_capacity(row_count);
    let mut days = Vec::with_capacity(row_count);
    let mut account_ids = Vec::with_capacity(row_count);
    let mut campaign_ids = Vec::with_capacity(row_count);
    let mut ad_ids = Vec::with_capacity(row_count);
    let mut user_ids = Vec::with_capacity(row_count);
    let mut spends = Vec::with_capacity(row_count);
    let mut impressions = Vec::with_capacity(row_count);

    let day_str = chrono::DateTime::from_timestamp_micros(base_time)
        .map(|dt| dt.format("%Y-%m-%d").to_string())
        .unwrap_or_else(|| "2026-02-15".to_string());

    for i in 0..row_count {
        row_ids.push((i + 1000) as i64);
        let hour = (i % 7) as i64;
        event_times.push(base_time + hour * 3_600_000_000);
        days.push(day_str.clone());
        account_ids.push(1002);
        campaign_ids.push(2004 + (i % 5) as i64);
        ad_ids.push(3004 + i as i64);
        user_ids.push(1003 + (i % 200) as i64);
        spends.push(Some(30.0 + (i % 80) as f64));
        impressions.push(500 + (i % 2000) as i64);
    }

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(row_ids)),
            Arc::new(TimestampMicrosecondArray::from(event_times)),
            Arc::new(StringArray::from(days)),
            Arc::new(Int64Array::from(account_ids)),
            Arc::new(Int64Array::from(campaign_ids)),
            Arc::new(Int64Array::from(ad_ids)),
            Arc::new(Int64Array::from(user_ids)),
            Arc::new(Float64Array::from(spends)),
            Arc::new(Int64Array::from(impressions)),
        ],
    )?;

    let file = File::create(path)?;
    let mut writer = ArrowWriter::try_new(file, batch.schema(), None)?;
    writer.write(&batch)?;
    writer.close()?;
    Ok(row_count)
}

fn generate_adwords_messy_alignment(path: &std::path::Path, base_time: i64, row_count: usize) -> anyhow::Result<usize> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("row_id", DataType::Int64, false),
        Field::new("event_time_utc", DataType::Timestamp(TimeUnit::Microsecond, None), false),
        Field::new("day", DataType::Utf8, false),
        Field::new("account_id", DataType::Int64, false),
        Field::new("campaign_id", DataType::Int64, false),
        Field::new("ad_id", DataType::Int64, false),
        Field::new("user_id", DataType::Int64, false),
        Field::new("cost", DataType::Float64, true),
        Field::new("impressions", DataType::Int64, false),
    ]));

    let mut row_ids = Vec::with_capacity(row_count);
    let mut event_times = Vec::with_capacity(row_count);
    let mut days = Vec::with_capacity(row_count);
    let mut account_ids = Vec::with_capacity(row_count);
    let mut campaign_ids = Vec::with_capacity(row_count);
    let mut ad_ids = Vec::with_capacity(row_count);
    let mut user_ids = Vec::with_capacity(row_count);
    let mut costs = Vec::with_capacity(row_count);
    let mut impressions = Vec::with_capacity(row_count);

    let day_str = chrono::DateTime::from_timestamp_micros(base_time)
        .map(|dt| dt.format("%Y-%m-%d").to_string())
        .unwrap_or_else(|| "2026-02-15".to_string());

    for i in 0..row_count {
        row_ids.push((i + 1) as i64);
        let hour = (i % 24) as i64;
        let minute = (i % 60) as i64;
        event_times.push(base_time + hour * 3_600_000_000 + minute * 60_000_000);
        days.push(day_str.clone());
        account_ids.push(1001 + (i % 3) as i64);
        campaign_ids.push(2001 + (i % 10) as i64);
        ad_ids.push(3001 + i as i64);
        user_ids.push(1001 + (i % 500) as i64);
        costs.push(Some(50.0 + (i % 100) as f64 * 0.5));
        impressions.push(1000 + (i % 5000) as i64);
    }

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(row_ids)),
            Arc::new(TimestampMicrosecondArray::from(event_times)),
            Arc::new(StringArray::from(days)),
            Arc::new(Int64Array::from(account_ids)),
            Arc::new(Int64Array::from(campaign_ids)),
            Arc::new(Int64Array::from(ad_ids)),
            Arc::new(Int64Array::from(user_ids)),
            Arc::new(Float64Array::from(costs)),
            Arc::new(Int64Array::from(impressions)),
        ],
    )?;

    let file = File::create(path)?;
    let mut writer = ArrowWriter::try_new(file, batch.schema(), None)?;
    writer.write(&batch)?;
    writer.close()?;
    Ok(row_count)
}

fn generate_facebook_messy_alignment(path: &std::path::Path, base_time: i64, row_count: usize) -> anyhow::Result<usize> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("row_id", DataType::Int64, false),
        Field::new("event_time_utc", DataType::Timestamp(TimeUnit::Microsecond, None), false),
        Field::new("day", DataType::Utf8, false),
        Field::new("account_id", DataType::Int64, false),
        Field::new("campaign_id", DataType::Int64, false),
        Field::new("ad_id", DataType::Int64, false),
        Field::new("user_id", DataType::Int64, false),
        Field::new("spend", DataType::Float64, true),
        Field::new("tax_amount", DataType::Float64, true),  // NEW: tax_amount column
        Field::new("impressions", DataType::Int64, false),
    ]));

    let mut row_ids = Vec::with_capacity(row_count);
    let mut event_times = Vec::with_capacity(row_count);
    let mut days = Vec::with_capacity(row_count);
    let mut account_ids = Vec::with_capacity(row_count);
    let mut campaign_ids = Vec::with_capacity(row_count);
    let mut ad_ids = Vec::with_capacity(row_count);
    let mut user_ids = Vec::with_capacity(row_count);
    let mut spends = Vec::with_capacity(row_count);
    let mut tax_amounts = Vec::with_capacity(row_count);
    let mut impressions = Vec::with_capacity(row_count);

    let day_str = chrono::DateTime::from_timestamp_micros(base_time)
        .map(|dt| dt.format("%Y-%m-%d").to_string())
        .unwrap_or_else(|| "2026-02-15".to_string());

    for i in 0..row_count {
        row_ids.push((i + 1000) as i64);

        // TIMEZONE DRIFT: Concentrate rows near midnight UTC (00:00-08:00)
        // This creates a boundary effect when compared to PT (UTC-8)
        let hour = if i < row_count / 2 {
            // First half: concentrated in 00:00-08:00 UTC (16:00-24:00 PT previous day)
            (i % 9) as i64  // 0-8 hours
        } else {
            // Second half: spread across the day but cap at 20:00 (last 4h incomplete)
            9 + (i % 12) as i64  // 9-20 hours
        };
        let minute = (i % 60) as i64;
        event_times.push(base_time + hour * 3_600_000_000 + minute * 60_000_000);

        days.push(day_str.clone());
        account_ids.push(1002);
        campaign_ids.push(2004 + (i % 5) as i64);
        ad_ids.push(3004 + i as i64);
        user_ids.push(1003 + (i % 200) as i64);

        // SPEND + 10% TAX OVERHEAD
        let spend = 30.0 + (i % 80) as f64;
        spends.push(Some(spend));

        // TAX_AMOUNT = 10% of spend
        let tax_amount = Some(spend * 0.10);  // Exactly 10% overhead
        tax_amounts.push(tax_amount);

        impressions.push(500 + (i % 2000) as i64);
    }

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(row_ids)),
            Arc::new(TimestampMicrosecondArray::from(event_times)),
            Arc::new(StringArray::from(days)),
            Arc::new(Int64Array::from(account_ids)),
            Arc::new(Int64Array::from(campaign_ids)),
            Arc::new(Int64Array::from(ad_ids)),
            Arc::new(Int64Array::from(user_ids)),
            Arc::new(Float64Array::from(spends)),
            Arc::new(Float64Array::from(tax_amounts)),
            Arc::new(Int64Array::from(impressions)),
        ],
    )?;

    let file = File::create(path)?;
    let mut writer = ArrowWriter::try_new(file, batch.schema(), None)?;
    writer.write(&batch)?;
    writer.close()?;
    Ok(row_count)
}

/// Generate AdWords campaign data with grain columns
fn generate_adwords_data(path: &std::path::Path) -> anyhow::Result<()> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("row_id", DataType::Int64, false),
        Field::new("event_time_utc", DataType::Timestamp(TimeUnit::Microsecond, None), false),
        Field::new("day", DataType::Utf8, false),
        Field::new("account_id", DataType::Int64, false),
        Field::new("campaign_id", DataType::Int64, false),
        Field::new("ad_id", DataType::Int64, false),
        Field::new("user_id", DataType::Int64, false),  // For distinct count testing
        Field::new("cost", DataType::Float64, true),  // Nullable for testing
        Field::new("impressions", DataType::Int64, false),
    ]));

    // Realistic AdWords campaign data
    let base_time = DateTime::parse_from_rfc3339("2024-01-01T00:00:00Z")?.timestamp_micros();

    let row_ids = Int64Array::from(vec![1, 2, 3]);
    let event_times = TimestampMicrosecondArray::from(vec![
        base_time,
        base_time + 6_000_000,     // +6 seconds
        base_time + 12_000_000,    // +12 seconds
    ]);
    let days = StringArray::from(vec!["2024-01-01", "2024-01-01", "2024-01-01"]);
    let account_ids = Int64Array::from(vec![1001, 1001, 1001]);
    let campaign_ids = Int64Array::from(vec![2001, 2002, 2003]);
    let ad_ids = Int64Array::from(vec![3001, 3002, 3003]);
    let user_ids = Int64Array::from(vec![1001, 1002, 1001]); // Intentional duplicate for distinct count testing
    let costs = Float64Array::from(vec![Some(150.25), Some(275.50), Some(200.75)]); // Note: intentionally no NULLs for cost
    // Total cost will be 626.50
    let impressions = Int64Array::from(vec![15000, 25000, 20000]);

    let batch = RecordBatch::try_new(schema, vec![
        Arc::new(row_ids),
        Arc::new(event_times),
        Arc::new(days),
        Arc::new(account_ids),
        Arc::new(campaign_ids),
        Arc::new(ad_ids),
        Arc::new(user_ids),
        Arc::new(costs),
        Arc::new(impressions),
    ])?;

    let file = File::create(path)?;
    let mut writer = ArrowWriter::try_new(file, batch.schema(), None)?;
    writer.write(&batch)?;
    writer.close()?;

    Ok(())
}

/// Generate Facebook campaign data with grain columns
fn generate_facebook_data(path: &std::path::Path) -> anyhow::Result<()> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("row_id", DataType::Int64, false),
        Field::new("event_time_utc", DataType::Timestamp(TimeUnit::Microsecond, None), false),
        Field::new("day", DataType::Utf8, false),
        Field::new("account_id", DataType::Int64, false),
        Field::new("campaign_id", DataType::Int64, false),
        Field::new("ad_id", DataType::Int64, false),
        Field::new("user_id", DataType::Int64, false),  // For distinct count testing
        Field::new("spend", DataType::Float64, true),  // Nullable for testing
        Field::new("tax_amount", DataType::Float64, true),  // Tax amount column
        Field::new("impressions", DataType::Int64, false),
    ]));

    // Realistic Facebook campaign data
    let base_time = DateTime::parse_from_rfc3339("2024-01-01T00:03:00Z")?.timestamp_micros();

    let row_ids = Int64Array::from(vec![4, 5]);
    let event_times = TimestampMicrosecondArray::from(vec![
        base_time,
        base_time + 6_000_000,     // +6 seconds
    ]);
    let days = StringArray::from(vec!["2024-01-01", "2024-01-01"]);
    let account_ids = Int64Array::from(vec![1002, 1002]);
    let campaign_ids = Int64Array::from(vec![2004, 2005]);
    let ad_ids = Int64Array::from(vec![3004, 3005]);
    let user_ids = Int64Array::from(vec![1003, 1001]); // One duplicate with AdWords for cross-platform distinct testing
    let spends = Float64Array::from(vec![Some(125.00), Some(135.50)]); // Note: intentionally different from semantic expectation
    let tax_amounts = Float64Array::from(vec![Some(12.50), Some(13.55)]); // 10% tax
    let impressions = Int64Array::from(vec![30000, 15000]);

    let batch = RecordBatch::try_new(schema, vec![
        Arc::new(row_ids),
        Arc::new(event_times),
        Arc::new(days),
        Arc::new(account_ids),
        Arc::new(campaign_ids),
        Arc::new(ad_ids),
        Arc::new(user_ids),
        Arc::new(spends),
        Arc::new(tax_amounts),
        Arc::new(impressions),
    ])?;

    let file = File::create(path)?;
    let mut writer = ArrowWriter::try_new(file, batch.schema(), None)?;
    writer.write(&batch)?;
    writer.close()?;

    Ok(())
}