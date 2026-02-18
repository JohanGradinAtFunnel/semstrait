//! Lookup table management for semantic enrichment

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;

const LOOKUP_BASE: &str = ".semstrait_demo/lookups";

pub async fn handle_create(
    name: String,
    key: String,
    value: String,
    from: String,
    output_dir: Option<String>,
) -> anyhow::Result<()> {
    let path = if from.starts_with("file:") {
        from.strip_prefix("file:").unwrap_or(&from).to_string()
    } else {
        anyhow::bail!("--from must be file:<path>");
    };

    let mut rdr = csv::Reader::from_path(&path)?;
    let headers = rdr.headers()?.clone();
    let key_idx = headers.iter().position(|h| h == key).ok_or_else(|| anyhow::anyhow!("Key column '{}' not found", key))?;
    let value_idx = headers.iter().position(|h| h == value).ok_or_else(|| anyhow::anyhow!("Value column '{}' not found", value))?;

    let mut map: HashMap<String, String> = HashMap::new();
    let mut dup_count = 0;
    for result in rdr.records() {
        let record = result?;
        let k = record.get(key_idx).unwrap_or("").to_string();
        let v = record.get(value_idx).unwrap_or("").to_string();
        if map.insert(k.clone(), v).is_some() {
            dup_count += 1;
        }
    }

    let base = output_dir
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(LOOKUP_BASE));
    let lookup_dir = base.join(&name);
    std::fs::create_dir_all(&lookup_dir)?;

    let lookup_json = serde_json::to_string_pretty(&map)?;
    std::fs::write(lookup_dir.join("lookup.json"), lookup_json)?;

    let meta = LookupMeta {
        name: name.clone(),
        key_column: key,
        value_column: value,
        key_count: map.len(),
        created_at: chrono::Utc::now().to_rfc3339(),
    };
    std::fs::write(lookup_dir.join("meta.json"), serde_json::to_string_pretty(&meta)?)?;

    println!("🔗 LOOKUP CREATED");
    println!("=====================");
    println!("{}: {} keys (deduped)", name, map.len());
    if dup_count > 0 {
        println!("  ({} duplicates removed)", dup_count);
    }
    println!("Used by: dimension campaign_name");

    Ok(())
}

pub async fn handle_list() -> anyhow::Result<()> {
    let base = PathBuf::from(LOOKUP_BASE);
    if !base.exists() {
        println!("No lookups found. Create one with: lookup create --name <n> --key <k> --value <v> --from file:<path>");
        return Ok(());
    }

    println!("🔗 SAVED LOOKUPS");
    println!("================");
    for entry in std::fs::read_dir(&base)? {
        let entry = entry?;
        if entry.file_type()?.is_dir() {
            let name = entry.file_name().to_string_lossy().to_string();
            let meta_path = entry.path().join("meta.json");
            if meta_path.exists() {
                let meta: LookupMeta = serde_json::from_str(&std::fs::read_to_string(meta_path)?)?;
                println!("  {}: {} keys (created {})", name, meta.key_count, meta.created_at);
            } else {
                println!("  {}: (no meta)", name);
            }
        }
    }
    Ok(())
}

#[derive(Serialize, Deserialize)]
struct LookupMeta {
    name: String,
    key_column: String,
    value_column: String,
    key_count: usize,
    created_at: String,
}
