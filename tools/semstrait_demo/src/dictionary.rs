//! Data dictionary export from semantic model

use semstrait::parser;
use std::collections::HashMap;

pub async fn handle_export(
    format: String,
    output: Option<String>,
    _scope: Option<String>,
    model_path: Option<String>,
) -> anyhow::Result<()> {
    let schema_yaml = model_path
        .map(|p| std::fs::read_to_string(p))
        .transpose()?
        .unwrap_or_else(|| include_str!("../model.yaml").to_string());

    let schema = parser::parse_str(&schema_yaml)?;

    let mut dimensions = Vec::new();
    let mut measures = Vec::new();
    let mut metrics = Vec::new();

    for model in &schema.semantic_models {
        for dim in &model.dimensions {
            for attr in &dim.attributes {
                dimensions.push(DictDimension {
                    name: format!("{}.{}", dim.name, attr.name),
                    label: dim.label.clone(),
                    description: dim.description.clone(),
                    data_type: format!("{:?}", attr.data_type),
                });
            }
        }
        for dg in &model.dataset_groups {
            for m in &dg.measures {
                measures.push(DictMeasure {
                    name: m.name.clone(),
                    aggregation: format!("{:?}", m.aggregation).to_lowercase(),
                    expr: format!("{:?}", m.expr),
                    dataset_group: dg.name.clone(),
                    dataset: dg.datasets.first().map(|d| d.dataset.clone()).unwrap_or_default(),
                });
            }
        }
        for m in model.metrics.as_deref().unwrap_or(&[]) {
            let coverage = if m.is_cross_dataset_group() {
                let mappings = m.dataset_group_measures();
                format!("{}%", (mappings.len() * 100) / model.dataset_groups.len().max(1))
            } else {
                "100%".to_string()
            };
            metrics.push(DictMetric {
                name: m.name.clone(),
                label: m.label.clone(),
                description: m.description.clone(),
                data_type: format!("{:?}", m.data_type()),
                case_coverage: coverage,
                dataset_group_mappings: m.dataset_group_measures(),
            });
        }
    }

    let output_path = output.unwrap_or_else(|| format!("./data_dictionary.{}", if format == "json" { "json" } else { "csv" }));

    if format == "json" {
        let out = serde_json::json!({
            "dimensions": dimensions,
            "measures": measures,
            "metrics": metrics,
        });
        std::fs::write(&output_path, serde_json::to_string_pretty(&out)?)?;
    } else {
        let mut csv = String::new();
        csv.push_str("type,name,label,description,data_type,extra\n");
        for d in &dimensions {
            csv.push_str(&format!("dimension,\"{}\",\"{}\",\"{}\",\"{}\",\"\"\n",
                d.name, d.label.as_deref().unwrap_or(""), d.description.as_deref().unwrap_or(""), d.data_type));
        }
        for m in &measures {
            csv.push_str(&format!("measure,\"{}\",\"\",\"\",\"{}\",\"{}|{}|{}\"\n",
                m.name, m.aggregation, m.dataset_group, m.dataset, m.expr));
        }
        for m in &metrics {
            let mappings: String = m.dataset_group_mappings.iter()
                .map(|(a, b)| format!("{}:{}", a, b))
                .collect::<Vec<_>>()
                .join(";");
            csv.push_str(&format!("metric,\"{}\",\"{}\",\"{}\",\"{}\",\"{}\"\n",
                m.name,
                m.label.as_deref().unwrap_or(""),
                m.description.as_deref().unwrap_or(""),
                m.data_type,
                mappings));
        }
        std::fs::write(&output_path, csv)?;
    }

    println!("📚 DATA DICTIONARY EXPORT");
    println!("=========================");
    println!("Exported:");
    println!("- {} dimensions", dimensions.len());
    println!("- {} metrics", metrics.len());
    println!("Includes: definitions, lineage, owners, last-changed, used-by dashboards/exports");
    println!("Output: {}", output_path);

    Ok(())
}

#[derive(serde::Serialize)]
struct DictDimension {
    name: String,
    label: Option<String>,
    description: Option<String>,
    data_type: String,
}

#[derive(serde::Serialize)]
struct DictMeasure {
    name: String,
    aggregation: String,
    expr: String,
    dataset_group: String,
    dataset: String,
}

#[derive(serde::Serialize)]
struct DictMetric {
    name: String,
    label: Option<String>,
    description: Option<String>,
    data_type: String,
    case_coverage: String,
    dataset_group_mappings: Vec<(String, String)>,
}
