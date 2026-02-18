//! Artifact-first reporting: report.md, report.html, slack.txt, JSON into snapshot dirs

use std::path::Path;

/// Paths to shareable artifacts written by a command
#[derive(Debug, Clone)]
pub struct ShareablePaths {
    pub report_md: Option<std::path::PathBuf>,
    pub report_html: Option<std::path::PathBuf>,
    pub slack_txt: Option<std::path::PathBuf>,
    pub json_path: Option<std::path::PathBuf>,
}

/// Ensure snapshot dir exists and write report.md
pub fn write_report_md(snapshot_dir: &Path, content: &str) -> anyhow::Result<std::path::PathBuf> {
    std::fs::create_dir_all(snapshot_dir)?;
    let p = snapshot_dir.join("report.md");
    std::fs::write(&p, content)?;
    Ok(p)
}

/// Write report.html (simple HTML wrapper around markdown content)
pub fn write_report_html(snapshot_dir: &Path, md_content: &str) -> anyhow::Result<std::path::PathBuf> {
    std::fs::create_dir_all(snapshot_dir)?;
    let html = md_to_html(md_content);
    let p = snapshot_dir.join("report.html");
    std::fs::write(&p, html)?;
    Ok(p)
}

/// Write slack.txt snippet
pub fn write_slack_txt(snapshot_dir: &Path, content: &str) -> anyhow::Result<std::path::PathBuf> {
    std::fs::create_dir_all(snapshot_dir)?;
    let p = snapshot_dir.join("slack.txt");
    std::fs::write(&p, content)?;
    Ok(p)
}

/// Write JSON artifact
pub fn write_json<T: serde::Serialize>(snapshot_dir: &Path, filename: &str, value: &T) -> anyhow::Result<std::path::PathBuf> {
    std::fs::create_dir_all(snapshot_dir)?;
    let p = snapshot_dir.join(filename);
    let json = serde_json::to_string_pretty(value)?;
    std::fs::write(&p, json)?;
    Ok(p)
}

/// Minimal markdown-to-HTML (escape + wrap in pre for simple content, or use basic blocks)
fn md_to_html(md: &str) -> String {
    let escaped = html_escape(md);
    format!(
        r#"<!DOCTYPE html>
<html>
<head><meta charset="utf-8"><title>Semstrait Report</title></head>
<body>
<pre style="font-family: monospace; white-space: pre-wrap;">{}</pre>
</body>
</html>"#,
        escaped
    )
}

fn html_escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
}
