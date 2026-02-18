# Semstrait Trust Layer

**The semantic layer that eliminates data fear in analytics.**

Traditional semantic layers tell you **how** numbers are calculated. Semstrait's trust layer tells you **why** they changed, **whether** changes are safe, and **whether** numbers match reality.

This demo showcases the complete trust substrate: five engines that transform semantic layers from "calculation compilers" into "trust platforms", plus an **incident orchestrator** that ties them together.

## 🎯 The Problem: Data Fear in Analytics

Marketing teams live in fear of their data:
- **Numbers change unexpectedly** - "Why did spend drop 30% this month?"
- **Changes break reports** - "Will this metric update crash the dashboard?"
- **Data quality is invisible** - "Is this number even real?"

Semantic layers promised trust, but delivered only lineage. Teams still can't answer the fundamental questions that matter.

## 🚀 The Solution: Trust Engines

Semstrait implements **five substrate engines** plus an **incident orchestrator** that solve data fear:

### 0. 🧠 Incident Orchestrator (`incident`)
**"What happened and what should I do?"**

Orchestrates health → diff → proof-pack to produce a verdict, confidence, and recommended actions:
- **Verdict-first**: Data gap vs mapping mismatch vs real performance change
- **Confidence score**: Heuristic 0–1 based on signal consistency
- **Recommended actions**: Backfill windows, stakeholder notifications, alert policies
- **Shareable artifacts**: `report.md`, `report.html`, `slack.txt`, JSON in snapshot dir

### 1. 📋 Prove Engine (`proof-pack`)
**"What evidence backs this number?"**

Generates reproducible proof packs with complete audit trails:
- **Value + meaning**: Executed metric value and human-readable definition
- **Definition (Human/Exact)**: SQL-ish CASE for cross-datasetGroup metrics
- **Lineage table**: datasetGroup → measure mappings
- **Shareable artifacts**: `report.md`, `report.html`, `sql.sql`, `proof_pack.json`

### 2. 🔍 Diagnose Engine (`diff`)
**"Why did this number change?"**

Compares semantic results against platform baselines with grain-aware drilldown:
- **Verdict + first divergence**: Where and why numbers diverge
- **Provenance breakdown**: By datasetGroup at divergence point
- **Explain/driver analysis**: Missing ingestion vs mapping mismatch
- **Shareable artifacts**: `report.md`, `report.html`, `slack.txt`, `diff_result.json`

### 3. ⚖️ Reconcile Engine (`reconcile`)
**"Do the numbers add up?"**

Validates semantic vs baseline with protocol output and knobs:
- **Verdict**: Equal vs close-but-not-equal
- **Ranked likely reasons**: Timezone, attribution, platform completeness
- **Evidence + next steps**: Try `--timezone`, `--as-of` to align
- **Shareable artifacts**: `report.md`, `report.html`, `slack.txt`, `reconcile.json`

### 4. 🎯 Validate Engine (`impact`)
**"Is this change safe to deploy?"**

Dual-executes current vs proposed models to predict deployment impact:
- **Verdict**: MAJOR/MINOR/BLOCK with risk checks
- **What changed**: Metric mappings by datasetGroup
- **Risk checks**: CASE coverage, non-additive metrics, null rate changes
- **Shareable artifacts**: `report.md`, `report.html`, `risk_summary.json`

### 5. 🏥 Monitor Engine (`health`)
**"Is the data even trustworthy?"**

Continuous health assessment with verdict-first output:
- **Safe to report**: Yes/no + critical issue count
- **Top alerts**: Ranked by severity
- **Completeness + freshness**: Watermarks, lag, expected SLA
- **Provenance**: Snapshot ID + data fingerprint

## 📊 The Transformation

| Traditional Semantic Layer | Semstrait Trust Layer |
|---------------------------|------------------------|
| "How is this calculated?" | "Here's the complete proof pack" |
| "Why did it change?" | "Root cause: Missing Facebook data" |
| "Do these numbers add up?" | "✅ Semantic matches baseline exactly" |
| "Good luck with changes" | "Safe to deploy? Here's the impact" |
| "Trust us, data is fresh" | "Data health score: 94%" |
| Fear-driven analytics | Confidence-driven decisions |

**Result**: Marketing teams stop being afraid of their data. They start trusting their numbers enough to make bold decisions.

## 🏗️ Architecture: Metadata & Contracts

The demo implements two key architectural patterns:

### Metadata/Inventory System

The `metadata.rs` module provides runtime dataset inventory computation:

- **DatasetInventory**: Row counts, schema hashes, watermarks, completeness scores
- **InventorySnapshot**: Complete state across all datasets with overall fingerprint
- **InventoryBuilder**: DataFusion-based computation avoiding duplicate work

Used by `snapshot_store.rs` (persists `inventory.json`) and `health.rs` (consumes inventory for quality assessment).

### Semantic Contracts

The demo showcases **semantic correctness as a contract** with strict enforcement:

- **Identity Scoping**: Campaign/ad IDs scoped by account (`scopedBy: ["accounts.id"]`)
- **Join Relationships**: Explicit cardinality prevents double counting (`relationship: many_to_one`)
- **Dataset Grain**: Declared uniqueness constraints validate aggregations (`grain: [...]`)
- **Aggregation Rules**: Blocks unsafe cross-datasetGroup rollups (`count_distinct`)

#### Contract Violation Testing

The demo includes `contract_violation_test.yaml` - a model that demonstrates contract enforcement:

**Test Contract Violations:**
```bash
# Run the full test suite including contract violation tests
bash tools/semstrait_demo/test_all_scenarios.sh

# Test specific contract violations
cargo run --manifest-path tools/semstrait_demo/Cargo.toml --quiet run --scenario union --model tools/semstrait_demo/contract_violation_test.yaml
# This should show contract violations if you modify the query to trigger them
```

**Contract Violation Example**: The `total_unique_users` metric uses `count_distinct` across datasetGroups, which would be blocked in strict mode as an unsafe holistic aggregation.

```yaml
# Contract configuration (model.yaml)
contract:
  mode: strict  # Blocks violations instead of warning

# Identity scoping example
dimensions:
  - name: campaigns
    attributes:
      - name: id
        scopedBy: ["accounts.id"]  # Campaign IDs only unique within accounts
```

## 🚀 Quick Start: Trust Workflow

Install Rust, then from the repository root:

```bash
# 🧪 Run all demo scenarios (comprehensive test suite)
bash tools/semstrait_demo/test_all_scenarios.sh

# 1. Incident triage (orchestrates health + diff + proof-pack) ⭐ PRIMARY
cargo run --manifest-path tools/semstrait_demo/Cargo.toml incident spend_drop --metric total_cost --since 2026-02-15

# 2. See current data health
cargo run --manifest-path tools/semstrait_demo/Cargo.toml health

# 3. Generate proof pack for any metric
cargo run --manifest-path tools/semstrait_demo/Cargo.toml proof-pack total_cost

# 4. Reconcile distinct counts
cargo run --manifest-path tools/semstrait_demo/Cargo.toml reconcile total_unique_users

# 5. Diagnose discrepancies
cargo run --manifest-path tools/semstrait_demo/Cargo.toml diff --metrics total_cost,total_impressions --explain

# 6. Drill down to contributing rows
cargo run --manifest-path tools/semstrait_demo/Cargo.toml drilldown adwords

# 7. Validate model changes
cargo run --manifest-path tools/semstrait_demo/Cargo.toml impact --preview
cargo run --manifest-path tools/semstrait_demo/Cargo.toml impact --proposed-model tools/semstrait_demo/proposed_changes.yaml

# 8. Export data dictionary
cargo run --manifest-path tools/semstrait_demo/Cargo.toml dictionary export --format csv
cargo run --manifest-path tools/semstrait_demo/Cargo.toml dictionary export --format json --output data_dictionary.json

# 9. Manage lookups
cargo run --manifest-path tools/semstrait_demo/Cargo.toml lookup create --name campaign_map --key campaign_id --value campaign_name --from file:tools/semstrait_demo/campaigns.csv
cargo run --manifest-path tools/semstrait_demo/Cargo.toml lookup list

# 10. Test contract violations (demonstrates enforcement)
bash tools/semstrait_demo/test_all_scenarios.sh  # Includes contract violation tests
cargo run --manifest-path tools/semstrait_demo/Cargo.toml --quiet run --scenario union --model tools/semstrait_demo/contract_violation_test.yaml  # Test with violation model
```

## 📋 Trust Engine Reference

### Common Options (shared across commands)
```bash
--as-of <TIMESTAMP>       Point-in-time analysis (ISO 8601, e.g. 2026-02-15T23:59:00Z)
--timezone <TZ>           Analysis timezone (default: UTC)
--currency <CURR>          Target currency for monetary values
--fx-rate <RATE>           Foreign exchange rate
--attribution-window <N>   Attribution window in days
--scope <SCOPE>            Scope filter (e.g. subscription:ACME)
--window <WINDOW>         Analysis window (e.g. 24h, 7d)
```

### `incident` - Trust Incident Orchestration
Orchestrates health → diff → proof-pack and produces verdict, confidence, and recommended actions.

```bash
cargo run --manifest-path tools/semstrait_demo/Cargo.toml incident <NAME> --metric <METRIC> [--since <YYYY-MM-DD>]

Example:
  incident spend_drop --metric total_cost --since 2026-02-15

Output:
  - Verdict (data gap / mapping mismatch / real change)
  - Confidence score (0–1)
  - Impact %
  - Recommended actions
  - Shareable artifacts in .semstrait_demo/snapshots/<id>/
```

### `run` - Reproducible Semantic Execution
Generates deterministic results with snapshot IDs for audit trails.

```bash
cargo run --manifest-path tools/semstrait_demo/Cargo.toml run [OPTIONS]

Options:
  --no-exec              Show plan without executing
  --json                 Output Substrait plan as JSON
  --scenario <NAME>       Fixture scenario (default: union)
```

### `diff` - Discrepancy Diagnosis
Semantic vs platform comparison with verdict, first divergence, and provenance breakdown.

```bash
cargo run --manifest-path tools/semstrait_demo/Cargo.toml diff [OPTIONS]

Options:
  --metrics <LIST>       Comma-separated metrics (e.g. total_cost,total_impressions)
  --baseline <TYPE>      raw, platform:facebook, platform:adwords (default: raw)
  --grain <GRAIN>        day, account, campaign, ad (default: day)
  --explain              Include driver/root-cause analysis
  --scenario <NAME>      Fixture scenario (e.g. messy_alignment)
```

### `impact` - Change Impact Validation
Predicts deployment impact before making changes live.

```bash
cargo run --manifest-path tools/semstrait_demo/Cargo.toml impact [OPTIONS]

Options:
  --preview                    Simulate changes (no proposed model needed)
  --proposed-model <PATH>      Path to proposed model YAML
  --sample <WINDOW>            Sample window (e.g. last_30_days)
  --metrics <LIST>             Comma-separated metrics
  --scenario <NAME>            Fixture scenario (e.g. messy_alignment)
```

### `health` - Data Health Monitoring
Verdict-first assessment: safe-to-report, top alerts, completeness, freshness.

```bash
cargo run --manifest-path tools/semstrait_demo/Cargo.toml health [OPTIONS]

Options:
  --alert-output <PATH>  Write alerts JSON to file
  --scenario <NAME>      Fixture scenario (e.g. messy_alignment)
```

### `proof-pack` - Evidence Generation
Creates reproducible proof packs with value, meaning, definitions, lineage, and exportables.

```bash
cargo run --manifest-path tools/semstrait_demo/Cargo.toml proof-pack <METRIC_NAME> [OPTIONS]

Output:
  - VALUE: executed metric result
  - WHAT THIS METRIC MEANS
  - DEFINITION (Human) and DEFINITION (Exact)
  - LINEAGE table (datasetGroup → measure)
  - EXPORTABLES: report.md, report.html, sql.sql in snapshot dir
```

### `reconcile` - Distinct Count Validation
Protocol output with baseline/timezone knobs and ranked likely reasons.

```bash
cargo run --manifest-path tools/semstrait_demo/Cargo.toml reconcile <METRIC_NAME> [OPTIONS]

Options:
  --baseline <TYPE>      raw, platform:facebook, platform:adwords (default: raw)
  --attribution <SPEC>    Attribution window (e.g. 1d_click,1d_view)
  --scenario <NAME>      Fixture scenario (e.g. messy_alignment)

Output:
  - VERDICT: Equal / Close but not equal
  - SEMANTIC vs BASELINE with delta %
  - MOST LIKELY REASONS (ranked)
  - EVIDENCE + NEXT steps
  - Artifacts: report.md, report.html, slack.txt, reconcile.json
```

### `drilldown` - Row-Level Analysis
Shows exact contributing rows for any table group.

```bash
cargo run --manifest-path tools/semstrait_demo/Cargo.toml drilldown <TABLE_GROUP> [OPTIONS]
```

### `dictionary export` - Data Dictionary
Exports dimensions, measures, and metrics from the semantic model.

```bash
cargo run --manifest-path tools/semstrait_demo/Cargo.toml dictionary export [OPTIONS]

Options:
  --format <FMT>         csv or json (default: csv)
  --output <PATH>        Output file (default: ./data_dictionary.csv or .json)
  --model <PATH>         Model YAML path (default: embedded model)
  --scope <SCOPE>        Scope filter
```

### `lookup create` / `lookup list` - Lookup Management
Create lookups from CSV (deduped by key) or list saved lookups.

```bash
# Create from CSV (sample file included)
cargo run --manifest-path tools/semstrait_demo/Cargo.toml lookup create \
  --name campaign_map --key campaign_id --value campaign_name --from file:tools/semstrait_demo/campaigns.csv

# List saved lookups
cargo run --manifest-path tools/semstrait_demo/Cargo.toml lookup list
```

Lookups are stored under `.semstrait_demo/lookups/<name>/` with `lookup.json` and `meta.json`.

## 🎬 Trust in Action: Incident Triage

**Scenario**: Spend dropped. Is it real performance or a data gap?

```bash
$ cargo run --manifest-path tools/semstrait_demo/Cargo.toml incident spend_drop --metric total_cost --since 2026-02-15
```

```
🧠 SEMSTRAIT TRUST INCIDENT: "Spend dropped" (total_cost)
=========================================================
VERDICT: 🟡 LIKELY DATA GAP (not real performance)
CONFIDENCE: HIGH (0.91)
IMPACT: 0.0% vs expected
NEXT: Re-ingest facebook_campaigns for 2026-02-15 00:00–23:59 UTC

1) SAFETY CHECK (Freshness + Completeness)
------------------------------------------
adwords         🟢 OK  ...
facebook        🟢 OK  ... 17h missing

2) WHERE IT BREAKS (Provenance)
-------------------------------

3) ROOT CAUSE HYPOTHESIS
------------------------
🔴 Missing ingestion window detected
- facebook_campaigns rows: expected 1,200–1,600/day, got 320
- watermark stalled at: 2026-02-15 06:00 UTC

4) RECOMMENDED ACTIONS
----------------------
A) Re-ingest facebook_campaigns [2026-02-15 06:00 → 23:59]
B) Notify stakeholders: "dashboard not safe until backfill completes"
C) Optional: enable alert policy "must be complete by 09:00 UTC daily"

5) SHAREABLE ARTIFACTS
----------------------
✅ Proof Pack:   .semstrait_demo/snapshots/<id>/report.html
✅ Audit JSON:   .semstrait_demo/snapshots/<id>/proof_pack.json
✅ Slack Paste:  .semstrait_demo/snapshots/<id>/slack.txt
```

**Result**: Incident triaged. Verdict, confidence, and actions in one run. Shareable artifacts for collaboration.

## 🎯 Trust in Action: Attribution & Tax Mismatch

**Scenario**: Numbers arrive daily but don't match platform reality. Facebook totals are consistently ~10% off, and daily cut-offs are inconsistent due to timezone drift.

```bash
# Health check shows data arrived but with partial-day lag
$ cargo run --manifest-path tools/semstrait_demo/Cargo.toml health --scenario messy_alignment

# Diff detects mapping mismatch with tax overhead guidance
$ cargo run --manifest-path tools/semstrait_demo/Cargo.toml diff --metrics total_cost --explain --scenario messy_alignment

# Reconcile shows timezone drift evidence
$ cargo run --manifest-path tools/semstrait_demo/Cargo.toml reconcile total_cost --scenario messy_alignment --timezone America/Los_Angeles

# Incident orchestrates all three engines for comprehensive analysis
$ cargo run --manifest-path tools/semstrait_demo/Cargo.toml incident messy_alignment --metric total_cost --since 2026-02-15

# Validate proposed fix via impact analysis
$ cargo run --manifest-path tools/semstrait_demo/Cargo.toml impact --proposed-model tools/semstrait_demo/messy_alignment_fix.yaml --scenario messy_alignment --metrics total_cost
```

**What each command highlights**:

- **`health`**: Safe-to-report (data fresh) but flags partial-day lag (last 4h incomplete)
- **`diff`**: Facebook first divergence with "~10% lower" explanation and unmapped tax guidance
- **`reconcile`**: Timezone mismatch ranked #1, with boundary drift evidence and tax overhead notes
- **`incident`**: 🟡 MAPPING MISMATCH DETECTED verdict (not data gap), recommends semantic model update
- **`impact`**: Proposed fix shows +10.0% change, confirming alignment with platform totals

**Result**: Multi-factor root cause identified. Semantic model fix proposed and validated. Complex alignment issues resolved through systematic analysis.

## 🔄 The Trust Loop

1. **Incident** → Triage spend drops, metric changes, data gaps
2. **Monitor** health daily → Catch pipeline issues early
3. **Prove** numbers on-demand → Generate evidence and audit trails
4. **Reconcile** counts regularly → Validate data quality and deduplication
5. **Diagnose** discrepancies immediately → Quick root cause analysis
6. **Validate** changes before deployment → Prevent outages
7. **Repeat** → Build institutional trust in data

## 🏗️ Artifact Layout

All commands emit shareable artifacts under `.semstrait_demo/`:

| Path | Contents |
|------|----------|
| `.semstrait_demo/snapshots/<id>/` | Report, HTML, Slack snippet, JSON per command |
| `.semstrait_demo/lookups/<name>/` | `lookup.json`, `meta.json` for created lookups |

## 🔧 Implementation Status

### ✅ Fully Working
- **Incident Orchestrator**: Health + diff + proof-pack → verdict, confidence, actions, artifacts
- **Health Engine**: Safe-to-report, top alerts, completeness, freshness, provenance
- **Proof Pack Engine**: Value, meaning, human/exact definitions, lineage, exportables
- **Diff Engine**: Verdict, first divergence, provenance breakdown, explain, artifacts
- **Impact Engine**: Verdict, what changed, risk checks, exportables
- **Reconcile Engine**: Protocol output, baseline/timezone knobs, ranked reasons, artifacts
- **Dictionary Export**: Dimensions, measures, metrics to CSV/JSON
- **Lookup Management**: Create from CSV (dedupe), list saved lookups
- **DataFusion Integration**: Real execution on Parquet with snapshot persistence

### ⚠️ Known Limitations
- Virtual dimension projection (`_dataset.datasetGroup`) has emitter schema validation issues in some edge cases
- Impact Engine: Full proposed-model comparison requires YAML file

## 🎯 The Trust Revolution

Traditional semantic layers solved the "how calculated" problem. Semstrait's trust layer solves the "why trust" problem.

**Before**: Teams fear their data and make conservative decisions.

**After**: Teams trust their data completely and make bold, data-driven decisions.
