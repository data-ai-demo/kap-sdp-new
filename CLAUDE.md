# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What This Project Is

**KAP Data Platform** — a Databricks-based data ingestion and analytics platform for Keystone Agency Partners (KAP), an insurance agency management company. KAP aggregates data from ~80 insurance carriers, each reporting in proprietary formats. The platform follows a Medallion architecture (Bronze → SSOT → Silver → Gold) using Lakeflow Spark Declarative Pipelines (SDP).

This repo contains the **Bronze layer** — raw ingestion from 3 source systems into Delta tables with a fixed 5-column schema. No business logic or normalization at this layer.

## Source Systems

1. **SharePoint** — Excel/CSV files from folders (new business domain: loss runs, submissions)
2. **RiskMatch API** — Structured CSV exports across 3 reporting periods: MTD, YTD, TTM
3. **Core Carrier Data** — Files from ~80 carriers, each with different schemas. CSV, JSON, or Excel depending on carrier.
4. **SQL Server** — bypasses Bronze entirely, enters at SSOT/Silver (do not build anything for it)

## Unity Catalog Namespace

- Catalog: `kap_demo`
- Schemas: `new_business`, `risk_match`, `core_carriers`
- Volumes: `kap_demo.new_business.sharepoint_landing`, `kap_demo.risk_match.riskmatch_landing`, `kap_demo.core_carriers.carrier_file_landing`

## Bronze Table Schema (Sacred — No Deviation)

Every Bronze table uses exactly these 5 columns:

| Column | Type | Purpose |
|---|---|---|
| `ingestion_id` | `BIGINT` | Unique identifier for the ingestion run |
| `file_path` | `STRING` | Source file path including filename |
| `extracted_at` | `TIMESTAMP` | When the row was extracted |
| `source_row_hash` | `STRING` | SHA-256 of raw row (keys sorted before hashing) |
| `raw_row_variant` | `STRING` | Full raw row as JSON string — all schema differences absorbed here |

## SDP Syntax (Mandatory)

All pipeline code uses Lakeflow Spark Declarative Pipelines. Never use legacy DLT.

```python
from pyspark import pipelines as dp   # ALWAYS this — never `import dlt`
```

| Use | Decorator |
|---|---|
| Bronze streaming tables | `@dp.table(name=..., cluster_by_auto=True)` |
| Batch transforms (Silver/Gold) | `@dp.materialized_view()` |
| Intermediate views | `@dp.temporary_view()` |

Decorated functions must **only return a DataFrame** — no `collect()`, `count()`, `save()`, `saveAsTable()`, or any write operations inside them.

## Auto Loader Patterns

- **CSV**: `spark.readStream.format("cloudFiles").option("cloudFiles.format", "csv").option("header", "true").option("cloudFiles.inferColumnTypes", "false")`
- **JSON**: same pattern with `cloudFiles.format` = `"json"`
- **Excel**: `cloudFiles.format` = `"binaryFile"`, parse via UDF using openpyxl, then `explode`

Schema location must be specified via pipeline config: `spark.conf.get("schema_location_base")`. Never use the source data volume for schema storage.

All CSV/JSON columns collapse to `raw_row_variant` via `F.to_json(F.struct("*"))`.

## Fixture Data Generation

```bash
cd fixtures
pip install -r ../requirements-fixtures.txt   # openpyxl, faker
python generate_all.py                         # generates 9 files in fixtures/output/
```

Output files are then uploaded to corresponding Databricks Volumes. The `fixtures/constants.py` module holds shared agency codes (40), carrier names (12+), and business names (80+) used across all generators for cross-source consistency.

## Credentials

All credentials live in `.claude/.env`. Source it before running Databricks CLI commands:
```bash
source .claude/.env
```

Never hardcode tokens or secrets in source files.

## Key Conventions

- `from pyspark import pipelines as dp` — never `import dlt`
- `cluster_by_auto=True` on all Bronze tables — never `partition_cols`
- `cloudFiles.inferColumnTypes` = `"false"` at Bronze — type casting belongs in Silver
- SHA-256 hashing must sort JSON keys before hashing for determinism
- Connectors return path strings, not DataFrames — Auto Loader needs paths
- Each carrier pipeline under `core_carriers/` follows the same repeatable pattern — adding carrier #6 through #80 should only require a new subfolder and pipeline module
- Pipeline config parameters accessed via `spark.conf.get()` — never hardcode catalog, schema, or paths
- Every Python module must have a docstring
- No `# TODO` comments
