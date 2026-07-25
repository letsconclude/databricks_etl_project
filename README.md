# Retail Sales Lakehouse ETL (Databricks + Delta Lake)

Production-grade batch ETL pipeline on Databricks implementing the **Medallion Architecture**
(Bronze → Silver → Gold) using PySpark and Delta Lake, deployed via **Databricks Asset Bundles (DAB)**.

## Why this exists

This is a reference implementation showing the patterns a real production pipeline needs:
schema enforcement, data quality gates, idempotent writes, incremental processing,
observability, testing, and CI/CD — not just a notebook that runs once.

## Architecture

```
                 ┌────────────────────────────────────────────────────┐
                 │                  Orchestration                     │
                 │        Databricks Workflows (jobs/*.yml)            │
                 └───────────────┬──────────────┬──────────────┬──────┘
                                  │              │              │
                       ┌──────────▼───┐  ┌───────▼──────┐ ┌─────▼──────┐
     Source (S3/ADLS)  │    BRONZE    │  │    SILVER    │ │    GOLD    │
     raw JSON/CSV  ───▶│ raw ingest,  │─▶│ clean, dedup,│▶│ business   │
                        │ schema      │  │ validate,    │ │ aggregates │
                        │ enforcement,│  │ SCD Type 2   │ │ (star      │
                        │ quarantine  │  │              │ │ schema)    │
                        └──────────────┘  └──────────────┘ └────────────┘
                              Delta Lake tables at every layer
```

- **Bronze**: raw data landed as-is (+ ingestion metadata), schema-on-read with a rescued
  data column so malformed records never crash the job.
- **Silver**: cleaned, deduplicated, conformed types, row-level data quality checks, bad
  records routed to a quarantine table instead of failing the pipeline.
- **Gold**: business-level aggregates (daily sales by store/product) ready for BI tools.

## Project layout

```
databricks_etl_project/
├── databricks.yml              # Databricks Asset Bundle (DAB) root config
├── jobs/
│   └── etl_pipeline_job.yml    # Workflow definition: task DAG, schedule, retries, alerts
├── conf/
│   ├── dev.yml                 # Environment-specific config (paths, catalog, schema)
│   └── prod.yml
├── src/
│   ├── config.py                # Typed config loader (env-aware)
│   ├── utils/
│   │   ├── logger.py             # Structured JSON logging
│   │   ├── spark_utils.py        # SparkSession + Delta helpers
│   │   └── data_quality.py       # Reusable DQ check framework
│   ├── bronze/ingest.py         # Raw -> Bronze
│   ├── silver/transform.py      # Bronze -> Silver
│   └── gold/aggregate.py        # Silver -> Gold
├── tests/
│   ├── conftest.py               # Local SparkSession fixture
│   ├── test_transform.py         # Silver transform unit tests
│   └── test_data_quality.py      # DQ framework unit tests
├── .github/workflows/ci-cd.yml  # Lint, test, deploy bundle to Databricks
├── requirements.txt
├── pyproject.toml
└── .gitignore
```

## Key production patterns implemented

| Concern | How it's handled |
|---|---|
| **Idempotency** | All writes use Delta `MERGE`, so re-running a job for the same date never duplicates data. |
| **Incremental processing** | Bronze ingestion uses Auto Loader-style `cloudFiles` (falls back to path-glob for local/dev) so only new files are processed. |
| **Schema evolution** | `mergeSchema` + rescued-data column (`_rescued_data`) capture unexpected fields without failing the job. |
| **Data quality** | A small declarative DQ framework (`data_quality.py`) tags each row as pass/fail; failing rows go to a quarantine Delta table, not `/dev/null`. |
| **Observability** | Structured JSON logs (easy to route to a log analytics workspace), row counts logged at every stage, DQ pass-rate metric. |
| **Configuration management** | No hardcoded paths/catalogs — a single `config.py` reads environment-specific YAML (`conf/dev.yml`, `conf/prod.yml`), swapped via a Databricks job parameter. |
| **Testing** | Unit tests run against a local (non-cluster) SparkSession using small in-memory DataFrames — fast, no cluster needed for CI. |
| **CI/CD** | GitHub Actions: lint (ruff) → unit tests (pytest) → validate DAB → deploy to `dev` on PR merge, deploy to `prod` on tag. |
| **Orchestration** | Databricks Workflows job DAG (bronze → silver → gold) with retries, timeout, and email/Slack alert on failure, defined as code in `jobs/etl_pipeline_job.yml`. |

## Setup

```bash
# 1. Install the Databricks CLI (v0.2xx, bundles support)
curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sh

# 2. Authenticate
databricks auth login --host https://<your-workspace>.cloud.databricks.com

# 3. Install deps locally (for running tests / linting)
pip install -r requirements.txt

# 4. Run unit tests (no cluster required)
pytest tests/ -v

# 5. Validate the bundle
databricks bundle validate -t dev

# 6. Deploy to dev workspace
databricks bundle deploy -t dev

# 7. Run the pipeline job
databricks bundle run etl_pipeline -t dev
```

## Running a single layer locally for debugging

```bash
python -m src.bronze.ingest --env dev --run-date 2026-07-24
python -m src.silver.transform --env dev --run-date 2026-07-24
python -m src.gold.aggregate --env dev --run-date 2026-07-24
```

## Extending this

- Swap the medallion tables from `dbfs:/` paths to a Unity Catalog 3-level namespace
  (`catalog.schema.table`) by editing `conf/*.yml` — the code already reads table names
  from config, not hardcoded strings.
- Add a `.github/workflows/ci-cd.yml` environment gate for manual prod approval.
- Add Delta Live Tables (DLT) as an alternative to the hand-rolled orchestration in
  `jobs/etl_pipeline_job.yml` if you want managed pipelines with built-in expectations.
