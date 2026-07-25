# Databricks notebook source
# MAGIC %md
# MAGIC # Daily Data Quality Summary
# MAGIC Runs after the Gold stage to summarize row counts and the quarantine rate
# MAGIC for the day's run, and raise a visible failure if quarantine volume spikes.

# COMMAND ----------

dbutils.widgets.text("env", "dev")
dbutils.widgets.text("run_date", "")

env = dbutils.widgets.get("env")
run_date = dbutils.widgets.get("run_date")

import sys
sys.path.append("/Workspace/Repos/retail-sales-lakehouse-etl")  # adjust to your repo checkout path

from src.config import load_config

cfg = load_config(env)

# COMMAND ----------

silver_count = spark.read.table(cfg.full_table_name(cfg.silver.table)) \
    .filter(f"_run_date = '{run_date}'").count()

quarantine_count = spark.read.table(cfg.full_table_name(cfg.silver.quarantine_table)) \
    .filter(f"_run_date = '{run_date}'").count()

gold_count = spark.read.table(cfg.full_table_name(cfg.gold.daily_sales_table)) \
    .filter(f"_run_date = '{run_date}'").count()

total = silver_count + quarantine_count
quarantine_rate = round(quarantine_count / total, 4) if total > 0 else 0.0

print(f"run_date={run_date}")
print(f"silver_rows={silver_count}")
print(f"quarantined_rows={quarantine_count}")
print(f"quarantine_rate={quarantine_rate:.2%}")
print(f"gold_rows={gold_count}")

# COMMAND ----------

# Fail visibly (red task in the Workflow UI) if quarantine rate spikes above 10%,
# so an on-call engineer investigates instead of the bad-data trend continuing silently.
assert quarantine_rate <= 0.10, f"Quarantine rate {quarantine_rate:.2%} exceeds 10% threshold for {run_date}"
