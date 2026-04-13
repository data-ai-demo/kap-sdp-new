# Databricks notebook source
# KAP Data Platform — RiskMatch Domain
# Bronze: CSV ingestion from RiskMatch API exports (MTD / YTD / TTM)

# COMMAND ----------

from pyspark import pipelines as dp
from pyspark.sql import functions as F

# COMMAND ----------

CATALOG = spark.conf.get("catalog", "kap_demo")
SCHEMA = "risk_match"
LANDING_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/riskmatch_landing"
SCHEMA_LOC = spark.conf.get("schema_location_base") + "/riskmatch"

# COMMAND ----------

@dp.table(name="bronze_riskmatch", cluster_by_auto=True)
def bronze_riskmatch():
    """Stream RiskMatch CSV drops into the 5-column Bronze schema.

    All three reporting periods (MTD, YTD, TTM) land in the same volume
    and get absorbed into a single Bronze table. The period is captured
    inside raw_row_variant along with everything else.
    """
    raw = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("cloudFiles.inferColumnTypes", "false")
        .option("cloudFiles.schemaLocation", SCHEMA_LOC)
        .load(LANDING_PATH)
    )

    # sort columns alphabetically before packing into JSON — keeps the
    # SHA-256 hash stable regardless of column ordering in the source file
    sorted_cols = sorted(raw.columns)
    raw_json = F.to_json(F.struct(*[F.col(c) for c in sorted_cols]))

    bronze = (
        raw
        .withColumn("raw_row_variant", raw_json)
        .withColumn("ingestion_id", F.xxhash64(F.col("raw_row_variant"), F.col("file_path")))
        .withColumn("file_path", F.col("_metadata.file_path"))
        .withColumn("extracted_at", F.current_timestamp())
        .withColumn("source_row_hash", F.sha2(F.col("raw_row_variant"), 256))
        .select("ingestion_id", "file_path", "extracted_at", "source_row_hash", "raw_row_variant")
    )

    return bronze
