# Databricks notebook source
# KAP Data Platform — Core Carriers
# Bronze: Patriot Indemnity (CSV — endorsement log)

# COMMAND ----------

from pyspark import pipelines as dp
from pyspark.sql import functions as F

# COMMAND ----------

CATALOG = spark.conf.get("catalog", "kap_demo")
LANDING_PATH = f"/Volumes/{CATALOG}/core_carriers/carrier_file_landing/patriot"
SCHEMA_LOC = spark.conf.get("schema_location_base") + "/carriers/patriot"

# COMMAND ----------

@dp.table(name="bronze_patriot", cluster_by_auto=True)
def bronze_patriot():
    """Patriot's endorsement log: adds, removes, address changes, class
    code reclassifications. Nine columns including premium change and
    new total premium.
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

    sorted_cols = sorted(raw.columns)
    raw_json = F.to_json(F.struct(*[F.col(c) for c in sorted_cols]))

    return (
        raw
        .withColumn("raw_row_variant", raw_json)
        .withColumn("file_path", F.col("_metadata.file_path"))
        .withColumn("ingestion_id", F.xxhash64(F.col("raw_row_variant"), F.col("file_path")))
        .withColumn("extracted_at", F.current_timestamp())
        .withColumn("source_row_hash", F.sha2(F.col("raw_row_variant"), 256))
        .select("ingestion_id", "file_path", "extracted_at", "source_row_hash", "raw_row_variant")
    )
