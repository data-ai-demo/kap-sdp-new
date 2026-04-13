# Databricks notebook source
# KAP Data Platform — Core Carriers
# Bronze: Acme Insurance Co (CSV — policy-level extracts)

# COMMAND ----------

from pyspark import pipelines as dp
from pyspark.sql import functions as F

# COMMAND ----------

CATALOG = spark.conf.get("catalog", "kap_demo")
LANDING_PATH = f"/Volumes/{CATALOG}/core_carriers/carrier_file_landing/acme"
SCHEMA_LOC = spark.conf.get("schema_location_base") + "/carriers/acme"

# COMMAND ----------

@dp.table(name="bronze_acme", cluster_by_auto=True)
def bronze_acme():
    """Acme sends monthly CSV policy extracts with 15 columns including
    CarrierPolicyID, AgencyCode, InsuredName, premium info, etc.
    All columns get packed into raw_row_variant.
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
