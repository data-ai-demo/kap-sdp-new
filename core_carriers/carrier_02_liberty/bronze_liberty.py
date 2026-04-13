# Databricks notebook source
# KAP Data Platform — Core Carriers
# Bronze: Liberty National Group (CSV — commission statements)

# COMMAND ----------

# MAGIC %run ../_carrier_template

# COMMAND ----------

from pyspark import pipelines as dp

# COMMAND ----------

@dp.table(name=f"{CATALOG}.{SCHEMA}.bronze_liberty", cluster_by_auto=True)
def bronze_liberty():
    """Liberty sends commission statement CSVs — 11 columns including
    agency ID, premium, commission rate/amount, and pay period.
    """
    return load_csv_to_bronze("liberty")
