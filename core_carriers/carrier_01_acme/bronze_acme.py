# Databricks notebook source
# KAP Data Platform — Core Carriers
# Bronze: Acme Insurance Co (CSV — policy-level extracts)

# COMMAND ----------

# MAGIC %run ../_carrier_template

# COMMAND ----------

from pyspark import pipelines as dp

# COMMAND ----------

@dp.table(name="bronze_acme", cluster_by_auto=True)
def bronze_acme():
    """Acme sends monthly CSV policy extracts with 15 columns.

    Columns include CarrierPolicyID, AgencyCode, InsuredName, premium info, etc.
    All of it gets packed into raw_row_variant — we don't care about their
    column names until Silver.
    """
    return load_csv_to_bronze("acme")
