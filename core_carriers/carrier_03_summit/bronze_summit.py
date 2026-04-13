# Databricks notebook source
# KAP Data Platform — Core Carriers
# Bronze: Summit Underwriters (JSON lines — claims feed)

# COMMAND ----------

# MAGIC %run ../_carrier_template

# COMMAND ----------

from pyspark import pipelines as dp

# COMMAND ----------

@dp.table(name="bronze_summit", cluster_by_auto=True)
def bronze_summit():
    """Summit is the one carrier that sends JSON instead of CSV.

    Their claims feed comes as newline-delimited JSON with fields like
    claim_id, policy_id, paid_amount, reserved_amount, injury_type, etc.
    The JSON loader handles it the same way — sort keys, pack, hash.
    """
    return load_json_to_bronze("summit")
