# Databricks notebook source
# KAP Data Platform — Core Carriers
# Bronze: Patriot Indemnity (CSV — endorsement log)

# COMMAND ----------

# MAGIC %run ../_carrier_template

# COMMAND ----------

from pyspark import pipelines as dp

# COMMAND ----------

@dp.table(name=f"{CATALOG}.{SCHEMA}.bronze_patriot", cluster_by_auto=True)
def bronze_patriot():
    """Patriot's endorsement log: adds, removes, address changes, class
    code reclassifications. Nine columns including premium change and
    new total premium.
    """
    return load_csv_to_bronze("patriot")
