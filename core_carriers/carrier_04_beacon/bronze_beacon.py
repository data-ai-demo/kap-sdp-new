# Databricks notebook source
# KAP Data Platform — Core Carriers
# Bronze: Beacon Mutual (Excel — workers comp premium bordereau)

# COMMAND ----------

# MAGIC %run ../_carrier_template

# COMMAND ----------

from pyspark import pipelines as dp

# COMMAND ----------

@dp.table(name=f"{CATALOG}.{SCHEMA}.bronze_beacon", cluster_by_auto=True)
def bronze_beacon():
    """Beacon sends a monthly .xlsx bordereau with WC class codes,
    payroll, experience mod, schedule credits, and net premium.

    Excel files go through the binaryFile Auto Loader path — openpyxl
    cracks the workbook open and we explode each row into its own record.
    """
    return load_excel_to_bronze("beacon")
