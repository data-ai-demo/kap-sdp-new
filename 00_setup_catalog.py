# Databricks notebook source
# KAP Data Platform — Catalog Bootstrap
# Step 00: Create catalog, schemas, and landing volumes
#
# Run this notebook ONCE before any pipeline execution.
# Safe to re-run — all CREATE statements use IF NOT EXISTS.

# COMMAND ----------

CATALOG = "kap_demo"

# COMMAND ----------

# ─── CREATE CATALOG ───

spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
spark.sql(f"USE CATALOG {CATALOG}")
print(f"✓ Catalog '{CATALOG}' ready")

# COMMAND ----------

# ─── CREATE SCHEMAS ───

SCHEMAS = ["new_business", "risk_match", "core_carriers"]

for schema in SCHEMAS:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{schema}")
    print(f"✓ Schema '{CATALOG}.{schema}' ready")

# COMMAND ----------

# ─── CREATE LANDING VOLUMES ───

VOLUMES = {
    "new_business": "sharepoint_landing",
    "risk_match": "riskmatch_landing",
    "core_carriers": "carrier_file_landing",
}

for schema, volume in VOLUMES.items():
    spark.sql(
        f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{schema}.{volume} "
        f"COMMENT 'Landing zone for {schema} raw files'"
    )
    print(f"✓ Volume '{CATALOG}.{schema}.{volume}' ready")

# COMMAND ----------

# ─── VERIFY ───

print("\n── Catalog contents ──")
for schema in SCHEMAS:
    tables = spark.sql(f"SHOW TABLES IN {CATALOG}.{schema}").collect()
    volumes = spark.sql(f"SHOW VOLUMES IN {CATALOG}.{schema}").collect()
    print(f"  {schema}: {len(tables)} tables, {len(volumes)} volumes")

print("\n✓ Setup complete — ready for pipeline deployment")
