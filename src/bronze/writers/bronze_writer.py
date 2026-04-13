"""Step 5 — Write.

Serialize the raw row into raw_row_variant and write to the target
Bronze table via @dp.table(name=..., cluster_by_auto=True).

Every Bronze table outputs exactly 5 columns:
  ingestion_id       BIGINT     xxhash64(raw_row_variant, file_path)
  file_path          STRING     Source file path including filename
  extracted_at       TIMESTAMP  When the row was extracted
  source_row_hash    STRING     SHA-256 of raw row (keys sorted)
  raw_row_variant    STRING     Full raw row as JSON string
"""
