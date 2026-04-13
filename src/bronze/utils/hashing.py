"""Hashing helpers for Bronze ingestion.

ingestion_id:    F.xxhash64(F.col("raw_row_variant"), F.col("file_path"))
                 Streaming-compatible, deterministic row identifier.

source_row_hash: F.sha2(F.col("raw_row_variant"), 256)
                 JSON keys must be sorted before hashing for determinism.
"""
