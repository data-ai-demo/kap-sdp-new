"""Step 3 — Parse.

Normalize file contents into raw-level records. SharePoint files require
format-specific parsing (Excel, CSV). Structured sources are converted
to JSON with minimal transformation.

Excel  → openpyxl UDF → list of JSON-string rows
CSV    → Auto Loader with header=true → to_json(struct(*))
JSON   → Auto Loader with inferColumnTypes=false → to_json(struct(*))
"""
