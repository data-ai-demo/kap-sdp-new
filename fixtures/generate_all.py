"""Orchestrator for all KAP fixture data generators.

Generates 9 synthetic files across 3 source systems:
  - SharePoint:  3 loss-run Excel + 2 submission Excel
  - RiskMatch:   3 CSV exports (MTD, YTD, TTM)
  - Carriers:    5 files (Acme CSV, Liberty CSV, Summit JSON, Beacon Excel, Patriot CSV)

Usage:
    cd fixtures
    pip install -r ../requirements-fixtures.txt
    python generate_all.py
"""

import generate_sharepoint
import generate_riskmatch
import generate_carriers


def main():
    """Run all fixture generators."""
    print("=" * 60)
    print("KAP Data Platform — Fixture Data Generator")
    print("=" * 60)
    print()

    generate_sharepoint.main()
    print()

    generate_riskmatch.main()
    print()

    generate_carriers.main()
    print()

    print("=" * 60)
    print("✓ All fixture files generated in fixtures/output/")
    print("  Upload to Databricks Volumes with scripts/upload_fixtures.py")
    print("=" * 60)


if __name__ == "__main__":
    main()
