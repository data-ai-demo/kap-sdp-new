"""Upload generated fixture files to Databricks Unity Catalog Volumes.

Reads fixture output from fixtures/output/ and copies each file to its
corresponding landing volume using the Databricks CLI. Make sure you've
run `source .claude/.env` before running this so the CLI is authenticated.

Usage:
    python3 scripts/upload_fixtures.py
    python3 scripts/upload_fixtures.py --dry-run   # just print, don't upload
"""

import os
import subprocess
import sys

# where the Databricks CLI lives on this machine
CLI = os.path.expanduser("~/bin/databricks")

# mapping from fixture subdirectory → volume path in Unity Catalog
VOLUME_MAP = {
    "sharepoint": "/Volumes/kap_demo/new_business/sharepoint_landing",
    "riskmatch": "/Volumes/kap_demo/risk_match/riskmatch_landing",
    # carrier files go into subfolders matching the carrier name
    "carriers": "/Volumes/kap_demo/core_carriers/carrier_file_landing",
}

FIXTURES_DIR = os.path.join(os.path.dirname(__file__), "..", "fixtures", "output")


def find_files(base_dir):
    """Walk the fixture output directory and yield (local_path, volume_dest) tuples."""
    for root, _, files in os.walk(base_dir):
        for filename in sorted(files):
            local_path = os.path.join(root, filename)
            # figure out which volume this belongs to based on the first subdirectory
            rel = os.path.relpath(local_path, base_dir)
            parts = rel.split(os.sep)
            source_type = parts[0]  # "sharepoint", "riskmatch", or "carriers"

            if source_type not in VOLUME_MAP:
                print(f"⚠ skipping {rel} — no volume mapping for '{source_type}'")
                continue

            volume_base = VOLUME_MAP[source_type]

            # for carriers, keep the subfolder structure (acme/, liberty/, etc.)
            if source_type == "carriers" and len(parts) > 2:
                subfolder = parts[1]
                dest = f"{volume_base}/{subfolder}/{filename}"
            else:
                dest = f"{volume_base}/{filename}"

            yield local_path, dest


def upload_file(local_path, dest, dry_run=False):
    """Upload a single file to a Databricks Volume."""
    label = f"  {os.path.basename(local_path)} → {dest}"
    if dry_run:
        print(f"  [dry-run] {label}")
        return True

    result = subprocess.run(
        [CLI, "fs", "cp", local_path, dest, "--overwrite"],
        capture_output=True,
        text=True,
    )
    if result.returncode == 0:
        print(f"✓ {label}")
        return True
    else:
        print(f"✗ {label}")
        print(f"    {result.stderr.strip()}")
        return False


def main():
    """Upload all fixture files to their respective landing volumes."""
    dry_run = "--dry-run" in sys.argv

    if not os.path.isdir(FIXTURES_DIR):
        print("✗ No fixture output found. Run `cd fixtures && python3 generate_all.py` first.")
        sys.exit(1)

    # quick sanity check that the CLI exists
    if not dry_run and not os.path.isfile(CLI):
        print(f"✗ Databricks CLI not found at {CLI}")
        print("  Install it or update the CLI path in this script.")
        sys.exit(1)

    print("=" * 60)
    print("KAP Data Platform — Fixture Upload")
    if dry_run:
        print("  (dry run — nothing will be uploaded)")
    print("=" * 60)

    files = list(find_files(FIXTURES_DIR))
    if not files:
        print("✗ No files found in fixtures/output/")
        sys.exit(1)

    print(f"\nUploading {len(files)} files...\n")

    success = 0
    for local_path, dest in files:
        if upload_file(local_path, dest, dry_run):
            success += 1

    print(f"\n{'=' * 60}")
    print(f"✓ {success}/{len(files)} files uploaded successfully")
    if success < len(files):
        print(f"⚠ {len(files) - success} files failed — check errors above")
    print("=" * 60)


if __name__ == "__main__":
    main()
