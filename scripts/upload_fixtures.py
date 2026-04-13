"""Upload generated fixture files to Databricks Unity Catalog Volumes.

Uses the Databricks REST Files API (PUT /api/2.0/fs/files/) since the CLI
`fs cp` command doesn't reliably support managed UC volumes. Make sure
you've run `source .claude/.env` before running this.

Usage:
    python3 scripts/upload_fixtures.py
    python3 scripts/upload_fixtures.py --dry-run   # just print, don't upload
"""

import configparser
import os
import subprocess
import sys
import urllib.parse

PROFILE = os.environ.get("DATABRICKS_CONFIG_PROFILE", "kap")

# mapping from fixture subdirectory → volume path in Unity Catalog
VOLUME_MAP = {
    "sharepoint": "/Volumes/kap_demo/new_business/sharepoint_landing",
    "riskmatch": "/Volumes/kap_demo/risk_match/riskmatch_landing",
    # carrier files go into subfolders matching the carrier name
    "carriers": "/Volumes/kap_demo/core_carriers/carrier_file_landing",
}

FIXTURES_DIR = os.path.join(os.path.dirname(__file__), "..", "fixtures", "output")


def load_databricks_config():
    """Read host and token from ~/.databrickscfg for the given profile."""
    cfg_path = os.path.expanduser("~/.databrickscfg")
    config = configparser.ConfigParser()
    config.read(cfg_path)

    if PROFILE not in config:
        print(f"✗ Profile '{PROFILE}' not found in {cfg_path}")
        sys.exit(1)

    host = config[PROFILE].get("host", "").rstrip("/")
    token = config[PROFILE].get("token", "")

    if not host or not token:
        print(f"✗ Missing host or token in [{PROFILE}] profile")
        sys.exit(1)

    return host, token


def find_files(base_dir):
    """Walk the fixture output directory and yield (local_path, volume_dest) tuples."""
    for root, _, files in os.walk(base_dir):
        for filename in sorted(files):
            local_path = os.path.join(root, filename)
            rel = os.path.relpath(local_path, base_dir)
            parts = rel.split(os.sep)
            source_type = parts[0]

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


def upload_file(local_path, dest, host, token, dry_run=False):
    """Upload a single file to a Databricks Volume via curl + Files API."""
    label = f"  {os.path.basename(local_path)} → {dest}"
    if dry_run:
        print(f"  [dry-run] {label}")
        return True

    # PUT /api/2.0/fs/files/<url-encoded-path>
    # keep slashes intact — only encode special chars within path segments
    encoded_path = urllib.parse.quote(dest, safe="/")
    url = f"{host}/api/2.0/fs/files/{encoded_path}"

    result = subprocess.run(
        [
            "curl", "-s", "-w", "\n%{http_code}",
            "-X", "PUT", url,
            "-H", f"Authorization: Bearer {token}",
            "-H", "Content-Type: application/octet-stream",
            "--data-binary", f"@{local_path}",
        ],
        capture_output=True,
        text=True,
    )

    lines = result.stdout.strip().rsplit("\n", 1)
    status_code = lines[-1] if lines else "000"
    body = lines[0] if len(lines) > 1 else ""

    if status_code in ("200", "204"):
        print(f"✓ {label}")
        return True
    else:
        print(f"✗ {label}")
        error_msg = body[:200] if body else result.stderr.strip() or f"HTTP {status_code}"
        print(f"    HTTP {status_code}: {error_msg}")
        return False


def main():
    """Upload all fixture files to their respective landing volumes."""
    dry_run = "--dry-run" in sys.argv

    if not os.path.isdir(FIXTURES_DIR):
        print("✗ No fixture output found. Run `cd fixtures && python3 generate_all.py` first.")
        sys.exit(1)

    host, token = load_databricks_config()

    print("=" * 60)
    print("KAP Data Platform — Fixture Upload")
    print(f"  Profile: {PROFILE}")
    print(f"  Host:    {host}")
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
        if upload_file(local_path, dest, host, token, dry_run):
            success += 1

    print(f"\n{'=' * 60}")
    print(f"✓ {success}/{len(files)} files uploaded successfully")
    if success < len(files):
        print(f"⚠ {len(files) - success} files failed — check errors above")
    print("=" * 60)


if __name__ == "__main__":
    main()
