#!/usr/bin/env python3
"""
One-time bulk loader: reads the local CSV and upserts all rows
into the Supabase `iroquois_oac` table.

Usage:
    1. Fill in .streamlit/secrets.toml with your Supabase URL and key.
    2. Run:  python load_csv_to_supabase.py

Credentials are read from .streamlit/secrets.toml (or SUPABASE_URL /
SUPABASE_KEY env vars as a fallback).

The script is idempotent — re-running it will upsert (not duplicate) rows
because Supabase is configured with a unique constraint on (gas_date, loc,
loc_purp_desc, flow_ind_desc).
"""

import os
import re
import sys

import pandas as pd
from supabase import create_client

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
CSV_FILE   = "iroquois_daily_flows_2025_to_present.csv"
TABLE_NAME = "iroquois_oac"
BATCH_SIZE = 500

# Map CSV column headers → Supabase (snake_case) column names
COL_MAP = {
    "gas_date":                  "gas_date",
    "Posting Date":              "posting_date",
    "Posting Time":              "posting_time",
    "Loc":                       "loc",
    "Loc Name":                  "loc_name",
    "Loc/QTI Desc":              "loc_qti_desc",
    "Loc Purp Desc":             "loc_purp_desc",
    "Flow Ind Desc":             "flow_ind_desc",
    "Meas Basis Desc":           "meas_basis_desc",
    "IT Indicator":              "it_indicator",
    "All Qty Avail":             "all_qty_avail",
    "Design Capacity":           "design_capacity",
    "Operating Capacity":        "operating_capacity",
    "Total Scheduled Quantity":  "total_scheduled_quantity",
    "OAC":                       "oac",
}

NUMERIC_COLS = {"design_capacity", "operating_capacity",
                "total_scheduled_quantity", "oac", "loc"}


def read_secrets_toml() -> dict:
    """Parse .streamlit/secrets.toml and return key/value pairs."""
    path = os.path.join(".streamlit", "secrets.toml")
    if not os.path.exists(path):
        return {}
    secrets = {}
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line.startswith("#") or "=" not in line:
                continue
            k, _, v = line.partition("=")
            secrets[k.strip()] = re.sub(r'^["\']|["\']$', "", v.strip())
    return secrets


def get_client():
    secrets = read_secrets_toml()
    url = os.environ.get("SUPABASE_URL") or secrets.get("SUPABASE_URL")
    key = os.environ.get("SUPABASE_KEY") or secrets.get("SUPABASE_KEY")
    if not url or not key:
        print(
            "ERROR: Supabase credentials not found.\n"
            "  Edit .streamlit/secrets.toml and set SUPABASE_URL and SUPABASE_KEY.",
            file=sys.stderr,
        )
        sys.exit(1)
    return create_client(url, key)


def coerce_numeric(val):
    """Convert numeric-looking strings to int/float; leave others as-is."""
    if val == "" or val is None:
        return None
    if isinstance(val, str):
        stripped = val.replace(",", "").strip()
        try:
            return int(stripped)
        except ValueError:
            try:
                return float(stripped)
            except ValueError:
                pass
    return val


def load_csv(path: str) -> list[dict]:
    print(f"Reading {path} ...")
    df = pd.read_csv(path, dtype=str, keep_default_na=False)
    print(f"  {len(df):,} rows, {len(df.columns)} columns")

    # Rename to snake_case
    df = df.rename(columns=COL_MAP)

    # Keep only columns we care about
    df = df[[c for c in COL_MAP.values() if c in df.columns]]

    # Coerce numeric fields
    for col in NUMERIC_COLS:
        if col in df.columns:
            df[col] = df[col].apply(coerce_numeric)

    # Replace empty strings with None so Supabase stores NULL
    df = df.where(df != "", other=None)

    return df.to_dict(orient="records")


def upsert_batches(client, rows: list[dict]) -> None:
    total   = len(rows)
    written = 0
    errors  = 0

    for i in range(0, total, BATCH_SIZE):
        batch = rows[i : i + BATCH_SIZE]
        try:
            client.table(TABLE_NAME).upsert(
                batch,
                on_conflict="gas_date,loc,loc_purp_desc,flow_ind_desc",
            ).execute()
            written += len(batch)
            pct = written / total * 100
            print(f"  Upserted {written:,}/{total:,} rows ({pct:.1f}%)", end="\r")
        except Exception as exc:
            errors += len(batch)
            print(f"\n  [ERROR] batch {i//BATCH_SIZE + 1}: {exc}", file=sys.stderr)

    print(f"\nDone.  {written:,} rows upserted, {errors} errors.")


def main():
    if not os.path.exists(CSV_FILE):
        print(f"ERROR: CSV file not found: {CSV_FILE}", file=sys.stderr)
        sys.exit(1)

    client = get_client()
    rows   = load_csv(CSV_FILE)
    print(f"Upserting {len(rows):,} rows into `{TABLE_NAME}` in batches of {BATCH_SIZE} ...")
    upsert_batches(client, rows)


if __name__ == "__main__":
    main()
