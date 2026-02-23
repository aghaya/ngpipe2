#!/usr/bin/env python3
"""
Iroquois Gas Transmission – Operationally Available Capacity scraper
Fetches Timely-cycle, all-locations data day-by-day from 2025-01-01 to today
and appends into a single CSV file.

Confirmed API (reverse-engineered from browser network inspection):
  GET https://ioly.iroquois.com/infopost/classes/common/RouterClass.php
  Params:
    class  = base64("OperationallyAvailableClass")   [static]
    type   = base64("getGrdCpctyOperAvail")           [static]
    _dc    = unix-ms timestamp (cache buster)
    param  = base64({"searchDateValue":"MM/DD/YYYY 09:00 AM",
                     "cycleDescValue":"Timely",
                     "locationValue":"All"})
    page=1, start=0, limit=500

Response: a JSON array of records (no wrapper), each record contains:
  Loc, Loc Name, Loc/QTI Desc, Loc Purp Desc, Flow Ind Desc,
  Meas Basis Desc, IT Indicator, All Qty Avail,
  Design Capacity (int), Operating Capacity (str w/ commas),
  Total Scheduled Quantity (str w/ commas), OAC (int),
  Posting Date, Posting Time, statusCode, statusText
"""

import base64
import csv
import json
import os
import sys
import time
from datetime import date, timedelta

import requests

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
OUTPUT_FILE = "iroquois_daily_flows_2025_to_present.csv"
START_DATE  = date(2010, 1, 1)

ROUTER_URL  = "https://ioly.iroquois.com/infopost/classes/common/RouterClass.php"
LANDING_URL = "https://ioly.iroquois.com/infopost/"

# Static base64 params confirmed from browser network capture
CLASS_B64   = base64.b64encode(b"OperationallyAvailableClass").decode()
TYPE_B64    = base64.b64encode(b"getGrdCpctyOperAvail").decode()

# Fields to drop from each record (API metadata, not data)
DROP_FIELDS = {"statusCode", "statusText"}

# Desired CSV column order (matches page display order)
CSV_COLUMNS = [
    "gas_date",
    "Posting Date",
    "Posting Time",
    "Loc",
    "Loc Name",
    "Loc/QTI Desc",
    "Loc Purp Desc",
    "Flow Ind Desc",
    "Meas Basis Desc",
    "IT Indicator",
    "All Qty Avail",
    "Design Capacity",
    "Operating Capacity",
    "Total Scheduled Quantity",
    "OAC",
]

DELAY_SECONDS = 0.5   # polite pause between requests
MAX_RETRIES   = 4


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def build_param(query_date: date) -> str:
    """Return a base64-encoded param JSON for the given date."""
    payload = {
        "searchDateValue": f"{query_date.strftime('%m/%d/%Y')} 09:00 AM",
        "cycleDescValue":  "Timely",
        "locationValue":   "All",
    }
    return base64.b64encode(json.dumps(payload, separators=(",", ":")).encode()).decode()


def clean_numeric(value: str) -> str:
    """Strip comma-formatting from numeric strings returned by the API."""
    if isinstance(value, str):
        stripped = value.replace(",", "").strip()
        # Keep as-is if it doesn't look numeric after stripping
        try:
            int(stripped)
            return stripped
        except ValueError:
            try:
                float(stripped)
                return stripped
            except ValueError:
                pass
    return value


def fetch_day(session: requests.Session, query_date: date) -> list[dict]:
    """
    Fetch all OAC records for a single date.
    Returns a list of cleaned record dicts, empty list on failure or no data.
    """
    params = {
        "class": CLASS_B64,
        "type":  TYPE_B64,
        "_dc":   int(time.time() * 1000),
        "param": build_param(query_date),
        "page":  1,
        "start": 0,
        "limit": 500,   # ~50 locations per day; 500 is a safe ceiling
    }

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = session.get(ROUTER_URL, params=params, timeout=30)
            resp.raise_for_status()
            raw = resp.json()

            # Response is a plain JSON array (numeric-keyed object when
            # parsed by some clients; requests gives a list directly)
            if isinstance(raw, list):
                records = raw
            elif isinstance(raw, dict):
                # Numeric-keyed dict → convert to list in order
                records = [raw[k] for k in sorted(raw.keys(), key=lambda x: int(x))
                           if str(x).isdigit()]
            else:
                return []

            # Drop API-metadata fields, clean numeric strings, add gas_date
            gas_date_str = query_date.strftime("%Y-%m-%d")
            cleaned = []
            for rec in records:
                if rec.get("statusCode") not in (None, 1):
                    # Non-success statusCode means no data for this date
                    continue
                out = {"gas_date": gas_date_str}
                for col in CSV_COLUMNS[1:]:   # skip gas_date, already set
                    if col in rec:
                        out[col] = clean_numeric(rec[col])
                    else:
                        out[col] = ""
                cleaned.append(out)

            return cleaned

        except requests.exceptions.RequestException as exc:
            wait = 2 ** attempt
            print(f"    [retry {attempt}/{MAX_RETRIES}] {exc} — waiting {wait}s",
                  file=sys.stderr)
            if attempt == MAX_RETRIES:
                print(f"    [SKIP] {query_date} after {MAX_RETRIES} failed attempts",
                      file=sys.stderr)
                return []
            time.sleep(wait)
        except (ValueError, KeyError) as exc:
            print(f"    [SKIP] {query_date} — parse error: {exc}", file=sys.stderr)
            return []

    return []


def get_existing_dates(output_file: str) -> set[str]:
    """Read the existing CSV and return the set of gas_date values already written."""
    if not os.path.exists(output_file):
        return set()
    dates = set()
    try:
        with open(output_file, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            if reader.fieldnames and "gas_date" in reader.fieldnames:
                for row in reader:
                    if row.get("gas_date"):
                        dates.add(row["gas_date"])
    except Exception as exc:
        print(f"[WARN] Could not read existing CSV: {exc}", file=sys.stderr)
    return dates


def init_session() -> requests.Session:
    """Build a requests.Session with browser-like headers and a landing-page cookie."""
    session = requests.Session()
    session.headers.update({
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/122.0.0.0 Safari/537.36"
        ),
        "Accept":           "application/json, text/javascript, */*; q=0.01",
        "Accept-Language":  "en-US,en;q=0.9",
        "X-Requested-With": "XMLHttpRequest",
        "Referer":          LANDING_URL,
    })
    # Hit the landing page to acquire PHPSESSID (may not be required,
    # but mirrors exactly what the browser does)
    try:
        session.get(LANDING_URL, timeout=15)
    except Exception:
        pass   # non-fatal
    return session


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    end_date = date.today()
    print(f"Iroquois OAC scraper  |  {START_DATE} to {end_date}")
    print(f"Output file: {OUTPUT_FILE}\n")

    # --- Resume support: skip dates already in the CSV ---
    existing = get_existing_dates(OUTPUT_FILE)
    file_exists = bool(existing) or os.path.exists(OUTPUT_FILE)

    if existing:
        latest = max(existing)
        print(f"Resuming  — {len(existing)} dates already in CSV (latest: {latest})\n")

    # Build the list of dates still needed
    todo: list[date] = []
    cur = START_DATE
    while cur <= end_date:
        if cur.strftime("%Y-%m-%d") not in existing:
            todo.append(cur)
        cur += timedelta(days=1)

    if not todo:
        print("Nothing to fetch — CSV is already up to date.")
        return

    total = len(todo)
    print(f"{total} date(s) to fetch ...\n")

    session = init_session()
    rows_written = 0

    with open(OUTPUT_FILE, "a", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=CSV_COLUMNS, extrasaction="ignore")

        # Write header only when creating the file from scratch
        if not file_exists:
            writer.writeheader()

        for idx, query_date in enumerate(todo, 1):
            label = query_date.strftime("%Y-%m-%d")
            records = fetch_day(session, query_date)

            if records:
                writer.writerows(records)
                fh.flush()                   # write-through so partial runs are safe
                rows_written += len(records)
                print(f"  [{idx:4d}/{total}]  {label}  {len(records):3d} records")
            else:
                print(f"  [{idx:4d}/{total}]  {label}  no data")

            time.sleep(DELAY_SECONDS)

    print(f"\nDone.  {rows_written} rows written to: {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
