#!/usr/bin/env python3
"""
Daily incremental updater — run by GitHub Actions every morning.

Fetches yesterday's and today's OAC data from the Iroquois EBB and
upserts the records into Supabase.  Only fetches dates that are either
missing from the DB or were posted in the last 2 days (to catch revisions).

Environment variables required:
    SUPABASE_URL
    SUPABASE_KEY
"""

import base64
import json
import os
import sys
import time
from datetime import date, timedelta

import requests
from supabase import create_client

# ---------------------------------------------------------------------------
# Constants (mirrors fetch_iroquois_oac.py)
# ---------------------------------------------------------------------------
ROUTER_URL = "https://ioly.iroquois.com/infopost/classes/common/RouterClass.php"
LANDING_URL = "https://ioly.iroquois.com/infopost/"
CLASS_B64   = base64.b64encode(b"OperationallyAvailableClass").decode()
TYPE_B64    = base64.b64encode(b"getGrdCpctyOperAvail").decode()

TABLE_NAME    = "iroquois_oac"
DELAY_SECONDS = 0.5
MAX_RETRIES   = 4

# How many days back to re-fetch (catches late postings / revisions)
LOOKBACK_DAYS = 3

COL_MAP = {
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


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def get_client():
    url = os.environ.get("SUPABASE_URL")
    key = os.environ.get("SUPABASE_KEY")
    if not url or not key:
        print("ERROR: SUPABASE_URL and SUPABASE_KEY must be set.", file=sys.stderr)
        sys.exit(1)
    return create_client(url, key)


def build_param(query_date: date) -> str:
    payload = {
        "searchDateValue": f"{query_date.strftime('%m/%d/%Y')} 09:00 AM",
        "cycleDescValue":  "Timely",
        "locationValue":   "All",
    }
    return base64.b64encode(json.dumps(payload, separators=(",", ":")).encode()).decode()


def clean_numeric(value):
    if isinstance(value, str):
        stripped = value.replace(",", "").strip()
        try:
            return int(stripped)
        except ValueError:
            try:
                return float(stripped)
            except ValueError:
                pass
    return value if value != "" else None


def init_session() -> requests.Session:
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
    try:
        session.get(LANDING_URL, timeout=15)
    except Exception:
        pass
    return session


def fetch_day(session: requests.Session, query_date: date) -> list[dict]:
    params = {
        "class": CLASS_B64,
        "type":  TYPE_B64,
        "_dc":   int(time.time() * 1000),
        "param": build_param(query_date),
        "page":  1,
        "start": 0,
        "limit": 500,
    }

    gas_date_str = query_date.strftime("%Y-%m-%d")

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = session.get(ROUTER_URL, params=params, timeout=30)
            resp.raise_for_status()
            raw = resp.json()

            if isinstance(raw, list):
                records = raw
            elif isinstance(raw, dict):
                records = [raw[k] for k in sorted(raw.keys(), key=lambda x: int(x))
                           if str(x).isdigit()]
            else:
                return []

            out = []
            for rec in records:
                if rec.get("statusCode") not in (None, 1):
                    continue
                row = {"gas_date": gas_date_str}
                for csv_col, db_col in COL_MAP.items():
                    val = rec.get(csv_col, "")
                    row[db_col] = clean_numeric(val) if db_col in NUMERIC_COLS else (val or None)
                out.append(row)
            return out

        except requests.exceptions.RequestException as exc:
            wait = 2 ** attempt
            print(f"  [retry {attempt}/{MAX_RETRIES}] {exc} - waiting {wait}s",
                  file=sys.stderr)
            if attempt == MAX_RETRIES:
                print(f"  [SKIP] {query_date} after {MAX_RETRIES} failed attempts",
                      file=sys.stderr)
                return []
            time.sleep(wait)
        except (ValueError, KeyError) as exc:
            print(f"  [SKIP] {query_date} - parse error: {exc}", file=sys.stderr)
            return []

    return []


def upsert(client, rows: list[dict]) -> None:
    if not rows:
        return
    client.table(TABLE_NAME).upsert(
        rows,
        on_conflict="gas_date,loc,loc_purp_desc,flow_ind_desc",
    ).execute()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    today = date.today()
    dates = [today - timedelta(days=i) for i in range(LOOKBACK_DAYS)]
    dates.reverse()  # oldest first

    print(f"Iroquois OAC daily updater  |  fetching {len(dates)} date(s): "
          f"{dates[0]} to {dates[-1]}")

    client  = get_client()
    session = init_session()
    total_written = 0

    for query_date in dates:
        records = fetch_day(session, query_date)
        if records:
            upsert(client, records)
            total_written += len(records)
            print(f"  {query_date}  {len(records)} records upserted")
        else:
            print(f"  {query_date}  no data")
        time.sleep(DELAY_SECONDS)

    print(f"\nDone.  {total_written} rows upserted.")


if __name__ == "__main__":
    main()
