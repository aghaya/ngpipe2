"""
FP Foundry — Natural Gas Pipeline Intelligence
Streamlit web app serving Iroquois OAC data from Supabase PostgREST API.
Uses requests (not supabase-py) for maximum compatibility.
"""

import io
from datetime import date, timedelta

import pandas as pd
import requests
import streamlit as st

# ---------------------------------------------------------------------------
# Page config
# ---------------------------------------------------------------------------
st.set_page_config(
    page_title="FP Foundry | Pipeline Data",
    page_icon="🔥",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ---------------------------------------------------------------------------
# Supabase REST helpers
# ---------------------------------------------------------------------------
def _headers() -> dict:
    key = st.secrets["SUPABASE_KEY"]
    return {
        "apikey":        key,
        "Authorization": f"Bearer {key}",
        "Content-Type":  "application/json",
    }

def _base_url(table: str) -> str:
    return f"{st.secrets['SUPABASE_URL'].rstrip('/')}/rest/v1/{table}"


# ---------------------------------------------------------------------------
# Data helpers
# ---------------------------------------------------------------------------
DISPLAY_COLS = {
    "gas_date":                   "Gas Date",
    "loc":                        "Loc ID",
    "loc_name":                   "Location",
    "loc_purp_desc":              "Purpose",
    "loc_qti_desc":               "QTI Desc",
    "flow_ind_desc":              "Flow Direction",
    "design_capacity":            "Design Cap (MMBtu)",
    "operating_capacity":         "Operating Cap (MMBtu)",
    "total_scheduled_quantity":   "Scheduled Qty (MMBtu)",
    "oac":                        "OAC (MMBtu)",
    "all_qty_avail":              "All Qty Avail",
    "it_indicator":               "IT",
    "posting_date":               "Posting Date",
    "posting_time":               "Posting Time",
}

PIPELINES = {
    "Iroquois Gas Transmission": "iroquois_oac",
}


@st.cache_data(ttl=3600, show_spinner=False)
def load_locations(table: str) -> list[dict]:
    """Return sorted unique (loc, loc_name) pairs for the sidebar."""
    resp = requests.get(
        _base_url(table),
        headers={**_headers(), "Accept": "application/json"},
        params={
            "select":  "loc,loc_name",
            "order":   "loc_name",
        },
        timeout=30,
    )
    resp.raise_for_status()
    seen, out = set(), []
    for r in resp.json():
        k = (r["loc"], r["loc_name"])
        if k not in seen:
            seen.add(k)
            out.append({"id": r["loc"], "name": r["loc_name"]})
    return out


@st.cache_data(ttl=300, show_spinner=False)
def load_data(
    table: str,
    start: date,
    end: date,
    loc_ids: tuple,
    purpose: str,
) -> pd.DataFrame:
    """Fetch data from Supabase PostgREST with pagination, return DataFrame."""
    rows, offset, page_size = [], 0, 1000

    while True:
        params = {
            "select":   "*",
            "gas_date": f"gte.{start},lte.{end}",
            "order":    "gas_date.desc,loc_name",
            "offset":   offset,
            "limit":    page_size,
        }
        # PostgREST filter syntax
        param_list = [
            ("select", "*"),
            ("gas_date", f"gte.{start}"),
            ("gas_date", f"lte.{end}"),
            ("order",   "gas_date.desc"),
            ("order",   "loc_name"),
            ("offset",  offset),
            ("limit",   page_size),
        ]
        if loc_ids:
            param_list.append(("loc", f"in.({','.join(str(i) for i in loc_ids)})"))
        if purpose != "All":
            param_list.append(("loc_purp_desc", f"ilike.*{purpose}*"))

        resp = requests.get(
            _base_url(table),
            headers={**_headers(), "Accept": "application/json",
                     "Prefer": "count=none"},
            params=param_list,
            timeout=30,
        )
        resp.raise_for_status()
        chunk = resp.json()
        rows.extend(chunk)
        if len(chunk) < page_size or offset > 100_000:
            break
        offset += page_size

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows)
    df["gas_date"] = pd.to_datetime(df["gas_date"]).dt.date
    for col in ("design_capacity", "operating_capacity",
                "total_scheduled_quantity", "oac"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


def df_to_csv_bytes(df: pd.DataFrame) -> bytes:
    buf = io.BytesIO()
    df.to_csv(buf, index=False)
    return buf.getvalue()


# ---------------------------------------------------------------------------
# Sidebar — filters
# ---------------------------------------------------------------------------
with st.sidebar:
    st.image("https://img.icons8.com/fluency/96/gas-industry.png", width=60)
    st.title("FP Foundry")
    st.caption("Natural Gas Pipeline Intelligence")
    st.divider()

    pipeline_name = st.selectbox("Pipeline", list(PIPELINES.keys()))
    table = PIPELINES[pipeline_name]

    st.subheader("Filters")

    col1, col2 = st.columns(2)
    with col1:
        start_date = st.date_input(
            "From",
            value=date.today() - timedelta(days=30),
            min_value=date(2010, 1, 1),
            max_value=date.today(),
        )
    with col2:
        end_date = st.date_input(
            "To",
            value=date.today(),
            min_value=date(2010, 1, 1),
            max_value=date.today(),
        )

    if start_date > end_date:
        st.error("Start date must be before end date.")
        st.stop()

    # Location multiselect
    with st.spinner("Loading locations..."):
        all_locs = load_locations(table)
    loc_options = {loc["name"]: loc["id"] for loc in all_locs}

    selected_loc_names = st.multiselect(
        "Locations",
        options=list(loc_options.keys()),
        default=[],
        placeholder="All locations",
    )
    selected_loc_ids = tuple(loc_options[n] for n in selected_loc_names)

    purpose = st.selectbox(
        "Flow Purpose",
        ["All", "Receipt", "Delivery"],
    )

    st.divider()
    st.caption("Data: Iroquois Gas Transmission EBB  \nUpdated daily via GitHub Actions")


# ---------------------------------------------------------------------------
# Main content
# ---------------------------------------------------------------------------
st.title("Natural Gas Pipeline — Operationally Available Capacity")
st.caption(
    f"Showing **{pipeline_name}** | "
    f"{start_date.strftime('%b %d, %Y')} – {end_date.strftime('%b %d, %Y')}"
)

with st.spinner("Fetching data..."):
    df = load_data(table, start_date, end_date, selected_loc_ids, purpose)

if df.empty:
    st.warning("No data found for the selected filters. Try adjusting the date range or location.")
    st.stop()

# --- Summary metrics ---
m1, m2, m3, m4 = st.columns(4)
m1.metric("Records", f"{len(df):,}")
m2.metric("Locations", df["loc_name"].nunique())
m3.metric(
    "Avg OAC (MMBtu)",
    f"{df['oac'].mean():,.0f}" if "oac" in df else "—",
)
m4.metric(
    "Date Range",
    f"{df['gas_date'].min()} – {df['gas_date'].max()}",
)

st.divider()

# --- Display table (rename columns) ---
rename = {k: v for k, v in DISPLAY_COLS.items() if k in df.columns}
display_df = df[list(rename.keys())].rename(columns=rename)

# Format numeric cols with commas
for col in ("Design Cap (MMBtu)", "Operating Cap (MMBtu)",
            "Scheduled Qty (MMBtu)", "OAC (MMBtu)"):
    if col in display_df.columns:
        display_df[col] = display_df[col].apply(
            lambda x: f"{int(x):,}" if pd.notna(x) else ""
        )

st.dataframe(display_df, use_container_width=True, height=500)

# --- Download ---
st.download_button(
    label="Download CSV",
    data=df_to_csv_bytes(df),
    file_name=f"iroquois_oac_{start_date}_{end_date}.csv",
    mime="text/csv",
)

st.caption(
    "Source: [Iroquois Gas Transmission EBB](https://ioly.iroquois.com/infopost/#operationallyavailable) | "
    "Built with [FP Foundry](https://fpfoundry.com)"
)
