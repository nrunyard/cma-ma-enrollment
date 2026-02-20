"""
CMS MA Enrollment – Rolling 24-Month Dashboard
===============================================
Pulls data live from CMS.gov — no local CSV needed.
Run locally:  streamlit run dashboard.py
Hosted on:    Streamlit Community Cloud
"""

import re
import io
import zipfile
import requests
import streamlit as st
import pandas as pd
import plotly.express as px
from bs4 import BeautifulSoup

# ── Page config ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="CMS MA Enrollment Dashboard",
    page_icon="🏥",
    layout="wide",
)

CMS_INDEX_URL = (
    "https://www.cms.gov/data-research/statistics-trends-and-reports/"
    "medicare-advantagepart-d-contract-and-enrollment-data/"
    "monthly-ma-enrollment-state/county/contract"
)
CMS_BASE_URL = "https://www.cms.gov"
ROLLING_MONTHS = 24

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    )
}
SESSION = requests.Session()
SESSION.headers.update(HEADERS)

SKIP_PATTERNS = re.compile(r"(read_?me|readme|__macosx|\.ds_store)", re.I)
DATA_PREF     = re.compile(r"(scc|enrollment|enroll|ma_)", re.I)


# ── CMS scraping helpers (cached aggressively) ────────────────────────────────
@st.cache_data(ttl=86400, show_spinner=False)
def get_scc_subpage_links() -> dict:
    r = SESSION.get(CMS_INDEX_URL, timeout=30)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")
    links = {}
    for a in soup.find_all("a", href=True):
        href = a["href"]
        m = re.search(r"ma-enrollment-scc-(\d{4}-\d{2})$", href)
        if m:
            period = m.group(1)
            links[period] = href if href.startswith("http") else CMS_BASE_URL + href
    return links


@st.cache_data(ttl=86400, show_spinner=False)
def get_zip_url(subpage_url: str) -> str | None:
    try:
        r = SESSION.get(subpage_url, timeout=30)
        r.raise_for_status()
    except Exception:
        return None
    soup = BeautifulSoup(r.text, "html.parser")
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if href.lower().endswith(".zip"):
            return href if href.startswith("http") else CMS_BASE_URL + href
    return None


@st.cache_data(ttl=86400, show_spinner=False)
def download_period(period: str, zip_url: str) -> pd.DataFrame | None:
    try:
        r = SESSION.get(zip_url, timeout=120)
        r.raise_for_status()
    except Exception:
        return None

    content = r.content
    try:
        with zipfile.ZipFile(io.BytesIO(content)) as zf:
            candidates = [
                n for n in zf.namelist()
                if re.search(r"\.(csv|txt)$", n, re.I)
                and not SKIP_PATTERNS.search(n)
                and not n.endswith("/")
            ]
            if not candidates:
                candidates = [
                    n for n in zf.namelist()
                    if re.search(r"\.(csv|txt)$", n, re.I)
                    and not n.endswith("/")
                    and "read" not in n.lower()
                ]
            if not candidates:
                return None
            preferred = [n for n in candidates if DATA_PREF.search(n)]
            data_name = preferred[0] if preferred else candidates[0]
            raw_bytes = zf.read(data_name)
    except zipfile.BadZipFile:
        return None

    df = None
    for enc in ("utf-8-sig", "latin-1", "cp1252"):
        for sep in (",", "\t", "|"):
            try:
                candidate = pd.read_csv(
                    io.BytesIO(raw_bytes), dtype=str,
                    encoding=enc, sep=sep, low_memory=False,
                )
                if candidate.shape[1] >= 2:
                    df = candidate
                    break
            except Exception:
                continue
        if df is not None:
            break

    if df is None:
        return None

    df.columns = [c.strip().upper().replace(" ", "_") for c in df.columns]
    df.insert(0, "REPORT_PERIOD", period)
    return df


@st.cache_data(ttl=86400, show_spinner=False)
def load_all_data() -> pd.DataFrame:
    all_links = get_scc_subpage_links()
    periods   = sorted(all_links.keys(), reverse=True)[:ROLLING_MONTHS]

    frames = []
    progress = st.progress(0, text="Loading CMS data…")
    for i, period in enumerate(sorted(periods)):
        progress.progress((i + 1) / len(periods), text=f"Loading {period}…")
        zip_url = get_zip_url(all_links[period])
        if not zip_url:
            continue
        df = download_period(period, zip_url)
        if df is not None and not df.empty:
            frames.append(df)
    progress.empty()

    if not frames:
        return pd.DataFrame()

    combined = pd.concat(frames, ignore_index=True)

    # Standardise enrollment column
    enroll_candidates = [
        c for c in combined.columns
        if any(k in c for k in ("ENROLL", "MEMBER", "BENE", "COUNT"))
        and "REPORT" not in c
    ]
    if enroll_candidates:
        combined = combined.rename(columns={enroll_candidates[0]: "ENROLLMENT"})
    combined["ENROLLMENT"] = pd.to_numeric(combined["ENROLLMENT"], errors="coerce")

    # Standardise other column names
    rename_map = {}
    for col in combined.columns:
        if col in ("ENROLLMENT", "REPORT_PERIOD"):
            continue
        if "STATE" in col and "FIPS" not in col:
            rename_map[col] = "STATE"
        elif "COUNTY" in col and "FIPS" not in col and "STATE" not in col:
            rename_map[col] = "COUNTY"
        elif re.search(r"CONTRACT.*(ID|NBR|NUM)", col):
            rename_map[col] = "CONTRACT_ID"
        elif re.search(r"(CONTRACT|ORG).*(NAME|NM)", col):
            rename_map[col] = "CONTRACT_NAME"
    combined = combined.rename(columns=rename_map)

    return combined


# ── Load data with a nice spinner ─────────────────────────────────────────────
with st.spinner("Fetching latest CMS enrollment data (first load may take ~60s)…"):
    df = load_all_data()

if df.empty:
    st.error("Could not load data from CMS. Please try refreshing.")
    st.stop()

# ── Sidebar filters ───────────────────────────────────────────────────────────
st.sidebar.image(
    "https://www.cms.gov/themes/custom/cms_evo/logo.svg", width=160
)
st.sidebar.title("Filters")

periods = sorted(df["REPORT_PERIOD"].dropna().unique())

selected_periods = st.sidebar.multiselect(
    "Report Period(s)", options=periods, default=periods,
)

if "STATE" in df.columns:
    states = sorted(df["STATE"].dropna().unique())
    selected_states = st.sidebar.multiselect(
        "State(s)", options=states, default=states,
    )
else:
    selected_states = None

if "CONTRACT_NAME" in df.columns:
    contracts = sorted(df["CONTRACT_NAME"].dropna().unique())
    selected_contracts = st.sidebar.multiselect(
        "Contract / Plan", options=contracts, default=contracts[:20],
    )
else:
    selected_contracts = None

# ── Apply filters ─────────────────────────────────────────────────────────────
mask = df["REPORT_PERIOD"].isin(selected_periods)
if selected_states and "STATE" in df.columns:
    mask &= df["STATE"].isin(selected_states)
if selected_contracts and "CONTRACT_NAME" in df.columns:
    mask &= df["CONTRACT_NAME"].isin(selected_contracts)
filtered = df[mask].copy()

# ── Header ────────────────────────────────────────────────────────────────────
st.title("🏥 CMS Medicare Advantage Enrollment")
st.caption(
    f"Rolling 24-month view  ·  "
    f"Periods: **{periods[0]}** – **{periods[-1]}**  ·  "
    f"{len(filtered):,} rows after filters"
)

# ── KPI row ───────────────────────────────────────────────────────────────────
latest_period   = periods[-1]
previous_period = periods[-2] if len(periods) >= 2 else None
latest_enroll   = filtered[filtered["REPORT_PERIOD"] == latest_period]["ENROLLMENT"].sum()
previous_enroll = (
    filtered[filtered["REPORT_PERIOD"] == previous_period]["ENROLLMENT"].sum()
    if previous_period else None
)
mom_delta = latest_enroll - previous_enroll if previous_enroll else None
mom_pct   = (mom_delta / previous_enroll * 100) if previous_enroll else None

col1, col2, col3, col4 = st.columns(4)
col1.metric("Latest Period",    latest_period)
col2.metric("Total Enrollment", f"{latest_enroll:,.0f}")
col3.metric(
    "MoM Change",
    f"{mom_delta:+,.0f}" if mom_delta is not None else "—",
    delta=f"{mom_pct:+.2f}%" if mom_pct is not None else None,
)
col4.metric("Periods Selected", len(selected_periods))
st.divider()

# ── Tabs ──────────────────────────────────────────────────────────────────────
tab1, tab2, tab3, tab4 = st.tabs([
    "📈 Enrollment Trend",
    "🗺️ By State / County",
    "📋 By Contract / Plan",
    "🔄 Month-over-Month",
])

with tab1:
    trend = (
        filtered.groupby("REPORT_PERIOD")["ENROLLMENT"]
        .sum().reset_index().sort_values("REPORT_PERIOD")
    )
    fig = px.line(
        trend, x="REPORT_PERIOD", y="ENROLLMENT", markers=True,
        title="Total MA Enrollment Over Time",
        labels={"REPORT_PERIOD": "Period", "ENROLLMENT": "Enrollees"},
    )
    fig.update_layout(hovermode="x unified")
    st.plotly_chart(fig, use_container_width=True)
    with st.expander("View data table"):
        st.dataframe(trend, use_container_width=True)

with tab2:
    if "STATE" not in df.columns:
        st.info("No STATE column found in this dataset.")
    else:
        geo_col = st.radio(
            "Group by",
            ["STATE", "COUNTY"] if "COUNTY" in df.columns else ["STATE"],
            horizontal=True,
        )
        geo = (
            filtered[filtered["REPORT_PERIOD"] == latest_period]
            .groupby(geo_col)["ENROLLMENT"].sum()
            .reset_index().sort_values("ENROLLMENT", ascending=False).head(30)
        )
        fig2 = px.bar(
            geo, x=geo_col, y="ENROLLMENT",
            title=f"Top 30 by {geo_col.title()} — {latest_period}",
            color="ENROLLMENT", color_continuous_scale="Blues",
        )
        st.plotly_chart(fig2, use_container_width=True)
        with st.expander("View full table"):
            full_geo = (
                filtered[filtered["REPORT_PERIOD"] == latest_period]
                .groupby(geo_col)["ENROLLMENT"].sum()
                .reset_index().sort_values("ENROLLMENT", ascending=False)
            )
            st.dataframe(full_geo, use_container_width=True)

with tab3:
    if "CONTRACT_NAME" not in df.columns and "CONTRACT_ID" not in df.columns:
        st.info("No contract/plan column found in this dataset.")
    else:
        contract_col = "CONTRACT_NAME" if "CONTRACT_NAME" in df.columns else "CONTRACT_ID"
        top_n = st.slider("Show top N contracts", 5, 50, 15)
        contracts_data = (
            filtered[filtered["REPORT_PERIOD"] == latest_period]
            .groupby(contract_col)["ENROLLMENT"].sum()
            .reset_index().sort_values("ENROLLMENT", ascending=False).head(top_n)
        )
        fig3 = px.bar(
            contracts_data, x="ENROLLMENT", y=contract_col, orientation="h",
            title=f"Top {top_n} Contracts by Enrollment — {latest_period}",
            color="ENROLLMENT", color_continuous_scale="Teal",
        )
        fig3.update_layout(yaxis={"categoryorder": "total ascending"})
        st.plotly_chart(fig3, use_container_width=True)
        with st.expander("View full table"):
            all_contracts = (
                filtered[filtered["REPORT_PERIOD"] == latest_period]
                .groupby(contract_col)["ENROLLMENT"].sum()
                .reset_index().sort_values("ENROLLMENT", ascending=False)
            )
            st.dataframe(all_contracts, use_container_width=True)

with tab4:
    group_choices = [c for c in ["STATE", "CONTRACT_NAME", "CONTRACT_ID"] if c in df.columns]
    if not group_choices:
        st.info("No grouping column available for MoM analysis.")
    elif not previous_period:
        st.info("Need at least 2 periods selected to show month-over-month changes.")
    else:
        group_choice = st.selectbox("Compare MoM by", group_choices)
        curr = filtered[filtered["REPORT_PERIOD"] == latest_period].groupby(group_choice)["ENROLLMENT"].sum()
        prev = filtered[filtered["REPORT_PERIOD"] == previous_period].groupby(group_choice)["ENROLLMENT"].sum()
        mom  = pd.DataFrame({"CURRENT": curr, "PREVIOUS": prev}).dropna()
        mom["CHANGE"]   = mom["CURRENT"] - mom["PREVIOUS"]
        mom["CHANGE_%"] = (mom["CHANGE"] / mom["PREVIOUS"] * 100).round(2)
        mom = mom.reset_index().sort_values("CHANGE", ascending=False)

        col_a, col_b = st.columns(2)
        fmt = {"PREVIOUS": "{:,.0f}", "CURRENT": "{:,.0f}", "CHANGE": "{:+,.0f}", "CHANGE_%": "{:+.2f}%"}
        with col_a:
            st.subheader(f"📈 Biggest Gainers  ({previous_period} → {latest_period})")
            st.dataframe(mom.head(15)[[group_choice, "PREVIOUS", "CURRENT", "CHANGE", "CHANGE_%"]].style.format(fmt), use_container_width=True)
        with col_b:
            st.subheader("📉 Biggest Decliners")
            st.dataframe(mom.tail(15)[[group_choice, "PREVIOUS", "CURRENT", "CHANGE", "CHANGE_%"]].sort_values("CHANGE").style.format(fmt), use_container_width=True)

        fig4 = px.bar(
            mom.head(20), x=group_choice, y="CHANGE",
            title=f"Top 20 MoM Change by {group_choice.replace('_',' ').title()}",
            color="CHANGE", color_continuous_scale="RdYlGn", color_continuous_midpoint=0,
        )
        st.plotly_chart(fig4, use_container_width=True)

st.divider()
st.caption(
    "Data source: [CMS Monthly MA Enrollment by State/County/Contract]"
    "(https://www.cms.gov/data-research/statistics-trends-and-reports/"
    "medicare-advantagepart-d-contract-and-enrollment-data/"
    "monthly-ma-enrollment-state/county/contract)"
)
