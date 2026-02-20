"""
CMS MA Enrollment – Rolling 24-Month Dashboard
===============================================
Pulls data live from CMS.gov on demand — no local CSV needed.
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
CMS_BASE_URL  = "https://www.cms.gov"
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

SKIP_RE = re.compile(r"(read_?me|readme|__macosx|\.ds_store)", re.I)
DATA_RE = re.compile(r"(scc|enrollment|enroll|ma_)", re.I)


# ── Helpers ───────────────────────────────────────────────────────────────────
@st.cache_data(ttl=86400, show_spinner=False)
def get_available_periods() -> dict:
    """Scrape the CMS index and return {period: subpage_url}."""
    try:
        r = SESSION.get(CMS_INDEX_URL, timeout=30)
        r.raise_for_status()
    except Exception as e:
        st.error(f"Could not reach CMS.gov: {e}")
        return {}
    soup  = BeautifulSoup(r.text, "html.parser")
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

    try:
        with zipfile.ZipFile(io.BytesIO(r.content)) as zf:
            candidates = [
                n for n in zf.namelist()
                if re.search(r"\.(csv|txt)$", n, re.I)
                and not SKIP_RE.search(n)
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
            preferred  = [n for n in candidates if DATA_RE.search(n)]
            data_name  = preferred[0] if preferred else candidates[0]
            raw_bytes  = zf.read(data_name)
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

    # Standardise enrollment column
    enroll_col = next(
        (c for c in df.columns if any(k in c for k in ("ENROLL", "MEMBER", "BENE"))
         and "REPORT" not in c),
        None,
    )
    if enroll_col:
        df = df.rename(columns={enroll_col: "ENROLLMENT"})
    df["ENROLLMENT"] = pd.to_numeric(df.get("ENROLLMENT", pd.Series()), errors="coerce")

    # Standardise common column names
    rename_map = {}
    for col in df.columns:
        if col in ("ENROLLMENT", "REPORT_PERIOD"):
            continue
        if "STATE" in col and "FIPS" not in col and col not in rename_map.values():
            rename_map[col] = "STATE"
        elif "COUNTY" in col and "FIPS" not in col and "STATE" not in col and col not in rename_map.values():
            rename_map[col] = "COUNTY"
        elif re.search(r"CONTRACT.*(ID|NBR|NUM)", col) and "CONTRACT_ID" not in rename_map.values():
            rename_map[col] = "CONTRACT_ID"
        elif re.search(r"(CONTRACT|ORG).*(NAME|NM)", col) and "CONTRACT_NAME" not in rename_map.values():
            rename_map[col] = "CONTRACT_NAME"
    df = df.rename(columns=rename_map)

    return df


def normalise(df: pd.DataFrame) -> pd.DataFrame:
    """Keep only the columns we actually use to reduce memory."""
    keep = ["REPORT_PERIOD", "ENROLLMENT"]
    for c in ["STATE", "COUNTY", "CONTRACT_ID", "CONTRACT_NAME"]:
        if c in df.columns:
            keep.append(c)
    return df[keep].copy()


# ── Sidebar – period selector ─────────────────────────────────────────────────
st.sidebar.image(
    "https://www.cms.gov/themes/custom/cms_evo/logo.svg", width=160
)
st.sidebar.title("CMS MA Enrollment")

with st.sidebar:
    with st.spinner("Fetching available periods from CMS…"):
        all_periods_map = get_available_periods()

if not all_periods_map:
    st.error("Could not retrieve period list from CMS.gov. Please refresh.")
    st.stop()

all_periods = sorted(all_periods_map.keys(), reverse=True)[:ROLLING_MONTHS]
all_periods_sorted = sorted(all_periods)   # oldest → newest for display

st.sidebar.markdown("### Select Periods")
st.sidebar.caption("Start with 3–6 months for fastest load.")

selected_periods = st.sidebar.multiselect(
    "Report Period(s)",
    options=all_periods_sorted,
    default=all_periods_sorted[-6:],   # default: most recent 6 months
)

if not selected_periods:
    st.info("👈 Select at least one period in the sidebar to begin.")
    st.stop()

# ── Load selected periods ─────────────────────────────────────────────────────
st.title("🏥 CMS Medicare Advantage Enrollment")

frames = []
failed = []
progress_bar = st.progress(0, text="Loading data…")

for i, period in enumerate(sorted(selected_periods)):
    progress_bar.progress((i + 1) / len(selected_periods), text=f"Loading {period}…")
    zip_url = get_zip_url(all_periods_map[period])
    if not zip_url:
        failed.append(period)
        continue
    df_period = download_period(period, zip_url)
    if df_period is not None and not df_period.empty:
        frames.append(normalise(df_period))
    else:
        failed.append(period)

progress_bar.empty()

if failed:
    st.warning(f"Could not load data for: {', '.join(failed)}")

if not frames:
    st.error("No data could be loaded. Please try different periods.")
    st.stop()

df = pd.concat(frames, ignore_index=True)

# ── Additional sidebar filters ────────────────────────────────────────────────
periods_loaded = sorted(df["REPORT_PERIOD"].dropna().unique())

if "STATE" in df.columns:
    states = sorted(df["STATE"].dropna().unique())
    selected_states = st.sidebar.multiselect("State(s)", states, default=states)
else:
    selected_states = None

if "CONTRACT_NAME" in df.columns:
    contracts = sorted(df["CONTRACT_NAME"].dropna().unique())
    selected_contracts = st.sidebar.multiselect(
        "Contract / Plan", contracts, default=contracts[:20]
    )
else:
    selected_contracts = None

# ── Apply filters ─────────────────────────────────────────────────────────────
mask = df["REPORT_PERIOD"].isin(periods_loaded)
if selected_states and "STATE" in df.columns:
    mask &= df["STATE"].isin(selected_states)
if selected_contracts and "CONTRACT_NAME" in df.columns:
    mask &= df["CONTRACT_NAME"].isin(selected_contracts)
filtered = df[mask].copy()

# ── KPI row ───────────────────────────────────────────────────────────────────
latest_period   = periods_loaded[-1]
previous_period = periods_loaded[-2] if len(periods_loaded) >= 2 else None
latest_enroll   = filtered[filtered["REPORT_PERIOD"] == latest_period]["ENROLLMENT"].sum()
previous_enroll = (
    filtered[filtered["REPORT_PERIOD"] == previous_period]["ENROLLMENT"].sum()
    if previous_period else None
)
mom_delta = (latest_enroll - previous_enroll) if previous_enroll else None
mom_pct   = (mom_delta / previous_enroll * 100) if previous_enroll else None

st.caption(
    f"Loaded **{len(periods_loaded)}** period(s): "
    f"**{periods_loaded[0]}** – **{periods_loaded[-1]}**  ·  "
    f"{len(filtered):,} rows"
)

c1, c2, c3, c4 = st.columns(4)
c1.metric("Latest Period",    latest_period)
c2.metric("Total Enrollment", f"{latest_enroll:,.0f}")
c3.metric(
    "MoM Change",
    f"{mom_delta:+,.0f}" if mom_delta is not None else "—",
    delta=f"{mom_pct:+.2f}%" if mom_pct is not None else None,
)
c4.metric("Periods Loaded", len(periods_loaded))
st.divider()

# ── Tabs ──────────────────────────────────────────────────────────────────────
tab1, tab2, tab3, tab4 = st.tabs([
    "📈 Enrollment Trend",
    "🗺️ By State / County",
    "📋 By Contract / Plan",
    "🔄 Month-over-Month",
])

# Tab 1 ── Trend
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

# Tab 2 ── State / County
with tab2:
    if "STATE" not in df.columns:
        st.info("No STATE column found in this dataset.")
    else:
        options = ["STATE"]
        if "COUNTY" in df.columns:
            options.append("COUNTY")
        geo_col = st.radio("Group by", options, horizontal=True)
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

# Tab 3 ── Contract / Plan
with tab3:
    contract_col = next(
        (c for c in ["CONTRACT_NAME", "CONTRACT_ID"] if c in df.columns), None
    )
    if not contract_col:
        st.info("No contract/plan column found in this dataset.")
    else:
        top_n = st.slider("Show top N contracts", 5, 50, 15)
        contracts_data = (
            filtered[filtered["REPORT_PERIOD"] == latest_period]
            .groupby(contract_col)["ENROLLMENT"].sum()
            .reset_index().sort_values("ENROLLMENT", ascending=False).head(top_n)
        )
        fig3 = px.bar(
            contracts_data, x="ENROLLMENT", y=contract_col, orientation="h",
            title=f"Top {top_n} Contracts — {latest_period}",
            color="ENROLLMENT", color_continuous_scale="Teal",
        )
        fig3.update_layout(yaxis={"categoryorder": "total ascending"})
        st.plotly_chart(fig3, use_container_width=True)
        with st.expander("View full table"):
            all_c = (
                filtered[filtered["REPORT_PERIOD"] == latest_period]
                .groupby(contract_col)["ENROLLMENT"].sum()
                .reset_index().sort_values("ENROLLMENT", ascending=False)
            )
            st.dataframe(all_c, use_container_width=True)

# Tab 4 ── MoM
with tab4:
    group_choices = [c for c in ["STATE", "CONTRACT_NAME", "CONTRACT_ID"] if c in df.columns]
    if not group_choices:
        st.info("No grouping column available.")
    elif not previous_period:
        st.info("Select at least 2 periods in the sidebar to see month-over-month changes.")
    else:
        group_choice = st.selectbox("Compare MoM by", group_choices)
        curr = filtered[filtered["REPORT_PERIOD"] == latest_period].groupby(group_choice)["ENROLLMENT"].sum()
        prev = filtered[filtered["REPORT_PERIOD"] == previous_period].groupby(group_choice)["ENROLLMENT"].sum()
        mom  = pd.DataFrame({"CURRENT": curr, "PREVIOUS": prev}).dropna()
        mom["CHANGE"]   = mom["CURRENT"] - mom["PREVIOUS"]
        mom["CHANGE_%"] = (mom["CHANGE"] / mom["PREVIOUS"] * 100).round(2)
        mom = mom.reset_index().sort_values("CHANGE", ascending=False)

        fmt = {
            "PREVIOUS": "{:,.0f}", "CURRENT": "{:,.0f}",
            "CHANGE": "{:+,.0f}", "CHANGE_%": "{:+.2f}%",
        }
        cols_show = [group_choice, "PREVIOUS", "CURRENT", "CHANGE", "CHANGE_%"]
        col_a, col_b = st.columns(2)
        with col_a:
            st.subheader(f"📈 Biggest Gainers  ({previous_period} → {latest_period})")
            st.dataframe(mom.head(15)[cols_show].style.format(fmt), use_container_width=True)
        with col_b:
            st.subheader("📉 Biggest Decliners")
            st.dataframe(mom.tail(15)[cols_show].sort_values("CHANGE").style.format(fmt), use_container_width=True)

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
