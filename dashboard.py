"""
CMS MA Enrollment – Rolling 24-Month Dashboard
===============================================
Pulls enrollment data live from CMS.gov on demand.
Joins MA Plan Directory to add Parent Organization filter.
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
CMS_PLAN_DIR_URL = (
    "https://www.cms.gov/data-research/statistics-trends-and-reports/"
    "medicare-advantagepart-d-contract-and-enrollment-data/ma-plan-directory"
)
CMS_BASE_URL   = "https://www.cms.gov"
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
DATA_RE = re.compile(r"(scc|enrollment|enroll|ma_|plan_dir|directory)", re.I)


# ── Generic ZIP download helper ───────────────────────────────────────────────
def _fetch_zip_df(zip_url: str) -> pd.DataFrame | None:
    """Download a ZIP and return the first plausible data file as a DataFrame."""
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
            preferred = [n for n in candidates if DATA_RE.search(n)]
            data_name = preferred[0] if preferred else candidates[0]
            raw_bytes = zf.read(data_name)
    except zipfile.BadZipFile:
        return None

    df = None
    for enc in ("utf-8-sig", "latin-1", "cp1252"):
        for sep in (",", "\t", "|"):
            try:
                cand = pd.read_csv(
                    io.BytesIO(raw_bytes), dtype=str,
                    encoding=enc, sep=sep, low_memory=False,
                )
                if cand.shape[1] >= 2:
                    df = cand
                    break
            except Exception:
                continue
        if df is not None:
            break

    if df is not None:
        df.columns = [c.strip().upper().replace(" ", "_") for c in df.columns]
    return df


def _get_zip_url_from_page(page_url: str) -> str | None:
    """Scrape a CMS page and return the first ZIP link found."""
    try:
        r = SESSION.get(page_url, timeout=30)
        r.raise_for_status()
    except Exception:
        return None
    soup = BeautifulSoup(r.text, "html.parser")
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if href.lower().endswith(".zip"):
            return href if href.startswith("http") else CMS_BASE_URL + href
    return None


# ── MA Plan Directory → CONTRACT_ID : PARENT_ORGANIZATION lookup ─────────────
# Store raw plan directory columns globally so diagnostics can inspect them
_plan_dir_raw_columns: list = []
_plan_dir_contract_col: str = ""
_plan_dir_parent_col: str = ""

@st.cache_data(ttl=86400, show_spinner=False)
def load_plan_directory() -> pd.DataFrame:
    """
    Downloads the MA Plan Directory and returns a DataFrame with:
        CONTRACT_ID  |  PARENT_ORGANIZATION  |  (PLAN_TYPE_DIR optional)
    Tries multiple column name patterns to find the contract number column,
    including the common "CONTRACT_NUMBER" name used in recent CMS files.
    Falls back gracefully if the file can't be fetched.
    """
    global _plan_dir_raw_columns, _plan_dir_contract_col, _plan_dir_parent_col

    zip_url = _get_zip_url_from_page(CMS_PLAN_DIR_URL)
    if not zip_url:
        return pd.DataFrame(columns=["CONTRACT_ID", "PARENT_ORGANIZATION"])

    df = _fetch_zip_df(zip_url)
    if df is None or df.empty:
        return pd.DataFrame(columns=["CONTRACT_ID", "PARENT_ORGANIZATION"])

    _plan_dir_raw_columns = list(df.columns)

    # --- Find contract number column ---
    # Priority order: exact names first, then pattern match, then value pattern
    contract_col = None
    for candidate in ["CONTRACT_NUMBER", "CONTRACT_ID", "CONTRACT_NO", "CONTRACT_NBR", "CONTRACTNUMBER"]:
        if candidate in df.columns:
            contract_col = candidate
            break
    if contract_col is None:
        contract_col = next(
            (c for c in df.columns if re.search(r"CONTRACT.*(NUMBER|NUM|NBR|ID|NO)", c)), None
        )
    if contract_col is None:
        # Detect by value pattern: H1234, R1234, S1234, E1234 etc.
        for c in df.columns:
            sample = df[c].dropna().head(30).astype(str)
            if sample.str.match(r"^[A-Z]\d{4}$").sum() >= 5:
                contract_col = c
                break

    # --- Find parent org column ---
    parent_col = None
    for candidate in ["PARENT_ORGANIZATION", "PARENT_ORG", "PARENT_ORGANIZATION_NAME"]:
        if candidate in df.columns:
            parent_col = candidate
            break
    if parent_col is None:
        parent_col = next(
            (c for c in df.columns if "PARENT" in c and "ORG" in c), None
        )
    if parent_col is None:
        parent_col = next(
            (c for c in df.columns if "PARENT" in c), None
        )

    _plan_dir_contract_col = contract_col or "(not found)"
    _plan_dir_parent_col   = parent_col   or "(not found)"

    if contract_col is None or parent_col is None:
        return pd.DataFrame(columns=["CONTRACT_ID", "PARENT_ORGANIZATION"])

    # --- Find plan/contract type column ---
    type_col = next(
        (c for c in df.columns if re.search(r"(PLAN|CONTRACT).*(TYPE|TYP)", c)), None
    )

    # Explicitly select only the columns we need — avoids the plan directory's
    # own ENROLLMENT column colliding with the enrollment data after the merge
    keep_cols   = [contract_col, parent_col]
    rename_dict = {contract_col: "CONTRACT_ID", parent_col: "PARENT_ORGANIZATION"}
    if type_col:
        keep_cols.append(type_col)
        rename_dict[type_col] = "PLAN_TYPE_DIR"

    lookup = (
        df[keep_cols]   # only these columns — ENROLLMENT from plan dir is excluded
        .rename(columns=rename_dict)
        .dropna(subset=["CONTRACT_ID"])
        .drop_duplicates(subset=["CONTRACT_ID"])
    )
    lookup["CONTRACT_ID"]         = lookup["CONTRACT_ID"].astype(str).str.strip().str.upper()
    lookup["PARENT_ORGANIZATION"] = lookup["PARENT_ORGANIZATION"].astype(str).str.strip().str.title()
    lookup["PARENT_ORGANIZATION"] = consolidate_parent_org(lookup["PARENT_ORGANIZATION"])
    if "PLAN_TYPE_DIR" in lookup.columns:
        lookup["PLAN_TYPE_DIR"] = lookup["PLAN_TYPE_DIR"].str.strip().str.title()
    return lookup


# ── Enrollment scraping helpers ───────────────────────────────────────────────
@st.cache_data(ttl=86400, show_spinner=False)
def get_available_periods() -> dict:
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
    return _get_zip_url_from_page(subpage_url)


@st.cache_data(ttl=86400, show_spinner=False)
def download_period(period: str, zip_url: str) -> pd.DataFrame | None:
    df = _fetch_zip_df(zip_url)
    if df is None:
        return None

    df.insert(0, "REPORT_PERIOD", period)

    # Standardise enrollment column
    # Use the "Enrolled" column directly — this is the member count column in the SCC file
    if "ENROLLED" in df.columns:
        enroll_col = "ENROLLED"
    else:
        # Fallback: find any column with ENROLL/MEMBER/BENE in the name
        enroll_col = next(
            (c for c in df.columns
             if any(k in c for k in ("ENROLL", "MEMBER", "BENE"))
             and "REPORT" not in c),
            None,
        )

    if enroll_col:
        df = df.rename(columns={enroll_col: "ENROLLMENT"})

    if "ENROLLMENT" in df.columns:
        raw = df["ENROLLMENT"].astype(str).str.strip()
        numeric_vals = pd.to_numeric(raw, errors="coerce")
        # CMS suppresses counts under 11 with "*" — substitute with 5 as midpoint estimate
        suppressed = numeric_vals.isna() & raw.notna() & (raw != "") & (raw.str.lower() != "nan")
        numeric_vals[suppressed] = 5
        df["ENROLLMENT"] = numeric_vals

    # Standardise other column names
    rename_map = {}
    for col in df.columns:
        if col in ("ENROLLMENT", "REPORT_PERIOD"):
            continue
        if "STATE" in col and "FIPS" not in col and "STATE" not in rename_map.values():
            rename_map[col] = "STATE"
        elif "COUNTY" in col and "FIPS" not in col and "STATE" not in col and "COUNTY" not in rename_map.values():
            rename_map[col] = "COUNTY"
        elif re.search(r"CONTRACT.*(NUMBER|ID|NBR|NUM)", col) and "CONTRACT_ID" not in rename_map.values():
            rename_map[col] = "CONTRACT_ID"
        elif re.search(r"(CONTRACT|ORG).*(NAME|NM)", col) and "CONTRACT_NAME" not in rename_map.values():
            rename_map[col] = "CONTRACT_NAME"
    df = df.rename(columns=rename_map)

    return df


# ── Parent org consolidation map ─────────────────────────────────────────────
# CMS Plan Directory fragments large insurers across multiple legal entity names.
# This map collapses known variants into a single canonical parent org name so
# that market share totals match industry-reported figures.
PARENT_ORG_CONSOLIDATION = {
    # UnitedHealth Group
    "Unitedhealthcare":                         "UnitedHealth Group",
    "Unitedhealth Group":                       "UnitedHealth Group",
    "United Healthcare":                        "UnitedHealth Group",
    "United Health Care":                       "UnitedHealth Group",
    "Aarp/Unitedhealthcare":                    "UnitedHealth Group",
    "Ovations":                                 "UnitedHealth Group",
    "Pacificare":                               "UnitedHealth Group",
    "Sierra Health And Life":                   "UnitedHealth Group",
    "Americhoice":                              "UnitedHealth Group",
    # CVS / Aetna
    "Cvs Health Corporation":                   "CVS Health / Aetna",
    "Aetna":                                    "CVS Health / Aetna",
    "Cvs Health":                               "CVS Health / Aetna",
    "Aetna Inc.":                               "CVS Health / Aetna",
    # Humana
    "Humana":                                   "Humana",
    "Humana Inc.":                              "Humana",
    "Humana Inc":                               "Humana",
    # Elevance (Anthem/BCBS)
    "Elevance Health":                          "Elevance Health",
    "Anthem":                                   "Elevance Health",
    "Anthem, Inc.":                             "Elevance Health",
    "Anthem Inc":                               "Elevance Health",
    # Centene
    "Centene Corporation":                      "Centene",
    "Centene":                                  "Centene",
    "Wellcare":                                 "Centene",
    "Wellcare Health Plans":                    "Centene",
    # Kaiser
    "Kaiser Foundation Health Plan":            "Kaiser Permanente",
    "Kaiser Foundation Health Plan, Inc":       "Kaiser Permanente",
    "Kaiser":                                   "Kaiser Permanente",
    # Cigna
    "Cigna":                                    "Cigna",
    "Cigna Corporation":                        "Cigna",
    "Cigna Healthcare":                         "Cigna",
    "Cigna-Healthspring":                       "Cigna",
    # Molina
    "Molina Healthcare":                        "Molina Healthcare",
    "Molina Healthcare, Inc":                   "Molina Healthcare",
    # SCAN
    "Scan Health Plan":                         "SCAN Health Plan",
    # UPMC
    "Upmc Health Plan":                         "UPMC Health Plan",
}

def consolidate_parent_org(series: pd.Series) -> pd.Series:
    """Map CMS directory parent org names to canonical consolidated names."""
    # Title-case to normalise, then look up in map; keep original if not found
    title = series.str.strip().str.title()
    return title.map(PARENT_ORG_CONSOLIDATION).fillna(title)

# Contract type label map derived from the first letter of CONTRACT_ID
CONTRACT_TYPE_MAP = {
    "H": "Local MA / HMO / Cost / PACE",
    "R": "Regional PPO",
    "S": "Standalone PDP",
    "E": "Employer / Union Direct",
    "9": "Other / Demo",
}

def derive_contract_type(contract_id_series: pd.Series) -> pd.Series:
    """Return a human-readable contract type from the first char of CONTRACT_ID."""
    first_char = contract_id_series.astype(str).str.strip().str.upper().str[0]
    return first_char.map(CONTRACT_TYPE_MAP).fillna("Other")

def normalise(df: pd.DataFrame) -> pd.DataFrame:
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

all_periods        = sorted(all_periods_map.keys(), reverse=True)[:ROLLING_MONTHS]
all_periods_sorted = sorted(all_periods)

st.sidebar.markdown("### Select Periods")
st.sidebar.caption("Start with 3–6 months for fastest load.")

selected_periods = st.sidebar.multiselect(
    "Report Period(s)",
    options=all_periods_sorted,
    default=all_periods_sorted[-6:],
)

if not selected_periods:
    st.info("👈 Select at least one period in the sidebar to begin.")
    st.stop()

st.sidebar.markdown("### Compare Periods")
st.sidebar.caption("Choose which two periods to compare in KPIs and the Period-over-Period tab.")

# ── Load enrollment data ──────────────────────────────────────────────────────
st.title("🏥 CMS Medicare Advantage Enrollment")

frames = []
failed = []
progress_bar = st.progress(0, text="Loading enrollment data…")

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

# ── Load & join Plan Directory for Parent Organization ────────────────────────
with st.spinner("Loading MA Plan Directory for parent organization data…"):
    plan_dir = load_plan_directory()

def clean_contract_id(series: pd.Series) -> pd.Series:
    """Aggressively normalise contract IDs: strip, upper, remove non-alphanumeric."""
    return series.astype(str).str.strip().str.upper().str.replace(r"[^A-Z0-9]", "", regex=True)

if not plan_dir.empty and "CONTRACT_ID" in df.columns:
    # Clean both sides before joining
    df["CONTRACT_ID"]       = clean_contract_id(df["CONTRACT_ID"])
    plan_dir                = plan_dir.copy()
    plan_dir["CONTRACT_ID"] = clean_contract_id(plan_dir["CONTRACT_ID"])

    df = df.merge(plan_dir, on="CONTRACT_ID", how="left")

    # If the plan directory had its own ENROLLMENT column, pandas renames to
    # ENROLLMENT_x (ours) and ENROLLMENT_y (theirs) — fix that immediately
    if "ENROLLMENT_x" in df.columns:
        df = df.rename(columns={"ENROLLMENT_x": "ENROLLMENT"}).drop(columns=["ENROLLMENT_y"], errors="ignore")

    has_parent_org = "PARENT_ORGANIZATION" in df.columns and df["PARENT_ORGANIZATION"].notna().any()

    # Derive CONTRACT_TYPE from CONTRACT_ID first letter (works even without directory)
    if "CONTRACT_ID" in df.columns:
        # Prefer directory plan type if available and well-populated
        if "PLAN_TYPE_DIR" in df.columns and df["PLAN_TYPE_DIR"].notna().mean() > 0.5:
            df["CONTRACT_TYPE"] = df["PLAN_TYPE_DIR"]
        else:
            df["CONTRACT_TYPE"] = derive_contract_type(df["CONTRACT_ID"])

    # ── Debug expander ────────────────────────────────────────────────────────
    total_rows     = len(df)
    matched_rows   = df["PARENT_ORGANIZATION"].notna().sum()
    unmatched_rows = total_rows - matched_rows
    match_pct      = matched_rows / total_rows * 100 if total_rows > 0 else 0

    with st.expander(
        f"🔍 Parent Org Join Diagnostics  "
        f"({matched_rows:,} / {total_rows:,} rows matched  —  {match_pct:.1f}%)",
        expanded=(match_pct < 50),
    ):
        st.markdown(
            f"- **Enrollment rows:** {total_rows:,}\n"
            f"- **Matched to a parent org:** {matched_rows:,} ({match_pct:.1f}%)\n"
            f"- **Unmatched (no parent org):** {unmatched_rows:,}\n"
            f"- **Unique contract IDs in enrollment data:** "
            f"{df['CONTRACT_ID'].nunique():,}\n"
            f"- **Unique contract IDs in plan directory:** "
            f"{plan_dir['CONTRACT_ID'].nunique():,}"
        )
        # Show raw column names from both files — key for diagnosing join mismatches
        st.markdown("**Plan Directory raw columns:**")
        st.code(", ".join(_plan_dir_raw_columns) if _plan_dir_raw_columns else "(not loaded)")
        st.markdown(
            f"- Contract col detected as: `{_plan_dir_contract_col}`\n"
            f"- Parent org col detected as: `{_plan_dir_parent_col}`"
        )
        enroll_cols = [c for c in df.columns if c not in ["PARENT_ORGANIZATION", "PLAN_TYPE_DIR", "CONTRACT_TYPE"]]
        st.markdown("**Enrollment file columns:**")
        st.code(", ".join(enroll_cols))

        if unmatched_rows > 0:
            unmatched_ids = (
                df[df["PARENT_ORGANIZATION"].isna()]["CONTRACT_ID"]
                .value_counts().head(20).reset_index()
            )
            unmatched_ids.columns = ["CONTRACT_ID_IN_ENROLLMENT", "ROW_COUNT"]
            st.markdown("**Top unmatched contract IDs (in enrollment but not in directory):**")
            st.dataframe(unmatched_ids, use_container_width=True)
            dir_sample    = plan_dir["CONTRACT_ID"].dropna().head(10).tolist()
            enroll_sample = df["CONTRACT_ID"].dropna().head(10).tolist()
            st.markdown(f"**Sample IDs from plan directory:** `{dir_sample}`")
            st.markdown(f"**Sample IDs from enrollment data:** `{enroll_sample}`")

        st.markdown("**Top 10 parent orgs by enrollment (post-consolidation):**")
        top_orgs = (
            df[df["PARENT_ORGANIZATION"].notna()]
            .groupby("PARENT_ORGANIZATION")["ENROLLMENT"].sum()
            .sort_values(ascending=False).head(10)
            .reset_index()
        )
        top_orgs.columns = ["Parent Organization", "Total Enrollment"]
        top_orgs["Total Enrollment"] = top_orgs["Total Enrollment"].apply(lambda x: f"{x:,.0f}")
        st.dataframe(top_orgs, use_container_width=True)
else:
    has_parent_org = False
    # Still derive contract type from CONTRACT_ID even without the directory
    if "CONTRACT_ID" in df.columns:
        df["CONTRACT_TYPE"] = derive_contract_type(df["CONTRACT_ID"])

# ── Additional sidebar filters ────────────────────────────────────────────────
# All filters default to [] (empty = no filter applied = show everything).
# Streamlit multiselects natively include a search bar when typing into them.
periods_loaded = sorted(df["REPORT_PERIOD"].dropna().unique())

st.sidebar.markdown("---")
st.sidebar.caption(
    "All filters below default to **unset** (= all data shown). "
    "Type to search within any filter."
)

# ── Parent Organization ───────────────────────────────────────────────────────
if has_parent_org:
    all_parents = sorted(df["PARENT_ORGANIZATION"].dropna().unique())
    selected_parents = st.sidebar.multiselect(
        "🏢 Parent Organization",
        options=all_parents,
        default=[],
        placeholder="Search or select parent org(s)…",
        help="Leave empty to include all. Type to search.",
    )
else:
    selected_parents = []
    st.sidebar.info("Parent organization data unavailable — Plan Directory could not be loaded.")

# ── State(s) — scoped by parent org if one is selected ───────────────────────
if "STATE" in df.columns:
    if selected_parents:
        states_in_scope = sorted(
            df[df["PARENT_ORGANIZATION"].isin(selected_parents)]["STATE"]
            .dropna().unique()
        )
    else:
        states_in_scope = sorted(df["STATE"].dropna().unique())

    selected_states = st.sidebar.multiselect(
        "🗺️ State(s)",
        options=states_in_scope,
        default=[],
        placeholder="Search or select state(s)…",
        help="Leave empty to include all. Narrows automatically when a parent org is selected.",
    )
    st.sidebar.caption(f"{len(states_in_scope):,} state(s) available")
else:
    selected_states = []

# ── Contract Type — scoped by parent org if one is selected ──────────────────
if "CONTRACT_TYPE" in df.columns:
    scope_df = df[df["PARENT_ORGANIZATION"].isin(selected_parents)] if selected_parents else df
    types_in_scope = sorted(scope_df["CONTRACT_TYPE"].dropna().unique())

    selected_contract_types = st.sidebar.multiselect(
        "📄 Contract Type",
        options=types_in_scope,
        default=[],
        placeholder="Search or select contract type(s)…",
        help="Leave empty to include all. HMO, Regional PPO, PDP, etc.",
    )
else:
    selected_contract_types = []

# ── Contract / Plan — scoped by all filters above ────────────────────────────
if "CONTRACT_NAME" in df.columns:
    contract_col_for_filter = "CONTRACT_NAME"
elif "CONTRACT_ID" in df.columns:
    contract_col_for_filter = "CONTRACT_ID"
else:
    contract_col_for_filter = None

if contract_col_for_filter:
    scope_df = df.copy()
    if selected_parents:
        scope_df = scope_df[scope_df["PARENT_ORGANIZATION"].isin(selected_parents)]
    if selected_states:
        scope_df = scope_df[scope_df["STATE"].isin(selected_states)]
    if selected_contract_types:
        scope_df = scope_df[scope_df["CONTRACT_TYPE"].isin(selected_contract_types)]

    contracts_in_scope = sorted(scope_df[contract_col_for_filter].dropna().unique())

    selected_contracts = st.sidebar.multiselect(
        "📋 Contract / Plan",
        options=contracts_in_scope,
        default=[],
        placeholder="Search or select contract(s)…",
        help=f"Leave empty to include all. {len(contracts_in_scope):,} contracts available.",
    )
    st.sidebar.caption(f"{len(contracts_in_scope):,} contracts available")
else:
    selected_contracts = []

# ── Apply filters ─────────────────────────────────────────────────────────────
# Empty selection on any filter = that filter is inactive (all rows pass through)
mask = df["REPORT_PERIOD"].isin(periods_loaded)

if selected_parents and has_parent_org:
    mask &= df["PARENT_ORGANIZATION"].isin(selected_parents)

if selected_contract_types and "CONTRACT_TYPE" in df.columns:
    mask &= df["CONTRACT_TYPE"].isin(selected_contract_types)

if selected_states and "STATE" in df.columns:
    mask &= df["STATE"].isin(selected_states)

if selected_contracts and contract_col_for_filter and contract_col_for_filter in df.columns:
    mask &= df[contract_col_for_filter].isin(selected_contracts)

filtered = df[mask].copy()

# ── Comparison period selectors (sidebar, rendered now that periods_loaded is known) ──
# Use the pre-filter periods_loaded so options are stable regardless of other filters
_all_periods_for_compare = sorted(df["REPORT_PERIOD"].dropna().unique())

compare_current = st.sidebar.selectbox(
    "Current period",
    options=_all_periods_for_compare,
    index=len(_all_periods_for_compare) - 1,
    help="The 'current' period shown in KPIs and Period-over-Period tab",
)

# Build prior period options — always the full list so index never goes out of bounds
_prior_options = [p for p in _all_periods_for_compare if p != compare_current]
_prior_default = max(0, len(_prior_options) - 1)  # safe index regardless of list length

compare_previous = st.sidebar.selectbox(
    "Compare to period",
    options=_prior_options,
    index=_prior_default,
    help="The 'prior' period to compare against",
) if _prior_options else None

latest_period   = compare_current
previous_period = compare_previous

# ── KPI row ───────────────────────────────────────────────────────────────────
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
    + (f"  ·  **{len(selected_parents)}** parent org(s) selected" if has_parent_org and selected_parents else "")
)

c1, c2, c3, c4 = st.columns(4)
c1.metric("Current Period",   latest_period)
c2.metric("Total Enrollment", f"{latest_enroll:,.0f}",
          help="CMS suppresses county-level counts under 11 enrollees. "
               "Those rows are estimated at 5 each and included in this total.")
c3.metric(
    f"Change vs {previous_period}" if previous_period else "Period Change",
    f"{mom_delta:+,.0f}" if mom_delta is not None else "—",
    delta=f"{mom_pct:+.2f}%" if mom_pct is not None else None,
)
c4.metric("Periods Loaded", len(periods_loaded))
st.divider()

# ── Tabs ──────────────────────────────────────────────────────────────────────
tab_labels = ["📈 Enrollment Trend", "🗺️ By State / County",
              "📋 By Contract / Plan", "🔄 Month-over-Month"]
if has_parent_org:
    tab_labels.append("🏢 By Parent Organization")

tabs = st.tabs(tab_labels)

# Tab 1 ── Trend
with tabs[0]:
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
with tabs[1]:
    if "STATE" not in df.columns:
        st.info("No STATE column found in this dataset.")
    else:
        options = ["STATE"] + (["COUNTY"] if "COUNTY" in df.columns else [])
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
with tabs[2]:
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

# Tab 4 ── Period-over-Period
with tabs[3]:
    group_choices = [c for c in ["PARENT_ORGANIZATION", "STATE", "CONTRACT_NAME", "CONTRACT_ID"] if c in df.columns]
    if not group_choices:
        st.info("No grouping column available.")
    elif not previous_period:
        st.info("Select a 'Compare to period' in the sidebar to enable period-over-period analysis.")
    else:
        st.caption(
            f"Comparing **{latest_period}** (current) vs **{previous_period}** (prior). "
            "Change the comparison periods in the sidebar under **Compare Periods**."
        )
        group_choice = st.selectbox("Compare by", group_choices)

        curr = filtered[filtered["REPORT_PERIOD"] == latest_period].groupby(group_choice)["ENROLLMENT"].sum()
        prev = filtered[filtered["REPORT_PERIOD"] == previous_period].groupby(group_choice)["ENROLLMENT"].sum()
        mom  = pd.DataFrame({"CURRENT": curr, "PREVIOUS": prev}).dropna()
        mom["CHANGE"]   = mom["CURRENT"] - mom["PREVIOUS"]
        mom["CHANGE_%"] = (mom["CHANGE"] / mom["PREVIOUS"] * 100).round(2)
        mom = mom.reset_index().sort_values("CHANGE", ascending=False)

        fmt = {"PREVIOUS": "{:,.0f}", "CURRENT": "{:,.0f}", "CHANGE": "{:+,.0f}", "CHANGE_%": "{:+.2f}%"}
        cols_show = [group_choice, "PREVIOUS", "CURRENT", "CHANGE", "CHANGE_%"]
        col_a, col_b = st.columns(2)
        with col_a:
            st.subheader(f"📈 Biggest Gainers  ({previous_period} → {latest_period})")
            st.dataframe(mom.head(15)[cols_show].style.format(fmt), use_container_width=True)
        with col_b:
            st.subheader(f"📉 Biggest Decliners  ({previous_period} → {latest_period})")
            st.dataframe(mom.tail(15)[cols_show].sort_values("CHANGE").style.format(fmt), use_container_width=True)

        fig4 = px.bar(
            mom.head(20), x=group_choice, y="CHANGE",
            title=f"Top 20 Change by {group_choice.replace('_',' ').title()}  ({previous_period} → {latest_period})",
            color="CHANGE", color_continuous_scale="RdYlGn", color_continuous_midpoint=0,
        )
        st.plotly_chart(fig4, use_container_width=True)

# Tab 5 ── By Parent Organization (only shown if data available)
if has_parent_org:
    with tabs[4]:
        st.subheader(f"Enrollment by Parent Organization — {latest_period}")

        parent_enroll = (
            filtered[filtered["REPORT_PERIOD"] == latest_period]
            .groupby("PARENT_ORGANIZATION")["ENROLLMENT"].sum()
            .reset_index().sort_values("ENROLLMENT", ascending=False)
        )

        top_n_p = st.slider("Show top N parent orgs", 5, 30, 15, key="parent_slider")

        fig5 = px.bar(
            parent_enroll.head(top_n_p),
            x="ENROLLMENT", y="PARENT_ORGANIZATION", orientation="h",
            title=f"Top {top_n_p} Parent Organizations by Enrollment — {latest_period}",
            color="ENROLLMENT", color_continuous_scale="Purples",
        )
        fig5.update_layout(yaxis={"categoryorder": "total ascending"})
        st.plotly_chart(fig5, use_container_width=True)

        # Trend by parent org over time
        if len(periods_loaded) > 1:
            st.subheader("Enrollment Trend by Parent Organization")
            top_parents_list = parent_enroll.head(10)["PARENT_ORGANIZATION"].tolist()
            trend_parent = (
                filtered[filtered["PARENT_ORGANIZATION"].isin(top_parents_list)]
                .groupby(["REPORT_PERIOD", "PARENT_ORGANIZATION"])["ENROLLMENT"]
                .sum().reset_index()
            )
            fig6 = px.line(
                trend_parent, x="REPORT_PERIOD", y="ENROLLMENT",
                color="PARENT_ORGANIZATION", markers=True,
                title="Top 10 Parent Organizations — Enrollment Over Time",
                labels={"REPORT_PERIOD": "Period", "ENROLLMENT": "Enrollees",
                        "PARENT_ORGANIZATION": "Parent Org"},
            )
            fig6.update_layout(hovermode="x unified")
            st.plotly_chart(fig6, use_container_width=True)

        # Market share pie
        fig7 = px.pie(
            parent_enroll.head(10),
            values="ENROLLMENT", names="PARENT_ORGANIZATION",
            title=f"Market Share — Top 10 Parent Organizations ({latest_period})",
        )
        fig7.update_traces(textposition="inside", textinfo="percent+label")
        st.plotly_chart(fig7, use_container_width=True)

        with st.expander("View full parent organization table"):
            st.dataframe(parent_enroll, use_container_width=True)

st.divider()
st.caption(
    "Data sources: "
    "[CMS Monthly MA Enrollment by State/County/Contract]"
    "(https://www.cms.gov/data-research/statistics-trends-and-reports/"
    "medicare-advantagepart-d-contract-and-enrollment-data/"
    "monthly-ma-enrollment-state/county/contract)"
    " · "
    "[CMS MA Plan Directory]"
    "(https://www.cms.gov/data-research/statistics-trends-and-reports/"
    "medicare-advantagepart-d-contract-and-enrollment-data/ma-plan-directory)"
)
