"""
CMS MA Enrollment Dashboard
============================
Reads pre-built data/enrollment.parquet committed by GitHub Actions.
No live CMS fetching at runtime — fast, stable, no timeouts.
"""

import re
import streamlit as st
import pandas as pd
import plotly.express as px
from pathlib import Path

# ── Page config ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="CMS MA Enrollment Dashboard",
    page_icon="🏥",
    layout="wide",
)

PARQUET_PATH = Path("data/enrollment.parquet")

PARENT_ORG_CONSOLIDATION = {
    "Unitedhealthcare": "UnitedHealth Group",
    "Unitedhealth Group": "UnitedHealth Group",
    "United Healthcare": "UnitedHealth Group",
    "United Health Care": "UnitedHealth Group",
    "Aarp/Unitedhealthcare": "UnitedHealth Group",
    "Ovations": "UnitedHealth Group",
    "Pacificare": "UnitedHealth Group",
    "Sierra Health And Life": "UnitedHealth Group",
    "Americhoice": "UnitedHealth Group",
    "Cvs Health Corporation": "CVS Health / Aetna",
    "Aetna": "CVS Health / Aetna",
    "Cvs Health": "CVS Health / Aetna",
    "Aetna Inc.": "CVS Health / Aetna",
    "Humana": "Humana",
    "Humana Inc.": "Humana",
    "Humana Inc": "Humana",
    "Elevance Health": "Elevance Health",
    "Anthem": "Elevance Health",
    "Anthem, Inc.": "Elevance Health",
    "Anthem Inc": "Elevance Health",
    "Centene Corporation": "Centene",
    "Centene": "Centene",
    "Wellcare": "Centene",
    "Wellcare Health Plans": "Centene",
    "Kaiser Foundation Health Plan": "Kaiser Permanente",
    "Kaiser Foundation Health Plan, Inc": "Kaiser Permanente",
    "Kaiser": "Kaiser Permanente",
    "Cigna": "Cigna",
    "Cigna Corporation": "Cigna",
    "Cigna Healthcare": "Cigna",
    "Cigna-Healthspring": "Cigna",
    "Molina Healthcare": "Molina Healthcare",
    "Molina Healthcare, Inc": "Molina Healthcare",
    "Scan Health Plan": "SCAN Health Plan",
    "Upmc Health Plan": "UPMC Health Plan",
}

CONTRACT_TYPE_MAP = {
    "H": "Local MA / HMO / Cost / PACE",
    "R": "Regional PPO",
    "S": "Standalone PDP",
    "E": "Employer / Union Direct",
    "9": "Other / Demo",
}


# ── Load data ─────────────────────────────────────────────────────────────────
@st.cache_data(show_spinner=False)
def load_data() -> pd.DataFrame:
    if not PARQUET_PATH.exists():
        return pd.DataFrame()
    df = pd.read_parquet(PARQUET_PATH)
    # Re-apply consolidation in case parquet was built before latest map
    if "PARENT_ORGANIZATION" in df.columns:
        title = df["PARENT_ORGANIZATION"].astype(str).str.strip().str.title()
        df["PARENT_ORGANIZATION"] = title.map(PARENT_ORG_CONSOLIDATION).fillna(title)
    return df


# ── Sidebar ───────────────────────────────────────────────────────────────────
st.sidebar.title("CMS MA Enrollment")

with st.sidebar:
    with st.spinner("Loading data…"):
        df_full = load_data()

if df_full.empty:
    st.error(
        "**No data file found.**\n\n"
        "Run the GitHub Actions workflow (`update_enrollment.yml`) to build "
        "`data/enrollment.parquet`, then redeploy."
    )
    st.stop()

all_periods = sorted(df_full["REPORT_PERIOD"].dropna().unique())

st.sidebar.markdown("### Report Periods")
st.sidebar.caption("Start with 6 months for fastest load.")
selected_periods = st.sidebar.multiselect(
    "Report Period(s)",
    options=all_periods,
    default=all_periods[-6:],
)

if not selected_periods:
    st.info("👈 Select at least one period in the sidebar to begin.")
    st.stop()

# Scope to selected periods
df = df_full[df_full["REPORT_PERIOD"].isin(selected_periods)].copy()

# ── Sidebar filters ───────────────────────────────────────────────────────────
st.sidebar.markdown("---")
st.sidebar.caption("All filters default to unset (= all data shown). Type to search.")

has_parent_org = "PARENT_ORGANIZATION" in df.columns and df["PARENT_ORGANIZATION"].notna().any()

# Parent org
if has_parent_org:
    all_parents = sorted(df["PARENT_ORGANIZATION"].dropna().unique())
    selected_parents = st.sidebar.multiselect(
        "🏢 Parent Organization",
        options=all_parents,
        default=[],
        help="Leave empty to include all. Type to search.",
    )
else:
    selected_parents = []
    st.sidebar.info("Parent organization data unavailable.")

# State — scoped by parent org
if "STATE" in df.columns:
    if selected_parents:
        states_in_scope = sorted(df[df["PARENT_ORGANIZATION"].isin(selected_parents)]["STATE"].dropna().unique())
    else:
        states_in_scope = sorted(df["STATE"].dropna().unique())
    selected_states = st.sidebar.multiselect(
        "🗺️ State(s)",
        options=states_in_scope,
        default=[],
        help="Leave empty to include all.",
    )
    st.sidebar.caption(f"{len(states_in_scope):,} state(s) available")
else:
    selected_states = []

# Contract type
if "CONTRACT_TYPE" in df.columns:
    scope_df = df[df["PARENT_ORGANIZATION"].isin(selected_parents)] if selected_parents else df
    types_in_scope = sorted(scope_df["CONTRACT_TYPE"].dropna().unique())
    selected_contract_types = st.sidebar.multiselect(
        "📄 Contract Type",
        options=types_in_scope,
        default=[],
        help="Leave empty to include all.",
    )
else:
    selected_contract_types = []

# Contract / Plan — scoped by all above
contract_col_for_filter = next(
    (c for c in ["CONTRACT_NAME", "CONTRACT_ID"] if c in df.columns), None
)
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
        help=f"Leave empty to include all. {len(contracts_in_scope):,} available.",
    )
    st.sidebar.caption(f"{len(contracts_in_scope):,} contracts available")
else:
    selected_contracts = []

# ── Apply filters ─────────────────────────────────────────────────────────────
mask = pd.Series(True, index=df.index)
if selected_parents and has_parent_org:
    mask &= df["PARENT_ORGANIZATION"].isin(selected_parents)
if selected_contract_types and "CONTRACT_TYPE" in df.columns:
    mask &= df["CONTRACT_TYPE"].isin(selected_contract_types)
if selected_states and "STATE" in df.columns:
    mask &= df["STATE"].isin(selected_states)
if selected_contracts and contract_col_for_filter:
    mask &= df[contract_col_for_filter].isin(selected_contracts)
filtered = df[mask].copy()

# ── Period references ─────────────────────────────────────────────────────────
periods_loaded    = sorted(filtered["REPORT_PERIOD"].dropna().unique())
latest_period     = periods_loaded[-1] if periods_loaded else None
mom_period        = periods_loaded[-2] if len(periods_loaded) >= 2 else None

def _yoy(p):
    try:
        y, m = p.split("-")
        return f"{int(y)-1}-{m}"
    except Exception:
        return None

def _prior_dec(p):
    try:
        y, m = p.split("-")
        return f"{int(y)-1}-12"
    except Exception:
        return None

yoy_label      = _yoy(latest_period)      if latest_period else None
prior_dec_label = _prior_dec(latest_period) if latest_period else None

# A period is usable for comparison if it exists in the full dataset
# (not just the currently selected periods) so filters still work
all_available = set(df_full["REPORT_PERIOD"].dropna().unique())
yoy_period       = yoy_label       if yoy_label       in all_available else None
prior_dec_period = prior_dec_label if prior_dec_label in all_available else None

# ── KPI helpers ───────────────────────────────────────────────────────────────
def _filtered_enroll(period):
    """Sum enrollment for any period with current sidebar filters applied."""
    if not period:
        return None
    # Query the full dataset so comparison periods outside selected_periods work
    m = df_full["REPORT_PERIOD"] == period
    if selected_parents and has_parent_org:
        m &= df_full["PARENT_ORGANIZATION"].isin(selected_parents)
    if selected_contract_types and "CONTRACT_TYPE" in df_full.columns:
        m &= df_full["CONTRACT_TYPE"].isin(selected_contract_types)
    if selected_states and "STATE" in df_full.columns:
        m &= df_full["STATE"].isin(selected_states)
    if selected_contracts and contract_col_for_filter:
        m &= df_full[contract_col_for_filter].isin(selected_contracts)
    total = df_full[m]["ENROLLMENT"].sum()
    return total if total > 0 else None

def _delta(cur, pri):
    if cur is None or pri is None:
        return None, None
    d = cur - pri
    pct = (d / pri * 100) if pri != 0 else None
    return d, pct

latest_enroll    = _filtered_enroll(latest_period) or 0
mom_enroll       = _filtered_enroll(mom_period)
yoy_enroll       = _filtered_enroll(yoy_period)
prior_dec_enroll = _filtered_enroll(prior_dec_period)

mom_delta,       mom_pct       = _delta(latest_enroll, mom_enroll)
yoy_delta,       yoy_pct       = _delta(latest_enroll, yoy_enroll)
prior_dec_delta, prior_dec_pct = _delta(latest_enroll, prior_dec_enroll)

# ── Page header + KPIs ───────────────────────────────────────────────────────
st.title("🏥 CMS Medicare Advantage Enrollment")
st.caption(
    f"**{len(periods_loaded)}** period(s) loaded: "
    f"**{periods_loaded[0]}** – **{periods_loaded[-1]}**  ·  "
    f"{len(filtered):,} rows"
    + (f"  ·  **{len(selected_parents)}** parent org(s) selected" if selected_parents else "")
)

c1, c2, c3, c4, c5 = st.columns(5)
c1.metric("Latest Period", latest_period or "—")
c2.metric(
    "Total Enrollment", f"{latest_enroll:,.0f}",
    help="CMS suppresses county-level counts under 11 enrollees (estimated at 5 each).",
)
c3.metric(
    f"MoM ({mom_period})" if mom_period else "MoM Change",
    f"{mom_delta:+,.0f}" if mom_delta is not None else "—",
    delta=f"{mom_pct:+.2f}%" if mom_pct is not None else None,
    help=f"Change from {mom_period} to {latest_period}",
)
c4.metric(
    f"vs Prior Dec ({prior_dec_period})" if prior_dec_period else "vs Prior Dec",
    f"{prior_dec_delta:+,.0f}" if prior_dec_delta is not None else "—",
    delta=f"{prior_dec_pct:+.2f}%" if prior_dec_pct is not None else None,
    help=(f"Change from {prior_dec_period} to {latest_period}"
          if prior_dec_period else f"Needs {prior_dec_label} — trigger workflow to include it."),
)
c5.metric(
    f"YoY ({yoy_period})" if yoy_period else "YoY Change",
    f"{yoy_delta:+,.0f}" if yoy_delta is not None else "—",
    delta=f"{yoy_pct:+.2f}%" if yoy_pct is not None else None,
    help=(f"Change from {yoy_period} to {latest_period}"
          if yoy_period else f"Needs {yoy_label} — trigger workflow to include it."),
)
st.divider()

# ── Tabs ──────────────────────────────────────────────────────────────────────
tab_labels = ["📈 Enrollment Trend", "🗺️ By State / County",
              "📋 By Contract / Plan", "🔄 Month-over-Month"]
if has_parent_org:
    tab_labels.append("🏢 By Parent Organization")
tabs = st.tabs(tab_labels)

# Tab 1 — Trend
with tabs[0]:
    trend = (
        filtered.groupby("REPORT_PERIOD")["ENROLLMENT"]
        .sum().reset_index().sort_values("REPORT_PERIOD")
    )
    fig = px.line(trend, x="REPORT_PERIOD", y="ENROLLMENT", markers=True,
                  title="Total MA Enrollment Over Time",
                  labels={"REPORT_PERIOD": "Period", "ENROLLMENT": "Enrollees"})
    fig.update_layout(hovermode="x unified")
    st.plotly_chart(fig, use_container_width=True)
    with st.expander("View data table"):
        st.dataframe(trend, use_container_width=True)

# Tab 2 — State / County
with tabs[1]:
    if "STATE" not in df.columns:
        st.info("No STATE column in this dataset.")
    else:
        options = ["STATE"] + (["COUNTY"] if "COUNTY" in df.columns else [])
        geo_col = st.radio("Group by", options, horizontal=True)
        geo = (
            filtered[filtered["REPORT_PERIOD"] == latest_period]
            .groupby(geo_col)["ENROLLMENT"].sum()
            .reset_index().sort_values("ENROLLMENT", ascending=False).head(30)
        )
        fig2 = px.bar(geo, x=geo_col, y="ENROLLMENT",
                      title=f"Top 30 by {geo_col.title()} — {latest_period}",
                      color="ENROLLMENT", color_continuous_scale="Blues")
        st.plotly_chart(fig2, use_container_width=True)
        with st.expander("View full table"):
            full_geo = (
                filtered[filtered["REPORT_PERIOD"] == latest_period]
                .groupby(geo_col)["ENROLLMENT"].sum()
                .reset_index().sort_values("ENROLLMENT", ascending=False)
            )
            st.dataframe(full_geo, use_container_width=True)

# Tab 3 — Contract / Plan
with tabs[2]:
    contract_col = next((c for c in ["CONTRACT_NAME", "CONTRACT_ID"] if c in df.columns), None)
    if not contract_col:
        st.info("No contract/plan column found.")
    else:
        top_n = st.slider("Show top N contracts", 5, 50, 15)
        contracts_data = (
            filtered[filtered["REPORT_PERIOD"] == latest_period]
            .groupby(contract_col)["ENROLLMENT"].sum()
            .reset_index().sort_values("ENROLLMENT", ascending=False).head(top_n)
        )
        fig3 = px.bar(contracts_data, x="ENROLLMENT", y=contract_col, orientation="h",
                      title=f"Top {top_n} Contracts — {latest_period}",
                      color="ENROLLMENT", color_continuous_scale="Teal")
        fig3.update_layout(yaxis={"categoryorder": "total ascending"})
        st.plotly_chart(fig3, use_container_width=True)
        with st.expander("View full table"):
            all_c = (
                filtered[filtered["REPORT_PERIOD"] == latest_period]
                .groupby(contract_col)["ENROLLMENT"].sum()
                .reset_index().sort_values("ENROLLMENT", ascending=False)
            )
            st.dataframe(all_c, use_container_width=True)

# Tab 4 — Month-over-Month
with tabs[3]:
    group_choices = [c for c in ["PARENT_ORGANIZATION", "STATE", "CONTRACT_NAME", "CONTRACT_ID"]
                     if c in df.columns]
    if not group_choices:
        st.info("No grouping column available.")
    elif not mom_period:
        st.info("Select at least 2 periods to see period-over-period changes.")
    else:
        compare_options = {f"MoM  ({mom_period} → {latest_period})": mom_period}
        if prior_dec_period:
            compare_options[f"vs Prior Dec  ({prior_dec_period} → {latest_period})"] = prior_dec_period
        if yoy_period:
            compare_options[f"YoY  ({yoy_period} → {latest_period})"] = yoy_period
        compare_label = st.radio("Compare against", list(compare_options.keys()), horizontal=True)
        prior_period  = compare_options[compare_label]
        group_choice  = st.selectbox("Group by", group_choices)

        # Apply all filters to both periods for apples-to-apples comparison
        def _period_df(period):
            m = df_full["REPORT_PERIOD"] == period
            if selected_parents and has_parent_org:
                m &= df_full["PARENT_ORGANIZATION"].isin(selected_parents)
            if selected_contract_types and "CONTRACT_TYPE" in df_full.columns:
                m &= df_full["CONTRACT_TYPE"].isin(selected_contract_types)
            if selected_states and "STATE" in df_full.columns:
                m &= df_full["STATE"].isin(selected_states)
            if selected_contracts and contract_col_for_filter:
                m &= df_full[contract_col_for_filter].isin(selected_contracts)
            return df_full[m]

        curr_df = _period_df(latest_period)
        prev_df = _period_df(prior_period)

        if prev_df.empty:
            st.warning(
                f"No data found for **{prior_period}**. "
                "Trigger the GitHub Actions workflow to ensure that period is included."
            )
        else:
            curr = curr_df.groupby(group_choice)["ENROLLMENT"].sum()
            prev = prev_df.groupby(group_choice)["ENROLLMENT"].sum()
            chg  = pd.DataFrame({"CURRENT": curr, "PREVIOUS": prev}).dropna()
            chg["CHANGE"]   = chg["CURRENT"] - chg["PREVIOUS"]
            chg["CHANGE_%"] = (chg["CHANGE"] / chg["PREVIOUS"] * 100).round(2)
            chg = chg.reset_index().sort_values("CHANGE", ascending=False)

            if chg.empty:
                st.info("No overlapping records found between the two periods for the current filters.")
            else:
                fmt = {"PREVIOUS": "{:,.0f}", "CURRENT": "{:,.0f}",
                       "CHANGE": "{:+,.0f}", "CHANGE_%": "{:+.2f}%"}
                cols_show = [group_choice, "PREVIOUS", "CURRENT", "CHANGE", "CHANGE_%"]
                col_a, col_b = st.columns(2)
                with col_a:
                    st.subheader(f"📈 Biggest Gainers  ({prior_period} → {latest_period})")
                    st.dataframe(chg.head(15)[cols_show].style.format(fmt), use_container_width=True)
                with col_b:
                    st.subheader(f"📉 Biggest Decliners  ({prior_period} → {latest_period})")
                    st.dataframe(chg.tail(15)[cols_show].sort_values("CHANGE").style.format(fmt),
                                 use_container_width=True)
                fig4 = px.bar(
                    chg.head(20), x=group_choice, y="CHANGE",
                    title=f"Top 20 Change by {group_choice.replace('_',' ').title()}  ({prior_period} → {latest_period})",
                    color="CHANGE", color_continuous_scale="RdYlGn", color_continuous_midpoint=0,
                )
                st.plotly_chart(fig4, use_container_width=True)

# Tab 5 — By Parent Organization
if has_parent_org:
    with tabs[4]:
        st.subheader(f"Enrollment by Parent Organization — {latest_period}")
        parent_enroll = (
            filtered[filtered["REPORT_PERIOD"] == latest_period]
            .groupby("PARENT_ORGANIZATION")["ENROLLMENT"].sum()
            .reset_index().sort_values("ENROLLMENT", ascending=False)
        )
        top_n_p = 15
        fig5 = px.bar(
            parent_enroll.head(top_n_p),
            x="ENROLLMENT", y="PARENT_ORGANIZATION", orientation="h",
            title=f"Top {top_n_p} Parent Organizations — {latest_period}",
            color="ENROLLMENT", color_continuous_scale="Purples",
        )
        fig5.update_layout(yaxis={"categoryorder": "total ascending"})
        st.plotly_chart(fig5, use_container_width=True)

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
    "Data: [CMS Monthly MA Enrollment by State/County/Contract]"
    "(https://www.cms.gov/data-research/statistics-trends-and-reports/"
    "medicare-advantagepart-d-contract-and-enrollment-data/"
    "monthly-ma-enrollment-state/county/contract)"
    " · [CMS MA Plan Directory]"
    "(https://www.cms.gov/data-research/statistics-trends-and-reports/"
    "medicare-advantagepart-d-contract-and-enrollment-data/ma-plan-directory)"
    " · Updated monthly via GitHub Actions"
)
