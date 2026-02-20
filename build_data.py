#!/usr/bin/env python3
"""
CMS MA Enrollment Data Builder
================================
Downloads 24 months of enrollment data + MA Plan Directory,
joins them, and writes data/enrollment.parquet to the repo.

Run by GitHub Actions monthly. The dashboard reads this file
directly instead of hitting CMS at runtime.
"""

import os, re, io, zipfile, logging
import requests, pandas as pd
from datetime import date
from bs4 import BeautifulSoup

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
RAW_DIR        = "raw"
OUT_DIR        = "data"
ROLLING_MONTHS = 24

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)s  %(message)s")
log = logging.getLogger(__name__)

HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0"}
SESSION = requests.Session()
SESSION.headers.update(HEADERS)

SKIP_RE = re.compile(r"(read_?me|readme|__macosx|\.ds_store)", re.I)
DATA_RE = re.compile(r"(scc|enrollment|enroll|ma_|plan_dir|directory)", re.I)

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


def _fetch_zip_df(zip_url):
    try:
        r = SESSION.get(zip_url, timeout=120)
        r.raise_for_status()
    except Exception as e:
        log.error("Download failed: %s", e)
        return None
    try:
        with zipfile.ZipFile(io.BytesIO(r.content)) as zf:
            candidates = [n for n in zf.namelist()
                          if re.search(r"\.(csv|txt)$", n, re.I)
                          and not SKIP_RE.search(n) and not n.endswith("/")]
            if not candidates:
                candidates = [n for n in zf.namelist()
                              if re.search(r"\.(csv|txt)$", n, re.I)
                              and not n.endswith("/") and "read" not in n.lower()]
            if not candidates:
                return None
            preferred = [n for n in candidates if DATA_RE.search(n)]
            raw_bytes = zf.read(preferred[0] if preferred else candidates[0])
    except zipfile.BadZipFile:
        return None

    for enc in ("utf-8-sig", "latin-1", "cp1252"):
        for sep in (",", "\t", "|"):
            try:
                df = pd.read_csv(io.BytesIO(raw_bytes), dtype=str,
                                 encoding=enc, sep=sep, low_memory=False)
                if df.shape[1] >= 2:
                    df.columns = [c.strip().upper().replace(" ", "_") for c in df.columns]
                    return df
            except Exception:
                continue
    return None


def _get_zip_url(page_url):
    try:
        r = SESSION.get(page_url, timeout=30)
        r.raise_for_status()
    except Exception:
        return None
    for a in BeautifulSoup(r.text, "html.parser").find_all("a", href=True):
        if a["href"].lower().endswith(".zip"):
            return a["href"] if a["href"].startswith("http") else CMS_BASE_URL + a["href"]
    return None


def load_plan_directory():
    log.info("Loading MA Plan Directory …")
    zip_url = _get_zip_url(CMS_PLAN_DIR_URL)
    if not zip_url:
        log.warning("Plan directory ZIP not found.")
        return pd.DataFrame(columns=["CONTRACT_ID", "PARENT_ORGANIZATION"])
    df = _fetch_zip_df(zip_url)
    if df is None or df.empty:
        return pd.DataFrame(columns=["CONTRACT_ID", "PARENT_ORGANIZATION"])

    contract_col = next((c for c in df.columns
                         if c in ["CONTRACT_NUMBER","CONTRACT_ID","CONTRACT_NO","CONTRACT_NBR"]), None)
    if not contract_col:
        contract_col = next((c for c in df.columns
                             if re.search(r"CONTRACT.*(NUMBER|NUM|NBR|ID|NO)", c)), None)
    if not contract_col:
        for c in df.columns:
            if df[c].dropna().head(30).astype(str).str.match(r"^[A-Z]\d{4}$").sum() >= 5:
                contract_col = c
                break

    parent_col = next((c for c in df.columns if c == "PARENT_ORGANIZATION"), None)
    if not parent_col:
        parent_col = next((c for c in df.columns if "PARENT" in c and "ORG" in c), None)
    if not parent_col:
        parent_col = next((c for c in df.columns if "PARENT" in c), None)

    type_col = next((c for c in df.columns
                     if re.search(r"(PLAN|CONTRACT).*(TYPE|TYP)", c)), None)

    if not contract_col or not parent_col:
        log.warning("Could not find contract or parent org column. Cols: %s", list(df.columns))
        return pd.DataFrame(columns=["CONTRACT_ID", "PARENT_ORGANIZATION"])

    keep = [contract_col, parent_col]
    rename = {contract_col: "CONTRACT_ID", parent_col: "PARENT_ORGANIZATION"}
    if type_col:
        keep.append(type_col)
        rename[type_col] = "PLAN_TYPE_DIR"

    lookup = (df[keep].rename(columns=rename)
              .dropna(subset=["CONTRACT_ID"])
              .drop_duplicates(subset=["CONTRACT_ID"]))
    lookup["CONTRACT_ID"] = lookup["CONTRACT_ID"].astype(str).str.strip().str.upper().str.replace(r"[^A-Z0-9]", "", regex=True)
    lookup["PARENT_ORGANIZATION"] = (lookup["PARENT_ORGANIZATION"].astype(str).str.strip().str.title()
                                     .map(PARENT_ORG_CONSOLIDATION)
                                     .fillna(lookup["PARENT_ORGANIZATION"].astype(str).str.strip().str.title()))
    if "PLAN_TYPE_DIR" in lookup.columns:
        lookup["PLAN_TYPE_DIR"] = lookup["PLAN_TYPE_DIR"].str.strip().str.title()
    log.info("Plan directory: %d contracts loaded.", len(lookup))
    return lookup


def get_periods():
    r = SESSION.get(CMS_INDEX_URL, timeout=30)
    r.raise_for_status()
    links = {}
    for a in BeautifulSoup(r.text, "html.parser").find_all("a", href=True):
        m = re.search(r"ma-enrollment-scc-(\d{4}-\d{2})$", a["href"])
        if m:
            period = m.group(1)
            links[period] = a["href"] if a["href"].startswith("http") else CMS_BASE_URL + a["href"]
    log.info("Found %d periods.", len(links))
    return links


def download_period(period, subpage_url):
    os.makedirs(RAW_DIR, exist_ok=True)
    cache = os.path.join(RAW_DIR, f"ma_scc_{period}.csv")
    if os.path.exists(cache):
        log.info("  [cache] %s", period)
        return pd.read_csv(cache, dtype=str, low_memory=False)
    zip_url = _get_zip_url(subpage_url)
    if not zip_url:
        return None
    df = _fetch_zip_df(zip_url)
    if df is not None:
        df.to_csv(cache, index=False)
    return df


def normalise(df, period):
    df = df.copy()
    rename = {}
    for col in df.columns:
        if col in ("REPORT_PERIOD",):
            continue
        if "STATE" in col and "FIPS" not in col and "STATE" not in rename.values():
            rename[col] = "STATE"
        elif "COUNTY" in col and "FIPS" not in col and "STATE" not in col and "COUNTY" not in rename.values():
            rename[col] = "COUNTY"
        elif re.search(r"CONTRACT.*(NUMBER|ID|NBR|NUM)", col) and "CONTRACT_ID" not in rename.values():
            rename[col] = "CONTRACT_ID"
        elif re.search(r"(CONTRACT|ORG).*(NAME|NM)", col) and "CONTRACT_NAME" not in rename.values():
            rename[col] = "CONTRACT_NAME"
        elif re.search(r"^ENROLLED$", col) and "ENROLLMENT" not in rename.values():
            rename[col] = "ENROLLMENT"
        elif re.search(r"ENROLL", col) and "REPORT" not in col and "ENROLLMENT" not in rename.values():
            rename[col] = "ENROLLMENT"
    df = df.rename(columns=rename)
    df.insert(0, "REPORT_PERIOD", period)

    if "ENROLLMENT" in df.columns:
        raw = df["ENROLLMENT"].astype(str).str.strip()
        numeric = pd.to_numeric(raw, errors="coerce")
        suppressed = numeric.isna() & raw.notna() & (raw != "") & (raw.str.lower() != "nan")
        numeric[suppressed] = 5
        df["ENROLLMENT"] = numeric

    keep = ["REPORT_PERIOD", "ENROLLMENT"]
    for c in ["STATE", "COUNTY", "CONTRACT_ID", "CONTRACT_NAME"]:
        if c in df.columns:
            keep.append(c)
    return df[keep]


def main():
    os.makedirs(OUT_DIR, exist_ok=True)

    # 1. Enrollment data
    all_periods = get_periods()
    periods = sorted(all_periods.keys(), reverse=True)[:ROLLING_MONTHS]
    frames = []
    for period in sorted(periods):
        log.info("Processing %s …", period)
        df = download_period(period, all_periods[period])
        if df is None or df.empty:
            log.warning("  Skipping %s", period)
            continue
        df.columns = [c.strip().upper().replace(" ", "_") for c in df.columns]
        frames.append(normalise(df, period))
    if not frames:
        raise RuntimeError("No enrollment data downloaded.")
    combined = pd.concat(frames, ignore_index=True)
    log.info("Enrollment rows: %d", len(combined))

    # 2. Plan directory join
    plan_dir = load_plan_directory()
    if not plan_dir.empty and "CONTRACT_ID" in combined.columns:
        combined["CONTRACT_ID"] = combined["CONTRACT_ID"].astype(str).str.strip().str.upper().str.replace(r"[^A-Z0-9]", "", regex=True)
        combined = combined.merge(plan_dir, on="CONTRACT_ID", how="left")
        if "ENROLLMENT_x" in combined.columns:
            combined = combined.rename(columns={"ENROLLMENT_x": "ENROLLMENT"}).drop(columns=["ENROLLMENT_y"], errors="ignore")

    # 3. Contract type
    if "CONTRACT_ID" in combined.columns:
        if "PLAN_TYPE_DIR" in combined.columns and combined["PLAN_TYPE_DIR"].notna().mean() > 0.5:
            combined["CONTRACT_TYPE"] = combined["PLAN_TYPE_DIR"]
        else:
            first_char = combined["CONTRACT_ID"].astype(str).str.strip().str.upper().str[0]
            combined["CONTRACT_TYPE"] = first_char.map(CONTRACT_TYPE_MAP).fillna("Other")

    # 4. Write parquet
    out_path = os.path.join(OUT_DIR, "enrollment.parquet")
    combined.to_parquet(out_path, index=False)
    log.info("Written: %s  (%d rows, %d cols)", out_path, len(combined), len(combined.columns))
    log.info("Periods: %s – %s", combined["REPORT_PERIOD"].min(), combined["REPORT_PERIOD"].max())
    print(f"\n✅  Done!  {out_path}  ({len(combined):,} rows)")


if __name__ == "__main__":
    main()
