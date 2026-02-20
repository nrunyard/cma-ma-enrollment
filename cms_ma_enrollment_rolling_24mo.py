#!/usr/bin/env python3
"""
CMS MA Enrollment Rolling 24-Month Data Builder
================================================
Scrapes the CMS Monthly MA Enrollment by State/County/Contract page,
downloads the most recent 24 months of data, and combines them into
a single CSV with a REPORT_PERIOD column added for tracking.

Source: https://www.cms.gov/data-research/statistics-trends-and-reports/
        medicare-advantagepart-d-contract-and-enrollment-data/
        monthly-ma-enrollment-state/county/contract

Usage:
    python cms_ma_enrollment_rolling_24mo.py

Output:
    ma_enrollment_rolling_24mo_YYYYMMDD.csv  (combined file)
    raw/                                      (individual monthly files cached)
"""

import os
import re
import io
import zipfile
import logging
import requests
import pandas as pd
from datetime import datetime, date
from dateutil.relativedelta import relativedelta
from bs4 import BeautifulSoup

# ── Configuration ──────────────────────────────────────────────────────────────
CMS_INDEX_URL = (
    "https://www.cms.gov/data-research/statistics-trends-and-reports/"
    "medicare-advantagepart-d-contract-and-enrollment-data/"
    "monthly-ma-enrollment-state/county/contract"
)
CMS_BASE_URL = "https://www.cms.gov"
OUTPUT_DIR   = "."
RAW_DIR      = os.path.join(OUTPUT_DIR, "raw")
ROLLING_MONTHS = 24

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)s  %(message)s")
log = logging.getLogger(__name__)

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    )
}

SESSION = requests.Session()
SESSION.headers.update(HEADERS)


# ── Step 1 – Scrape the index page for SCC sub-page links ─────────────────────
def get_scc_subpage_links() -> dict[str, str]:
    """
    Returns {period_str: absolute_url} for all MA Enrollment by SCC periods
    listed on the CMS index page.
    Period strings look like '2025-12'.
    """
    log.info("Fetching CMS index page …")
    r = SESSION.get(CMS_INDEX_URL, timeout=30)
    r.raise_for_status()

    soup = BeautifulSoup(r.text, "html.parser")
    links: dict[str, str] = {}

    # The table rows contain links like /…/ma-enrollment-scc-2025-12
    for a in soup.find_all("a", href=True):
        href = a["href"]
        m = re.search(r"ma-enrollment-scc-(\d{4}-\d{2})$", href)
        if m:
            period = m.group(1)
            full_url = href if href.startswith("http") else CMS_BASE_URL + href
            links[period] = full_url

    log.info("Found %d SCC period links on index page.", len(links))
    return links


# ── Step 2 – Scrape an individual SCC sub-page to find the ZIP download URL ───
def get_zip_url_from_subpage(subpage_url: str) -> str | None:
    """
    Visits the individual period sub-page and returns the ZIP download URL,
    or None if not found.
    """
    try:
        r = SESSION.get(subpage_url, timeout=30)
        r.raise_for_status()
    except requests.RequestException as e:
        log.warning("Could not fetch sub-page %s: %s", subpage_url, e)
        return None

    soup = BeautifulSoup(r.text, "html.parser")

    # Look for a link to a ZIP file
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if href.lower().endswith(".zip"):
            return href if href.startswith("http") else CMS_BASE_URL + href

    # Sometimes the file is a direct CSV/TXT link (no ZIP)
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if re.search(r"\.(csv|txt|xlsx?)$", href, re.I):
            return href if href.startswith("http") else CMS_BASE_URL + href

    log.warning("No downloadable file found on sub-page: %s", subpage_url)
    return None


# ── Step 3 – Download a ZIP and extract the data file into a DataFrame ─────────
def download_and_read(
    period: str,
    download_url: str,
) -> pd.DataFrame | None:
    """
    Downloads the ZIP (or CSV/TXT) for *period* and returns a DataFrame.
    Raw files are cached in RAW_DIR to avoid re-downloading on subsequent runs.
    """
    os.makedirs(RAW_DIR, exist_ok=True)
    cache_path = os.path.join(RAW_DIR, f"ma_scc_{period}.csv")

    if os.path.exists(cache_path):
        log.info("  [cache] %s", cache_path)
        return pd.read_csv(cache_path, dtype=str, low_memory=False)

    log.info("  Downloading %s …", download_url)
    try:
        r = SESSION.get(download_url, timeout=120)
        r.raise_for_status()
    except requests.RequestException as e:
        log.error("  Download failed for %s: %s", period, e)
        return None

    content = r.content
    filename_lower = download_url.lower()

    # Handle ZIP
    if filename_lower.endswith(".zip") or content[:2] == b"PK":
        try:
            with zipfile.ZipFile(io.BytesIO(content)) as zf:
                all_names = zf.namelist()
                log.info("  ZIP contents: %s", all_names)

                # Exclude readmes, Mac metadata, and directory entries
                SKIP_PATTERNS = re.compile(
                    r"(read_?me|readme|__macosx|\.ds_store)", re.I
                )
                candidates = [
                    n for n in all_names
                    if re.search(r"\.(csv|txt)$", n, re.I)
                    and not SKIP_PATTERNS.search(n)
                    and not n.endswith("/")
                ]

                # If filtering removed everything, fall back to ALL csv/txt
                # but still skip obvious readmes
                if not candidates:
                    candidates = [
                        n for n in all_names
                        if re.search(r"\.(csv|txt)$", n, re.I)
                        and not n.endswith("/")
                        and "read" not in n.lower()
                    ]

                if not candidates:
                    log.error(
                        "  No data CSV/TXT found inside ZIP for %s. "
                        "Contents: %s", period, all_names
                    )
                    return None

                # Prefer files whose name looks like the data file
                # (contains 'SCC', 'Enrollment', or 'MA') over generic names
                DATA_PREF = re.compile(r"(scc|enrollment|enroll|ma_)", re.I)
                preferred = [n for n in candidates if DATA_PREF.search(n)]
                data_name = preferred[0] if preferred else candidates[0]

                log.info("  Extracting data file '%s' from ZIP …", data_name)
                raw_bytes = zf.read(data_name)
        except zipfile.BadZipFile:
            log.error("  Bad ZIP file for %s", period)
            return None
    else:
        raw_bytes = content

    # Detect encoding and parse — also try tab-separated (CMS uses both)
    df = None
    for enc in ("utf-8-sig", "latin-1", "cp1252"):
        for sep in (",", "\t", "|"):
            try:
                candidate = pd.read_csv(
                    io.BytesIO(raw_bytes),
                    dtype=str,
                    encoding=enc,
                    sep=sep,
                    low_memory=False,
                )
                # Must have at least 2 columns — single-column means wrong sep
                if candidate.shape[1] >= 2:
                    df = candidate
                    log.info(
                        "  Parsed with encoding=%s sep=%r → %d cols",
                        enc, sep, df.shape[1],
                    )
                    break
            except (UnicodeDecodeError, pd.errors.ParserError):
                continue
        if df is not None:
            break

    if df is None:
        log.error("  Could not decode file for %s", period)
        return None

    # Cache the parsed data
    df.to_csv(cache_path, index=False)
    return df



# ── Step 4 – Determine the 24 most-recent available periods ───────────────────
def select_rolling_periods(
    available: dict[str, str],
    n: int = ROLLING_MONTHS,
) -> list[str]:
    """
    Sorts available periods (YYYY-MM) descending and returns the top *n*.
    """
    sorted_periods = sorted(available.keys(), reverse=True)
    selected = sorted_periods[:n]
    log.info(
        "Selected %d periods: %s … %s",
        len(selected), selected[-1], selected[0],
    )
    return selected


# ── Main ───────────────────────────────────────────────────────────────────────
def main():
    # 1. Get all available SCC sub-page links
    scc_links = get_scc_subpage_links()
    if not scc_links:
        log.error("No SCC links found. Check the CMS page structure.")
        return

    # 2. Select the rolling 24 most-recent periods
    periods_to_fetch = select_rolling_periods(scc_links, ROLLING_MONTHS)

    # 3. Download and read each period
    all_frames: list[pd.DataFrame] = []

    for period in sorted(periods_to_fetch):            # process oldest → newest
        log.info("Processing period %s …", period)
        subpage_url = scc_links[period]

        zip_url = get_zip_url_from_subpage(subpage_url)
        if not zip_url:
            log.warning("Skipping period %s (no download URL found).", period)
            continue

        df = download_and_read(period, zip_url)
        if df is None or df.empty:
            log.warning("Skipping period %s (empty or failed download).", period)
            continue

        # Normalize column names (strip whitespace, upper-case)
        df.columns = [c.strip().upper().replace(" ", "_") for c in df.columns]

        # Add a REPORT_PERIOD column for easy filtering / tracking
        df.insert(0, "REPORT_PERIOD", period)

        log.info("  Period %s: %d rows, %d cols", period, len(df), df.shape[1])
        all_frames.append(df)

    if not all_frames:
        log.error("No data downloaded. Exiting.")
        return

    # 4. Combine all periods
    log.info("Combining %d period(s) …", len(all_frames))
    combined = pd.concat(all_frames, ignore_index=True)
    log.info("Combined shape: %s", combined.shape)

    # 5. Write the rolling file
    today = date.today().strftime("%Y%m%d")
    out_name = f"ma_enrollment_rolling_24mo_{today}.csv"
    out_path = os.path.join(OUTPUT_DIR, out_name)
    combined.to_csv(out_path, index=False)

    # Print summary
    log.info("=" * 60)
    log.info("Output file : %s", out_path)
    log.info("Total rows  : %s", f"{len(combined):,}")
    log.info("Periods     : %s – %s", combined["REPORT_PERIOD"].min(),
             combined["REPORT_PERIOD"].max())
    log.info("Columns     : %s", list(combined.columns))
    log.info("=" * 60)
    print(f"\n✅  Done!  Output saved to: {out_path}")
    print(f"   Rows   : {len(combined):,}")
    print(f"   Periods: {combined['REPORT_PERIOD'].min()} – {combined['REPORT_PERIOD'].max()}")


if __name__ == "__main__":
    main()
