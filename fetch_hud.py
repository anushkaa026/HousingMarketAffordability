"""
fetch_hud.py
Downloads HUD Fair Market Rents county-level data from HUD's ArcGIS
open data portal. No API key or token required — public CSV endpoint.

Source: https://hudgis-hud.opendata.arcgis.com
Rate limit: None (static file download).
"""

import requests
import pandas as pd
import os
from io import StringIO

RAW_DIR = "data/raw"
SAMPLE_DIR = "data/samples"

# HUD open data ArcGIS REST endpoint — returns all FMR counties as CSV
# This is the stable public export URL, no auth required
HUD_FMR_URL = (
    "https://services.arcgis.com/VTyQ9soqVukalItT/arcgis/rest/services/"
    "Fair_Market_Rents/FeatureServer/0/query"
    "?where=1%3D1&outFields=*&f=json&resultRecordCount=5000"
)

# Backup: direct CSV from HUD open data portal
HUD_FMR_CSV_URL = (
    "https://hudgis-hud.opendata.arcgis.com/datasets/"
    "HUD::fair-market-rents.csv"
)

KEY_COLS = [
    "FIPS", "County_Name", "State_Alpha",
    "FMR_0", "FMR_1", "FMR_2", "FMR_3", "FMR_4",
    "Fiscal_Year_2025"
]


def fetch_hud_csv(url: str) -> pd.DataFrame:
    """
    Downloads HUD FMR data from a public CSV URL.

    Args:
        url: Direct CSV download URL from HUD open data portal.

    Returns:
        DataFrame with standardized column names.
    """
    print(f"  Downloading HUD FMR data...")
    resp = requests.get(url, timeout=60)
    resp.raise_for_status()

    df = pd.read_csv(StringIO(resp.text))
    print(f"  {len(df)} rows, columns: {list(df.columns[:8])}")

    # Normalize column names
    df.columns = [c.strip().lower() for c in df.columns]

    # Ensure FIPS is zero-padded
    if "fips" in df.columns:
        df["fips"] = df["fips"].astype(str).str.zfill(5)

    return df


def fetch_hud_arcgis() -> pd.DataFrame:
    """
    Fetches HUD FMR data via ArcGIS REST API with pagination.

    Returns:
        DataFrame of all FMR records.
    """
    print("  Fetching from ArcGIS REST endpoint...")
    all_features = []
    offset = 0
    batch = 2000

    while True:
        params = {
            "where": "1=1",
            "outFields": "*",
            "f": "json",
            "resultRecordCount": batch,
            "resultOffset": offset,
        }
        base = (
            "https://services.arcgis.com/VTyQ9soqVukalItT/arcgis/rest/services/"
            "Fair_Market_Rents/FeatureServer/0/query"
        )
        resp = requests.get(base, params=params, timeout=60)
        resp.raise_for_status()

        data = resp.json()
        features = data.get("features", [])
        if not features:
            break

        for f in features:
            all_features.append(f["attributes"])

        print(f"  Fetched {len(all_features)} records so far...")

        if not data.get("exceededTransferLimit", False):
            break
        offset += batch

    df = pd.DataFrame(all_features)
    df.columns = [c.strip().lower() for c in df.columns]
    return df


def save_sample(df: pd.DataFrame, name: str) -> None:
    """
    Saves full dataset to raw/ and first 100 rows to samples/.

    Args:
        df: Full DataFrame to save.
        name: Identifier used in output filenames.
    """
    os.makedirs(RAW_DIR, exist_ok=True)
    os.makedirs(SAMPLE_DIR, exist_ok=True)

    df.to_csv(os.path.join(RAW_DIR, f"{name}.csv"), index=False)
    df.head(100).to_csv(os.path.join(SAMPLE_DIR, f"{name}_sample.csv"), index=False)
    print(f"  Saved raw    → data/raw/{name}.csv")
    print(f"  Saved sample → data/samples/{name}_sample.csv")


def main():
    print("Fetching HUD Fair Market Rents...")
    try:
        df = fetch_hud_arcgis()
    except Exception as e:
        print(f"  ArcGIS fetch failed ({e}), trying direct CSV...")
        df = fetch_hud_csv(HUD_FMR_CSV_URL)

    print(f"  Total rows: {len(df)}")
    save_sample(df, "hud_fmr")


if __name__ == "__main__":
    main()