"""
fetch_zillow.py
Fetches Zillow Home Value Index (ZHVI) and Observed Rent Index (ZORI)
static CSV files and saves the first 100 rows as samples.
No API key required.
"""

import pandas as pd
import os

RAW_DIR = "data/raw"
SAMPLE_DIR = "data/samples"

ZILLOW_URLS = {
    "zhvi_metro": "https://files.zillowstatic.com/research/public_csvs/zhvi/Metro_zhvi_uc_sfrcondo_tier_0.33_0.67_sm_sa_month.csv",
    "zori_metro": "https://files.zillowstatic.com/research/public_csvs/zori/Metro_zori_uc_sfrcondomfr_sm_month.csv",
}


def fetch_zillow(name: str, url: str) -> pd.DataFrame:
    """
    Downloads a Zillow CSV from a static URL.

    Args:
        name: Identifier string for the dataset (used in filenames).
        url: Direct URL to the Zillow CSV file.

    Returns:
        DataFrame containing the full Zillow dataset.
    """
    print(f"Fetching Zillow {name}...")
    df = pd.read_csv(url)
    print(f"  {len(df)} rows, {len(df.columns)} columns")
    return df


def save_sample(df: pd.DataFrame, name: str) -> None:
    """
    Saves the first 100 rows of a DataFrame as a sample CSV.

    Args:
        df: The full DataFrame to sample.
        name: Identifier used in the output filename.
    """
    os.makedirs(RAW_DIR, exist_ok=True)
    os.makedirs(SAMPLE_DIR, exist_ok=True)

    raw_path = os.path.join(RAW_DIR, f"{name}.csv")
    sample_path = os.path.join(SAMPLE_DIR, f"{name}_sample.csv")

    df.to_csv(raw_path, index=False)
    df.head(100).to_csv(sample_path, index=False)
    print(f"  Saved raw → {raw_path}")
    print(f"  Saved sample → {sample_path}")


def main():
    for name, url in ZILLOW_URLS.items():
        df = fetch_zillow(name, url)
        save_sample(df, name)


if __name__ == "__main__":
    main()