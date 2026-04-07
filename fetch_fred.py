"""
fetch_fred.py
Fetches housing-related time series from the FRED API
(Federal Reserve Bank of St. Louis).

API key: Free at https://fred.stlouisfed.org/docs/api/api_key.html
Rate limit: 120 requests/minute.
"""

import requests
import pandas as pd
import os
import time

RAW_DIR = "data/raw"
SAMPLE_DIR = "data/samples"

FRED_API_KEY = os.getenv("FRED_API_KEY", "d42f0eeb6a0b0884dd1714fb2fd293d5")

SERIES = {
    "MORTGAGE30US": "mortgage_rate_30yr",
    "CSUSHPINSA":   "case_shiller_home_price_index",
    "CUSR0000SEHA": "cpi_rent_primary_residence",
    "HOUST":        "housing_starts_thousands",
}

START_DATE = "2018-01-01"
END_DATE   = "2024-12-31"


def fetch_series(series_id: str, label: str, api_key: str) -> pd.DataFrame:
    """
    Fetches a single FRED time series between START_DATE and END_DATE.

    Args:
        series_id: FRED series identifier (e.g. 'MORTGAGE30US').
        label: Human-readable column name for the value field.
        api_key: FRED API key.

    Returns:
        DataFrame with columns [date, <label>, series_id].
    """
    url = "https://api.stlouisfed.org/fred/series/observations"
    params = {
        "series_id":         series_id,
        "api_key":           api_key,
        "file_type":         "json",
        "observation_start": START_DATE,
        "observation_end":   END_DATE,
    }

    print(f"  Fetching FRED series {series_id}...")
    resp = requests.get(url, params=params, timeout=30)
    resp.raise_for_status()

    observations = resp.json().get("observations", [])
    df = pd.DataFrame(observations)[["date", "value"]]
    df = df.rename(columns={"value": label})
    df["series_id"] = series_id
    df[label] = pd.to_numeric(df[label], errors="coerce")
    df["date"] = pd.to_datetime(df["date"])
    return df


def fetch_all_series() -> pd.DataFrame:
    """
    Fetches all target FRED series and merges them on date.

    Returns:
        Wide-format DataFrame with one column per series, indexed by date.
    """
    frames = []
    for series_id, label in SERIES.items():
        df = fetch_series(series_id, label, FRED_API_KEY)
        df = df[["date", label]]
        frames.append(df)
        time.sleep(0.6)  # stay under 120 req/min

    merged = frames[0]
    for df in frames[1:]:
        merged = pd.merge(merged, df, on="date", how="outer")

    merged = merged.sort_values("date").reset_index(drop=True)
    return merged


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
    print(f"  Saved raw → data/raw/{name}.csv")
    print(f"  Saved sample → data/samples/{name}_sample.csv")


def main():
    print("Fetching FRED data...")
    df = fetch_all_series()
    print(f"  Total rows fetched: {len(df)}")
    save_sample(df, "fred")


if __name__ == "__main__":
    main()