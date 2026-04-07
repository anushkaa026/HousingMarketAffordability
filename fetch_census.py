"""
fetch_census.py
Fetches median household income, median gross rent, and population
from the Census Bureau ACS 5-Year Estimates API at the county level.

API key: Free at https://api.census.gov/data/key_signup.html
Rate limit: 500 requests/day without key; no hard limit with key.
"""

import requests
import pandas as pd
import os
import time

RAW_DIR = "data/raw"
SAMPLE_DIR = "data/samples"

CENSUS_API_KEY = os.getenv("CENSUS_API_KEY", "")

VARIABLES = {
    "B19013_001E": "median_household_income",
    "B25064_001E": "median_gross_rent",
    "B01003_001E": "population",
    "NAME": "county_name",
}

YEARS = [2019, 2020, 2021, 2022, 2023]


def fetch_county_data(year: int, variables: dict, api_key: str) -> pd.DataFrame:
    """
    Fetches ACS 5-year county-level data for a given year.

    Args:
        year: The ACS survey year (e.g. 2022).
        variables: Dict mapping Census variable codes to readable column names.
        api_key: Census Bureau API key.

    Returns:
        DataFrame with one row per county and renamed columns.
    """
    var_str = ",".join(variables.keys())
    url = f"https://api.census.gov/data/{year}/acs/acs5"
    params = {"get": var_str, "for": "county:*", "key": api_key}

    print(f"  Fetching Census ACS5 {year}...")
    resp = requests.get(url, params=params, timeout=30)
    resp.raise_for_status()

    data = resp.json()
    df = pd.DataFrame(data[1:], columns=data[0])
    df = df.rename(columns=variables)
    df["year"] = year
    df["fips"] = df["state"] + df["county"]

    for col in ["median_household_income", "median_gross_rent", "population"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    return df


def fetch_all_years() -> pd.DataFrame:
    """
    Iterates over all target years and concatenates results.

    Returns:
        Combined DataFrame across all years.
    """
    frames = []
    for year in YEARS:
        df = fetch_county_data(year, VARIABLES, CENSUS_API_KEY)
        frames.append(df)
        time.sleep(0.5)
    return pd.concat(frames, ignore_index=True)


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
    print("Fetching Census ACS5 data...")
    df = fetch_all_years()
    print(f"  Total rows fetched: {len(df)}")
    save_sample(df, "census_acs5")


if __name__ == "__main__":
    main()