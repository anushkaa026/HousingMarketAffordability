"""
clean_merge.py
Loads the 5 CSVs, cleans them, and saves processed parquet files.

Run once before dashboard.py:  python clean_merge.py

Actual column names discovered from the files:
  census_acs5.csv : fips, county_name, state, county, year,
                    median_household_income, median_gross_rent, population
  fred.csv        : date, mortgage_rate_30yr, case_shiller_home_price_index,
                    cpi_rent_primary_residence, housing_starts_thousands
  hud_fmr.csv     : fmr_code, fmr_areaname, fmr_0bdr-fmr_4bdr  (metro-level, no FIPS)
  zhvi_metro.csv  : RegionID, SizeRank, RegionName, RegionType, StateName, <date cols>
  zori_metro.csv  : same structure as zhvi
"""

import os
import re
import pandas as pd

HERE    = os.path.dirname(os.path.abspath(__file__))
OUT_DIR = os.path.join(HERE, "data", "processed")
os.makedirs(OUT_DIR, exist_ok=True)


def find_csv(filename):
    """Search for a CSV next to this script, or in data/raw/ or data/.

    Args:
        filename: CSV filename to locate.

    Returns:
        Absolute path to the file.

    Raises:
        FileNotFoundError: If not found in any candidate location.
    """
    candidates = [
        os.path.join(HERE, filename),
        os.path.join(HERE, "data", "raw", filename),
        os.path.join(HERE, "data", filename),
    ]
    for path in candidates:
        if os.path.exists(path):
            return path
    raise FileNotFoundError(
        f"Could not find '{filename}'. Looked in:\n"
        + "\n".join(f"  {p}" for p in candidates)
    )


# ── 1. Census ─────────────────────────────────────────────────────────────────

def load_census():
    """Load and clean census_acs5.csv.

    Columns: fips, county_name, state, county, year,
             median_household_income, median_gross_rent, population.

    Returns:
        Cleaned county-year DataFrame.
    """
    path = find_csv("census_acs5.csv")
    print(f"  Census: {path}")
    df = pd.read_csv(path, dtype={"fips": str, "state": str, "county": str})

    df["fips"] = df["fips"].astype(str).str.zfill(5)

    for col in ["median_household_income", "median_gross_rent", "population"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
        df.loc[df[col] < 0, col] = None

    df = df.dropna(subset=["median_household_income", "fips"])
    df = df.drop_duplicates(subset=["fips", "year"])

    # Derived affordability columns
    df["rent_burden"]        = (df["median_gross_rent"] * 12) / df["median_household_income"]
    df["affordable_monthly"] = df["median_household_income"] / 12 * 0.30
    df["rent_gap"]           = df["median_gross_rent"] - df["affordable_monthly"]
    df["state_name"]         = df["county_name"].str.split(", ").str[-1]

    print(f"  → {len(df)} rows")
    return df


# ── 2. HUD FMR  (metro-level, no FIPS) ───────────────────────────────────────

def load_hud():
    """Load and clean hud_fmr.csv.

    Columns: fmr_code, fmr_areaname, fmr_0bdr, fmr_1bdr,
             fmr_2bdr, fmr_3bdr, fmr_4bdr.

    HUD data is at metro-area level (not county FIPS), so it is kept
    as a standalone table used only for the FMR reference chart.

    Returns:
        Cleaned HUD DataFrame.
    """
    path = find_csv("hud_fmr.csv")
    print(f"  HUD FMR: {path}")
    df = pd.read_csv(path)

    fmr_cols = ["fmr_0bdr", "fmr_1bdr", "fmr_2bdr", "fmr_3bdr", "fmr_4bdr"]
    for col in fmr_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df = df[["fmr_code", "fmr_areaname"] + fmr_cols].dropna(subset=["fmr_2bdr"])
    df = df.drop_duplicates(subset=["fmr_code"])
    print(f"  → {len(df)} metro areas")
    return df


# ── 3. FRED ───────────────────────────────────────────────────────────────────

def load_fred():
    """Load and clean fred.csv.

    Columns: date, mortgage_rate_30yr, case_shiller_home_price_index,
             cpi_rent_primary_residence, housing_starts_thousands.

    Returns:
        Cleaned FRED time-series DataFrame.
    """
    path = find_csv("fred.csv")
    print(f"  FRED: {path}")
    df = pd.read_csv(path)
    df["date"] = pd.to_datetime(df["date"])
    df["year"] = df["date"].dt.year

    for col in [c for c in df.columns if c not in ("date", "year")]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    print(f"  → {len(df)} rows")
    return df


# ── 4. Zillow ─────────────────────────────────────────────────────────────────

def load_zillow_annual(filename, value_name):
    """Load a wide Zillow CSV and return annual averages in long format.

    Args:
        filename:   CSV filename (e.g. 'zhvi_metro.csv').
        value_name: Value column name ('zhvi' or 'zori').

    Returns:
        DataFrame with columns: RegionName, StateName, year, <value_name>.
    """
    path = find_csv(filename)
    print(f"  Zillow {value_name}: {path}")
    df = pd.read_csv(path)

    id_cols   = [c for c in df.columns if not re.match(r"^\d{4}-\d{2}", str(c))]
    date_cols = [c for c in df.columns if re.match(r"^\d{4}-\d{2}", str(c))]

    melted = df[id_cols + date_cols].melt(
        id_vars=id_cols, value_vars=date_cols,
        var_name="date", value_name=value_name
    )
    melted["date"]     = pd.to_datetime(melted["date"], errors="coerce")
    melted[value_name] = pd.to_numeric(melted[value_name], errors="coerce")
    melted = melted.dropna(subset=["date", value_name])
    melted["year"] = melted["date"].dt.year

    annual = (
        melted.groupby(["RegionName", "StateName", "year"])[value_name]
        .mean()
        .reset_index()
    )
    print(f"  → {len(annual)} rows")
    return annual


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    print("Loading data...")
    panel = load_census()          # Census is self-contained with derived cols
    hud   = load_hud()             # Metro-level FMR reference
    fred  = load_fred()            # National macro time series
    zhvi  = load_zillow_annual("zhvi_metro.csv", "zhvi")
    zori  = load_zillow_annual("zori_metro.csv", "zori")

    panel.to_parquet(os.path.join(OUT_DIR, "panel.parquet"), index=False)
    hud.to_parquet(os.path.join(OUT_DIR,   "hud.parquet"),   index=False)
    fred.to_parquet(os.path.join(OUT_DIR,  "fred.parquet"),  index=False)
    zhvi.to_parquet(os.path.join(OUT_DIR,  "zhvi.parquet"),  index=False)
    zori.to_parquet(os.path.join(OUT_DIR,  "zori.parquet"),  index=False)

    print(f"\nSaved 5 parquet files to {OUT_DIR}/")


if __name__ == "__main__":
    main()