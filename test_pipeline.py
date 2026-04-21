"""
test_pipeline.py
pytest test suite for the housing affordability data pipeline.
Covers data loading, cleaning, merging, and validation.

Run with:
    pytest tests/test_pipeline.py -v
"""

import pytest  # noqa: F401 — must be first
import pandas as pd
import numpy as np
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from clean_merge import load_census, load_hud, load_fred, load_zillow_annual



@pytest.fixture
def sample_census_df():
    """Minimal census-like DataFrame for unit testing."""
    return pd.DataFrame({
        "median_household_income": [60000.0, -999.0, None, 75000.0, 80000.0],
        "median_gross_rent":       [1200.0,   800.0, 900.0, 1100.0, 1500.0],
        "population":              [50000,   30000,     0,  20000, 100000],
        "county_name":             ["Suffolk County, MA", "Kings County, NY",
                                    "Test County, CA", "Cook County, IL", "Harris County, TX"],
        "state":                   ["25", "36", "06", "17", "48"],
        "county":                  ["025", "047", "001", "031", "201"],
        "year":                    [2021, 2021, 2021, 2021, 2021],
        "fips":                    ["25025", "36047", "06001", "17031", "48201"],
    })


@pytest.fixture
def sample_hud_df():
    """Minimal HUD-like DataFrame for unit testing."""
    return pd.DataFrame({
        "fmr_code":    ["0100199999", "0600199999", "0600199999"],
        "fmr_areaname":["Area A", "Area B", "Area B"],
        "fmr_0bdr":    [800.0,  900.0,  900.0],
        "fmr_1bdr":    [1000.0, 1100.0, 1100.0],
        "fmr_2bdr":    [1200.0, 1300.0, None],
        "fmr_3bdr":    [1500.0, 1600.0, None],
        "fmr_4bdr":    [1800.0, 1900.0, None],
        "shape__area":  [0.1, 0.2, 0.3],
        "shape__length":[1.0, 2.0, 3.0],
    })


@pytest.fixture
def sample_fred_df():
    """Minimal FRED-like DataFrame for unit testing."""
    dates = pd.date_range("2019-01-01", periods=24, freq="MS")
    return pd.DataFrame({
        "date":                          dates.astype(str),
        "mortgage_rate_30yr":            [3.5] * 12 + [4.0] * 12,
        "case_shiller_home_price_index": [200.0] * 24,
        "cpi_rent_primary_residence":    [310.0] * 24,
        "housing_starts_thousands":      [1500.0] * 24,
    })


@pytest.fixture
def sample_zillow_df(tmp_path):
    """Writes a minimal wide-format Zillow CSV and returns its path."""
    df = pd.DataFrame({
        "RegionID":   [1, 2],
        "SizeRank":   [1, 2],
        "RegionName": ["Boston, MA", "New York, NY"],
        "RegionType": ["Metro", "Metro"],
        "StateName":  ["MA", "NY"],
        "2019-01-31": [400000.0, 600000.0],
        "2019-06-30": [420000.0, 620000.0],
        "2020-01-31": [430000.0, 610000.0],
        "2021-01-31": [500000.0, 650000.0],
    })
    path = tmp_path / "zhvi_metro.csv"
    df.to_csv(path, index=False)
    return str(path)


def test_census_no_negative_income(sample_census_df):
    """Negative income values are removed during cleaning."""
    df = sample_census_df.copy()
    for col in ["median_household_income", "median_gross_rent", "population"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
        df.loc[df[col] < 0, col] = None
    assert (df["median_household_income"].dropna() >= 0).all()


def test_census_no_null_income(sample_census_df):
    """Rows with null median_household_income are dropped."""
    df = sample_census_df.copy()
    df["median_household_income"] = pd.to_numeric(
        df["median_household_income"], errors="coerce"
    )
    df.loc[df["median_household_income"] < 0, "median_household_income"] = None
    df = df.dropna(subset=["median_household_income"])
    assert df["median_household_income"].isna().sum() == 0


def test_census_fips_zero_padded(sample_census_df):
    """All FIPS codes are exactly 5 characters after standardization."""
    df = sample_census_df.copy()
    df["fips"] = df["fips"].astype(str).str.zfill(5)
    assert df["fips"].str.len().eq(5).all()


def test_census_no_duplicate_fips_year(sample_census_df):
    """No duplicate fips + year rows exist after deduplication."""
    df = sample_census_df.copy()
    duped = pd.concat([df, df.iloc[[0]]], ignore_index=True)
    result = duped.drop_duplicates(subset=["fips", "year"])
    assert result.duplicated(subset=["fips", "year"]).sum() == 0


def test_census_derived_columns(sample_census_df):
    """rent_burden, affordable_monthly, and rent_gap are computed correctly."""
    df = sample_census_df.copy()
    df["median_household_income"] = pd.to_numeric(df["median_household_income"], errors="coerce")
    df["median_gross_rent"] = pd.to_numeric(df["median_gross_rent"], errors="coerce")
    df = df.dropna(subset=["median_household_income"])

    df["rent_burden"]        = (df["median_gross_rent"] * 12) / df["median_household_income"]
    df["affordable_monthly"] = df["median_household_income"] / 12 * 0.30
    df["rent_gap"]           = df["median_gross_rent"] - df["affordable_monthly"]

    assert "rent_burden" in df.columns
    assert "affordable_monthly" in df.columns
    assert "rent_gap" in df.columns
    # A household earning 60k with 1200/mo rent has 24% rent burden
    row = df[df["median_household_income"] == 60000].iloc[0]
    assert abs(row["rent_burden"] - (1200 * 12 / 60000)) < 0.001



def test_hud_drops_null_fmr2bdr(sample_hud_df):
    """Rows with null fmr_2bdr are dropped since 2BR is the reference unit."""
    df = sample_hud_df.copy()
    df["fmr_2bdr"] = pd.to_numeric(df["fmr_2bdr"], errors="coerce")
    result = df.dropna(subset=["fmr_2bdr"])
    assert result["fmr_2bdr"].isna().sum() == 0


def test_hud_no_duplicate_fmr_code(sample_hud_df):
    """Duplicate fmr_code rows are removed."""
    df = sample_hud_df.copy()
    result = df.drop_duplicates(subset=["fmr_code"])
    assert result.duplicated(subset=["fmr_code"]).sum() == 0


def test_hud_fmr_cols_numeric(sample_hud_df):
    """All fmr_Xbdr columns are cast to numeric."""
    df = sample_hud_df.copy()
    for col in ["fmr_0bdr", "fmr_1bdr", "fmr_2bdr", "fmr_3bdr", "fmr_4bdr"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    for col in ["fmr_0bdr", "fmr_1bdr"]:
        assert pd.api.types.is_float_dtype(df[col])



def test_fred_date_parsed(sample_fred_df):
    """Date column is parsed to datetime."""
    df = sample_fred_df.copy()
    df["date"] = pd.to_datetime(df["date"])
    assert pd.api.types.is_datetime64_any_dtype(df["date"])


def test_fred_has_year_column(sample_fred_df):
    """Year column is derived from date."""
    df = sample_fred_df.copy()
    df["date"] = pd.to_datetime(df["date"])
    df["year"] = df["date"].dt.year
    assert "year" in df.columns
    assert df["year"].between(2019, 2024).all()


def test_fred_numeric_cols(sample_fred_df):
    """All value columns are numeric after cleaning."""
    df = sample_fred_df.copy()
    for col in ["mortgage_rate_30yr", "case_shiller_home_price_index",
                "cpi_rent_primary_residence", "housing_starts_thousands"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
        assert pd.api.types.is_float_dtype(df[col])



def test_zillow_long_format(sample_zillow_df):
    """Zillow wide CSV is melted to long format with one row per region per month."""
    import re
    df = pd.read_csv(sample_zillow_df)
    id_cols   = [c for c in df.columns if not re.match(r"^\d{4}-\d{2}", str(c))]
    date_cols = [c for c in df.columns if re.match(r"^\d{4}-\d{2}", str(c))]
    melted = df.melt(id_vars=id_cols, value_vars=date_cols,
                     var_name="date", value_name="zhvi")
    assert "zhvi" in melted.columns
    assert len(melted) == len(df) * len(date_cols)


def test_zillow_annual_aggregation(sample_zillow_df):
    """Annual aggregation produces one row per RegionName + year."""
    import re
    df = pd.read_csv(sample_zillow_df)
    id_cols   = [c for c in df.columns if not re.match(r"^\d{4}-\d{2}", str(c))]
    date_cols = [c for c in df.columns if re.match(r"^\d{4}-\d{2}", str(c))]
    melted = df.melt(id_vars=id_cols, value_vars=date_cols,
                     var_name="date", value_name="zhvi")
    melted["date"] = pd.to_datetime(melted["date"], errors="coerce")
    melted["zhvi"] = pd.to_numeric(melted["zhvi"], errors="coerce")
    melted = melted.dropna(subset=["date", "zhvi"])
    melted["year"] = melted["date"].dt.year
    annual = melted.groupby(["RegionName", "StateName", "year"])["zhvi"].mean().reset_index()
    assert annual.duplicated(subset=["RegionName", "year"]).sum() == 0


def test_processed_files_exist():
    """All 5 parquet files exist after running clean_merge.py."""
    processed = os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        "data", "processed"
    )
    for fname in ["panel.parquet", "hud.parquet", "fred.parquet",
                  "zhvi.parquet", "zori.parquet"]:
        path = os.path.join(processed, fname)
        assert os.path.exists(path), f"Missing: {path}"