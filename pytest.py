"""
test_pipeline.py
pytest test suite for the housing affordability data pipeline.
Covers data loading, cleaning, merging, and validation.

Run with:
    pytest tests/test_pipeline.py -v
"""

import pytest
import pandas as pd
import numpy as np
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from clean_census import (
    cast_numerics, drop_sentinel_values, drop_null_income,
    drop_zero_population, standardize_fips, remove_duplicates as census_dedup,
)
from clean_fred import (
    drop_all_null_rows, interpolate_gaps, clip_valid_ranges, aggregate_annual,
)
from clean_hud import (
    drop_geometry_cols, rename_fmr_cols, drop_null_fmr_rows, remove_duplicates as hud_dedup,
)
from clean_zillow import melt_dates, filter_years, aggregate_annual as zillow_annual
from merge import merge_fred, merge_zillow, add_affordability_metrics


# ── Fixtures ─────────────────────────────────────────────────────────────────

@pytest.fixture
def sample_census():
    return pd.DataFrame({
        "median_household_income": [60000, -666666666, None, 75000, 80000],
        "median_gross_rent":       [1200,  800,         900,  None,  1500],
        "population":              [50000, 30000, 0, 20000, 100000],
        "county_name":             ["A", "B", "C", "D", "E"],
        "state":                   ["25", "25", "36", "36", "06"],
        "county":                  ["001", "003", "005", "007", "001"],
        "year":                    [2021, 2021, 2021, 2021, 2021],
        "fips":                    ["25001", "25003", "36005", "36007", "06001"],
    })


@pytest.fixture
def sample_fred():
    dates = pd.date_range("2019-01-01", periods=12, freq="MS")
    return pd.DataFrame({
        "date": dates,
        "mortgage_rate_30yr":            [3.5]*6 + [None]*6,
        "case_shiller_home_price_index": [200.0]*12,
        "cpi_rent_primary_residence":    [310.0]*12,
        "housing_starts_thousands":      [1500.0]*12,
    })


@pytest.fixture
def sample_hud():
    return pd.DataFrame({
        "objectid":     [1, 2, 3],
        "fmr_code":     ["0100199999", "0600199999", "0600199999"],
        "fmr_areaname": ["Area A", "Area B", "Area B"],
        "fmr_0bdr":     [800, 900, 900],
        "fmr_1bdr":     [1000, 1100, 1100],
        "fmr_2bdr":     [1200, 1300, None],
        "fmr_3bdr":     [1500, 1600, None],
        "fmr_4bdr":     [1800, 1900, None],
        "shape__area":  [0.1, 0.2, 0.3],
        "shape__length":[1.0, 2.0, 3.0],
    })


@pytest.fixture
def sample_zillow():
    return pd.DataFrame({
        "RegionID":   [1, 2],
        "RegionName": ["Boston, MA", "New York, NY"],
        "RegionType": ["Metro", "Metro"],
        "StateName":  ["MA", "NY"],
        "SizeRank":   [1, 2],
        "2019-01-31": [400000.0, 600000.0],
        "2019-06-30": [420000.0, 620000.0],
        "2020-01-31": [430000.0, 610000.0],
        "2021-01-31": [500000.0, 650000.0],
    })


# ── Census Tests ──────────────────────────────────────────────────────────────

def test_sentinel_values_removed(sample_census):
    """Sentinel value -666666666 rows are dropped from Census data."""
    df = cast_numerics(sample_census.copy())
    df = drop_sentinel_values(df)
    assert -666666666 not in df["median_household_income"].values


def test_null_income_dropped(sample_census):
    """Rows with null median_household_income are removed."""
    df = cast_numerics(sample_census.copy())
    df = drop_null_income(df)
    assert df["median_household_income"].isna().sum() == 0


def test_zero_population_dropped(sample_census):
    """Rows with zero or null population are removed."""
    df = cast_numerics(sample_census.copy())
    df = drop_zero_population(df)
    assert (df["population"] == 0).sum() == 0
    assert df["population"].isna().sum() == 0


def test_fips_zero_padded(sample_census):
    """All FIPS codes are exactly 5 characters after standardization."""
    df = standardize_fips(sample_census.copy())
    assert df["fips"].str.len().eq(5).all()


def test_census_duplicates_removed(sample_census):
    """Duplicate fips + year rows are removed, keeping one per county-year."""
    duped = pd.concat([sample_census, sample_census.iloc[[0]]], ignore_index=True)
    result = census_dedup(duped)
    assert result.duplicated(subset=["fips", "year"]).sum() == 0


# ── FRED Tests ────────────────────────────────────────────────────────────────

def test_all_null_fred_rows_dropped(sample_fred):
    """Rows where every value column is null are removed."""
    df = sample_fred.copy()
    df.iloc[0, 1:] = None  # make first row all null
    result = drop_all_null_rows(df)
    assert len(result) < len(df)


def test_fred_aggregates_to_annual(sample_fred):
    """FRED data aggregates to one row per year."""
    df = sample_fred.copy()
    df["date"] = pd.to_datetime(df["date"])
    result = aggregate_annual(df)
    assert len(result) == df["date"].dt.year.nunique()


def test_mortgage_rate_clipped(sample_fred):
    """Mortgage rates outside valid range (0–30) are clipped."""
    df = sample_fred.copy()
    df["mortgage_rate_30yr"] = 999.0
    result = clip_valid_ranges(df)
    assert result["mortgage_rate_30yr"].max() <= 30


# ── HUD Tests ─────────────────────────────────────────────────────────────────

def test_geometry_cols_dropped(sample_hud):
    """ArcGIS geometry columns are removed from HUD data."""
    result = drop_geometry_cols(sample_hud.copy())
    assert "shape__area" not in result.columns
    assert "shape__length" not in result.columns
    assert "objectid" not in result.columns


def test_fmr_cols_renamed(sample_hud):
    """fmr_Xbdr columns are renamed to fmr_X."""
    df = drop_geometry_cols(sample_hud.copy())
    result = rename_fmr_cols(df)
    assert "fmr_0" in result.columns
    assert "fmr_0bdr" not in result.columns


def test_hud_duplicates_removed(sample_hud):
    """Duplicate fmr_code rows are removed."""
    df = drop_geometry_cols(sample_hud.copy())
    df = rename_fmr_cols(df)
    result = hud_dedup(df)
    assert result.duplicated(subset=["fmr_code"]).sum() == 0


# ── Merge Tests ───────────────────────────────────────────────────────────────

def test_fred_merge_preserves_census_rows(sample_census, sample_fred):
    """Merging FRED onto Census preserves all Census rows (left join)."""
    census = cast_numerics(sample_census.copy())
    fred_df = sample_fred.copy()
    fred_df["date"] = pd.to_datetime(fred_df["date"])
    fred_annual = aggregate_annual(fred_df)
    result = merge_fred(census, fred_annual)
    assert len(result) == len(census)


def test_affordability_metrics_computed():
    """price_to_income_ratio and rent_to_income_ratio are computed correctly."""
    df = pd.DataFrame({
        "zhvi":                    [400000.0],
        "zori":                    [2000.0],
        "median_household_income": [80000.0],
    })
    result = add_affordability_metrics(df)
    assert abs(result["price_to_income_ratio"].iloc[0] - 5.0) < 0.01
    assert abs(result["rent_to_income_ratio"].iloc[0] - 0.3) < 0.01


def test_merge_no_duplicate_fips_year(sample_census, sample_fred):
    """Final merged dataset has no duplicate fips + year rows."""
    census = cast_numerics(sample_census.copy())
    fred_df = sample_fred.copy()
    fred_df["date"] = pd.to_datetime(fred_df["date"])
    fred_annual = aggregate_annual(fred_df)
    result = merge_fred(census, fred_annual)
    assert result.duplicated(subset=["fips", "year"]).sum() == 0