# U.S. Housing Market Affordability Dashboard

**DS3500 Capstone Project**

**Team: The Schemers**

Deesha Busarapu · Anushka Anand

An interactive data dashboard exploring housing affordability across U.S. counties and metro areas from 2019–2023, built from four integrated data sources.

---

## Research Questions

1. How has the **price-to-income ratio** changed across U.S. metros from 2019-2023, and which regions have seen the steepest decline in affordability?
2. Is **rent growth or home price growth** the larger driver of unaffordability in major cities?
3. How do **rising mortgage rates** correlate with housing starts and market supply?
4. Which counties show the widest gap between **rent and median household income**?

---

## Data Sources

| Source | Type | Key Fields |
|--------|------|------------|
| [U.S. Census Bureau ACS5](https://api.census.gov/) | REST API | Median income, gross rent, population — county × year |
| [FRED](https://fred.stlouisfed.org/) | REST API | 30-yr mortgage rate, housing starts, CPI rent, Case-Shiller index |
| [Zillow Research Data](https://www.zillow.com/research/data/) | Static CSV | ZHVI (home values), ZORI (rent index) — metro × month |
| [HUD Fair Market Rents](https://hudgis-hud.opendata.arcgis.com) | ArcGIS CSV | Fair Market Rent by bedroom size — metro area level |

---

## Project Structure

```
HousingMarketAffordability/
│
├── fetch_census.py          # Pulls ACS5 county data from Census API (2019–2023)
├── fetch_fred.py            # Pulls 4 FRED time series (mortgage rate, starts, etc.)
├── fetch_hud.py             # Downloads HUD Fair Market Rents via ArcGIS
├── fetch_zillow.py          # Downloads ZHVI and ZORI CSVs from Zillow static URLs
│
├── clean_merge.py           # Cleans all sources, computes affordability metrics,
│                            # saves 5 parquet files to data/processed/
├── dashboard.py             # Panel dashboard — launches on http://localhost:5006
│
├── tests/
│   ├── test_acquisition.py  # Tests for fetch functions (mocked HTTP)
│   ├── test_cleaning.py     # Tests for data cleaning logic
│   ├── test_merging.py      # Tests for merge correctness and derived columns
│   └── test_validation.py   # Tests for data quality validators
│
├── data/
│   ├── raw/                 # CSVs saved by fetch_*.py scripts
│   └── processed/           # Parquet files saved by clean_merge.py
│
└── requirements.txt
```

### Data Flow

```
[Census API]  [FRED API]  [Zillow CSVs]  [HUD ArcGIS]
      │             │            │               │
  fetch_census  fetch_fred  fetch_zillow     fetch_hud
      │             │            │               │
      └─────────────┴────────────┴───────────────┘
                          │
                    clean_merge.py
                    ├── cleans each source
                    ├── computes rent_burden, rent_gap
                    └── saves to data/processed/*.parquet
                          │
                    dashboard.py
                    └── Panel app → http://localhost:5006
```

---

## Setup & Installation

### Prerequisites
- Python 3.10+
- API keys for [FRED](https://fred.stlouisfed.org/docs/api/api_key.html) and [Census Bureau](https://api.census.gov/data/key_signup.html) (both free)

### 1. Clone the repository

```bash
git clone https://github.com/your-team/housing-affordability.git
cd housing-affordability
```

### 2. Install dependencies

```bash
pip install -r requirements.txt
```

### 3. Set API keys

Create a `.env` file in the project root:

```
FRED_API_KEY=your_fred_key_here
CENSUS_API_KEY=your_census_key_here
```

### 4. Fetch the raw data

Run each fetch script once to populate `data/raw/`:

```bash
python fetch_census.py
python fetch_fred.py
python fetch_hud.py
python fetch_zillow.py
```

> **Note:** `fetch_zillow.py` downloads directly from Zillow's public static URLs — no API key needed.

### 5. Launch the dashboard

```bash
python dashboard.py
```

`dashboard.py` will automatically run `clean_merge.py` to build the processed data if it hasn't been run yet. The dashboard opens at **http://localhost:5006**.

---

## Dashboard Features

| Feature | Description |
|---------|-------------|
| **5 KPI Cards** | Median income, rent, rent burden, rent gap, % counties over 30% burden — update with year slider |
| **Animated Scatter** | All counties plotted as rent vs income, animated by year with a 30% affordability threshold line |
| **Rent Gap Bar Chart** | Top N counties where rent most exceeds the 30% affordability threshold — filterable by state |
| **FRED Dual-Axis Chart** | 30-yr mortgage rate overlaid with housing starts, showing the supply response to rate changes |
| **Zillow ZHVI / ZORI** | Side-by-side home value and rent index trends for user-selected metro areas |
| **HUD FMR Bar Chart** | Top metro areas by 2-bedroom Fair Market Rent — the government's official affordability benchmark |

**Sidebar controls:** Year slider · State filter · Top-N slider · Metro multi-picker

---

## Running Tests

```bash
pytest tests/ -v
```

All tests use fixtures and mocked HTTP responses — no API keys or downloaded files required. The suite covers:

- `test_acquisition.py` — Zillow melt/aggregate logic, HUD column standardization, FRED response parsing, Census FIPS construction
- `test_cleaning.py` — FIPS validation, Census sentinel removal, FRED NaN handling, HUD range checks
- `test_merging.py` — Left join row preservation, derived column formulas, FRED annual pivot shape
- `test_validation.py` — FIPS format, null checks, year range, positive values, duplicate detection

---

## Key Design Decisions

**Merge strategy:** The Census ACS5 data forms the base panel (county × year). HUD Fair Market Rents are joined as a supplemental reference on `fips_code`. FRED national series are broadcast to all counties by `year`. Zillow metro data is kept as a separate table due to the CBSA-to-county mapping complexity.

**HUD data structure:** The HUD ArcGIS export provides metro-area level data with `fmr_code` identifiers rather than county FIPS codes, making a direct county-level join impossible without a CBSA crosswalk. HUD is therefore used as a standalone metro reference dataset.

**Parquet storage:** Processed data is saved as Parquet rather than CSV for ~10× faster load times in the dashboard and automatic preservation of column data types.

**Affordability metrics:**
- `rent_burden` = (median gross rent × 12) / median household income
- `rent_gap` = median gross rent − (median household income / 12 × 0.30)
- The 30% threshold follows the standard HUD definition of housing cost burden

---

## Known Limitations

- Census ACS5 lags by ~2 years — most recent data is the 2023 vintage (covering 2019–2023)
- Zillow ZORI covers ~700 major metros; rural counties won't have rent index values
- HUD FMR is cross-sectional (single fiscal year) — not a time series
- FRED series are national averages and don't capture local mortgage rate variation
- Zillow ZHVI is metro-level; county-level home value data requires a CBSA crosswalk

---

## Requirements

```
pandas>=2.0
numpy>=1.24
requests>=2.28
panel>=1.4
plotly>=5.18
pytest>=7.4
python-dotenv>=1.0
pyarrow>=14.0
bokeh>=3.3
```
