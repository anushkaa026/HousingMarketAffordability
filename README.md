# U.S. Housing Market Affordability Dashboard

**DS3500 Capstone -- Team: The Schemers**
Deesha Busarapu and Anushka Anand

An interactive dashboard exploring housing affordability across U.S. counties and metro areas from 2019 to 2023, built from four integrated data sources.

---

## Research Questions

1. How has the price-to-income ratio changed across U.S. metros from 2019 to 2023, and which regions have seen the steepest decline in affordability?
2. Is rent growth or home price growth the larger driver of unaffordability in major cities?
3. How do rising mortgage rates correlate with housing starts and market supply?
4. Which counties show the widest gap between rent and median household income?

---

## Data Sources

| Source | Type | Key Fields |
|--------|------|------------|
| [U.S. Census Bureau ACS5](https://api.census.gov/) | REST API | Median income, gross rent, population |
| [FRED](https://fred.stlouisfed.org/) | REST API | 30-yr mortgage rate, housing starts, CPI rent |
| [Zillow Research Data](https://www.zillow.com/research/data/) | Static CSV | ZHVI (home values), ZORI (rent index) |
| [HUD Fair Market Rents](https://hudgis-hud.opendata.arcgis.com) | ArcGIS CSV | Fair Market Rent by bedroom size |

---

## Project Structure

```
├── fetch_census.py          # Pulls ACS5 county data from Census API
├── fetch_fred.py            # Pulls FRED macro time series
├── fetch_hud.py             # Downloads HUD Fair Market Rents via ArcGIS
├── fetch_zillow.py          # Downloads ZHVI and ZORI CSVs from Zillow
├── run_fetch_all.py         # Runs all four fetch scripts in sequence
├── main.py                  # Entry point -- runs pipeline then launches dashboard
├── clean_merge.py           # Cleans all sources, computes affordability metrics
├── dashboard.py             # Panel dashboard -- runs on http://localhost:5006
├── test_pipeline.py         # Test suite for pipeline functions
├── census_acs5.csv          # Raw Census data
├── census_acs5_sample.csv   # First 100 rows (sample)
├── fred.csv                 # Raw FRED data
├── fred_sample.csv          # First 100 rows (sample)
├── hud_fmr.csv              # Raw HUD Fair Market Rents
├── hud_fmr_sample.csv       # First 100 rows (sample)
├── zhvi_metro.csv           # Zillow Home Value Index
├── zhvi_metro_sample.csv    # First 100 rows (sample)
├── zori_metro.csv           # Zillow Observed Rent Index
├── zori_metro_sample.csv    # First 100 rows (sample)
└── data/                    # Processed parquet files (auto-generated)
```

---

## Setup

### 1. Install dependencies
```bash
pip install -r requirements.txt
```

### 2. Set API keys

Create a `.env` file in the project root:
```
FRED_API_KEY=your_fred_key_here
CENSUS_API_KEY=your_census_key_here
```
Free keys: [FRED](https://fred.stlouisfed.org/docs/api/api_key.html) and [Census](https://api.census.gov/data/key_signup.html)

### 3. Fetch raw data
```bash
python run_fetch_all.py
```
Or run each script individually:
```bash
python fetch_census.py
python fetch_fred.py
python fetch_hud.py
python fetch_zillow.py
```

### 4. Launch the dashboard
```bash
python main.py
```
Or run the dashboard directly:
```bash
python dashboard.py
```
Opens at **http://localhost:5006**. Automatically runs `clean_merge.py` to build processed data if needed.

---

## Running Tests
```bash
pytest test_pipeline.py -v
```
No API keys or downloaded files required. Tests use fixtures and mocked HTTP responses.

---

## Dashboard Features

- **KPI Cards** -- median income, rent, rent burden, and % of counties over the 30% affordability threshold, updating with the year slider
- **Animated Scatter** -- all counties plotted as rent vs income, animated by year with a 30% threshold line
- **Rent Gap Bar Chart** -- top N counties where rent most exceeds the 30% threshold, filterable by state
- **FRED Dual-Axis Chart** -- 30-yr mortgage rate vs housing starts over time
- **Zillow ZHVI / ZORI** -- home value and rent index trends for selected metro areas
- **HUD FMR Bar Chart** -- top metro areas by 2-bedroom Fair Market Rent

---

## Affordability Metrics

- **Rent burden** = (median gross rent x 12) / median household income
- **Rent gap** = median gross rent - (median household income / 12 x 0.30)
- The 30% threshold follows the standard HUD definition of housing cost burden
