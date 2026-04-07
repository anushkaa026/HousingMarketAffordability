"""
run_fetch_all.py
Runs all fetch scripts in sequence to generate raw data and samples.

Usage:
    Set environment variables first, then run:

    export CENSUS_API_KEY="your_census_key_here"
    export FRED_API_KEY="your_fred_key_here"
    python run_fetch_all.py
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import fetch_zillow
import fetch_census
import fetch_fred
import fetch_hud


def main():
    print("=" * 50)
    print("Step 1: Zillow (no API key needed)")
    print("=" * 50)
    fetch_zillow.main()

    print("\n" + "=" * 50)
    print("Step 2: Census ACS5 (requires CENSUS_API_KEY)")
    print("=" * 50)
    fetch_census.main()

    print("\n" + "=" * 50)
    print("Step 3: FRED (requires FRED_API_KEY)")
    print("=" * 50)
    fetch_fred.main()

    print("\n" + "=" * 50)
    print("Step 4: HUD Fair Market Rents (no API key needed)")
    print("=" * 50)
    fetch_hud.main()

    print("\n✓ All sources fetched. Check data/raw/ and data/samples/")


if __name__ == "__main__":
    main()