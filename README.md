# Charity_Recommender

Data Source 1: https://docs.every.org/docs/endpoints/nonprofit-search

## IRS Data Cleaning Workflow

To build the offline dataset of IRS 501(c)(3) organizations:

1. Download the four IRS EO BMF CSV region files into the local `data/` directory.
2. Update `REGION_FILES` near the top of `scripts/build_clean_irs_dataframe.py` so that each entry points to the correct CSV filename.
3. Install dependencies (pandas is required) and run:
   ```
   python scripts/build_clean_irs_dataframe.py
   ```
4. The script merges, standardizes, and filters the raw CSVs, then writes the cleaned output to `data/cleaned_irs_charities.csv`. The console log includes row counts before/after cleaning plus a quick sample of the resulting data.

This cleaned file can then be used for the subsequent database import step.



Testing: 

Backend:

cd '/Users/matthewplambeck/Desktop/Charity_Recommender copy'
sqlite3 data/charities.db "SELECT COUNT(*) FROM charities;"
export EVERY_API_KEY=pk_live_eb9141b3fe7b78b7644c2c838016abd5
cd backend
source .venv/bin/activate
-r requirements.txt
uvicorn app:app --reload --host 0.0.0.0 --port 8000

Frontend:
cd '/Users/matthewplambeck/Desktop/Charity_Recommender copy/docs'
python3 -m http.server 5500