# How To

## Add indicators
Edit `catalog.yml > world_bank > indicators` (key = WB code).

## Add countries
- World Bank: `project.countries` (ISO2)
- FAOSTAT: `faostat_fbs.countries_iso3` (ISO3)

## Run locally
```bash
pip install -r requirements.txt
python scripts/build_wb_fwo.py
python scripts/generate_quality_report.py
