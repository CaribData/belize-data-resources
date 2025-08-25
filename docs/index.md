# CaribData Open Data — Caribbean

This micro-site lists a few example datasets pulled from openly available internet sources. 

The data are mostly pulled from the World Bank and the Food and Agriculture Organization (FAO) open data repositories. 
All data sources are extracted automatically, with a weekly check made for data source updates. 

Data have been extracted for each our four partner countries: 

- Belize (BZ),
- Guyana (GY).
- Jamaica (JM), and 
- Trinidad & Tobago (TT).

A full list of extracted datasets is maintained in `catalog.yml`.

## What’s included

### Data
- World Bank per-country CSVs under `data/world_bank/<ISO2>/`
- FAOSTAT Food and Balance Sheets per-country CSVs under `data/faostat_fbs/`
- Selected 'messy' datasets for Belize under `data/messy/`

### Associated files
Several information files have been created:
- Brief data dictionary for the extracted files: `data/world_bank/_dictionary.csv`
- Manifests: `data/world_bank/_manifest.json`, `data/faostat_fbs/_manifest.json`
- Example quality report, lists percent compleness for each extracted indicator: `data/_quality_report.json|csv`
