# CaribData Open Data — Caribbean

This small site lists a few example datasets pulled from openly available internet sources. 

The data are mostly pulled from the World Bank and the Food and Agriculture Organization (FAO) open data repositories. 
All sources are extracted automatically, with a weekly check made for data source updates. 
Data have been extracted for our four partner countries: 

- Belize (BZ),
- Guyana (GY).
- Jamaica (JM), and 
- Trinidad & Tobago (TT).

Change countries & indicators in `catalog.yml`

## What’s included
- World Bank per-country CSVs under `data/world_bank/<ISO2>/`
- FAOSTAT FBS per-country CSVs under `data/faostat_fbs/`
- Indicator dictionary: `data/world_bank/_dictionary.csv`
- Manifests: `data/world_bank/_manifest.json`, `data/faostat_fbs/_manifest.json`
- Quality: `data/_quality_report.json|csv`
