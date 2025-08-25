# CaribData Open Data — Caribbean

This micro-site lists a few example datasets pulled from openly available internet sources. 

The data are mostly pulled from the World Bank and the Food and Agriculture Organization (FAO) open data repositories. 
All data sources are extracted automatically, with a weekly check made for data source updates. 

Data have been extracted for Belize(BZ).

A full list of extracted datasets is maintained in `catalog.yml`.

## What’s included

### Data
- World Bank per-country CSVs under `data/world_bank/<ISO2>/`
- FAOSTAT Food and Balance Sheets per-country CSVs under `data/faostat_fbs/`
- Selected 'messy' datasets for Belize under `data/messy/`

### Associated files
Several information files have been created:

- Brief variable-level data dictionary for the extracted World Bank files: `data/world_bank/_dictionary.json|csv`
- Manifests, providing basic extraction metadata: `data/world_bank/_manifest.json`, `data/faostat_fbs/_manifest.json`
- An example quality report, listing percent completeness for each extracted indicator: `data/_quality_report.json|csv`

### Data Availability
Datasets are available in two ways:

#### Repository 
The full data package as a `.zip` file - containing all datasets - is available via versioned repository releases.

#### Website download
Individual datasets can be downloaded from this website - via the [download](downloads.md) page.
