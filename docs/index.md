# CaribData Open Data — Caribbean

Automated pull of World Bank indicators and FAOSTAT FBS for BZ, JM, TT, GY.

- Change countries & indicators in `catalog.yml`
- Run the build (CI or local)
- Freshness: <!--FRESHNESS-->Last updated: (pending)<!--/FRESHNESS-->

## What’s included
- World Bank per-country CSVs under `data/world_bank/<ISO2>/`
- FAOSTAT FBS per-country CSVs under `data/faostat_fbs/`
- Indicator dictionary: `data/world_bank/_dictionary.csv`
- Manifests: `data/world_bank/_manifest.json`, `data/faostat_fbs/_manifest.json`
- Quality: `data/_quality_report.json|csv`
