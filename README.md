# CaribData Open Data Repository (Caribbean)
## Currently BLZ, GUY, JAM, TTO
## Created by Ian Hambleton (22-Aug-2025)

![Build](https://github.com/CaribData/open-data-caribbean/actions/workflows/build-release.yml/badge.svg)

Automated, versioned data pulls for **Belize (BZ)**, **Jamaica (JM)**, **Trinidad & Tobago (TT)**, **Guyana (GY)**:
- World Bank indicators (curated in `catalog.yml`)
- FAOSTAT Food Balance Sheets (FBS)

## Quick start (browserless optional)
```bash
pip install -r requirements.txt
python scripts/build_wb_fao.py
python scripts/generate_quality_report.py

