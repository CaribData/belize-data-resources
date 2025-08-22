# Belize, Guyana, Jamaica, Trinidad & Tobago - Open Data Resources
## Created by Ian Hambleton (22-Aug-2025)

# CaribData Open Data Repository

![Build](https://github.com/CaribData/belize-data-resources/actions/workflows/build-release.yml/badge.svg)

Automated, versioned data pulls for **Belize (BZ)**, **Jamaica (JM)**, **Trinidad & Tobago (TT)**, **Guyana (GY)**:
- World Bank indicators (curated in `catalog.yml`)
- FAOSTAT Food Balance Sheets (FBS)

## Quick start (local)
```bash
pip install -r requirements.txt
python scripts/build_wb_fao.py
python scripts/generate_quality_report.py

