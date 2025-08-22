# Belize, Guyana, Jamaica, Trinidad & Tobago - Open Data Resources
## Created by Ian Hambleton (22-Aug-2025)

![Build](https://github.com/CaribData/belize-data-resources/actions/workflows/build-release.yml/badge.svg)

An updatable, versioned data resource for **Belize (BZ)** and neighbours **Jamaica (JM)**, **Trinidad & Tobago (TT)**, **Guyana (GY)** pulling:
- World Bank indicators (curated in `catalog.yml`)
- FAOSTAT Food Balance Sheets (FBS)

## Quick start
```bash
pip install -r requirements.txt
python scripts/build_wb_fwo.py
python scripts/generate_quality_report.py
```

**Belize versioned data pack:**
1. Go to **Actions → “Build & Release — Belize” → Run workflow**.
2. Tag: e.g. `belize-data-2025-08-21 [optional -#1/-#2 etc] (date).
3. Name: e.g. Belize Data Pack - (2025-08-21, v1.0.1) (shown on Releases).

The workflow produces a **Release** containing:
- `belize-data-pack.zip` (all datasets + catalog + provenance)
- `_meta/manifest.json` (build time, commit SHA, catalog hash, file checksums/rows)
- `_meta/checksums.json` (path → SHA-256)

**Versioning policy**
- **New data ⇒ new tag.** Don’t overwrite tags.
- The most recent release is automatically marked **Latest**.

**MetaData sources**
- Included in `catalog.yaml`.
