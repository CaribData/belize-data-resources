# Messy Data (Belize)

This repository includes a small bundle of intentionally messy public datasets from Belize to test ingest and cleaning.

- Source list lives in `catalog.yml` under `messy.items`.
- The workflow **CaribData Messy Data — Fetch & Bundle** builds:
  - `data/messy/_bundle.zip` (all raw files)
  - `data/messy/_manifest.json` (file metadata)
  - `data/messy/_report.json` (heuristics: sheets, merged cells, header guesses)

> Tip: Download the latest **artifact** named `messy-bundle` from the workflow run to inspect the ZIP and metadata.
