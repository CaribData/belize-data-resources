name: CaribData Messy Data — Fetch & Bundle

on:
  workflow_dispatch:
  schedule:
    - cron: "15 7 * * 2"  # Tuesdays 07:15 UTC
  push:
    paths:
      - "scripts/fetch_messy.py"
      - "catalog.yml"
      - ".github/workflows/messy-bundle.yml"

permissions:
  contents: write

jobs:
  messy:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: "3.11"

      - name: Install deps
        run: |
          pip install -r requirements.txt

      - name: Fetch messy datasets
        env:
          CARIBDATA_HTTP_TIMEOUT: "120"
          CARIBDATA_HTTP_RETRIES: "6"
          CARIBDATA_HTTP_BACKOFF: "0.8"
        run: python scripts/fetch_messy.py

      - name: Preview manifest & report
        run: |
          ls -lah data/messy || true
          (cat data/messy/_manifest.json | head -n 200) || true
          (cat data/messy/_report.json | head -n 200) || true

      - name: Upload bundle artifact
        uses: actions/upload-artifact@v4
        with:
          name: messy-bundle
          path: |
            data/messy/_bundle.zip
            data/messy/_manifest.json
            data/messy/_report.json
            data/messy/_dataset_card.md
            data/messy/raw/**
