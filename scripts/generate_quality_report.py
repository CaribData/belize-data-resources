#!/usr/bin/env python3
"""
Lightweight data quality report:
- Scans data/ for CSVs
- Checks: row count, missing values %, duplicate rows
- Emits data/_quality_report.json and a friendly CSV summary
"""
import json
import pathlib
import pandas as pd

ROOT = pathlib.Path(__file__).resolve().parents[1]
DATA = ROOT / "data"

def scan_csvs():
    for p in DATA.rglob("*.csv"):
        yield p

def analyze(path: pathlib.Path):
    try:
        df = pd.read_csv(path)
    except Exception as e:
        return {"path": str(path), "error": str(e)}
    rows = len(df)
    dups = int(df.duplicated().sum())
    na_pct = float(df.isna().sum().sum()) / float(df.shape[0]*max(1,df.shape[1])) * 100.0
    return {
        "path": str(path),
        "rows": rows,
        "columns": list(df.columns),
        "duplicate_rows": dups,
        "missing_percent": round(na_pct, 2)
    }

def main():
    results = [analyze(p) for p in scan_csvs()]
    out_json = DATA / "_quality_report.json"
    out_csv = DATA / "_quality_report.csv"
    out_json.write_text(json.dumps(results, indent=2), encoding="utf-8")
    pd.DataFrame(results).to_csv(out_csv, index=False)
    print("Quality report written:", out_json, out_csv)

if __name__ == "__main__":
    main()
