#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
build_wb_fwo.py
Belize data harvester for World Bank + FAOSTAT (Food Balance Sheets).

- Reads ./catalog.yml
- Writes CSVs + a small JSON manifest per source
- Friendly for non-coders (all knobs live in catalog.yml)

Requirements: PyYAML (yaml), requests
"""
from __future__ import annotations
import csv
import json
import os
import pathlib
import time
import typing as t
from datetime import datetime

import requests
import yaml

# ----------------------------- helpers ---------------------------------------

def utc_now() -> str:
    return datetime.utcnow().isoformat(timespec="seconds") + "Z"

def ensure_dir(p: pathlib.Path) -> None:
    p.mkdir(parents=True, exist_ok=True)

def retry_get(url: str, params: dict | None = None, tries: int = 4, backoff_s: float = 0.8, timeout: int = 60):
    last = None
    for attempt in range(1, tries + 1):
        try:
            r = requests.get(url, params=params, timeout=timeout)
            r.raise_for_status()
            return r
        except Exception as e:
            last = e
            if attempt == tries:
                raise
            time.sleep(backoff_s * attempt)
    raise last  # pragma: no cover

def read_catalog(path: str = "catalog.yml") -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}

def normalize_str(x) -> str:
    return str(x).strip().lower() if x is not None else ""

# ----------------------- World Bank harvesting --------------------------------

WB_BASE = "https://api.worldbank.org/v2"

def wb_fetch_indicator(country_iso3: str, indicator: str, per_page: int = 20000) -> list[dict]:
    url = f"{WB_BASE}/country/{country_iso3}/indicator/{indicator}"
    params = {"format": "json", "per_page": per_page}
    r = retry_get(url, params=params)
    data = r.json()
    # World Bank returns [meta, rows]
    if not isinstance(data, list) or len(data) < 2:
        return []
    rows = data[1] or []
    return rows

def wb_save_csv(out_dir: pathlib.Path, code: str, cat: str, rows: list[dict]) -> pathlib.Path:
    ensure_dir(out_dir)
    out = out_dir / f"{code}.csv"
    with out.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow([
            "countryiso3code", "date", "value",
            "indicator", "indicatorCode", "category", "last_updated_utc"
        ])
        now = utc_now()
        for r in rows:
            w.writerow([
                r.get("countryiso3code"),
                r.get("date"),
                r.get("value"),
                (r.get("indicator") or {}).get("value"),
                (r.get("indicator") or {}).get("id"),
                cat,
                now
            ])
    return out

def harvest_world_bank(cfg: dict) -> dict:
    wb = (cfg.get("world_bank") or {})
    country = wb.get("country", "BZ")
    indicators = wb.get("indicators") or []
    out_dir = pathlib.Path((cfg.get("paths") or {}).get("world_bank_dir", "data/world_bank"))
    ensure_dir(out_dir)

    results = []
    for i, meta in enumerate(indicators, start=1):
        code = meta.get("code")
        cat = meta.get("category", "Uncategorized")
        if not code:
            print(f"[WB] Skipping indicator without code: {meta}")
            continue
        print(f"[{i}/{len(indicators)}] WB {code} ({cat}) …")
        try:
            rows = wb_fetch_indicator(country, code)
            if rows:
                p = wb_save_csv(out_dir, code, cat, rows)
                results.append({"code": code, "category": cat, "path": str(p)})
            else:
                print("  -> no data")
        except Exception as e:
            print(f"  -> error: {e}")
        time.sleep(0.25)  # be polite
    manifest = {
        "source": "World Bank Open Data",
        "country": country,
        "generated_utc": utc_now(),
        "items": results
    }
    (out_dir / "_manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    print(f"[WB] Wrote manifest at {out_dir / '_manifest.json'}")
    return manifest

# ----------------------- FAOSTAT (Food Balance Sheets) ------------------------

FAO_BASE = "https://fenixservices.fao.org/faostat/api/v1/en"

def fao_fetch_domain_all(domain_path: str, page_size: int = 500000) -> list[dict]:
    """
    Pull a large page in one go. If the endpoint supports paging metadata, you could
    add a loop; most FBS endpoints return all rows with big page_size.
    """
    url = f"{FAO_BASE}/{domain_path}"
    params = {"page_size": page_size}
    r = retry_get(url, params=params, timeout=90)
    j = r.json()
    # API often returns {"data":[...], "totalRecords":..., ...}
    data = j.get("data") if isinstance(j, dict) else None
    if isinstance(data, list):
        return data
    # Some older endpoints return top-level list
    if isinstance(j, list):
        return j
    return []

def _guess_area_column(sample: dict) -> str | None:
    candidates = [k for k in sample.keys() if normalize_str(k) in
                  ("area", "areaitem", "areaname", "area_name", "area code", "area_code", "area_code_m49")]
    if candidates:
        return candidates[0]
    # Fall back: search fields that look like area name
    for k in sample.keys():
        if "area" in normalize_str(k):
            return k
    return None

def harvest_fao_fbs(cfg: dict) -> dict:
    fao = (cfg.get("faostat") or {})
    fbs = (fao.get("food_balance_sheets") or {})
    domains: list[str] = fbs.get("domains") or []
    area_filter = fbs.get("area_filter", "Belize")
    out_dir = pathlib.Path((cfg.get("paths") or {}).get("faostat_fbs_dir", "data/faostat_fbs"))
    ensure_dir(out_dir)

    results = []
    for dom in domains:
        print(f"[FAO FBS] Domain {dom} …")
        try:
            data = fao_fetch_domain_all(dom)
            if not data:
                print("  -> empty")
                continue
            first = data[0]
            area_col = _guess_area_column(first)
            if not area_col:
                # Try common ones explicitly
                for k in ("Area", "areaname", "area", "Area Code (M49)"):
                    if k in first:
                        area_col = k
                        break
            if not area_col:
                print("  -> could not find an Area column; writing full domain without filter")
                filtered = data
            else:
                filtered = [row for row in data if normalize_str(row.get(area_col)) == normalize_str(area_filter)
                            or normalize_str(row.get(area_col)) == "84"  # Belize M49 code
                           ]
                if not filtered:
                    print("  -> no Belize rows; writing full domain for debugging")
                    filtered = data

            # Write CSV
            out = out_dir / f"{dom.replace('/', '_')}.csv"
            # Collect all keys to make stable header
            keys: set[str] = set()
            for r in filtered:
                keys.update(r.keys())
            header = sorted(keys)
            with out.open("w", newline="", encoding="utf-8") as f:
                w = csv.DictWriter(f, fieldnames=header, extrasaction="ignore")
                w.writeheader()
                for r in filtered:
                    w.writerow(r)
            print(f"  -> wrote {out}")
            results.append({"domain": dom, "path": str(out)})
        except Exception as e:
            print(f"  -> error: {e}")
        time.sleep(0.5)

    manifest = {
        "source": "FAOSTAT Food Balance Sheets",
        "area_filter": area_filter,
        "generated_utc": utc_now(),
        "domains": domains,
        "items": results
    }
    (out_dir / "_manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    print(f"[FAO FBS] Wrote manifest at {out_dir / '_manifest.json'}")
    return manifest

# --------------------------------- main ---------------------------------------

def main():
    cfg = read_catalog("catalog.yml")
    # World Bank
    harvest_world_bank(cfg)
    # FAOSTAT (FBS)
    if cfg.get("faostat", {}).get("food_balance_sheets"):
        harvest_fao_fbs(cfg)

if __name__ == "__main__":
    main()
