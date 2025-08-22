#!/usr/bin/env python3
"""
Build script for World Bank + FAOSTAT Food Balance Sheets (multi-country).

- Reads config from catalog.yml
- Pulls World Bank indicators for listed ISO2 countries
- Pulls FAOSTAT FBS for listed ISO3 countries
- Writes: tidy CSVs, per-source manifests, indicator dictionary, and freshness stamp
- Uses simple 24h on-disk cache to reduce API calls
- Produces dataset cards if missing

Outputs:
  data/world_bank/{ISO2}/<indicator>.csv
  data/world_bank/_manifest.json
  data/world_bank/_dictionary.csv
  data/faostat_fbs/<ISO3>_fbs.csv
  data/faostat_fbs/_manifest.json
  data/_freshness.json
"""

import os
import time
import json
import yaml
import csv
import hashlib
import pathlib
from datetime import datetime, timedelta, timezone
from typing import Dict, Any, List, Tuple

import requests
import pandas as pd

ROOT = pathlib.Path(__file__).resolve().parents[1]  # repo root
CATALOG = ROOT / "catalog.yml"


def load_config() -> Dict[str, Any]:
    with open(CATALOG, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def ensure_dir(p: pathlib.Path):
    p.mkdir(parents=True, exist_ok=True)


def sha1(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8")).hexdigest()


def cache_get(cache_dir: pathlib.Path, key: str, ttl_hours: int):
    f = cache_dir / f"{sha1(key)}.json"
    if not f.exists():
        return None
    if ttl_hours > 0:
        mtime = datetime.fromtimestamp(f.stat().st_mtime, tz=timezone.utc)
        if datetime.now(timezone.utc) - mtime > timedelta(hours=ttl_hours):
            return None
    try:
        return json.loads(f.read_text(encoding="utf-8"))
    except Exception:
        return None


def cache_set(cache_dir: pathlib.Path, key: str, value: Any):
    ensure_dir(cache_dir)
    (cache_dir / f"{sha1(key)}.json").write_text(json.dumps(value), encoding="utf-8")


def wb_fetch_series(api_base: str, indicator: str, country_iso2: str, per_page: int, cache_dir: pathlib.Path, ttl: int):
    url = f"{api_base}/country/{country_iso2}/indicator/{indicator}?format=json&per_page={per_page}"
    cached = cache_get(cache_dir, url, ttl)
    if cached is not None:
        return cached
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    data = r.json()
    cache_set(cache_dir, url, data)
    return data


def wb_fetch_indicator_meta(api_base: str, indicator: str, cache_dir: pathlib.Path, ttl: int):
    url = f"{api_base}/indicator/{indicator}?format=json&per_page=20000"
    cached = cache_get(cache_dir, url, ttl)
    if cached is not None:
        return cached
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    data = r.json()
    cache_set(cache_dir, url, data)
    return data


def fao_fetch_fbs(api_base: str, country_iso3: str, elements: List[str], cache_dir: pathlib.Path, ttl: int):
    # Simple pull of entire FBS dataset by country, then filter elements locally to reduce request complexity
    url = f"{api_base}?area_code={country_iso3}&per_page=50000"
    cached = cache_get(cache_dir, url, ttl)
    if cached is None:
        r = requests.get(url, timeout=60)
        r.raise_for_status()
        cached = r.json()
        cache_set(cache_dir, url, cached)
    # Response format: { data: [ ... rows ... ], ... }
    rows = cached.get("data", [])
    if elements:
        rows = [r for r in rows if r.get("element") in elements]
    return rows


def write_manifest(path: pathlib.Path, manifest: Dict[str, Any]):
    path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")


def now_iso():
    return datetime.now(timezone.utc).isoformat()


def build_world_bank(cfg: Dict[str, Any], out_dir: pathlib.Path, cache_dir: pathlib.Path) -> Dict[str, Any]:
    wb = cfg["world_bank"]
    if not wb.get("enabled", True):
        return {}

    indicators: Dict[str, Dict[str, Any]] = wb["indicators"]
    countries: List[str] = cfg["project"]["countries"]
    per_page = wb.get("per_page", 20000)
    api_base = wb["api_base"]
    ttl = int(cfg["project"].get("cache_ttl_hours", 24))

    out = out_dir / "world_bank"
    ensure_dir(out)

    dictionary_rows: List[Dict[str, Any]] = []
    manifest_entries: List[Dict[str, Any]] = []

    for code, meta in indicators.items():
        # fetch indicator metadata (WB dict)
        md = wb_fetch_indicator_meta(api_base, code, cache_dir, ttl)
        title = meta.get("name", "")
        unit = meta.get("unit", "")
        group = meta.get("group", "")
        # WB meta name if available
        try:
            wb_name = md[1][0].get("name")
            wb_source_note = md[1][0].get("sourceNote")
        except Exception:
            wb_name, wb_source_note = None, None

        # record dictionary entry
        dictionary_rows.append({
            "indicator_code": code,
            "name": title or wb_name or "",
            "unit": unit,
            "group": group,
            "wb_name": wb_name or "",
            "wb_source_note": (wb_source_note or "").replace("\n", " ").strip()
        })

        for c in countries:
            data = wb_fetch_series(api_base, code, c, per_page, cache_dir, ttl)
            if not isinstance(data, list) or len(data) < 2:
                continue
            rows = data[1] or []
            if not rows:
                continue

            # tidy: year,value,country,iso2,indicator
            tidy = []
            for r in rows:
                tidy.append({
                    "country": r.get("country", {}).get("value"),
                    "iso2c": r.get("countryiso3code", "")[:2] if r.get("countryiso3code") else c,
                    "year": r.get("date"),
                    "indicator": code,
                    "value": r.get("value"),
                    "unit": unit or "",
                })

            df = pd.DataFrame(tidy)
            country_folder = out / c
            ensure_dir(country_folder)
            dest = country_folder / f"{code}.csv"
            df.sort_values(by=["year"], ascending=True).to_csv(dest, index=False)

            manifest_entries.append({
                "path": str(dest.as_posix()),
                "indicator": code,
                "country": c,
                "rows": int(df.shape[0]),
                "updated_at": now_iso()
            })

    # write dictionary and manifest
    dict_path = out / "_dictionary.csv"
    pd.DataFrame(dictionary_rows).to_csv(dict_path, index=False)

    manifest = {
        "source": "World Bank Open Data",
        "generated_at": now_iso(),
        "items": manifest_entries
    }
    write_manifest(out / "_manifest.json", manifest)

    # dataset card (create once)
    card = out / "_dataset_card.md"
    if not card.exists():
        card.write_text(
            "# World Bank Indicators (Caribbean)\n\n"
            "This folder contains per-country CSVs for indicators defined in catalog.yml.\n\n"
            "## Columns\n- country\n- iso2c\n- year\n- indicator (code)\n- value\n- unit\n\n"
            "## Notes\n- Source: World Bank Open Data API\n- See `_dictionary.csv` for indicator details.\n",
            encoding="utf-8"
        )
    return {"world_bank": manifest}


def build_faostat_fbs(cfg: Dict[str, Any], out_dir: pathlib.Path, cache_dir: pathlib.Path) -> Dict[str, Any]:
    fwo = cfg["faostat_fbs"]
    if not fwo.get("enabled", True):
        return {}

    api_base = fwo["api_base"]
    countries_iso3: List[str] = fwo["countries_iso3"]
    elements = fwo.get("elements", [])
    ttl = int(cfg["project"].get("cache_ttl_hours", 24))

    out = out_dir / fwo.get("out_folder", "faostat_fbs")
    ensure_dir(out)

    manifest_entries: List[Dict[str, Any]] = []

    for iso3 in countries_iso3:
        rows = fao_fetch_fbs(api_base, iso3, elements, cache_dir, ttl)
        if not rows:
            continue
        df = pd.DataFrame(rows)
        # normalize common fields
        keep = [c for c in df.columns if c in ("area_code", "area", "item_code", "item", "element", "year", "value", "unit")]
        if keep:
            df = df[keep]
        dest = out / f"{iso3}_fbs.csv"
        df.sort_values(by=["item", "element", "year"], ascending=True).to_csv(dest, index=False)
        manifest_entries.append({
            "path": str(dest.as_posix()),
            "country_iso3": iso3,
            "rows": int(df.shape[0]),
            "updated_at": now_iso()
        })

    manifest = {
        "source": "FAOSTAT — Food Balance Sheets",
        "generated_at": now_iso(),
        "items": manifest_entries
    }
    write_manifest(out / "_manifest.json", manifest)

    # dataset card (create once)
    card = out / "_dataset_card.md"
    if not card.exists():
        card.write_text(
            "# FAOSTAT Food Balance Sheets (Caribbean)\n\n"
            "Per-country food balance sheets with common elements.\n\n"
            "## Columns (common)\n- area_code, area, item_code, item, element, year, value, unit\n\n"
            "## Notes\n- Source: FAOSTAT API (FBS).\n",
            encoding="utf-8"
        )
    return {"faostat_fbs": manifest}


def write_freshness(out_dir: pathlib.Path, parts: Dict[str, Any]):
    stamp = {
        "generated_at": now_iso(),
        "sources": {k: v.get("generated_at") for k, v in parts.items() if v}
    }
    (out_dir / "_freshness.json").write_text(json.dumps(stamp, indent=2), encoding="utf-8")


def main():
    cfg = load_config()
    out_dir = ROOT / cfg["project"]["out_dir"]
    cache_dir = ROOT / cfg["project"]["cache_dir"]
    ensure_dir(out_dir)
    ensure_dir(cache_dir)

    parts = {}
    parts.update(build_world_bank(cfg, out_dir, cache_dir))
    parts.update(build_faostat_fbs(cfg, out_dir, cache_dir))

    write_freshness(out_dir, parts)

    print("Build complete ✅")


if __name__ == "__main__":
    main()
