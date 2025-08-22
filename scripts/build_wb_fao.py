#!/usr/bin/env python3
"""
Build script for World Bank + FAOSTAT Food Balance Sheets (multi-country).

- Reads config from catalog.yml
- Pulls World Bank indicators for listed ISO2 countries
- Pulls FAOSTAT FBS for listed ISO3 countries
- Writes: tidy CSVs, per-source manifests, indicator dictionary, freshness stamp
- Uses 24h on-disk cache + robust HTTP retries/backoff
- Fail-soft: records errors to _errors.json instead of crashing the build

Outputs:
  data/world_bank/{ISO2}/<indicator>.csv
  data/world_bank/_manifest.json
  data/world_bank/_dictionary.csv
  data/faostat_fbs/<ISO3>_fbs.csv
  data/faostat_fbs/_manifest.json
  data/_freshness.json
"""

import csv
import hashlib
import json
import os
import random
import pathlib
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List

import pandas as pd
import requests
import yaml
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ---------------------------------------------------------------------------

ROOT = pathlib.Path(__file__).resolve().parents[1]  # repo root
CATALOG = ROOT / "catalog.yml"

# -------------------------- util & caching ---------------------------------

def ensure_dir(p: pathlib.Path) -> None:
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

def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")

# -------------------------- robust HTTP ------------------------------------

def _make_session():
    s = requests.Session()
    retries = int(os.getenv("CARIBDATA_HTTP_RETRIES", "6"))
    backoff = float(os.getenv("CARIBDATA_HTTP_BACKOFF", "0.8"))
    retry = Retry(
        total=retries,
        connect=retries,
        read=retries,
        status=retries,
        backoff_factor=backoff,           # 0.8, 1.6, 3.2, ...
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET", "HEAD", "OPTIONS"],
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=20, pool_maxsize=50)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    s.headers["User-Agent"] = "CaribData/1.0 (+github.com/CaribData)"
    return s

SESSION = _make_session()
HTTP_TIMEOUT = float(os.getenv("CARIBDATA_HTTP_TIMEOUT", "90"))

def http_get(url: str, params=None, timeout: float = HTTP_TIMEOUT):
    time.sleep(random.uniform(0.05, 0.25))  # jitter
    r = SESSION.get(url, params=params, timeout=timeout)
    r.raise_for_status()
    return r

# ------------------------ config -------------------------------------------

def load_config() -> Dict[str, Any]:
    with open(CATALOG, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

# ------------------------ World Bank ---------------------------------------

def wb_fetch_series(api_base: str, indicator: str, country_iso2: str, per_page: int,
                    cache_dir: pathlib.Path, ttl: int):
    url = f"{api_base}/country/{country_iso2}/indicator/{indicator}"
    params = {"format": "json", "per_page": per_page}
    cache_key = url + "?" + "&".join(f"{k}={v}" for k, v in sorted(params.items()))
    cached = cache_get(cache_dir, cache_key, ttl)
    if cached is not None:
        return cached
    r = http_get(url, params=params)
    data = r.json()
    cache_set(cache_dir, cache_key, data)
    return data

def wb_fetch_indicator_meta(api_base: str, indicator: str, cache_dir: pathlib.Path, ttl: int):
    url = f"{api_base}/indicator/{indicator}"
    params = {"format": "json", "per_page": 20000}
    cache_key = url + "?" + "&".join(f"{k}={v}" for k, v in sorted(params.items()))
    cached = cache_get(cache_dir, cache_key, ttl)
    if cached is not None:
        return cached
    r = http_get(url, params=params)
    data = r.json()
    cache_set(cache_dir, cache_key, data)
    return data

def build_world_bank(cfg: Dict[str, Any], out_dir: pathlib.Path, cache_dir: pathlib.Path) -> Dict[str, Any]:
    wb = cfg.get("world_bank", {})
    if not wb.get("enabled", True):
        return {}

    indicators: Dict[str, Dict[str, Any]] = wb.get("indicators", {})
    countries: List[str] = cfg.get("project", {}).get("countries", [])
    per_page = int(wb.get("per_page", 20000))
    api_base = wb.get("api_base", "https://api.worldbank.org/v2")
    ttl = int(cfg.get("project", {}).get("cache_ttl_hours", 24))

    out = out_dir / "world_bank"
    ensure_dir(out)

    dictionary_rows: List[Dict[str, Any]] = []
    manifest_entries: List[Dict[str, Any]] = []
    errors: List[Dict[str, Any]] = []

    for code, meta in indicators.items():
        # indicator dictionary (API + local overrides)
        wb_name = wb_source_note = ""
        try:
            md = wb_fetch_indicator_meta(api_base, code, cache_dir, ttl)
            if isinstance(md, list) and len(md) > 1 and md[1]:
                wb_name = md[1][0].get("name") or ""
                wb_source_note = (md[1][0].get("sourceNote") or "").replace("\n", " ").strip()
        except Exception as e:
            errors.append({"stage": "wb_meta", "indicator": code, "error": str(e)})

        dictionary_rows.append({
            "indicator_code": code,
            "name": meta.get("name", "") or wb_name,
            "unit": meta.get("unit", ""),
            "group": meta.get("group", ""),
            "wb_name": wb_name,
            "wb_source_note": wb_source_note
        })

        # data per country
        for c in countries:
            try:
                data = wb_fetch_series(api_base, code, c, per_page, cache_dir, ttl)
                if not (isinstance(data, list) and len(data) > 1 and data[1]):
                    continue
                rows = data[1]
                tidy = []
                for r in rows:
                    tidy.append({
                        "country": (r.get("country") or {}).get("value"),
                        "iso2c": c,
                        "year": r.get("date"),
                        "indicator": code,
                        "value": r.get("value"),
                        "unit": meta.get("unit", "")
                    })
                df = pd.DataFrame(tidy)
                if df.empty:
                    continue
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
            except Exception as e:
                errors.append({"stage": "wb_data", "indicator": code, "country": c, "error": str(e)})
                continue

    # write dictionary & manifest
    pd.DataFrame(dictionary_rows).to_csv(out / "_dictionary.csv", index=False)
    manifest = {"source": "World Bank Open Data", "generated_at": now_iso(), "items": manifest_entries}
    (out / "_manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    if errors:
        (out / "_errors.json").write_text(json.dumps(errors, indent=2), encoding="utf-8")

    # dataset card (if missing)
    card = out / "_dataset_card.md"
    if not card.exists():
        card.write_text(
            "# World Bank Indicators (Caribbean)\n\n"
            "Per-country CSVs for indicators listed in `catalog.yml`.\n\n"
            "## Columns\n- country\n- iso2c\n- year\n- indicator\n- value\n- unit\n\n"
            "See `_dictionary.csv` for names and notes.\n",
            encoding="utf-8"
        )
    return {"world_bank": manifest}

# ------------------------ FAOSTAT FBS --------------------------------------

def fao_fetch_fbs(api_base: str, country_iso3: str, elements: List[str],
                  cache_dir: pathlib.Path, ttl: int):
    url = f"{api_base}"
    params = {"area_code": country_iso3, "per_page": 50000}
    cache_key = url + "?" + "&".join(f"{k}={v}" for k, v in sorted(params.items()))
    cached = cache_get(cache_dir, cache_key, ttl)
    if cached is None:
        r = http_get(url, params=params, timeout=max(HTTP_TIMEOUT, 120))
        cached = r.json()
        cache_set(cache_dir, cache_key, cached)
    rows = cached.get("data", []) if isinstance(cached, dict) else []
    if elements:
        rows = [r for r in rows if r.get("element") in elements]
    return rows

def build_faostat_fbs(cfg: Dict[str, Any], out_dir: pathlib.Path, cache_dir: pathlib.Path) -> Dict[str, Any]:
    fwo = cfg.get("faostat_fbs", {})
    if not fwo.get("enabled", True):
        return {}

    api_base = fwo.get("api_base", "https://fenixservices.fao.org/api/faostat/api/v1/en/FBS")
    countries_iso3: List[str] = fwo.get("countries_iso3", [])
    elements = fwo.get("elements", [])
    ttl = int(cfg.get("project", {}).get("cache_ttl_hours", 24))

    out = out_dir / fwo.get("out_folder", "faostat_fbs")
    ensure_dir(out)

    manifest_entries: List[Dict[str, Any]] = []
    errors: List[Dict[str, Any]] = []

    for iso3 in countries_iso3:
        try:
            rows = fao_fetch_fbs(api_base, iso3, elements, cache_dir, ttl)
            if not rows:
                continue
            df = pd.DataFrame(rows)
            keep = [c for c in df.columns if c in ("area_code", "area", "item_code", "item", "element", "year", "value", "unit")]
            if keep:
                df = df[keep]
            dest = out / f"{iso3}_fbs.csv"
            df.sort_values(by=[col for col in ["item", "element", "year"] if col in df.columns], ascending=True).to_csv(dest, index=False)
            manifest_entries.append({
                "path": str(dest.as_posix()),
                "country_iso3": iso3,
                "rows": int(df.shape[0]),
                "updated_at": now_iso()
            })
        except Exception as e:
            errors.append({"stage": "fao_fbs", "country_iso3": iso3, "error": str(e)})
            continue

    manifest = {"source": "FAOSTAT — Food Balance Sheets", "generated_at": now_iso(), "items": manifest_entries}
    (out / "_manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    if errors:
        (out / "_errors.json").write_text(json.dumps(errors, indent=2), encoding="utf-8")

    # dataset card (if missing)
    card = out / "_dataset_card.md"
    if not card.exists():
        card.write_text(
            "# FAOSTAT Food Balance Sheets (Caribbean)\n\n"
            "Per-country food balance sheets with common elements.\n\n"
            "## Columns (common)\n- area_code, area, item_code, item, element, year, value, unit\n",
            encoding="utf-8"
        )
    return {"faostat_fbs": manifest}

# ------------------------ freshness ----------------------------------------

def write_freshness(out_dir: pathlib.Path, parts: Dict[str, Any]):
    stamp = {
        "generated_at": now_iso(),
        "sources": {k: v.get("generated_at") for k, v in parts.items() if isinstance(v, dict) and v.get("generated_at")}
    }
    (out_dir / "_freshness.json").write_text(json.dumps(stamp, indent=2), encoding="utf-8")

# ------------------------ main ---------------------------------------------

def main():
    cfg = load_config()
    proj = cfg.get("project", {})
    out_dir = ROOT / proj.get("out_dir", "data")
    cache_dir = ROOT / proj.get("cache_dir", ".cache")
    ensure_dir(out_dir)
    ensure_dir(cache_dir)

    parts = {}
    parts.update(build_world_bank(cfg, out_dir, cache_dir))
    parts.update(build_faostat_fbs(cfg, out_dir, cache_dir))
    write_freshness(out_dir, parts)

    print("Build complete ✅")

if __name__ == "__main__":
    main()
