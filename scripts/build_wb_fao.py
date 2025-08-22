#!/usr/bin/env python3
"""
Build script for World Bank + FAOSTAT Food Balance Sheets (multi-country).

Strategy:
1) World Bank via API (with retries/backoff)
2) FAOSTAT FBS: try API; on failure, fallback to bulk ZIP mirrors and filter locally

Outputs:
  data/world_bank/{ISO2}/<indicator>.csv
  data/world_bank/_manifest.json
  data/world_bank/_dictionary.csv
  data/faostat_fbs/<ISO3>_fbs.csv
  data/faostat_fbs/_manifest.json
  data/_freshness.json
  (optional) data/world_bank/_errors.json, data/faostat_fbs/_errors.json
"""

import csv
import hashlib
import json
import os
import random
import pathlib
import time
import io
import zipfile
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

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

    pd.DataFrame(dictionary_rows).to_csv(out / "_dictionary.csv", index=False)
    manifest = {"source": "World Bank Open Data", "generated_at": now_iso(), "items": manifest_entries}
    (out / "_manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    if errors:
        (out / "_errors.json").write_text(json.dumps(errors, indent=2), encoding="utf-8")

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

M49_BY_ISO3 = {"BLZ": 84, "JAM": 388, "TTO": 780, "GUY": 328}
NAME_BY_ISO3 = {"BLZ": "Belize", "JAM": "Jamaica", "TTO": "Trinidad and Tobago", "GUY": "Guyana"}

def _normalize_fao_payload(obj) -> List[dict]:
    if isinstance(obj, dict) and "data" in obj and isinstance(obj["data"], list):
        return obj["data"]
    if isinstance(obj, list):
        return obj
    return []

def fao_fetch_domain(base: str, domain: str, params: dict, cache_dir: pathlib.Path, ttl: int, timeout: float) -> List[dict]:
    url = f"{base}/{domain}"
    cache_key = url + "?" + "&".join(f"{k}={v}" for k, v in sorted(params.items())) if params else url
    cached = cache_get(cache_dir, cache_key, ttl)
    if cached is None:
        r = http_get(url, params=params or None, timeout=timeout)
        try:
            cached = r.json()
        except Exception:
            cached = {}
        cache_set(cache_dir, cache_key, cached)
    return _normalize_fao_payload(cached)

def _choose_csv_in_zip(zf: zipfile.ZipFile) -> Optional[str]:
    # Prefer the CSV whose name matches the ZIP (All_Data) else first CSV
    candidates = [n for n in zf.namelist() if n.lower().endswith(".csv")]
    if not candidates:
        return None
    candidates.sort(key=lambda s: (0 if "All_Data" in s or "all_data" in s.lower() else 1, len(s)))
    return candidates[0]

def _read_bulk_zip_to_df(zip_bytes: bytes, usecols: Optional[List[str]] = None) -> pd.DataFrame:
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        name = _choose_csv_in_zip(zf)
        if not name:
            return pd.DataFrame()
        with zf.open(name) as f:
            return pd.read_csv(f, low_memory=False, usecols=usecols)

def _std_cols(df: pd.DataFrame) -> pd.DataFrame:
    # Flexible rename to a common schema
    rename_map = {}
    for col in df.columns:
        c = col.strip().lower()
        if c in ("area code (m49)", "m49_code", "area_code", "areacode"):
            rename_map[col] = "area_code"
        elif c == "area":
            rename_map[col] = "area"
        elif c in ("item code", "item_code"):
            rename_map[col] = "item_code"
        elif c == "item":
            rename_map[col] = "item"
        elif c == "element":
            rename_map[col] = "element"
        elif c == "year":
            rename_map[col] = "year"
        elif c == "value":
            rename_map[col] = "value"
        elif c == "unit":
            rename_map[col] = "unit"
    return df.rename(columns=rename_map)

def _filter_country_elements(df: pd.DataFrame, iso3: str, elements: List[str]) -> pd.DataFrame:
    m49 = M49_BY_ISO3.get(iso3)
    name = NAME_BY_ISO3.get(iso3, iso3)
    if "area_code" in df.columns and pd.api.types.is_numeric_dtype(df["area_code"]):
        df = df[df["area_code"] == m49]
    elif "area" in df.columns:
        df = df[df["area"].astype(str).str.strip().str.lower() == name.lower()]
    if elements and "element" in df.columns:
        df = df[df["element"].astype(str).isin(set(elements))]
    return df

def _download_with_cache(url: str, cache_dir: pathlib.Path, ttl_hours: int, timeout: float) -> Optional[bytes]:
    # Save ZIP bytes in cache_dir / 'faostat_bulk' / sha1(url).zip
    bulk_dir = cache_dir / "faostat_bulk"
    ensure_dir(bulk_dir)
    p = bulk_dir / f"{sha1(url)}.zip"
    if p.exists():
        mtime = datetime.fromtimestamp(p.stat().st_mtime, tz=timezone.utc)
        if datetime.now(timezone.utc) - mtime <= timedelta(hours=ttl_hours):
            return p.read_bytes()
    try:
        r = http_get(url, timeout=timeout)
        p.write_bytes(r.content)
        return r.content
    except Exception:
        return None

def build_faostat_fbs(cfg: Dict[str, Any], out_dir: pathlib.Path, cache_dir: pathlib.Path) -> Dict[str, Any]:
    fwo = cfg.get("faostat_fbs", {})
    if not fwo.get("enabled", True):
        return {}

    # API attempt (can be flaky)
    api_base = fwo.get("api_base", "https://fenixservices.fao.org/faostat/api/v1/en")
    domains: List[str] = fwo.get("domains", ["FBS/FBS"])
    countries_iso3: List[str] = fwo.get("countries_iso3", [])
    elements = fwo.get("elements", [])
    ttl = int(cfg.get("project", {}).get("cache_ttl_hours", 24))

    # Bulk mirrors (robust)
    bulk_urls: List[str] = fwo.get("bulk_urls", [
        "https://bulks-faostat.fao.org/production/FoodBalanceSheets_E_All_Data_(Normalized).zip",
        "https://fenixservices.fao.org/faostat/static/bulkdownloads/FoodBalanceSheets_E_All_Data_(Normalized).zip"
    ])

    out = out_dir / fwo.get("out_folder", "faostat_fbs")
    ensure_dir(out)

    manifest_entries: List[Dict[str, Any]] = []
    errors: List[Dict[str, Any]] = []

    # ---------- Try API first ----------
    api_got_any = False
    try:
        for iso3 in countries_iso3:
            combined = []
            for dom in domains:
                try:
                    rows = fao_fetch_domain(
                        api_base, dom,
                        params={"area_code": M49_BY_ISO3.get(iso3), "per_page": 50000},
                        cache_dir=cache_dir, ttl=ttl, timeout=max(HTTP_TIMEOUT, 120)
                    )
                except Exception as e:
                    errors.append({"stage": "fao_api_fetch", "country_iso3": iso3, "domain": dom, "error": str(e)})
                    rows = []
                if not rows:
                    continue
                api_got_any = True
                df = pd.DataFrame(rows)
                df = _std_cols(df)
                df = _filter_country_elements(df, iso3, elements)
                if df.empty:
                    continue
                df["_source"] = "api"
                df["_domain"] = dom
                combined.append(df)

            if combined:
                out_df = pd.concat(combined, ignore_index=True)
                keep = [c for c in out_df.columns if c in ("area_code","area","item_code","item","element","year","value","unit","_domain","_source")]
                if keep:
                    out_df = out_df[keep]
                dest = out / f"{iso3}_fbs.csv"
                sort_cols = [c for c in ["_domain","item","element","year"] if c in out_df.columns]
                if sort_cols:
                    out_df.sort_values(by=sort_cols, inplace=True)
                out_df.to_csv(dest, index=False)
                manifest_entries.append({
                    "path": str(dest.as_posix()),
                    "country_iso3": iso3,
                    "rows": int(out_df.shape[0]),
                    "updated_at": now_iso()
                })
    except Exception as e:
        errors.append({"stage": "fao_api_top", "error": str(e)})

    # ---------- Fallback to BULK (if API yielded nothing for some/all countries) ----------
    need_bulk_for = [iso3 for iso3 in countries_iso3 if not (out / f"{iso3}_fbs.csv").exists()]
    if need_bulk_for:
        bulk_df = pd.DataFrame()
        last_used_url = None
        for url in bulk_urls:
            try:
                b = _download_with_cache(url, cache_dir, ttl_hours=ttl, timeout=max(HTTP_TIMEOUT, 180))
                if not b:
                    continue
                df = _read_bulk_zip_to_df(b)
                if df is None or df.empty:
                    continue
                df = _std_cols(df)
                last_used_url = url
                bulk_df = df
                break
            except Exception as e:
                errors.append({"stage": "fao_bulk_download", "url": url, "error": str(e)})
                continue

        if not bulk_df.empty:
            for iso3 in need_bulk_for:
                try:
                    part = _filter_country_elements(bulk_df, iso3, elements)
                    if part.empty:
                        continue
                    part["_source"] = "bulk"
                    part["_domain"] = "FBS_BULK"
                    keep = [c for c in part.columns if c in ("area_code","area","item_code","item","element","year","value","unit","_domain","_source")]
                    part = part[keep]
                    dest = out / f"{iso3}_fbs.csv"
                    sort_cols = [c for c in ["item","element","year"] if c in part.columns]
                    if sort_cols:
                        part.sort_values(by=sort_cols, inplace=True)
                    part.to_csv(dest, index=False)
                    manifest_entries.append({
                        "path": str(dest.as_posix()),
                        "country_iso3": iso3,
                        "rows": int(part.shape[0]),
                        "updated_at": now_iso(),
                        "bulk_url": last_used_url
                    })
                except Exception as e:
                    errors.append({"stage": "fao_bulk_filter", "country_iso3": iso3, "error": str(e)})
        else:
            errors.append({"stage": "fao_bulk_all_failed", "message": "All bulk mirrors failed or returned empty"})

    # ---------- Wrap up ----------
    manifest = {"source": "FAOSTAT — Food Balance Sheets", "generated_at": now_iso(), "items": manifest_entries}
    (out / "_manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    if errors:
        (out / "_errors.json").write_text(json.dumps(errors, indent=2), encoding="utf-8")

    card = out / "_dataset_card.md"
    if not card.exists():
        card.write_text(
            "# FAOSTAT Food Balance Sheets (Caribbean)\n\n"
            "API first, then bulk ZIP fallback; per-country extracts with provenance columns.\n\n"
            "## Columns (common)\n- area_code, area, item_code, item, element, year, value, unit, _domain, _source\n",
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
