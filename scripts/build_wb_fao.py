#!/usr/bin/env python3
import os, io, time, json, zipfile, hashlib, datetime
from pathlib import Path

import requests
import pandas as pd
import yaml

ROOT = Path(__file__).resolve().parents[1]
CATALOG = ROOT / "catalog.yaml"
DATA = ROOT / "data"
META = DATA / "_meta"
DATA.mkdir(parents=True, exist_ok=True); META.mkdir(parents=True, exist_ok=True)

def sha256_of(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1<<20), b""): h.update(chunk)
    return h.hexdigest()

def linecount_csv(path: Path) -> int:
    with open(path, "rb") as f:
        return max(0, sum(1 for _ in f) - 1)

def backoff(i): time.sleep(2**i)

# ---------------- World Bank ----------------
def wb_fetch(country_iso3: str, indicators: dict):
    base = "https://api.worldbank.org/v2/country/{cc}/indicator/{code}"
    prov, wb_names = [], {}  # wb_names: code -> human name
    for code, relpath in indicators.items():
        url = base.format(cc=country_iso3, code=code)
        params = {"format":"json","per_page":20000}
        try:
            r = requests.get(url, params=params, timeout=90); r.raise_for_status()
            js = r.json()
            rows = js[1] if isinstance(js, list) and len(js)>1 else []
            if not rows:
                print(f"[WB][WARN] {code}: empty"); continue
            df = pd.DataFrame(rows)
            # try to extract readable indicator name from first row
            try:
                ind_name = df["indicator"].iloc[0]["value"]
            except Exception:
                ind_name = code
            wb_names[code] = ind_name
            out = DATA / relpath; out.parent.mkdir(parents=True, exist_ok=True)
            df_out = df[["indicator","country","date","value"]].copy()
            df_out["indicator_code"] = code
            df_out.to_csv(out, index=False)
            prov.append({"source":"WorldBank WDI","indicator":code,"indicator_name":ind_name,"url":r.url,"path":str(out)})
            print(f"[WB] {code} -> {out}")
        except Exception as e:
            print(f"[WB][WARN] {code}: {e}")
    return prov, wb_names

# ---------------- FAOSTAT ----------------
def save_bytes(url, outfile, params=None, tries=3, timeout=120):
    outfile = Path(outfile); outfile.parent.mkdir(parents=True, exist_ok=True)
    last = None
    for i in range(tries):
        try:
            r = requests.get(url, params=params, timeout=timeout, stream=True)
            r.raise_for_status()
            with open(outfile, "wb") as f:
                for chunk in r.iter_content(8192):
                    if chunk: f.write(chunk)
            return True
        except Exception as e:
            last = e; backoff(i)
    print(f"[GET][ERROR] {url}: {last}")
    return False

def fao_fetch(downloads: list):
    """Try FAOSTAT CSV → JSON; fallback to HDX mirror (CSV) if FAO is down."""
    prov, fao_files = [], []  # fao_files: list of {"domain","path"}
    for item in downloads:
        domain = item["domain"]            # Food_Security or Prices
        out = DATA / item["outfile"]
        # 1) CSV endpoint (fast but flaky)
        csv_url = f"https://fenixservices.fao.org/faostat/api/v1/en/{domain}/data"
        if save_bytes(csv_url, out, params={"area_code":"084","downloadFormat":"csv"}):
            prov.append({"source":"FAOSTAT","domain":domain,"url":csv_url,"path":str(out)})
            fao_files.append({"domain":domain, "path": str(out)})
            print(f"[FAO] {domain} CSV -> {out}"); continue
        # 2) JSON endpoint → CSV
        try:
            json_url = f"https://fenixservices.fao.org/faostat/api/v1/en/{domain}"
            js = requests.get(json_url, params={"area_code":"084","page_size":50000}, timeout=120).json()
            data = js.get("data", [])
            if data:
                pd.DataFrame(data).to_csv(out, index=False)
                prov.append({"source":"FAOSTAT","domain":domain,"url":json_url,"path":str(out)})
                fao_files.append({"domain":domain, "path": str(out)})
                print(f"[FAO] {domain} JSON -> {out}"); continue
        except Exception as e:
            print(f"[FAO][WARN] {domain} JSON: {e}")
        # 3) HDX mirror fallback (CSV)
        try:
            search = requests.get(
                "https://data.humdata.org/api/3/action/package_search",
                params={"q": f"FAOSTAT {domain} Belize CSV", "rows": 5}, timeout=120
            ).json()["result"]["results"]
            got = False
            for pkg in search:
                for rsrc in pkg.get("resources", []):
                    if (rsrc.get("format","") or "").lower()=="csv" and rsrc.get("url"):
                        if save_bytes(rsrc["url"], out):
                            prov.append({"source":"HDX (FAOSTAT mirror)","title":pkg.get("title"),
                                         "url":rsrc["url"],"path":str(out)})
                            fao_files.append({"domain":domain, "path": str(out)})
                            print(f"[FAO][HDX] {domain} -> {out}")
                            got = True; break
                if got: break
            if not got: print(f"[FAO][ERROR] No HDX CSV found for {domain}.")
        except Exception as e:
            print(f"[FAO][WARN] HDX search: {e}")
    return prov, fao_files


# ---------------- Fetch examples of messy ----------------
def fetch_messy(items):
    prov = []
    for it in items or []:
        out = (ROOT / "data" / it["outfile"]); out.parent.mkdir(parents=True, exist_ok=True)
        try:
            r = requests.get(it["url"], timeout=180, stream=True); r.raise_for_status()
            with open(out, "wb") as f:
                for ch in r.iter_content(8192):
                    if ch: f.write(ch)
            prov.append({"source":"MessyURL","url":it["url"],"path":str(out),
                         "license":it.get("license"),"notes":it.get("notes")})
            print(f"[MESSY] {it['url']} -> {out}")
        except Exception as e:
            print(f"[MESSY][WARN] {it['url']}: {e}")
    return prov

# ---------------- Metadata & Packaging ----------------
def write_checks_and_manifest(prov, catalog_text: str):
    files = []
    for p in DATA.rglob("*.csv"):
        files.append({
            "path": str(p.relative_to(ROOT)),
            "rows": linecount_csv(p),
            "bytes": p.stat().st_size,
            "sha256": sha256_of(p)
        })
    manifest = {
        "built_at_utc": datetime.datetime.utcnow().isoformat(timespec="seconds") + "Z",
        "builder_commit": os.getenv("GITHUB_SHA"),
        "repository": os.getenv("GITHUB_REPOSITORY"),
        "catalog_sha256": hashlib.sha256(catalog_text.encode("utf-8")).hexdigest(),
        "sources": prov,
        "files": files
    }
    (META/"checksums.json").write_text(
        json.dumps({f["path"]: f["sha256"] for f in files}, indent=2), encoding="utf-8")
    (META/"manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    print(f"[META] wrote checksums.json & manifest.json")

def write_data_dictionary(wb_names: dict, fao_files: list):
    """Create data/_meta/data_dictionary.md (WB indicators + FAO file summaries)."""
    lines = []
    lines.append("# Belize Data Pack — Data Dictionary\n")
    lines.append("This document is auto-generated at build time.\n")

    # World Bank section
    if wb_names:
        lines.append("## World Bank indicators\n")
        lines.append("| Code | Indicator name | CSV path | Reference |\n|---|---|---|---|\n")
        for code, name in sorted(wb_names.items()):
            path = _guess_wb_path_for(code)
            link = f"https://data.worldbank.org/indicator/{code}"
            lines.append(f"| `{code}` | {name} | `{path}` | {link} |\n")
        lines.append("\n**Common columns:** `indicator` (object in source; name string in CSV), `country`, `date` (year), `value`, `indicator_code`.\n")

    # FAOSTAT section
    if fao_files:
        lines.append("## FAOSTAT files\n")
        for item in fao_files:
            p = Path(item["path"])
            lines.append(f"### {item['domain']} — `{p.relative_to(ROOT)}`\n")
            # Try to inspect columns and sample units
            try:
                df = pd.read_csv(p, nrows=200)
                cols = ", ".join(df.columns[:20]) + ("…" if len(df.columns) > 20 else "")
                # Pick a likely unit column
                unit_cols = [c for c in df.columns if c.lower() in ("unit","units")]
                unit_note = ""
                if unit_cols:
                    samples = df[unit_cols[0]].dropna().astype(str).unique()[:5]
                    unit_note = f"  \n**Units (sample):** {', '.join(samples)}"
                lines.append(f"**Columns (first 20):** {cols}{unit_note}\n")
            except Exception as e:
                lines.append(f"_Could not inspect columns (read error: {e})._\n")
        lines.append("\n**Note:** FAOSTAT schemas vary by domain. Expect fields like `Year`, `Area`, `Item`, `Element`, `Value`, `Unit`.\n")

    out = META / "data_dictionary.md"
    out.write_text("\n".join(lines), encoding="utf-8")
    print(f"[META] data dictionary -> {out}")
    return out

def _guess_wb_path_for(code: str) -> str:
    # map known codes to the relative CSV paths used in catalog.yaml
    mapping = {
        "NY.GDP.MKTP.CD":"data/economy/gdp_current_usd.csv",
        "NY.GDP.PCAP.CD":"data/economy/gdp_per_capita_current_usd.csv",
        "FP.CPI.TOTL.ZG":"data/economy/inflation_annual_pct.csv",
        "NE.EXP.GNFS.ZS":"data/economy/exports_pct_gdp.csv",
        "NE.IMP.GNFS.ZS":"data/economy/imports_pct_gdp.csv",
        "SP.POP.TOTL":"data/social/population_total.csv",
        "SP.DYN.LE00.IN":"data/health/life_expectancy_total_years.csv",
        "SH.XPD.CHEX.GD.ZS":"data/health/health_expenditure_pct_gdp.csv",
        "SE.SEC.ENRR":"data/education/secondary_enrolment_gross_pct.csv",
        "IT.NET.USER.ZS":"data/social/internet_users_pct.csv",
        "SI.POV.DDAY":"data/poverty/extreme_poverty_2_15_usd_ppp_pct.csv",
        "SG.GEN.PARL.ZS":"data/gender/women_in_parliament_pct.csv",
        "ST.INT.ARVL":"data/tourism/international_tourist_arrivals.csv",
    }
    return mapping.get(code, f"data/<unknown_path_for_{code}>.csv")

def zip_output(zipname="belize-wb-fao-pack.zip"):
    zpath = ROOT / zipname
    with zipfile.ZipFile(zpath, "w", zipfile.ZIP_DEFLATED) as z:
        for p in (ROOT/"data").rglob("*"):
            if p.is_file(): z.write(p, p.relative_to(ROOT))
        z.write(ROOT/"catalog.yaml", "catalog.yaml")
        z.write(ROOT/"data/_meta/manifest.json", "_meta/manifest.json")
        z.write(ROOT/"data/_meta/checksums.json", "_meta/checksums.json")
        z.write(ROOT/"data/_meta/data_dictionary.md", "_meta/data_dictionary.md")
    print(f"[ZIP] {zpath}")
    return str(zpath)

def main():
    catalog_text = (ROOT/"catalog.yaml").read_text(encoding="utf-8")
    cfg = yaml.safe_load(catalog_text)
    prov = []

    print("== World Bank ==")
    wb_prov, wb_names = wb_fetch(cfg["country"], cfg["world_bank"]["indicators"]); prov += wb_prov

    print("== FAOSTAT ==")
    fao_prov, fao_files = fao_fetch(cfg["fao"]["downloads"]); prov += fao_prov

    # metadata outputs
    write_checks_and_manifest(prov, catalog_text)
    write_data_dictionary(wb_names, fao_files)

    z = zip_output()
    print("DONE:", z)

if __name__ == "__main__":
    main()
