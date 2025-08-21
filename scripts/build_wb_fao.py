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
    # count rows excluding header
    with open(path, "rb") as f:
        return max(0, sum(1 for _ in f) - 1)

def backoff(i): time.sleep(2**i)

def wb_fetch(country_iso3: str, indicators: dict):
    base = "https://api.worldbank.org/v2/country/{cc}/indicator/{code}"
    prov = []
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
            out = DATA / relpath; out.parent.mkdir(parents=True, exist_ok=True)
            df_out = df[["indicator","country","date","value"]].copy()
            df_out["indicator_code"] = code
            df_out.to_csv(out, index=False)
            prov.append({"source":"WorldBank WDI","indicator":code,"url":r.url,"path":str(out)})
            print(f"[WB] {code} -> {out}")
        except Exception as e:
            print(f"[WB][WARN] {code}: {e}")
    return prov

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
    prov = []
    for item in downloads:
        domain = item["domain"]            # Food_Security or Prices
        out = DATA / item["outfile"]
        # 1) CSV endpoint (fast but flaky)
        csv_url = f"https://fenixservices.fao.org/faostat/api/v1/en/{domain}/data"
        if save_bytes(csv_url, out, params={"area_code":"084","downloadFormat":"csv"}):
            prov.append({"source":"FAOSTAT","domain":domain,"url":csv_url,"path":str(out)})
            print(f"[FAO] {domain} CSV -> {out}"); continue
        # 2) JSON endpoint → CSV
        try:
            json_url = f"https://fenixservices.fao.org/faostat/api/v1/en/{domain}"
            js = requests.get(json_url, params={"area_code":"084","page_size":50000}, timeout=120).json()
            data = js.get("data", [])
            if data:
                pd.DataFrame(data).to_csv(out, index=False)
                prov.append({"source":"FAOSTAT","domain":domain,"url":json_url,"path":str(out)})
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
                            print(f"[FAO][HDX] {domain} -> {out}")
                            got = True; break
                if got: break
            if not got: print(f"[FAO][ERROR] No HDX CSV found for {domain}.")
        except Exception as e:
            print(f"[FAO][WARN] HDX search: {e}")
    return prov

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

def zip_output(zipname="belize-wb-fao-pack.zip"):
    zpath = ROOT / zipname
    with zipfile.ZipFile(zpath, "w", zipfile.ZIP_DEFLATED) as z:
        for p in (ROOT/"data").rglob("*"):
            if p.is_file(): z.write(p, p.relative_to(ROOT))
        z.write(ROOT/"catalog.yaml", "catalog.yaml")
        # also include manifest at root for convenience
        z.write(ROOT/"data/_meta/manifest.json", "_meta/manifest.json")
        z.write(ROOT/"data/_meta/checksums.json", "_meta/checksums.json")
    print(f"[ZIP] {zpath}")
    return str(zpath)

def main():
    catalog_text = (ROOT/"catalog.yaml").read_text(encoding="utf-8")
    cfg = yaml.safe_load(catalog_text)
    prov = []
    print("== World Bank ==")
    prov += wb_fetch(cfg["country"], cfg["world_bank"]["indicators"])
    print("== FAOSTAT ==")
    prov += fao_fetch(cfg["fao"]["downloads"])
    write_checks_and_manifest(prov, catalog_text)
    z = zip_output()
    print("DONE:", z)

if __name__ == "__main__":
    main()
