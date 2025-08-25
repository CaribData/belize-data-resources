#!/usr/bin/env python3
"""
Generate docs/downloads.md with per-file HTTP links and short descriptions.

- Reads data already published on gh-pages under ghp/data/...
- Adds World Bank indicator names from world_bank/_dictionary.csv
- Safe: stdlib only
"""
import os, re, json, csv, pathlib
from urllib.parse import quote

GHP = pathlib.Path("ghp")           # checked-out gh-pages path (see docs.yml)
BASE = GHP / "data"
OUT = pathlib.Path("docs") / "downloads.md"

OWNER = os.environ.get("GITHUB_REPOSITORY_OWNER", "CaribData")
REPO  = os.environ.get("GITHUB_REPOSITORY", "CaribData/open-data-caribbean").split("/", 1)[1]
BASE_URL = f"https://{OWNER}.github.io/{REPO}"

def url(path: pathlib.Path) -> str:
    # Keep slashes, encode spaces/specials
    rel = "data/" + str(path.relative_to(GHP)).replace("\\", "/")
    return f"{BASE_URL}/{quote(rel, safe='/')}"

def latest_tag() -> str:
    lj = BASE / "latest.json"
    if lj.exists():
        try:
            return (json.loads(lj.read_text(encoding="utf-8")).get("tag") or "").strip()
        except Exception:
            pass
    tags = [p.name for p in BASE.iterdir() if p.is_dir() and re.match(r"^(v|od-)", p.name)]
    return sorted(tags)[-1] if tags else ""

def latest_messy_tag() -> str:
    root = BASE / "messy"
    if not root.exists(): return ""
    tags = [p.name for p in root.iterdir() if p.is_dir()]
    return sorted(tags)[-1] if tags else ""

def read_dictionary(dict_csv: pathlib.Path) -> dict:
    """Return {indicator_code: indicator_name}."""
    m = {}
    if not dict_csv.exists(): return m
    with open(dict_csv, "r", encoding="utf-8-sig", newline="") as f:
        r = csv.reader(f)
        rows = list(r)
    if not rows: return m
    headers = [h.strip().lower() for h in rows[0]]
    def col(opts, default_idx):
        for o in opts:
            if o in headers: return headers.index(o)
        return default_idx
    code_i = col(["indicator_code","code","id"], 0)
    name_i = col(["indicator_name","name","label","title"], 1 if len(headers) > 1 else 0)
    for row in rows[1:]:
        if not row: continue
        if code_i >= len(row): continue
        code = (row[code_i] or "").strip().lstrip("\ufeff")
        if not code: continue
        name = (row[name_i] or "").strip() if name_i < len(row) else ""
        m[code] = name
    return m

def build():
    OUT.parent.mkdir(parents=True, exist_ok=True)
    tag  = latest_tag()
    mtag = latest_messy_tag()

    lines = []
    lines.append("# Downloads\n")

    # -------- Open Data --------
    if tag and (BASE/tag).exists():
        lines.append(f"## Open Data — Latest: `{tag}`\n")
        quick = [
            "_freshness.json",
            "_quality_report.csv",
            "_quality_report.json",
            "world_bank/_dictionary.csv",
            "world_bank/_manifest.json",
            "faostat_fbs/_manifest.json",
        ]
        for p in quick:
            fp = BASE / tag / p
            if fp.exists():
                lines.append(f"- [{p}]({url(fp)})")
        lines.append("")

        # World Bank CSVs + descriptions
        wb_root = BASE / tag / "world_bank"
        dmap = read_dictionary(wb_root / "_dictionary.csv")
        if wb_root.exists():
            lines.append("### World Bank CSVs")
            for country_dir in sorted([p for p in wb_root.iterdir() if p.is_dir()]):
                lines.append(f"- **{country_dir.name}**")
                for f in sorted(country_dir.glob("*.csv")):
                    code = f.stem
                    desc = dmap.get(code, "")
                    if desc:
                        lines.append(f"  - [{f.name}]({url(f)}) — {desc}")
                    else:
                        lines.append(f"  - [{f.name}]({url(f)})")
            lines.append("")

        # FAOSTAT FBS CSVs
        fbs_root = BASE / tag / "faostat_fbs"
        if fbs_root.exists():
            lines.append("### FAOSTAT FBS CSVs")
            for f in sorted(fbs_root.glob("*_fbs.csv")):
                iso3 = f.name[:-8] if f.name.endswith("_fbs.csv") else ""
                lines.append(f"- [{f.name}]({url(f)}) — FAOSTAT Food Balance Sheets ({iso3})")
            lines.append("")
    else:
        lines.append("## Open Data — (no published tag found yet)\n")

    # -------- Messy --------
    lines.append("## Messy Data (Belize)")
    mroot = BASE / "messy" / mtag if mtag else None
    if mroot and mroot.exists():
        lines.append(f"_Latest messy tag:_ `{mtag}`\n")
        for p in ["_manifest.json","_report.json","_dataset_card.md"]:
            fp = mroot / p
            if fp.exists():
                lines.append(f"- [{p}]({url(fp)})")
        lines.append("")
        raw = mroot / "raw"
        if raw.exists():
            lines.append("### Raw files")
            for slug_name in sorted([p.name for p in raw.iterdir() if p.is_dir()]):
                lines.append(f"- **{slug_name}**")
                slug_dir = raw / slug_name            # <-- build the path first
                for f in sorted(slug_dir.rglob("*")): # <-- then glob (no precedence issues)
                    if f.is_file() and f.suffix.lower() in [".xlsx",".xls",".csv"]:
                        lines.append(f"  - [{f.name}]({url(f)})")
            lines.append("")
    else:
        lines.append("_No messy data published yet._\n")

    # -------- All tags --------
    tags = [p.name for p in BASE.iterdir() if p.is_dir() and re.match(r"^(v|od-)", p.name)]
    if tags:
        lines.append("## All Open Data tags")
        for t in sorted(tags):
            lines.append(f"- [{t}](github.com/CaribData/open-data-caribbean/releases/tag/{t}/)")
        lines.append("")

    OUT.write_text("\n".join(lines), encoding="utf-8")
    print(f"Wrote {OUT} ({len(lines)} lines)")
    print("--- preview ---")
    for line in lines[:30]:
        print(line)

if __name__ == "__main__":
    build()
