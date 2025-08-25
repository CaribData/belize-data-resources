#!/usr/bin/env python3
"""
Generate docs/downloads.md with per-file HTTP links + short descriptions.

- Uses files already on gh-pages under ghp/data/...
- Adds WB indicator names from world_bank/_dictionary.csv
- Shows ONLY the latest Open Data (od-) and Messy (md-) releases
- Release links go to GitHub (https://github.com/<org>/<repo>/releases/tag/<tag>)
"""
import os, re, json, csv, pathlib
from urllib.parse import quote

GHP = pathlib.Path("ghp")
BASE = GHP / "data"
OUT  = pathlib.Path("docs") / "downloads.md"

OWNER = os.environ.get("GITHUB_REPOSITORY_OWNER", "CaribData")
REPO  = os.environ.get("GITHUB_REPOSITORY", "CaribData/open-data-caribbean").split("/", 1)[1]
REPO_FULL = f"{OWNER}/{REPO}"
BASE_URL = f"https://{OWNER}.github.io/{REPO}"

def pages_url(path: pathlib.Path) -> str:
    # path is under ghp/..., typically ghp/data/<...>
    rel = path.relative_to(GHP).as_posix()  # already starts with 'data/...'
    return f"{BASE_URL}/{quote(rel, safe='/')}"

def release_url(tag: str) -> str:
    return f"https://github.com/{REPO_FULL}/releases/tag/{tag}"

def latest_od_tag() -> str:
    """Prefer data/latest.json; fallback to newest dir starting with 'od-' or 'v'."""
    lj = BASE / "latest.json"
    if lj.exists():
        try:
            tag = (json.loads(lj.read_text(encoding="utf-8")).get("tag") or "").strip()
            if tag: return tag
        except Exception:
            pass
    tags = [p.name for p in BASE.iterdir() if p.is_dir() and re.match(r"^(od-|v)", p.name)]
    return sorted(tags)[-1] if tags else ""

def latest_md_tag() -> str:
    root = BASE / "messy"
    if not root.exists(): return ""
    tags = [p.name for p in root.iterdir() if p.is_dir() and p.name.startswith("md-")]
    return sorted(tags)[-1] if tags else ""

def read_dictionary(dict_csv: pathlib.Path) -> dict:
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

def main():
    OUT.parent.mkdir(parents=True, exist_ok=True)
    od_tag = latest_od_tag()
    md_tag = latest_md_tag()

    lines = []
    lines.append("# Downloads\n")

    # ===== Latest releases summary =====
    lines.append("## Latest Releases\n")
    if od_tag and (BASE / od_tag).exists():
        lines.append(f"- **Open Data** — `{od_tag}` · "
                     f"[Files]({BASE_URL}/data/{od_tag}/) · "
                     f"[Release]({release_url(od_tag)})")
    else:
        lines.append("- **Open Data** — *(not published yet)*")
    if md_tag and (BASE / "messy" / md_tag).exists():
        lines.append(f"- **Messy Data (Belize)** — `{md_tag}` · "
                     f"[Files]({BASE_URL}/data/messy/{md_tag}/) · "
                     f"[Release]({release_url(md_tag)})")
    else:
        lines.append("- **Messy Data (Belize)** — *(not published yet)*")
    lines.append("")

    # ===== Open Data (latest only) =====
    if od_tag and (BASE/od_tag).exists():
        lines.append(f"## Open Data — Latest: `{od_tag}`\n")

        # Quick assets
        quick = [
            "_freshness.json",
            "_quality_report.csv",
            "_quality_report.json",
            "world_bank/_dictionary.csv",
            "world_bank/_manifest.json",
            "faostat_fbs/_manifest.json",
        ]
        for p in quick:
            fp = BASE / od_tag / p
            if fp.exists():
                lines.append(f"- [{p}]({pages_url(fp)})")
        lines.append("")

        # World Bank CSVs with descriptions
        wb_root = BASE / od_tag / "world_bank"
        dmap = read_dictionary(wb_root / "_dictionary.csv")
        if wb_root.exists():
            lines.append("### World Bank CSVs")
            for country_dir in sorted([p for p in wb_root.iterdir() if p.is_dir()]):
                lines.append(f"- **{country_dir.name}**")
                for f in sorted(country_dir.glob("*.csv")):
                    code = f.stem
                    desc = dmap.get(code, "")
                    if desc:
                        lines.append(f"  - [{f.name}]({pages_url(f)}) — {desc}")
                    else:
                        lines.append(f"  - [{f.name}]({pages_url(f)})")
            lines.append("")

        # FAOSTAT FBS CSVs
        fbs_root = BASE / od_tag / "faostat_fbs"
        if fbs_root.exists():
            lines.append("### FAOSTAT FBS CSVs")
            for f in sorted(fbs_root.glob("*_fbs.csv")):
                iso3 = f.name[:-8] if f.name.endswith("_fbs.csv") else ""
                lines.append(f"- [{f.name}]({pages_url(f)}) — FAOSTAT Food Balance Sheets ({iso3})")
            lines.append("")
    else:
        lines.append("## Open Data — (no published tag found yet)\n")

    # ===== Messy Data (latest only) =====
    lines.append("## Messy Data (Belize)")
    mroot = BASE / "messy" / md_tag if md_tag else None
    if mroot and mroot.exists():
        lines.append(f"_Latest messy tag:_ `{md_tag}` · "
                     f"[Files]({BASE_URL}/data/messy/{md_tag}/) · "
                     f"[Release]({release_url(md_tag)})\n")
        for p in ["_manifest.json","_report.json","_dataset_card.md"]:
            fp = mroot / p
            if fp.exists():
                lines.append(f"- [{p}]({pages_url(fp)})")
        lines.append("")
        raw = mroot / "raw"
        if raw.exists():
            lines.append("### Raw files")
            for slug_dir in sorted([p for p in raw.iterdir() if p.is_dir()]):
                lines.append(f"- **{slug_dir.name}**")
                for f in sorted(slug_dir.rglob("*")):
                    if f.is_file() and f.suffix.lower() in [".xlsx",".xls",".csv"]:
                        lines.append(f"  - [{f.name}]({pages_url(f)})")
            lines.append("")
    else:
        lines.append("_No messy data published yet._\n")

    OUT.write_text("\n".join(lines), encoding="utf-8")
    print(f"Wrote {OUT} ({len(lines)} lines)")
    print("--- preview ---")
    for line in lines[:25]:
        print(line)

if __name__ == "__main__":
    main()
