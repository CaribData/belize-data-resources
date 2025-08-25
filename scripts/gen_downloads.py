#!/usr/bin/env python3
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
    rel = path.relative_to(GHP).as_posix()
    return f"{BASE_URL}/{quote(rel, safe='/')}"

def release_url(tag: str) -> str:
    return f"https://github.com/{REPO_FULL}/releases/tag/{tag}"

_TAG_PAT = re.compile(r"^(?:od-|md-|v)v?(\d{4})[.\-](\d{2})[.\-](\d{2})(?:[.\-](?:rc|v)?(\d+))?$", re.I)

def _parse_key(tag: str):
    m = _TAG_PAT.match(tag)
    return (int(m.group(1)), int(m.group(2)), int(m.group(3)), int(m.group(4) or 0)) if m else None

def _latest_by_rule(dirs):
    if not dirs: return ""
    scored = []
    for p in dirs:
        key = _parse_key(p.name)
        try: mtime = p.stat().st_mtime
        except Exception: mtime = 0.0
        scored.append((p.name, key, mtime))
    scored.sort(key=lambda t: (t[1] is None, t[1] or (0,0,0,0), t[2]))
    return scored[-1][0]

def latest_od_tag():
    lj = BASE / "latest.json"
    if lj.exists():
        try:
            tag = (json.loads(lj.read_text(encoding="utf-8")).get("tag") or "").strip()
            if tag: return tag
        except Exception: pass
    cand = [p for p in BASE.iterdir() if p.is_dir() and (p.name.startswith("od-") or p.name.startswith("v"))]
    return _latest_by_rule(cand)

def latest_md_tag():
    # Prefer messy/latest.json if present
    lj = BASE / "messy" / "latest.json"
    if lj.exists():
        try:
            tag = (json.loads(lj.read_text(encoding="utf-8")).get("tag") or "").strip()
            if tag: return tag
        except Exception: pass
    root = BASE / "messy"
    if not root.exists(): return ""
    cand = [p for p in root.iterdir() if p.is_dir() and p.name.startswith("md-")]
    return _latest_by_rule(cand)

def read_dictionary(dict_csv: pathlib.Path) -> dict:
    m = {}
    if not dict_csv.exists(): return m
    import csv
    with open(dict_csv, "r", encoding="utf-8-sig", newline="") as f:
        rows = list(csv.reader(f))
    if not rows: return m
    headers = [h.strip().lower() for h in rows[0]]
    def col(opts, default_idx):
        for o in opts:
            if o in headers: return headers.index(o)
        return default_idx
    code_i = col(["indicator_code","code","id"], 0)
    name_i = col(["indicator_name","name","label","title"], 1 if len(headers) > 1 else 0)
    for row in rows[1:]:
        if not row or code_i >= len(row): continue
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

    # Latest Releases (GitHub Release links only)
    lines.append("## Latest Releases\n")
    lines.append(f"- **Open Data** — `{od_tag}` · [Release]({release_url(od_tag)})" if od_tag and (BASE/od_tag).exists()
                 else "- **Open Data** — *(not published yet)*")
    lines.append(f"- **Messy Data (Belize)** — `{md_tag}` · [Release]({release_url(md_tag)})" if md_tag and (BASE/'messy'/md_tag).exists()
                 else "- **Messy Data (Belize)** — *(not published yet)*")
    lines.append("")

    # Open Data
    if od_tag and (BASE/od_tag).exists():
        lines.append(f"## Open Data — Latest: `{od_tag}`\n")
        wb_root = BASE / od_tag / "world_bank"
        dmap = read_dictionary(wb_root / "_dictionary.csv")
        if wb_root.exists():
            lines.append("### World Bank (CSV)")
            for country_dir in sorted([p for p in wb_root.iterdir() if p.is_dir()]):
                lines.append(f"- **{country_dir.name}**")
                for f in sorted(country_dir.glob("*.csv")):
                    code = f.stem
                    desc = dmap.get(code, "")
                    lines.append(f"  - [{f.name}]({pages_url(f)})" + (f" — {desc}" if desc else ""))
            lines.append("")
        fbs_root = BASE / od_tag / "faostat_fbs"
        if fbs_root.exists():
            lines.append("### FAOSTAT FBS (CSV)")
            for f in sorted(fbs_root.glob("*_fbs.csv")):
                iso3 = f.name[:-8] if f.name.endswith("_fbs.csv") else ""
                lines.append(f"- [{f.name}]({pages_url(f)}) — FAOSTAT Food Balance Sheets ({iso3})")
            lines.append("")
    else:
        lines.append("## Open Data — (not published yet)\n")

    # Messy Data
    lines.append("## Messy Data (Belize)")
    mroot = BASE / "messy" / md_tag if md_tag else None
    if mroot and mroot.exists():
        lines.append(f"_Latest messy tag:_ `{md_tag}` · [Release]({release_url(md_tag)})\n")
        raw = mroot / "raw"
        if raw.exists():
            lines.append("### Raw files (XLS/CSV)")
            for slug_dir in sorted([p for p in raw.iterdir() if p.is_dir()]):
                lines.append(f"- **{slug_dir.name}**")
                for f in sorted(slug_dir.rglob("*")):
                    if f.is_file() and f.suffix.lower() in [".xlsx",".xls",".csv"]:
                        lines.append(f"  - [{f.name}]({pages_url(f)})")
            lines.append("")
    else:
        lines.append("_Not published yet._\n")

    OUT.write_text("\n".join(lines), encoding="utf-8")
    print(f"Chosen tags => od: {od_tag!r}, md: {md_tag!r}")
    print(f"Wrote {OUT}")

if __name__ == "__main__":
    main()
