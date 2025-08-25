#!/usr/bin/env python3
"""
Generate docs/downloads.md with per-file HTTP links and short descriptions.

- Uses files already on gh-pages under ghp/data/...
- Adds World Bank indicator names from world_bank/_dictionary.csv.
- Shows ONLY latest Open Data (od-/v) and latest Messy Data (md-) releases.
- Release links go to GitHub. We list only data files (CSV/XLS/XLSX).
"""
import os, re, json, csv, pathlib, time
from urllib.parse import quote

GHP = pathlib.Path("ghp")
BASE = GHP / "data"
OUT  = pathlib.Path("docs") / "downloads.md"

OWNER = os.environ.get("GITHUB_REPOSITORY_OWNER", "CaribData")
REPO  = os.environ.get("GITHUB_REPOSITORY", "CaribData/open-data-caribbean").split("/", 1)[1]
REPO_FULL = f"{OWNER}/{REPO}"
BASE_URL = f"https://{OWNER}.github.io/{REPO}"

def pages_url(path: pathlib.Path) -> str:
    """Build Pages URL for a file under ghp/data/... (path already includes 'data/')."""
    rel = path.relative_to(GHP).as_posix()
    return f"{BASE_URL}/{quote(rel, safe='/')}"

def release_url(tag: str) -> str:
    return f"https://github.com/{REPO_FULL}/releases/tag/{tag}"

# ---- Robust "latest tag" pickers ----

_TAG_PAT = re.compile(r"""
    ^(?:od-|md-|v)         # prefix
    v?(\d{4})[.\-](\d{2})[.\-](\d{2})   # YYYY.MM.DD or YYYY-MM-DD
    (?:[.\-](?:rc|v)?(\d+))?            # optional rcN or vN
    $""", re.I | re.X)

def _parse_tag_key(tag: str):
    m = _TAG_PAT.match(tag)
    if not m:
        return None
    y, mo, d, n = m.groups()
    return (int(y), int(mo), int(d), int(n or 0))

def _latest_by_rule(dirs: list[pathlib.Path]) -> str:
    """
    Return name of the 'latest' tag folder:
    1) Prefer parsed (YYYY,MM,DD,rcN) ordering
    2) If unparsable/equal, fall back to folder mtime
    """
    if not dirs:
        return ""
    scored = []
    for p in dirs:
        key = _parse_tag_key(p.name)
        try:
            mtime = p.stat().st_mtime
        except Exception:
            mtime = 0.0
        scored.append((p.name, key, mtime))
    # sort ascending, take last
    scored.sort(key=lambda t: (t[1] is None, t[1] or (0,0,0,0), t[2]))
    return scored[-1][0]

def latest_od_tag() -> str:
    """Prefer data/latest.json; fallback to newest dir starting with 'od-' or 'v'."""
    lj = BASE / "latest.json"
    if lj.exists():
        try:
            tag = (json.loads(lj.read_text(encoding="utf-8")).get("tag") or "").strip()
            if tag:
                return tag
        except Exception:
            pass
    cand = [p for p in BASE.iterdir() if p.is_dir() and (p.name.startswith("od-") or p.name.startswith("v"))]
    return _latest_by_rule(cand)

def latest_md_tag() -> str:
    root = BASE / "messy"
    if not root.exists():
        return ""
    cand = [p for p in root.iterdir() if p.is_dir() and p.name.startswith("md-")]
    return _latest_by_rule(cand)

# ---- WB indicator dictionary ----

def read_dictionary(dict_csv: pathlib.Path) -> dict:
    """Return {indicator_code: indicator_name} (robust to BOM/headers)."""
    m = {}
    if not dict_csv.exists():
        return m
    with open(dict_csv, "r", encoding="utf-8-sig", newline="") as f:
        r = csv.reader(f)
        rows = list(r)
    if not rows:
        return m
    headers = [h.strip().lower() for h in rows[0]]
    def col(opts, default_idx):
        for o in opts:
            if o in headers:
                return headers.index(o)
        return default_idx
    code_i = col(["indicator_code","code","id"], 0)
    name_i = col(["indicator_name","name","label","title"], 1 if len(headers) > 1 else 0)
    for row in rows[1:]:
        if not row:
            continue
        if code_i >= len(row):
            continue
        code = (row[code_i] or "").strip().lstrip("\ufeff")
        if not code:
            continue
        name = (row[name_i] or "").strip() if name_i < len(row) else ""
        m[code] = name
    return m

def main():
    OUT.parent.mkdir(parents=True, exist_ok=True)
    od_tag = latest_od_tag()
    md_tag = latest_md_tag()

    lines = []
    lines.append("# Downloads\n")

    # ===== Latest Releases (GitHub Release links only) =====
    lines.append("## Latest Releases\n")
    if od_tag and (BASE / od_tag).exists():
        lines.append(f"- **Open Data** — `{od_tag}` · [Release]({release_url(od_tag)})")
    else:
        lines.append("- **Open Data** — *(not published yet)*")
    if md_tag and (BASE / 'messy' / md_tag).exists():
        lines.append(f"- **Messy Data (Belize)** — `{md_tag}` · [Release]({release_url(md_tag)})")
    else:
        lines.append("- **Messy Data (Belize)** — *(not published yet)*")
    lines.append("")

    # ===== Open Data (latest only) =====
    if od_tag and (BASE/od_tag).exists():
        lines.append(f"## Open Data — Latest: `{od_tag}`\n")

        # World Bank (CSV) — data files only
        wb_root = BASE / od_tag / "world_bank"
        dmap = read_dictionary(wb_root / "_dictionary.csv")
        if wb_root.exists():
            lines.append("### World Bank (CSV)")
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

        # FAOSTAT FBS (CSV) — data files only
        fbs_root = BASE / od_tag / "faostat_fbs"
        if fbs_root.exists():
            lines.append("### FAOSTAT FBS (CSV)")
            for f in sorted(fbs_root.glob("*_fbs.csv")):
                iso3 = f.name[:-8] if f.name.endswith("_fbs.csv") else ""
                lines.append(f"- [{f.name}]({pages_url(f)}) — FAOSTAT Food Balance Sheets ({iso3})")
            lines.append("")
    else:
        lines.append("## Open Data — (not published yet)\n")

    # ===== Messy Data (latest only) =====
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
                    if f.is_file() and f.suffix.lower() in [".xlsx", ".xls", ".csv"]:
                        lines.append(f"  - [{f.name}]({pages_url(f)})")
            lines.append("")
    else:
        lines.append("_Not published yet._\n")

    OUT.write_text("\n".join(lines), encoding="utf-8")
    print(f"Wrote {OUT} ({len(lines)} lines)")
    print("Chosen tags:", {"od": od_tag, "md": md_tag})

if __name__ == "__main__":
    main()
