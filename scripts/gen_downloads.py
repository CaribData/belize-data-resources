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
    """B
