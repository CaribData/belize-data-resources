# Belize Data Resources (World Bank + FAOSTAT)

**Our versioned data pack:**
1. Go to **Actions → “Build & Release — Belize (WB + FAO, versioned)” → Run workflow**.
2. Tag: e.g. `wb-fao-2025-08-21` (date) or `v1.0.0` (semantic).
3. Name: a friendly title (shown on Releases).

The workflow produces a **Release** containing:
- `belize-wb-fao-pack.zip` (all CSVs + catalog + provenance)
- `_meta/manifest.json` (build time, commit SHA, catalog hash, file checksums/rows)
- `_meta/checksums.json` (path → SHA-256)

**Versioning policy**
- **New data ⇒ new tag.** Don’t overwrite tags.
- The most recent release is automatically marked **Latest**.

**Add sources later**
- Edit `catalog.yaml` (add indicators or FAOSTAT domains), then re-run the workflow.
