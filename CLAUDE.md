# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Python utilities for working with BANC (Brain and Nerve Cord) neuron segment root IDs via the CAVE (Connectome Annotation Versioning Engine) API. Two standalone scripts share a common pattern: parallel per-ID lookups via ThreadPoolExecutor, then batched bulk API calls.

No build step, test suite, or linter is configured.

## Running the Scripts

```bash
# Validate/update stale root IDs
python fast_validate_ids.py --input ids.txt
python fast_validate_ids.py --input ids.txt --output results.txt --workers 20

# Get coordinates for root IDs (tab-separated output for Google Sheets)
python fast_get_coords.py --input ids.txt
python fast_get_coords.py --input ids.txt --output coords.tsv --workers 20
```

Both scripts share CLI arguments: `--input/-i` (required), `--output/-o` (auto-named from input), `--datastack/-d` (default: `brain_and_nerve_cord`), `--workers/-w` (default: 20).

## Architecture

Both scripts follow the same three-stage pipeline pattern with a single shared CAVEclient instance reused across all threads (critical for performance):

### fast_validate_ids.py
1. **Parallel Supervoxel Fetch** — `get_leaves()` per ID to get one supervoxel each.
2. **Batched Root Lookup** — `get_roots()` converts all supervoxels to current root IDs (batches of 5,000).
3. **Result Comparison** — Writes a detailed report (`*_updated.txt`) and a clean ID list (`*_updated_clean.txt`).

Supervoxel tracking is used intentionally instead of `get_latest_roots()` because supervoxels accurately follow physical voxel movement through splits/merges.

### fast_get_coords.py
1. **Parallel L2 Fetch** — `get_leaves(stop_layer=2)` per ID to get one L2 chunk ID each.
2. **Batched Coordinate Fetch** — `l2cache.get_l2data()` retrieves `rep_coord_nm` (batches of 100 to avoid 504 timeouts).
3. **Output** — Converts nm coordinates to voxel space using viewer resolution, writes TSV.

## Dependencies

- `caveclient` (pip) — requires CAVE credentials at `~/.cloudvolume/secrets/cave-secret.json`
- `tracer_tools` — auto-discovered from several hardcoded paths (see `sys.path` setup at top of each script)
- Python 3 standard library only beyond these

## Input Format

Plain text, one ID per line. Both scripts support `ID` or `N → ID` (arrow notation, extracts right side). `fast_get_coords.py` also supports `->` ASCII arrows and `#` comment lines.
