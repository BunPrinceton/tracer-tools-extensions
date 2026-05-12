# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Standalone Python utilities for BANC / CAVE workflows that aren't in upstream tracer_tools or need Windows-friendly variants. Two categories so far:

1. **Performance scripts** for CAVE API calls (`fast_validate_ids.py`, `fast_get_coords.py`): parallel per-ID lookups via ThreadPoolExecutor, then batched bulk calls.
2. **Mesh pipeline** (`state_to_ng_layer_ben.py` + three helpers): annotation layer → public neuroglancer mesh layer in one command, with convex hull and alpha-shape ("shrinkwrap") methods.

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

## Mesh pipeline

`state_to_ng_layer_ben.py` is the user-facing CLI; it composes three helpers:

- `json_to_volume_ben.py` — annotation points → mesh. `method="convex"` uses `trimesh.PointCloud.convex_hull`; `method="alpha"` runs `alpha_shape_3d` (3D alpha shape via `scipy.spatial.Delaunay`). Default auto-grow climbs alpha by ×1.5 per iter until the mesh is single-component AND watertight, then calls `fix_normals()` for consistent winding. Top-level `info` bounds come from the datastack's EM-source info, so the bbox is correct for any datastack with a CAVE config in `tracer_tools` (BANC, FlyWire, MANC, retina).
- `obj_to_volume_ben.py` — OBJ → precomputed volume folder. Shares helpers with `json_to_volume_ben.py`. Use this when continuing from a Blender-edited mesh.
- `bucket_upload_folder_ben.py` — local folder → bucket. Translates `___` filenames back to `:` in destination keys via direct PUT (the upstream `bucket_upload_folder` does this via `cf.move` → `cf.delete`, which fails on nokura because its S3 emulator rejects bulk `DeleteObjects` without a `Content-MD5` header). Sets `ACL=public-read` per object via boto3 after every PUT. Verifies anonymous HTTPS HEAD before returning.

The three Windows / nokura bugs the `_ben` variants exist to fix:

1. `cloudfiles.monitoring.TransmissionMonitor.end_io` crashes on `intervaltree`'s "no zero-length intervals" guard when local IO is sub-microsecond (every `_ben` script applies a monkey-patch).
2. Windows rejects `:` in filenames, so anything writing the precomputed mesh format locally (manifest `1:0`, fragment `1:0:1`) breaks. The `_ben` scripts write `___` substitutes locally and translate at upload time.
3. Nokura objects default to private. Without `put_object_acl(..., ACL="public-read")` after each upload, browser fetches return 403 even though `cloudfiles ls` shows the files.

## Dependencies

- `caveclient` (pip) — requires CAVE credentials at `~/.cloudvolume/secrets/cave-secret.json`
- `tracer_tools` — auto-discovered from several path candidates (see `_find_tracer_tools` / `_find_ben_dir` at the top of each script). Override with `--tracer-path`.
- Mesh pipeline only: `trimesh`, `cloud-volume`, `cloud-files`, `boto3`, `scipy`. Nokura uploads require `~/.cloudvolume/secrets/nokura-secret.json`.
- Python 3 standard library otherwise.

## Input Format

Plain text, one ID per line. Both scripts support `ID` or `N → ID` (arrow notation, extracts right side). `fast_get_coords.py` also supports `->` ASCII arrows and `#` comment lines.
