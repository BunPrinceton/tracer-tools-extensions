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

## 2D + 3D pipeline (mesh + seg-volume + local preview)

Two extra files for the case where the user wants the NG layer to render in **both 2D cross-sections and 3D**, not just 3D:

- `state_to_ng_seg_layer_ben.py` — parallels `state_to_ng_layer_ben.py` but additionally voxelizes the generated mesh into a chunked precomputed segmentation volume so NG's 2D panel has per-voxel segment IDs to sample. Uses `trimesh.voxelized(method="subdivide", max_iter=adaptive)` + `scipy.ndimage.binary_fill_holes` with 1-voxel padding (the trimesh default `vox.fill()` is a no-op on shell voxelizations in current trimesh). Sanitizes the state file first by stripping non-`type:point` annotations, since the upstream `get_anno_array_from_json` does `anno["point"]` blindly and crashes on `axis_aligned_bounding_box` mixed in. CLI flags: same as `state_to_ng_layer_ben.py` plus `--seg-resolution rx,ry,rz` (nm/voxel, default `64,64,90`), `--chunk-size` (voxels, default `128,128,16`), and `--no-upload` for local-only generation. Two behaviors worth knowing about: (1) `shutil.rmtree`s `<scale_key>/` before writing chunks so re-runs don't leak orphan chunks from a wider previous mesh (debugging that visually is painful — the seg appears to "drift"); (2) preserves the EM-derived size/resolution that `_write_volume_packaging` wrote, so the seg layer's bbox in NG collapses into the EM bbox rather than drawing a second small yellow rectangle. Assumes `em_offset == [0,0,0]` (BANC); warns otherwise since chunks are anchored at global voxel 0.
- `serve_local_precomputed_ben.py` — Python stdlib `http.server` subclass that serves a precomputed folder with `Access-Control-Allow-Origin: *` headers and translates `:` → `___` in request paths (since the Windows-safe local mesh filenames use `___` instead of `:`). Lets you preview a `--no-upload` build in NG by adding a layer with source `precomputed://http://localhost:9000`. Browsers treat `http://localhost` as a secure context so this works against hosted HTTPS NG instances.

Local-test workflow: `state_to_ng_seg_layer_ben.py ... --no-upload` → `serve_local_precomputed_ben.py <workdir>/image` → paste `precomputed://http://localhost:9000` into a new NG segmentation layer → add segid 1 to visible segments. The mesh-only pipeline remains the default for the ~75-90% of cases where 2D fills aren't needed (voxelization adds ~2-3 minutes and 30 MB - 1 GB of output per mesh).

The three Windows / nokura bugs the `_ben` variants exist to fix:

1. `cloudfiles.monitoring.TransmissionMonitor.end_io` crashes on `intervaltree`'s "no zero-length intervals" guard when local IO is sub-microsecond (every `_ben` script applies a monkey-patch).
2. Windows rejects `:` in filenames, so anything writing the precomputed mesh format locally (manifest `1:0`, fragment `1:0:1`) breaks. The `_ben` scripts write `___` substitutes locally and translate at upload time.
3. Nokura objects default to private. Without `put_object_acl(..., ACL="public-read")` after each upload, browser fetches return 403 even though `cloudfiles ls` shows the files.

## Upstream repo (renamed June 2026)

Jay rebuilt his package as a NEW repo: **`jaybgager/tracertools`** (one word; package `tracertools`). The old `jaybgager/tracer_tools` (underscore) is abandoned. Ben's tracking fork is **`BunPrinceton/tracertools`**; canonical local clone is `C:\Users\Benjamin\Desktop\_scratch\tracertools_sync` (origin=fork, upstream=Jay).

The new repo overlaps a lot of this repo's `_ben` mesh work and is good jump-board material for future scripts: `make_mesh_from_points` (internal `_alpha_shape_3d`), `make_volume_mesh_from_state_file`, `make_bucket_volume_from_obj`, `host_ng_volume_locally`, `bucket_copy_folder`, `get_anno_array_from_state_file` (renamed from `get_anno_array_from_json`), `get_config`.

NOTE: the existing `_ben` scripts still import the OLD `tracer_tools` API (e.g. `from tracer_tools.utils import get_config, get_anno_array_from_json`). Migrating them to import `tracertools` (with its renamed functions) is a future iteration, not done yet. On this machine auto-detect grabs the wrong copy, so pass `--tracer-path "C:/Users/Benjamin/Desktop/_scratch/tracer_tools_sync/src"`.

## Bucket utilities

- `bucket_upload_folder_ben.py` — local folder → bucket (colon-name translation, public-read ACL, anon-HEAD verify).
- `bucket_copy_folder_ben.py` — bucket→bucket copy; local stand-in for Jay's `tracertools.bucket_copy_folder` with our nokura-safe client. Mirrors his `(source_path, dest_path)` API. Used to copy meshes into the shared central folder `nokura://tracers/swamps/banc/individual_meshes/<NN>`.

## Dependencies

- `caveclient` (pip) — requires CAVE credentials at `~/.cloudvolume/secrets/cave-secret.json`
- `tracer_tools` — auto-discovered from several path candidates (see `_find_tracer_tools` / `_find_ben_dir` at the top of each script). Override with `--tracer-path` (required on Ben's box; see Upstream repo note above).
- Mesh pipeline only: `trimesh`, `cloud-volume`, `cloud-files`, `boto3`, `scipy`. Nokura uploads require `~/.cloudvolume/secrets/nokura-secret.json`.
- **`cloud-files` must be >= 6.x.** Will's June-2026 fix (in 6.x) corrects a bucket-copy permissions bug where `transfer_to` forced copied objects private; older cloud-files (<=5.8.2) silently makes shared-folder copies unreadable. `cloud-volume` 12.8.0 formally pins `cloud-files<6.0.0` but works fine at 6.3.1 (conservative pin); suspect this skew first if the mesh builder errors oddly.
- Python 3 standard library otherwise.

## Input Format

Plain text, one ID per line. Both scripts support `ID` or `N → ID` (arrow notation, extracts right side). `fast_get_coords.py` also supports `->` ASCII arrows and `#` comment lines.
