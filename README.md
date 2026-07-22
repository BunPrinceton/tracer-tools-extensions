# Tracer Tools - Extensions

Standalone tools for BANC (Brain and Nerve Cord) proofreading workflows: fast ID/proofread lookups, a Neuroglancer mesh pipeline, and a link-restoration pipeline.

This is **not a fork** of the upstream tracer tools — it's a collection of standalone scripts, each solving a specific bottleneck. Everything here is experimental; the README flags the recommended tool per task.

**Most common task — take a list of IDs and get back the updated IDs with their proofread status?** → ⭐ `resolve_and_status.py` (first tool listed below).

---

## Included tools

### ⭐ `resolve_and_status.py` — Update IDs + proofread status (recommended — use this today)

**The current best way to take a list of IDs and get back the updated (current) IDs with their proofread status.** It uses two sanctioned, ground-truth CAVE calls and nothing custom — so the output needs no external corroboration and cannot produce a false positive:

1. `chunkedgraph.suggest_latest_roots(id)` — resolves each input to its **current** ID by max voxel-overlap (CAVE's canonical updater). Returns the ID unchanged if already current, so no separate `is_latest_roots` step is needed. On a split it picks the largest-overlap piece.
2. `materialize.live_live_query("backbone_proofread")` — the same **live** status call banc-bot runs for `<id>??`. Because it's live (not a materialized snapshot) it never lags behind recent proofreading, and it reports proofread only when a real label exists on that cell right now.

```bash
python resolve_and_status.py --input ids.txt
python resolve_and_status.py -i ids.txt -o out.tsv --workers 20
```

**Output** (`*_status.tsv`): `input_id · current_id · id_changed · proofread_status`. No `--tracer-path` needed (pure caveclient). ~330 IDs in ~20 s.

Prefer this over `fast_validate_ids.py` for the "is it current **and** is it proofread" question — it adds proofread status and uses `suggest_latest_roots` (robust on splits) instead of following one supervoxel (which can land on a minor fragment of a split neuron).

> **Split note:** after a real split there is no single canonical "current ID" — it depends which piece you mean, exactly as in Neuroglancer (a 2D double-click returns whichever piece you clicked). `suggest_latest_roots` returns the biggest-overlap piece; treat those rows as a human "look at it" flag, not a tool error.

#### `check_backbone_proofread_hybrid.py` — three-way audit / corroboration

When you need to **prove** the answer (skeptics, validating a new script), this runs the same IDs through three engines side by side and flags every disagreement:

- **ours** — supervoxel tracking + materialized `query_table` (the original method)
- **banc-bot** — its own code, imported from the `banc` package (`is_latest_roots` + `banc.lookup.annotations`, i.e. the literal `<id>??`), plus a `*_bancbot_log.txt` that reproduces banc-bot's Slack replies as an offline audit trail
- **cave** — `suggest_latest_roots` as an independent resolver

It emits reconciled `best_current_id` / `best_proofread` columns (which match `resolve_and_status.py`) alongside the per-engine columns and an agreement verdict, so nothing is silently overridden. Needs the `banc` package: `pip install banc` — then restore `pip install cloud-files==6.3.1` afterward (banc downgrades cloud-files, which breaks the mesh pipeline). banc reads its token under the default `token` key via `banc.auth.configs['cave_auth_token_key']='token'` (handled in the script).

#### `check_backbone_proofread.py` — original (superseded, kept for reference)

The first version: supervoxel tracking + a materialized `backbone_proofread` table map. Still correct on a freshly-materialized dataset with no splits, but it reads the **materialized snapshot** (can lag weeks behind live proofreading) and follows a **single supervoxel** (split-fragile). Superseded by `resolve_and_status.py`; kept for historical reference. See the hybrid tool for a line-by-line comparison of the two methods.

---

### `fast_validate_ids*.py` — ID-currency validator (ID-only)

> **For updating a list of IDs *and* getting proofread status, use ⭐ `resolve_and_status.py` above.** Reach for these only when you want ID-currency validation **alone** for very large lists. They resolve via single-supervoxel tracking, which can pick a minor fragment on splits; `resolve_and_status.py` uses the split-robust `suggest_latest_roots` instead.

Checks whether root IDs are still current and resolves outdated ones via supervoxel tracking. Parallelizes supervoxel lookups across 20 threads, then batches `get_roots()` (5,000/batch). Outputs a detailed report (`*_updated.txt`) and a clean ID list (`*_updated_clean.txt`).

Two near-duplicate copies exist; **prefer the portable one:**

- **`fast_validate_ids_updated_auto_detect_path.py`** — portable version for coworkers/other machines. Auto-detects `tracer_tools/src` relative to the script; if that fails, pass `--tracer-path "C:\path\to\tracer_tools\src"` (open the `tracer_tools` folder, go into `src`, copy that path from the address bar). Auto-detect works when the script sits next to / one level above `tracer_tools/`, or next to a `Tracer - Workspace/` that contains it.
- **`fast_validate_ids.py`** — the original; kept for reference. Same behavior; use the portable copy instead.

```bash
python fast_validate_ids_updated_auto_detect_path.py --input ids.txt
python fast_validate_ids_updated_auto_detect_path.py --input ids.txt --output results.txt --workers 20
```

---

### `fast_get_coords.py` — Coordinate Fetcher

Fetches a representative voxel coordinate for each root ID, output as tab-separated values ready for Google Sheets.

```bash
python fast_get_coords.py --input ids.txt
python fast_get_coords.py --input ids.txt --output coords.tsv --workers 20
```

- Parallelizes L2 chunk lookups across 20 threads, then batches L2 cache coordinate fetches (100 per batch)
- Coordinates are converted from nanometers to voxel space using the datastack's viewer resolution
- Each coordinate is the centroid of one L2 chunk on the neuron (not the soma)

### Mesh pipeline (`state_to_ng_layer_ben.py` + helpers)

Turn a neuroglancer annotation layer into a publicly-viewable precomputed mesh layer in one command. Supports convex hull (default) and 3D alpha-shape ("shrinkwrap") meshing, auto-tunes alpha to produce single-component watertight output, and uploads to a bucket with the correct ACLs and `info` dimensions.

```bash
# Default: convex hull, uploaded to nokura://tracers/ben/<name>/
python state_to_ng_layer_ben.py state.json --layer annotation3 --name region_v1

# Shrinkwrap with auto-tuned alpha
python state_to_ng_layer_ben.py state.json --layer annotation4 --name region_v2 --method alpha

# Override alpha (in nm) and target bucket folder
python state_to_ng_layer_ben.py state.json --layer my_pts --name foo --method alpha --alpha 8000 --bucket-root nokura://tracers/alice
```

Prints back the NG layer source URL (e.g. `https://.../<name>|neuroglancer-precomputed:`) and the state.json subsources snippet to paste into your layer config.

**Files:**

- `state_to_ng_layer_ben.py` — orchestrator CLI; calls the three below.
- `json_to_volume_ben.py` — annotation layer → mesh (`alpha_shape_3d` is the 3D alpha-shape implementation; auto-grow finds smallest alpha that produces a single-component watertight mesh, then fixes face winding).
- `obj_to_volume_ben.py` — OBJ → precomputed volume folder. Pulls volume bounds from the datastack's EM-source `info` so the bbox works for any datastack (BANC, FlyWire, MANC, retina), not just BANC.
- `bucket_upload_folder_ben.py` — folder → bucket. Translates `___` substitutes back to `:` in object keys (Windows-safe filenames on disk, real colon names on the bucket). Sets `ACL=public-read` per file via boto3. Verifies anonymous HTTPS HEAD before returning.

**Why these exist:** Jay's upstream `json_to_volume` / `obj_to_volume` / `bucket_upload_folder` crash on Windows + nokura due to (a) `intervaltree` zero-length intervals from sub-µs local IO, (b) Windows rejecting `:` in filenames, and (c) nokura's S3 emulator rejecting bulk `DeleteObjects` without a `Content-MD5` header. These `_ben` versions bypass all three.

**Dependencies (beyond the existing ones):** `trimesh`, `cloud-volume`, `cloud-files`, `boto3`, `scipy`. Nokura uploads require `~/.cloudvolume/secrets/nokura-secret.json`.

### 2D + 3D viewable layers (`state_to_ng_seg_layer_ben.py` + `serve_local_precomputed_ben.py`)

Extends the mesh pipeline: in addition to the 3D mesh, voxelizes the mesh into a chunked precomputed **segmentation volume** so the layer also shows up in NG's 2D cross-section panel — like a regular proofreading layer, but for any shape you can outline with annotation points. Includes a tiny local CORS HTTP server so you can preview the result in NG before uploading anything.

```bash
# 1. Generate mesh + seg volume LOCALLY (no upload, no nokura quota burn)
python state_to_ng_seg_layer_ben.py state.json \
    --layer annotation1 --name my_region \
    --method alpha --seg-resolution 128,128,90 \
    --workdir C:\path\to\workdir --no-upload

# 2. Serve the resulting precomputed folder locally with CORS
python serve_local_precomputed_ben.py C:\path\to\workdir\image
# -> http://localhost:9000

# 3. In your NG instance, add a new segmentation layer with source:
#    precomputed://http://localhost:9000
# Add segid 1 to its visible segments. The fill appears in BOTH 2D and 3D.
```

Drop `--no-upload` from step 1 to publish to `nokura://tracers/ben/<name>/` like `state_to_ng_layer_ben.py` does.

**Why this is separate from the mesh-only pipeline:**

`state_to_ng_layer_ben.py` produces mesh-only precomputed sources — its `info` claims `type: "segmentation"`, but no voxel chunks are written. NG renders the mesh in 3D and shows nothing in 2D because the 2D view needs per-voxel segment IDs to sample. This script voxelizes the mesh (subdivide method with adaptive `max_iter`, then `scipy.ndimage.binary_fill_holes` with 1-voxel padding to fill the interior) and writes chunked uint64 segmentation data alongside the mesh fragments. Chunks are anchored at the global EM voxel `(0, 0, 0)` so the 2D fill aligns with the same world coordinates as the 3D mesh.

**Cost:** voxelization adds ~2-3 minutes per mesh; each output is 30 MB – 1 GB depending on resolution and region size. For ~75-90% of use cases where 2D fills aren't needed, stay on the mesh-only `state_to_ng_layer_ben.py`.

**Tuning:**

- `--seg-resolution rx,ry,rz` — voxel size in nm (default `64,64,90`). Coarser → smaller files, blockier 2D outline. `128,128,90` is a good middle ground for ~100 µm regions.
- `--chunk-size` — chunk shape in voxels (default `128,128,16`). Raw uint64 stores zero voxels too, so each chunk file is `chunk_size_xyz * 8` bytes regardless of fill ratio; smaller chunks waste fewer bytes per empty corner but multiply HTTP requests.
- All `state_to_ng_layer_ben.py` flags (`--method`, `--alpha`, `--datastack`, etc.) work the same way.

**Notes:**

- Auto-strips non-`type:point` annotations (e.g. stray `axis_aligned_bounding_box`) into a temp sanitized state file before meshing, since the upstream `get_anno_array_from_json` does `anno["point"]` blindly.
- Adaptive subdivide `max_iter` is computed from the mesh's longest edge in scaled (pitch=1) space; the trimesh default of 10 trips on meshes whose edges are >1000 units long in scaled space.
- Mesh fragment files on disk use `___` substitutes for `:` (Windows-safe). `serve_local_precomputed_ben.py` translates these on the fly via `translate_path` so NG can fetch them at the colon paths it expects; `bucket_upload_folder_ben.py` does the equivalent rename on upload.
- Browsers treat `http://localhost` as a secure context, so the local server works with HTTPS NG instances (spelunker, ng-app, etc.) without mixed-content blocks.
- **Re-running on the same workdir is safe.** The script `shutil.rmtree`s the `<scale_key>/` chunk directory before writing, so chunks from a previous run never leak into the new output. (Without this, a tighter mesh would leave behind orphan chunks from the wider previous mesh at the same scale, and NG would render the *union* — a confusing "drifting bbox" effect that's hard to diagnose visually.)
- **Seg-volume bbox matches the datastack EM extent**, not the tight mesh bbox. Without this, NG draws a separate small yellow rectangle around just the mesh in addition to the EM's big rectangle, cluttering the view; matching the EM extent collapses them to one combined bbox. Implementation: the script reads the EM-derived `size * resolution` from the info written by `_write_volume_packaging` and re-expresses it at the new seg resolution. Assumes `em_offset == [0,0,0]` (true for BANC); warns otherwise.

**Dependencies (in addition to the mesh pipeline):** `scipy` (already pulled in by `trimesh`).

### `merge_layers_to_ng_ben.py` — Multi-layer mesh merger

Combines multiple annotation layers from a Neuroglancer state into **one** hosted mesh — versus `state_to_ng_layer_ben.py`, which builds one mesh per `--layer` invocation. Useful when N point-annotation clusters should be visualized as a single 3D region (e.g. 12 sub-regions of a brain structure → one watertight envelope for neuron-passes-through testing).

**Self-contained** — pulls voxel scale from the state's `dimensions` field and the EM source URL from the image layer directly, so it doesn't depend on `tracer_tools.utils` like the other `_ben` scripts.

```bash
# Auto-pick all annotation layers (excluding region_boundaries/region_outlines/bbox
# variants and stray layers with <10 points), build one convex hull, upload to nokura.
python merge_layers_to_ng_ben.py state.json --name all_regions

# Recommended pipeline for a clean watertight result suitable for inclusion testing:
python merge_layers_to_ng_ben.py state.json --name all_regions \
  --combine per-layer --method alpha --union \
  --dilate 1500 --smooth 40 --remesh-pitch 500

# Preview the resolved layer set without building or uploading anything:
python merge_layers_to_ng_ben.py state.json --name foo --dry-run
```

**Combine modes:**

- `--combine merged` (default) — concatenate point clouds from all layers, then build ONE mesh. Bridges naturally across layers but fills intentional voids (e.g. tower-over-base becomes a bell shape).
- `--combine per-layer` — mesh each layer independently, then concatenate. Preserves voids between non-touching clusters (each blob is its own watertight shape).

**Post-processing (combine=per-layer):**

- `--union` — boolean union (manifold3d) over per-layer components. Fuses overlapping blobs into one watertight surface, eliminating visible overlap creases. Falls back to plain concatenation on failure.
- `--dilate <nm>` — inflate each component along vertex normals before union to bridge near-but-non-touching clusters.
- `--smooth <N>` — N Taubin smoothing iterations on the final mesh. Topology-preserving (watertight stays `True`). Each +20 iters slightly shrinks volume.
- `--remesh-pitch <nm>` — voxelize at this pitch and re-extract surface via marching cubes. **Guarantees watertight + manifold output** regardless of upstream artifacts. Use this instead of `--decimate` whenever testing/inclusion-correctness matters.
- `--decimate <fraction>` — quadric edge-collapse simplification (fast-simplification backend). Note: can break watertightness by creating non-manifold geometry — `--remesh-pitch` is the safer knob for "smaller + still watertight."

**Layer auto-discovery:** picks every `type=="annotation"` layer minus `--exclude` names (region_boundaries / region_outlines / bbox variants) and layers below `--min-points` annotations (default 10 — filters orphan/stray layers without hard-coded names). Pass `--layers a,b,c` to override.

**Dependencies (beyond the mesh-pipeline ones):** `manifold3d` (for `--union`), `scikit-image` (for `--remesh-pitch`), `fast-simplification` (for `--decimate`).

A complete worked example with the resulting OBJ and a public Neuroglancer source URL lives in [`examples/`](examples/).

### `bucket_copy_folder_ben.py` — bucket-to-bucket precomputed copy

Copies a precomputed folder from one bucket to another with the nokura-safe client (colon-name translation, public-read ACLs). A local stand-in for Jay's `tracertools.bucket_copy_folder`, mirroring its `(source_path, dest_path)` API — used to copy meshes into the shared central folder `nokura://tracers/swamps/banc/individual_meshes/<NN>`. Requires `cloud-files >= 6.x` (older versions silently make shared-folder copies unreadable).

---

## Link restoration pipeline

Collect old Neuroglancer / appspot / CAVE-state share links, classify which still work, and rebuild the dead-but-restorable ones into working open-in-viewer URLs. Companion to the borkbook Link Restorer web tool (`/link-restore/`) — reuses the same `restore_old_ng_links.py` fetch/route logic so the CLI and web tool agree. The guiding rule throughout: **a link's host does not identify the dataset; only the fetched state contents do.**

- **`link_pipeline.py`** — the end-to-end orchestrator. Point it at Google Sheets and/or a text file of pasted links; get back one self-contained HTML report of clickable **fixed** links grouped by status.
  ```bash
  python link_pipeline.py --input links.txt --output report.html
  python link_pipeline.py --sheet <SHEET_ID> --output report.html
  python link_pipeline.py --sheet <ID1> --sheet <ID2> --input more.txt -o report.html
  python link_pipeline.py --input links.txt --no-fetch        # shape-only, offline
  ```
  If `restore_old_ng_links.py` isn't auto-found, pass `--restore-path <its dir>`.
- **`extract_sheet_links.py`** — stage 1 (collect). Scans every cell of one or more worksheets and pulls out anything that looks like a shareable NG link/source (matches `appspot.com`, `json_url=`, `nglstate`, `graphene://`, `ngl.flywire.ai`, `local_id=`, `spelunker`); captures an adjacent "Notes" column when present. OAuth via your personal Google account (`google_credentials.json`; token cached at `~/.tracer_tools_token.pickle`).
  ```bash
  python extract_sheet_links.py --sheet <SHEET_ID> --output links.json
  python extract_sheet_links.py --sheet <SHEET_ID> --worksheet "Sheet1" --output links.csv --format csv
  ```
- **`validate_links.py`** — stage 2 (classify). Given any link shape, decides whether it still works and why, returning exactly one status: `ok`, `dead-host`, `truncated-id`, `auth-gated`, `dead-em`, `seg-gone`, `local-id-unportable`, `unrecognized`. Optionally fetches stored states with your CAVE token to route by real contents.
  ```bash
  python validate_links.py --input links.txt --output report.json
  python validate_links.py --input links.txt --no-fetch          # shape-only, no network
  ```

**Dependencies:** `gspread` + `google-auth-oauthlib` (sheet extraction only); `caveclient` for state fetching. Auth/secrets are read only from the standard locations and never printed.

---

### Shared options

The **ID/coord tools** (`resolve_and_status.py`, `fast_*`, `check_backbone_proofread*.py`, `fast_get_coords.py`) accept: `--input/-i` (required), `--output/-o` (auto-named from input), `--datastack/-d` (default: `brain_and_nerve_cord`), `--workers/-w` (default: 20). Input format: plain text, one ID per line; arrow notation (`N → ID` or `N -> ID`) is also accepted.

The mesh and link tools take their own arguments (a positional `state.json`, `--sheet`, etc.) — see each section above.

---

## Dependencies

Everything needs **Python 3** and CAVE credentials at `~/.cloudvolume/secrets/cave-secret.json`. Beyond that, deps are per-tool — install only what you use:

| Tool group | Requires |
|---|---|
| ID/proofread + coords (`resolve_and_status.py`, `fast_*`, `check_backbone_proofread*.py`) | `caveclient` (+ `pandas`) |
| Hybrid audit (`check_backbone_proofread_hybrid.py`) | `caveclient`, `pandas`, **`banc`** — `pip install banc`, then restore `pip install cloud-files==6.3.1` (banc downgrades it and breaks the mesh pipeline) |
| Mesh pipeline (`state_to_ng_*`, `*_volume_ben.py`, `merge_layers_to_ng_ben.py`, bucket tools) | `trimesh`, `cloud-volume`, **`cloud-files>=6.x`**, `boto3`, `scipy` (+ `manifold3d`/`scikit-image`/`fast-simplification` for the merger's optional flags). Nokura uploads need `~/.cloudvolume/secrets/nokura-secret.json` |
| Link pipeline (`link_pipeline.py`, `extract_sheet_links.py`, `validate_links.py`) | `gspread` + `google-auth-oauthlib` (sheet extraction), `caveclient` (state fetch) |

The `_ben` mesh scripts still import the **old** `tracer_tools` API and auto-detect it from several path candidates; on machines where auto-detect grabs the wrong copy, pass `--tracer-path`. `resolve_and_status.py` needs no `--tracer-path` (pure caveclient).

---

## Why these are faster

The ID/coord scripts share one pattern: a single shared `CAVEclient` instance, threaded per-ID lookups for the slow step, and batched bulk API calls for the fast step. This avoids sequential per-ID requests and redundant client initialization — ~5,000 IDs typically complete in under a minute (`resolve_and_status.py` does ~330 in ~20 s).

---

## Relationship to upstream tools

These scripts are inspired by Princeton tracer tooling but are independently maintained. They don't aim to stay in sync with upstream — they just need to produce correct output faster.
