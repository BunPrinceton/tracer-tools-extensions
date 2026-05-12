# Tracer Tools - Extensions

Performance-oriented extensions for BANC (Brain and Nerve Cord) proofreading workflows, focused on **batched and parallelized** CAVE API calls for large ID sets.

This is **not a fork** of the upstream tracer tools. It collects standalone scripts that solve specific performance bottlenecks when working with thousands of IDs.

---

## Included tools

### `fast_validate_ids.py` — ID Validator

Checks whether root IDs are still current and resolves outdated ones to their latest version via supervoxel tracking.

```bash
python fast_validate_ids.py --input ids.txt
python fast_validate_ids.py --input ids.txt --output results.txt --workers 20
```

- Parallelizes supervoxel lookups across 20 threads, then batches `get_roots()` calls (5,000 per batch)
- Outputs a detailed report (`*_updated.txt`) and a clean ID list (`*_updated_clean.txt`)
- Uses supervoxel tracking instead of `get_latest_roots()` to accurately follow splits/merges

### `fast_validate_ids_updated_auto_detect_path.py` — Portable ID Validator (Recommended)

**This is the recommended version for new users / coworkers.** It does everything the original `fast_validate_ids.py` does, but removes hardcoded paths so it works on any machine.

The script auto-detects the `tracer_tools/src` directory relative to where the script is saved. If auto-detect fails, you can point it manually with `--tracer-path`.

**Quick start:**

1. Install dependencies:
   ```
   pip install caveclient
   ```

2. Save the script anywhere on your computer and try running it:
   ```bash
   python fast_validate_ids_updated_auto_detect_path.py --input ids.txt
   ```

3. If you get an error saying it can't find `tracer_tools/src`, use `--tracer-path`:
   ```bash
   python fast_validate_ids_updated_auto_detect_path.py --input ids.txt --tracer-path "C:\path\to\tracer_tools\src"
   ```
   To find the right path: locate the `tracer_tools` folder on your computer, open the `src` subfolder inside it, and copy that full path from your file explorer address bar.

**Auto-detect works when the script is placed:**
- Next to the `tracer_tools/` folder
- One level above the `tracer_tools/` folder
- Inside or next to a `Tracer - Workspace/` folder that contains `tracer_tools/`

**Output:** Same as the original — a detailed report (`*_updated.txt`) and a clean ID list (`*_updated_clean.txt`).

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

### Shared options

All scripts accept: `--input/-i` (required), `--output/-o` (auto-named from input), `--datastack/-d` (default: `brain_and_nerve_cord`), `--workers/-w` (default: 20).

The portable version also accepts: `--tracer-path` (manual path to `tracer_tools/src`, only needed if auto-detect fails).

Input format: plain text, one ID per line. Arrow notation (`N → ID` or `N -> ID`) is also supported.

---

## Dependencies

- **Python 3**
- **caveclient** (`pip install caveclient`) — requires CAVE credentials at `~/.cloudvolume/secrets/cave-secret.json`

---

## Why these are faster

Both scripts share the same pattern: a single shared `CAVEclient` instance, threaded per-ID lookups for the slow step, and batched bulk API calls for the fast step. This avoids the overhead of sequential per-ID requests and redundant client initialization. ~5,000 IDs typically complete in under a minute.

---

## Relationship to upstream tools

These scripts are inspired by Princeton tracer tooling but are independently maintained. They don't aim to stay in sync with upstream — they just need to produce correct output faster.
