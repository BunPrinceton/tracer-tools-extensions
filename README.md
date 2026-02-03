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

### `fast_get_coords.py` — Coordinate Fetcher

Fetches a representative voxel coordinate for each root ID, output as tab-separated values ready for Google Sheets.

```bash
python fast_get_coords.py --input ids.txt
python fast_get_coords.py --input ids.txt --output coords.tsv --workers 20
```

- Parallelizes L2 chunk lookups across 20 threads, then batches L2 cache coordinate fetches (100 per batch)
- Coordinates are converted from nanometers to voxel space using the datastack's viewer resolution
- Each coordinate is the centroid of one L2 chunk on the neuron (not the soma)

### Shared options

Both scripts accept: `--input/-i` (required), `--output/-o` (auto-named from input), `--datastack/-d` (default: `brain_and_nerve_cord`), `--workers/-w` (default: 20).

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
