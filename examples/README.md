# Example: `all_blobs_merged`

Sample output of `merge_layers_to_ng_ben.py` — a single watertight mesh built
from 12 annotation layers of a BANC Neuroglancer state, hosted publicly on
nokura so you can view it from any machine without re-running the pipeline.

## View it in Neuroglancer

1. Open a fresh Neuroglancer session (any BANC state link works as the base).
2. Add a new layer with **Source**:

   ```
   precomputed://https://c10s.pni.princeton.edu/tracers/ben/all_blobs_merged
   ```

3. Set the layer **type** to `segmentation`.
4. Add segment id **`1`** (this mesh is stored under a single segment).
5. *(Optional, advanced)* If you're editing the layer JSON directly, set
   `subsources: {"bounds": true, "mesh": true}` and
   `enableDefaultSubsources: false`.

The mesh should appear immediately — there's no large volume to chunk-load.

## Preview locally

The OBJ in this folder (`all_blobs_merged.obj`) is the same geometry that's
hosted. Open it in any 3D viewer (MeshLab, Blender, `trimesh.load(...).show()`,
macOS Preview, etc.) to inspect without going through Neuroglancer.

```
V = 11,522
F = 23,040
watertight = True
body_count = 1
volume = 4,939 µm³
bounds (nm) = (~573k, ~779k, ~178k) -> (~599k, ~807k, ~219k)
```

## Reproducing it

The command that produced this exact mesh (run against the source state.json
on the author's machine):

```
python merge_layers_to_ng_ben.py state.json \
  --name all_blobs_merged \
  --combine per-layer \
  --method alpha \
  --union \
  --dilate 1500 \
  --smooth 40 \
  --remesh-pitch 500
```

Pipeline summary, step by step:
- **per-layer alpha** — build a separate alpha-shape mesh for each of the 12
  annotation layers (auto-tuned alpha ≈ 2,770 nm).
- **dilate 1,500 nm** — inflate each component along its vertex normals so
  near-but-non-touching clusters bridge.
- **boolean union** (manifold3d) — fuse the dilated components into one
  manifold surface.
- **smooth 40** — Taubin smoothing iterations to silken cheeto-junction seams
  (topology-preserving, so watertight stays true).
- **remesh-pitch 500 nm** — voxelize + marching cubes for a guaranteed
  watertight, single-component, 2-manifold output.

See the top-level [`merge_layers_to_ng_ben.py`](../merge_layers_to_ng_ben.py)
docstring and `--help` for the full flag list.
