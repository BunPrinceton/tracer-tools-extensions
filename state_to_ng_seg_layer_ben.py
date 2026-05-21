"""state_to_ng_seg_layer_ben — state.json annotation layer → public NG layer
that appears in BOTH the 2D cross-section view AND the 3D projection.

Background
----------
`state_to_ng_layer_ben` produces a mesh-only precomputed source: the `info`
claims `type: "segmentation"`, but no voxel chunks are written, so NG shows
the mesh in 3D and nothing in 2D. This script extends that pipeline by also
voxelizing the mesh and writing chunked precomputed segmentation data, so
the layer fills cross-sections like a normal proofreading layer.

Why a separate script
---------------------
The seg-volume voxelization adds disk and upload cost proportional to the
mesh's bbox at the chosen pitch. ~75-90% of mesh layers don't need 2D, so
the original mesh-only flow remains the default. Use this when you want
the mesh to show on 2D EM slices.

Example
-------
  python state_to_ng_seg_layer_ben.py "state (7).json" \\
      --layer annotation1 --name annotation1_2d \\
      --method alpha --seg-resolution 64,64,90

After uploading, add a new segmentation layer in NG with the printed source
URL, then add the printed segid to its visible segments. The layer should
appear in both 2D and 3D.
"""
import os
import sys
import json
import shutil
import argparse
import tempfile
from pathlib import Path

import numpy as np
import trimesh
from scipy.ndimage import binary_fill_holes


def _find_ben_dir():
    here = Path(__file__).resolve().parent
    if (here / "json_to_volume_ben.py").exists():
        return here
    for sib in (here / "tracer_tools" / "src", here.parent / "tracer_tools" / "src"):
        if (sib / "json_to_volume_ben.py").exists():
            return sib
    return None


_ben_dir = _find_ben_dir()
if _ben_dir is None:
    print("ERROR: could not locate the *_ben.py modules. Place this script next to "
          "json_to_volume_ben.py / obj_to_volume_ben.py / bucket_upload_folder_ben.py.")
    sys.exit(1)
sys.path.insert(0, str(_ben_dir))

from json_to_volume_ben import json_to_volume_ben  # noqa: E402
from bucket_upload_folder_ben import bucket_upload_folder_ben, ng_layer_source_for  # noqa: E402


def _sanitize_state_for_layer(json_filepath, layer_name, workdir):
    """Write a temp copy of the state with non-point annotations stripped from layer.

    The upstream ``get_anno_array_from_json`` does ``anno["point"]`` blindly,
    which KeyErrors on ``axis_aligned_bounding_box`` (and any other) annotation
    types that get accidentally mixed into a points-only layer. Filter them out
    so the rest of the pipeline doesn't care.

    Returns: (sanitized_json_path, stats_dict) where stats has 'kept' and 'dropped'.
    """
    with open(json_filepath) as f:
        state = json.load(f)
    kept = dropped = 0
    found = False
    for layer in state.get("layers", []):
        if layer.get("name") == layer_name and layer.get("type") == "annotation":
            found = True
            new_annos = []
            for anno in layer.get("annotations", []):
                if anno.get("type") == "point" and "point" in anno:
                    new_annos.append(anno)
                    kept += 1
                else:
                    dropped += 1
            layer["annotations"] = new_annos
            break
    if not found:
        raise ValueError(f"layer {layer_name!r} not found (or not an annotation layer) in {json_filepath}")
    out_path = os.path.join(workdir, "_sanitized_state.json")
    with open(out_path, "w") as f:
        json.dump(state, f)
    return out_path, {"kept": kept, "dropped": dropped}


def voxelize_mesh_to_chunks(
    volume_path,
    mesh,
    seg_resolution,
    chunk_size=(128, 128, 16),
    segid=1,
    verbose=True,
):
    """Voxelize ``mesh`` (vertices in nm) and write chunked raw uint64 segmentation.

    Side effects:
      * writes ``<volume_path>/<scale_key>/<x0-x1>_<y0-y1>_<z0-z1>`` for each
        non-empty chunk (raw uint64, Fortran order, single channel)
      * rewrites ``<volume_path>/info`` so the only scale is the one we just
        wrote and ``mesh: "mesh"`` is preserved (mesh subdir is untouched)

    Chunks are aligned to the global EM frame at the chosen resolution
    (voxel_offset = 0, so chunk filename coords are real EM voxels). Missing
    chunks render as background in NG (= no segid present at that voxel).

    Args:
      volume_path: directory containing the existing `info` and `mesh/`
                   subdir (typically the ``image_dir`` returned by
                   ``json_to_volume_ben``).
      mesh: trimesh.Trimesh with vertices already in nm.
      seg_resolution: (rx, ry, rz) target voxel size, nm per voxel.
      chunk_size: chunk shape in voxels.
      segid: integer label to write inside the mesh interior.
      verbose: print per-stage progress.
    """
    seg_res = np.asarray(seg_resolution, dtype=float)
    cs = np.asarray(chunk_size, dtype=np.int64)

    # 1) Scale mesh by 1/resolution so we can voxelize with isotropic pitch=1.
    #    After this scaling, "world" coords ARE voxel indices at the target res.
    inv_res = 1.0 / seg_res
    scaled_verts = np.asarray(mesh.vertices) * inv_res
    m_scaled = trimesh.Trimesh(
        vertices=scaled_verts,
        faces=np.asarray(mesh.faces),
        process=False,
    )
    # Use method='subdivide' because it produces a CLOSED shell on the voxel
    # grid (every face is bisected until edges are < pitch, then vertices are
    # snapped to voxel centers). method='ray' is faster but leaves gaps where
    # rays graze parallel faces, which lets binary_fill_holes leak through
    # and fill nothing. Default max_iter=10 is often too few for meshes scaled
    # into our pitch-1 frame — compute the needed iterations from the longest
    # edge so we don't trip the "max_iter exceeded" error.
    edges = m_scaled.edges_unique
    max_edge = float(np.linalg.norm(
        m_scaled.vertices[edges[:, 0]] - m_scaled.vertices[edges[:, 1]], axis=1
    ).max()) if len(edges) else 1.0
    needed_iter = max(10, int(np.ceil(np.log2(max(max_edge / 1.5, 1.0)))) + 2)
    if verbose:
        print(f"  longest scaled edge: {max_edge:.1f}; using subdivide max_iter={needed_iter}")
    vox = m_scaled.voxelized(pitch=1.0, method="subdivide", max_iter=needed_iter)
    shell = np.asarray(vox.matrix, dtype=bool)

    # vox.fill() in current trimesh is a near no-op on shell voxelizations
    # (it only fills isolated holes, not the entire interior). Do a real
    # flood-fill via scipy. Pad by 1 so the surface doesn't touch the array
    # boundary — otherwise binary_fill_holes treats "outside" as connected to
    # the interior and fills nothing.
    padded = np.pad(shell, 1, mode="constant", constant_values=False)
    filled = binary_fill_holes(padded)
    matrix = filled[1:-1, 1:-1, 1:-1].astype(bool)

    if not matrix.any():
        raise ValueError(
            f"voxelization produced no filled voxels. seg_resolution={seg_resolution} "
            f"may be too coarse for the mesh's extent (bounds in nm = "
            f"{np.asarray(mesh.bounds).tolist()})."
        )

    # vox.transform[:3,3] is the world-coord (= voxel-index, after our scaling)
    # of matrix[0,0,0]'s corner. Floor to int for chunk math.
    origin_voxel = np.floor(np.array(vox.transform[:3, 3])).astype(np.int64)
    matrix_shape = np.array(matrix.shape, dtype=np.int64)
    vox_min = origin_voxel
    vox_max = origin_voxel + matrix_shape

    if verbose:
        extent_um = (matrix_shape * seg_res / 1000.0)
        print(f"  voxelized: matrix shape {tuple(matrix_shape)} "
              f"~= ({extent_um[0]:.1f} x {extent_um[1]:.1f} x {extent_um[2]:.1f}) um")
        print(f"  voxel bbox: {tuple(vox_min)} -> {tuple(vox_max)} "
              f"(at {tuple(int(r) for r in seg_resolution)} nm/voxel)")

    # 2) Iterate over chunks that intersect the matrix.
    chunk_min = vox_min // cs
    chunk_max = (vox_max + cs - 1) // cs  # ceil

    scale_key = "_".join(str(int(r)) for r in seg_resolution)
    scale_dir = os.path.join(volume_path, scale_key)
    # Wipe any pre-existing chunks at this scale. Re-running on the same workdir
    # with different annotations would otherwise leave stale chunk files outside
    # the new mesh's bbox, and NG would render the union — making the seg layer
    # look like it covers more area than the current mesh. See README "Re-running
    # on the same workdir" note.
    if os.path.isdir(scale_dir):
        shutil.rmtree(scale_dir)
    os.makedirs(scale_dir, exist_ok=True)

    cs_x, cs_y, cs_z = int(cs[0]), int(cs[1]), int(cs[2])
    written = 0
    skipped = 0
    for cx in range(int(chunk_min[0]), int(chunk_max[0])):
        for cy in range(int(chunk_min[1]), int(chunk_max[1])):
            for cz in range(int(chunk_min[2]), int(chunk_max[2])):
                gx0, gy0, gz0 = cx * cs_x, cy * cs_y, cz * cs_z
                gx1, gy1, gz1 = gx0 + cs_x, gy0 + cs_y, gz0 + cs_z

                mx0 = max(0, gx0 - int(origin_voxel[0]))
                my0 = max(0, gy0 - int(origin_voxel[1]))
                mz0 = max(0, gz0 - int(origin_voxel[2]))
                mx1 = min(int(matrix_shape[0]), gx1 - int(origin_voxel[0]))
                my1 = min(int(matrix_shape[1]), gy1 - int(origin_voxel[1]))
                mz1 = min(int(matrix_shape[2]), gz1 - int(origin_voxel[2]))
                if mx0 >= mx1 or my0 >= my1 or mz0 >= mz1:
                    skipped += 1
                    continue
                sub = matrix[mx0:mx1, my0:my1, mz0:mz1]
                if not sub.any():
                    skipped += 1
                    continue

                lx0 = (int(origin_voxel[0]) + mx0) - gx0
                ly0 = (int(origin_voxel[1]) + my0) - gy0
                lz0 = (int(origin_voxel[2]) + mz0) - gz0

                chunk = np.zeros((cs_x, cs_y, cs_z), dtype=np.uint64)
                chunk[lx0:lx0 + (mx1 - mx0),
                      ly0:ly0 + (my1 - my0),
                      lz0:lz0 + (mz1 - mz0)] = sub.astype(np.uint64) * np.uint64(segid)

                fname = f"{gx0}-{gx1}_{gy0}-{gy1}_{gz0}-{gz1}"
                # Raw encoding: [x, y, z, channel=1] in Fortran (column-major) order.
                with open(os.path.join(scale_dir, fname), "wb") as fp:
                    fp.write(chunk.tobytes(order="F"))
                written += 1

    if verbose:
        print(f"  wrote {written} chunks ({skipped} empty chunks skipped) "
              f"under {scale_key}/")

    # 3) Rewrite info: single coarse scale, keep mesh pointer.
    info_path = os.path.join(volume_path, "info")
    with open(info_path) as f:
        info = json.load(f)

    # Match the full datastack EM extent so NG draws one combined bbox with the
    # EM layer instead of two boxes (the EM's big one + our tight mesh bbox).
    # `_write_volume_packaging` already set size from `em_size * em_res / voxel_scale`
    # using the datastack's EM-source info, so converting that to our seg resolution
    # keeps the same nm extent.
    orig_scale = info["scales"][0]
    em_extent_nm = [orig_scale["size"][i] * orig_scale["resolution"][i] for i in range(3)]
    em_offset_voxels = orig_scale.get("voxel_offset", [0, 0, 0])
    em_offset_nm = [em_offset_voxels[i] * orig_scale["resolution"][i] for i in range(3)]
    if any(v != 0 for v in em_offset_nm):
        # Chunks here are anchored at global voxel (0,0,0); if EM has a non-zero
        # offset, NG-side chunk alignment will be off by em_offset_nm / seg_res
        # voxels. BANC has em_offset=[0,0,0] so this branch never fires for it.
        print(f"  WARN: datastack EM has non-zero voxel_offset (in nm: {em_offset_nm}); "
              f"chunks are anchored at voxel 0 so 2D fill may be offset.")
    new_size = [int(-(-em_extent_nm[i] // int(seg_res[i]))) for i in range(3)]  # ceil

    info["scales"] = [{
        "encoding": "raw",
        "chunk_sizes": [[cs_x, cs_y, cs_z]],
        "key": scale_key,
        "resolution": [int(r) for r in seg_resolution],
        "voxel_offset": [0, 0, 0],
        "size": new_size,
    }]
    info["mesh"] = "mesh"
    info["type"] = "segmentation"
    info["data_type"] = "uint64"
    info["num_channels"] = 1
    with open(info_path, "w") as f:
        json.dump(info, f)

    return {
        "scale_key": scale_key,
        "chunks_written": written,
        "chunks_skipped": skipped,
        "volume_size": new_size,
        "origin_voxel": [int(v) for v in origin_voxel],
        "matrix_shape": [int(v) for v in matrix_shape],
    }


def state_to_ng_seg_layer_ben(
    json_filepath,
    layer_name,
    name,
    datastack_name="brain_and_nerve_cord",
    method="convex",
    alpha=None,
    bucket_root="nokura://tracers/ben",
    workdir=None,
    seg_resolution=(64, 64, 90),
    chunk_size=(128, 128, 16),
    segid=1,
    upload=True,
):
    """state.json annotation layer -> mesh + chunked seg volume + public NG layer.

    Like ``state_to_ng_layer_ben``, but ALSO voxelizes the generated mesh and
    writes chunked uint64 segmentation data so the NG layer renders in 2D
    cross-sections in addition to 3D.

    Returns dict with keys:
      'obj_path', 'volume_path', 'bucket_path', 'https_url', 'ng_source',
      'alpha_used', 'segid', 'voxelize_stats'.
    """
    if workdir is None:
        workdir = tempfile.mkdtemp(prefix=f"tracer_seg_{name}_")
    os.makedirs(workdir, exist_ok=True)

    print(f"[1/3] building mesh from {layer_name} in {json_filepath} (method={method})")
    sanitized_path, san_stats = _sanitize_state_for_layer(json_filepath, layer_name, workdir)
    if san_stats["dropped"]:
        print(f"      dropped {san_stats['dropped']} non-point annotation(s) "
              f"(kept {san_stats['kept']} point(s))")
    gen = json_to_volume_ben(
        datastack_name=datastack_name,
        json_filepath=sanitized_path,
        layer_name=layer_name,
        output_filepath=workdir,
        method=method,
        alpha=alpha,
        export_obj=True,
        segid=segid,
        build_volume=True,
    )
    mesh = gen["mesh"]
    volume_path = gen["volume_path"]

    print(f"      mesh: vertices={len(mesh.vertices)} faces={len(mesh.faces)} "
          f"watertight={mesh.is_watertight}")
    if gen["alpha_used"] is not None:
        print(f"      alpha used: {gen['alpha_used']:.1f} nm")

    print(f"[2/3] voxelizing mesh into chunks at resolution "
          f"{tuple(int(r) for r in seg_resolution)} nm/voxel "
          f"(chunk size {tuple(int(c) for c in chunk_size)} voxels)")
    stats = voxelize_mesh_to_chunks(
        volume_path=volume_path,
        mesh=mesh,
        seg_resolution=seg_resolution,
        chunk_size=chunk_size,
        segid=segid,
    )

    if not upload:
        print(f"[3/3] --no-upload: skipping bucket upload")
        return {
            "obj_path": gen["obj_path"],
            "volume_path": volume_path,
            "bucket_path": None,
            "https_url": None,
            "ng_source": None,
            "alpha_used": gen["alpha_used"],
            "segid": segid,
            "voxelize_stats": stats,
        }

    print(f"[3/3] uploading {volume_path} to {bucket_root.rstrip('/')}/{name}")
    bucket_path = f"{bucket_root.rstrip('/')}/{name}"
    upload_result = bucket_upload_folder_ben(
        local_path=volume_path,
        bucket_path=bucket_path,
        public_read=True,
    )
    ng_source = (
        ng_layer_source_for(upload_result["https_url"])
        if upload_result["https_url"] else None
    )

    return {
        "obj_path": gen["obj_path"],
        "volume_path": volume_path,
        "bucket_path": bucket_path,
        "https_url": upload_result["https_url"],
        "ng_source": ng_source,
        "alpha_used": gen["alpha_used"],
        "segid": segid,
        "voxelize_stats": stats,
    }


def _parse_triple(s):
    parts = [int(x) for x in s.split(",")]
    if len(parts) != 3:
        raise argparse.ArgumentTypeError(f"expected 'x,y,z'; got {s!r}")
    return tuple(parts)


def _cli():
    p = argparse.ArgumentParser(
        description="state.json annotation layer -> public NG layer with BOTH 2D and 3D"
    )
    p.add_argument("json_filepath")
    p.add_argument("--layer", required=True, help="annotation layer name")
    p.add_argument("--name", required=True, help="short name for the new layer (bucket folder)")
    p.add_argument("--datastack", default="brain_and_nerve_cord")
    p.add_argument("--method", choices=["convex", "alpha"], default="convex")
    p.add_argument("--alpha", type=float, default=None,
                   help="alpha-shape radius (nm); default auto")
    p.add_argument("--bucket-root", default="nokura://tracers/ben")
    p.add_argument("--workdir", default=None, help="scratch folder for intermediates")
    p.add_argument("--seg-resolution", type=_parse_triple, default=(64, 64, 90),
                   help="voxel size in nm for the seg volume; default 64,64,90. "
                        "Coarser -> faster + smaller upload, blockier 2D fill.")
    p.add_argument("--chunk-size", type=_parse_triple, default=(128, 128, 16),
                   help="chunk shape in voxels; default 128,128,16")
    p.add_argument("--segid", type=int, default=1)
    p.add_argument("--no-upload", action="store_true",
                   help="generate locally only; skip bucket upload")
    p.add_argument("--tracer-path", default=None,
                   help="manual path to tracer_tools/src (only if auto-detect failed)")
    args = p.parse_args()

    result = state_to_ng_seg_layer_ben(
        json_filepath=args.json_filepath,
        layer_name=args.layer,
        name=args.name,
        datastack_name=args.datastack,
        method=args.method,
        alpha=args.alpha,
        bucket_root=args.bucket_root,
        workdir=args.workdir,
        seg_resolution=args.seg_resolution,
        chunk_size=args.chunk_size,
        segid=args.segid,
        upload=not args.no_upload,
    )

    print(f"\n=== done ===")
    if result["alpha_used"] is not None:
        print(f"alpha used: {result['alpha_used']:.1f} nm")
    print(f"obj:    {result['obj_path']}")
    print(f"volume: {result['volume_path']}")
    print(f"chunks written: {result['voxelize_stats']['chunks_written']} "
          f"(scale {result['voxelize_stats']['scale_key']} nm)")
    if result["bucket_path"]:
        print(f"bucket: {result['bucket_path']}")
        print(f"https:  {result['https_url']}")
        print(f"\nadd a new segmentation layer in NG with source:")
        print(f"  {result['ng_source']}")
        print(f"and add segid {result['segid']} to its visible segments. "
              f"The layer should appear in both 2D and 3D.")
    else:
        print(f"\nlocal-only mode. inspect {result['volume_path']}, then re-run without "
              f"--no-upload to publish.")


if __name__ == "__main__":
    _cli()
