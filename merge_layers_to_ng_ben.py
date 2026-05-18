"""merge_layers_to_ng_ben — merge multiple annotation layers into ONE mesh + NG layer.

Same end-to-end flow as state_to_ng_layer_ben, but the point clouds from N
annotation layers are concatenated BEFORE meshing, producing a single connected
shape (one convex hull or one alpha shape) rather than N separate meshes.

Default layer selection:
  - Picks every layer with type=="annotation" in the state.json.
  - Excludes layers whose name matches the --exclude list (default:
    region_boundaries, region_outlines, region_boundary, region_outline,
    bounding_box, bounds, bbox). These are typically bbox/outline annotations
    unrelated to the structures being meshed.
  - Pass --layers a,b,c to override and use an explicit list instead.

Example:
  python merge_layers_to_ng_ben.py state.json --name first_bulb_merged
  python merge_layers_to_ng_ben.py state.json --name all_blobs --method alpha
  python merge_layers_to_ng_ben.py state.json --name custom --layers annotation1,first_bulb
  python merge_layers_to_ng_ben.py state.json --name preview --dry-run
"""
import os
import sys
import json
import tempfile
import argparse
from pathlib import Path

import numpy as np


DEFAULT_EXCLUDE = [
    "region_boundaries",
    "region_boundary",
    "region_outlines",
    "region_outline",
    "region boundaries",
    "region boundary",
    "region outlines",
    "region outline",
    "bounding_box",
    "bounds",
    "bbox",
]


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


def discover_annotation_layers(json_filepath, exclude=None):
    """Return [layer_name, ...] for every type=='annotation' layer in the state,
    minus any whose name matches `exclude` (case/space/underscore insensitive)."""
    if exclude is None:
        exclude = DEFAULT_EXCLUDE
    norm_excl = {_norm(n) for n in exclude}

    with open(json_filepath) as f:
        state = json.load(f)

    names = []
    for layer in state.get("layers", []):
        if layer.get("type") != "annotation":
            continue
        name = layer.get("name", "")
        if _norm(name) in norm_excl:
            continue
        if not layer.get("annotations"):
            continue
        names.append(name)
    return names


def _norm(s):
    return "".join(s.lower().split()).replace("_", "").replace("-", "")


def merge_layers_to_ng_ben(
    json_filepath,
    name,
    layer_names=None,
    exclude=None,
    datastack_name="brain_and_nerve_cord",
    method="convex",
    alpha=None,
    bucket_root="nokura://tracers/ben",
    workdir=None,
    segid=1,
    export_obj=True,
    upload=True,
):
    """Merge N annotation layers' points into ONE mesh and (optionally) upload it.

    Args:
      json_filepath: absolute path to a NG state.json.
      name: short name for the new mesh (used as the bucket folder).
      layer_names: explicit list of annotation layer names to merge. If None,
                   auto-discover all annotation-type layers, minus `exclude`.
      exclude: list of layer names to skip during auto-discovery
               (default: region_boundaries/region_outlines/bbox variants).
      datastack_name: datastack the state is from (controls voxel scale + info bounds).
      method: "convex" (default) or "alpha".
      alpha: alpha-shape radius in nm; None=auto.
      bucket_root: bucket prefix; default 'nokura://tracers/ben'.
      workdir: scratch folder; default a temp dir.
      segid: integer segment id for the mesh.
      export_obj: also write the merged OBJ alongside the volume.
      upload: if False, skip the bucket upload step (local-only build).

    Returns dict with keys:
      'layers_used': [str, ...] — layer names actually merged
      'point_counts': [int, ...] — per-layer point counts (same order)
      'total_points': int
      'mesh': trimesh.Trimesh — the merged mesh
      'obj_path': str | None
      'volume_path': str
      'alpha_used': float | None
      'bucket_path': str | None
      'https_url': str | None
      'ng_source': str | None
    """
    # Imports deferred so --dry-run doesn't require trimesh/cloudvolume/etc.
    import trimesh
    from json_to_volume_ben import (
        alpha_shape_3d,
        _write_volume_packaging,
        _write_mesh_files,
    )
    from tracer_tools.utils import get_config, get_anno_array_from_json, convert_coord_res

    if layer_names is None:
        layer_names = discover_annotation_layers(json_filepath, exclude=exclude)
    if not layer_names:
        raise ValueError("no annotation layers selected; pass --layers or check --exclude")

    cfg = get_config(datastack_name)
    voxel_scale = cfg["voxel_scale"]

    per_layer_points = []
    point_counts = []
    for ln in layer_names:
        pts_voxel = get_anno_array_from_json(ln, json_filepath=json_filepath)
        if pts_voxel is None or len(pts_voxel) == 0:
            print(f"  [skip] {ln}: no point annotations")
            point_counts.append(0)
            continue
        pts_nm = np.array([convert_coord_res(p, res_current=voxel_scale, res_desired=[1, 1, 1]) for p in pts_voxel])
        per_layer_points.append(pts_nm)
        point_counts.append(len(pts_nm))
        print(f"  [layer] {ln}: {len(pts_nm)} points")

    if not per_layer_points:
        raise ValueError("all selected layers were empty after loading points")

    merged = np.concatenate(per_layer_points, axis=0)
    print(f"  [merged] {len(merged)} total points across {len(per_layer_points)} layers")

    if method == "convex":
        mesh = trimesh.PointCloud(merged).convex_hull
        alpha_used = None
    elif method == "alpha":
        verts, faces, alpha_used = alpha_shape_3d(merged, alpha=alpha)
        mesh = trimesh.Trimesh(vertices=verts, faces=faces, process=False)
    else:
        raise ValueError(f"unknown method {method!r}; use 'convex' or 'alpha'")

    if workdir is None:
        workdir = tempfile.mkdtemp(prefix=f"tracer_merge_{name}_")
    os.makedirs(workdir, exist_ok=True)

    obj_path = None
    if export_obj:
        suffix = method if method == "convex" else f"alpha{int(alpha_used)}"
        obj_path = os.path.join(workdir, f"{name}_merged_{suffix}.obj")
        mesh.export(obj_path, file_type="obj")

    volume_path = _write_volume_packaging(workdir, datastack_name)
    _write_mesh_files(volume_path, mesh, segid=segid)

    bucket_path = https_url = ng_source = None
    if upload:
        from bucket_upload_folder_ben import bucket_upload_folder_ben, ng_layer_source_for
        bucket_path = f"{bucket_root.rstrip('/')}/{name}"
        up = bucket_upload_folder_ben(local_path=volume_path, bucket_path=bucket_path, public_read=True)
        https_url = up["https_url"]
        ng_source = ng_layer_source_for(https_url) if https_url else None

    return {
        "layers_used": layer_names,
        "point_counts": point_counts,
        "total_points": int(sum(point_counts)),
        "mesh": mesh,
        "obj_path": obj_path,
        "volume_path": volume_path,
        "alpha_used": alpha_used,
        "bucket_path": bucket_path,
        "https_url": https_url,
        "ng_source": ng_source,
    }


def _cli():
    p = argparse.ArgumentParser(
        description="Merge multiple annotation layers from a state.json into ONE NG-hosted mesh layer"
    )
    p.add_argument("json_filepath")
    p.add_argument("--name", required=True, help="short name for the merged mesh (used as bucket folder)")
    p.add_argument("--layers", default=None,
                   help="comma-separated layer names to merge; if omitted, auto-pick all annotation layers minus --exclude")
    p.add_argument("--exclude", default=",".join(DEFAULT_EXCLUDE),
                   help="comma-separated names to skip during auto-discovery (case/space/underscore insensitive)")
    p.add_argument("--datastack", default="brain_and_nerve_cord")
    p.add_argument("--method", choices=["convex", "alpha"], default="convex")
    p.add_argument("--alpha", type=float, default=None, help="alpha-shape radius (nm); default auto")
    p.add_argument("--bucket-root", default="nokura://tracers/ben")
    p.add_argument("--workdir", default=None)
    p.add_argument("--segid", type=int, default=1)
    p.add_argument("--no-upload", action="store_true", help="build mesh + volume locally, skip bucket upload")
    p.add_argument("--dry-run", action="store_true",
                   help="just print the resolved layer list and per-layer point counts; no mesh build, no upload")
    p.add_argument("--tracer-path", default=None, help="manual path to tracer_tools/src (only if auto-detect failed)")
    args = p.parse_args()

    if args.tracer_path:
        sys.path.insert(0, args.tracer_path)

    layer_names = [s.strip() for s in args.layers.split(",")] if args.layers else None
    exclude = [s.strip() for s in args.exclude.split(",")] if args.exclude else []

    if args.dry_run:
        if layer_names is None:
            layer_names = discover_annotation_layers(args.json_filepath, exclude=exclude)
        with open(args.json_filepath) as f:
            state = json.load(f)
        counts_by_name = {
            l.get("name"): len(l.get("annotations", []))
            for l in state.get("layers", []) if l.get("type") == "annotation"
        }
        print(f"=== dry-run: {len(layer_names)} layer(s) selected ===")
        total = 0
        for ln in layer_names:
            c = counts_by_name.get(ln, 0)
            total += c
            print(f"  {ln}: {c} annotations")
        print(f"--- total: {total} annotations (raw count; per-layer point yield may differ if non-point types are present) ---")
        skipped = [n for n, _ in counts_by_name.items() if n not in layer_names]
        if skipped:
            print(f"skipped (excluded or empty): {skipped}")
        return

    result = merge_layers_to_ng_ben(
        json_filepath=args.json_filepath,
        name=args.name,
        layer_names=layer_names,
        exclude=exclude,
        datastack_name=args.datastack,
        method=args.method,
        alpha=args.alpha,
        bucket_root=args.bucket_root,
        workdir=args.workdir,
        segid=args.segid,
        upload=not args.no_upload,
    )

    print(f"\n=== done ===")
    print(f"layers merged: {len(result['layers_used'])}")
    print(f"total points:  {result['total_points']}")
    if result["alpha_used"] is not None:
        print(f"alpha used:    {result['alpha_used']:.1f} nm")
    if result["obj_path"]:
        print(f"obj:    {result['obj_path']}")
    print(f"volume: {result['volume_path']}")
    if result["bucket_path"]:
        print(f"bucket: {result['bucket_path']}")
        print(f"https:  {result['https_url']}")
        print(f"\nNG layer source (paste into a new layer's source field):")
        print(f"  {result['ng_source']}")
        print(f"state.json subsources: {{\"bounds\": true, \"mesh\": true}}, enableDefaultSubsources: false")


if __name__ == "__main__":
    _cli()
