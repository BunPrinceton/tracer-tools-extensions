"""merge_layers_to_ng_ben — merge multiple annotation layers into ONE mesh + NG layer.

Same end-to-end flow as state_to_ng_layer_ben, but the point clouds from N
annotation layers are concatenated BEFORE meshing, producing a single connected
shape (one convex hull or one alpha shape) rather than N separate meshes.

Self-contained: derives voxel scale + EM source URL directly from the
state.json, so it doesn't depend on tracer_tools' utils.get_config /
get_anno_array_from_json (which aren't published yet). Only external sibling
import is bucket_upload_folder_ben for the upload step.

Combine modes (--combine):
  - merged (default): concatenate the point clouds from all selected layers BEFORE
    meshing. One global hull/alpha shape. Bridges naturally across layers but
    will fill in intentional voids between clusters (e.g. a tower-with-base gap
    becomes a bell shape).
  - per-layer: mesh each layer independently, then concatenate the meshes into
    a single OBJ. Voids between layers are preserved (each blob is its own
    watertight shape). Best when the layers represent structurally distinct
    pieces (towers, U-shapes, separated chambers) and the inter-layer gaps
    should remain empty.

Default layer selection:
  - Picks every layer with type=="annotation" in the state.json.
  - Excludes layers whose name matches the --exclude list (default:
    region_boundaries, region_outlines, region_boundary, region_outline,
    bounding_box, bounds, bbox). These are typically bbox/outline annotations
    unrelated to the structures being meshed.
  - Excludes layers with fewer than --min-points annotations (default 10) —
    filters out orphan/stray layers (e.g. a UI-deleted layer that still sits in
    the JSON with 1-3 points) without needing to hard-code their names.
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
import urllib.request
from collections import Counter
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


# ---------- state.json helpers (replace tracer_tools.utils.get_config + get_anno_array_from_json) ----------

def _norm(s):
    return "".join(s.lower().split()).replace("_", "").replace("-", "")


def _load_state(json_filepath):
    with open(json_filepath) as f:
        return json.load(f)


def _voxel_scale_from_state(state):
    """Voxel size in nm per axis, read from the state's top-level 'dimensions' field.

    Neuroglancer stores each axis as [value, unit] where unit is "m" (meters),
    "nm", etc. We normalize to nm.
    """
    dims = state.get("dimensions") or {}
    scale = []
    for ax in ("x", "y", "z"):
        if ax not in dims:
            raise ValueError(f"state.json missing dimensions.{ax}; cannot derive voxel scale")
        val, unit = dims[ax]
        unit = unit.lower()
        if unit == "m":
            scale.append(float(val) * 1e9)
        elif unit == "nm":
            scale.append(float(val))
        elif unit == "um":
            scale.append(float(val) * 1e3)
        else:
            raise ValueError(f"unsupported dimensions unit {unit!r} for {ax}; want m/nm/um")
    return scale


def _em_source_url_from_state(state, prefer_name_contains=None):
    """Pull the EM (image-layer) source URL from the state.

    If multiple image layers exist, prefer one whose name contains
    prefer_name_contains (case-insensitive); otherwise return the first.
    Strips a leading 'precomputed://' if present.
    """
    candidates = []
    for layer in state.get("layers", []):
        if layer.get("type") != "image":
            continue
        src = layer.get("source")
        url = src.get("url") if isinstance(src, dict) else src
        if not url:
            continue
        candidates.append((layer.get("name", ""), url))
    if not candidates:
        raise ValueError("no image layer with a source URL found in state.json")
    if prefer_name_contains:
        needle = prefer_name_contains.lower()
        for name, url in candidates:
            if needle in name.lower():
                url = url
                break
        else:
            url = candidates[0][1]
    else:
        url = candidates[0][1]
    if url.startswith("precomputed://"):
        url = url[len("precomputed://"):]
    return url.rstrip("/")


def _fetch_em_source_size(em_url):
    """Return (size, voxel_offset, resolution) from <em_url>/info.

    Routes gs:// through the public storage.googleapis.com HTTPS endpoint
    (these EM source buckets are public). Falls back to cloudfiles for
    other protocols (s3://, nokura://, etc).
    """
    info = None
    if em_url.startswith("gs://"):
        https_url = "https://storage.googleapis.com/" + em_url[len("gs://"):] + "/info"
        with urllib.request.urlopen(https_url, timeout=15) as resp:
            info = json.loads(resp.read())
    elif em_url.startswith(("http://", "https://")):
        with urllib.request.urlopen(em_url + "/info", timeout=15) as resp:
            info = json.loads(resp.read())
    else:
        from cloudfiles import CloudFiles
        info = CloudFiles(em_url).get_json("info")
    if info is None:
        raise RuntimeError(f"could not fetch info from {em_url}/info")
    scale = info["scales"][0]
    return scale["size"], scale.get("voxel_offset", [0, 0, 0]), scale["resolution"]


def _points_from_annotation_layer(layer):
    """Return (n, 3) array of point coords (voxel units, as stored in the state).

    Accepts annotations of type 'point' (uses 'point') or 'ellipsoid' (uses
    'center'). Other types (line/aabb) are skipped — they don't naturally
    contribute a single representative point.
    """
    out = []
    for a in layer.get("annotations", []):
        p = a.get("point") or a.get("center")
        if p is None or len(p) < 3:
            continue
        out.append([float(p[0]), float(p[1]), float(p[2])])
    return np.asarray(out, dtype=float)


# ---------- mesh build + volume packaging (inlined from json_to_volume_ben) ----------

def _tet_circumradii(tetras):
    p0 = tetras[:, 0]
    a = tetras[:, 1] - p0
    b = tetras[:, 2] - p0
    c = tetras[:, 3] - p0
    A = np.stack([a, b, c], axis=1)
    rhs = 0.5 * np.stack([(a*a).sum(1), (b*b).sum(1), (c*c).sum(1)], axis=1)
    try:
        x = np.linalg.solve(A, rhs[..., None])[..., 0]
    except np.linalg.LinAlgError:
        x = np.zeros_like(rhs)
        for i in range(A.shape[0]):
            try:
                x[i] = np.linalg.solve(A[i], rhs[i])
            except np.linalg.LinAlgError:
                x[i] = np.full(3, np.inf)
    return np.linalg.norm(x, axis=1)


def _alpha_shape_once(points, tess, radii, alpha):
    keep = tess.simplices[radii < alpha]
    if len(keep) == 0:
        return None, None, 0
    face_counter = Counter()
    for tet in keep:
        for combo in ((0, 1, 2), (0, 1, 3), (0, 2, 3), (1, 2, 3)):
            face_counter[tuple(sorted(int(tet[i]) for i in combo))] += 1
    faces = np.array([f for f, c in face_counter.items() if c == 1], dtype=np.int64)
    if len(faces) == 0:
        return None, None, len(keep)
    used = np.unique(faces.ravel())
    remap = -np.ones(len(points), dtype=np.int64)
    remap[used] = np.arange(len(used))
    return points[used], remap[faces], len(keep)


def alpha_shape_3d(points, alpha=None, auto_grow=True, max_iters=15):
    """3D alpha shape (concave hull) via Delaunay tetrahedralization."""
    import trimesh
    from scipy.spatial import Delaunay, cKDTree

    points = np.asarray(points, dtype=float)
    tess = Delaunay(points)
    tets = points[tess.simplices]
    radii = _tet_circumradii(tets)

    if alpha is None:
        kd = cKDTree(points)
        nn_dist, _ = kd.query(points, k=2)
        alpha = 1.5 * float(np.median(nn_dist[:, 1]))

    if not auto_grow:
        v, f, _ = _alpha_shape_once(points, tess, radii, alpha)
        if v is None:
            raise ValueError(f"alpha={alpha:.1f} produced no boundary triangles.")
        return v, f, alpha

    single_piece = None
    cur_alpha = alpha
    for _ in range(max_iters):
        v, f, _ = _alpha_shape_once(points, tess, radii, cur_alpha)
        if v is None:
            cur_alpha *= 1.5
            continue
        m = trimesh.Trimesh(vertices=v, faces=f, process=False)
        if m.body_count == 1:
            if single_piece is None:
                single_piece = (v, f, cur_alpha)
            if m.is_watertight:
                m.fix_normals()
                return np.asarray(m.vertices), np.asarray(m.faces), cur_alpha
        cur_alpha *= 1.5

    if single_piece is None:
        raise ValueError(f"alpha shape failed to produce a single-component mesh up to {cur_alpha:.1f}.")
    return single_piece


def _write_volume_packaging(output_filepath, voxel_scale, em_url):
    """Create <output_filepath>/image/{info, mesh/info} sized to the EM source."""
    em_size, em_offset, em_res = _fetch_em_source_size(em_url)
    size = [int(em_size[i] * em_res[i] / voxel_scale[i]) for i in range(3)]
    voxel_offset = [int(em_offset[i] * em_res[i] / voxel_scale[i]) for i in range(3)]

    image_dir = os.path.join(output_filepath, "image")
    mesh_dir = os.path.join(image_dir, "mesh")
    os.makedirs(mesh_dir, exist_ok=True)

    info = {
        "num_channels": 1,
        "type": "segmentation",
        "data_type": "uint64",
        "scales": [{
            "encoding": "raw",
            "chunk_sizes": [[512, 512, 16]],
            "key": "_".join(str(v) for v in voxel_scale),
            "resolution": voxel_scale,
            "voxel_offset": voxel_offset,
            "size": size,
        }],
        "mesh": "mesh",
    }
    with open(os.path.join(image_dir, "info"), "w") as f:
        json.dump(info, f)

    mesh_info = {"@type": "neuroglancer_legacy_mesh", "transform": [1, 0, 0, 0, 0, 1, 0, 0, 0, 0, 1, 0]}
    with open(os.path.join(mesh_dir, "info"), "w") as f:
        json.dump(mesh_info, f)
    return image_dir


def _write_mesh_files(image_dir, mesh, segid=1):
    """Write the manifest + binary fragment to image/mesh/ with '___' substitutes
    (translated back to ':' by bucket_upload_folder_ben on upload)."""
    import cloudvolume
    mesh_dir = os.path.join(image_dir, "mesh")
    cv_mesh = cloudvolume.Mesh(vertices=np.asarray(mesh.vertices), faces=np.asarray(mesh.faces), segid=segid)
    manifest_path = os.path.join(mesh_dir, f"{segid}___0")
    fragment_path = os.path.join(mesh_dir, f"{segid}___0___1")
    with open(manifest_path, "w") as f:
        json.dump({"fragments": [f"{segid}:0:1"]}, f)
    with open(fragment_path, "wb") as f:
        f.write(cv_mesh.to_precomputed())


# ---------- single mesh build (called once for combine=merged, N times for per-layer) ----------

def _build_mesh(points, method, alpha, auto_grow):
    """Build a single mesh from a point array. Returns (mesh, alpha_used_or_None)."""
    import trimesh
    if method == "convex":
        return trimesh.PointCloud(points).convex_hull, None
    if method == "alpha":
        v, f, a = alpha_shape_3d(points, alpha=alpha, auto_grow=auto_grow)
        return trimesh.Trimesh(vertices=v, faces=f, process=False), a
    raise ValueError(f"unknown method {method!r}; use 'convex' or 'alpha'")


# ---------- bucket_upload_folder_ben locator (the only sibling we still need) ----------

def _find_ben_dir():
    here = Path(__file__).resolve().parent
    if (here / "bucket_upload_folder_ben.py").exists():
        return here
    for sib in (here / "tracer_tools" / "src", here.parent / "tracer_tools" / "src"):
        if (sib / "bucket_upload_folder_ben.py").exists():
            return sib
    return None


# ---------- public layer discovery + merge entrypoint ----------

def discover_annotation_layers(json_filepath, exclude=None, min_points=10):
    """Return [layer_name, ...] for every type=='annotation' layer in the state,
    minus any whose name matches `exclude` (case/space/underscore insensitive)
    or that has fewer than `min_points` annotations."""
    if exclude is None:
        exclude = DEFAULT_EXCLUDE
    norm_excl = {_norm(n) for n in exclude}

    state = _load_state(json_filepath)
    names = []
    for layer in state.get("layers", []):
        if layer.get("type") != "annotation":
            continue
        name = layer.get("name", "")
        if _norm(name) in norm_excl:
            continue
        anns = layer.get("annotations", [])
        if len(anns) < min_points:
            continue
        names.append(name)
    return names


def merge_layers_to_ng_ben(
    json_filepath,
    name,
    layer_names=None,
    exclude=None,
    min_points=10,
    em_layer_name_hint="BANC EM",
    method="convex",
    alpha=None,
    auto_grow=True,
    combine="merged",
    union=False,
    bucket_root="nokura://tracers/ben",
    workdir=None,
    segid=1,
    export_obj=True,
    upload=True,
):
    """Merge N annotation layers' points into ONE mesh and (optionally) upload it.

    Returns dict with keys:
      'layers_used', 'point_counts', 'total_points', 'mesh', 'obj_path',
      'volume_path', 'alpha_used', 'bucket_path', 'https_url', 'ng_source'.
    """
    import trimesh

    state = _load_state(json_filepath)
    voxel_scale = _voxel_scale_from_state(state)
    em_url = _em_source_url_from_state(state, prefer_name_contains=em_layer_name_hint)

    if layer_names is None:
        layer_names = discover_annotation_layers(json_filepath, exclude=exclude, min_points=min_points)
    if not layer_names:
        raise ValueError("no annotation layers selected; pass --layers or relax --exclude/--min-points")

    layers_by_name = {l.get("name"): l for l in state.get("layers", []) if l.get("type") == "annotation"}

    per_layer_points_nm = []
    used_layer_names = []
    point_counts = []
    for ln in layer_names:
        layer = layers_by_name.get(ln)
        if layer is None:
            print(f"  [skip] {ln}: not found in state")
            point_counts.append(0)
            continue
        pts_voxel = _points_from_annotation_layer(layer)
        if len(pts_voxel) == 0:
            print(f"  [skip] {ln}: no point-bearing annotations")
            point_counts.append(0)
            continue
        # voxel -> nm
        pts_nm = pts_voxel * np.asarray(voxel_scale, dtype=float)
        per_layer_points_nm.append(pts_nm)
        used_layer_names.append(ln)
        point_counts.append(len(pts_nm))
        print(f"  [layer] {ln}: {len(pts_nm)} points")

    if not per_layer_points_nm:
        raise ValueError("all selected layers were empty after loading points")

    if combine == "merged":
        merged = np.concatenate(per_layer_points_nm, axis=0)
        print(f"  [combine=merged] {len(merged)} total points across {len(per_layer_points_nm)} layers")
        mesh, alpha_used = _build_mesh(merged, method, alpha, auto_grow)
    elif combine == "per-layer":
        print(f"  [combine=per-layer] meshing {len(per_layer_points_nm)} layers independently")
        sub_meshes = []
        alphas_used = []
        for ln, pts in zip(used_layer_names, per_layer_points_nm):
            if len(pts) < 4:
                print(f"    [skip mesh] {ln}: needs >=4 points for 3D hull")
                continue
            try:
                m, a = _build_mesh(pts, method, alpha, auto_grow)
            except Exception as e:
                if method == "alpha":
                    print(f"    [fallback convex] {ln}: alpha failed ({e})")
                    m, a = _build_mesh(pts, "convex", None, False)
                else:
                    raise
            if a is not None:
                alphas_used.append(a)
            print(f"    [mesh] {ln}: V={len(m.vertices)} F={len(m.faces)} watertight={m.is_watertight}")
            sub_meshes.append(m)
        if not sub_meshes:
            raise ValueError("no per-layer meshes built (all layers had <4 points?)")
        alpha_used = max(alphas_used) if alphas_used else None

        if union:
            import time
            t0 = time.time()
            print(f"  [union] running boolean union over {len(sub_meshes)} components...")
            try:
                mesh = trimesh.boolean.union(sub_meshes)
                print(f"  [union] done in {time.time()-t0:.1f}s — V={len(mesh.vertices)} F={len(mesh.faces)} components={mesh.body_count} watertight={mesh.is_watertight}")
            except Exception as e:
                print(f"  [union] FAILED after {time.time()-t0:.1f}s: {e}")
                print(f"  [union] falling back to plain concatenation")
                mesh = trimesh.util.concatenate(sub_meshes)
                print(f"  [concatenated] V={len(mesh.vertices)} F={len(mesh.faces)} components={mesh.body_count}")
        else:
            mesh = trimesh.util.concatenate(sub_meshes)
            print(f"  [concatenated] V={len(mesh.vertices)} F={len(mesh.faces)} components={mesh.body_count}")
    else:
        raise ValueError(f"unknown combine mode {combine!r}; use 'merged' or 'per-layer'")

    if workdir is None:
        workdir = tempfile.mkdtemp(prefix=f"tracer_merge_{name}_")
    os.makedirs(workdir, exist_ok=True)

    obj_path = None
    if export_obj:
        suffix = method if method == "convex" else f"alpha{int(alpha_used)}"
        obj_path = os.path.join(workdir, f"{name}_merged_{suffix}.obj")
        mesh.export(obj_path, file_type="obj")

    volume_path = _write_volume_packaging(workdir, voxel_scale, em_url)
    _write_mesh_files(volume_path, mesh, segid=segid)

    bucket_path = https_url = ng_source = None
    if upload:
        ben_dir = _find_ben_dir()
        if ben_dir is None:
            raise RuntimeError("bucket_upload_folder_ben.py not found beside this script; pass --no-upload to skip")
        sys.path.insert(0, str(ben_dir))
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
    p.add_argument("--min-points", type=int, default=10,
                   help="skip auto-discovered layers with fewer than this many annotations (default 10; 0 = include all)")
    p.add_argument("--em-layer-hint", default="BANC EM",
                   help="image-layer name substring to prefer when picking the EM source for bbox sizing")
    p.add_argument("--combine", choices=["merged", "per-layer"], default="merged",
                   help="merged (default): one hull/alpha over all points; per-layer: mesh each layer independently then concat (preserves voids between layers)")
    p.add_argument("--union", action="store_true",
                   help="(combine=per-layer only) after per-layer meshing, run boolean union over the components so overlapping blobs merge into one watertight surface (cleaner visuals, no overlap creases). Slower (~seconds-minutes); falls back to plain concatenation on failure.")
    p.add_argument("--method", choices=["convex", "alpha"], default="convex")
    p.add_argument("--alpha", type=float, default=None, help="alpha-shape radius (nm); default auto")
    p.add_argument("--no-auto-grow", action="store_true",
                   help="(method=alpha only) use --alpha exactly; don't grow it until single-component/watertight. May produce holes or disconnected pieces if alpha is too small.")
    p.add_argument("--bucket-root", default="nokura://tracers/ben")
    p.add_argument("--workdir", default=None)
    p.add_argument("--segid", type=int, default=1)
    p.add_argument("--no-upload", action="store_true", help="build mesh + volume locally, skip bucket upload")
    p.add_argument("--dry-run", action="store_true",
                   help="just print the resolved layer list and per-layer point counts; no mesh build, no upload")
    args = p.parse_args()

    layer_names = [s.strip() for s in args.layers.split(",")] if args.layers else None
    exclude = [s.strip() for s in args.exclude.split(",")] if args.exclude else []

    if args.dry_run:
        if layer_names is None:
            layer_names = discover_annotation_layers(args.json_filepath, exclude=exclude, min_points=args.min_points)
        state = _load_state(args.json_filepath)
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
        print(f"--- total: {total} annotations ---")
        skipped = [n for n, _ in counts_by_name.items() if n not in layer_names]
        if skipped:
            print(f"skipped (excluded or below --min-points): {skipped}")
        print(f"\nvoxel scale (nm): {_voxel_scale_from_state(state)}")
        print(f"EM source URL:   {_em_source_url_from_state(state, prefer_name_contains=args.em_layer_hint)}")
        return

    result = merge_layers_to_ng_ben(
        json_filepath=args.json_filepath,
        name=args.name,
        layer_names=layer_names,
        exclude=exclude,
        min_points=args.min_points,
        em_layer_name_hint=args.em_layer_hint,
        method=args.method,
        alpha=args.alpha,
        auto_grow=not args.no_auto_grow,
        combine=args.combine,
        union=args.union,
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
