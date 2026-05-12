"""json_to_volume_ben — state.json annotation layer → mesh + precomputed volume folder.

Improvements over the upstream `json_to_volume`:
  - Works on Windows (skips the cloudvolume.Mesh.put step that crashes with
    `intervaltree` zero-length intervals and colon-in-filename errors).
  - Supports `method="alpha"` for concave/shrinkwrap shapes via 3D alpha shape
    (scipy.spatial.Delaunay-based, no extra deps).
  - Writes the precomputed volume with `___` substitutes for filenames
    containing ':', so the local folder is Windows-safe. `bucket_upload_folder_ben`
    translates them back to ':' on upload.
  - Pulls bbox/`size` for the top-level info from the datastack's EM-source info
    file, not hardcoded BANC values — works for any datastack.
"""
import os
import json
import argparse
import urllib.request
from collections import Counter

import numpy as np
import trimesh
import cloudvolume

# Portable tracer_tools import: works whether this script lives inside
# tracer_tools/src/ (dev clone) or alongside the package as a sibling
# (e.g. inside tracer-tools-extensions/). Mirrors the pattern used by
# fast_validate_ids_updated_auto_detect_path.py.
import sys
from pathlib import Path


def _find_tracer_tools(manual_path=None):
    here = Path(__file__).resolve().parent
    candidates = []
    if manual_path:
        candidates.append(Path(manual_path))
    candidates += [
        here,                                                # script lives in tracer_tools/src/
        here / "tracer_tools" / "src",                       # script alongside a tracer_tools checkout
        here.parent / "tracer_tools" / "src",                # one level above
        here / "Tracer - Workspace" / "tracer_tools" / "src",
        here.parent / "Tracer - Workspace" / "tracer_tools" / "src",
    ]
    for p in candidates:
        if (p / "tracer_tools" / "__init__.py").exists():
            return p
    return None


import argparse as _ap
_pre = _ap.ArgumentParser(add_help=False)
_pre.add_argument("--tracer-path", default=None)
_known, _ = _pre.parse_known_args()
_tt_path = _find_tracer_tools(_known.tracer_path)
if _tt_path is None:
    print("ERROR: Could not find tracer_tools/src directory.")
    print("Pass --tracer-path \"C:\\path\\to\\tracer_tools\\src\" or place this script "
          "alongside your tracer_tools checkout.")
    sys.exit(1)
sys.path.insert(0, str(_tt_path))

from tracer_tools.utils import get_config, get_anno_array_from_json, convert_coord_res


def _tet_circumradii(tetras):
    """Vectorized circumradius for an array of tetrahedra. shape (n,4,3) -> (n,)."""
    p0 = tetras[:, 0]
    a = tetras[:, 1] - p0
    b = tetras[:, 2] - p0
    c = tetras[:, 3] - p0
    A = np.stack([a, b, c], axis=1)                          # (n, 3, 3)
    rhs = 0.5 * np.stack([(a*a).sum(1), (b*b).sum(1), (c*c).sum(1)], axis=1)
    try:
        x = np.linalg.solve(A, rhs[..., None])[..., 0]       # (n, 3)
    except np.linalg.LinAlgError:
        # Fall back to per-tet, tolerate degenerate (coplanar) tetrahedra.
        x = np.zeros_like(rhs)
        for i in range(A.shape[0]):
            try:
                x[i] = np.linalg.solve(A[i], rhs[i])
            except np.linalg.LinAlgError:
                x[i] = np.full(3, np.inf)
    return np.linalg.norm(x, axis=1)


def _alpha_shape_once(points, tess, radii, alpha):
    """One pass of alpha-complex boundary extraction at a fixed alpha.

    Returns (vertices_clean, faces_clean, kept_count) with unreferenced
    annotation points dropped and face indices remapped.
    """
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
    """3D alpha shape (concave hull) via Delaunay tetrahedralization.

    Tetrahedra with circumradius < alpha are kept; the mesh is the boundary
    (triangles that appear in exactly one kept tet).

    Args:
      points: (n, 3) array of point positions.
      alpha:  float or None.
              - If a float: use it directly.
              - If None and auto_grow=False: initial guess = 1.5 × median NN distance.
              - If None and auto_grow=True (default): start at the initial guess and
                grow alpha (×1.5 per iter) until the boundary mesh is a single
                connected component. This is almost always what you want for region
                outlines — small enough to follow concavities, large enough not to break.

    Returns (vertices, faces, alpha_used). Vertices are remapped (no orphans).
    """
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
            raise ValueError(
                f"alpha={alpha:.1f} produced no boundary triangles. "
                f"Try a larger alpha (median tet circumradius is {np.median(radii):.1f})."
            )
        return v, f, alpha

    # Auto-grow target: smallest alpha that produces a single-component AND watertight mesh.
    # If watertight is unreachable within max_iters, fall back to the smallest-alpha
    # single-component result we found (still NG-renderable, just has small holes).
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
                m.fix_normals()  # consistent winding for clean rendering in NG
                return np.asarray(m.vertices), np.asarray(m.faces), cur_alpha
        cur_alpha *= 1.5

    if single_piece is None:
        raise ValueError(f"alpha shape failed to produce a single-component mesh at any alpha up to {cur_alpha:.1f}.")
    return single_piece


def _fetch_em_source_size(datastack_name):
    """Fetch the EM-source `info` and return (size, voxel_offset, resolution).

    Used to dimension the top-level segmentation info so the bbox matches the
    actual EM volume extent for this datastack (instead of hardcoding BANC's).

    Routes gs:// through the public storage.googleapis.com HTTPS endpoint to
    avoid GCS auth (these EM source buckets are public). Falls back to
    cloudfiles for s3:// / nokura:// / etc.
    """
    cfg = get_config(datastack_name)
    em_url = cfg["em_source_url"]
    if em_url.startswith("precomputed://"):
        em_url = em_url[len("precomputed://"):]
    em_url = em_url.rstrip("/")

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


def _write_volume_packaging(output_filepath, datastack_name):
    """Create <output_filepath>/image/{info, mesh/info} using EM-derived bounds.

    info `size` comes from the EM source so the bbox matches the canonical volume
    (matches Jay's pattern: austin/test_volume, rough_spots/banc/01 both use BANC EM size).
    """
    cfg = get_config(datastack_name)
    voxel_scale = cfg["voxel_scale"]
    em_size, em_offset, em_res = _fetch_em_source_size(datastack_name)

    # Convert EM-resolution voxel size to mesh-resolution voxel size.
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
    """Write the manifest and fragment to image/mesh/ with `___` substitutes.

    Local files: `1___0` (manifest JSON, content uses real ':') and `1___0___1`
    (binary fragment via cloudvolume.Mesh.to_precomputed()). bucket_upload_folder_ben
    translates the filenames to real ':' on upload.
    """
    mesh_dir = os.path.join(image_dir, "mesh")
    cv_mesh = cloudvolume.Mesh(vertices=np.asarray(mesh.vertices), faces=np.asarray(mesh.faces), segid=segid)
    manifest_path = os.path.join(mesh_dir, f"{segid}___0")
    fragment_path = os.path.join(mesh_dir, f"{segid}___0___1")
    with open(manifest_path, "w") as f:
        json.dump({"fragments": [f"{segid}:0:1"]}, f)
    with open(fragment_path, "wb") as f:
        f.write(cv_mesh.to_precomputed())


def json_to_volume_ben(
    datastack_name,
    json_filepath,
    layer_name,
    output_filepath,
    method="convex",
    alpha=None,
    export_obj=True,
    segid=1,
    build_volume=True,
):
    """Generate a mesh (and optionally a precomputed volume folder) from a NG annotation layer.

    Args:
      datastack_name: e.g. "brain_and_nerve_cord" — must appear in get_config_names().
      json_filepath: absolute path to a NG state JSON.
      layer_name: name of the annotation layer to read points from.
      output_filepath: folder where outputs are written.
      method: "convex" (default) or "alpha".
      alpha: alpha-shape radius threshold in nm. None = auto (1.5 × median NN distance).
      export_obj: also write `<layer_name>_<method>.obj` for Blender editing.
      segid: integer segment id (manifest is `<segid>:0`, fragment `<segid>:0:1`).
      build_volume: if False, skip creating the `image/` precomputed folder
                    (use when you only want the obj and will edit in Blender first).

    Returns dict with keys:
      'mesh': trimesh.Trimesh — the generated mesh
      'obj_path': str | None — path to written obj, if export_obj
      'volume_path': str | None — path to `image/` folder, if build_volume
      'alpha_used': float | None — actual alpha value used, for method="alpha"
    """
    os.makedirs(output_filepath, exist_ok=True)

    cfg = get_config(datastack_name)
    voxel_scale = cfg["voxel_scale"]
    points_voxel = get_anno_array_from_json(layer_name, json_filepath=json_filepath)
    points_nm = np.array([convert_coord_res(p, res_current=voxel_scale, res_desired=[1, 1, 1]) for p in points_voxel])

    if method == "convex":
        mesh = trimesh.PointCloud(points_nm).convex_hull
        alpha_used = None
    elif method == "alpha":
        vertices, faces, alpha_used = alpha_shape_3d(points_nm, alpha=alpha)
        mesh = trimesh.Trimesh(vertices=vertices, faces=faces, process=False)
    else:
        raise ValueError(f"unknown method {method!r}; use 'convex' or 'alpha'")

    obj_path = None
    if export_obj:
        suffix = method if method == "convex" else f"alpha{int(alpha_used)}"
        obj_path = os.path.join(output_filepath, f"{layer_name}_{suffix}.obj")
        mesh.export(obj_path, file_type="obj")

    volume_path = None
    if build_volume:
        volume_path = _write_volume_packaging(output_filepath, datastack_name)
        _write_mesh_files(volume_path, mesh, segid=segid)

    return {"mesh": mesh, "obj_path": obj_path, "volume_path": volume_path, "alpha_used": alpha_used}


def _cli():
    p = argparse.ArgumentParser(description="state.json annotation layer → mesh + precomputed volume folder")
    p.add_argument("json_filepath")
    p.add_argument("--datastack", default="brain_and_nerve_cord")
    p.add_argument("--layer", required=True, help="name of the annotation layer to mesh")
    p.add_argument("--out", required=True, help="output folder")
    p.add_argument("--method", choices=["convex", "alpha"], default="convex")
    p.add_argument("--alpha", type=float, default=None, help="alpha-shape radius (nm); default auto")
    p.add_argument("--segid", type=int, default=1)
    p.add_argument("--no-obj", action="store_true", help="skip writing the obj file")
    p.add_argument("--no-volume", action="store_true", help="skip writing the image/ folder (obj only)")
    p.add_argument("--tracer-path", default=None, help="manual path to tracer_tools/src (only if auto-detect failed)")
    args = p.parse_args()

    result = json_to_volume_ben(
        datastack_name=args.datastack,
        json_filepath=args.json_filepath,
        layer_name=args.layer,
        output_filepath=args.out,
        method=args.method,
        alpha=args.alpha,
        export_obj=not args.no_obj,
        segid=args.segid,
        build_volume=not args.no_volume,
    )
    m = result["mesh"]
    print(f"vertices: {len(m.vertices)}, faces: {len(m.faces)}, watertight: {m.is_watertight}")
    if result["alpha_used"] is not None:
        print(f"alpha used: {result['alpha_used']:.1f} nm")
    if result["obj_path"]:
        print(f"obj: {result['obj_path']}")
    if result["volume_path"]:
        print(f"volume: {result['volume_path']}")


if __name__ == "__main__":
    _cli()
